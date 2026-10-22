"""Telegram follow-up questions continue the agent session that produced a card or answer.

A reader replies to a Tank message, or forwards it to the bot with a comment. The referenced
message identifies the session through the delivery ledger; the question goes into that same
native session and the agent's final message of that turn is sent back as the answer.
"""

import asyncio
import contextlib
import html
import json
import logging
import re
import time
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher

import httpx

from tank.analysis.cards import telegram_card_html
from tank.time_precision import collection_clock, offset_schedule
from tank.util import new_id, parse_time, sha256, utcnow

log = logging.getLogger(__name__)

CURSOR_KEY = "telegram:bot-updates"
POLL_TIMEOUT_S = 20
PAIR_WINDOW_S = 10
FORWARD_MATCH_S = 30
MEMBER_CACHE_S = 600
MESSAGE_LIMIT = 4000
ANSWER_CHANNEL = "telegram:answer"
CARD_CHANNEL = "telegram:alerts"
MEMBER_STATUSES = {"creator", "administrator", "member"}

NO_SESSION_TEXT = (
    "Не нашёл сессию для этого сообщения. Ответьте на карточку Tank или перешлите её боту вместе с вопросом."
)
HINT_TEXT = "Ответьте на карточку Tank или перешлите её боту вместе с вопросом."
PROMPT_TEXT = "Какой вопрос по этой карточке? Ответьте на это сообщение."
FAILED_TEXT = "Не удалось получить ответ агента на этот вопрос."
ARCHIVED_TEXT = "Сессия этой карточки уже закрыта и архивирована, продолжить её нельзя."
EMPTY_TEXT = "Агент завершил ход без текстового ответа."


def plain(text):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", "", text or "")).split())


def chunks(text, limit=MESSAGE_LIMIT):
    parts = []
    rest = text.strip()
    while len(rest) > limit:
        cut = rest.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = limit
        parts.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip()
    if rest:
        parts.append(rest)
    return parts or [EMPTY_TEXT]


REFERENCE_LIMIT = 1024
DOCUMENT_LIMIT = 45 * 1024 * 1024


def question_message(question):
    """The follow-up as the agent reads it: the referenced Telegram text and the reader's words."""
    reference = ((question.get("reference") or {}).get("text") or "")[:REFERENCE_LIMIT]
    return (
        f"Пришёл фоллоуап через Telegram на сообщение с текстом:\n{reference}\n\n"
        f"{question.get('text', '')}\n\n"
        "Твоё последнее сообщение в этом ходе уйдёт пользователю ответом в Telegram; "
        "файл можно отправить тулом send_telegram_file."
    )


def question_id_for(chat_id, message_id):
    return "q_" + sha256({"chat_id": str(chat_id), "message_id": int(message_id)})[:32]


async def record_answer(repo, question_id, text, *, status):
    """Store the agent's answer (or failure) once, together with its Telegram delivery job.

    An already answered question re-queues its idempotent delivery job, so a lost enqueue is
    reconciled by any later call.
    """
    async with repo.lock("question:" + question_id), repo.transaction():
        question = await repo.get("out.user_questions", question_id)
        if question is None:
            return None
        if not question.get("answer_status"):
            question.update(
                status=status,
                answer_status=status,
                answer_text=text if status == "ANSWERED" and text else None,
                **collection_clock(utcnow()).fields("answered_at"),
            )
            await repo.put("out.user_questions", question_id, question)
        await repo.enqueue(
            "TELEGRAM_ANSWER",
            {"question_id": question_id},
            question_id,
            priority=0,
            max_attempts=5,
            expires_at=offset_schedule(collection_clock(utcnow()), timedelta(days=1)),
        )
    return question


class TelegramInbox:
    def __init__(self, repo, settings, *, http=None):
        self.repo, self.settings = repo, settings
        self.http = http or httpx.AsyncClient(timeout=httpx.Timeout(POLL_TIMEOUT_S + 15))
        self.owns_http = http is None
        self.bot_id = None
        self.members = {}

    @property
    def enabled(self):
        return bool(getattr(self.settings, "telegram_token", None)) and bool(
            getattr(self.settings, "telegram_alerts_chat", None)
        )

    def url(self, method):
        return f"https://api.telegram.org/bot{self.settings.telegram_token}/{method}"

    async def call(self, method, body):
        response = await self.http.post(self.url(method), json=body)
        data = response.json()
        if response.status_code != 200 or not data.get("ok"):
            retry = (data.get("parameters") or {}).get("retry_after", 1)
            error = RuntimeError(f"Telegram {method} rejected (HTTP {response.status_code})")
            error.retry_after = retry
            raise error
        return data["result"]

    async def run(self, stop):
        if not self.enabled:
            await stop.wait()
            return
        while not stop.is_set():
            try:
                waiting = [
                    row
                    async for row in self.repo.iter_all(
                        "ops.telegram_messages", filters={"status": "RECEIVED"}
                    )
                ]
                await self.poll(timeout=2 if waiting else POLL_TIMEOUT_S)
                await self.sweep()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Telegram inbox cycle failed")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=5)

    async def close(self):
        if self.owns_http:
            await self.http.aclose()

    async def bot(self):
        if self.bot_id is None:
            self.bot_id = (await self.call("getMe", {}))["id"]
        return self.bot_id

    async def poll(self, timeout=POLL_TIMEOUT_S):
        state = await self.repo.get("ops.source_checkpoints", CURSOR_KEY) or {}
        offset = int(state.get("cursor") or 0)
        updates = await self.call(
            "getUpdates", {"offset": offset, "timeout": timeout, "allowed_updates": ["message"]}
        )
        for update in updates:
            message = update.get("message")
            if message and message.get("chat") and message.get("message_id") is not None:
                await self.store(message, update["update_id"])
            offset = max(offset, int(update["update_id"]) + 1)
        if updates:
            await self.repo.put(
                "ops.source_checkpoints",
                CURSOR_KEY,
                {
                    "source_id": CURSOR_KEY,
                    "cursor": str(offset),
                    **collection_clock(utcnow()).fields("updated_at"),
                },
            )
        return len(updates)

    async def store(self, message, update_id):
        key = f"{message['chat']['id']}:{message['message_id']}"
        if await self.repo.get("ops.telegram_messages", key):
            return
        await self.repo.put(
            "ops.telegram_messages",
            key,
            {
                "chat_id": str(message["chat"]["id"]),
                "message_id": int(message["message_id"]),
                "user_id": str((message.get("from") or {}).get("id", "")),
                "update_id": int(update_id),
                "status": "RECEIVED",
                "message": message,
                **collection_clock(utcnow()).fields("received_at"),
            },
        )

    async def mark(self, row, status, **extra):
        key = f"{row['chat_id']}:{row['message_id']}"
        current = await self.repo.get("ops.telegram_messages", key)
        current.update(status=status, **extra, **collection_clock(utcnow()).fields("handled_at"))
        await self.repo.put("ops.telegram_messages", key, current)

    async def sweep(self, now=None):
        now = now if now is not None else time.time()
        rows = [
            row async for row in self.repo.iter_all("ops.telegram_messages", filters={"status": "RECEIVED"})
        ]
        rows.sort(key=lambda row: (row["message"].get("date", 0), row["message_id"]))
        for row in rows:
            current = await self.repo.get("ops.telegram_messages", f"{row['chat_id']}:{row['message_id']}")
            if current and current["status"] == "RECEIVED":
                await self.handle(current, now)

    async def authorized(self, message):
        chat = message["chat"]
        if str(chat["id"]) == str(self.settings.telegram_alerts_chat):
            return True
        if chat.get("type") != "private":
            return False
        user = (message.get("from") or {}).get("id")
        if user is None:
            return False
        cached = self.members.get(user)
        if cached and cached[1] > time.monotonic():
            return cached[0]
        try:
            member = await self.call(
                "getChatMember", {"chat_id": self.settings.telegram_alerts_chat, "user_id": user}
            )
            allowed = member.get("status") in MEMBER_STATUSES or (
                member.get("status") == "restricted" and member.get("is_member") is True
            )
        except RuntimeError:
            allowed = False
        self.members[user] = (allowed, time.monotonic() + MEMBER_CACHE_S)
        return allowed

    async def handle(self, row, now):
        message = row["message"]
        if not await self.authorized(message):
            await self.mark(row, "IGNORED", reason="UNAUTHORIZED")
            return
        bot = await self.bot()
        text = message.get("text") or message.get("caption") or ""
        reply = message.get("reply_to_message") or {}
        age = now - message.get("date", now)
        if message.get("forward_origin"):
            companion = await self.companion(row, forwarded=True)
            if companion is None and age < PAIR_WINDOW_S:
                return
            target = await self.forwarded_target(message, bot)
            if target is None:
                await self.notice(row, NO_SESSION_TEXT)
                await self.mark(row, "UNMATCHED")
                if companion:
                    await self.mark(companion, "PAIRED", paired_with=row["message_id"])
                return
            if companion is None:
                await self.prompt(row, target)
                return
            await self.question(companion, target, forwarded=row)
            await self.mark(row, "PAIRED", paired_with=companion["message_id"])
            return
        if reply and (reply.get("from") or {}).get("id") == bot and text:
            target = await self.reply_target(message["chat"]["id"], reply)
            if target is None:
                await self.notice(row, NO_SESSION_TEXT)
                await self.mark(row, "UNMATCHED")
                return
            await self.question(row, target)
            return
        if message["chat"].get("type") != "private" or not text:
            await self.mark(row, "IGNORED", reason="NOT_A_QUESTION")
            return
        if await self.companion(row, forwarded=False) is not None:
            return
        if age < PAIR_WINDOW_S:
            return
        await self.notice(row, HINT_TEXT)
        await self.mark(row, "IGNORED", reason="NO_REFERENCE")

    async def companion(self, row, *, forwarded):
        """The comment sent with a forward (or the forward sent with a comment) by the same user."""
        date = row["message"].get("date", 0)
        best = None
        async for other in self.repo.iter_all(
            "ops.telegram_messages", filters={"chat_id": row["chat_id"], "user_id": row["user_id"]}
        ):
            body = other["message"]
            if other["message_id"] == row["message_id"] or other["status"] != "RECEIVED":
                continue
            if bool(body.get("forward_origin")) == forwarded or body.get("reply_to_message"):
                continue
            if forwarded and not (body.get("text") or body.get("caption")):
                continue
            distance = abs(body.get("date", 0) - date)
            if distance <= PAIR_WINDOW_S and (best is None or distance < best[0]):
                best = (distance, other)
        return best[1] if best else None

    async def sent(self, channel, **filters):
        return [
            row
            async for row in self.repo.iter_all(
                "out.deliveries", filters={"channel": channel, "status": "SENT", **filters}
            )
            if row.get("message_ref")
        ]

    async def sent_between(self, channel, start, end):
        return [
            row
            async for row in self.repo.iter_range(
                "out.deliveries", "at", start=start, end=end, filters={"channel": channel, "status": "SENT"}
            )
            if row.get("message_ref")
        ]

    async def card_target(self, delivery):
        alert = await self.repo.get("out.alerts", f"{delivery['alert_id']}:{delivery['version']}")
        if alert is None:
            return None
        session_id = alert.get("session_id")
        if session_id is None:
            async for row in self.repo.iter_all(
                "ops.agent_sessions", filters={"event_id": alert["event_id"]}
            ):
                if not row.get("replay_run_id"):
                    session_id = row["session_id"]
                    break
        if session_id is None:
            return None
        return {
            "kind": "CARD",
            "session_id": session_id,
            "event_id": alert["event_id"],
            "alert_id": alert["alert_id"],
            "version": alert["version"],
            "text": plain(telegram_card_html(alert["card_text"])) if alert.get("card_text") else None,
        }

    async def answer_target(self, delivery):
        question = await self.repo.get("out.user_questions", delivery["question_id"])
        if question is None:
            return None
        return {
            "kind": "ANSWER" if delivery.get("purpose") != "PROMPT" else "PROMPT",
            "session_id": question["session_id"],
            "event_id": question.get("event_id"),
            "question_id": question["question_id"],
            "text": delivery.get("text"),
        }

    async def reply_target(self, chat_id, reply):
        ref = str(reply["message_id"])
        if str(chat_id) == str(self.settings.telegram_alerts_chat):
            for delivery in await self.sent(CARD_CHANNEL, message_ref=ref):
                return await self.card_target(delivery)
        for delivery in await self.sent(ANSWER_CHANNEL, message_ref=ref, chat_id=str(chat_id)):
            return await self.answer_target(delivery)
        return None

    async def forwarded_target(self, message, bot):
        origin = message["forward_origin"]
        if origin.get("type") == "user" and (origin.get("sender_user") or {}).get("id") != bot:
            return None
        if origin.get("type") in {"chat", "channel"}:
            chat = (origin.get("sender_chat") or origin.get("chat") or {}).get("id")
            if str(chat) == str(self.settings.telegram_alerts_chat) and origin.get("message_id"):
                return await self.reply_target(chat, {"message_id": origin["message_id"]})
            return None
        date = origin.get("date")
        if date is None:
            return None
        text = plain(message.get("text") or message.get("caption") or "")
        start = datetime.fromtimestamp(date - FORWARD_MATCH_S, UTC)
        end = datetime.fromtimestamp(date + FORWARD_MATCH_S, UTC)
        candidates = []
        for channel in (CARD_CHANNEL, ANSWER_CHANNEL):
            for delivery in await self.sent_between(channel, start, end):
                stamp = delivery.get("telegram_date")
                if stamp is not None and int(stamp) != int(date):
                    continue
                if stamp is None:
                    stamp = parse_time(delivery["at"]).timestamp()
                target = (
                    await self.card_target(delivery)
                    if channel == CARD_CHANNEL
                    else await self.answer_target(delivery)
                )
                if target:
                    score = SequenceMatcher(None, text[:600], plain(target.get("text") or "")[:600]).ratio()
                    candidates.append((score, -abs(stamp - date), target))
        if not candidates:
            return None
        candidates.sort(key=lambda row: (row[0], row[1]), reverse=True)
        return candidates[0][2] if candidates[0][0] >= 0.5 or len(candidates) == 1 else None

    async def send(self, chat_id, text, reply_to=None):
        body = {"chat_id": chat_id, "text": text, "link_preview_options": {"is_disabled": True}}
        if reply_to is not None:
            body["reply_parameters"] = {"message_id": int(reply_to), "allow_sending_without_reply": True}
        return await self.call("sendMessage", body)

    async def send_document(self, chat_id, filename, content, *, caption=None, reply_to=None):
        data = {"chat_id": str(chat_id)}
        if caption:
            data["caption"] = caption[:1024]
        if reply_to is not None:
            data["reply_parameters"] = json.dumps(
                {"message_id": int(reply_to), "allow_sending_without_reply": True}
            )
        response = await self.http.post(
            self.url("sendDocument"), data=data, files={"document": (filename, content)}
        )
        body = response.json()
        if response.status_code != 200 or not body.get("ok"):
            error = RuntimeError(f"Telegram sendDocument rejected (HTTP {response.status_code})")
            error.retry_after = (body.get("parameters") or {}).get("retry_after", 1)
            raise error
        return body["result"]

    async def send_session_file(self, question_id, call_id, filename, content, caption=None):
        """Deliver an agent file as a reply in the follow-up thread, once per tool call."""
        question = await self.repo.get("out.user_questions", question_id)
        if question is None:
            raise ValueError("Follow-up question does not exist")
        async for row in self.repo.iter_all(
            "out.deliveries",
            filters={
                "question_id": question_id,
                "channel": ANSWER_CHANNEL,
                "call_id": call_id,
                "status": "SENT",
            },
        ):
            return {"sent": True, "message_id": row["message_ref"], "duplicate": True}
        result = await self.send_document(
            question["chat_id"],
            filename,
            content,
            caption=caption,
            reply_to=question.get("reply_to_message_id"),
        )
        await self.repo.put(
            "out.deliveries",
            new_id("delivery"),
            {
                "channel": ANSWER_CHANNEL,
                "question_id": question_id,
                "session_id": question["session_id"],
                "purpose": "ATTACHMENT",
                "call_id": call_id,
                "chat_id": question["chat_id"],
                "status": "SENT",
                "attempt": 1,
                "message_ref": str(result["message_id"]),
                "telegram_date": result.get("date"),
                "text": caption or filename,
                "filename": filename,
                "bytes": len(content),
                **collection_clock(utcnow()).fields("at"),
            },
            immutable=True,
        )
        return {
            "sent": True,
            "message_id": str(result["message_id"]),
            "filename": filename,
            "bytes": len(content),
        }

    async def notice(self, row, text):
        try:
            await self.send(row["chat_id"], text, row["message_id"])
        except (RuntimeError, httpx.HTTPError):
            log.warning("Telegram notice to %s failed", row["chat_id"])

    async def prompt(self, row, target):
        """Ask for the question; a reply to this prompt continues the referenced session."""
        question_id = question_id_for(row["chat_id"], row["message_id"])
        await self.repo.put(
            "out.user_questions",
            question_id,
            {
                "question_id": question_id,
                "session_id": target["session_id"],
                "event_id": target.get("event_id"),
                "status": "PROMPTED",
                "chat_id": row["chat_id"],
                "user_id": row["user_id"],
                "reference": target,
                **collection_clock(utcnow()).fields("asked_at"),
            },
        )
        result = await self.send(row["chat_id"], PROMPT_TEXT, row["message_id"])
        await self.repo.put(
            "out.deliveries",
            new_id("delivery"),
            {
                "channel": ANSWER_CHANNEL,
                "question_id": question_id,
                "session_id": target["session_id"],
                "purpose": "PROMPT",
                "chat_id": row["chat_id"],
                "status": "SENT",
                "attempt": 1,
                "message_ref": str(result["message_id"]),
                "telegram_date": result.get("date"),
                "text": target.get("text"),
                **collection_clock(utcnow()).fields("at"),
            },
            immutable=True,
        )
        await self.mark(row, "PROMPTED", question_id=question_id)

    async def question(self, row, target, *, forwarded=None):
        message = row["message"]
        question_id = question_id_for(row["chat_id"], row["message_id"])
        if not await self.repo.get("out.user_questions", question_id):
            await self.repo.put(
                "out.user_questions",
                question_id,
                {
                    "question_id": question_id,
                    "session_id": target["session_id"],
                    "event_id": target.get("event_id"),
                    "status": "QUEUED",
                    "text": message.get("text") or message.get("caption") or "",
                    "chat_id": row["chat_id"],
                    "user_id": row["user_id"],
                    "reply_to_message_id": row["message_id"],
                    "forwarded_message_id": forwarded["message_id"] if forwarded else None,
                    "reference": target,
                    "telegram_date": message.get("date"),
                    **collection_clock(utcnow()).fields("asked_at"),
                },
            )
        await self.repo.enqueue(
            "AGENT_MESSAGE",
            {"kind": "USER_QUESTION", "session_id": target["session_id"], "question_id": question_id},
            question_id,
            priority=0,
            expires_at=offset_schedule(collection_clock(utcnow()), timedelta(days=1)),
        )
        await self.mark(row, "QUESTION", question_id=question_id)
        with contextlib.suppress(RuntimeError, httpx.HTTPError):
            await self.call("sendChatAction", {"chat_id": row["chat_id"], "action": "typing"})

    async def answer(self, payload):
        """TELEGRAM_ANSWER job: send the recorded answer as replies, chunk by chunk, exactly once."""
        question = await self.repo.get("out.user_questions", payload["question_id"])
        if question is None:
            raise ValueError("Answer subject does not exist")
        if question.get("answer_status") == "ANSWERED":
            text = question.get("answer_text") or EMPTY_TEXT
        elif question.get("answer_status") == "NO_SESSION":
            text = NO_SESSION_TEXT
        elif question.get("answer_status") == "ARCHIVED":
            text = ARCHIVED_TEXT
        else:
            text = FAILED_TEXT
        parts = chunks(text)
        ledger = [
            row
            async for row in self.repo.iter_all(
                "out.deliveries",
                filters={"question_id": question["question_id"], "channel": ANSWER_CHANNEL, "status": "SENT"},
            )
            if row.get("purpose") == "ANSWER"
        ]
        done = {row["chunk"]: row for row in ledger}
        previous = question.get("reply_to_message_id")
        for index, part in enumerate(parts):
            if index in done:
                previous = done[index]["message_ref"]
                continue
            result = await self.send(question["chat_id"], part, previous)
            previous = str(result["message_id"])
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                {
                    "channel": ANSWER_CHANNEL,
                    "question_id": question["question_id"],
                    "session_id": question["session_id"],
                    "purpose": "ANSWER",
                    "chat_id": question["chat_id"],
                    "chunk": index,
                    "chunks": len(parts),
                    "status": "SENT",
                    "attempt": 1,
                    "message_ref": previous,
                    "telegram_date": result.get("date"),
                    "text": part,
                    **collection_clock(utcnow()).fields("at"),
                },
                immutable=True,
            )
        async with self.repo.lock("question:" + question["question_id"]):
            question = await self.repo.get("out.user_questions", question["question_id"])
            question.update(status="DELIVERED", **collection_clock(utcnow()).fields("delivered_at"))
            await self.repo.put("out.user_questions", question["question_id"], question)

    async def answer_failed(self, payload, reason):
        """A TELEGRAM_ANSWER job that ran out of attempts leaves a terminal, monitorable record."""
        question_id = payload["question_id"]
        async with self.repo.lock("question:" + question_id):
            question = await self.repo.get("out.user_questions", question_id)
            if question is None or question.get("status") in ("DELIVERED", "DELIVERY_FAILED"):
                return
            question.update(
                status="DELIVERY_FAILED",
                delivery_failure=str(reason),
                **collection_clock(utcnow()).fields("delivery_failed_at"),
            )
            await self.repo.put("out.user_questions", question_id, question)
        await self.repo.put(
            "out.deliveries",
            new_id("delivery"),
            {
                "channel": ANSWER_CHANNEL,
                "question_id": question_id,
                "session_id": question["session_id"],
                "purpose": "ANSWER",
                "chat_id": question.get("chat_id"),
                "status": "FAILED",
                "terminal": True,
                "attempt": 5,
                "error": str(reason),
                **collection_clock(utcnow()).fields("at"),
            },
            immutable=True,
        )
