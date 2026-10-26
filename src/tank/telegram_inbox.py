"""Telegram follow-up questions continue the agent session that produced a card or answer.

A reader replies to a Tank message, or forwards it to the bot with a comment. The referenced
message identifies the session through the delivery ledger; the question goes into that same
native session and the agent's final message of that turn is sent back as the answer.
"""

import asyncio
import base64
import contextlib
import hashlib
import html
import json
import logging
import re
import time
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
from typing import Literal

import httpx
from pydantic import JsonValue

from tank import ops_cards
from tank.analysis.cards import telegram_card_html
from tank.contracts.native_readback import NativeSessionMissing
from tank.contracts.session_followups import (
    FollowupCandidate,
    FollowupClosure,
    FollowupRefusalTarget,
    FollowupSession,
    NoSession,
    RetentionArchived,
    Terminated,
    followup_eligibility,
)
from tank.contracts.telegram_state import (
    KnownTelegramFileDelivery,
    TelegramAnsweredQuestion,
    TelegramAttachmentAttempt,
    TelegramFailedAttachment,
    TelegramFileAlreadySent,
    TelegramFileDeliveryContext,
    TelegramFileDeliveryOutcome,
    TelegramFileSent,
    TelegramFileUncertain,
    TelegramGeneratedAttachment,
    TelegramInboxSettings,
    TelegramInboxStore,
    TelegramQuestionRecipient,
    TelegramSentAttachment,
    TelegramSentChunk,
    answer_chunk,
    attachment_delivery,
    settled_question,
)
from tank.contracts.telegram_wire import (
    TelegramAttachmentReference,
    TelegramCachedFile,
    TelegramDownloadedFile,
    TelegramFilePayload,
    TelegramMediaSelection,
    TelegramMembershipCache,
    TelegramMessageReceipt,
    TelegramMimeSelection,
    TelegramSentMessageReceipt,
    TelegramUpdateCursor,
    TelegramUpdateReceipt,
)
from tank.ops_progress import STAGES
from tank.question_answers import (
    QuestionAnswerRepository as QuestionAnswerRepository,
)
from tank.question_answers import (
    record_answer as record_answer,
)
from tank.question_answers import (
    validate_question_identity,
)
from tank.runtime_contracts import TelegramAnswerJob
from tank.telegram_api import FileTooLarge as FileTooLarge
from tank.telegram_api import (
    TelegramAuthorizationUnavailable,
    TelegramConfigurationError,
    TelegramResponseError,
    authorization_subject,
    bot_identity,
    download_file,
    media_selection,
    membership_observation,
    message_receipt,
    pairing_sender_id,
    require_membership_selector,
    require_telegram_token,
    response_result,
    sent_message_receipt,
    update_receipts,
)
from tank.telegram_rich import message_text, rich_message_text
from tank.time_precision import collection_clock, offset_schedule
from tank.util import new_id, parse_time, sha256, utcnow

log = logging.getLogger(__name__)

CURSOR_KEY = "telegram:bot-updates"
LEGACY_CHAT_KEY = "telegram:legacy-alerts-chat"
FILE_RETENTION = timedelta(days=30)
MAX_MESSAGE_ATTEMPTS = 5
RETRY_BASE_S = 15
PRUNE_INTERVAL_S = 3600
POLL_TIMEOUT_S = 20
PAIR_WINDOW_S = 10
FORWARD_MATCH_S = 30
MEMBER_CACHE_S = 600
MESSAGE_LIMIT = 4000
ANSWER_CHANNEL: Literal["telegram:answer"] = "telegram:answer"
CARD_CHANNEL = "telegram:alerts"

NO_SESSION_TEXT = "Не нашёл сессию агента для этого сообщения. Ответьте на карточку Tank или перешлите её боту вместе с вопросом."
HINT_TEXT = "Ответьте на карточку Tank или перешлите её боту вместе с вопросом."
PROMPT_TEXT = "Какой вопрос по этой карточке? Ответьте на это сообщение."
FAILED_TEXT = (
    "Агент не ответил: ход сессии завершился ошибкой. Сессия сохранена, вопрос можно задать ещё раз."
)
GONE_TEXT = "Продолжить нельзя: сессии агента для этого сообщения больше нет."
TERMINATED_TEXT = "Продолжить нельзя: сессия агента по этому сообщению прервана в OpenMA."
ARCHIVED_TEXT = "Продолжить нельзя: сессия агента по этому сообщению закрыта и архивирована в OpenMA."
KEPT_ARCHIVED_TEXT = "Продолжить нельзя: сессия агента по этому сообщению закрыта после {days} без сообщений."
EARLY_ARCHIVED_TEXT = (
    "Продолжить нельзя: сессия агента по этому сообщению закрыта сразу после разбора, "
    "по прежнему правилу закрытия сессий."
)
EMPTY_TEXT = "Агент завершил ход без текстового ответа."
OPS_CHANNEL = "telegram:ops"
OPS_KIND = "OPS_CARD"
OPS_UNMATCHED_TEXT = (
    "Не нашёл материал для этого сообщения. Ответьте на сообщение Tank Ops о конкретном материале."
)
OPS_JEV_TEXT = "Продолжить нельзя: агент не получал этот материал, его отсеял фильтр. Сессии по нему нет."
OPS_WAITING_TEXT = "Продолжить пока нельзя: материал ещё не передан агенту, сессии по нему нет."
OPS_NOT_RECEIVED_TEXT = "Продолжить нельзя: агент не получал этот материал ({stage}). Сессии по нему нет."
OPS_LOST_TEXT = "Продолжить нельзя: не нашёл сессию агента, которая разбирала этот материал."
WAITING_STAGES = {"RECEIVED", "JEV_PASSED", "JEV_BYPASSED", "AGENT_QUEUED"}
STOPPING_STAGES = {"PROCESSING_FAILED", "PROCESSING_EXPIRED"}


def days_text(days: int) -> str:
    """A validated day count with its Russian plural form."""
    return f"{days} {'дня' if days % 10 == 1 and days % 100 != 11 else 'дней'}"


def format_followup_closure(closure: FollowupClosure) -> str:
    closure = type(closure).model_validate(closure)
    if isinstance(closure, NoSession):
        return GONE_TEXT
    if isinstance(closure, Terminated):
        return TERMINATED_TEXT
    if isinstance(closure, RetentionArchived):
        return KEPT_ARCHIVED_TEXT.format(days=days_text(closure.keep_days))
    if closure.reason == "EARLY_CLOSURE":
        return EARLY_ARCHIVED_TEXT
    return ARCHIVED_TEXT


def remote_session_missing(session: Mapping[str, JsonValue]) -> bool:
    return isinstance(FollowupSession.from_record(session).remote, NativeSessionMissing)


def closed_reason(
    session: Mapping[str, JsonValue] | None,
) -> tuple[Literal["NO_SESSION", "TERMINATED", "ARCHIVED"], str] | None:
    """Format the inward lifecycle decision without introducing eligibility policy."""
    eligibility = followup_eligibility(None if session is None else FollowupSession.from_record(session))
    if isinstance(eligibility, FollowupCandidate):
        return None
    return eligibility.status, format_followup_closure(eligibility)


def not_received_reason(item, history):
    """Why no agent session took this material: (reason code, text). The text states only the reason."""
    stages = [row["stage"] for row in history]
    if (item.get("relevance") or {}).get("decision") == "SKIP" or "JEV_REJECTED" in stages:
        return "JEV_REJECTED", OPS_JEV_TEXT
    if "AGENT_ACCEPTED" in stages or any(stage not in WAITING_STAGES | STOPPING_STAGES for stage in stages):
        return "SESSION_NOT_FOUND", OPS_LOST_TEXT
    stopped = [stage for stage in stages if stage in STOPPING_STAGES]
    if stopped:
        return stopped[-1], OPS_NOT_RECEIVED_TEXT.format(stage=STAGES[stopped[-1]].lower())
    return "NOT_YET_RECEIVED", OPS_WAITING_TEXT


def plain(text):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", "", text or "")).split())


def utf16_length(text):
    """Telegram measures message length in UTF-16 code units, not Python code points."""
    return len(text.encode("utf-16-le")) // 2


def utf16_prefix(text, limit):
    """The longest prefix of text that fits in limit UTF-16 code units."""
    low, high = 0, len(text)
    while low < high:
        middle = (low + high + 1) // 2
        if utf16_length(text[:middle]) <= limit:
            low = middle
        else:
            high = middle - 1
    return text[:low]


def chunks(text, limit=MESSAGE_LIMIT):
    parts = []
    rest = text.strip()
    while utf16_length(rest) > limit:
        low = max(1, len(utf16_prefix(rest, limit)))
        cut = rest.rfind("\n", 0, low)
        if cut < low // 2:
            cut = low
        parts.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip()
    if rest:
        parts.append(rest)
    return parts or [EMPTY_TEXT]


REFERENCE_LIMIT = 1024
DOCUMENT_LIMIT = 45 * 1024 * 1024
UPLOAD_LIMIT = 20 * 1024 * 1024
TOO_LARGE_TEXT = "Файл больше 20 МБ, Telegram не даёт боту его скачать. Пришлите файл поменьше."
NOT_SENT = (TelegramResponseError, TelegramConfigurationError, httpx.ConnectError, httpx.ConnectTimeout)


def question_message(question):
    """The follow-up as the agent reads it: the referenced Telegram text, the reader's words and files."""
    target = question.get("reference") or {}
    reference = (target.get("text") or "")[:REFERENCE_LIMIT]
    lines = [f"Пришёл фоллоуап через Telegram на сообщение с текстом:\n{reference}"]
    if target.get("kind") == OPS_KIND:
        lines.append(
            f"Вопрос задан в чате Tank Ops о материале {target.get('item_id')}; ответ уйдёт туда же."
        )
    lines.append(question.get("text") or "(без текста)")
    for item in question.get("attachments") or []:
        lines.append(
            f"Пользователь приложил файл {item['filename']} ({item['size']} байт): {item.get('path', 'файл недоступен')}."
        )
    lines.append(
        "Твоё последнее сообщение в этом ходе уйдёт пользователю ответом в Telegram; "
        "файл можно отправить тулом send_telegram_file."
    )
    return "\n\n".join(lines)


def media(message: Mapping[str, JsonValue]) -> TelegramMediaSelection | None:
    """The admitted file selection; source metadata is not a downloaded byte receipt."""
    return media_selection(message)


def question_id_for(chat_id, message_id):
    return "q_" + sha256({"chat_id": str(chat_id), "message_id": int(message_id)})[:32]


class RepositoryFollowupRefusals:
    def __init__(self, repo: QuestionAnswerRepository) -> None:
        self.repo = repo

    async def record(self, target: FollowupRefusalTarget, closure: FollowupClosure) -> None:
        closure = type(closure).model_validate(closure)
        await record_answer(self.repo, target, format_followup_closure(closure), status=closure.status)


class TelegramInbox:
    def __init__(
        self,
        repo: TelegramInboxStore,
        settings: TelegramInboxSettings,
        *,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.repo, self.settings = repo, settings
        self.http = (
            http if http is not None else httpx.AsyncClient(timeout=httpx.Timeout(POLL_TIMEOUT_S + 15))
        )
        self.owns_http = http is None
        self.bot_id: int | None = None
        self.members: dict[tuple[str, int], TelegramMembershipCache] = {}
        self.next_prune = 0.0

    @property
    def enabled(self):
        return bool(getattr(self.settings, "telegram_token", None)) and bool(
            getattr(self.settings, "telegram_alerts_chat", None)
        )

    def active_token(self) -> str:
        return require_telegram_token(self.settings.telegram_token)

    def url(self, method: str) -> str:
        return f"https://api.telegram.org/bot{self.active_token()}/{method}"

    async def call(self, method: str, body: dict[str, JsonValue]) -> JsonValue:
        response = await self.http.post(self.url(method), json=body)
        return response_result(response.content, method=method, status=response.status_code)

    async def download(self, item: TelegramMediaSelection) -> TelegramDownloadedFile:
        token = require_telegram_token(self.settings.telegram_token)
        return await download_file(self.http, token=token, media=item, limit=UPLOAD_LIMIT)

    async def run(self, stop):
        if not self.enabled:
            await stop.wait()
            return
        await self.legacy_chat()
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
                if time.monotonic() >= self.next_prune:
                    await self.prune_files()
                    self.next_prune = time.monotonic() + PRUNE_INTERVAL_S
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Telegram inbox cycle failed")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=5)

    async def prune_files(self, now=None):
        """Reader attachments are kept 30 days, so follow-ups never grow the database without bound."""
        cutoff = (now or utcnow()) - FILE_RETENTION
        expired = [
            row["file_key"]
            async for row in self.repo.iter_metadata("ops.telegram_files", ("file_key", "stored_at"))
            if row.get("stored_at") and parse_time(row["stored_at"]) < cutoff
        ]
        for key in expired:
            await self.repo.delete("ops.telegram_file_payloads", key)
            await self.repo.delete("ops.telegram_files", key)
        old = [
            f"{row['chat_id']}:{row['message_id']}"
            async for row in self.repo.iter_metadata(
                "ops.telegram_messages", ("chat_id", "message_id", "status", "session_id", "received_at")
            )
            if row.get("status") != "RECEIVED"
            and not row.get("session_id")
            and row.get("received_at")
            and parse_time(row["received_at"]) < cutoff
        ]
        for key in old:
            await self.repo.delete("ops.telegram_messages", key)
        return len(expired)

    async def close(self):
        if self.owns_http:
            await self.http.aclose()

    async def legacy_chat(self):
        """The alerts chat at upgrade time; card deliveries recorded before chat_id belong to it."""
        state = await self.repo.get("ops.source_checkpoints", LEGACY_CHAT_KEY)
        if state is None:
            state = {
                "source_id": LEGACY_CHAT_KEY,
                "cursor": str(self.settings.telegram_alerts_chat),
                **collection_clock(utcnow()).fields("updated_at"),
            }
            await self.repo.put("ops.source_checkpoints", LEGACY_CHAT_KEY, state)
        return state["cursor"]

    async def bot(self) -> int:
        if self.bot_id is None:
            self.bot_id = bot_identity(await self.call("getMe", {})).id
        return self.bot_id

    async def poll(self, timeout: int = POLL_TIMEOUT_S) -> int:
        state = await self.repo.get("ops.source_checkpoints", CURSOR_KEY)
        if state is None:
            offset = 0
        else:
            cursor = TelegramUpdateCursor.model_validate(
                {"source_id": state["source_id"], "cursor": state["cursor"]}
            )
            offset = int(cursor.cursor)
        updates = await self.call(
            "getUpdates", {"offset": offset, "timeout": timeout, "allowed_updates": ["message"]}
        )
        receipts = update_receipts(updates)
        for update in receipts:
            if isinstance(update.payload, TelegramMessageReceipt) and update.payload.identity.message_id != 0:
                await self.store_receipt(update.payload, update.update_id)
            offset = max(offset, update.update_id + 1)
        if receipts:
            await self.repo.put(
                "ops.source_checkpoints",
                CURSOR_KEY,
                {
                    "source_id": CURSOR_KEY,
                    "cursor": str(offset),
                    **collection_clock(utcnow()).fields("updated_at"),
                },
            )
        return len(receipts)

    async def store(self, message: dict[str, JsonValue], update_id: int) -> None:
        receipt = message_receipt(message)
        update = TelegramUpdateReceipt.model_validate({"update_id": update_id, "payload": receipt})
        return await self.store_receipt(receipt, update.update_id)

    async def store_receipt(self, receipt: TelegramMessageReceipt, update_id: int) -> None:
        identity = receipt.identity
        if identity.message_id == 0:
            raise ValueError("Telegram message is not yet usable")
        key = f"{identity.chat.id}:{identity.message_id}"
        if await self.repo.get("ops.telegram_messages", key):
            return
        await self.repo.put(
            "ops.telegram_messages",
            key,
            {
                "chat_id": str(identity.chat.id),
                "message_id": identity.message_id,
                "user_id": str(identity.sender.id) if identity.sender is not None else None,
                "update_id": update_id,
                "date": identity.date,
                "pair_bucket": identity.date // PAIR_WINDOW_S,
                "status": "RECEIVED",
                "message": receipt.source_message,
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
            if row.get("retry_at", 0) > now:
                continue
            current = await self.repo.get("ops.telegram_messages", f"{row['chat_id']}:{row['message_id']}")
            if not current or current["status"] != "RECEIVED":
                continue
            try:
                try:
                    await self.handle(current, now)
                except FileTooLarge:
                    await self.notice(current, TOO_LARGE_TEXT, "IGNORED", reason="FILE_TOO_LARGE")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self.defer(current, error, now)

    async def defer(self, row, error, now):
        """One failing message waits with backoff and gives up after five tries; the sweep goes on."""
        log.warning(
            "Telegram message %s:%s failed: %s", row["chat_id"], row["message_id"], type(error).__name__
        )
        key = f"{row['chat_id']}:{row['message_id']}"
        current = await self.repo.get("ops.telegram_messages", key)
        if not current or current["status"] != "RECEIVED":
            return
        attempts = int(current.get("attempts", 0)) + 1
        current.update(attempts=attempts, last_error=type(error).__name__)
        if attempts >= MAX_MESSAGE_ATTEMPTS:
            current.update(status="FAILED", **collection_clock(utcnow()).fields("handled_at"))
        else:
            current["retry_at"] = now + min(300, RETRY_BASE_S * 2 ** (attempts - 1))
        await self.repo.put("ops.telegram_messages", key, current)

    def ops_chat(self, chat_id):
        """Whether a chat is the configured Tank Ops chat (and not also the Alerts chat)."""
        ops = getattr(self.settings, "telegram_ops_chat", None)
        return (
            bool(ops) and str(chat_id) == str(ops) and str(chat_id) != str(self.settings.telegram_alerts_chat)
        )

    async def authorized(self, message: Mapping[str, JsonValue]) -> bool:
        subject = authorization_subject(message)
        if str(subject.chat.id) == str(self.settings.telegram_alerts_chat) or self.ops_chat(subject.chat.id):
            return True
        if subject.chat.type != "private":
            return False
        if subject.sender is None:
            raise TelegramAuthorizationUnavailable("Private Telegram sender identity was not reported")
        selector = require_membership_selector(self.settings.telegram_alerts_chat)
        user = subject.sender.id
        key = (selector, user)
        cached = self.members.get(key)
        if cached is not None and cached.expires_monotonic > time.monotonic():
            if cached.observation.user.id != user:
                raise ValueError("Telegram membership cache belongs to another requested user")
            return cached.observation.permitted
        observation = membership_observation(
            await self.call("getChatMember", {"chat_id": selector, "user_id": user}), user
        )
        self.members[key] = TelegramMembershipCache(
            observation=observation, expires_monotonic=time.monotonic() + MEMBER_CACHE_S
        )
        return observation.permitted

    async def handle(self, row, now):
        message = row["message"]
        if not await self.authorized(message):
            await self.mark(row, "IGNORED", reason="UNAUTHORIZED")
            return
        bot = await self.bot()
        text = message_text(message)
        attachment = media(message)
        if (
            attachment is not None
            and attachment.declared_size is not None
            and attachment.declared_size > UPLOAD_LIMIT
        ):
            if message.get("forward_origin"):
                partner = await self.pair(row)
            else:
                partner = await self.companion(row, forwarded=False)
                if partner is not None:
                    back = await self.pair(partner)
                    partner = (
                        partner if back is not None and back["message_id"] == row["message_id"] else None
                    )
            also = (
                [(partner, {"reason": "FILE_TOO_LARGE", "paired_with": row["message_id"]})] if partner else []
            )
            await self.notice(row, TOO_LARGE_TEXT, "IGNORED", reason="FILE_TOO_LARGE", also=also)
            return
        reply = message.get("reply_to_message") or {}
        age = now - message.get("date", now)
        if message.get("forward_origin"):
            async for linked in self.repo.iter_all(
                "out.user_questions",
                filters={"chat_id": row["chat_id"], "forwarded_message_id": row["message_id"]},
            ):
                await self.mark(
                    row,
                    "PAIRED",
                    paired_with=linked["reply_to_message_id"],
                    session_id=linked["session_id"],
                    event_id=linked.get("event_id"),
                )
                return
            companion = await self.pair(row)
            if companion is None and age < PAIR_WINDOW_S:
                return
            target = await self.forwarded_target(message, bot)
            if target is None:
                also = [(companion, {"paired_with": row["message_id"]})] if companion else []
                await self.notice(row, NO_SESSION_TEXT, "UNMATCHED", also=also)
                return
            if companion is None:
                await self.prompt(row, target)
                return
            try:
                await self.question(companion, target, forwarded=row)
            except FileTooLarge:
                await self.notice(
                    companion,
                    TOO_LARGE_TEXT,
                    "IGNORED",
                    reason="FILE_TOO_LARGE",
                    also=[(row, {"reason": "FILE_TOO_LARGE", "paired_with": companion["message_id"]})],
                )
                return
            await self.mark(
                row,
                "PAIRED",
                paired_with=companion["message_id"],
                session_id=target["session_id"],
                event_id=target.get("event_id"),
            )
            return
        if reply and (text or attachment):
            target = await self.reply_target(message["chat"]["id"], reply)
            if target is not None and target["kind"] == "OPS_ITEM":
                await self.ops_question(row, target)
                return
            if target is not None:
                await self.question(row, target)
                return
            if (reply.get("from") or {}).get("id") == bot:
                await self.notice(
                    row,
                    OPS_UNMATCHED_TEXT if self.ops_chat(message["chat"]["id"]) else NO_SESSION_TEXT,
                    "UNMATCHED",
                )
                return
        if message["chat"].get("type") != "private" or not (text or attachment):
            await self.mark(row, "IGNORED", reason="NOT_A_QUESTION")
            return
        if await self.companion(row, forwarded=False) is not None:
            return
        if age < PAIR_WINDOW_S:
            return
        await self.notice(row, HINT_TEXT, "IGNORED", reason="NO_REFERENCE")

    async def companion(self, row, *, forwarded):
        """The comment sent with a forward (or the forward sent with a comment) by the same user."""
        user_id = pairing_sender_id(row)
        if user_id is None:
            return None
        date = row["message"]["date"]
        candidates = []
        bucket = date // PAIR_WINDOW_S
        nearby = []
        for near in (bucket - 1, bucket, bucket + 1):
            nearby += [
                other
                async for other in self.repo.iter_all(
                    "ops.telegram_messages",
                    filters={"chat_id": row["chat_id"], "user_id": user_id, "pair_bucket": near},
                )
            ]
        for other in nearby:
            body = other["message"]
            if other["message_id"] == row["message_id"] or other["status"] != "RECEIVED":
                continue
            if pairing_sender_id(other) != user_id:
                continue
            if bool(body.get("forward_origin")) == forwarded or body.get("reply_to_message"):
                continue
            if forwarded and not (message_text(body) or media(body)):
                continue
            if abs(body.get("date", 0) - date) <= PAIR_WINDOW_S:
                candidates.append(other)
        return candidates[0] if len(candidates) == 1 else None

    async def pair(self, forward):
        """The comment of a forward, only when each is the other's single candidate in the window."""
        comment = await self.companion(forward, forwarded=True)
        if comment is None:
            return None
        back = await self.companion(comment, forwarded=False)
        return comment if back is not None and back["message_id"] == forward["message_id"] else None

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
            "text": " ".join(rich_message_text(alert["rich_message"]).split())
            if alert.get("rich_message")
            else plain(telegram_card_html(alert["card_text"]))
            if alert.get("card_text")
            else None,
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
            "text": delivery.get("reference_text") or delivery.get("text"),
            "sent_text": delivery.get("text"),
        }

    async def reply_target(self, chat_id, reply):
        ref = str(reply["message_id"])
        if str(chat_id) == str(self.settings.telegram_alerts_chat):
            legacy = await self.legacy_chat()
            for delivery in await self.sent(CARD_CHANNEL, message_ref=ref):
                scope = delivery.get("chat_id") or legacy
                if scope != str(chat_id):
                    continue
                return await self.card_target(delivery)
        if self.ops_chat(chat_id):
            target = await self.ops_message_target(str(chat_id), ref)
            if target is not None:
                return target
        for delivery in await self.sent(ANSWER_CHANNEL, message_ref=ref, chat_id=str(chat_id)):
            if delivery.get("purpose") == "NOTICE" and delivery.get("item_id"):
                return {"kind": "OPS_ITEM", "item_id": delivery["item_id"], "text": delivery.get("text")}
            return await self.answer_target(delivery)
        own = await self.repo.get("ops.telegram_messages", f"{chat_id}:{ref}")
        return self.message_target(own)

    @staticmethod
    def message_target(row):
        """A reader message that already went to a session (question, its forward or file) targets it too."""
        if not row or not row.get("session_id"):
            return None
        body = row["message"]
        return {
            "kind": "USER_MESSAGE",
            "session_id": row["session_id"],
            "event_id": row.get("event_id"),
            "question_id": row.get("question_id"),
            "text": message_text(body)
            or (selected.filename if (selected := media(body)) is not None else None),
        }

    async def ops_message_target(self, chat_id, ref):
        """The material a Tank Ops message is about: its outcome card, or the progress notice it carried."""
        async for card in self.repo.iter_all(
            "out.ops_cards", filters={"chat_id": chat_id, "message_ref": ref}
        ):
            return {
                "kind": "OPS_ITEM",
                "item_id": card["item_id"],
                "text": ops_cards.plain(card.get("rendered_html"), REFERENCE_LIMIT) or None,
            }
        for delivery in await self.sent(OPS_CHANNEL, message_ref=ref, chat_id=chat_id):
            notice = await self.repo.get("out.ops_notices", delivery.get("notice_id") or "")
            if notice and notice.get("kind") == "ITEM_PROGRESS" and notice.get("subject_id"):
                return {"kind": "OPS_ITEM", "item_id": notice["subject_id"], "text": notice.get("text")}
        return None

    async def item_session(self, item_id, events):
        """The agent session that took this material as NEW_ITEM or ITEM_ADDED, a live one first."""
        found = []
        for field, value in [("item_id", item_id)] + [("event_id", event) for event in sorted(events)]:
            async for session in self.repo.iter_all("ops.agent_sessions", filters={field: value}):
                if session.get("replay_run_id") or any(
                    row["session_id"] == session["session_id"] for row in found
                ):
                    continue
                if any(
                    (entry.get("payload") or {}).get("kind") in {"NEW_ITEM", "ITEM_ADDED"}
                    and (entry.get("payload") or {}).get("item_id") == item_id
                    for entry in session.get("message_log", [])
                ):
                    found.append(session)
        return next((session for session in found if closed_reason(session) is None), None) or next(
            iter(found), None
        )

    async def item_events(self, item_id):
        events = {
            row["event_id"]
            async for row in self.repo.iter_metadata(
                "evt.event_observations", ["event_id"], filters={"item_id": item_id}
            )
            if row.get("event_id")
        }
        events |= {
            row["event_id"]
            async for row in self.repo.iter_metadata(
                "out.ops_notices", ["event_id"], filters={"kind": "ITEM_PROGRESS", "subject_id": item_id}
            )
            if row.get("event_id")
        }
        item = await self.repo.get("ingest.source_items", item_id)
        if item and item.get("route_event_id"):
            events.add(item["route_event_id"])
        return item, events

    async def ops_question(self, row, target):
        """A reply in Tank Ops continues the agent session that handled the material, answered in Ops.

        Published or not, the material's own session takes the question. When no session took the
        material, the reader gets only the reason.
        """
        item_id = target["item_id"]
        item, events = await self.item_events(item_id)
        if item is None:
            await self.notice(row, OPS_UNMATCHED_TEXT, "UNMATCHED", item_id=item_id)
            return
        session = await self.item_session(item_id, events)
        if session is not None:
            await self.question(
                row,
                {
                    "kind": OPS_KIND,
                    "session_id": session["session_id"],
                    "event_id": session.get("event_id"),
                    "item_id": item_id,
                    "text": target.get("text"),
                },
            )
            return
        history = [
            notice
            async for notice in self.repo.iter_all(
                "out.ops_notices", filters={"kind": "ITEM_PROGRESS", "subject_id": item_id}
            )
        ]
        history.sort(key=lambda notice: (ops_cards.time_of(notice), notice["notice_id"]))
        reason, text = not_received_reason(item, history)
        await self.notice(
            row, text, "NO_SESSION", subject={"item_id": item_id}, reason=reason, item_id=item_id
        )

    async def forwarded_target(self, message, bot):
        origin = message["forward_origin"]
        if origin.get("type") in {"chat", "channel"}:
            chat = (origin.get("sender_chat") or origin.get("chat") or {}).get("id")
            if str(chat) == str(self.settings.telegram_alerts_chat) and origin.get("message_id"):
                return await self.reply_target(chat, {"message_id": origin["message_id"]})
            return None
        if origin.get("type") != "user":
            return None
        date = origin.get("date")
        sender = (origin.get("sender_user") or {}).get("id")
        if date is None:
            return None
        if sender is not None and sender == (message.get("from") or {}).get("id"):
            text = plain(message_text(message))
            matches = [
                row
                async for row in self.repo.iter_all(
                    "ops.telegram_messages", filters={"user_id": str(sender), "date": int(date)}
                )
                if row["message"].get("date") == date
                and row.get("session_id")
                and row["message_id"] != message["message_id"]
            ]
            exact = [row for row in matches if plain(message_text(row["message"])) == text] or matches
            if len({row["session_id"] for row in exact}) != 1:
                return None
            return self.message_target(exact[0])
        if sender != bot:
            return None
        text = plain(message_text(message))
        start = datetime.fromtimestamp(date - FORWARD_MATCH_S, UTC)
        end = datetime.fromtimestamp(date + FORWARD_MATCH_S, UTC)
        candidates = []
        chats = {str(message["chat"]["id"]), str(self.settings.telegram_alerts_chat)}
        for channel in (CARD_CHANNEL, ANSWER_CHANNEL):
            for delivery in await self.sent_between(channel, start, end):
                if channel == ANSWER_CHANNEL and str(delivery.get("chat_id")) not in chats:
                    continue
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
                    sent = target.get("sent_text") or target.get("text") or ""
                    score = SequenceMatcher(None, text[:600], plain(sent)[:600]).ratio()
                    candidates.append((score, -abs(stamp - date), target))
        if not candidates:
            return None
        candidates.sort(key=lambda row: (row[0], row[1]), reverse=True)
        return candidates[0][2] if candidates[0][0] >= 0.5 or len(candidates) == 1 else None

    async def send(
        self, chat_id: str | int, text: str, reply_to: str | int | None = None
    ) -> TelegramSentMessageReceipt:
        body: dict[str, JsonValue] = {
            "chat_id": chat_id,
            "text": text,
            "link_preview_options": {"is_disabled": True},
        }
        if reply_to is not None:
            body["reply_parameters"] = {"message_id": int(reply_to), "allow_sending_without_reply": True}
        return sent_message_receipt(await self.call("sendMessage", body))

    async def send_document(
        self,
        chat_id: str | int,
        filename: str,
        content: bytes,
        *,
        caption: str | None = None,
        reply_to: str | int | None = None,
    ) -> TelegramSentMessageReceipt:
        data = {"chat_id": str(chat_id)}
        if caption:
            data["caption"] = utf16_prefix(caption, 1024)
        if reply_to is not None:
            data["reply_parameters"] = json.dumps(
                {"message_id": int(reply_to), "allow_sending_without_reply": True}
            )
        response = await self.http.post(
            self.url("sendDocument"), data=data, files={"document": (filename, content)}
        )
        result = response_result(response.content, method="sendDocument", status=response.status_code)
        return sent_message_receipt(result)

    @contextlib.asynccontextmanager
    async def _locked_session_file(
        self,
        target: FollowupRefusalTarget,
        call_id: str,
        filename: str,
    ) -> AsyncIterator[TelegramFileDeliveryContext]:
        target = FollowupRefusalTarget.model_validate(target)
        async with self.repo.lock("question:" + target.question_id):
            question = await self.repo.get("out.user_questions", target.question_id)
            if question is None:
                raise ValueError("Follow-up question does not exist")
            recipient = TelegramQuestionRecipient.from_record(question, target)
            scope = TelegramAttachmentAttempt(
                question_id=recipient.question_id,
                session_id=recipient.session_id,
                channel=ANSWER_CHANNEL,
                purpose="ATTACHMENT",
                chat_id=recipient.chat_id,
                call_id=call_id,
                filename=filename,
            )
            ledger = [
                attachment_delivery(row, recipient, call_id=scope.call_id, filename=scope.filename)
                async for row in self.repo.iter_all(
                    "out.deliveries",
                    filters={
                        "question_id": target.question_id,
                        "channel": ANSWER_CHANNEL,
                        "call_id": scope.call_id,
                    },
                )
            ]
            known: KnownTelegramFileDelivery | None = None
            for row in ledger:
                if isinstance(row, TelegramSentAttachment):
                    known = TelegramFileAlreadySent(
                        sent=True, message_id=str(row.message_ref), duplicate=True
                    )
                    break
            if known is None and sum(isinstance(row, TelegramGeneratedAttachment) for row in ledger) > sum(
                isinstance(row, TelegramFailedAttachment) for row in ledger
            ):
                known = TelegramFileUncertain(
                    sent="UNKNOWN",
                    reason="An earlier attempt of this call was interrupted after sending started; it may already have arrived. Call again to resend.",
                )
            yield TelegramFileDeliveryContext(recipient=recipient, attempt=scope, known=known)

    async def lookup_session_file(
        self,
        target: FollowupRefusalTarget,
        call_id: str,
        filename: str,
    ) -> KnownTelegramFileDelivery | None:
        async with self._locked_session_file(target, call_id, filename) as context:
            if context.known is None:
                self.active_token()
            return context.known

    async def send_session_file(
        self,
        target: FollowupRefusalTarget,
        call_id: str,
        filename: str,
        content: bytes,
        caption: str | None = None,
    ) -> TelegramFileDeliveryOutcome:
        """Deliver to the locked authoritative question/session pair, once per tool call."""
        async with self._locked_session_file(target, call_id, filename) as context:
            if context.known is not None:
                return context.known
            recipient, scope = context.recipient, context.attempt
            self.active_token()
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                scope.model_dump(mode="json")
                | {"status": "GENERATED", "attempt": 1}
                | collection_clock(utcnow()).fields("at"),
                immutable=True,
            )
            try:
                result = await self.send_document(
                    recipient.chat_id,
                    scope.filename,
                    content,
                    caption=caption,
                    reply_to=recipient.reply_to_message_id,
                )
            except NOT_SENT:
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    scope.model_dump(mode="json")
                    | {"status": "FAILED", "attempt": 1}
                    | collection_clock(utcnow()).fields("at"),
                    immutable=True,
                )
                raise
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                scope.model_dump(mode="json")
                | {
                    "status": "SENT",
                    "attempt": 1,
                    "message_ref": str(result.message_id),
                    "telegram_date": result.date,
                    "text": scope.filename if caption is None else caption,
                    "bytes": len(content),
                }
                | collection_clock(utcnow()).fields("at"),
                immutable=True,
            )
            return TelegramFileSent(
                sent=True,
                message_id=str(result.message_id),
                filename=scope.filename,
                bytes=len(content),
            )

    async def notice(self, row, text, status, *, also=(), subject=None, **extra):
        """Reply with a notice at most once, then settle the input (and any paired rows) together.

        A rejected send leaves every row retryable.
        """
        key = question_id_for(row["chat_id"], row["message_id"])
        ledger = [
            delivery
            async for delivery in self.repo.iter_all(
                "out.deliveries", filters={"question_id": key, "channel": ANSWER_CHANNEL, "purpose": "NOTICE"}
            )
        ]
        settled = any(delivery["status"] == "SENT" for delivery in ledger) or sum(
            delivery["status"] == "GENERATED" for delivery in ledger
        ) > sum(delivery["status"] == "FAILED" for delivery in ledger)
        if not settled:
            self.active_token()
            base = {
                "channel": ANSWER_CHANNEL,
                "question_id": key,
                "purpose": "NOTICE",
                "chat_id": row["chat_id"],
                "attempt": 1,
                "text": text,
                **(subject or {}),
            }
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                {**base, "status": "GENERATED", **collection_clock(utcnow()).fields("at")},
                immutable=True,
            )
            try:
                result = await self.send(row["chat_id"], text, row["message_id"])
            except NOT_SENT:
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {**base, "status": "FAILED", **collection_clock(utcnow()).fields("at")},
                    immutable=True,
                )
                raise
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                {
                    **base,
                    "status": "SENT",
                    "message_ref": str(result.message_id),
                    "telegram_date": result.date,
                    **collection_clock(utcnow()).fields("at"),
                },
                immutable=True,
            )
        async with self.repo.transaction():
            await self.mark(row, status, **extra)
            for other, other_extra in also:
                await self.mark(other, status, **other_extra)

    async def prompt(self, row, target):
        """Ask for the question; a reply to this prompt continues the referenced session."""
        question_id = question_id_for(row["chat_id"], row["message_id"])
        binding = FollowupRefusalTarget(question_id=question_id, session_id=target["session_id"])
        async with self.repo.lock("question:" + question_id), self.repo.transaction():
            existing = await self.repo.get("out.user_questions", question_id)
            if existing is not None:
                state = validate_question_identity(existing, binding)
                if state.answer_status is not None:
                    return
            prior = [
                delivery
                async for delivery in self.repo.iter_all(
                    "out.deliveries",
                    filters={"question_id": question_id, "channel": ANSWER_CHANNEL, "purpose": "PROMPT"},
                )
            ]
            sent = any(delivery["status"] == "SENT" for delivery in prior)
            unresolved = sum(delivery["status"] == "GENERATED" for delivery in prior) > sum(
                delivery["status"] == "FAILED" for delivery in prior
            )
            if not sent and not unresolved:
                self.active_token()
            if existing is None:
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
        if not sent and not unresolved:
            ledger = {
                "channel": ANSWER_CHANNEL,
                "question_id": question_id,
                "session_id": target["session_id"],
                "purpose": "PROMPT",
                "chat_id": row["chat_id"],
                "attempt": 1,
                "text": PROMPT_TEXT,
                "reference_text": target.get("text"),
            }
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                {**ledger, "status": "GENERATED", **collection_clock(utcnow()).fields("at")},
                immutable=True,
            )
            try:
                result = await self.send(row["chat_id"], PROMPT_TEXT, row["message_id"])
            except NOT_SENT:
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {**ledger, "status": "FAILED", **collection_clock(utcnow()).fields("at")},
                    immutable=True,
                )
                raise
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                {
                    **ledger,
                    "status": "SENT",
                    "message_ref": str(result.message_id),
                    "telegram_date": result.date,
                    **collection_clock(utcnow()).fields("at"),
                },
                immutable=True,
            )
        await self.mark(
            row,
            "PROMPTED",
            question_id=question_id,
            session_id=target["session_id"],
            event_id=target.get("event_id"),
        )

    async def attachments(
        self, question_id: str, session_id: str, *messages: Mapping[str, JsonValue] | None
    ) -> list[TelegramAttachmentReference]:
        """Acquire attested file identities without relabelling historical borrowed-ID caches."""
        stored: list[TelegramAttachmentReference] = []
        for message in messages:
            item = media(message) if message is not None else None
            if item is None:
                continue
            key = (
                "tgf_"
                + sha256(
                    {
                        "question_id": question_id,
                        "file_unique_id": item.file_unique_id,
                        "identity_basis": "TELEGRAM_FILE_UNIQUE_ID",
                    }
                )[:32]
            )
            async with self.repo.lock("telegram-file:" + key):
                existing = await self.repo.get("ops.telegram_files", key)
                if existing is not None:
                    cached = TelegramCachedFile.model_validate(
                        {field: existing[field] for field in TelegramCachedFile.model_fields}
                    )
                    if (
                        cached.file_key != key
                        or cached.question_id != question_id
                        or cached.session_id != session_id
                        or cached.file_unique_id != item.file_unique_id
                    ):
                        raise ValueError("Telegram cached attachment ownership conflicts")
                    payload_record = await self.repo.get("ops.telegram_file_payloads", key)
                    if payload_record is None:
                        raise ValueError("Telegram cached attachment bytes are unavailable")
                    payload = TelegramFilePayload.model_validate(payload_record)
                    if (
                        payload.file_key != key
                        or cached.size > UPLOAD_LIMIT
                        or len(payload.content_base64) > 4 * ((UPLOAD_LIMIT + 2) // 3)
                    ):
                        raise ValueError("Telegram cached attachment payload identity or size conflicts")
                    try:
                        content = base64.b64decode(payload.content_base64, validate=True)
                    except ValueError:
                        raise ValueError("Telegram cached attachment bytes are invalid") from None
                    if len(content) != cached.size or hashlib.sha256(content).hexdigest() != cached.sha256:
                        raise ValueError("Telegram cached attachment bytes conflict with their receipt")
                    if item.declared_size is not None and item.declared_size != len(content):
                        raise ValueError(
                            "Telegram current declared size conflicts with verified cached bytes"
                        )
                else:
                    downloaded = await self.download(item)
                    content = downloaded.content
                    mime = TelegramMimeSelection.from_facts(
                        item.reported_mime_type, downloaded.response_content_type
                    )
                    cached = TelegramCachedFile(
                        file_key=key,
                        question_id=question_id,
                        session_id=session_id,
                        file_unique_id=item.file_unique_id,
                        file_identity_basis="TELEGRAM_FILE_UNIQUE_ID",
                        filename=item.filename,
                        mime=mime.value,
                        mime_basis=mime.basis,
                        reported_mime_type=item.reported_mime_type,
                        declared_size=item.declared_size,
                        response_content_type=downloaded.response_content_type,
                        size=len(content),
                        sha256=hashlib.sha256(content).hexdigest(),
                    )
                    payload = TelegramFilePayload(
                        file_key=key, content_base64=base64.b64encode(content).decode()
                    )
                    async with self.repo.transaction():
                        await self.repo.put(
                            "ops.telegram_file_payloads", key, payload.model_dump(mode="json")
                        )
                        await self.repo.put(
                            "ops.telegram_files",
                            key,
                            {
                                **cached.model_dump(mode="json"),
                                **collection_clock(utcnow()).fields("stored_at"),
                            },
                        )
                stored.append(
                    TelegramAttachmentReference(
                        file_key=cached.file_key, filename=cached.filename, mime=cached.mime, size=cached.size
                    )
                )
        return stored

    async def question(self, row, target, *, forwarded=None):
        message = row["message"]
        question_id = question_id_for(row["chat_id"], row["message_id"])
        binding = FollowupRefusalTarget(question_id=question_id, session_id=target["session_id"])
        async with self.repo.lock("question:" + question_id), self.repo.transaction():
            existing = await self.repo.get("out.user_questions", question_id)
            if existing is not None:
                validate_question_identity(existing, binding)
            files = await self.attachments(
                question_id, target["session_id"], message, forwarded["message"] if forwarded else None
            )
            if existing is None:
                await self.repo.put(
                    "out.user_questions",
                    question_id,
                    {
                        "question_id": question_id,
                        "session_id": target["session_id"],
                        "event_id": target.get("event_id"),
                        "status": "QUEUED",
                        "text": message_text(message),
                        "chat_id": row["chat_id"],
                        "user_id": row["user_id"],
                        "reply_to_message_id": row["message_id"],
                        "forwarded_message_id": forwarded["message_id"] if forwarded else None,
                        "reference": target,
                        "attachments": [file.model_dump(mode="json") for file in files],
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
        await self.mark(
            row,
            "QUESTION",
            question_id=question_id,
            session_id=target["session_id"],
            event_id=target.get("event_id"),
        )
        with contextlib.suppress(RuntimeError, httpx.HTTPError):
            await self.call("sendChatAction", {"chat_id": row["chat_id"], "action": "typing"})

    async def answer(self, payload: TelegramAnswerJob | dict[str, JsonValue]) -> None:
        """TELEGRAM_ANSWER job: send the recorded answer as replies, chunk by chunk, exactly once."""
        job = payload if isinstance(payload, TelegramAnswerJob) else TelegramAnswerJob.model_validate(payload)
        question = await self.repo.get("out.user_questions", job.question_id)
        if question is None:
            raise ValueError("Answer subject does not exist")
        binding = FollowupRefusalTarget.model_validate(
            {"question_id": job.question_id, "session_id": question["session_id"]}
        )
        subject = settled_question(question, binding)
        if isinstance(subject, TelegramAnsweredQuestion):
            text = EMPTY_TEXT if subject.answer_text is None else subject.answer_text
        else:
            text = FAILED_TEXT if subject.reason_text is None else subject.reason_text
        parts = chunks(text)
        ledger = [
            answer_chunk(row, subject)
            async for row in self.repo.iter_all(
                "out.deliveries", filters={"question_id": subject.question_id, "channel": ANSWER_CHANNEL}
            )
            if row.get("purpose") not in ("ATTACHMENT", "NOTICE", "PROMPT")
        ]
        previous = subject.reply_to_message_id
        unresolved = False
        for index, part in enumerate(parts):
            rows = [row for row in ledger if row.chunk == index]
            sent = next((row for row in rows if isinstance(row, TelegramSentChunk)), None)
            if sent:
                previous = sent.message_ref
                continue
            if sum(row.status == "GENERATED" for row in rows) > sum(row.status == "FAILED" for row in rows):
                unresolved = True
                continue
            self.active_token()
            base = {
                "channel": ANSWER_CHANNEL,
                "question_id": subject.question_id,
                "session_id": subject.session_id,
                "purpose": "ANSWER",
                "chat_id": subject.chat_id,
                "chunk": index,
                "chunks": len(parts),
                "attempt": 1,
                "text": part,
            }
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                {**base, "status": "GENERATED", **collection_clock(utcnow()).fields("at")},
                immutable=True,
            )
            try:
                result = await self.send(subject.chat_id, part, previous)
            except NOT_SENT:
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {**base, "status": "FAILED", **collection_clock(utcnow()).fields("at")},
                    immutable=True,
                )
                raise
            previous = str(result.message_id)
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                {
                    **base,
                    "status": "SENT",
                    "message_ref": previous,
                    "telegram_date": result.date,
                    **collection_clock(utcnow()).fields("at"),
                },
                immutable=True,
            )
        if unresolved:
            return
        async with self.repo.lock("question:" + binding.question_id):
            question = await self.repo.get("out.user_questions", binding.question_id)
            if question is None:
                raise ValueError("Answer subject disappeared before receipt recording")
            validate_question_identity(question, binding)
            question.update(status="DELIVERED", **collection_clock(utcnow()).fields("delivered_at"))
            await self.repo.put("out.user_questions", binding.question_id, question)

    async def answer_failed(self, payload: TelegramAnswerJob | dict[str, JsonValue], reason: str) -> None:
        """A TELEGRAM_ANSWER job that ran out of attempts leaves a terminal, monitorable record."""
        job = payload if isinstance(payload, TelegramAnswerJob) else TelegramAnswerJob.model_validate(payload)
        question_id = job.question_id
        async with self.repo.lock("question:" + question_id):
            question = await self.repo.get("out.user_questions", question_id)
            if question is None:
                return
            validate_question_identity(
                question,
                FollowupRefusalTarget.model_validate(
                    {"question_id": question_id, "session_id": question["session_id"]}
                ),
            )
            if question.get("status") in ("DELIVERED", "DELIVERY_FAILED"):
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
