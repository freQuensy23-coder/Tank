"""Immutable Telegram delivery ledger and delivered-call activation."""

import asyncio
import html
import json
import time
from contextlib import AsyncExitStack
from datetime import datetime, timedelta

import httpx
from pydantic import JsonValue

from tank.analysis.cards import telegram_card_html
from tank.diagnostic_documents import documents
from tank.ops_progress import event_progress
from tank.runtime_contracts import AlertDeliveryJob, DeliveryJob, NoticeDeliveryJob, parse_delivery_job
from tank.time_precision import Timestamp, collection_clock, offset_schedule, propagate_timestamp
from tank.util import new_id, parse_time, sha256, utcnow

DELIVERY_LOCK_WAIT_S = 6


class DeliveryError(RuntimeError):
    def __init__(self, message: str, retry_after: float = 1) -> None:
        super().__init__(message)
        self.retry_after = retry_after
        self.terminal = False


class DeliveryDeferred(DeliveryError):
    """An accepted card remains queued; no network attempt has occurred."""

    def __init__(self, not_before: Timestamp) -> None:
        if not_before.value is None:
            raise ValueError("Deferred delivery requires a scheduled timestamp")
        self.not_before = not_before
        super().__init__(
            "Accepted card is scheduled for later delivery",
            retry_after=max(1, (parse_time(not_before.value) - utcnow()).total_seconds()),
        )


def delivery_moment(timestamp: Timestamp) -> datetime:
    if timestamp.value is None:
        raise ValueError("Delivery scheduling requires a timestamp value")
    return parse_time(timestamp.value)


def delivery_payload(payload: DeliveryJob | dict[str, JsonValue]) -> DeliveryJob:
    if isinstance(payload, (AlertDeliveryJob, NoticeDeliveryJob)):
        return payload
    return parse_delivery_job(payload)


def delivery_subject(payload: DeliveryJob) -> str:
    return (
        payload.notice_id
        if isinstance(payload, NoticeDeliveryJob)
        else f"{payload.alert_id}:{payload.version}"
    )


def delivery_filters(payload: DeliveryJob) -> dict[str, JsonValue]:
    if isinstance(payload, NoticeDeliveryJob):
        return {"channel": payload.channel, "notice_id": payload.notice_id}
    return {"channel": payload.channel, "alert_id": payload.alert_id, "version": payload.version}


class DeliveryService:
    def __init__(self, repo, settings, *, http=None):
        self.repo, self.settings = repo, settings
        self.http = http if http is not None else httpx.AsyncClient(timeout=8)
        self.owns_http = http is None

    async def close(self) -> None:
        if self.owns_http:
            await self.http.aclose()

    async def _all(self, collection, *, filters=None):
        return [row async for row in self.repo.iter_all(collection, filters=filters)]

    async def terminal_failure(
        self,
        payload: DeliveryJob | dict[str, JsonValue],
        error: BaseException | str,
        attempt: int | None = None,
    ) -> None:
        """Persist terminal queue failures, including outcomes before any HTTP send."""
        payload = delivery_payload(payload)
        subject, channel = delivery_subject(payload), payload.channel
        filters = delivery_filters(payload)
        async with self.repo.lock("deliver:" + subject + ":" + channel), self.repo.transaction():
            ledger = await self._all("out.deliveries", filters=filters)
            if any(row["status"] == "SENT" for row in ledger):
                return
            source = None
            if isinstance(payload, AlertDeliveryJob):
                alerts = await self._all(
                    "out.alerts", filters={"alert_id": payload.alert_id, "version": payload.version}
                )
                source = alerts[0] if alerts else None
            if not any(row.get("terminal") for row in ledger):
                source_times = {
                    key: value
                    for field in ("generated_at", "delivery_not_before")
                    if source is not None and field in source
                    for key, value in propagate_timestamp(source, field).fields(field).items()
                }
                failed_at = collection_clock(utcnow())
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {
                        **filters,
                        **source_times,
                        "status": "FAILED",
                        "terminal": True,
                        "attempt": attempt
                        if attempt is not None
                        else max((r.get("attempt", 0) for r in ledger), default=0),
                        **failed_at.fields("at"),
                        **failed_at.fields("failed_at"),
                        "error": type(error).__name__ if isinstance(error, BaseException) else str(error),
                    },
                    immutable=True,
                )
            if isinstance(payload, NoticeDeliveryJob):
                return
            await event_progress(
                self.repo,
                self.settings,
                source.get("event_id") if source is not None else None,
                "ALERT_FAILED",
                f"Не доставлена карточка {subject}",
                key=subject,
            )
            notice_id = "delivery_failed_" + subject.replace(":", "_")
            if not await self.repo.get("out.ops_notices", notice_id):
                await self.repo.put(
                    "out.ops_notices",
                    notice_id,
                    {
                        "notice_id": notice_id,
                        "kind": "DELIVERY_FAILED",
                        "subject_id": payload.alert_id,
                        "text": f"Delivery failed: {subject}",
                        **collection_clock(utcnow()).fields("created_at"),
                        "created_by": "delivery",
                    },
                    immutable=True,
                )
            await self.repo.enqueue(
                "DELIVER",
                {"notice_id": notice_id, "channel": "telegram:ops"},
                notice_id,
                priority=0,
                max_attempts=5,
                expires_at=offset_schedule(collection_clock(utcnow()), timedelta(days=1)),
            )

    async def _enter_delivery_lock(self, stack: AsyncExitStack, key: str) -> None:
        try:
            async with asyncio.timeout(DELIVERY_LOCK_WAIT_S):
                await stack.enter_async_context(self.repo.lock(key))
        except TimeoutError:
            raise DeliveryDeferred(
                offset_schedule(collection_clock(utcnow()), timedelta(seconds=1))
            ) from None

    async def deliver(self, payload: DeliveryJob | dict[str, JsonValue]) -> None:
        payload = delivery_payload(payload)
        if isinstance(payload, NoticeDeliveryJob):
            notice = await self.repo.get("out.ops_notices", payload.notice_id)
            if notice and notice.get("kind") == "ITEM_PROGRESS":
                error = await self._deliver_progress(notice)
            elif notice and notice.get("document_manifest"):
                async with AsyncExitStack() as locks:
                    await self._enter_delivery_lock(locks, "ops-documents:" + notice["notice_id"])
                    error = await self._deliver(payload)
                    if error is None:
                        error = await self._deliver_documents(notice)
            else:
                error = await self._deliver(payload)
        else:
            rows = await self._all(
                "out.alerts", filters={"alert_id": payload.alert_id, "version": payload.version}
            )
            if not rows:
                raise ValueError("Delivery subject does not exist")

            async with AsyncExitStack() as locks:
                await self._enter_delivery_lock(locks, "event:" + rows[0]["event_id"])
                error = await self._deliver(payload)
        if isinstance(error, DeliveryError):
            if error.terminal:
                await self.terminal_failure(payload, error, 5)
            raise error

    async def _deliver_documents(self, notice):
        deadline = time.monotonic() + 30
        chat = str(self.settings.telegram_ops_chat or "")
        ledger = await self._all(
            "out.deliveries", filters={"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        )
        messages = [row for row in ledger if row["status"] == "SENT" and row.get("method") != "sendDocument"]
        if not messages:
            raise DeliveryError("Ops diagnostic parent message was not delivered")
        parent = messages[0]["message_ref"]
        attachments = documents(notice["entry_errors"])
        manifest = [
            {"filename": row.filename, "sha256": row.sha256, "bytes": len(row.content)} for row in attachments
        ]
        if manifest != notice["document_manifest"]:
            raise DeliveryError("Ops diagnostic document manifest mismatch")
        terminal_failure = None
        for document in attachments:
            document_id = sha256({"filename": document.filename, "sha256": document.sha256})
            history = [row for row in ledger if row.get("document_id") == document_id]
            if any(row["status"] == "SENT" for row in history):
                continue
            attempt = max((row.get("attempt", 0) for row in history), default=0) + 1
            if attempt > 5 or any(row.get("terminal") for row in history):
                terminal_failure = DeliveryError("Ops document delivery exhausted five attempts")
                terminal_failure.terminal = True
                continue
            if time.monotonic() >= deadline:
                return DeliveryDeferred(offset_schedule(collection_clock(utcnow()), timedelta(seconds=1)))
            transition = {
                "notice_id": notice["notice_id"],
                "channel": "telegram:ops",
                "method": "sendDocument",
                "document_id": document_id,
                "document_sha256": document.sha256,
                "document_bytes": len(document.content),
                "filename": document.filename,
                "attempt": attempt,
                "request_reply_to_message_ref": parent,
                **collection_clock(utcnow()).fields("at"),
            }
            await self.repo.put(
                "out.deliveries", new_id("delivery"), {**transition, "status": "GENERATED"}, immutable=True
            )
            try:
                response = await self.http.post(
                    f"https://api.telegram.org/bot{self.settings.telegram_token}/sendDocument",
                    data={
                        "chat_id": chat,
                        "reply_parameters": json.dumps({"message_id": int(parent)}),
                        "caption": f"{notice['notice_id']} · {document.filename}\nsha256={document.sha256} · {len(document.content)} bytes",
                        "disable_content_type_detection": "true",
                    },
                    files={"document": (document.filename, document.content, "application/octet-stream")},
                )
                data = response.json()
                if response.status_code != 200 or not data.get("ok"):
                    raise DeliveryError(
                        "Telegram rejected Ops document",
                        max(2 ** (attempt - 1), (data.get("parameters") or {}).get("retry_after", 0)),
                    )
                message_ref = str(data["result"]["message_id"])
            except Exception as exc:
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {
                        **transition,
                        **collection_clock(utcnow()).fields("at"),
                        "status": "FAILED",
                        "error": type(exc).__name__,
                        "terminal": attempt == 5,
                    },
                    immutable=True,
                )
                if attempt == 5:
                    terminal_failure = DeliveryError("Ops document delivery exhausted five attempts")
                    terminal_failure.terminal = True
                    continue
                return DeliveryDeferred(
                    offset_schedule(
                        collection_clock(utcnow()),
                        timedelta(seconds=getattr(exc, "retry_after", 2 ** (attempt - 1))),
                    )
                )
            await self.repo.put(
                "out.deliveries",
                new_id("delivery"),
                {
                    **transition,
                    **collection_clock(utcnow()).fields("at"),
                    "status": "SENT",
                    "message_ref": message_ref,
                    "chat_id": chat,
                },
                immutable=True,
            )
        return terminal_failure

    async def _deliver_progress(self, notice):
        from tank.ops_cards import render, snapshot, time_of

        item_id = notice["subject_id"]
        chat = str(self.settings.telegram_ops_chat or "")
        if not self.settings.telegram_token or not chat:
            raise ValueError("Telegram token and destination must be configured")
        key = sha256({"item_id": item_id, "chat_id": chat})
        async with AsyncExitStack() as locks:
            await self._enter_delivery_lock(locks, "ops-card:" + key)
            card = await self.repo.get("out.ops_cards", key)
            if card and notice["notice_id"] in card.get("covered_notice_ids", []):
                return None
            data = await snapshot(self.repo, item_id)
            if data is None:
                return None
            if data["item"].get("source_id") == "WEB" and data["item"].get("evidence_kind"):
                return None
            if card is None:
                card = {"item_id": item_id, "chat_id": chat, "message_ref": None}
                for previous in reversed(data["history"]):
                    if previous.get("format_version") == 2:
                        continue
                    receipts = await self.repo.list_latest(
                        "out.deliveries",
                        "at",
                        filters={
                            "notice_id": previous["notice_id"],
                            "channel": "telegram:ops",
                            "status": "SENT",
                            "chat_id": chat,
                        },
                        limit=1,
                    )
                    if receipts:
                        card["message_ref"] = receipts[0]["message_ref"]
                        card["adopted_notice_id"] = previous["notice_id"]
                        break
                await self.repo.put("out.ops_cards", key, card)
            result = render(data)
            digest = sha256(result["html"])
            covered = [row["notice_id"] for row in data["history"]]
            if card.get("render_digest") == digest:
                await self.repo.put("out.ops_cards", key, {**card, "covered_notice_ids": covered})
                return None
            if card.get("blocked"):
                return None
            delay = getattr(self.settings, "ops_progress_delay_s", 15)
            first = time_of(data["history"][0]) if data["history"] else utcnow()
            if not card.get("message_ref"):
                due = first + timedelta(seconds=delay)
            else:
                due = (
                    parse_time(card["updated_at"]) + timedelta(seconds=min(delay, 5))
                    if card.get("updated_at")
                    else utcnow()
                )
            if utcnow() < due:
                raise DeliveryDeferred(collection_clock(due))
            await self._enter_delivery_lock(locks, "telegram-ops-progress")
            backoff = await self.repo.get("ops.scheduler_checkpoints", "ops-progress-backoff")
            if backoff and utcnow() < parse_time(backoff["state"]["until"]):
                raise DeliveryDeferred(collection_clock(backoff["state"]["until"]))
            latest = await self.repo.list_latest(
                "out.deliveries", "at", filters={"channel": "telegram:ops", "status": "SENT"}, limit=1
            )
            if latest:
                next_send = offset_schedule(propagate_timestamp(latest[0], "at"), timedelta(seconds=3.1))
                if utcnow() < parse_time(next_send.value):
                    raise DeliveryDeferred(next_send)
            ledger = await self._all(
                "out.deliveries", filters={"progress_card": key, "render_digest": digest}
            )
            attempt = max((row.get("attempt", 0) for row in ledger), default=0) + 1
            if attempt > 5:
                failure = DeliveryError("Ops card delivery exhausted five attempts")
                failure.terminal = True
                return failure
            method = "editMessageText" if card.get("message_ref") else "sendMessage"
            body = {
                "chat_id": chat,
                "text": result["html"],
                "parse_mode": "HTML",
                "link_preview_options": {"is_disabled": True},
            }
            if card.get("message_ref"):
                body["message_id"] = int(card["message_ref"])
            else:
                body["disable_notification"] = True
            transition = {
                "notice_id": notice["notice_id"],
                "channel": "telegram:ops",
                "attempt": attempt,
                "render_digest": digest,
                "progress_card": key,
                "rendered_notice_id": data["history"][-1]["notice_id"],
                "method": method,
                "status": "GENERATED",
                **collection_clock(utcnow()).fields("at"),
            }
            await self.repo.put("out.deliveries", new_id("delivery"), transition, immutable=True)
            code = None
            try:
                response = await self.http.post(
                    f"https://api.telegram.org/bot{self.settings.telegram_token}/{method}", json=body
                )
                response_data = response.json()
                unchanged = (
                    bool(card.get("message_ref"))
                    and response.status_code == 400
                    and "message is not modified" in str(response_data.get("description", "")).lower()
                )
                if not unchanged and (response.status_code != 200 or not response_data.get("ok")):
                    code = response.status_code
                    retry_after = max(
                        2 ** (attempt - 1), (response_data.get("parameters") or {}).get("retry_after", 0)
                    )
                    if code == 429:
                        stamp = collection_clock(utcnow())
                        await self.repo.put(
                            "ops.scheduler_checkpoints",
                            "ops-progress-backoff",
                            {
                                "checkpoint_id": "ops-progress-backoff",
                                **stamp.fields("at"),
                                "state": {
                                    "until": offset_schedule(stamp, timedelta(seconds=retry_after)).value
                                },
                            },
                        )
                    raise DeliveryError(f"Telegram rejected Ops card (HTTP {code})", retry_after)
                message_ref = (
                    str(card["message_ref"])
                    if card.get("message_ref")
                    else str(response_data["result"]["message_id"])
                )
            except Exception as exc:
                terminal = attempt == 5 or code in {400, 403}
                await self.repo.put(
                    "out.ops_cards",
                    key,
                    {
                        **card,
                        "delivery_status": "FAILED",
                        "delivery_error": f"HTTP {code}" if code else type(exc).__name__,
                        "blocked": code in {400, 403},
                    },
                )
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {
                        **transition,
                        **collection_clock(utcnow()).fields("at"),
                        "status": "FAILED",
                        "terminal": terminal,
                        "error": f"HTTP {code}" if code else type(exc).__name__,
                    },
                    immutable=True,
                )
                failure = DeliveryError(
                    "Ops card delivery failed", getattr(exc, "retry_after", 2 ** (attempt - 1))
                )
                failure.terminal = terminal
                return failure
            stamp = collection_clock(utcnow())
            async with self.repo.transaction():
                await self.repo.put(
                    "out.ops_cards",
                    key,
                    {
                        **card,
                        "message_ref": message_ref,
                        "render_digest": digest,
                        "rendered_html": result["html"],
                        "covered_notice_ids": covered,
                        "outcome": result["code"],
                        "notice_id": data["history"][-1]["notice_id"],
                        "delivery_status": "SENT",
                        "delivery_error": None,
                        **stamp.fields("updated_at"),
                    },
                )
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {
                        **transition,
                        **stamp.fields("at"),
                        "status": "SENT",
                        "message_ref": message_ref,
                        "chat_id": chat,
                    },
                    immutable=True,
                )
            return None

    async def _deliver(self, payload: DeliveryJob) -> DeliveryError | None:
        subject, channel = delivery_subject(payload), payload.channel
        async with AsyncExitStack() as locks:
            await self._enter_delivery_lock(locks, "deliver:" + subject + ":" + channel)
            filters = delivery_filters(payload)
            ledger = await self._all("out.deliveries", filters=filters)
            if any(row["status"] == "SENT" or row.get("terminal") for row in ledger):
                return None
            attempt = max((r["attempt"] for r in ledger), default=0) + 1
            if attempt > 5:
                failure = DeliveryError("Delivery exhausted five attempts")
                failure.terminal = True
                return failure
            notice = isinstance(payload, NoticeDeliveryJob)
            if isinstance(payload, NoticeDeliveryJob):
                record = await self.repo.get("out.ops_notices", payload.notice_id)
            else:
                rows = await self._all(
                    "out.alerts", filters={"alert_id": payload.alert_id, "version": payload.version}
                )
                record = rows[0] if rows else None
            if record is None:
                raise ValueError("Delivery subject does not exist")
            if not notice:
                prior = await self._all("out.alerts", filters={"event_id": record["event_id"]})
                if record.get("kind") == "UPDATE" and any(
                    row["alert_id"] == record["alert_id"] and row["version"] > record["version"]
                    for row in prior
                ):
                    return None
                ids = {r["alert_id"] for r in prior}
                sent = []
                for alert_id in ids:
                    sent.extend(
                        await self._all(
                            "out.deliveries",
                            filters={"alert_id": alert_id, "channel": channel, "status": "SENT"},
                        )
                    )

                boundaries = []
                if record.get("delivery_not_before"):
                    boundaries.append(propagate_timestamp(record, "delivery_not_before"))
                if record.get("kind") == "UPDATE" and not record.get("delivery_immediate", False) and sent:
                    latest_sent = max(sent, key=lambda row: parse_time(row["at"]))
                    limits = ((record.get("card_struct") or {}).get("event") or {}).get("thresholds") or {}
                    boundaries.append(
                        offset_schedule(
                            propagate_timestamp(latest_sent, "at"),
                            timedelta(seconds=limits.get("update_rate_limit_s", 900)),
                        )
                    )
                if boundaries:
                    due = max(boundaries, key=delivery_moment)
                    if utcnow() < delivery_moment(due):
                        return DeliveryDeferred(due)
            chat = self.settings.telegram_ops_chat if notice else self.settings.telegram_alerts_chat
            if not self.settings.telegram_token or not chat:
                raise ValueError("Telegram token and destination must be configured")
            rich_message = record.get("rich_message") if not notice else None
            method = "sendRichMessage" if rich_message else "sendMessage"
            body = {"chat_id": chat}
            if rich_message:
                body["rich_message"] = rich_message
            else:
                body.update(
                    text=html.escape(record["text"]) if notice else telegram_card_html(record["card_text"]),
                    parse_mode="HTML",
                    link_preview_options={"is_disabled": True},
                )
            if not notice:
                sent = sorted(sent, key=lambda r: r["at"])
                if sent:
                    body["reply_parameters"] = {"message_id": int(sent[0]["message_ref"])}
            attempted = collection_clock(utcnow())
            source_times = {
                key: value
                for field in ("generated_at", "delivery_not_before")
                if field in record
                for key, value in propagate_timestamp(record, field).fields(field).items()
            }
            transition = {
                **filters,
                **source_times,
                "attempt": attempt,
                **attempted.fields("at"),
                **attempted.fields("attempted_at"),
                "status": "GENERATED",
                "request_reply_to_message_ref": (
                    str(body["reply_parameters"]["message_id"]) if body.get("reply_parameters") else None
                ),
            }
            await self.repo.put("out.deliveries", new_id("delivery"), transition, immutable=True)
            response_status = None
            try:
                response = await self.http.post(
                    f"https://api.telegram.org/bot{self.settings.telegram_token}/{method}", json=body
                )
                response_status = response.status_code
                data = response.json()
                if response.status_code != 200 or not data.get("ok"):
                    delay = max(2 ** (attempt - 1), data.get("parameters", {}).get("retry_after", 0))
                    raise DeliveryError(
                        f"Telegram API rejected delivery (HTTP {response.status_code})", delay
                    )
                message_ref = str(data["result"]["message_id"])
                telegram_date = data["result"].get("date")
                returned_reply = data["result"].get("reply_to_message") or {}
                response_reply_to_message_ref = (
                    str(returned_reply["message_id"])
                    if returned_reply.get("message_id") is not None
                    else None
                )
            except Exception as exc:
                failed_at = collection_clock(utcnow())
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {
                        **transition,
                        **failed_at.fields("at"),
                        **failed_at.fields("failed_at"),
                        "status": "FAILED",
                        "error": type(exc).__name__,
                        "http_status": response_status,
                    },
                    immutable=True,
                )
                failure = DeliveryError(
                    "Telegram delivery failed", getattr(exc, "retry_after", 2 ** (attempt - 1))
                )
                failure.terminal = attempt == 5
                return failure
            async with (
                self.repo.transaction(),
                self.repo.lock("event:" + record["event_id"] if not notice else "notice:" + subject),
            ):
                sent_at = collection_clock(utcnow())
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {
                        **transition,
                        **sent_at.fields("at"),
                        **sent_at.fields("sent_at"),
                        "status": "SENT",
                        "message_ref": message_ref,
                        "chat_id": str(chat),
                        "telegram_date": telegram_date,
                        "response_reply_to_message_ref": response_reply_to_message_ref,
                    },
                    immutable=True,
                )
                if not notice:
                    await event_progress(
                        self.repo,
                        self.settings,
                        record["event_id"],
                        "ALERT_SENT",
                        f"Карточка {subject}; Telegram message_id={message_ref}",
                        key=subject,
                    )
                    await self._activate_calls(record, sent_at)
                    event = await self.repo.get("evt.events", record["event_id"])
                    if event and not event.get("first_card_delivered_at"):
                        delivered = utcnow()
                        event.update(
                            **collection_clock(delivered).fields("first_card_delivered_at"),
                            first_card_latency_s=(
                                delivered - parse_time(event["known_to_us_at"])
                            ).total_seconds()
                            if event.get("known_to_us_at")
                            else None,
                        )
                        assessment = (record.get("card_struct") or {}).get("assessment") or {}
                        if (
                            assessment.get("class") == "STRUCTURAL_SHOCK"
                            and event.get("known_to_us_at")
                            and not event.get("first_card_deadline")
                        ):
                            event.update(
                                offset_schedule(
                                    propagate_timestamp(event, "known_to_us_at"), timedelta(minutes=3)
                                ).fields("first_card_deadline")
                            )
                        if event.get("first_card_deadline"):
                            event["first_card_deadline_met"] = delivered <= parse_time(
                                event["first_card_deadline"]
                            )
                        await self.repo.put("evt.events", event["event_id"], event)
                    if event and event.get("lifecycle") in ("ANALYZING", "ALERTED"):
                        await self.repo.put(
                            "evt.events",
                            event["event_id"],
                            {
                                **event,
                                "lifecycle": "ALERTED",
                                **collection_clock(utcnow()).fields("alerted_at"),
                            },
                        )
                    if (
                        event
                        and event.get("lifecycle") == "INVALIDATED"
                        and record.get("terminal_correction")
                    ):
                        open_calls = await self._all(
                            "ana.calls", filters={"event_id": event["event_id"], "status": "OPEN"}
                        )
                        if not open_calls:
                            await self.repo.put(
                                "evt.events",
                                event["event_id"],
                                {
                                    **event,
                                    **collection_clock(utcnow()).fields("session_finalized_at"),
                                    "terminal_alert_id": record["alert_id"],
                                    "terminal_alert_version": record["version"],
                                },
                            )
                if not notice and record.get("milestone_resolution_id"):
                    event = await self.repo.get("evt.events", record["event_id"])
                    if event and event.get("milestone_resolution_id") == record["milestone_resolution_id"]:
                        await self.repo.put(
                            "evt.events", event["event_id"], {**event, "milestone_resolved": False}
                        )
                if not notice:
                    expired_ids = (
                        record.get("expired_call_ids")
                        or record.get("call_expired_ids")
                        or ((record.get("card_struct") or {}).get("event") or {}).get("call_expired_ids")
                        or []
                    )
                    if expired_ids:
                        event = await self.repo.get("evt.events", record["event_id"])
                        remaining = sorted(set(event.get("call_expired_ids", [])) - set(expired_ids))
                        await self.repo.put(
                            "evt.events",
                            event["event_id"],
                            {
                                **event,
                                "call_expired": bool(remaining),
                                "call_expired_ids": remaining,
                                "expiry_notices_sent": sorted(
                                    set(event.get("expiry_notices_sent", [])) | set(expired_ids)
                                ),
                                **collection_clock(utcnow()).fields("last_expiry_notice_at"),
                            },
                        )

        return None

    async def _activate_calls(self, alert, sent_at=None):
        sent_at = sent_at or collection_clock(utcnow())
        for dec_id in alert.get("decisions", []):
            decision = await self.repo.get("ana.decisions", dec_id)
            if not decision:
                raise ValueError("Delivered card references missing decision")
            if decision.get("tracking_ended") or decision.get("actionable") is False:
                continue
            calls = await self._all(
                "ana.calls",
                filters={"event_id": alert["event_id"], "token_id": decision["token_id"], "status": "OPEN"},
            )
            if decision["action"] == "START" and not calls:
                entry = decision.get("entry_ref_price")
                invalidation = decision.get("invalidation_price")
                if entry is None or invalidation is None:
                    raise ValueError(
                        "START decision must freeze entry_ref_price and invalidation_price before delivery"
                    )
                call_id = new_id("call")
                await self.repo.put(
                    "ana.calls",
                    call_id,
                    {
                        "call_id": call_id,
                        "event_id": alert["event_id"],
                        "token_id": decision["token_id"],
                        "opened_by_alert": alert["alert_id"],
                        "opened_by_version": alert["version"],
                        **sent_at.fields("opened_at"),
                        **propagate_timestamp(alert, "generated_at").fields("alert_generated_at"),
                        "entry_ref_price": entry,
                        "invalidation_price": invalidation,
                        "status": "OPEN",
                    },
                )
            elif decision["action"] == "EXIT":
                for call in calls:
                    await self.repo.put(
                        "ana.calls",
                        call["call_id"],
                        {**call, "status": "CLOSED", "closed_by": "EXIT", **sent_at.fields("closed_at")},
                    )
