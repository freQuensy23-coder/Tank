"""Immutable Telegram delivery ledger and delivered-call activation."""

import asyncio
import html
import json
import time
from collections.abc import Mapping
from contextlib import AsyncExitStack
from datetime import datetime, timedelta

import httpx
from pydantic import JsonValue

from tank.analysis.cards import telegram_card_html
from tank.contracts.delivery import (
    CardDeliveryHeader,
    CardDeliveryText,
    DeliveredCardActivation,
    DeliveredCorrection,
    DeliveredEventView,
    DeliveryAttemptOrdinal,
    DeliveryEventBinding,
    DeliveryExpiryReferences,
    DeliveryLedgerPhase,
    DeliverySettings,
    DeliveryStore,
    DeliveryTelegramReceipt,
    DeliveryTelegramRejection,
    DeliveryText,
    DocumentDeliveryAttempt,
    DocumentDeliveryNotice,
    ExitCallDecision,
    FrozenDeliveryAssessment,
    FrozenDeliveryInterval,
    InvalidatedDeliveryEvent,
    OpenDeliveryCall,
    OpsBackoffState,
    OpsCardDeliveryState,
    ProgressDeliveryNotice,
    SentDeliveryReceipt,
    StartCallDecision,
    StartCallPrices,
    call_activation_decision,
)
from tank.contracts.json import decode_unique_json
from tank.diagnostic_documents import documents
from tank.ops_progress import event_progress
from tank.runtime_contracts import AlertDeliveryJob, DeliveryJob, NoticeDeliveryJob, parse_delivery_job
from tank.telegram_api import response_result, sent_message_receipt
from tank.telegram_rich import RichMessage
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
    if isinstance(payload, AlertDeliveryJob):
        return AlertDeliveryJob.model_validate(payload, by_name=True)
    if isinstance(payload, NoticeDeliveryJob):
        return NoticeDeliveryJob.model_validate(payload, by_name=True)
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
    def __init__(
        self, repo: DeliveryStore, settings: DeliverySettings, *, http: httpx.AsyncClient | None = None
    ) -> None:
        self.repo, self.settings = repo, settings
        self.http = http if http is not None else httpx.AsyncClient(timeout=8)
        self.owns_http = http is None

    async def close(self) -> None:
        if self.owns_http:
            await self.http.aclose()

    async def _all(
        self, collection: str, *, filters: dict[str, JsonValue] | None = None
    ) -> list[dict[str, JsonValue]]:
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
            phases = [
                DeliveryLedgerPhase.model_validate(
                    {name: row[name] for name in DeliveryLedgerPhase.model_fields if name in row}
                )
                for row in ledger
            ]
            if any(row.status == "SENT" for row in phases):
                return
            source = None
            if isinstance(payload, AlertDeliveryJob):
                alerts = await self._all(
                    "out.alerts", filters={"alert_id": payload.alert_id, "version": payload.version}
                )
                source = alerts[0] if alerts else None
            if not any(row.terminal is True for row in phases):
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
                        else max(
                            (
                                DeliveryAttemptOrdinal.model_validate(
                                    {name: row[name] for name in ("attempt",) if name in row}
                                ).attempt
                                for row in ledger
                            ),
                            default=0,
                        ),
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
                DeliveryEventBinding.model_validate(
                    {name: source[name] for name in ("event_id",) if name in source}
                ).event_id
                if source is not None
                else None,
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
                error = await self._deliver_progress(ProgressDeliveryNotice.from_record(notice))
            elif notice and notice.get("document_manifest"):
                document_notice = DocumentDeliveryNotice.from_record(notice)
                if (
                    tuple(row.reference() for row in documents(document_notice.entry_errors))
                    != document_notice.document_manifest
                ):
                    raise DeliveryError("Ops diagnostic document manifest mismatch")
                async with AsyncExitStack() as locks:
                    await self._enter_delivery_lock(locks, "ops-documents:" + document_notice.notice_id)
                    error = await self._deliver(payload)
                    if error is None:
                        error = await self._deliver_documents(document_notice)
            else:
                error = await self._deliver(payload)
        else:
            rows = await self._all(
                "out.alerts", filters={"alert_id": payload.alert_id, "version": payload.version}
            )
            if not rows:
                raise ValueError("Delivery subject does not exist")

            owner = CardDeliveryHeader.from_record(rows[0])
            if owner.alert_id != payload.alert_id or owner.version != payload.version:
                raise ValueError("Delivery publication differs from its actual queue target")
            async with AsyncExitStack() as locks:
                await self._enter_delivery_lock(locks, "event:" + owner.event_id)
                error = await self._deliver(payload)
        if isinstance(error, DeliveryError):
            if error.terminal:
                await self.terminal_failure(payload, error, 5)
            raise error

    async def _deliver_documents(self, notice: DocumentDeliveryNotice) -> DeliveryError | None:
        deadline = time.monotonic() + 30
        chat = str(self.settings.telegram_ops_chat or "")
        ledger = [
            DocumentDeliveryAttempt.from_record(row)
            for row in await self._all(
                "out.deliveries", filters={"notice_id": notice.notice_id, "channel": "telegram:ops"}
            )
        ]
        messages = [row for row in ledger if row.status == "SENT" and row.method != "sendDocument"]
        if not messages:
            raise DeliveryError("Ops diagnostic parent message was not delivered")
        parent = messages[0].message_ref
        if parent is None:
            raise DeliveryError("Ops diagnostic parent lacks its actual message receipt")
        attachments = documents(notice.entry_errors)
        manifest = tuple(row.reference() for row in attachments)
        if manifest != notice.document_manifest:
            raise DeliveryError("Ops diagnostic document manifest mismatch")
        terminal_failure = None
        for document in attachments:
            document_id = sha256({"filename": document.filename, "sha256": document.sha256})
            history = [row for row in ledger if row.document_id == document_id]
            if any(row.status == "SENT" for row in history):
                continue
            attempt = max((row.attempt for row in history), default=0) + 1
            if attempt > 5 or any(row.terminal for row in history):
                terminal_failure = DeliveryError("Ops document delivery exhausted five attempts")
                terminal_failure.terminal = True
                continue
            if time.monotonic() >= deadline:
                return DeliveryDeferred(offset_schedule(collection_clock(utcnow()), timedelta(seconds=1)))
            transition = {
                "notice_id": notice.notice_id,
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
                        "caption": f"{notice.notice_id} · {document.filename}\nsha256={document.sha256} · {len(document.content)} bytes",
                        "disable_content_type_detection": "true",
                    },
                    files={"document": (document.filename, document.content, "application/octet-stream")},
                )
                observed = decode_unique_json(response.content)
                if isinstance(observed, dict) and observed.get("ok") is False:
                    rejected = DeliveryTelegramRejection.model_validate(
                        {name: observed[name] for name in ("ok", "parameters") if name in observed}
                    )
                    delay = 2 ** (attempt - 1)
                    if rejected.parameters is not None and rejected.parameters.retry_after is not None:
                        delay = max(delay, rejected.parameters.retry_after)
                    raise DeliveryError("Telegram rejected Ops document", delay)
                result = response_result(response.content, method="sendDocument", status=response.status_code)
                message_ref = str(sent_message_receipt(result).message_id)
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

    async def _deliver_progress(self, notice: ProgressDeliveryNotice) -> DeliveryError | None:
        from tank.ops_cards import render, snapshot, time_of

        item_id = notice.subject_id
        chat = str(self.settings.telegram_ops_chat or "")
        if not self.settings.telegram_token or not chat:
            raise ValueError("Telegram token and destination must be configured")
        key = sha256({"item_id": item_id, "chat_id": chat})
        async with AsyncExitStack() as locks:
            await self._enter_delivery_lock(locks, "ops-card:" + key)
            card_record = await self.repo.get("out.ops_cards", key)
            card = (
                OpsCardDeliveryState.from_record(card_record, item_id=item_id, chat_id=chat)
                if card_record is not None
                else None
            )
            if (
                card is not None
                and card.covered_notice_ids is not None
                and notice.notice_id in card.covered_notice_ids
            ):
                return None
            data = await snapshot(self.repo, item_id)
            if data is None:
                return None
            if data.item.source_id == "WEB" and data.item.evidence_kind:
                return None
            if card is None:
                card_record = {"item_id": item_id, "chat_id": chat, "message_ref": None}
                for previous in reversed(data.history):
                    if previous.format_version == 2:
                        continue
                    receipts = await self.repo.list_latest(
                        "out.deliveries",
                        "at",
                        filters={
                            "notice_id": previous.notice_id,
                            "channel": "telegram:ops",
                            "status": "SENT",
                            "chat_id": chat,
                        },
                        limit=1,
                    )
                    if receipts:
                        card_record["message_ref"] = receipts[0]["message_ref"]
                        card_record["adopted_notice_id"] = previous.notice_id
                        break
                await self.repo.put("out.ops_cards", key, card_record)
                card = OpsCardDeliveryState.from_record(card_record, item_id=item_id, chat_id=chat)
            if card_record is None:
                raise ValueError("Ops card delivery lacks its stored state")
            result = render(data)
            digest = sha256(result.html)
            covered: list[JsonValue] = [row.notice_id for row in data.history]
            if card.render_digest == digest:
                await self.repo.put("out.ops_cards", key, {**card_record, "covered_notice_ids": covered})
                return None
            if card.blocked:
                return None
            delay = self.settings.ops_progress_delay_s
            first = time_of(data.history[0]) if data.history else utcnow()
            if not card.message_ref:
                due = first + timedelta(seconds=delay)
            else:
                due = (
                    parse_time(card.updated_at) + timedelta(seconds=min(delay, 5))
                    if card.updated_at
                    else utcnow()
                )
            if utcnow() < due:
                raise DeliveryDeferred(collection_clock(due))
            await self._enter_delivery_lock(locks, "telegram-ops-progress")
            backoff = await self.repo.get("ops.scheduler_checkpoints", "ops-progress-backoff")
            if backoff is not None:
                state = OpsBackoffState.model_validate(backoff.get("state"))
                if utcnow() < parse_time(state.until):
                    raise DeliveryDeferred(collection_clock(state.until))
            latest = await self.repo.list_latest(
                "out.deliveries", "at", filters={"channel": "telegram:ops", "status": "SENT"}, limit=1
            )
            if latest:
                next_send = offset_schedule(propagate_timestamp(latest[0], "at"), timedelta(seconds=3.1))
                if utcnow() < delivery_moment(next_send):
                    raise DeliveryDeferred(next_send)
            ledger = await self._all(
                "out.deliveries", filters={"progress_card": key, "render_digest": digest}
            )
            attempt = (
                max(
                    (
                        DeliveryAttemptOrdinal.model_validate(
                            {name: row[name] for name in ("attempt",) if name in row}
                        ).attempt
                        for row in ledger
                    ),
                    default=0,
                )
                + 1
            )
            if attempt > 5:
                failure = DeliveryError("Ops card delivery exhausted five attempts")
                failure.terminal = True
                return failure
            method = "editMessageText" if card.message_ref else "sendMessage"
            body: dict[str, JsonValue] = {
                "chat_id": chat,
                "text": result.html,
                "parse_mode": "HTML",
                "link_preview_options": {"is_disabled": True},
            }
            if card.message_ref:
                body["message_id"] = int(card.message_ref)
            else:
                body["disable_notification"] = True
            transition: dict[str, JsonValue] = {
                "notice_id": notice.notice_id,
                "channel": "telegram:ops",
                "attempt": attempt,
                "render_digest": digest,
                "progress_card": key,
                "rendered_notice_id": data.history[-1].notice_id,
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
                observed = decode_unique_json(response.content)
                rejection = (
                    DeliveryTelegramRejection.model_validate(
                        {
                            name: observed[name]
                            for name in ("ok", "description", "parameters")
                            if name in observed
                        }
                    )
                    if isinstance(observed, dict) and observed.get("ok") is False
                    else None
                )
                unchanged = (
                    card.message_ref is not None
                    and response.status_code == 400
                    and rejection is not None
                    and rejection.description is not None
                    and "message is not modified" in rejection.description.lower()
                )
                if not unchanged and (response.status_code != 200 or rejection is not None):
                    code = response.status_code
                    retry_after = 2 ** (attempt - 1)
                    if (
                        rejection is not None
                        and rejection.parameters is not None
                        and rejection.parameters.retry_after is not None
                    ):
                        retry_after = max(retry_after, rejection.parameters.retry_after)
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
                if card.message_ref is not None:
                    if not unchanged:
                        response_result(response.content, method=method, status=response.status_code)
                    message_ref = str(card.message_ref)
                else:
                    message_ref = str(
                        sent_message_receipt(
                            response_result(response.content, method=method, status=response.status_code)
                        ).message_id
                    )
            except Exception as exc:
                terminal = attempt == 5 or code in {400, 403}
                await self.repo.put(
                    "out.ops_cards",
                    key,
                    {
                        **card_record,
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
                        **card_record,
                        "message_ref": message_ref,
                        "render_digest": digest,
                        "rendered_html": result.html,
                        "covered_notice_ids": covered,
                        "outcome": result.code,
                        "notice_id": data.history[-1].notice_id,
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
            phases = [
                DeliveryLedgerPhase.model_validate(
                    {name: row[name] for name in DeliveryLedgerPhase.model_fields if name in row}
                )
                for row in ledger
            ]
            if any(row.status == "SENT" or row.terminal is True for row in phases):
                return None
            attempt = (
                max(
                    (
                        DeliveryAttemptOrdinal.model_validate(
                            {name: row[name] for name in ("attempt",) if name in row}
                        ).attempt
                        for row in ledger
                    ),
                    default=0,
                )
                + 1
            )
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
            card = CardDeliveryHeader.from_record(record) if isinstance(payload, AlertDeliveryJob) else None
            if card is not None:
                if (
                    not isinstance(payload, AlertDeliveryJob)
                    or card.alert_id != payload.alert_id
                    or card.version != payload.version
                ):
                    raise ValueError("Delivery publication differs from its actual queue target")
                prior = await self._all("out.alerts", filters={"event_id": card.event_id})
                revisions = [CardDeliveryHeader.from_record(row) for row in prior]
                if card.kind in {"UPDATE", "PRELIMINARY"} and any(
                    row.alert_id == card.alert_id and row.version > card.version for row in revisions
                ):
                    return None
                ids = {row.alert_id for row in revisions}
                sent: list[SentDeliveryReceipt] = []
                for alert_id in ids:
                    sent.extend(
                        SentDeliveryReceipt.from_record(row)
                        for row in await self._all(
                            "out.deliveries",
                            filters={"alert_id": alert_id, "channel": channel, "status": "SENT"},
                        )
                    )

                boundaries = []
                if card.delivery_not_before:
                    boundaries.append(propagate_timestamp(record, "delivery_not_before"))
                if card.kind in {"UPDATE", "PRELIMINARY"} and not card.delivery_immediate and sent:
                    latest_sent = max(sent, key=lambda row: delivery_moment(row.at))
                    limits = FrozenDeliveryInterval.from_card(record)
                    boundaries.append(
                        offset_schedule(
                            latest_sent.at,
                            timedelta(
                                seconds=limits.update_rate_limit_s
                                if limits.update_rate_limit_s is not None
                                else 900
                            ),
                        )
                    )
                if boundaries:
                    due = max(boundaries, key=delivery_moment)
                    if utcnow() < delivery_moment(due):
                        return DeliveryDeferred(due)
            chat = self.settings.telegram_ops_chat if notice else self.settings.telegram_alerts_chat
            if not self.settings.telegram_token or not chat:
                raise ValueError("Telegram token and destination must be configured")
            raw_rich_message = record.get("rich_message") if card is not None else None
            rich_message = RichMessage.model_validate(raw_rich_message) if raw_rich_message else None
            method = "sendRichMessage" if rich_message else "sendMessage"
            body: dict[str, JsonValue] = {"chat_id": chat}
            if rich_message:
                body["rich_message"] = rich_message.telegram_payload()
            else:
                body.update(
                    text=html.escape(
                        DeliveryText.model_validate(
                            {name: record[name] for name in ("text",) if name in record}
                        ).text
                    )
                    if card is None
                    else telegram_card_html(
                        CardDeliveryText.model_validate(
                            {name: record[name] for name in ("card_text",) if name in record}
                        ).card_text
                    ),
                    parse_mode="HTML",
                    link_preview_options={"is_disabled": True},
                )
            reply_ref: str | None = None
            if card is not None:
                sent = sorted(sent, key=lambda row: delivery_moment(row.at))
                if sent:
                    reply_ref = str(sent[0].message_ref)
                    body["reply_parameters"] = {"message_id": int(sent[0].message_ref)}
            attempted = collection_clock(utcnow())
            source_times = {
                key: value
                for field in ("generated_at", "delivery_not_before")
                if field in record
                for key, value in propagate_timestamp(record, field).fields(field).items()
            }
            transition: dict[str, JsonValue] = {
                **filters,
                **source_times,
                "attempt": attempt,
                **attempted.fields("at"),
                **attempted.fields("attempted_at"),
                "status": "GENERATED",
                "request_reply_to_message_ref": (reply_ref),
            }
            await self.repo.put("out.deliveries", new_id("delivery"), transition, immutable=True)
            response_status = None
            try:
                response = await self.http.post(
                    f"https://api.telegram.org/bot{self.settings.telegram_token}/{method}", json=body
                )
                response_status = response.status_code
                observed = decode_unique_json(response.content)
                if isinstance(observed, dict) and observed.get("ok") is False:
                    rejected = DeliveryTelegramRejection.model_validate(
                        {
                            name: observed[name]
                            for name in ("ok", "parameters", "description")
                            if name in observed
                        }
                    )
                    delay = 2 ** (attempt - 1)
                    if rejected.parameters is not None and rejected.parameters.retry_after is not None:
                        delay = max(delay, rejected.parameters.retry_after)
                    raise DeliveryError(
                        f"Telegram API rejected delivery (HTTP {response.status_code})", delay
                    )
                receipt = DeliveryTelegramReceipt.from_result(
                    response_result(response.content, method=method, status=response.status_code)
                )
                message_ref = str(receipt.message_id)
                telegram_date = receipt.date
                response_reply_to_message_ref = (
                    str(receipt.reply_to_message.message_id) if receipt.reply_to_message is not None else None
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
                self.repo.lock("event:" + card.event_id if card is not None else "notice:" + subject),
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
                if card is not None:
                    await event_progress(
                        self.repo,
                        self.settings,
                        card.event_id,
                        "ALERT_SENT",
                        f"Карточка {subject}; Telegram message_id={message_ref}",
                        key=subject,
                    )
                    if card.kind == "PRELIMINARY":
                        event = await self.repo.get("evt.events", card.event_id)
                        if event and event.get("lifecycle") == "INVALIDATED" and card.terminal_correction:
                            await self._finalize_invalidated(event, record)
                        return None
                    await self._activate_calls(record, sent_at)
                    event = await self.repo.get("evt.events", card.event_id)
                    if event is not None:
                        current = DeliveredEventView.from_record(event, event_id=card.event_id)
                        if current.first_card_delivered_at is None:
                            delivered = utcnow()
                            event.update(
                                **collection_clock(delivered).fields("first_card_delivered_at"),
                                first_card_latency_s=(
                                    delivered - parse_time(current.known_to_us_at)
                                ).total_seconds()
                                if current.known_to_us_at is not None
                                else None,
                            )
                            assessment = FrozenDeliveryAssessment.from_card(record)
                            deadline = current.first_card_deadline
                            if (
                                assessment.classification == "STRUCTURAL_SHOCK"
                                and current.known_to_us_at is not None
                                and deadline is None
                            ):
                                deadline_stamp = offset_schedule(
                                    propagate_timestamp(event, "known_to_us_at"), timedelta(minutes=3)
                                )
                                deadline = deadline_stamp.value
                                event.update(deadline_stamp.fields("first_card_deadline"))
                            if deadline is not None:
                                event["first_card_deadline_met"] = delivered <= parse_time(deadline)
                            await self.repo.put("evt.events", current.event_id, event)
                        if current.lifecycle in ("ANALYZING", "ALERTED"):
                            await self.repo.put(
                                "evt.events",
                                current.event_id,
                                {
                                    **event,
                                    "lifecycle": "ALERTED",
                                    **collection_clock(utcnow()).fields("alerted_at"),
                                },
                            )
                        if current.lifecycle == "INVALIDATED" and card.terminal_correction:
                            open_calls = await self._all(
                                "ana.calls", filters={"event_id": current.event_id, "status": "OPEN"}
                            )
                            if not open_calls:
                                await self._finalize_invalidated(event, record)
                if card is not None and card.milestone_resolution_id:
                    event = await self.repo.get("evt.events", card.event_id)
                    if event is not None:
                        current = DeliveredEventView.from_record(event, event_id=card.event_id)
                        if current.milestone_resolution_id == card.milestone_resolution_id:
                            await self.repo.put(
                                "evt.events", current.event_id, {**event, "milestone_resolved": False}
                            )
                if card is not None:
                    expired_ids = DeliveryExpiryReferences.from_card(record).delivered_ids()
                    if expired_ids:
                        event = await self.repo.get("evt.events", card.event_id)
                        if event is None:
                            raise ValueError("Delivered expiry notice requires its actual event")
                        current = DeliveredEventView.from_record(event, event_id=card.event_id)
                        if current.call_expired_ids is None:
                            raise ValueError(
                                "Delivered expiry notice requires its recorded pending call identities"
                            )
                        remaining = sorted(set(current.call_expired_ids) - set(expired_ids))
                        previously_recorded = (
                            current.expiry_notices_sent if current.expiry_notices_sent is not None else []
                        )
                        remaining_wire: list[JsonValue] = list(remaining)
                        delivered_wire: list[JsonValue] = [
                            value for value in sorted(set(previously_recorded) | set(expired_ids))
                        ]
                        await self.repo.put(
                            "evt.events",
                            current.event_id,
                            {
                                **event,
                                "call_expired": bool(remaining),
                                "call_expired_ids": remaining_wire,
                                "expiry_notices_sent": delivered_wire,
                                **collection_clock(utcnow()).fields("last_expiry_notice_at"),
                            },
                        )

        return None

    async def _finalize_invalidated(
        self, event: Mapping[str, JsonValue], record: Mapping[str, JsonValue]
    ) -> None:
        """The delivered denial correction lets the invalidated event's session finish."""
        correction = DeliveredCorrection.from_record(record)
        owner = InvalidatedDeliveryEvent.model_validate(
            {name: event[name] for name in ("event_id", "lifecycle") if name in event}
        )
        if owner.event_id != correction.event_id:
            raise ValueError("Delivered correction does not belong to the invalidated event")
        await self.repo.put(
            "evt.events",
            owner.event_id,
            {
                **event,
                **collection_clock(utcnow()).fields("session_finalized_at"),
                "terminal_alert_id": correction.alert_id,
                "terminal_alert_version": correction.version,
            },
        )

    async def _activate_calls(self, record: Mapping[str, JsonValue], sent_at: Timestamp) -> None:
        alert = DeliveredCardActivation.from_record(record)
        delivery_moment(sent_at)
        for dec_id in alert.decisions:
            stored = await self.repo.get("ana.decisions", dec_id)
            if stored is None:
                raise ValueError("Delivered card references missing decision")
            decision = call_activation_decision(stored)
            if decision is None:
                continue
            calls = await self._all(
                "ana.calls",
                filters={"event_id": alert.event_id, "token_id": decision.token_id, "status": "OPEN"},
            )
            if isinstance(decision, StartCallDecision) and not calls:
                prices = StartCallPrices.model_validate(
                    {
                        name: stored[name]
                        for name in ("entry_ref_price", "invalidation_price")
                        if name in stored
                    }
                )
                call_id = new_id("call")
                await self.repo.put(
                    "ana.calls",
                    call_id,
                    {
                        "call_id": call_id,
                        "event_id": alert.event_id,
                        "token_id": decision.token_id,
                        "opened_by_alert": alert.alert_id,
                        "opened_by_version": alert.version,
                        **sent_at.fields("opened_at"),
                        **propagate_timestamp(record, "generated_at").fields("alert_generated_at"),
                        "entry_ref_price": prices.entry_ref_price,
                        "invalidation_price": prices.invalidation_price,
                        "status": "OPEN",
                    },
                )
            elif isinstance(decision, ExitCallDecision):
                for call in calls:
                    owner = OpenDeliveryCall.model_validate(
                        {name: call[name] for name in OpenDeliveryCall.model_fields if name in call}
                    )
                    if owner.event_id != alert.event_id or owner.token_id != decision.token_id:
                        raise ValueError("Delivered EXIT call differs from its requested event and token")
                    await self.repo.put(
                        "ana.calls",
                        owner.call_id,
                        {**call, "status": "CLOSED", "closed_by": "EXIT", **sent_at.fields("closed_at")},
                    )
