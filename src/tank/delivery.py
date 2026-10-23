"""Immutable Telegram delivery ledger and delivered-call activation."""

import asyncio
import html
from contextlib import AsyncExitStack
from datetime import timedelta

import httpx

from tank.analysis.cards import telegram_card_html
from tank.time_precision import collection_clock, offset_schedule, propagate_timestamp
from tank.util import new_id, parse_time, utcnow

DELIVERY_LOCK_WAIT_S = 6


class DeliveryError(RuntimeError):
    def __init__(self, message, retry_after=1):
        super().__init__(message)
        self.retry_after = retry_after


class DeliveryDeferred(DeliveryError):
    """An accepted card remains queued; no network attempt has occurred."""

    def __init__(self, not_before):
        self.not_before = not_before
        super().__init__(
            "Accepted card is scheduled for later delivery",
            retry_after=max(1, (parse_time(not_before.value) - utcnow()).total_seconds()),
        )


class DeliveryService:
    def __init__(self, repo, settings, *, http=None):
        self.repo, self.settings = repo, settings
        self.http = http or httpx.AsyncClient(timeout=8)
        self.owns_http = http is None

    async def close(self):
        if self.owns_http:
            await self.http.aclose()

    async def _all(self, collection, *, filters=None):
        return [row async for row in self.repo.iter_all(collection, filters=filters)]

    async def terminal_failure(self, payload, error, attempt=None):
        """Persist terminal queue failures, including outcomes before any HTTP send."""
        subject = payload.get("notice_id") or f"{payload['alert_id']}:{payload['version']}"
        channel = payload.get("channel", "telegram:ops" if payload.get("notice_id") else "telegram:alerts")
        filters = {"channel": channel}
        filters.update(
            {"notice_id": payload["notice_id"]}
            if payload.get("notice_id")
            else {"alert_id": payload["alert_id"], "version": payload["version"]}
        )
        async with self.repo.lock("deliver:" + subject + ":" + channel), self.repo.transaction():
            ledger = await self._all("out.deliveries", filters=filters)
            if any(row["status"] == "SENT" for row in ledger):
                return
            if not any(row.get("terminal") for row in ledger):
                source = payload
                if not payload.get("notice_id"):
                    alerts = await self._all(
                        "out.alerts",
                        filters={"alert_id": payload["alert_id"], "version": payload["version"]},
                    )
                    source = alerts[0] if alerts else payload
                source_times = {
                    key: value
                    for field in ("generated_at", "delivery_not_before")
                    if field in source
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
            if payload.get("notice_id"):
                return
            notice_id = "delivery_failed_" + subject.replace(":", "_")
            if not await self.repo.get("out.ops_notices", notice_id):
                await self.repo.put(
                    "out.ops_notices",
                    notice_id,
                    {
                        "notice_id": notice_id,
                        "kind": "DELIVERY_FAILED",
                        "subject_id": payload["alert_id"],
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

    async def _enter_delivery_lock(self, stack, key):
        try:
            async with asyncio.timeout(DELIVERY_LOCK_WAIT_S):
                await stack.enter_async_context(self.repo.lock(key))
        except TimeoutError:
            raise DeliveryDeferred(
                offset_schedule(collection_clock(utcnow()), timedelta(seconds=1))
            ) from None

    async def deliver(self, payload):
        if payload.get("notice_id"):
            error = await self._deliver(payload)
        else:
            rows = await self._all(
                "out.alerts", filters={"alert_id": payload["alert_id"], "version": payload["version"]}
            )
            if not rows:
                raise ValueError("Delivery subject does not exist")

            async with AsyncExitStack() as locks:
                await self._enter_delivery_lock(locks, "event:" + rows[0]["event_id"])
                error = await self._deliver(payload)
        if isinstance(error, DeliveryError):
            if getattr(error, "terminal", False):
                await self.terminal_failure(payload, error, 5)
            raise error

    async def _deliver(self, payload):
        subject = payload.get("notice_id") or f"{payload['alert_id']}:{payload['version']}"
        channel = payload.get("channel", "telegram:ops" if payload.get("notice_id") else "telegram:alerts")
        async with AsyncExitStack() as locks:
            await self._enter_delivery_lock(locks, "deliver:" + subject + ":" + channel)
            filters = {"channel": channel}
            filters.update(
                {"notice_id": payload["notice_id"]}
                if payload.get("notice_id")
                else {"alert_id": payload["alert_id"], "version": payload["version"]}
            )
            ledger = await self._all("out.deliveries", filters=filters)
            if any(row["status"] == "SENT" or row.get("terminal") for row in ledger):
                return
            attempt = max((r["attempt"] for r in ledger), default=0) + 1
            if attempt > 5:
                failure = DeliveryError("Delivery exhausted five attempts")
                failure.terminal = True
                return failure
            notice = bool(payload.get("notice_id"))
            if notice:
                record = await self.repo.get("out.ops_notices", payload["notice_id"])
            else:
                rows = await self._all(
                    "out.alerts", filters={"alert_id": payload["alert_id"], "version": payload["version"]}
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
                    due = max(boundaries, key=lambda stamp: parse_time(stamp.value))
                    if utcnow() < parse_time(due.value):
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
            try:
                response = await self.http.post(
                    f"https://api.telegram.org/bot{self.settings.telegram_token}/{method}", json=body
                )
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
                self.repo.lock(
                    "event:" + record["event_id"] if not notice else "notice:" + payload["notice_id"]
                ),
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
