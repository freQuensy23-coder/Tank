"""Immutable Telegram delivery ledger and delivered-call activation."""

import html
from datetime import timedelta

import httpx

from tank.util import iso, new_id, parse_time, utcnow


class DeliveryError(RuntimeError):
    def __init__(self, message, retry_after=1):
        super().__init__(message)
        self.retry_after = retry_after


class DeliveryDeferred(DeliveryError):
    def __init__(self, run_after):
        super().__init__("Ordinary update awaits the durable delivery rate window")
        self.run_after = run_after


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
        async with self.repo.lock("deliver:" + subject + ":" + channel):
            async with self.repo.transaction():
                ledger = await self._all("out.deliveries", filters=filters)
                if any(row["status"] == "SENT" or row.get("suppressed") for row in ledger):
                    return
                if not any(row.get("terminal") for row in ledger):
                    await self.repo.put(
                        "out.deliveries",
                        new_id("delivery"),
                        {
                            **filters,
                            "status": "FAILED",
                            "terminal": True,
                            "attempt": attempt
                            if attempt is not None
                            else max((r.get("attempt", 0) for r in ledger), default=0),
                            "at": iso(),
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
                            "created_at": iso(),
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
                    expires_at=iso(utcnow() + timedelta(days=1)),
                )

    async def deliver(self, payload):
        if payload.get("notice_id"):
            error = await self._deliver(payload)
        else:
            rows = await self._all(
                "out.alerts", filters={"alert_id": payload["alert_id"], "version": payload["version"]}
            )
            if not rows:
                raise ValueError("Delivery subject does not exist")
            # Serialization includes policy checks and the network send, not just ledger writes.
            async with self.repo.lock("event:" + rows[0]["event_id"]):
                error = await self._deliver(payload)
        if isinstance(error, DeliveryError):
            if getattr(error, "terminal", False):
                await self.terminal_failure(payload, error, 5)
            raise error

    async def _deliver(self, payload):
        subject = payload.get("notice_id") or f"{payload['alert_id']}:{payload['version']}"
        channel = payload.get("channel", "telegram:ops" if payload.get("notice_id") else "telegram:alerts")
        async with self.repo.lock("deliver:" + subject + ":" + channel):
            filters = {"channel": channel}
            filters.update(
                {"notice_id": payload["notice_id"]}
                if payload.get("notice_id")
                else {"alert_id": payload["alert_id"], "version": payload["version"]}
            )
            ledger = await self._all("out.deliveries", filters=filters)
            if any(row["status"] == "SENT" or row.get("suppressed") or row.get("terminal") for row in ledger):
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
                ids = {r["alert_id"] for r in prior}
                sent = []
                for alert_id in ids:
                    sent.extend(
                        await self._all(
                            "out.deliveries",
                            filters={"alert_id": alert_id, "channel": channel, "status": "SENT"},
                        )
                    )

                def newer(candidate):
                    return (
                        candidate["alert_id"] == record["alert_id"]
                        and candidate["version"] > record["version"]
                    ) or (
                        candidate.get("generated_at")
                        and record.get("generated_at")
                        and parse_time(candidate["generated_at"]) > parse_time(record["generated_at"])
                    )

                newer_candidates = [r for r in prior if newer(r)]
                sent_subjects = {(r["alert_id"], r["version"]) for r in sent}
                obsolete = (
                    record.get("kind") == "UPDATE"
                    and newer_candidates
                    or any((r["alert_id"], r["version"]) in sent_subjects for r in newer_candidates)
                )
                if obsolete:
                    await self.repo.put(
                        "out.deliveries",
                        new_id("delivery"),
                        {
                            **filters,
                            "status": "GENERATED",
                            "attempt": 0,
                            "at": iso(),
                            "suppressed": True,
                            "suppression_reason": "SUPERSEDED_BY_NEWER_EVENT_VERSION",
                        },
                        immutable=True,
                    )
                    return
                decisions = [await self.repo.get("ana.decisions", key) for key in record.get("decisions", [])]
                actionable = [
                    d
                    for d in decisions
                    if d and not d.get("tracking_ended") and d.get("actionable") is not False
                ]
                event_snapshot = (record.get("card_struct") or {}).get("event") or {}
                immediate = (
                    record.get("delivery_immediate", False)
                    or any(d.get("action") in {"START", "EXIT"} for d in actionable)
                    or any(
                        c.get("type") == "FATAL" and not c.get("resolved_at")
                        for c in event_snapshot.get("contradictions", [])
                    )
                )
                if record.get("kind") == "UPDATE" and not immediate:
                    limits = (
                        [parse_time(record["delivery_not_before"])]
                        if record.get("delivery_not_before")
                        else []
                    )
                    if sent:
                        limits.append(
                            max(parse_time(r["at"]) for r in sent)
                            + timedelta(
                                seconds=event_snapshot.get("thresholds", {}).get("update_rate_limit_s", 900)
                            )
                        )
                    if limits and utcnow() < max(limits):
                        return DeliveryDeferred(iso(max(limits)))
            if not notice:
                for dec_id in record.get("decisions", []):
                    dec = await self.repo.get("ana.decisions", dec_id)
                    if not dec or (
                        dec["action"] == "START"
                        and not dec.get("tracking_ended")
                        and dec.get("actionable") is not False
                        and (dec.get("entry_ref_price") is None or dec.get("invalidation_price") is None)
                    ):
                        raise ValueError(
                            "Delivery requires complete frozen decisions and START reference prices"
                        )
            chat = self.settings.telegram_ops_chat if notice else self.settings.telegram_alerts_chat
            if not self.settings.telegram_token or not chat:
                raise ValueError("Telegram token and destination must be configured")
            body = {
                "chat_id": chat,
                "text": html.escape(record["text"] if notice else record["card_text"]),
                "parse_mode": "HTML",
            }
            if not notice:
                sent = sorted(sent, key=lambda r: r["at"])
                if sent:
                    body["reply_parameters"] = {"message_id": int(sent[0]["message_ref"])}
            transition = {**filters, "attempt": attempt, "at": iso(), "status": "GENERATED"}
            await self.repo.put("out.deliveries", new_id("delivery"), transition, immutable=True)
            try:
                response = await self.http.post(
                    f"https://api.telegram.org/bot{self.settings.telegram_token}/sendMessage", json=body
                )
                data = response.json()
                if response.status_code != 200 or not data.get("ok"):
                    delay = max(2 ** (attempt - 1), data.get("parameters", {}).get("retry_after", 0))
                    raise DeliveryError(
                        f"Telegram API rejected delivery (HTTP {response.status_code})", delay
                    )
                message_ref = str(data["result"]["message_id"])
            except Exception as exc:
                await self.repo.put(
                    "out.deliveries",
                    new_id("delivery"),
                    {**transition, "at": iso(), "status": "FAILED", "error": type(exc).__name__},
                    immutable=True,
                )
                failure = DeliveryError(
                    "Telegram delivery failed", getattr(exc, "retry_after", 2 ** (attempt - 1))
                )
                failure.terminal = attempt == 5
                return failure
            async with self.repo.transaction():
                async with self.repo.lock(
                    "event:" + record["event_id"] if not notice else "notice:" + payload["notice_id"]
                ):
                    await self.repo.put(
                        "out.deliveries",
                        new_id("delivery"),
                        {**transition, "at": iso(), "status": "SENT", "message_ref": message_ref},
                        immutable=True,
                    )
                    if not notice:
                        await self._activate_calls(record)
                        event = await self.repo.get("evt.events", record["event_id"])
                        if event and not event.get("first_card_delivered_at"):
                            delivered = utcnow()
                            event.update(
                                first_card_delivered_at=iso(delivered),
                                first_card_latency_s=(
                                    delivered - parse_time(event["known_to_us_at"])
                                ).total_seconds()
                                if event.get("known_to_us_at")
                                else None,
                            )
                            assessment = (record.get("card_struct") or {}).get("assessment") or {}
                            if assessment.get("class") == "STRUCTURAL_SHOCK" and event.get("known_to_us_at"):
                                event.setdefault(
                                    "first_card_deadline",
                                    iso(parse_time(event["known_to_us_at"]) + timedelta(minutes=3)),
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
                                {**event, "lifecycle": "ALERTED", "alerted_at": iso()},
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
                                        "session_finalized_at": iso(),
                                        "terminal_alert_id": record["alert_id"],
                                        "terminal_alert_version": record["version"],
                                    },
                                )
                    if not notice and record.get("milestone_resolution_id"):
                        event = await self.repo.get("evt.events", record["event_id"])
                        if (
                            event
                            and event.get("milestone_resolution_id") == record["milestone_resolution_id"]
                        ):
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
                                    "last_expiry_notice_at": iso(),
                                },
                            )

    async def _activate_calls(self, alert):
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
                        "opened_at": iso(),
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
                        {**call, "status": "CLOSED", "closed_by": "EXIT", "closed_at": iso()},
                    )
