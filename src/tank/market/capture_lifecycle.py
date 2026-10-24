"""Capture application orchestration over validated persistence and an injected clock."""

import math
from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal

from tank.market.capture_contracts import (
    BackfillRun,
    CaptureCadences,
    CapturePolicy,
    CaptureReadiness,
    CaptureTier,
    CaptureTierTransition,
    CaptureWaitOutcome,
    HotToken,
    HotTokenIntent,
    NewBackfillRequest,
    NewRepairRequest,
    RepairComplete,
    RepairCompleted,
    RepairExpired,
    RepairFailed,
    RepairNotRun,
    RepairOutcome,
    RepairUnavailable,
    TokenEscalation,
)
from tank.market.capture_ports import CaptureClock, CaptureInvariantError, CaptureRepositoryPort
from tank.market.supply import supply_cadences
from tank.time_precision import Timestamp, chosen_boundary, collection_clock
from tank.util import iso, parse_time, sha256


class CaptureLifecycle:
    def __init__(
        self,
        repository: CaptureRepositoryPort,
        policy: CapturePolicy,
        clock: CaptureClock,
        known_tokens: frozenset[str],
    ) -> None:
        self.repository = repository
        self.policy = policy
        self.clock = clock
        self.known_tokens = known_tokens
        if policy.thresholds.hot_token_cap < 0:
            raise ValueError("HOT token capacity must be nonnegative")
        self.waits: dict[str, CaptureWaitOutcome] = {}

    @staticmethod
    def backfill_identity(event_id: str, token_id: str, anchor: Timestamp) -> str:
        if anchor.value is None:
            raise ValueError("Capture obligation requires its event anchor")
        return "BF-" + sha256({"event_id": event_id, "token_id": token_id, "anchor": iso(anchor.value)})

    async def _anchor(self, event_id: str) -> Timestamp:
        event = await self.repository.get_event_context(event_id)
        if event is None:
            raise CaptureInvariantError("Capture references a missing event")
        return event.anchor.timestamp

    async def schedule_backfill(self, event_id: str, token_id: str, anchor: Timestamp) -> BackfillRun:
        backfill_id = self.backfill_identity(event_id, token_id, anchor)
        current = await self.repository.backfill(backfill_id)
        if current is not None and current.status != "FAILED":
            self._validate_scope(current, event_id, token_id, anchor)
            return current
        return await self.repository.request_backfill(
            NewBackfillRequest(
                backfill_id=backfill_id,
                event_id=event_id,
                token_id=token_id,
                anchor=anchor,
                requested_at=collection_clock(self.clock.now()),
            )
        )

    @staticmethod
    def _validate_scope(run: BackfillRun, event_id: str, token_id: str, anchor: Timestamp) -> None:
        if anchor.value is None:
            raise ValueError("Capture obligation requires its event anchor")
        if (
            run.event_id != event_id
            or run.token_id != token_id
            or run.anchor is None
            or parse_time(run.anchor) != parse_time(anchor.value)
        ):
            raise CaptureInvariantError(
                "Capture obligation differs from its requested event, token or anchor"
            )

    async def readiness(
        self, event_id: str, token_id: str, *, anchor: Timestamp | None = None
    ) -> CaptureReadiness:
        selected = await self._anchor(event_id) if anchor is None else anchor
        backfill_id = self.backfill_identity(event_id, token_id, selected)
        run = await self.repository.backfill(backfill_id)
        if run is None:
            return CaptureReadiness(
                event_id=event_id,
                token_id=token_id,
                status="NOT_REQUESTED",
                reason="NO_DURABLE_CAPTURE_REQUEST_FOR_EXACT_ANCHOR",
                backfill_id=None,
                anchor=selected.value,
            )
        self._validate_scope(run, event_id, token_id, selected)
        return CaptureReadiness(
            event_id=event_id,
            token_id=token_id,
            status=run.status,
            reason=run.reason,
            backfill_id=run.backfill_id,
            anchor=run.anchor,
        )

    async def wait_capture(self, event_id: str, token_id: str, *, timeout_s: float) -> CaptureWaitOutcome:
        if not math.isfinite(timeout_s) or timeout_s < 0:
            raise ValueError("Capture wait duration must be finite and nonnegative")
        selected = await self._anchor(event_id)
        started = self.clock.monotonic()
        while True:
            current = await self.readiness(event_id, token_id, anchor=selected)
            elapsed = self.clock.monotonic() - started
            status = current.status
            if status == "COMPLETE" or status == "PARTIAL" or status == "FAILED":
                result = CaptureWaitOutcome(
                    status=status,
                    backfill_id=current.backfill_id,
                    reason=current.reason,
                    bounded_wait_s=elapsed,
                    observed_at=collection_clock(self.clock.now()),
                )
                break
            if elapsed >= timeout_s:
                result = CaptureWaitOutcome(
                    status="INCOMPLETE",
                    backfill_id=current.backfill_id,
                    reason="BOUNDED_PREREQUISITE_WAIT_EXPIRED",
                    bounded_wait_s=elapsed,
                    observed_at=collection_clock(self.clock.now()),
                )
                break
            await self.clock.sleep(min(0.25, timeout_s - elapsed))
        if result.backfill_id is not None:
            self.waits[result.backfill_id] = result
        return result

    async def repair_windows(
        self, token_id: str, start: Timestamp, end: Timestamp, *, budget_s: Decimal | None = None
    ) -> RepairOutcome:
        if self.policy.replay:
            return RepairNotRun(status="NOT_RUN", reason="REPLAY_USES_RETAINED_MARKET_INPUTS")
        budget = self.policy.request_budget_s if budget_s is None else budget_s
        requested = self.clock.now()
        if start.value is None or end.value is None:
            raise ValueError("Repair windows require both declared bounds")
        lo = parse_time(start.value).replace(second=0, microsecond=0)
        hi = parse_time(end.value)
        request_id = "CR-" + sha256(
            {"token_id": token_id, "start": iso(lo), "end": iso(hi), "requested_at": iso(requested)}
        )
        command = NewRepairRequest(
            request_id=request_id,
            token_id=token_id,
            start=chosen_boundary(lo, precision="EXACT"),
            end=chosen_boundary(hi, precision="EXACT"),
            requested_at=collection_clock(requested),
            deadline=collection_clock(requested + timedelta(seconds=float(budget))),
            budget_s=budget,
        )
        await self.repository.request_repair(command)
        until = self.clock.monotonic() + float(budget + self.policy.request_grace_s)
        while True:
            row = await self.repository.repair(request_id)
            if row is None:
                raise CaptureInvariantError("Persisted repair request disappeared while awaiting capture")
            if isinstance(row, RepairComplete):
                return RepairCompleted(
                    status="COMPLETE", request_id=request_id, instruments=row.result.instruments
                )
            if isinstance(row, RepairFailed):
                return RepairUnavailable(status="FAILED", request_id=request_id, reason=row.reason)
            if isinstance(row, RepairExpired):
                return RepairUnavailable(status="EXPIRED", request_id=request_id, reason=row.reason)
            remaining = until - self.clock.monotonic()
            if remaining <= 0:
                return RepairUnavailable(
                    status="INCOMPLETE", request_id=request_id, reason="BOUNDED_CAPTURE_REQUEST_WAIT_EXPIRED"
                )
            await self.clock.sleep(min(0.25, remaining))

    async def hot_protected(self, event_id: str, token_id: str, now: datetime) -> bool:
        if await self.repository.has_open_call(event_id, token_id):
            return True
        expiries = await self.repository.watch_expiries(event_id, token_id)
        return any(expiry > now for expiry in expiries.values())

    async def record_tier(self, token_id: str, tier: CaptureTier, reason: str) -> None:
        thresholds = self.policy.thresholds
        cadences = CaptureCadences(
            oi_funding_s=float(
                thresholds.hot_oi_funding_interval_s
                if tier == "HOT"
                else thresholds.base_oi_funding_interval_s
            ),
            kline_s=float(thresholds.base_kline_interval_s),
            book_s=float(thresholds.book_interval_s),
            supply_by_kind=supply_cadences(tier == "HOT"),
        )
        history = await self.repository.tier_history(token_id)
        latest = (
            max(
                history, key=lambda row: (parse_time(row.effective_at), row.transition_seq, row.transition_id)
            )
            if history
            else None
        )
        if (
            latest is not None
            and latest.tier == tier
            and latest.capture_cadences == cadences
            and latest.config_version == self.policy.config_version
        ):
            return
        sequence = max((row.transition_seq for row in history), default=0) + 1
        await self.repository.append_tier(
            CaptureTierTransition(
                transition_id=self.clock.identity("TIER"),
                transition_seq=sequence,
                event_ids=sorted(row.event_id for row in await self.repository.hot_tokens(token_id)),
                token_id=token_id,
                tier=tier,
                effective_at=iso(self.clock.now()),
                effective_at_prec="COLLECTION_ONLY",
                effective_at_prec_reason=None,
                reason=reason,
                capture_cadences=cadences,
                config_version=self.policy.config_version,
                code_version=self.policy.code_version,
            )
        )

    async def escalate(self, event_id: str, tokens: Sequence[TokenEscalation]) -> None:
        async with self.repository.hot_set_lock(), self.repository.transaction():
            event = await self.repository.get_event_context(event_id)
            if event is None:
                raise CaptureInvariantError("Market escalation references a missing event")
            assessment = await self.repository.current_assessment(event)
            priorities = {"LOW": 1, "MATERIAL": 2, "MAJOR": 3, "STRUCTURAL_SHOCK": 4}
            event_priority = 1 if assessment is None else priorities[assessment.assessment_class]
            now = self.clock.now()
            for request in tokens:
                if request.token_id not in self.known_tokens:
                    raise ValueError("unknown token " + request.token_id)
                existing = next(
                    (
                        row
                        for row in await self.repository.hot_tokens(request.token_id)
                        if row.event_id == event_id
                    ),
                    None,
                )
                since = (
                    event.anchor.timestamp
                    if existing is None
                    else Timestamp(existing.since, existing.since_prec, existing.since_prec_reason)
                )
                protected = await self.hot_protected(event_id, request.token_id, now)
                expiry = (
                    None
                    if protected
                    else collection_clock(
                        parse_time(event.anchor.value) + timedelta(hours=self.policy.hot_ttl_hours)
                    )
                )
                await self.repository.put_hot(
                    HotTokenIntent(
                        event_id=event_id,
                        token_id=request.token_id,
                        priority=event_priority if request.priority is None else request.priority,
                        since=since,
                        expires_at=expiry,
                    ),
                    collection_clock(now),
                )
            grouped: dict[str, list[HotToken]] = {}
            for row in await self.repository.hot_tokens():
                grouped.setdefault(row.token_id, []).append(row)

            def rank(token_id: str) -> tuple[int, datetime]:
                rows = grouped[token_id]
                priority = max(row.priority for row in rows)
                return priority, max(parse_time(row.since) for row in rows if row.priority == priority)

            ranked = sorted(grouped, key=rank, reverse=True)
            cap = self.policy.thresholds.hot_token_cap
            retained = set(ranked[:cap])
            for token_id in ranked[cap:]:
                for row in grouped[token_id]:
                    await self.repository.delete_hot(token_id, row.event_id)
            for token_id in grouped:
                await self.record_tier(token_id, "HOT" if token_id in retained else "BASE", "ESCALATION_CAP")
        if not self.policy.replay:
            for request in tokens:
                if request.token_id in retained:
                    await self.schedule_backfill(event_id, request.token_id, event.anchor.timestamp)

    async def refresh_hot_expiry(self, now: datetime) -> list[HotToken]:
        async with self.repository.hot_set_lock(), self.repository.transaction():
            result = []
            for row in await self.repository.hot_tokens():
                anchor = await self._anchor(row.event_id)
                if anchor.value is None:
                    raise CaptureInvariantError("HOT token event has no declared anchor")
                protected = await self.hot_protected(row.event_id, row.token_id, now)
                expiry = (
                    None
                    if protected
                    else collection_clock(
                        parse_time(anchor.value) + timedelta(hours=self.policy.hot_ttl_hours)
                    )
                )
                expiry_value = None if expiry is None else expiry.value
                if row.expires_at != expiry_value:
                    row = await self.repository.put_hot(
                        HotTokenIntent(
                            event_id=row.event_id,
                            token_id=row.token_id,
                            priority=row.priority,
                            since=Timestamp(row.since, row.since_prec, row.since_prec_reason),
                            expires_at=expiry,
                        ),
                        collection_clock(now),
                    )
                result.append(row)
            return result

    async def demote(self, token_id: str, event_id: str | None) -> None:
        async with self.repository.hot_set_lock(), self.repository.transaction():
            for row in await self.repository.hot_tokens(token_id):
                if event_id is None or row.event_id == event_id:
                    await self.repository.delete_hot(token_id, row.event_id)
            remaining = await self.repository.hot_tokens(token_id)
            await self.record_tier(token_id, "HOT" if remaining else "BASE", "DEMOTION")

    async def schedule_hot_backfills(self) -> None:
        if not self.policy.replay:
            for row in await self.repository.hot_tokens():
                await self.schedule_backfill(row.event_id, row.token_id, await self._anchor(row.event_id))
