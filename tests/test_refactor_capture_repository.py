from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.market.capture_contracts import (
    CapturePolicy,
    NewBackfillRequest,
    NewRepairRequest,
    RepairPending,
    RepairRunning,
    TokenEscalation,
)
from tank.market.capture_lifecycle import CaptureLifecycle
from tank.market.capture_repository import CaptureRepository
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp
from tank.util import parse_time


def observed(value: str = "2026-10-25T00:00:00Z") -> Timestamp:
    return Timestamp(value, "COLLECTION_ONLY", None)


def repository(store: MemoryRepository) -> CaptureRepository:
    return CaptureRepository(store, config_version="cfg-test", code_version="code-test", secrets=lambda: ())


@pytest.mark.asyncio
async def test_backfill_queue_clock_does_not_claim_worker_started() -> None:
    store = MemoryRepository()
    repo = repository(store)
    result = await repo.request_backfill(
        NewBackfillRequest(
            backfill_id="BF-test",
            event_id="EVT-test",
            token_id="TOKEN-test",
            anchor=Timestamp("2026-10-24T00:00:00Z", "EXACT", None),
            requested_at=observed(),
        )
    )
    assert result.status == "PENDING"
    assert result.started_at is None
    assert result.started_at_prec_reason == "PENDING"
    assert result.recorded_at == observed().value
    assert await repo.backfill("BF-test") == result
    assert await repo.backfill("BF-missing") is None


@pytest.mark.asyncio
async def test_claimed_invalid_budget_remains_evidence_until_worker_rejection() -> None:
    store = MemoryRepository()
    repo = repository(store)
    queued = await repo.request_repair(
        NewRepairRequest(
            request_id="REQ-test",
            token_id="TOKEN-test",
            start=observed(),
            end=observed("2026-10-25T00:01:00Z"),
            requested_at=observed(),
            deadline=observed("2026-10-25T00:00:20Z"),
            budget_s=Decimal("20"),
        )
    )
    assert isinstance(queued, RepairPending)
    assert queued.budget_s == "20"
    wire = await store.get("mkt.capture_requests", "REQ-test")
    assert wire is not None
    wire.update(
        status="RUNNING",
        budget_s="NaN",
        started_at="2026-10-25T00:00:01Z",
        started_at_prec="COLLECTION_ONLY",
        started_at_prec_reason=None,
        writer="go-peer",
    )
    await store.put("mkt.capture_requests", "REQ-test", wire)
    claimed = await repo.repair("REQ-test")
    assert isinstance(claimed, RepairRunning)
    assert claimed.budget_s == "NaN"
    assert claimed.started_at == "2026-10-25T00:00:01Z"
    with pytest.raises(ValidationError):
        NewRepairRequest(
            request_id="REQ-invalid",
            token_id="TOKEN-test",
            start=observed(),
            end=observed("2026-10-25T00:01:00Z"),
            requested_at=observed(),
            deadline=observed("2026-10-25T00:00:20Z"),
            budget_s=Decimal("NaN"),
        )


@pytest.mark.asyncio
async def test_armed_watch_without_expiry_is_rejected_instead_of_protected_forever() -> None:
    store = MemoryRepository()
    await store.put(
        "ana.watch_conditions",
        "COND-test",
        {
            "cond_id": "COND-test",
            "event_id": "EVT-test",
            "token_id": "TOKEN-test",
            "status": "ARMED",
        },
    )
    with pytest.raises(ValidationError):
        await repository(store).watch_expiries("EVT-test", "TOKEN-test")


class TestClock:
    __test__ = False

    def __init__(self) -> None:
        self.elapsed = 0.0
        self.sequence = 0

    def now(self) -> datetime:
        return parse_time("2026-10-25T00:00:00Z") + timedelta(seconds=self.elapsed)

    def monotonic(self) -> float:
        return self.elapsed

    async def sleep(self, delay_s: float) -> None:
        self.elapsed += delay_s

    def identity(self, prefix: str) -> str:
        self.sequence += 1
        return prefix + "-test-" + str(self.sequence)


def lifecycle(store: MemoryRepository, clock: TestClock) -> CaptureLifecycle:
    return CaptureLifecycle(
        repository(store),
        CapturePolicy(
            thresholds=Settings().thresholds(),
            replay=False,
            request_budget_s=Decimal("1"),
            request_grace_s=Decimal("0"),
            hot_ttl_hours=72,
            config_version="cfg-test",
            code_version="code-test",
        ),
        clock,
        frozenset({"TOKEN-test"}),
    )


@pytest.mark.asyncio
async def test_bounded_wait_uses_current_anchor_not_newer_unrelated_run() -> None:
    store = MemoryRepository()
    await store.put(
        "evt.events",
        "EVT-test",
        {
            "event_id": "EVT-test",
            "known_to_us_at": "2026-10-24T00:00:00Z",
            "known_to_us_at_prec": "COLLECTION_ONLY",
            "known_to_us_at_prec_reason": None,
        },
    )
    app = lifecycle(store, TestClock())
    anchor = Timestamp("2026-10-24T00:00:00Z", "COLLECTION_ONLY", None)
    queued = await app.schedule_backfill("EVT-test", "TOKEN-test", anchor)
    newer = await app.schedule_backfill("EVT-test", "TOKEN-test", observed())
    assert queued.backfill_id != newer.backfill_id
    readiness = await app.readiness("EVT-test", "TOKEN-test")
    assert readiness.backfill_id == queued.backfill_id
    waited = await app.wait_capture("EVT-test", "TOKEN-test", timeout_s=0.5)
    assert waited.status == "INCOMPLETE"
    assert waited.backfill_id == queued.backfill_id
    assert waited.bounded_wait_s == 0.5
    persisted = await app.repository.backfill(queued.backfill_id)
    assert persisted is not None
    assert persisted.status == "PENDING"


@pytest.mark.asyncio
async def test_repair_timeout_is_distinct_from_complete_empty_instrument_result() -> None:
    store = MemoryRepository()
    app = lifecycle(store, TestClock())
    result = await app.repair_windows("TOKEN-test", observed(), observed("2026-10-25T00:01:00Z"))
    assert result.status == "INCOMPLETE"
    assert result.reason == "BOUNDED_CAPTURE_REQUEST_WAIT_EXPIRED"
    assert isinstance(await app.repository.repair(result.request_id), RepairPending)


@pytest.mark.asyncio
async def test_escalation_and_demotion_append_real_tier_history_and_keep_obligation() -> None:
    store = MemoryRepository()
    await store.put(
        "evt.events",
        "EVT-test",
        {
            "event_id": "EVT-test",
            "known_to_us_at": "2026-10-24T00:00:00Z",
            "known_to_us_at_prec": "COLLECTION_ONLY",
            "known_to_us_at_prec_reason": None,
        },
    )
    app = lifecycle(store, TestClock())
    await app.escalate("EVT-test", [TokenEscalation(token_id="TOKEN-test")])
    hot = await app.repository.hot_tokens()
    assert len(hot) == 1
    assert hot[0].priority == 1
    assert hot[0].since == "2026-10-24T00:00:00Z"
    assert hot[0].expires_at is not None
    assert parse_time(hot[0].expires_at) == parse_time("2026-10-27T00:00:00Z")
    obligations = await app.repository.backfills("TOKEN-test", "EVT-test")
    assert len(obligations) == 1
    assert obligations[0].status == "PENDING"
    await app.escalate("EVT-test", [TokenEscalation(token_id="TOKEN-test")])
    assert len(await app.repository.tier_history("TOKEN-test")) == 1
    await app.demote("TOKEN-test", "EVT-test")
    assert await app.repository.hot_tokens() == []
    history = sorted(await app.repository.tier_history("TOKEN-test"), key=lambda row: row.transition_seq)
    assert [row.tier for row in history] == ["HOT", "BASE"]
    assert [row.transition_seq for row in history] == [1, 2]
    assert len(await app.repository.backfills("TOKEN-test", "EVT-test")) == 1
