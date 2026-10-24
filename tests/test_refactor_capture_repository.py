from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.market.capture_contracts import NewBackfillRequest, NewRepairRequest, RepairPending, RepairRunning
from tank.market.capture_repository import CaptureRepository
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp


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
