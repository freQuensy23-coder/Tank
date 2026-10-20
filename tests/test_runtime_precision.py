import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tank.runtime import Runtime
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp, chosen_boundary
from tank.util import utcnow


@pytest.mark.asyncio
async def test_job_expiring_after_real_claim_records_actual_finish_precision(monkeypatch):
    repo = MemoryRepository()
    stop = asyncio.Event()
    now = utcnow()
    expiry = chosen_boundary(now + timedelta(minutes=1), precision="EXACT")
    queued = await repo.enqueue("ROUTE_ITEM", {}, "expiry", expires_at=expiry)
    claim = repo.claim_job

    async def claim_then_advance_runtime_clock():
        job = await claim()
        assert job is not None and job["status"] == "RUNNING"
        monkeypatch.setattr("tank.runtime.utcnow", lambda: now + timedelta(minutes=2))
        stop.set()
        return job

    monkeypatch.setattr(repo, "claim_job", claim_then_advance_runtime_clock)
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.handlers = {"ROUTE_ITEM": AsyncMock()}
    await runtime.worker(stop)
    row = await repo.get("ops.jobs", queued["job_id"])
    assert row["status"] == "EXPIRED"
    assert row["finished_at_prec"] == "COLLECTION_ONLY"
    assert row["finished_at_prec_reason"] is None
    runtime.handlers["ROUTE_ITEM"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("precision", ["EXACT", "BUCKET(1m)", None])
async def test_market_job_preserves_its_declared_cutoff_precision(precision):
    repo = MemoryRepository()
    await repo.put(
        "evt.events",
        "event",
        {"event_id": "event", "public_at": "2026-10-18T12:00:00Z", "public_at_prec": "EXACT"},
    )
    cutoff = Timestamp("2026-10-18T13:00:00Z", precision, None if precision else "UNVERIFIED_SEMANTICS")
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.market = SimpleNamespace(capture_readiness=AsyncMock(return_value={"status": "COMPLETE"}))
    runtime.truth = SimpleNamespace(assess=AsyncMock(return_value={"mta_id": "actual-result"}))
    assert await runtime.assess({"event_id": "event", "token_id": "token", **cutoff.fields("cutoff")}) == {
        "mta_id": "actual-result"
    }
    runtime.truth.assess.assert_awaited_once_with("event", "token", cutoff)
