import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tank.analysis.truth_contracts import MarketTruthAssessmentRef
from tank.market.capture_contracts import CaptureReadiness
from tank.runtime import Runtime
from tank.runtime_contracts import MarketTruthJob
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp, chosen_boundary
from tank.util import utcnow


@pytest.mark.asyncio
async def test_job_expiring_after_real_claim_records_actual_finish_precision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
async def test_market_job_preserves_its_declared_cutoff_precision(precision) -> None:
    repo = MemoryRepository()
    await repo.put(
        "evt.events",
        "event",
        {"event_id": "event", "public_at": "2026-10-18T12:00:00Z", "public_at_prec": "EXACT"},
    )
    cutoff = Timestamp("2026-10-18T13:00:00Z", precision, None if precision else "UNVERIFIED_SEMANTICS")
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.market = SimpleNamespace(
        capture_readiness=AsyncMock(
            return_value=CaptureReadiness(
                event_id="event",
                token_id="token",
                status="COMPLETE",
                reason=None,
                backfill_id="capture",
                anchor="2026-10-18T12:00:00Z",
            )
        ),
        repair_windows=AsyncMock(),
    )
    reference = MarketTruthAssessmentRef.model_validate(
        {"mta_id": "actual-result", "event_id": "event", "token_id": "token", **cutoff.fields("cutoff")}
    )
    runtime.truth = SimpleNamespace(assess_for_job=AsyncMock(return_value=reference))
    assert (
        await runtime.assess(
            MarketTruthJob.model_validate(
                {"event_id": "event", "token_id": "token", **cutoff.fields("cutoff")}
            )
        )
        == reference
    )
    runtime.truth.assess_for_job.assert_awaited_once_with("event", "token", cutoff)


@pytest.mark.asyncio
async def test_real_admission_deferral_replaces_clock_facets_and_copies_creation_precision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx
    from agent_source_fixtures import declared_versions_fixture
    from pydantic import JsonValue
    from test_session_admission import settings

    from tank.agents.sessions import SessionService
    from tank.time_precision import propagate_timestamp

    repo = MemoryRepository()
    opts = settings(cap=1)
    await repo.put(
        "ops.agent_sessions",
        "busy",
        {
            "session_id": "busy",
            "event_id": "other-event",
            "status": "OPEN",
            "runtime_status": "RUNNING",
        },
    )
    previous = Timestamp((utcnow() - timedelta(minutes=1)).isoformat(), "EXACT")
    job = await repo.enqueue(
        "AGENT_MESSAGE",
        {"kind": "NEW_ITEM", "item_id": "new-source"},
        "defer-clock",
        run_after=previous,
        expires_at=utcnow() + timedelta(hours=1),
    )
    job_id = job["job_id"]
    assert isinstance(job_id, str)
    job.update(run_after_precision="EXACT", run_after_precision_reason=None, run_after_reason=None)
    await repo.put("ops.jobs", job_id, job)
    stop = asyncio.Event()

    def no_native_request(request: httpx.Request) -> httpx.Response:
        raise AssertionError("An admission deferral cannot send native requests")

    async def no_dispatch(payload: dict[str, JsonValue]) -> None:
        raise AssertionError("A deferred job cannot dispatch its payload")

    async with httpx.AsyncClient(transport=httpx.MockTransport(no_native_request)) as http:
        sessions = SessionService(
            repo, opts, None, http=http, stream_http=http, versions=declared_versions_fixture()
        )
        admit = sessions.admit

        async def admit_then_stop(payload: dict[str, JsonValue]) -> None:
            try:
                await admit(payload)
            finally:
                stop.set()

        monkeypatch.setattr(sessions, "admit", admit_then_stop)
        runtime = object.__new__(Runtime)
        runtime.repo = repo
        runtime.settings = opts
        runtime.sessions = sessions
        runtime.handlers = {"AGENT_MESSAGE": no_dispatch}
        await runtime.worker(stop)
    deferred = await repo.get("ops.jobs", job_id)
    assert deferred is not None
    assert deferred["status"] == "QUEUED" and deferred["attempts"] == 0
    assert deferred["payload"] == job["payload"] and deferred["expires_at"] == job["expires_at"]
    assert propagate_timestamp(deferred, "run_after").precision == "COLLECTION_ONLY"
    assert "run_after_precision" not in deferred and "run_after_reason" not in deferred
    assert propagate_timestamp(deferred, "slot_waiting_since") == propagate_timestamp(job, "created_at")


@pytest.mark.asyncio
async def test_runtime_prepare_refuses_missing_admission_capacity_before_gateway_effects() -> None:
    import httpx
    from agent_source_fixtures import declared_versions_fixture
    from pydantic import JsonValue, ValidationError
    from test_session_admission import AdmissionSettings

    from tank.agents.sessions import SessionService

    class MissingCapacitySettings(AdmissionSettings):
        def load_config(self, name: str) -> dict[str, JsonValue]:
            configuration = super().load_config(name)
            if name == "openma":
                del configuration["max_running_sessions"]
            return configuration

    effects: list[str] = []

    class Gateway:
        async def preflight(self) -> None:
            effects.append("preflight")
            raise AssertionError("Missing capacity cannot reach gateway preflight")

        async def start(self) -> None:
            effects.append("start")

        async def close(self) -> None:
            effects.append("close")

    def no_native_request(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Capacity preflight cannot send native requests")

    opts = MissingCapacitySettings(
        admission_cap=1, openma_api_key="synthetic-prepare-key", ops_progress_enabled=False
    )
    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(no_native_request)) as client:
        runtime = object.__new__(Runtime)
        runtime.repo = repo
        runtime.settings = opts
        runtime.sessions = SessionService(
            repo, opts, None, http=client, stream_http=client, versions=declared_versions_fixture()
        )
        runtime.gateway = Gateway()
        runtime._validate = lambda: None
        with pytest.raises(ValidationError, match="max_running_sessions"):
            await runtime.prepare()
    assert effects == []
