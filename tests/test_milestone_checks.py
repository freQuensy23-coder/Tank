"""Actual milestone producer, worker and recorded timer contracts."""

import asyncio
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import JsonValue, ValidationError
from runtime_public_fixtures import public_runtime
from test_runtime_condition_evaluation import finished_attempt
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.milestone_checks import MilestoneCheckCommand, MilestoneKind, MilestoneResolutionHistory
from tank.replay import replay_milestone_timer
from tank.storage import StorageBackend
from tank.time_precision import chosen_boundary, collection_clock
from tank.util import iso, utcnow


def resolution(due: str) -> dict[str, JsonValue]:
    return {
        "kind": "VOTE",
        "due_at": due,
        "outcome": "Passed",
        "quotes": ["Official vote passed"],
        "evidence": ["accepted-claim"],
    }


@pytest.mark.parametrize("resolved", [False, True])
async def test_actual_tick_queue_worker_emits_only_accepted_milestone_facts_and_null_result(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, resolved: bool
) -> None:
    now = utcnow()
    due = now - timedelta(minutes=5)
    milestone: dict[str, JsonValue] = {
        "kind": "VOTE",
        "source_id": "official-vote",
        **chosen_boundary(due, precision="EXACT").fields("due_at"),
    }
    event: dict[str, JsonValue] = {
        "event_id": "event",
        "lifecycle": "TRACKED",
        "next_milestone": milestone,
        **collection_clock(now).fields("known_to_us_at"),
    }
    await repo.put("evt.events", "event", event)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        await runtime.scheduler.tick(now)
        queued = await repo.list("ops.jobs", filters={"type": "MILESTONE_CHECK"})
        assert len(queued) == 1
        job = queued[0]
        assert isinstance(job["job_id"], str)
        assert isinstance(job["payload"], dict)
        assert job["payload"]["due_at"] == iso(due)
        if resolved:
            event = event | {"next_milestone": None, "milestone_resolutions": [resolution(iso(due))]}
            await repo.put("evt.events", "event", event)
        stop = asyncio.Event()
        check = runtime.scheduler.milestone_check
        observed: list[MilestoneCheckCommand] = []

        async def stopping(command: MilestoneCheckCommand) -> None:
            observed.append(command)
            await check(command)
            stop.set()

        monkeypatch.setattr(runtime.scheduler, "milestone_check", stopping)
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            final = await finished_attempt(repo, job["job_id"])
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
        assert final["status"] == "DONE" and final["result"] is None
        assert len(observed) == 1 and observed[0].due_at == iso(due)
        assert not router.calls
    messages = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(messages) == 1
    payload = messages[0]["payload"]
    assert isinstance(payload, dict) and payload["milestone"] == "VOTE"
    assert payload["verification_required"] is not resolved
    assert payload["outcome"] == ("Passed" if resolved else None)
    assert payload["quotes"] == (["Official vote passed"] if resolved else [])
    assert payload["evidence"] == (["accepted-claim"] if resolved else [])


@pytest.mark.parametrize(
    "missing", ["event_id", "milestone", "due_at", "due_at_prec", "due_at_prec_reason", "source_id"]
)
async def test_queue_missing_required_fact_fails_before_event_lookup_or_message(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    payload: dict[str, JsonValue] = {
        "event_id": "unavailable-event",
        "milestone": "VOTE",
        "source_id": "official-vote",
        **collection_clock().fields("due_at"),
    }
    del payload[missing]
    job = await JobRepository(repo).enqueue("MILESTONE_CHECK", payload, "invalid-milestone", max_attempts=1)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            final = await finished_attempt(repo, job.job_id)
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
        assert final["status"] == "FAILED" and final["last_error"] == "ValidationError"
        assert final["payload"] == payload and "result" not in final
        assert not router.calls
    assert await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"}) == []


@pytest.mark.parametrize("field", ["outcome", "quotes", "evidence", "due_at"])
def test_accepted_resolution_cannot_invent_absent_evidence_or_outcome(field: str) -> None:
    row = resolution("2026-10-26T12:00:00Z")
    del row[field]
    with pytest.raises(ValidationError):
        MilestoneResolutionHistory.from_record(
            {"event_id": "event", "milestone_resolutions": [row]},
            MilestoneCheckCommand(event_id="event", milestone="VOTE"),
        )


async def test_recorded_verification_with_unreported_due_selects_latest_real_resolution(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows: list[JsonValue] = [
        resolution("2026-10-01T00:00:00Z"),
        resolution("2026-10-02T00:00:00Z") | {"outcome": "Rejected"},
    ]
    await repo.put("evt.events", "event", {"event_id": "event", "milestone_resolutions": rows})
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        await replay_milestone_timer(
            runtime.scheduler, {"event_id": "event", "milestone": "VOTE"}, collection_clock()
        )
        assert not router.calls
    messages = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(messages) == 1
    payload = messages[0]["payload"]
    assert isinstance(payload, dict) and payload["outcome"] == "Rejected"
    assert (
        "due_at"
        not in MilestoneCheckCommand.from_notification(
            {"event_id": "event", "milestone": "VOTE"}
        ).model_fields_set
    )


async def test_recorded_missed_snapshot_runs_actual_expiry_at_original_trigger(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    due = utcnow() - timedelta(hours=3)
    trigger = collection_clock(due + timedelta(hours=2))
    milestone: dict[str, JsonValue] = {
        "kind": "VOTE",
        "source_id": "official-vote",
        **chosen_boundary(due, precision="EXACT").fields("due_at"),
    }
    await repo.put("evt.events", "event", {"event_id": "event", "next_milestone": milestone})
    missed = milestone | {"status": "MISSED", **trigger.fields("missed_at")}
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        await replay_milestone_timer(runtime.scheduler, {"event_id": "event", "milestone": missed}, trigger)
        assert not router.calls
    current = await repo.get("evt.events", "event")
    assert current is not None and current["next_milestone"] == missed
    messages = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(messages) == 1
    payload = messages[0]["payload"]
    assert isinstance(payload, dict) and payload["milestone"] == missed
    assert payload["verification_required"] is True
    assert "outcome" not in payload


@pytest.mark.parametrize("fault", ["foreign-event", "null-history", "missing-quote"])
async def test_actual_core_refuses_unattested_resolution_before_message(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    row: dict[str, JsonValue] = {"event_id": "event"}
    if fault == "foreign-event":
        row["event_id"] = "foreign"
    elif fault == "null-history":
        row["milestone_resolutions"] = None
    else:
        accepted = resolution("2026-10-26T12:00:00Z")
        del accepted["quotes"]
        row["milestone_resolutions"] = [accepted]
    await repo.put("evt.events", "event", row)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        with pytest.raises((ValueError, ValidationError)):
            await runtime.scheduler.milestone_check(MilestoneCheckCommand(event_id="event", milestone="VOTE"))
        assert not router.calls
    assert await repo.get("evt.events", "event") == row
    assert await repo.list("ops.jobs") == []


def test_resolution_alias_is_explicit_and_conflicting_names_refuse() -> None:
    row = resolution("2026-10-26T12:00:00Z")
    row["milestone"] = row.pop("kind")
    command = MilestoneCheckCommand(event_id="event", milestone="VOTE")
    history = MilestoneResolutionHistory.from_record(
        {"event_id": "event", "milestone_resolutions": [row]}, command
    )
    accepted = history.latest(command)
    assert accepted is not None and accepted.outcome == "Passed"
    row["kind"] = "different-kind"
    with pytest.raises(ValueError, match="conflict"):
        MilestoneResolutionHistory.from_record({"event_id": "event", "milestone_resolutions": [row]}, command)


@pytest.mark.parametrize("name", [None, "", " ", False, 1])
def test_milestone_kind_never_defaults_invalid_required_facts(name: JsonValue) -> None:
    with pytest.raises(ValidationError):
        MilestoneKind.from_record({"kind": name, "milestone": name})


def test_kind_alias_preserves_actual_name_and_rejects_conflict() -> None:
    assert MilestoneKind.from_record({"milestone": "VOTE"}).kind == "VOTE"
    with pytest.raises(ValueError, match="conflict"):
        MilestoneKind.from_record({"kind": "VOTE", "milestone": "HEARING"})
