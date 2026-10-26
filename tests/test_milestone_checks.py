"""Actual milestone producer, worker and recorded timer contracts."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import respx
from agent_source_fixtures import source_item_fixture
from pydantic import JsonValue, ValidationError
from runtime_public_fixtures import public_runtime
from test_runtime_condition_evaluation import finished_attempt
from test_storage import repo as repo

from tank.contracts.milestone_checks import MilestoneCheckCommand, MilestoneKind, MilestoneResolutionHistory
from tank.job_repository import JobRepository
from tank.replay import replay_milestone_timer
from tank.runtime import Runtime
from tank.sources.raw_store import write_raw_item
from tank.storage import StorageBackend
from tank.time_precision import chosen_boundary, collection_clock, offset_schedule
from tank.util import canonical_json, iso, new_id, sha256, utcnow


async def seed_unverified_event(
    runtime: Runtime, repo: StorageBackend, observed: datetime
) -> dict[str, JsonValue]:
    published = iso(observed - timedelta(seconds=1))
    statement = "Unverified social lead about a scheduled vote"
    payload = canonical_json({"text": statement, "published_at": published}).encode()
    raw_ref, raw_sha256 = write_raw_item(
        runtime.settings.data_dir, "S29", "milestone-context", observed, payload
    )
    versions = runtime.settings.require_versions()
    item = source_item_fixture(
        "milestone-context",
        source_id="S29",
        source="Synthetic social post",
        author="synthetic-reader",
        text=statement,
        source_class="SOCIAL_LEAD",
        observed_at=iso(observed),
        published_at=published,
        published_prec="EXACT",
        raw_ref=raw_ref,
        raw_sha256=raw_sha256,
        content_hash=sha256(statement),
        config_version=versions.config_version,
        code_version=versions.code_version,
    )
    await repo.put("ingest.source_items", item.item_id, item.storage_record())
    created = await runtime.sessions.tool_dispatch(
        "record_event",
        {
            "item_id": item.item_id,
            "is_event": True,
            "statement": statement,
            "certainty": "LOW",
            "certainty_reason": "Unverified synthetic social lead; no accepted outcome evidence",
            "entities": [],
            "token_ids": [],
            "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
            "public_at": {
                "value": published,
                "precision": "EXACT",
                "basis": "Synthetic social post publication metadata",
                "quote": None,
            },
            "timing_confidence": "LOW",
            "freshness": "FRESH",
            "claims": [],
            "next_milestone": None,
            "checked_sources": [],
        },
        {},
    )
    assert isinstance(created, dict) and isinstance(created["event_id"], str)
    event = await repo.get("evt.events", created["event_id"])
    assert event is not None
    assert event["event_state"] == "RUMOR" and event["state_verified"] is False
    assert event["certainty"] == "LOW" and event["best_source_class"] == "SOCIAL_LEAD"
    assert event["public_at"] == published and event["known_to_us_at"] == iso(observed)
    assert await repo.list("ops.jobs") == []
    return event


@dataclass(frozen=True)
class ActualMilestoneContext:
    runtime: Runtime
    router: respx.MockRouter
    event_id: str
    event: dict[str, JsonValue]


@asynccontextmanager
async def milestone_runtime(
    repo: StorageBackend,
    directory: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    observed_at: datetime | None = None,
) -> AsyncIterator[ActualMilestoneContext]:
    async with public_runtime(repo, directory, monkeypatch) as (runtime, router):
        event = await seed_unverified_event(runtime, repo, utcnow() if observed_at is None else observed_at)
        event_id = event["event_id"]
        assert isinstance(event_id, str)
        yield ActualMilestoneContext(runtime=runtime, router=router, event_id=event_id, event=event)


async def test_unverified_milestone_context_uses_actual_source_and_event_producers(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        event = await seed_unverified_event(runtime, repo, utcnow())
        assert event["next_milestone"] is None
        assert not router.calls


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
    due = utcnow() - timedelta(minutes=5)
    milestone: dict[str, JsonValue] = {
        "kind": "VOTE",
        "source_id": "S01",
        **chosen_boundary(due, precision="EXACT").fields("due_at"),
    }
    async with milestone_runtime(
        repo, tmp_path, monkeypatch, observed_at=due - timedelta(minutes=1)
    ) as context:
        runtime, event_id = context.runtime, context.event_id
        event = context.event | {"next_milestone": milestone}
        await repo.put("evt.events", event_id, event)
        await runtime.scheduler.tick(utcnow())
        queued = await repo.list("ops.jobs", filters={"type": "MILESTONE_CHECK"})
        assert len(queued) == 1
        job = queued[0]
        assert isinstance(job["job_id"], str) and isinstance(job["payload"], dict)
        assert job["payload"]["due_at"] == iso(due)
        if resolved:
            event = event | {"next_milestone": None, "milestone_resolutions": [resolution(iso(due))]}
            await repo.put("evt.events", event_id, event)
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
        assert observed[0].event_id == event_id and not context.router.calls
    messages = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(messages) == 1
    payload = messages[0]["payload"]
    assert isinstance(payload, dict) and payload["milestone"] == "VOTE"
    assert payload["event_id"] == event_id and payload["verification_required"] is not resolved
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
    job = await JobRepository(repo).enqueue(
        "MILESTONE_CHECK",
        payload,
        "invalid-milestone",
        max_attempts=1,
        expires_at=offset_schedule(collection_clock(), timedelta(hours=2)),
    )
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
    async with milestone_runtime(repo, tmp_path, monkeypatch) as context:
        event_id = context.event_id
        await repo.put("evt.events", event_id, context.event | {"milestone_resolutions": rows})
        message: dict[str, JsonValue] = {"event_id": event_id, "milestone": "VOTE"}
        await replay_milestone_timer(context.runtime.scheduler, message, collection_clock())
        assert not context.router.calls
    messages = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(messages) == 1
    payload = messages[0]["payload"]
    assert isinstance(payload, dict) and payload["outcome"] == "Rejected"
    assert payload["event_id"] == event_id
    assert "due_at" not in MilestoneCheckCommand.from_notification(message).model_fields_set


async def test_recorded_missed_snapshot_runs_actual_expiry_at_original_trigger(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    due = utcnow() - timedelta(hours=3)
    trigger = collection_clock(due + timedelta(hours=2))
    milestone: dict[str, JsonValue] = {
        "kind": "VOTE",
        "source_id": "S01",
        **chosen_boundary(due, precision="EXACT").fields("due_at"),
    }
    missed = milestone | {"status": "MISSED", **trigger.fields("missed_at")}
    async with milestone_runtime(
        repo, tmp_path, monkeypatch, observed_at=due - timedelta(minutes=1)
    ) as context:
        event_id = context.event_id
        await repo.put("evt.events", event_id, context.event | {"next_milestone": milestone})
        await replay_milestone_timer(
            context.runtime.scheduler, {"event_id": event_id, "milestone": missed}, trigger
        )
        assert not context.router.calls
    current = await repo.get("evt.events", event_id)
    assert current is not None and current["next_milestone"] == missed
    messages = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(messages) == 1
    payload = messages[0]["payload"]
    assert isinstance(payload, dict) and payload["milestone"] == missed
    assert payload["event_id"] == event_id and payload["verification_required"] is True
    assert "outcome" not in payload


@pytest.mark.parametrize("fault", ["foreign-event", "null-history", "missing-quote"])
async def test_actual_core_refuses_unattested_resolution_before_message(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    async with milestone_runtime(repo, tmp_path, monkeypatch) as context:
        event_id = new_id("evt") if fault == "foreign-event" else context.event_id
        row = dict(context.event)
        if fault == "foreign-event":
            row["event_id"] = new_id("evt")
        elif fault == "null-history":
            row["milestone_resolutions"] = None
        else:
            accepted = resolution("2026-10-26T12:00:00Z")
            del accepted["quotes"]
            row["milestone_resolutions"] = [accepted]
        await repo.put("evt.events", event_id, row)
        with pytest.raises((ValueError, ValidationError)):
            await context.runtime.scheduler.milestone_check(
                MilestoneCheckCommand(event_id=event_id, milestone="VOTE")
            )
        assert not context.router.calls
        if fault == "foreign-event":
            assert await repo.get("evt.events", context.event_id) == context.event
    assert await repo.get("evt.events", event_id) == row
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


@pytest.mark.parametrize("difference", ["kind", "deadline"])
async def test_recorded_missed_timer_cannot_expire_a_successor_before_divergence(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, difference: str
) -> None:
    due = utcnow() - timedelta(hours=3)
    original: dict[str, JsonValue] = {
        "kind": "VOTE",
        "source_id": "S01",
        **chosen_boundary(due, precision="EXACT").fields("due_at"),
    }
    successor = original | (
        {"kind": "SUCCESSOR"}
        if difference == "kind"
        else chosen_boundary(due + timedelta(minutes=5), precision="EXACT").fields("due_at")
    )
    async with milestone_runtime(
        repo, tmp_path, monkeypatch, observed_at=due - timedelta(minutes=1)
    ) as context:
        event_id = context.event_id
        event = context.event | {"next_milestone": successor}
        await repo.put("evt.events", event_id, event)
        with pytest.raises(ValueError, match="expected timer"):
            await replay_milestone_timer(
                context.runtime.scheduler,
                {"event_id": event_id, "milestone": original | {"status": "MISSED"}},
                collection_clock(),
            )
        assert not context.router.calls
    assert await repo.get("evt.events", event_id) == event
    assert await repo.list("ops.jobs") == []


async def test_expiry_rechecks_expected_timer_after_waiting_for_event_lock(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    due = utcnow() - timedelta(hours=3)
    original: dict[str, JsonValue] = {
        "kind": "VOTE",
        **chosen_boundary(due, precision="EXACT").fields("due_at"),
    }
    entered = asyncio.Event()
    async with milestone_runtime(
        repo, tmp_path, monkeypatch, observed_at=due - timedelta(minutes=1)
    ) as context:
        event_id = context.event_id
        event = context.event | {"next_milestone": original}
        await repo.put("evt.events", event_id, event)

        async def expire() -> None:
            entered.set()
            await replay_milestone_timer(
                context.runtime.scheduler,
                {"event_id": event_id, "milestone": original | {"status": "MISSED"}},
                collection_clock(),
            )

        async with repo.lock("event:" + event_id):
            task = asyncio.create_task(expire())
            await entered.wait()
            await asyncio.sleep(0)
            assert not task.done()
            successor = event | {"next_milestone": original | {"kind": "SUCCESSOR"}}
            await repo.put("evt.events", event_id, successor)
        with pytest.raises(ValueError, match="expected timer"):
            await task
        assert not context.router.calls
    assert await repo.get("evt.events", event_id) == successor
    assert await repo.list("ops.jobs") == []


async def test_actual_tool_schedule_queue_worker_preserves_declared_clock_and_escalation(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    due_stamp = chosen_boundary(utcnow() - timedelta(minutes=5), precision="BUCKET(1m)")
    async with milestone_runtime(repo, tmp_path, monkeypatch) as context:
        runtime, event_id = context.runtime, context.event_id
        result = await runtime.sessions.tool_dispatch(
            "schedule_milestone",
            {
                "event_id": event_id,
                "milestone": "VOTE",
                "source_id": "S01",
                "due_at": due_stamp.value,
                "due_at_prec": due_stamp.precision,
            },
            {},
        )
        assert isinstance(result, dict) and isinstance(result["job_id"], str)
        job_id = result["job_id"]
        job = await repo.get("ops.jobs", job_id)
        assert job is not None and isinstance(job["payload"], dict)
        assert all(job["payload"][field] == value for field, value in due_stamp.fields("due_at").items())
        stop = asyncio.Event()
        check = runtime.scheduler.milestone_check

        async def stopping(command: MilestoneCheckCommand) -> None:
            await check(command)
            stop.set()

        monkeypatch.setattr(runtime.scheduler, "milestone_check", stopping)
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            final = await finished_attempt(repo, job_id)
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
        assert final["status"] == "DONE" and final["result"] is None
        assert not context.router.calls
    escalations = await repo.list("ops.source_escalations")
    assert len(escalations) == 1
    assert (
        escalations[0]["event_id"] == event_id
        and escalations[0]["source_id"] == "S01"
        and escalations[0]["interval_s"] == 15
    )
    assert escalations[0]["starts_at_prec"] == escalations[0]["expires_at_prec"] == "BUCKET(1m)"
    messages = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(messages) == 1
    payload = messages[0]["payload"]
    assert (
        isinstance(payload, dict)
        and payload["verification_required"] is True
        and payload["event_id"] == event_id
    )


@pytest.mark.parametrize("fault", ["foreign-owner", "blank-name", "null-name"])
async def test_actual_tool_schedule_validates_before_event_or_queue_effects(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    async with milestone_runtime(repo, tmp_path, monkeypatch) as context:
        event_id = new_id("evt") if fault == "foreign-owner" else context.event_id
        event = context.event | ({"event_id": new_id("evt")} if fault == "foreign-owner" else {})
        await repo.put("evt.events", event_id, event)
        name: JsonValue = "VOTE" if fault == "foreign-owner" else "" if fault == "blank-name" else None
        due = chosen_boundary(utcnow(), precision="EXACT")
        expected = (
            "requested identity"
            if fault == "foreign-owner"
            else "milestone"
            if fault == "blank-name"
            else "invalid type"
        )
        with pytest.raises((ValueError, ValidationError), match=expected):
            await context.runtime.sessions.tool_dispatch(
                "schedule_milestone",
                {
                    "event_id": event_id,
                    "milestone": name,
                    "source_id": "S01",
                    "due_at": due.value,
                    "due_at_prec": due.precision,
                },
                {},
            )
        assert not context.router.calls
        if fault == "foreign-owner":
            assert await repo.get("evt.events", context.event_id) == context.event
    assert await repo.get("evt.events", event_id) == event
    assert await repo.list("ops.jobs") == []
    assert await repo.list("ops.source_escalations") == []
