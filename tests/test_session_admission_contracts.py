"""Actual queue-backed admission preserves order, leases and unrelated captured data."""

import copy
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import JsonValue, ValidationError

from tank.agents.admission_repository import (
    SessionAdmissionRepository,
    admission_candidate,
    build_admission,
)
from tank.contracts.session_admission import AdmissionDeferred
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp, collection_clock
from tank.util import canonical_json, parse_time, utcnow


def clock() -> Timestamp:
    return collection_clock(datetime(2026, 10, 25, 13, tzinfo=UTC))


def identity(row: dict[str, JsonValue]) -> str:
    value = row["job_id"]
    assert isinstance(value, str)
    return value


async def stored(repo: MemoryRepository, job_id: str) -> dict[str, JsonValue]:
    row = await repo.get("ops.jobs", job_id)
    assert row is not None
    return row


@pytest.mark.parametrize(
    "configuration",
    [
        {},
        {"max_running_sessions": True},
        {"max_running_sessions": 0},
        {"max_running_sessions": "2"},
        {"max_running_sessions": None},
    ],
)
def test_admission_factory_requires_a_declared_positive_integer_cap(
    configuration: dict[str, JsonValue],
) -> None:
    repo = MemoryRepository()
    with pytest.raises(ValidationError):
        build_admission(repo, configuration, clock, timedelta(seconds=60))


@pytest.mark.asyncio
async def test_offset_aware_wait_order_defers_newer_input_and_wakes_without_changing_attempts() -> None:
    repo = MemoryRepository()
    old = await repo.enqueue(
        "AGENT_MESSAGE",
        {"kind": "NEW_ITEM", "item_id": "old-source"},
        "old",
        run_after=utcnow() + timedelta(days=1),
        expires_at=utcnow() + timedelta(days=2),
    )
    old_id = identity(old)
    old["slot_waiting_since"] = "2026-10-25T12:30:00+02:00"
    old["retained_capture"] = {"original": ["bytes", 4, False]}
    await repo.put("ops.jobs", old_id, old)
    new = await repo.enqueue(
        "AGENT_MESSAGE",
        {"kind": "NEW_ITEM", "item_id": "new-source"},
        "new",
        expires_at=utcnow() + timedelta(days=2),
    )
    new_id = identity(new)
    claimed = await repo.claim_job()
    assert claimed is not None and identity(claimed) == new_id
    claimed["slot_waiting_since"] = "2026-10-25T11:00:00+00:00"
    await repo.put("ops.jobs", new_id, claimed)
    before_new = canonical_json(await stored(repo, new_id))
    admission = build_admission(repo, {"max_running_sessions": 1}, clock, timedelta(seconds=60))
    with pytest.raises(AdmissionDeferred) as deferred:
        await admission.admit(admission_candidate(claimed))
    assert deferred.value.not_before.value is not None
    assert parse_time(deferred.value.not_before.value) == datetime(2026, 10, 25, 13, 1, tzinfo=UTC)
    assert canonical_json(await stored(repo, new_id)) == before_new
    await admission.wake()
    awakened = await stored(repo, old_id)
    assert awakened["attempts"] == old["attempts"] == 0
    assert awakened["expires_at"] == old["expires_at"]
    assert awakened["payload"] == old["payload"]
    assert canonical_json(awakened["retained_capture"]) == canonical_json(old["retained_capture"])
    assert awakened["run_after"] == clock().value
    assert canonical_json(await stored(repo, new_id)) == before_new


@pytest.mark.asyncio
async def test_running_session_bypass_does_not_reserve_a_second_slot_or_guess_an_unknown_id() -> None:
    repo = MemoryRepository()
    await repo.put(
        "ops.agent_sessions",
        "busy",
        {
            "session_id": "busy",
            "event_id": "event",
            "status": "OPEN",
            "runtime_status": "RUNNING",
        },
    )
    new = await repo.enqueue(
        "AGENT_MESSAGE",
        {
            "kind": "ITEM_ADDED",
            "session_id": "busy",
            "event_id": "event",
            "item_id": "source",
        },
        "to-busy",
    )
    job_id = identity(new)
    claimed = await repo.claim_job()
    assert claimed is not None and identity(claimed) == job_id
    admission = build_admission(repo, {"max_running_sessions": 1}, clock, timedelta(seconds=60))
    before = canonical_json(claimed)
    await admission.admit(admission_candidate(claimed))
    assert canonical_json(await stored(repo, job_id)) == before
    stale = copy.deepcopy(claimed)
    payload = stale["payload"]
    assert isinstance(payload, dict)
    payload["session_id"] = "missing-explicit-target"
    with pytest.raises(AdmissionDeferred):
        await admission.admit(admission_candidate(stale))
    assert canonical_json(await stored(repo, job_id)) == before


@pytest.mark.asyncio
async def test_sparse_reservation_validates_identity_and_preserves_captured_job_input() -> None:
    repo = MemoryRepository()
    job = await repo.enqueue(
        "AGENT_MESSAGE",
        {"kind": "NEW_ITEM", "item_id": "source"},
        "reserve",
    )
    job_id = identity(job)
    claimed = await repo.claim_job()
    assert claimed is not None
    claimed["retained_capture"] = {"original": ["wire", None, 1, 1.0, True]}
    await repo.put("ops.jobs", job_id, claimed)
    adapter = SessionAdmissionRepository(repo)
    await adapter.reserve(job_id)
    reserved = await stored(repo, job_id)
    assert reserved["slot_admitted"] is True
    assert canonical_json(
        {key: value for key, value in reserved.items() if key != "slot_admitted"}
    ) == canonical_json(claimed)
    reserved["job_id"] = "wrong-storage-identity"
    await repo.put("ops.jobs", job_id, reserved)
    before = canonical_json(await stored(repo, job_id))
    with pytest.raises(ValueError, match="identity conflicts"):
        await adapter.reserve(job_id)
    assert canonical_json(await stored(repo, job_id)) == before


@pytest.mark.asyncio
async def test_missing_routing_identity_is_refused_before_a_slot_can_be_reserved() -> None:
    repo = MemoryRepository()
    await repo.enqueue("AGENT_MESSAGE", {"kind": "NEW_ITEM"}, "without-identity")
    claimed = await repo.claim_job()
    assert claimed is not None
    job_id = identity(claimed)
    before = canonical_json(claimed)
    with pytest.raises(ValidationError, match="routing identity"):
        admission_candidate(claimed)
    assert canonical_json(await stored(repo, job_id)) == before


@pytest.mark.asyncio
async def test_slot_counting_uses_reservation_facts_without_consuming_unrelated_payloads() -> None:
    from tank.contracts.session_admission import JobSlot

    repo = MemoryRepository()
    await repo.enqueue("AGENT_MESSAGE", {"unconsumed": "retained payload"}, "reserved")
    claimed = await repo.claim_job()
    assert claimed is not None
    job_id = identity(claimed)
    claimed["slot_admitted"] = True
    await repo.put("ops.jobs", job_id, claimed)
    queued = await repo.enqueue("AGENT_MESSAGE", {}, "not-yet-waiting")
    admission = build_admission(repo, {"max_running_sessions": 1}, clock, timedelta(seconds=60))
    slots = await admission.slots()
    assert slots.occupied == frozenset({JobSlot(job_id=job_id)})
    assert slots.waiting == ()
    assert canonical_json(await stored(repo, job_id)) == canonical_json(claimed)
    assert canonical_json(await stored(repo, identity(queued))) == canonical_json(queued)


@pytest.mark.asyncio
async def test_unsupported_native_state_cannot_be_treated_as_a_free_slot() -> None:
    repo = MemoryRepository()
    await repo.put(
        "ops.agent_sessions",
        "unknown",
        {
            "session_id": "unknown",
            "status": "OPEN",
            "runtime_status": "UNSUPPORTED_NATIVE_STATE",
        },
    )
    await repo.enqueue("AGENT_MESSAGE", {"kind": "NEW_ITEM", "item_id": "source"}, "candidate")
    claimed = await repo.claim_job()
    assert claimed is not None
    job_id = identity(claimed)
    before = canonical_json(claimed)
    admission = build_admission(repo, {"max_running_sessions": 1}, clock, timedelta(seconds=60))
    with pytest.raises(ValidationError, match="runtime_status"):
        await admission.admit(admission_candidate(claimed))
    assert canonical_json(await stored(repo, job_id)) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("bucketed", [False, True])
async def test_wake_replaces_all_owned_clock_aliases_and_preserves_other_record_bytes(
    bucketed: bool,
) -> None:
    from tank.time_precision import propagate_timestamp, timestamp_owned_fields

    repo = MemoryRepository()
    previous = Timestamp(
        (utcnow() - timedelta(days=1)).isoformat(),
        "BUCKET(1d)" if bucketed else "EXACT",
        "Original declared schedule",
        "START" if bucketed else None,
    )
    row = await repo.enqueue(
        "AGENT_MESSAGE",
        {"kind": "NEW_ITEM", "item_id": "source"},
        "wake-clock",
        run_after=previous,
    )
    job_id = identity(row)
    row.update(
        {
            "run_after_precision": previous.precision,
            "run_after_precision_reason": previous.precision_reason,
            "run_after_reason": previous.precision_reason,
            "run_after_capture_note": {"retained": [1, 1.0, True]},
        }
    )
    if bucketed:
        row["run_after_bucket_size_s"] = 86400
    await repo.put("ops.jobs", job_id, row)
    prior_value = propagate_timestamp(row, "run_after").value
    assert prior_value is not None and previous.value is not None
    assert parse_time(prior_value) == parse_time(previous.value)
    owned = timestamp_owned_fields("run_after")
    original = canonical_json({key: value for key, value in row.items() if key not in owned})
    await SessionAdmissionRepository(repo).wake(job_id, clock())
    updated = await stored(repo, job_id)
    assert propagate_timestamp(updated, "run_after") == clock()
    assert set(updated) & owned == set(clock().fields("run_after"))
    assert canonical_json({key: value for key, value in updated.items() if key not in owned}) == original
