"""Actual pending capture jobs cannot confer successful completion on another change."""

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Literal, Never

import pytest
from pydantic import JsonValue
from runtime_public_fixtures import public_runtime
from test_storage import repo as repo

from tank.contracts.jobs import JobEnvelope
from tank.contracts.scheduler_commands import InitialReactionCommand
from tank.delivery import DeliveryDeferred
from tank.job_repository import JobRepository
from tank.runtime_dispatch_contracts import ClaimedWork
from tank.runtime_reaction import ReactionCoalescingDeferred, ReactionJobInvocation
from tank.scheduler import CAPTURE_EXPIRY_S, REACTION_RETRY_S
from tank.storage import StorageBackend
from tank.time_precision import collection_clock, offset_schedule
from tank.util import iso, utcnow


def revised_payload(change_id: str) -> dict[str, JsonValue]:
    return {
        "event_id": "event",
        "token_id": "test:T",
        "checkpoint": "PRE_1H",
        "automatic_revision": True,
        "change_id": change_id,
        "reason": "Changed captured data: " + change_id,
    }


async def test_legal_queue_metadata_does_not_invalidate_a_pending_coalescing_target(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    jobs = JobRepository(repo)
    current = await jobs.enqueue(
        "REACTION_CAPTURE",
        revised_payload("older"),
        "reaction-late:older",
        priority=2,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    claimed = await jobs.claim()
    assert isinstance(claimed, JobEnvelope) and claimed.job_id == current.job_id
    peer = await jobs.enqueue(
        "REACTION_CAPTURE",
        revised_payload("newer") | {"_depends_on": []},
        "reaction-late:newer",
        priority=1,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        with pytest.raises(ReactionCoalescingDeferred) as deferred:
            await runtime.reaction_capture_wire(ReactionJobInvocation.from_envelope(claimed))
        assert deferred.value.pending.coalesced_into == peer.job_id
        unchanged = await jobs.get(current.job_id)
        assert (
            unchanged is not None
            and unchanged.status == "RUNNING"
            and "result" not in unchanged.model_fields_set
        )
        assert not router.calls
    assert await repo.list("out.reaction_checkpoints") == []
    assert await repo.list("ana.market_truth") == []


@pytest.mark.parametrize("failure", ["FAILED", "DEFERRED"])
async def test_pending_or_failed_other_capture_cannot_make_original_change_done(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: Literal["FAILED", "DEFERRED"],
) -> None:
    initial: dict[str, JsonValue] = {
        "event_id": "event",
        "token_id": "test:T",
        "checkpoint": "PRE_1H",
        "revision": 1,
        "data_fingerprint": "a" * 64,
        "retro": True,
        **collection_clock(utcnow() - timedelta(hours=1)).fields("captured_at"),
        "anchor": {
            "type": "T1",
            "ts": iso(utcnow() - timedelta(hours=2)),
            "ts_prec": "EXACT",
            "ts_prec_reason": None,
        },
    }
    await repo.put("out.reaction_checkpoints", "event:test:T:PRE_1H:1", initial, immutable=True)
    jobs = JobRepository(repo)
    older_payload = revised_payload("older")
    older = await jobs.enqueue(
        "REACTION_CAPTURE",
        older_payload,
        "reaction-late:older",
        priority=2,
        max_attempts=1,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    first_claimed, release_first, measured_peer = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        stop = asyncio.Event()
        measured: list[str | None] = []
        execute = runtime.execute_work_wire

        async def staged(work: ClaimedWork) -> JsonValue:
            if work.job.job_id == older.job_id:
                first_claimed.set()
                await release_first.wait()
            return await execute(work)

        async def unavailable(request: InitialReactionCommand) -> Never:
            measured.append(request.reason)
            measured_peer.set()
            stop.set()
            if request.reason != "Changed captured data: newer":
                raise AssertionError("The pending original must defer before starting capture")
            if failure == "DEFERRED":
                raise DeliveryDeferred(
                    offset_schedule(collection_clock(), timedelta(seconds=REACTION_RETRY_S))
                )
            raise RuntimeError("Synthetic captured-data reader is unavailable")

        monkeypatch.setattr(runtime, "execute_work_wire", staged)
        monkeypatch.setattr(runtime.scheduler, "_measure_reaction", unavailable)
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            await asyncio.wait_for(first_claimed.wait(), 2)
            newer = await jobs.enqueue(
                "REACTION_CAPTURE",
                revised_payload("newer"),
                "reaction-late:newer",
                priority=1,
                max_attempts=1,
                expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
            )
            release_first.set()
            await asyncio.wait_for(measured_peer.wait(), 2)
            await asyncio.wait_for(worker, 2)
        finally:
            release_first.set()
            stop.set()
            if not worker.done():
                worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        later = await jobs.get(newer.job_id)
        assert later is not None and measured == ["Changed captured data: newer"]
        assert later.status == ("FAILED" if failure == "FAILED" else "QUEUED")
        assert not router.calls
    original = await jobs.get(older.job_id)
    assert original is not None and original.status == "QUEUED" and original.attempts == 0
    assert "result" not in original.model_fields_set
    duplicate = await jobs.enqueue(
        "REACTION_CAPTURE",
        older_payload,
        "reaction-late:older",
        priority=2,
        max_attempts=1,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    assert duplicate.job_id == older.job_id and duplicate.status == "QUEUED"
    assert await repo.get("out.reaction_checkpoints", "event:test:T:PRE_1H:1") == initial
    assert len(await repo.list("out.reaction_checkpoints")) == 1
    for change_id in ("older", "newer"):
        assert await repo.get("ops.scheduler_checkpoints", "reaction-change:" + change_id) is None
        assert await repo.get("ops.scheduler_checkpoints", "reaction-change-owner:" + change_id) is None
