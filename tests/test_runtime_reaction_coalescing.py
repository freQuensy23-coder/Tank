"""Actual pending capture jobs cannot confer successful completion on another change."""

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Literal, Never

import pytest
from pydantic import JsonValue
from runtime_public_fixtures import public_runtime
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.scheduler_commands import InitialReactionCommand, RevisedReactionCommand
from tank.contracts.scheduler_results import CoalescedReaction
from tank.delivery import DeliveryDeferred
from tank.scheduler import CAPTURE_EXPIRY_S
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
    job = await JobRepository(repo).enqueue(
        "REACTION_CAPTURE",
        revised_payload("newer") | {"_depends_on": []},
        "reaction-late:newer",
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        receipt = await runtime.scheduler.reaction_capture(
            RevisedReactionCommand.model_validate(revised_payload("older"))
        )
        assert isinstance(receipt, CoalescedReaction) and receipt.coalesced_into == job.job_id
        assert not router.calls
    assert await repo.list("out.reaction_checkpoints") == []


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
    newer = await jobs.enqueue(
        "REACTION_CAPTURE",
        revised_payload("newer"),
        "reaction-late:newer",
        priority=2,
        max_attempts=1,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        stop = asyncio.Event()
        measured: list[str | None] = []

        async def unavailable(request: InitialReactionCommand) -> Never:
            measured.append(request.reason)
            stop.set()
            if failure == "DEFERRED":
                raise DeliveryDeferred(offset_schedule(collection_clock(), timedelta(minutes=5)))
            raise RuntimeError("Synthetic captured-data reader is unavailable")

        monkeypatch.setattr(runtime.scheduler, "_measure_reaction", unavailable)
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            await asyncio.wait_for(stop.wait(), 2)
            await asyncio.wait_for(worker, 2)
        finally:
            stop.set()
            if not worker.done():
                await asyncio.wait_for(worker, 2)
        later = await repo.get("ops.jobs", newer.job_id)
        assert later is not None
        assert measured == ["Changed captured data: newer"]
        assert later["status"] == ("FAILED" if failure == "FAILED" else "QUEUED")
        assert not router.calls
    original = await repo.get("ops.jobs", older.job_id)
    assert original is not None
    duplicate = await jobs.enqueue(
        "REACTION_CAPTURE",
        older_payload,
        "reaction-late:older",
        priority=2,
        max_attempts=1,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    assert duplicate.job_id == older.job_id
    assert await repo.get("out.reaction_checkpoints", "event:test:T:PRE_1H:1") == initial
    assert len(await repo.list("out.reaction_checkpoints")) == 1
    for change_id in ("older", "newer"):
        assert await repo.get("ops.scheduler_checkpoints", "reaction-change:" + change_id) is None
        assert await repo.get("ops.scheduler_checkpoints", "reaction-change-owner:" + change_id) is None
    assert original["status"] != "DONE", {
        "original_status": original["status"],
        "original_result": original.get("result"),
        "other_status": later["status"],
        "idempotent_retry_status": duplicate.status,
    }
