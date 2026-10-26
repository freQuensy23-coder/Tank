"""Reaction queue admission preserves source facts and refuses uncertain receipts."""

import asyncio
from pathlib import Path

import pytest
from pydantic import JsonValue, ValidationError
from runtime_public_fixtures import public_runtime
from test_runtime_condition_evaluation import finished_attempt
from test_storage import repo as repo

from tank.contracts.scheduler_commands import (
    InitialReactionCommand,
    ManualReactionRevisionCommand,
    ReactionCaptureCommand,
    RevisedReactionCommand,
)
from tank.contracts.scheduler_results import CapturedReaction, ReactionCaptureResult
from tank.job_repository import JobRepository
from tank.runtime_reaction import reaction_job_command, validate_reaction_receipt
from tank.storage import StorageBackend


def initial_payload() -> dict[str, JsonValue]:
    return {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "PRE_1H",
        "anchor": {"type": "T1", "ts": "2026-10-26T12:00:00Z"},
        "cutoff": "2026-10-26T11:00:00Z",
        "retro": True,
    }


def test_queue_projection_preserves_unreported_scheduling_and_anchor_precision() -> None:
    payload = initial_payload() | {"_depends_on": ["actual-job"]}
    command = reaction_job_command(payload)
    assert isinstance(command, InitialReactionCommand)
    assert command.model_fields_set == {"event_id", "token_id", "checkpoint", "anchor", "cutoff", "retro"}
    assert command.anchor.model_fields_set == {"type", "ts"}
    assert command.anchor.timestamp.precision is None
    assert command.anchor.timestamp.precision_reason == "INCOMPLETE"
    explicit = reaction_job_command(
        initial_payload() | {"retro": False, "cutoff_prec": None, "cutoff_prec_reason": "INCOMPLETE"}
    )
    assert isinstance(explicit, InitialReactionCommand)
    assert explicit.model_fields_set == command.model_fields_set | {"cutoff_prec", "cutoff_prec_reason"}
    assert explicit.retro is False
    assert "cutoff_prec" not in command.model_fields_set


def test_changed_job_projects_actual_identity_without_synthesizing_anchor() -> None:
    command = reaction_job_command(
        {
            "event_id": "event",
            "token_id": "token",
            "checkpoint": "PRE_1H",
            "automatic_revision": True,
            "change_id": "actual-change",
            "reason": "LATE_CAPTURE",
            "_depends_on": [],
        }
    )
    assert isinstance(command, RevisedReactionCommand)
    assert command.change_id == "actual-change"
    assert "anchor" not in type(command).model_fields


@pytest.mark.parametrize("field", ["event_id", "token_id", "checkpoint", "anchor", "cutoff", "retro"])
async def test_actual_worker_rejects_missing_capture_fact_without_effects(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    payload = initial_payload()
    del payload[field]
    job = await JobRepository(repo).enqueue("REACTION_CAPTURE", payload, "invalid-reaction", max_attempts=1)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            final = await finished_attempt(repo, job.job_id)
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
        assert final["status"] == "FAILED" and final["last_error"] == "INVALID_QUEUE_ENVELOPE"
        assert final["attempts"] == 1 and "result" not in final
        assert final["payload"] == payload and not router.calls
    assert await repo.list("out.reaction_checkpoints") == []
    assert await repo.list("ana.market_truth") == []


@pytest.mark.parametrize("fault", ["foreign-subject", "wrong-key", "mutated-revision"])
def test_result_refuses_conflicting_or_mutated_authority(fault: str) -> None:
    command = reaction_job_command(initial_payload())
    receipt = CapturedReaction(
        kind="CAPTURED",
        checkpoint_key="event:token:PRE_1H:1",
        event_id="event",
        token_id="token",
        checkpoint="PRE_1H",
        revision=1,
        data_fingerprint="a" * 64,
    )
    if fault == "foreign-subject":
        receipt = receipt.model_copy(update={"event_id": "foreign"})
    elif fault == "wrong-key":
        receipt = receipt.model_copy(update={"checkpoint_key": "foreign:key"})
    else:
        receipt = receipt.model_copy(update={"revision": 0})
    with pytest.raises((ValueError, ValidationError)):
        validate_reaction_receipt(command, receipt)


@pytest.mark.parametrize("corrupt", [False, True])
async def test_actual_worker_revalidates_real_checkpoint_receipt_before_storage(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corrupt: bool,
) -> None:
    stored: dict[str, JsonValue] = {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "PRE_1H",
        "revision": 1,
        "data_fingerprint": "a" * 64,
    }
    await repo.put("out.reaction_checkpoints", "event:token:PRE_1H:1", stored)
    job = await JobRepository(repo).enqueue(
        "REACTION_CAPTURE", initial_payload(), "recorded-reaction", max_attempts=1
    )
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        capture = runtime.scheduler.reaction_capture

        async def observed(command: ReactionCaptureCommand) -> ReactionCaptureResult:
            receipt = await capture(command)
            if corrupt:
                assert isinstance(receipt, CapturedReaction)
                return receipt.model_copy(update={"token_id": "foreign"})
            return receipt

        monkeypatch.setattr(runtime.scheduler, "reaction_capture", observed)
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            final = await finished_attempt(repo, job.job_id)
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
        if corrupt:
            assert final["status"] == "FAILED" and final["last_error"] == "ValueError"
            assert "result" not in final
        else:
            assert final["status"] == "DONE"
            receipt = CapturedReaction.model_validate(final["result"])
            assert receipt.checkpoint_key == "event:token:PRE_1H:1"
            assert receipt.data_fingerprint == stored["data_fingerprint"]
        assert not router.calls
    assert await repo.get("out.reaction_checkpoints", "event:token:PRE_1H:1") == stored
    assert len(await repo.list("out.reaction_checkpoints")) == 1


@pytest.mark.parametrize("field", ["cutoff", "retro"])
def test_initial_queue_schedule_facts_cannot_be_reported_null(field: str) -> None:
    with pytest.raises(ValidationError):
        reaction_job_command(initial_payload() | {field: None})


def test_manual_queued_revision_retains_the_declared_schedule_contract() -> None:
    payload = initial_payload() | {"revision": 2, "reason": "Operator correction"}
    command = reaction_job_command(payload)
    assert isinstance(command, ManualReactionRevisionCommand)
    assert command.revision == 2 and command.anchor is not None
    for field in ("anchor", "cutoff", "retro"):
        incomplete = dict(payload)
        del incomplete[field]
        projected = reaction_job_command(incomplete)
        assert isinstance(projected, ManualReactionRevisionCommand)
        assert field not in projected.model_fields_set
        assert projected.storage_fields() == incomplete
