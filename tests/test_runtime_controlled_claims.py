"""Controlled stop refunds only the actual token owned before task cancellation."""

import asyncio
from collections.abc import Callable
from pathlib import Path

import pytest
from runtime_public_fixtures import public_runtime
from test_storage import repo as repo

from tank.contracts.jobs import JobClaimMutation, JobClaimScope, JobEnvelope
from tank.contracts.scheduler_results import ConditionEvaluationResult
from tank.job_repository import JobRepository
from tank.runtime_dispatch_contracts import ClaimedWork
from tank.runtime_worker_repository import RepositoryWorkerQueue
from tank.storage import StorageBackend
from tank.time_precision import Timestamp, collection_clock
from tank.util import canonical_json


@pytest.mark.parametrize("recovered", [False, True])
async def test_actual_owned_worker_stop_preserves_or_refunds_only_its_current_claim(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recovered: bool
) -> None:
    stamp = collection_clock()
    job = await JobRepository(repo).enqueue("EVAL_CONDITIONS", stamp.fields("cutoff"), "stop")
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        entered = asyncio.Event()
        release = asyncio.Event()
        evaluate = runtime.scheduler.eval_conditions

        async def paused(cutoff: Timestamp) -> ConditionEvaluationResult:
            entered.set()
            await release.wait()
            return await evaluate(cutoff)

        monkeypatch.setattr(runtime.scheduler, "eval_conditions", paused)
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            snapshot = runtime.snapshot_active_claims()
            assert len(snapshot) == 1 and snapshot[0].job_id == job.job_id
            assert runtime.active_jobs == {job.job_id}
            if recovered:
                assert await repo.recover_jobs() == 1
                replacement = await RepositoryWorkerQueue(repo).claim()
                assert isinstance(replacement, ClaimedWork)
                assert replacement.job.attempts == 1
                assert replacement.job.claim_token != snapshot[0].claim_token
            before = await repo.get("ops.jobs", job.job_id)
            assert before is not None
        finally:
            stop.set()
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        interrupted_at = collection_clock()
        acknowledged = await runtime.interrupt_claims(snapshot, interrupted_at)
        after = await repo.get("ops.jobs", job.job_id)
        assert after is not None and not runtime.active_jobs
        if recovered:
            assert acknowledged == ()
            assert canonical_json(after) == canonical_json(before)
        else:
            assert acknowledged == snapshot
            assert after["status"] == "QUEUED" and after["attempts"] == 0
            assert after["interrupted_at"] == interrupted_at.value
            assert after["interruption_reason"] == "CONTROLLED_GENERATION_STOP"
            assert after["claim_token"] == snapshot[0].claim_token
            assert canonical_json(after["payload"]) == canonical_json(before["payload"])
        assert not router.calls


async def test_lost_interruption_commit_ack_never_refunds_twice_or_claims_a_false_ack(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    await JobRepository(repo).enqueue("FILTER_ITEM", {"item_id": "item"}, "lost-ack")
    work = await RepositoryWorkerQueue(repo).claim()
    assert isinstance(work, ClaimedWork)
    transition = repo.transition_claimed_job
    committed = asyncio.Event()

    async def lose_ack(
        scope: JobClaimScope, transform: Callable[[JobEnvelope], JobClaimMutation]
    ) -> JobEnvelope | None:
        current = await transition(scope, transform)
        if current is not None:
            committed.set()
            raise ConnectionError("Synthetic lost transition acknowledgment")
        return current

    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        monkeypatch.setattr(repo, "transition_claimed_job", lose_ack)
        stamp = collection_clock()
        assert await runtime.interrupt_claims((work.scope(),), stamp) == ()
        assert committed.is_set()
        before = await repo.get("ops.jobs", work.job.job_id)
        assert before is not None and before["status"] == "QUEUED" and before["attempts"] == 0
        assert before["interrupted_at"] == stamp.value
        assert await runtime.interrupt_claims((work.scope(),), collection_clock()) == ()
        assert canonical_json(await repo.get("ops.jobs", work.job.job_id)) == canonical_json(before)
        assert not router.calls
