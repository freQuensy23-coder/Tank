"""The real scheduler clock survives queue admission and actual worker dispatch."""

import asyncio
from pathlib import Path

import pytest
from pydantic import JsonValue
from runtime_public_fixtures import public_runtime
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.scheduler_results import ConditionEvaluationResult
from tank.storage import StorageBackend
from tank.time_precision import Timestamp
from tank.util import iso, utcnow


async def finished_attempt(repo: StorageBackend, job_id: str) -> dict[str, JsonValue]:
    async with asyncio.timeout(2):
        while True:
            current = await repo.get("ops.jobs", job_id)
            assert current is not None
            if current["attempts"] == 1 and current["status"] != "RUNNING":
                return current
            await asyncio.sleep(0.005)


async def test_actual_tick_queue_worker_preserves_cutoff_and_stores_real_evaluation_result(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, _):
        observed: list[Timestamp] = []
        evaluate = runtime.scheduler.eval_conditions

        async def observing(cutoff: Timestamp) -> ConditionEvaluationResult:
            observed.append(cutoff)
            return await evaluate(cutoff)

        monkeypatch.setattr(runtime.scheduler, "eval_conditions", observing)
        at = utcnow()
        await runtime.scheduler.tick(at)
        queued = await repo.list("ops.jobs", filters={"type": "EVAL_CONDITIONS"})
        assert len(queued) == 1
        job_id = queued[0]["job_id"]
        assert isinstance(job_id, str)
        payload = queued[0]["payload"]
        assert isinstance(payload, dict) and payload["cutoff"] == iso(at.replace(second=0, microsecond=0))
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            result = await finished_attempt(repo, job_id)
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
        assert result["status"] == "DONE", result["last_error"]
        assert result["result"] == {"evaluated": [], "fired": [], "deferred": []}
        assert len(observed) == 1
        assert observed[0].value == iso(at.replace(second=0, microsecond=0))
        assert observed[0].precision == "BUCKET(1m)" and observed[0].precision_reason is None
        assert result["finished_at"] is not None and result["attempts"] == 1


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"cutoff": "2026-10-26T00:00:00", "cutoff_prec": "EXACT", "cutoff_prec_reason": None},
        {"cutoff": "2026-10-26T00:00:00Z", "cutoff_prec": None, "cutoff_prec_reason": None},
    ],
)
async def test_worker_refuses_missing_or_invalid_cutoff_without_calling_evaluator_or_guessing_clock(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, JsonValue],
) -> None:
    jobs = JobRepository(repo)
    job = await jobs.enqueue("EVAL_CONDITIONS", payload, "invalid-cutoff", max_attempts=1)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, _):
        calls: list[Timestamp] = []
        evaluate = runtime.scheduler.eval_conditions

        async def observing(cutoff: Timestamp) -> ConditionEvaluationResult:
            calls.append(cutoff)
            return await evaluate(cutoff)

        monkeypatch.setattr(runtime.scheduler, "eval_conditions", observing)
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            result = await finished_attempt(repo, job.job_id)
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
    assert result["status"] == "FAILED" and result["last_error"] == "INVALID_QUEUE_ENVELOPE"
    assert result["attempts"] == 1
    assert result["payload"] == payload and "result" not in result
    assert calls == []
    assert await repo.list("ana.watch_conditions") == []
