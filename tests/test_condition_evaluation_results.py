"""One evaluation pass cannot serialize duplicate or contradictory terminal outcomes."""

import asyncio
from pathlib import Path
from typing import Literal

import pytest
from pydantic import ValidationError
from runtime_public_fixtures import public_runtime
from test_runtime_condition_evaluation import finished_attempt
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.scheduler_results import ConditionEvaluationResult, DeferredCondition
from tank.storage import StorageBackend
from tank.time_precision import Timestamp, collection_clock


def test_deferred_and_evaluated_ids_are_not_two_outcomes_for_one_condition() -> None:
    with pytest.raises(ValidationError, match="disjoint"):
        ConditionEvaluationResult(
            evaluated=["c"], fired=[], deferred=[DeferredCondition(cond_id="c", reason="READ_TIMEOUT")]
        )


@pytest.mark.parametrize("category", ["evaluated", "fired", "deferred"])
def test_duplicate_ids_are_rejected_in_each_terminal_category(category: str) -> None:
    with pytest.raises(ValidationError, match="unique"):
        ConditionEvaluationResult(
            evaluated=["c", "c"] if category == "evaluated" else [],
            fired=["c", "c"] if category == "fired" else [],
            deferred=[
                DeferredCondition(cond_id="c", reason="BATCH_BUDGET"),
                DeferredCondition(cond_id="c", reason="READ_TIMEOUT"),
            ]
            if category == "deferred"
            else [],
        )


@pytest.mark.parametrize("other", ["evaluated", "deferred"])
def test_fired_ids_cannot_also_have_another_terminal_outcome(other: str) -> None:
    with pytest.raises(ValidationError, match="disjoint"):
        ConditionEvaluationResult(
            evaluated=["c"] if other == "evaluated" else [],
            fired=["c"],
            deferred=[DeferredCondition(cond_id="c", reason="BATCH_BUDGET")] if other == "deferred" else [],
        )


@pytest.mark.parametrize("fault", ["duplicate", "overlap"])
async def test_actual_worker_refuses_semantically_invalid_mutated_result_before_serialization(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: Literal["duplicate", "overlap"],
) -> None:
    job = await JobRepository(repo).enqueue(
        "EVAL_CONDITIONS", collection_clock().fields("cutoff"), "invalid-terminal-result", max_attempts=1
    )
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        evaluate = runtime.scheduler.eval_conditions
        observed: list[Timestamp] = []

        async def corrupted(cutoff: Timestamp) -> ConditionEvaluationResult:
            observed.append(cutoff)
            result = await evaluate(cutoff)
            result.evaluated.append("invalid-result-condition")
            if fault == "duplicate":
                result.evaluated.append("invalid-result-condition")
            else:
                result.fired.append("invalid-result-condition")
            return result

        monkeypatch.setattr(runtime.scheduler, "eval_conditions", corrupted)
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            final = await finished_attempt(repo, job.job_id)
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
        assert final["status"] == "FAILED" and final["last_error"] == "ValidationError"
        assert final["attempts"] == 1 and "result" not in final
        assert final["payload"] == job.payload and len(observed) == 1
        assert not router.calls
    assert await repo.list("ana.watch_conditions") == []
    assert await repo.list("out.deliveries") == []
