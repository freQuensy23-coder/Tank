"""Typed evaluator receipts are admitted before durable wire serialization."""

import pytest
from pydantic import ValidationError

from tank.contracts.scheduler_results import ConditionEvaluationResult
from tank.runtime_evaluation import evaluate_condition_job
from tank.time_precision import Timestamp


class MalformedEvaluator:
    async def eval_conditions(self, cutoff_stamp: Timestamp) -> ConditionEvaluationResult:
        return ConditionEvaluationResult.model_construct(evaluated=[""], fired=[], deferred=[])


async def test_constructed_invalid_receipt_cannot_cross_result_boundary() -> None:
    with pytest.raises(ValidationError):
        await evaluate_condition_job(
            MalformedEvaluator(),
            {"cutoff": "2026-10-26T00:00:00Z", "cutoff_prec": "EXACT", "cutoff_prec_reason": None},
        )
