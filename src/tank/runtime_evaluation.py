"""Durable evaluation wire admission and result serialization around the typed use case."""

from typing import Protocol

from pydantic import JsonValue

from tank.contracts.json import decode_json
from tank.contracts.scheduler_results import ConditionEvaluationResult
from tank.runtime_contracts import EvalConditionsJob
from tank.time_precision import Timestamp


class ConditionEvaluator(Protocol):
    async def eval_conditions(self, cutoff_stamp: Timestamp) -> ConditionEvaluationResult: ...


async def evaluate_condition_job(
    evaluator: ConditionEvaluator, payload: dict[str, JsonValue]
) -> dict[str, JsonValue]:
    command = EvalConditionsJob.model_validate(payload)
    cutoff = Timestamp(
        command.cutoff,
        command.cutoff_prec,
        command.cutoff_prec_reason,
        command.cutoff_bucket_anchor,
    )
    outcome = ConditionEvaluationResult.model_validate(await evaluator.eval_conditions(cutoff))
    serialized = decode_json(outcome.model_dump_json())
    if not isinstance(serialized, dict):
        raise TypeError("Condition evaluation result must serialize as an object")
    return serialized
