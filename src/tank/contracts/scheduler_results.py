"""Actual scheduler execution receipts, separate from persisted financial evidence."""

from typing import Annotated, Literal

from pydantic import Field

from tank.contracts import DomainModel

type SchedulerIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]


class DeferredCondition(DomainModel):
    cond_id: SchedulerIdentity
    reason: Literal["BATCH_BUDGET", "READ_TIMEOUT"]


class ConditionEvaluationResult(DomainModel):
    evaluated: list[SchedulerIdentity]
    fired: list[SchedulerIdentity]
    deferred: list[DeferredCondition]
