"""Actual scheduler execution receipts, separate from persisted financial evidence."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel

type SchedulerIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]


class DeferredCondition(DomainModel):
    cond_id: SchedulerIdentity
    reason: Literal["BATCH_BUDGET", "READ_TIMEOUT"]


class ConditionEvaluationResult(DomainModel):
    evaluated: list[SchedulerIdentity]
    fired: list[SchedulerIdentity]
    deferred: list[DeferredCondition]

    @model_validator(mode="after")
    def unique_disjoint_outcomes(self) -> Self:
        evaluated = set(self.evaluated)
        fired = set(self.fired)
        deferred = {item.cond_id for item in self.deferred}
        if (
            len(evaluated) != len(self.evaluated)
            or len(fired) != len(self.fired)
            or len(deferred) != len(self.deferred)
        ):
            raise ValueError("Condition evaluation IDs must be unique within each outcome")
        if (
            not evaluated.isdisjoint(fired)
            or not evaluated.isdisjoint(deferred)
            or not fired.isdisjoint(deferred)
        ):
            raise ValueError("Condition evaluation outcomes must be disjoint")
        return self
