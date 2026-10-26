"""Concrete durable commands and the queue facts consumed by worker orchestration."""

from typing import Annotated, Literal, Self

from pydantic import Field, RootModel, model_validator

from tank.contracts import DomainModel
from tank.contracts.jobs import JobEnvelope
from tank.runtime_contracts import (
    AgentMessage,
    DeliveryJob,
    EscalateTokensJob,
    EvalConditionsJob,
    ItemJob,
    MarketTruthJob,
    MilestoneCheckJob,
    RevisedReactionJob,
    ScheduledReactionJob,
    ScoutReviewJob,
    TelegramAnswerJob,
)
from tank.runtime_retry import ApplicationBackoff, WorkerRetrySchedule
from tank.time_precision import Timestamp
from tank.util import sha256

type RuntimeJobKind = Literal[
    "FILTER_ITEM",
    "ROUTE_ITEM",
    "AGENT_MESSAGE",
    "ESCALATE_TOKENS",
    "RUN_MARKET_TRUTH",
    "DELIVER",
    "EVAL_CONDITIONS",
    "MILESTONE_CHECK",
    "REACTION_CAPTURE",
    "SCOUT_REVIEW",
    "TELEGRAM_ANSWER",
]


class FilterItemCommand(DomainModel):
    type: Literal["FILTER_ITEM"]
    payload: ItemJob


class RouteItemCommand(DomainModel):
    type: Literal["ROUTE_ITEM"]
    payload: ItemJob


class AgentMessageCommand(DomainModel):
    type: Literal["AGENT_MESSAGE"]
    payload: AgentMessage


class EscalateTokensCommand(DomainModel):
    type: Literal["ESCALATE_TOKENS"]
    payload: EscalateTokensJob


class MarketTruthCommand(DomainModel):
    type: Literal["RUN_MARKET_TRUTH"]
    payload: MarketTruthJob


class DeliverCommand(DomainModel):
    type: Literal["DELIVER"]
    payload: DeliveryJob


class EvalConditionsCommand(DomainModel):
    type: Literal["EVAL_CONDITIONS"]
    payload: EvalConditionsJob


class MilestoneCheckCommand(DomainModel):
    type: Literal["MILESTONE_CHECK"]
    payload: MilestoneCheckJob


class ReactionCaptureCommand(DomainModel):
    type: Literal["REACTION_CAPTURE"]
    payload: ScheduledReactionJob | RevisedReactionJob


class ScoutReviewCommand(DomainModel):
    type: Literal["SCOUT_REVIEW"]
    payload: ScoutReviewJob


class TelegramAnswerCommand(DomainModel):
    type: Literal["TELEGRAM_ANSWER"]
    payload: TelegramAnswerJob


type RuntimeCommand = Annotated[
    FilterItemCommand
    | RouteItemCommand
    | AgentMessageCommand
    | EscalateTokensCommand
    | MarketTruthCommand
    | DeliverCommand
    | EvalConditionsCommand
    | MilestoneCheckCommand
    | ReactionCaptureCommand
    | ScoutReviewCommand
    | TelegramAnswerCommand,
    Field(discriminator="type"),
]


class RuntimeCommandRecord(RootModel[RuntimeCommand]):
    model_config = {"strict": True, "hide_input_in_errors": True, "revalidate_instances": "always"}


class DependencyState(DomainModel):
    job_id: str = Field(min_length=1, pattern=r"\S")
    status: Literal["QUEUED", "RUNNING", "DONE", "FAILED", "EXPIRED"]


class RunningJobClaim(DomainModel):
    job_id: str = Field(min_length=1, pattern=r"\S")
    type: RuntimeJobKind
    status: Literal["RUNNING"]
    attempts: int = Field(ge=1)
    payload_revision: str = Field(pattern=r"^[0-9a-f]{64}$")


class ClaimedWork(DomainModel):
    payload_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    job: JobEnvelope
    command: RuntimeCommand

    @model_validator(mode="after")
    def actual_execution(self) -> Self:
        if self.job.status != "RUNNING" or self.job.attempts < 1:
            raise ValueError("Worker execution requires an actual running claimed attempt")
        if self.job.type != self.command.type:
            raise ValueError("Worker command differs from its claimed job kind")
        if sha256(self.job.payload) != self.payload_revision:
            raise ValueError("Worker revision differs from its original claimed payload")
        admitted = RuntimeCommandRecord.model_validate(
            {"type": self.job.type, "payload": self.job.payload}
        ).root
        if admitted != self.command:
            raise ValueError("Worker command differs from its original claimed payload")
        return self

    def scope(self) -> RunningJobClaim:
        return RunningJobClaim.model_validate(
            {
                "job_id": self.job.job_id,
                "type": self.job.type,
                "status": self.job.status,
                "attempts": self.job.attempts,
                "payload_revision": self.payload_revision,
            }
        )


class MissingDependency(DomainModel):
    job_id: str = Field(min_length=1, pattern=r"\S")
    reason: Literal["MISSING"]


class FailedDependency(DomainModel):
    job_id: str = Field(min_length=1, pattern=r"\S")
    reason: Literal["FAILED", "EXPIRED"]


class DependencyFailure(RuntimeError):
    def __init__(self, failed: MissingDependency | FailedDependency) -> None:
        self.failed = failed
        super().__init__("Durable job dependency failed or is missing")


class ReadyDependencies(DomainModel):
    status: Literal["READY"]


class WaitingDependencies(DomainModel):
    status: Literal["WAITING"]
    job_ids: tuple[Annotated[str, Field(min_length=1, pattern=r"\S")], ...] = Field(min_length=1)


type DependencyReadiness = ReadyDependencies | WaitingDependencies


class RequeueRequest(DomainModel):
    claim: RunningJobClaim
    not_before: Timestamp
    reason: Literal["DEPENDENCY_WAIT", "DELIVERY_WAIT", "SESSION_ADMISSION"]

    @model_validator(mode="after")
    def actual_schedule(self) -> Self:
        if self.not_before.value is None:
            raise ValueError("Worker requeue requires an actual schedule boundary")
        return self


class WorkerFailureRequest(DomainModel):
    claim: RunningJobClaim
    error_class: str = Field(min_length=1, pattern=r"\S")
    schedule: WorkerRetrySchedule
    observed_at: Timestamp

    @model_validator(mode="after")
    def actual_failure_scope(self) -> Self:
        if self.observed_at.value is None:
            raise ValueError("Worker failure requires an actual controller observation")
        if isinstance(self.schedule, ApplicationBackoff) and self.schedule.attempts != self.claim.attempts:
            raise ValueError("Application backoff differs from its actual claimed attempt")
        return self
