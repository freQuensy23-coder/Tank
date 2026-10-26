"""Closed application payloads decoded at the durable worker boundary."""

from typing import Annotated, Literal, Self

from pydantic import AliasChoices, ConfigDict, Field, JsonValue, TypeAdapter, model_validator

from tank.analysis.watch_contracts import MilestoneSnapshot, TimedReviewCondition, WatchCall, WatchCondition
from tank.contracts import DomainModel
from tank.contracts.scalars import TrueFlag
from tank.time_precision import Timestamp, timestamp_owned_fields

type BucketAnchor = Literal["START", "END", "UNSPECIFIED"]


class JobPayload(DomainModel):
    depends_on: list[str] = Field(default_factory=list, alias="_depends_on")


class ItemJob(JobPayload):
    item_id: str = Field(min_length=1)


class EscalateTokensJob(JobPayload):
    event_id: str = Field(min_length=1)
    tokens: list[str] = Field(min_length=1)
    token_id: str | None = None


class MarketTruthJob(JobPayload):
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    cutoff: str | None = None
    cutoff_prec: str | None = None
    cutoff_prec_reason: str | None = None
    cutoff_bucket_anchor: BucketAnchor | None = None

    @model_validator(mode="after")
    def validate_cutoff(self) -> Self:
        if self.cutoff is not None:
            Timestamp(self.cutoff, self.cutoff_prec, self.cutoff_prec_reason, self.cutoff_bucket_anchor)
        elif self.cutoff_prec is not None or self.cutoff_bucket_anchor is not None:
            raise ValueError("Cutoff metadata requires a cutoff value")
        return self


class EvalConditionsJob(JobPayload):
    cutoff: str
    cutoff_prec: str | None
    cutoff_prec_reason: str | None
    cutoff_bucket_anchor: BucketAnchor | None = None


class MilestoneCheckJob(JobPayload):
    event_id: str
    milestone: str
    due_at: str
    due_at_prec: str | None
    due_at_prec_reason: str | None
    source_id: str | None
    due_at_bucket_anchor: BucketAnchor | None = None


class ReactionAnchor(DomainModel):
    type: Literal["T0", "T1", "KNOWN_TO_US_AT"]
    ts: str
    ts_prec: str | None
    ts_prec_reason: str | None
    precision: str | None = None
    ts_bucket_anchor: BucketAnchor | None = None


type ReactionCheckpoint = Literal[
    "PRE_24H", "PRE_4H", "PRE_1H", "POST_15M", "POST_1H", "POST_4H", "POST_24H", "POST_72H"
]


class ScheduledReactionJob(JobPayload):
    event_id: str
    token_id: str
    checkpoint: ReactionCheckpoint
    anchor: ReactionAnchor
    cutoff: str
    cutoff_prec: str | None
    cutoff_prec_reason: str | None
    cutoff_bucket_anchor: BucketAnchor | None = None
    retro: bool
    revision: int = Field(default=1, ge=1)
    reason: str | None = None


class RevisedReactionJob(JobPayload):
    event_id: str
    token_id: str
    checkpoint: ReactionCheckpoint
    automatic_revision: TrueFlag
    change_id: str
    reason: str = Field(min_length=1)


class AlertDeliveryJob(JobPayload):
    alert_id: str
    version: int = Field(ge=1)
    channel: Literal["telegram:alerts"]


class NoticeDeliveryJob(JobPayload):
    notice_id: str
    channel: Literal["telegram:ops"]


type DeliveryJob = AlertDeliveryJob | NoticeDeliveryJob


def parse_delivery_job(payload: dict[str, JsonValue]) -> DeliveryJob:
    if "notice_id" in payload:
        return NoticeDeliveryJob.model_validate(payload)
    return AlertDeliveryJob.model_validate(payload)


class TelegramAnswerJob(JobPayload):
    question_id: str


class ScoutReviewJob(JobPayload):
    proposal_id: str = Field(min_length=1, validation_alias=AliasChoices("proposal_id", "prp_id"))


class ReplayBinding(DomainModel):
    replay_run_id: str | None = None
    replay_source_session_id: str | None = None
    replay_evidence_sha256: str | None = None
    replay_cutoff: str | None = None


class SourceMessage(JobPayload, ReplayBinding):
    kind: Literal["NEW_ITEM", "ITEM_ADDED"]
    item_id: str
    event_id: str | None = None
    session_id: str | None = None


class UserQuestionMessage(JobPayload):
    kind: Literal["USER_QUESTION"]
    session_id: str
    question_id: str


class ScoutMessage(JobPayload, ReplayBinding):
    kind: Literal["SCOUT_REVIEW"]
    proposal_id: str
    session_id: str | None = None


type DirectAgentMessage = Annotated[
    SourceMessage | UserQuestionMessage | ScoutMessage,
    Field(discriminator="kind"),
]


def parse_direct_agent_message(payload: dict[str, JsonValue]) -> DirectAgentMessage:
    match payload["kind"]:
        case "NEW_ITEM" | "ITEM_ADDED":
            return SourceMessage.model_validate(payload)
        case "USER_QUESTION":
            return UserQuestionMessage.model_validate(payload)
        case "SCOUT_REVIEW":
            return ScoutMessage.model_validate(payload)
        case _:
            raise ValueError("Unsupported direct agent message kind")


class ConditionFiredMessage(JobPayload, ReplayBinding):
    kind: Literal["CONDITION_FIRED"]
    event_id: str
    condition: WatchCondition
    token_id: str
    session_id: str | None = None

    @model_validator(mode="after")
    def matches_condition(self) -> Self:
        if self.condition.event_id != self.event_id or self.condition.token_id != self.token_id:
            raise ValueError("Condition notification identity differs from its snapshot")
        if self.condition.status != "FIRED":
            raise ValueError("Condition notification requires its fired snapshot")
        return self


class TimedReviewFiredMessage(JobPayload, ReplayBinding):
    kind: Literal["CONDITION_FIRED"]
    event_id: str = Field(min_length=1)
    condition: TimedReviewCondition
    review_at: str
    review_question: str = Field(min_length=1, max_length=500, pattern=r"\S")
    session_id: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def matches_review(self) -> Self:
        if self.condition.event_id != self.event_id or self.condition.status != "FIRED":
            raise ValueError("Review notification differs from its fired event snapshot")
        if (
            self.review_at != self.condition.expr.review_at
            or self.review_question != self.condition.expr.review_question
        ):
            raise ValueError("Review notification differs from its saved question or schedule")
        if self.condition.session_id is not None and self.session_id != self.condition.session_id:
            raise ValueError("Review notification differs from its saved originating session")
        return self


type FiredConditionMessage = ConditionFiredMessage | TimedReviewFiredMessage

_FIRED_CONDITIONS: TypeAdapter[FiredConditionMessage] = TypeAdapter(
    ConditionFiredMessage | TimedReviewFiredMessage,
    config=ConfigDict(hide_input_in_errors=True),
)


class CallExpiredMessage(JobPayload, ReplayBinding):
    kind: Literal["CALL_EXPIRED"]
    event_id: str
    call: WatchCall

    @model_validator(mode="after")
    def describes_expired_call(self) -> Self:
        if self.call.event_id != self.event_id:
            raise ValueError("Call notification event differs from its snapshot")
        if self.call.status != "CLOSED" or self.call.closed_by != "EXPIRED":
            raise ValueError("Expired call notification requires its closed snapshot")
        return self


class MilestoneMessage(JobPayload, ReplayBinding):
    kind: Literal["MILESTONE"]
    event_id: str
    milestone: str | MilestoneSnapshot
    outcome: str | None = None
    quotes: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    verification_required: bool
    instruction: str


type AgentMessage = (
    SourceMessage
    | UserQuestionMessage
    | ScoutMessage
    | ConditionFiredMessage
    | TimedReviewFiredMessage
    | CallExpiredMessage
    | MilestoneMessage
)


def parse_agent_message(payload: dict[str, JsonValue]) -> AgentMessage:
    match payload["kind"]:
        case "CONDITION_FIRED":
            return _FIRED_CONDITIONS.validate_python(payload, strict=True)
        case "CALL_EXPIRED":
            return CallExpiredMessage.model_validate(payload)
        case "MILESTONE":
            return MilestoneMessage.model_validate(payload)
        case _:
            return parse_direct_agent_message(payload)


type JobClockField = Literal["run_after", "slot_waiting_since", "interrupted_at", "finished_at"]


def replace_job_clock(
    record: dict[str, JsonValue], field: JobClockField, timestamp: Timestamp
) -> dict[str, JsonValue]:
    if timestamp.value is None:
        raise ValueError("Job clock replacement requires an actual timestamp")
    owned = timestamp_owned_fields(field)
    result = {key: value for key, value in record.items() if key not in owned}
    result.update(timestamp.fields(field))
    return result
