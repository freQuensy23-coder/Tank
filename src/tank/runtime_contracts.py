"""Closed application payloads decoded at the durable worker boundary."""

from typing import Annotated, Literal, Self

from pydantic import AliasChoices, ConfigDict, Field, JsonValue, RootModel, model_validator

from tank.analysis.watch_contracts import MilestoneSnapshot, TimedReviewCondition, WatchCall, WatchCondition
from tank.contracts import DomainModel
from tank.contracts.milestone_checks import ScheduledMilestoneCheck
from tank.contracts.progress import JobFailureProgressView
from tank.contracts.question_retirement import QuestionFailureRefusalReceipt
from tank.contracts.scalars import FalseFlag, TrueFlag
from tank.contracts.session_admission import SessionTarget
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.time_precision import Timestamp, propagate_timestamp, timestamp_owned_fields
from tank.util import parse_iso_time

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


class MilestoneCheckJob(ScheduledMilestoneCheck, JobPayload):
    pass


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
    replay_at: str | None = None
    replay_at_prec: str | None = None
    replay_at_prec_reason: str | None = None
    replay_at_bucket_anchor: BucketAnchor | None = None

    @model_validator(mode="after")
    def actual_replay_schedule(self) -> Self:
        facets = (self.replay_at_prec, self.replay_at_prec_reason, self.replay_at_bucket_anchor)
        if self.replay_at is None:
            if any(value is not None for value in facets):
                raise ValueError("Replay message clock metadata requires its actual occurrence")
            return self
        if self.replay_run_id is None:
            raise ValueError("Replay message timing requires its bound run identity")
        if any(value is not None for value in facets):
            Timestamp(
                self.replay_at, self.replay_at_prec, self.replay_at_prec_reason, self.replay_at_bucket_anchor
            )
        else:
            parse_iso_time(self.replay_at)
        return self


class SourceMessage(JobPayload, ReplayBinding):
    kind: Literal["NEW_ITEM", "ITEM_ADDED"]
    item_id: str
    event_id: str | None = None
    session_id: str | None = None
    origin: Literal["FIXTURE_AUTHORED_REFERENCE"] | None = None
    historical: FalseFlag | None = None

    @model_validator(mode="after")
    def authored_reference_is_bound(self) -> Self:
        if self.origin is None and self.historical is None:
            return self
        if (
            self.origin != "FIXTURE_AUTHORED_REFERENCE"
            or self.historical is not False
            or self.replay_run_id is None
            or self.replay_at is None
            or self.replay_source_session_id is not None
        ):
            raise ValueError("Authored reference messages require their nonhistorical bound replay scope")
        return self


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


class _FiredConditionRecord(RootModel[FiredConditionMessage]):
    model_config = ConfigDict(strict=True, hide_input_in_errors=True, revalidate_instances="always")


class CallExpiredMessage(JobPayload, ReplayBinding):
    kind: Literal["CALL_EXPIRED"]
    event_id: str
    session_id: str | None = Field(default=None, min_length=1)
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
    session_id: str | None = Field(default=None, min_length=1)
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


def agent_message_target(message: AgentMessage) -> SessionTarget:
    return SessionTarget(
        session_id=message.session_id,
        event_id=None if isinstance(message, (ScoutMessage, UserQuestionMessage)) else message.event_id,
        item_id=message.item_id if isinstance(message, SourceMessage) else None,
        proposal_id=message.proposal_id if isinstance(message, ScoutMessage) else None,
    )


def parse_agent_message(payload: dict[str, JsonValue]) -> AgentMessage:
    match payload["kind"]:
        case "CONDITION_FIRED":
            return _FiredConditionRecord.model_validate(payload).root
        case "CALL_EXPIRED":
            return CallExpiredMessage.model_validate(payload)
        case "MILESTONE":
            return MilestoneMessage.model_validate(payload)
        case _:
            return parse_direct_agent_message(payload)


type JobClockField = Literal[
    "run_after",
    "slot_waiting_since",
    "interrupted_at",
    "finished_at",
    "ops_progress_recorded_at",
    "delivery_failure_recorded_at",
    "followup_failure_recorded_at",
]


def replace_job_clock(
    record: dict[str, JsonValue], field: JobClockField, timestamp: Timestamp
) -> dict[str, JsonValue]:
    if timestamp.value is None:
        raise ValueError("Job clock replacement requires an actual timestamp")
    owned = timestamp_owned_fields(field)
    result = {key: value for key, value in record.items() if key not in owned}
    result.update(timestamp.fields(field))
    return result


class NoFailureProgress(DomainModel):
    reason: Literal["DISABLED", "NOT_APPLICABLE"]


class NoTerminalDelivery(DomainModel):
    kind: Literal["NONE"]
    reason: Literal["NOT_REQUESTED", "NOT_APPLICABLE"]


class FailedDeliveryNotification(DomainModel):
    kind: Literal["DELIVERY"]
    payload: DeliveryJob
    attempt: int = Field(ge=0)


class FailedQuestionNotification(DomainModel):
    kind: Literal["QUESTION"]
    target: FollowupRefusalTarget


class FailedAnswerNotification(DomainModel):
    kind: Literal["ANSWER"]
    payload: TelegramAnswerJob

    @model_validator(mode="after")
    def actual_question_identity(self) -> Self:
        if not self.payload.question_id.strip():
            raise ValueError("Terminal answer notification requires its actual question identity")
        return self


type TerminalDeliveryNotification = Annotated[
    NoTerminalDelivery | FailedDeliveryNotification | FailedQuestionNotification | FailedAnswerNotification,
    Field(discriminator="kind"),
]


class FailureRecordIdentity(DomainModel):
    job_id: str = Field(min_length=1, pattern=r"\S")
    job_type: str = Field(min_length=1, pattern=r"\S")
    status: Literal["QUEUED", "RUNNING", "DONE", "FAILED", "EXPIRED"]

    @classmethod
    def from_record(cls, record: dict[str, JsonValue]) -> Self:
        return cls.model_validate(
            {"job_id": record["job_id"], "job_type": record["type"], "status": record["status"]}
        )


class FailureJobHeader(FailureRecordIdentity):
    status: Literal["FAILED", "EXPIRED"]


type FailureMarkerField = Literal[
    "ops_progress_recorded_at", "delivery_failure_recorded_at", "followup_failure_recorded_at"
]


def read_failure_marker(record: dict[str, JsonValue], field: FailureMarkerField) -> Timestamp | None:
    declared = timestamp_owned_fields(field).intersection(record)
    if not declared or all(record[name] is None for name in declared):
        return None
    return propagate_timestamp(record, field)


def failure_payload(record: dict[str, JsonValue]) -> dict[str, JsonValue]:
    payload = record["payload"]
    if not isinstance(payload, dict):
        raise ValueError("Terminal queue failure requires its structured action payload")
    return payload


def failure_agent_kind(payload: dict[str, JsonValue]) -> str:
    kind = payload["kind"]
    if not isinstance(kind, str) or not kind.strip():
        raise ValueError("Terminal agent message requires its declared string kind")
    return kind


def terminal_notification(
    header: FailureJobHeader, record: dict[str, JsonValue], *, requested: bool
) -> TerminalDeliveryNotification:
    if not requested:
        return NoTerminalDelivery(kind="NONE", reason="NOT_REQUESTED")
    if header.job_type == "DELIVER":
        return FailedDeliveryNotification.model_validate(
            {
                "kind": "DELIVERY",
                "payload": parse_delivery_job(failure_payload(record)),
                "attempt": record["attempts"],
            },
            by_name=True,
        )
    if header.job_type == "AGENT_MESSAGE":
        payload = failure_payload(record)
        if failure_agent_kind(payload) == "USER_QUESTION":
            message = UserQuestionMessage.model_validate(payload)
            return FailedQuestionNotification(
                kind="QUESTION",
                target=FollowupRefusalTarget(question_id=message.question_id, session_id=message.session_id),
            )
    if header.job_type == "TELEGRAM_ANSWER":
        return FailedAnswerNotification.model_validate(
            {"kind": "ANSWER", "payload": TelegramAnswerJob.model_validate(failure_payload(record))},
            by_name=True,
        )
    return NoTerminalDelivery(kind="NONE", reason="NOT_APPLICABLE")


def terminal_progress(
    header: FailureJobHeader, record: dict[str, JsonValue], *, requested: bool
) -> JobFailureProgressView | NoFailureProgress:
    if not requested:
        return NoFailureProgress(reason="DISABLED")
    if header.job_type not in {
        "FILTER_ITEM",
        "ROUTE_ITEM",
        "AGENT_MESSAGE",
        "RUN_MARKET_TRUTH",
        "ESCALATE_TOKENS",
    }:
        return NoFailureProgress(reason="NOT_APPLICABLE")
    payload = failure_payload(record)
    if header.job_type == "AGENT_MESSAGE" and failure_agent_kind(payload) in {
        "USER_QUESTION",
        "SCOUT_REVIEW",
    }:
        return NoFailureProgress(reason="NOT_APPLICABLE")
    return JobFailureProgressView.model_validate(
        {
            "job_id": header.job_id,
            "type": header.job_type,
            "status": header.status,
            "target": {name: payload[name] for name in ("item_id", "event_id") if name in payload},
        }
    )


class SuppliedFailureReason(DomainModel):
    kind: Literal["SUPPLIED"]
    value: str = Field(min_length=1, pattern=r"\S")


class RecordedJobErrorReason(DomainModel):
    kind: Literal["RECORDED"]
    value: str = Field(min_length=1, pattern=r"\S")


class JobStateFailureReason(DomainModel):
    kind: Literal["STATE"]
    status: Literal["FAILED", "EXPIRED"]


type FailureReason = Annotated[
    SuppliedFailureReason | RecordedJobErrorReason | JobStateFailureReason, Field(discriminator="kind")
]


def failure_reason_text(reason: FailureReason) -> str:
    return reason.status if isinstance(reason, JobStateFailureReason) else reason.value


class RecordedFailureReason(DomainModel):
    last_error: str | None = Field(min_length=1, pattern=r"\S")


def recorded_failure_reason(header: FailureJobHeader, record: dict[str, JsonValue]) -> FailureReason:
    recorded = RecordedFailureReason.model_validate({"last_error": record.get("last_error")})
    return (
        RecordedJobErrorReason(kind="RECORDED", value=recorded.last_error)
        if recorded.last_error is not None
        else JobStateFailureReason(kind="STATE", status=header.status)
    )


class TerminalJobFailure(DomainModel):
    header: FailureJobHeader
    reason: FailureReason
    notify_delivery: bool
    report_progress: bool
    notification: TerminalDeliveryNotification
    progress: JobFailureProgressView | NoFailureProgress

    @model_validator(mode="after")
    def declared_failure_scope(self) -> Self:
        if isinstance(self.reason, JobStateFailureReason) and self.reason.status != self.header.status:
            raise ValueError("Terminal failure reason differs from its actual job state")
        if isinstance(self.progress, JobFailureProgressView) and (
            self.progress.job_id != self.header.job_id
            or self.progress.type != self.header.job_type
            or self.progress.status != self.header.status
            or not self.report_progress
        ):
            raise ValueError("Terminal progress differs from its actual queue failure")
        if isinstance(self.progress, NoFailureProgress):
            if (self.progress.reason == "DISABLED") == self.report_progress:
                raise ValueError("Terminal progress differs from its requested reporting scope")
            if self.report_progress and self.header.job_type in {
                "FILTER_ITEM",
                "ROUTE_ITEM",
                "RUN_MARKET_TRUTH",
                "ESCALATE_TOKENS",
            }:
                raise ValueError("Requested terminal source progress requires its actual subject")
        expected = {"DELIVERY": "DELIVER", "QUESTION": "AGENT_MESSAGE", "ANSWER": "TELEGRAM_ANSWER"}
        if isinstance(self.notification, NoTerminalDelivery):
            if (self.notification.reason == "NOT_REQUESTED") == self.notify_delivery:
                raise ValueError("Terminal notification differs from its requested delivery scope")
            if self.notify_delivery and self.header.job_type in {"DELIVER", "TELEGRAM_ANSWER"}:
                raise ValueError("Requested terminal delivery requires its actual subject")
        elif not self.notify_delivery or expected[self.notification.kind] != self.header.job_type:
            raise ValueError("Terminal notification differs from its actual queue failure")
        return self

    @classmethod
    def from_record(
        cls,
        record: dict[str, JsonValue],
        *,
        reason: FailureReason,
        progress_requested: bool,
        notifications_requested: bool,
    ) -> Self:
        header = FailureJobHeader.from_record(record)
        return cls.model_validate(
            {
                "header": header,
                "reason": reason,
                "notify_delivery": notifications_requested,
                "report_progress": progress_requested,
                "notification": terminal_notification(header, record, requested=notifications_requested),
                "progress": terminal_progress(header, record, requested=progress_requested),
            },
            by_name=True,
        )


class FailureReconciliationItem(DomainModel):
    failure: TerminalJobFailure
    marker_name: FailureMarkerField
    marker: Timestamp | None

    @classmethod
    def from_record(
        cls,
        record: dict[str, JsonValue],
        *,
        marker_name: FailureMarkerField,
        progress_requested: bool,
        notifications_requested: bool,
    ) -> Self:
        header = FailureJobHeader.from_record(record)
        return cls.model_validate(
            {
                "failure": TerminalJobFailure.from_record(
                    record,
                    reason=recorded_failure_reason(header, record),
                    progress_requested=progress_requested,
                    notifications_requested=notifications_requested,
                ),
                "marker_name": marker_name,
                "marker": read_failure_marker(record, marker_name),
            },
            by_name=True,
        )


class TerminalFailureHandled(DomainModel):
    status: Literal["HANDLED"]


class TerminalFailureSkipped(DomainModel):
    status: Literal["SKIPPED"]
    reason: Literal["JOB_CHANGED", "JOB_MISSING"]


class TerminalFailureRefused(DomainModel):
    status: Literal["REFUSED"]
    receipt: QuestionFailureRefusalReceipt


type TerminalFailureOutcome = TerminalFailureHandled | TerminalFailureSkipped | TerminalFailureRefused


class FailureJournalUnavailable(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Terminal failure journal persistence is unavailable")


class FailureJournalRejected(ValueError):
    def __init__(self) -> None:
        super().__init__("Terminal failure journal rejected its persistence request")


class FailureJournalScopeChanged(ValueError):
    def __init__(self) -> None:
        super().__init__("Terminal failure journal scope differs from its queue record")


class TerminalFailureOutcomeRecord(RootModel[TerminalFailureOutcome]):
    model_config = ConfigDict(strict=True, hide_input_in_errors=True, revalidate_instances="always")
