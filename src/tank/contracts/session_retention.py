"""Consumed session activity and archive eligibility facts without persistence I/O."""

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, field_validator

from tank.contracts import DomainModel
from tank.contracts.session_admission import SessionRuntimeStatus
from tank.time_precision import Timestamp, offset_schedule, propagate_timestamp, timestamp_owned_fields
from tank.util import parse_time

type RetentionIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]
type ArchiveLifecycle = Literal[
    "DETECTED",
    "SCORED",
    "TRACKED",
    "ANALYZING",
    "ALERTED",
    "MONITORING",
    "COOLDOWN",
    "INVALIDATED",
    "CLOSED",
    "MERGED",
]


class ObservedArchiveLifecycle(DomainModel):
    lifecycle: ArchiveLifecycle | None


class ObservedArchiveRuntime(DomainModel):
    runtime_status: SessionRuntimeStatus | None


class ArchiveDenied(DomainModel):
    reason: Literal[
        "UNREPORTED_EVENT_LIFECYCLE", "EVENT_NOT_TERMINAL", "UNREPORTED_SESSION_RUNTIME", "SESSION_NOT_IDLE"
    ]


class TerminalArchiveCandidate(DomainModel):
    session_id: RetentionIdentity
    event_id: RetentionIdentity
    lifecycle: Literal["CLOSED", "MERGED"]


class InvalidatedArchiveCandidate(DomainModel):
    session_id: RetentionIdentity
    event_id: RetentionIdentity
    lifecycle: Literal["INVALIDATED"]
    session_finalized_at: str | None

    @field_validator("session_finalized_at")
    @classmethod
    def reported_finalization_clock(cls, value: str | None) -> str | None:
        if value is not None:
            parse_time(value)
        return value


type ArchiveCandidate = ArchiveDenied | TerminalArchiveCandidate | InvalidatedArchiveCandidate


def archive_candidate(session: Mapping[str, JsonValue], event: Mapping[str, JsonValue]) -> ArchiveCandidate:
    state = ObservedArchiveLifecycle.model_validate({"lifecycle": event.get("lifecycle")})
    if state.lifecycle is None:
        return ArchiveDenied(reason="UNREPORTED_EVENT_LIFECYCLE")
    if state.lifecycle not in ("CLOSED", "MERGED", "INVALIDATED"):
        return ArchiveDenied(reason="EVENT_NOT_TERMINAL")
    runtime = ObservedArchiveRuntime.model_validate({"runtime_status": session.get("runtime_status")})
    if runtime.runtime_status is None:
        return ArchiveDenied(reason="UNREPORTED_SESSION_RUNTIME")
    if runtime.runtime_status != "IDLE":
        return ArchiveDenied(reason="SESSION_NOT_IDLE")
    scope = {"session_id": session["session_id"], "event_id": event["event_id"], "lifecycle": state.lifecycle}
    if session.get("event_id") is not None and session["event_id"] != event["event_id"]:
        raise ValueError("Archive event differs from the session's actual event")
    if state.lifecycle == "INVALIDATED":
        return InvalidatedArchiveCandidate.model_validate(
            {**scope, "session_finalized_at": event.get("session_finalized_at")}
        )
    return TerminalArchiveCandidate.model_validate(scope)


class UnreportedEventBinding(DomainModel):
    kind: Literal["UNREPORTED"] = "UNREPORTED"


class UnboundRetentionEvent(DomainModel):
    kind: Literal["UNBOUND"] = "UNBOUND"


class BoundRetentionEvent(DomainModel):
    kind: Literal["BOUND"] = "BOUND"
    event_id: RetentionIdentity


type RetentionEventBinding = UnreportedEventBinding | UnboundRetentionEvent | BoundRetentionEvent


class RetainedSession(DomainModel):
    session_id: RetentionIdentity
    event: RetentionEventBinding
    no_event: bool | None
    followups_only: bool | None
    archive_after: str | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        event: RetentionEventBinding
        if "event_id" not in record:
            event = UnreportedEventBinding()
        elif record["event_id"] is None:
            event = UnboundRetentionEvent()
        else:
            event = BoundRetentionEvent.model_validate({"event_id": record["event_id"]})
        return cls.model_validate(
            {**{name: record.get(name) for name in cls.model_fields if name != "event"}, "event": event}
        )

    @property
    def bound_event_id(self) -> str | None:
        return self.event.event_id if isinstance(self.event, BoundRetentionEvent) else None

    @property
    def known_eventless(self) -> bool:
        return isinstance(self.event, UnboundRetentionEvent) and self.no_event is True

    @field_validator("archive_after")
    @classmethod
    def recorded_retention_clock(cls, value: str | None) -> str | None:
        if value is not None:
            parse_time(value)
        return value


class SessionSweepEntry(RetainedSession):
    status: Literal["OPEN"]
    runtime_status: SessionRuntimeStatus | None
    active_message_id: RetentionIdentity | None

    def should_probe(self, *, following: bool, periodic_probe: bool, now: datetime) -> bool:
        settled = self.runtime_status == "IDLE" and self.active_message_id is None
        kept = self.archive_after is not None and parse_time(self.archive_after) > now
        return not (settled and not following and (self.followups_only is True or kept or not periodic_probe))


class RecordedActivity(Timestamp):
    value: str

    @classmethod
    def from_timestamp(cls, timestamp: Timestamp) -> Self:
        timestamp = Timestamp.model_validate(timestamp)
        if timestamp.value is None:
            raise ValueError("Recorded activity requires its actual clock")
        return cls(
            value=timestamp.value,
            precision=timestamp.precision,
            precision_reason=timestamp.precision_reason,
            bucket_anchor=timestamp.bucket_anchor,
        )


class RetentionPolicy(DomainModel):
    keep_days: int = Field(ge=1)

    def deadline(self, activity: RecordedActivity) -> RecordedActivity:
        activity = RecordedActivity.model_validate(activity)
        return RecordedActivity.from_timestamp(offset_schedule(activity, timedelta(days=self.keep_days)))


def _recorded_activity(record: Mapping[str, JsonValue], names: Sequence[str]) -> tuple[Timestamp, ...]:
    return tuple(
        propagate_timestamp(record, name)
        for name in names
        if any(record.get(field) is not None for field in timestamp_owned_fields(name))
    )


class SessionActivity(DomainModel):
    session_id: RetentionIdentity
    clocks: tuple[Timestamp, ...]

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        clocks = list(_recorded_activity(record, ("opened_at", "last_event_at")))
        journal = record.get("message_log")
        if journal is not None:
            if not isinstance(journal, list):
                raise ValueError("Session activity journal must be an array")
            for entry in journal:
                if not isinstance(entry, dict):
                    raise ValueError("Session activity journal entry must be an object")
                clocks.extend(_recorded_activity(entry, ("requested_at", "sent_at")))
        return cls.model_validate({"session_id": record["session_id"], "clocks": tuple(clocks)})


class QuestionActivity(DomainModel):
    session_id: RetentionIdentity
    clocks: tuple[Timestamp, ...]

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue], session_id: str) -> Self:
        admitted = cls.model_validate(
            {
                "session_id": record["session_id"],
                "clocks": _recorded_activity(record, ("asked_at", "answered_at", "delivered_at")),
            }
        )
        if admitted.session_id != session_id:
            raise ValueError("Question activity belongs to a different session")
        return admitted


def latest_activity(session: SessionActivity, questions: Sequence[QuestionActivity]) -> RecordedActivity:
    session = SessionActivity.model_validate(session)
    clocks = list(session.clocks)
    for question in questions:
        question = QuestionActivity.model_validate(question)
        if question.session_id != session.session_id:
            raise ValueError("Question activity belongs to a different session")
        clocks.extend(question.clocks)
    known = [RecordedActivity.from_timestamp(stamp) for stamp in clocks if stamp.value is not None]
    if not known:
        raise ValueError("Session has no recorded message time")
    return max(known, key=lambda stamp: parse_time(stamp.value))


class FollowupPublicationScope(DomainModel):
    session_id: RetentionIdentity
    event_id: RetentionIdentity | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({"session_id": record["session_id"], "event_id": record.get("event_id")})


class FollowupQuestionScope(DomainModel):
    session_id: RetentionIdentity


class FollowupAlertScope(DomainModel):
    alert_id: RetentionIdentity
    event_id: RetentionIdentity


class FollowupDeliveryScope(DomainModel):
    alert_id: RetentionIdentity
    channel: Literal["telegram:alerts"]
    status: Literal["SENT"]


class PendingArchiveTool(DomainModel):
    session_id: RetentionIdentity
    status: Literal["PENDING"]

    @property
    def pending(self) -> bool:
        return True


class ReturnedArchiveTool(DomainModel):
    session_id: RetentionIdentity
    status: Literal["DONE", "ERROR", "REFUSED", "SKIPPED"] | None
    returned_at: str | None

    @field_validator("returned_at")
    @classmethod
    def reported_return_clock(cls, value: str | None) -> str | None:
        if value is not None:
            parse_time(value)
        return value

    @property
    def pending(self) -> bool:
        return self.returned_at is None


type ArchiveTool = PendingArchiveTool | ReturnedArchiveTool


def archive_tool(record: Mapping[str, JsonValue], session_id: str) -> ArchiveTool:
    scope = {"session_id": record["session_id"], "status": record.get("status")}
    if scope["session_id"] != session_id:
        raise ValueError("Archive tool belongs to a different session")
    if scope["status"] == "PENDING":
        return PendingArchiveTool.model_validate(scope)
    return ReturnedArchiveTool.model_validate({**scope, "returned_at": record.get("returned_at")})


class ArchiveEventBlocker(DomainModel):
    event_id: RetentionIdentity
    status: Literal["OPEN", "ARMED"]


class PendingArchiveJob(DomainModel):
    status: Literal["QUEUED", "RUNNING"]
    event_id: RetentionIdentity | None
    session_id: RetentionIdentity | None
    alert_id: RetentionIdentity | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        payload = record["payload"]
        if not isinstance(payload, dict):
            raise ValueError("Pending archive job requires its actual payload object")
        return cls.model_validate(
            {
                "status": record["status"],
                **{name: payload.get(name) for name in ("event_id", "session_id", "alert_id")},
            }
        )

    def blocks(self, *, session_id: str, event_id: str, alert_ids: set[str]) -> bool:
        return self.session_id == session_id or self.event_id == event_id or self.alert_id in alert_ids
