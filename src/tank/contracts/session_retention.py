"""Consumed session activity and archive eligibility facts without persistence I/O."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, field_validator

from tank.contracts import DomainModel
from tank.contracts.session_admission import SessionRuntimeStatus
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
    reason: Literal["EVENT_NOT_TERMINAL", "SESSION_NOT_IDLE"]


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
    if state.lifecycle not in ("CLOSED", "MERGED", "INVALIDATED"):
        return ArchiveDenied(reason="EVENT_NOT_TERMINAL")
    runtime = ObservedArchiveRuntime.model_validate({"runtime_status": session.get("runtime_status")})
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


class RetainedSession(DomainModel):
    session_id: RetentionIdentity
    event_id: RetentionIdentity | None
    no_event: bool | None
    followups_only: bool | None
    archive_after: str | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({name: record.get(name) for name in cls.model_fields})

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


def _recorded_activity(record: Mapping[str, JsonValue], names: Sequence[str]) -> tuple[datetime, ...]:
    observed: list[datetime] = []
    for name in names:
        value = record.get(name)
        if value is None:
            continue
        if not isinstance(value, str):
            raise ValueError("Recorded session activity requires an aware clock")
        observed.append(parse_time(value))
    return tuple(observed)


class SessionActivity(DomainModel):
    session_id: RetentionIdentity
    clocks: tuple[datetime, ...]

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
    clocks: tuple[datetime, ...]

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


def latest_activity(session: SessionActivity, questions: Sequence[QuestionActivity]) -> datetime:
    session = SessionActivity.model_validate(session)
    clocks = list(session.clocks)
    for question in questions:
        question = QuestionActivity.model_validate(question)
        if question.session_id != session.session_id:
            raise ValueError("Question activity belongs to a different session")
        clocks.extend(question.clocks)
    if not clocks:
        raise ValueError("Session has no recorded message time")
    return max(clocks)


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


class ArchiveToolState(DomainModel):
    session_id: RetentionIdentity
    status: Literal["PENDING", "DONE", "ERROR", "REFUSED", "SKIPPED"] | None
    returned_at: str | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue], session_id: str) -> Self:
        admitted = cls.model_validate({name: record.get(name) for name in cls.model_fields})
        if admitted.session_id != session_id:
            raise ValueError("Archive tool belongs to a different session")
        return admitted

    @field_validator("returned_at")
    @classmethod
    def reported_return_clock(cls, value: str | None) -> str | None:
        if value is not None:
            parse_time(value)
        return value

    @property
    def pending(self) -> bool:
        return self.status == "PENDING" or self.returned_at is None


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
