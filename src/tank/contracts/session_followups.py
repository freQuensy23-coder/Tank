"""Pure follow-up eligibility from observed local and native lifecycle facts."""

from collections.abc import Mapping
from typing import Literal, Protocol, Self

from pydantic import Field, JsonValue, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.native_readback import NativeLifecycle, NativeSessionMissing, NativeSessionState
from tank.time_precision import timestamp_owned_fields
from tank.util import parse_time


class TerminationEvidence(DomainModel):
    event_id: str = Field(min_length=1)


class LegacyNativeObservation(DomainModel):
    """Unreported historical lifecycle fields remain unknown, never idle or unarchived."""

    observed_session_id: str | None
    status: NativeLifecycle | None
    archived_at: str | None

    @field_validator("archived_at")
    @classmethod
    def observed_archive_clock(cls, value: str | None) -> str | None:
        if value is not None:
            parse_time(value)
        return value


type FollowupNativeObservation = NativeSessionState | NativeSessionMissing | LegacyNativeObservation


class FollowupSession(DomainModel):
    session_id: str = Field(min_length=1)
    status: Literal["OPEN", "FAILED", "ENDED"] | None
    replay_run_id: str | None = Field(min_length=1)
    termination: TerminationEvidence | None
    remote: FollowupNativeObservation | None
    archive_reason: str | None = Field(min_length=1)
    archive_keep_days: int | None = Field(ge=1)

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        """Project only consumed facts without rewriting or reconstructing historical rows."""
        identity = record["session_id"]
        termination = record.get("termination_receipt")
        terminal: JsonValue = None
        if termination is not None:
            if not isinstance(termination, dict):
                raise ValueError("Termination evidence must be an object")
            if termination:
                terminal = {"event_id": termination["event_id"]}
        remote = _remote_observation(record.get("remote_state"))
        result = cls.model_validate(
            {
                "session_id": identity,
                "status": record.get("status"),
                "replay_run_id": record.get("replay_run_id"),
                "termination": terminal,
                "remote": remote,
                "archive_reason": record.get("archive_reason"),
                "archive_keep_days": record.get("archive_keep_days"),
            }
        )
        return result

    @model_validator(mode="after")
    def consistent_observed_identity(self) -> Self:
        observation = self.remote
        observed_id: str | None
        if isinstance(observation, (NativeSessionState, NativeSessionMissing)):
            observed_id = observation.session_id
        elif isinstance(observation, LegacyNativeObservation):
            observed_id = observation.observed_session_id
        else:
            observed_id = None
        if observed_id is not None and observed_id != self.session_id:
            raise ValueError("Native follow-up observation belongs to another session")
        return self


def _remote_observation(value: JsonValue) -> JsonValue:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("Native remote observation must be an object")
    if "kind" in value or "missing" in value:
        clock_fields = timestamp_owned_fields("checked_at")
        return {key: field for key, field in value.items() if key not in clock_fields}
    observed_id = value.get("session_id")
    resource_id = value.get("id")
    if observed_id is not None and resource_id is not None and observed_id != resource_id:
        raise ValueError("Historical native identity declarations disagree")
    return {
        "observed_session_id": observed_id if observed_id is not None else resource_id,
        "status": value.get("status"),
        "archived_at": value.get("archived_at"),
    }


class FollowupCandidate(DomainModel):
    """Local preflight permits native readback, never authorizes input by itself."""

    status: Literal["CANDIDATE"]


class NoSession(DomainModel):
    status: Literal["NO_SESSION"]
    reason: Literal["ABSENT", "REPLAY", "NATIVE_MISSING"]


class Terminated(DomainModel):
    status: Literal["TERMINATED"]
    reason: Literal["TERMINATED"]


class Archived(DomainModel):
    status: Literal["ARCHIVED"]
    reason: Literal["ARCHIVED", "EARLY_CLOSURE", "RETENTION_UNREPORTED"]


class RetentionArchived(DomainModel):
    status: Literal["ARCHIVED"]
    reason: Literal["RETENTION"]
    keep_days: int = Field(ge=1)


type FollowupClosure = NoSession | Terminated | Archived | RetentionArchived
type FollowupEligibility = FollowupCandidate | FollowupClosure


class FollowupRefusalTarget(DomainModel):
    question_id: str = Field(min_length=1, pattern=r"\S")
    session_id: str = Field(min_length=1, pattern=r"\S")


class FollowupRefusalSink(Protocol):
    async def record(self, target: FollowupRefusalTarget, closure: FollowupClosure) -> None: ...


def followup_eligibility(session: FollowupSession | None) -> FollowupEligibility:
    if session is None:
        return NoSession(status="NO_SESSION", reason="ABSENT")
    if session.replay_run_id is not None:
        return NoSession(status="NO_SESSION", reason="REPLAY")
    remote = session.remote
    if session.termination is not None or (
        isinstance(remote, (NativeSessionState, LegacyNativeObservation)) and remote.status == "terminated"
    ):
        return Terminated(status="TERMINATED", reason="TERMINATED")
    if isinstance(remote, NativeSessionMissing):
        return NoSession(status="NO_SESSION", reason="NATIVE_MISSING")
    archived = (
        isinstance(remote, (NativeSessionState, LegacyNativeObservation)) and remote.archived_at is not None
    )
    if session.status != "ENDED" and not archived:
        return FollowupCandidate(status="CANDIDATE")
    if session.archive_reason == "RETENTION":
        if session.archive_keep_days is None:
            return Archived(status="ARCHIVED", reason="RETENTION_UNREPORTED")
        return RetentionArchived(status="ARCHIVED", reason="RETENTION", keep_days=session.archive_keep_days)
    if session.status == "ENDED" and session.archive_reason is None:
        return Archived(status="ARCHIVED", reason="EARLY_CLOSURE")
    return Archived(status="ARCHIVED", reason="ARCHIVED")
