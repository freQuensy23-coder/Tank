"""Actual session lifecycle facts consumed by this controller's native follower."""

from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.native_readback import NativeLifecycle, NativeSessionMissing, NativeSessionState
from tank.contracts.session_admission import SessionRuntimeStatus


class FollowedSession(DomainModel):
    session_id: str = Field(min_length=1, pattern=r"\S")
    status: Literal["OPEN", "FAILED", "ENDED"]
    runtime_status: SessionRuntimeStatus | None
    event_id: str | None = Field(min_length=1)

    @classmethod
    def from_record(cls, session: dict[str, JsonValue], requested_id: str) -> Self:
        result = cls.model_validate(
            {
                "session_id": session["session_id"],
                "status": session["status"],
                "runtime_status": session.get("runtime_status"),
                "event_id": session.get("event_id"),
            }
        )
        if result.session_id != requested_id:
            raise ValueError("Followed session identity differs from its actual storage key")
        return result


class RecordedEventScope(DomainModel):
    session_id: str = Field(min_length=1, pattern=r"\S")
    tool: Literal["record_event"]


class RecordedEventDecision(DomainModel):
    is_event: bool | None

    @classmethod
    def from_record(cls, record: dict[str, JsonValue], session_id: str) -> Self:
        scope = RecordedEventScope.model_validate(
            {field: record[field] for field in ("session_id", "tool") if field in record}
        )
        if scope.session_id != session_id:
            raise ValueError("Recorded event decision belongs to a different session")
        result = record.get("result")
        if result is None:
            return cls(is_event=None)
        if not isinstance(result, dict):
            raise ValueError("Recorded event decision must be an object when reported")
        return cls.model_validate({"is_event": result.get("is_event")})


def native_runtime_status(status: NativeLifecycle) -> SessionRuntimeStatus:
    states: dict[NativeLifecycle, SessionRuntimeStatus] = {
        "idle": "IDLE",
        "running": "RUNNING",
        "rescheduling": "PENDING",
        "terminated": "ENDED",
    }
    return states[status]


class FollowerReady(DomainModel):
    phase: Literal["READY"]
    state: FollowedSession


class FollowerUnavailable(DomainModel):
    phase: Literal["UNAVAILABLE"]
    observation: NativeSessionMissing | NativeSessionState

    @model_validator(mode="after")
    def actual_unavailability(self) -> Self:
        if isinstance(self.observation, NativeSessionState) and self.observation.archived_at is None:
            raise ValueError("An unavailable native follower requires actual absence or archival")
        return self


type FollowerObservation = FollowerReady | FollowerUnavailable


class NativeFollowerUnavailable(RuntimeError):
    def __init__(self, observed: FollowerUnavailable) -> None:
        self.observed = observed
        super().__init__("Native follower resource is not currently available")
