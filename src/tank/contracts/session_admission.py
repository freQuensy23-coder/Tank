"""Required admission facts; native execution and durable queue payloads have separate owners."""

from datetime import datetime
from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp

type AdmissionJobKind = Literal["AGENT_MESSAGE", "SCOUT_REVIEW"]
type SessionRuntimeStatus = Literal["PENDING", "RUNNING", "REQUIRES_ACTION", "IDLE", "ENDED"]
ADMITTED_JOB_TYPES: tuple[AdmissionJobKind, ...] = ("AGENT_MESSAGE", "SCOUT_REVIEW")
BUSY_RUNTIME_STATES = frozenset({"PENDING", "RUNNING", "REQUIRES_ACTION"})
SESSION_ADMISSION_FIELDS = ("session_id", "status", "runtime_status", "active_message_id")
JOB_ADMISSION_FIELDS = ("job_id", "type", "status", "priority", "payload", "slot_waiting_since")
RESERVED_JOB_FIELDS = ("job_id", "type", "status", "slot_admitted")


class AdmissionDeferred(Exception):
    def __init__(self, not_before: Timestamp) -> None:
        self.not_before = not_before
        super().__init__("Running-session cap reached; the message waits for a free slot")


class AdmissionPolicy(DomainModel):
    max_running_sessions: int = Field(ge=1)


class SessionTarget(DomainModel):
    session_id: str | None = Field(min_length=1)
    event_id: str | None = Field(min_length=1)
    item_id: str | None = Field(min_length=1)
    proposal_id: str | None = Field(min_length=1)

    @model_validator(mode="after")
    def has_routing_identity(self) -> Self:
        if all(value is None for value in (self.session_id, self.event_id, self.item_id, self.proposal_id)):
            raise ValueError("Session admission requires a declared routing identity")
        return self


class SessionAdmissionState(DomainModel):
    session_id: str = Field(min_length=1)
    status: Literal["OPEN", "FAILED", "ENDED"]
    runtime_status: SessionRuntimeStatus | None
    active_message_id: str | None = Field(min_length=1)

    def running(self) -> bool:
        return self.status == "OPEN" and (
            self.runtime_status in BUSY_RUNTIME_STATES or self.active_message_id is not None
        )


class AdmissionOrder(DomainModel):
    priority: int = Field(ge=0, le=4)
    since: AwareDatetime

    def precedes(self, other: "AdmissionOrder") -> bool:
        return self.priority < other.priority or (
            self.priority == other.priority and self.since < other.since
        )


class AdmissionCandidate(DomainModel):
    job_id: str = Field(min_length=1)
    target: SessionTarget
    order: AdmissionOrder


class SessionSlot(DomainModel):
    session_id: str = Field(min_length=1)


class JobSlot(DomainModel):
    job_id: str = Field(min_length=1)


type OccupiedSlot = SessionSlot | JobSlot


class AdmissionSlots(DomainModel):
    occupied: frozenset[OccupiedSlot]
    waiting: tuple[AdmissionCandidate, ...]


class AdmissionReservation(DomainModel):
    job_id: str = Field(min_length=1)
    status: Literal["RUNNING"]


class WakeCandidate(DomainModel):
    job_id: str = Field(min_length=1)
    status: Literal["QUEUED", "RUNNING", "DONE", "FAILED", "EXPIRED"]


def order_key(job: AdmissionCandidate) -> tuple[int, datetime, str]:
    return job.order.priority, job.order.since, job.job_id
