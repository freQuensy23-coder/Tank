"""Persisted metadata projections and sparse admission updates at the repository boundary."""

from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import timedelta
from typing import Literal, Protocol, Self

from pydantic import Field, JsonValue

from tank.agents.admission import SessionAdmission
from tank.contracts import DomainModel
from tank.contracts.session_admission import (
    ADMITTED_JOB_TYPES,
    JOB_ADMISSION_FIELDS,
    RESERVED_JOB_FIELDS,
    SESSION_ADMISSION_FIELDS,
    AdmissionCandidate,
    AdmissionJobKind,
    AdmissionOrder,
    AdmissionPolicy,
    AdmissionReservation,
    JobSlot,
    SessionAdmissionState,
    SessionTarget,
    WakeCandidate,
)
from tank.runtime_contracts import ScoutReviewJob, replace_job_clock
from tank.time_precision import Timestamp
from tank.util import parse_time


class AdmissionStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(
        self, collection: str, key: str, record: dict[str, JsonValue], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def iter_metadata(
        self,
        collection: str,
        fields: Sequence[str],
        *,
        filters: dict[str, JsonValue] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[dict[str, JsonValue]]: ...


class AdmissionPayloadWire(DomainModel):
    job_id: str = Field(min_length=1)
    type: AdmissionJobKind
    priority: int = Field(ge=0, le=4)
    payload: dict[str, JsonValue]


class CandidateAdmissionWire(AdmissionPayloadWire):
    status: Literal["RUNNING"]


class WaitingAdmissionWire(AdmissionPayloadWire):
    status: Literal["QUEUED"]
    slot_waiting_since: str


class ReservedAdmissionWire(DomainModel):
    job_id: str = Field(min_length=1)
    type: AdmissionJobKind
    status: Literal["RUNNING"]
    slot_admitted: bool


class FirstAdmissionClockWire(DomainModel):
    created_at: str


class WaitingAdmissionClockWire(DomainModel):
    slot_waiting_since: str


def admission_policy(document: dict[str, JsonValue]) -> AdmissionPolicy:
    return AdmissionPolicy.model_validate(
        {field: document[field] for field in ("max_running_sessions",) if field in document}
    )


def session_target(payload: dict[str, JsonValue]) -> SessionTarget:
    return SessionTarget.model_validate(
        {field: payload.get(field) for field in ("session_id", "event_id", "item_id", "proposal_id")}
    )


def session_admission_state(document: dict[str, JsonValue]) -> SessionAdmissionState:
    return SessionAdmissionState.model_validate(
        {field: document[field] for field in ("session_id", "status") if field in document}
        | {field: document.get(field) for field in ("runtime_status", "active_message_id")}
    )


def payload_target(wire: AdmissionPayloadWire) -> SessionTarget:
    if wire.type == "SCOUT_REVIEW":
        proposal = ScoutReviewJob.model_validate(wire.payload)
        return SessionTarget(session_id=None, event_id=None, item_id=None, proposal_id=proposal.proposal_id)
    return session_target(wire.payload)


def admission_candidate(document: dict[str, JsonValue]) -> AdmissionCandidate:
    wire = CandidateAdmissionWire.model_validate(
        {
            field: document[field]
            for field in ("job_id", "type", "status", "priority", "payload")
            if field in document
        }
    )
    if document.get("slot_waiting_since") is None:
        clock = FirstAdmissionClockWire.model_validate(
            {field: document[field] for field in ("created_at",) if field in document}
        )
        since = parse_time(clock.created_at)
    else:
        waiting = WaitingAdmissionClockWire.model_validate(
            {"slot_waiting_since": document["slot_waiting_since"]}
        )
        since = parse_time(waiting.slot_waiting_since)
    return AdmissionCandidate(
        job_id=wire.job_id,
        target=payload_target(wire),
        order=AdmissionOrder(priority=wire.priority, since=since),
    )


class SessionAdmissionRepository:
    def __init__(self, store: AdmissionStore) -> None:
        self.store = store

    @asynccontextmanager
    async def lock(self) -> AsyncIterator[None]:
        async with self.store.lock("session-admission"):
            yield

    async def find(self, target: SessionTarget) -> SessionAdmissionState | None:
        for field, value in (
            ("session_id", target.session_id),
            ("event_id", target.event_id),
            ("item_id", target.item_id if target.event_id is None else None),
            ("proposal_id", target.proposal_id),
        ):
            if value is not None:
                async for row in self.store.iter_metadata(
                    "ops.agent_sessions", SESSION_ADMISSION_FIELDS, filters={field: value}
                ):
                    return session_admission_state(row)
                if field == "session_id":
                    return None
        return None

    async def open_sessions(self) -> tuple[SessionAdmissionState, ...]:
        return tuple(
            [
                session_admission_state(row)
                async for row in self.store.iter_metadata(
                    "ops.agent_sessions", SESSION_ADMISSION_FIELDS, filters={"status": "OPEN"}
                )
            ]
        )

    async def reserved_jobs(self) -> tuple[JobSlot, ...]:
        result: list[JobSlot] = []
        for kind in ADMITTED_JOB_TYPES:
            async for row in self.store.iter_metadata(
                "ops.jobs",
                RESERVED_JOB_FIELDS,
                filters={"type": kind, "status": "RUNNING", "slot_admitted": True},
            ):
                wire = ReservedAdmissionWire.model_validate(
                    {field: row[field] for field in RESERVED_JOB_FIELDS if field in row}
                )
                if wire.slot_admitted is not True:
                    raise ValueError("Running reservation is not explicitly admitted")
                result.append(JobSlot(job_id=wire.job_id))
        return tuple(result)

    async def waiting_jobs(self) -> tuple[AdmissionCandidate, ...]:
        result: list[AdmissionCandidate] = []
        for kind in ADMITTED_JOB_TYPES:
            async for row in self.store.iter_metadata(
                "ops.jobs", JOB_ADMISSION_FIELDS, filters={"type": kind, "status": "QUEUED"}
            ):
                if row.get("slot_waiting_since") is None:
                    continue
                wire = WaitingAdmissionWire.model_validate(
                    {field: row[field] for field in JOB_ADMISSION_FIELDS if field in row}
                )
                result.append(
                    AdmissionCandidate(
                        job_id=wire.job_id,
                        target=payload_target(wire),
                        order=AdmissionOrder(
                            priority=wire.priority, since=parse_time(wire.slot_waiting_since)
                        ),
                    )
                )
        return tuple(result)

    async def reserve(self, job_id: str) -> None:
        current = await self.store.get("ops.jobs", job_id)
        if current is None:
            raise LookupError("Claimed admission job no longer exists")
        reservation = AdmissionReservation.model_validate(
            {field: current[field] for field in ("job_id", "status") if field in current}
        )
        if reservation.job_id != job_id:
            raise ValueError("Admission job identity conflicts with its storage key")
        await self.store.put("ops.jobs", job_id, {**current, "slot_admitted": True})

    async def wake(self, job_id: str, when: Timestamp) -> None:
        current = await self.store.get("ops.jobs", job_id)
        if current is None:
            return
        candidate = WakeCandidate.model_validate(
            {field: current[field] for field in ("job_id", "status") if field in current}
        )
        if candidate.job_id != job_id:
            raise ValueError("Waiting admission job identity conflicts with its storage key")
        if candidate.status == "QUEUED":
            await self.store.put("ops.jobs", job_id, replace_job_clock(current, "run_after", when))


def build_admission(
    store: AdmissionStore,
    configuration: dict[str, JsonValue],
    clock: Callable[[], Timestamp],
    retry_delay: timedelta,
) -> SessionAdmission:
    return SessionAdmission(
        SessionAdmissionRepository(store), admission_policy(configuration), clock, retry_delay
    )
