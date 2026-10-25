"""Session admission preserves queue order while letting an already-running session receive input."""

from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Protocol

from tank.contracts.session_admission import (
    AdmissionCandidate,
    AdmissionDeferred,
    AdmissionPolicy,
    AdmissionSlots,
    JobSlot,
    OccupiedSlot,
    SessionAdmissionState,
    SessionSlot,
    SessionTarget,
    order_key,
)
from tank.time_precision import Timestamp, offset_schedule


class AdmissionRepository(Protocol):
    def lock(self) -> AbstractAsyncContextManager[None]: ...
    def find(self, target: SessionTarget) -> Awaitable[SessionAdmissionState | None]: ...
    def open_sessions(self) -> Awaitable[tuple[SessionAdmissionState, ...]]: ...
    def reserved_jobs(self) -> Awaitable[tuple[JobSlot, ...]]: ...
    def waiting_jobs(self) -> Awaitable[tuple[AdmissionCandidate, ...]]: ...
    def reserve(self, job_id: str) -> Awaitable[None]: ...
    def wake(self, job_id: str, when: Timestamp) -> Awaitable[None]: ...


class SessionAdmission:
    def __init__(
        self,
        repository: AdmissionRepository,
        policy: AdmissionPolicy,
        clock: Callable[[], Timestamp],
        retry_delay: timedelta,
    ) -> None:
        self.repository = repository
        self.policy = AdmissionPolicy.model_validate(policy)
        self.clock = clock
        if retry_delay.total_seconds() <= 0:
            raise ValueError("Session admission retry delay must be positive")
        self.retry_delay = retry_delay

    async def existing(self, target: SessionTarget) -> SessionAdmissionState | None:
        return await self.repository.find(target)

    async def slots(self, exclude_job: str | None = None) -> AdmissionSlots:
        occupied: set[OccupiedSlot] = {
            SessionSlot(session_id=session.session_id)
            for session in await self.repository.open_sessions()
            if session.running()
        }
        for slot in await self.repository.reserved_jobs():
            if slot.job_id != exclude_job:
                occupied.add(slot)
        waiting = [job for job in await self.repository.waiting_jobs() if job.job_id != exclude_job]
        return AdmissionSlots(occupied=frozenset(occupied), waiting=tuple(sorted(waiting, key=order_key)))

    async def admit(self, candidate: AdmissionCandidate) -> None:
        target = await self.existing(candidate.target)
        if target is not None and target.running():
            return
        async with self.repository.lock():
            slots = await self.slots(exclude_job=candidate.job_id)
            older = any(job.order.precedes(candidate.order) for job in slots.waiting)
            if older or len(slots.occupied) >= self.policy.max_running_sessions:
                raise AdmissionDeferred(offset_schedule(self.clock(), self.retry_delay))
            await self.repository.reserve(candidate.job_id)

    async def wake(self) -> None:
        async with self.repository.lock():
            slots = await self.slots()
            free = max(0, self.policy.max_running_sessions - len(slots.occupied))
            now = self.clock()
            if now.value is None:
                raise ValueError("Admission wake requires an actual collection clock")
            for job in slots.waiting:
                target = await self.existing(job.target)
                already_running = target is not None and target.running()
                if free > 0 or already_running:
                    if not already_running:
                        free -= 1
                    await self.repository.wake(job.job_id, now)
