"""Queue wire admission for concrete worker execution and sparse dependency facts."""

from datetime import timedelta
from typing import Literal

from pydantic import JsonValue, ValidationError

from tank.contracts.job_repository import QueueStore
from tank.contracts.jobs import ClaimedJob, InvalidJobClaim, JobClaimMutation, JobEnvelope
from tank.job_repository import JobRepository
from tank.runtime_dispatch_contracts import (
    ClaimedWork,
    DependencyFailure,
    DependencyReadiness,
    DependencyState,
    FailedDependency,
    MissingDependency,
    ReadyDependencies,
    RequeueRequest,
    RunningJobClaim,
    RuntimeCommandRecord,
    WaitingDependencies,
    WorkerFailureRequest,
)
from tank.time_precision import Timestamp, offset_schedule
from tank.util import parse_time, sha256


class RepositoryWorkerQueue:
    def __init__(self, store: QueueStore) -> None:
        self.store = store
        self.jobs = JobRepository(store)

    async def claim(self) -> ClaimedWork | InvalidJobClaim | None:
        claimed = await self.jobs.claim_with_revision()
        if claimed is None or isinstance(claimed, InvalidJobClaim):
            return claimed
        envelope = claimed.envelope
        try:
            command = RuntimeCommandRecord.model_validate(
                {"type": envelope.type, "payload": envelope.payload}
            ).root
        except ValidationError as error:
            return self.invalid_claim(claimed, error, ("payload",))
        try:
            return ClaimedWork.model_validate(
                {
                    "job": envelope,
                    "command": command,
                    "payload_revision": sha256(envelope.payload),
                },
                by_name=True,
            )
        except ValidationError as error:
            return self.invalid_claim(claimed, error, ("claim",))

    @staticmethod
    def invalid_claim(
        claimed: ClaimedJob, error: ValidationError, fields: tuple[str, ...]
    ) -> InvalidJobClaim:
        return InvalidJobClaim(
            claim_token=claimed.claim_token,
            job_id=claimed.envelope.job_id,
            kind=claimed.envelope.type,
            attempts=claimed.envelope.attempts,
            revision=claimed.revision,
            error_codes=tuple(
                sorted(
                    {
                        entry["type"]
                        for entry in error.errors(
                            include_input=False,
                            include_context=False,
                            include_url=False,
                        )
                    }
                )
            ),
            error_fields=fields,
        )

    async def reject_invalid(self, invalid: InvalidJobClaim) -> bool:
        return await self.jobs.reject_invalid(invalid)

    async def current(self, claim: RunningJobClaim) -> bool:
        """Observe current claim scope before dispatch; this read grants no network lease."""
        claim = RunningJobClaim.model_validate(claim)
        document = await self.store.get("ops.jobs", claim.job_id)
        return document is not None and claim.matches(document)

    async def terminal(self, claim: RunningJobClaim) -> JobEnvelope | None:
        claim = RunningJobClaim.model_validate(claim)
        document = await self.store.get("ops.jobs", claim.job_id)
        if document is None:
            return None
        current = JobEnvelope.model_validate(document)
        if (
            current.status not in {"FAILED", "EXPIRED"}
            or current.job_id != claim.job_id
            or current.type != claim.kind
            or current.claim_token != claim.claim_token
            or current.attempts != claim.attempts
            or current.recovery_count != claim.recovery_count
            or sha256(current.payload) != claim.payload_revision
        ):
            return None
        return current

    async def interrupt(self, claim: RunningJobClaim, observed_at: Timestamp) -> bool:
        claim = RunningJobClaim.model_validate(claim)
        observed_at = Timestamp.model_validate(observed_at)
        if observed_at.value is None:
            raise ValueError("Controlled interruption requires its actual controller clock")

        def interrupted(current: JobEnvelope) -> JobClaimMutation:
            return JobClaimMutation(
                status="QUEUED",
                attempts=current.attempts - 1,
                run_after=observed_at,
                finished_at=Timestamp(None, None, "PENDING"),
                interrupted_at=observed_at,
                interruption_reason="CONTROLLED_GENERATION_STOP",
            )

        return await self.store.transition_claimed_job(claim, interrupted) is not None

    async def dependency(self, job_id: str) -> DependencyState | None:
        record = await self.store.get("ops.jobs", job_id)
        if record is None:
            return None
        dependency = DependencyState.model_validate(
            {field: record[field] for field in ("job_id", "status") if field in record}
        )
        if dependency.job_id != job_id:
            raise ValueError("Job dependency identity differs from its storage key")
        return dependency

    async def dependencies(self, work: ClaimedWork) -> DependencyReadiness:
        waiting: list[str] = []
        for job_id in work.command.payload.depends_on:
            dependency = await self.dependency(job_id)
            if dependency is None:
                raise DependencyFailure(MissingDependency(job_id=job_id, reason="MISSING"))
            if dependency.status in {"FAILED", "EXPIRED"}:
                raise DependencyFailure(
                    FailedDependency.model_validate(
                        {
                            "job_id": dependency.job_id,
                            "reason": dependency.status,
                        }
                    )
                )
            if dependency.status != "DONE":
                waiting.append(dependency.job_id)
        return (
            WaitingDependencies(status="WAITING", job_ids=tuple(waiting))
            if waiting
            else ReadyDependencies(status="READY")
        )

    async def requeue(self, request: RequeueRequest) -> bool:
        request = RequeueRequest.model_validate(request, by_name=True)

        def defer(current: JobEnvelope) -> JobClaimMutation:
            pending = Timestamp(None, None, "PENDING")
            if request.reason == "SESSION_ADMISSION" and current.slot_waiting_since is None:
                return JobClaimMutation(
                    status="QUEUED",
                    attempts=current.attempts - 1,
                    run_after=request.not_before,
                    finished_at=pending,
                    slot_waiting_since=Timestamp(
                        current.created_at,
                        current.created_at_prec,
                        current.created_at_prec_reason,
                        current.created_at_bucket_anchor,
                    ),
                )
            return JobClaimMutation(
                status="QUEUED",
                attempts=current.attempts - 1,
                run_after=request.not_before,
                finished_at=pending,
            )

        return await self.store.transition_claimed_job(request.claim, defer) is not None

    async def expire(self, claim: RunningJobClaim, observed_at: Timestamp) -> bool:
        claim = RunningJobClaim.model_validate(claim)
        observed_at = Timestamp.model_validate(observed_at)
        observed_value = observed_at.value
        if observed_value is None:
            raise ValueError("Worker expiry requires an actual controller observation clock")

        def expire_due(current: JobEnvelope) -> JobClaimMutation:
            if current.expires_at is None or parse_time(current.expires_at) > parse_time(observed_value):
                raise _ExpiryNotDue
            return JobClaimMutation(status="EXPIRED", finished_at=observed_at)

        try:
            return await self.store.transition_claimed_job(claim, expire_due) is not None
        except _ExpiryNotDue:
            return False

    async def complete(
        self, claim: RunningJobClaim, result_wire: JsonValue, observed_at: Timestamp
    ) -> JobEnvelope | None:
        """Persist only the serialized result of an acknowledged, still-owned command."""
        claim = RunningJobClaim.model_validate(claim)
        observed_at = Timestamp.model_validate(observed_at)
        if observed_at.value is None:
            raise ValueError("Worker completion requires an actual controller observation")

        mutation = JobClaimMutation(status="DONE", result=result_wire, finished_at=observed_at)

        def complete_current(_current: JobEnvelope) -> JobClaimMutation:
            return mutation

        return await self.store.transition_claimed_job(claim, complete_current)

    async def fail(self, request: WorkerFailureRequest) -> JobEnvelope | None:
        request = WorkerFailureRequest.model_validate(request)
        observed_value = request.observed_at.value
        if observed_value is None:
            raise ValueError("Worker failure requires an actual controller observation")

        def fail_current(current: JobEnvelope) -> JobClaimMutation:
            expired = current.expires_at is not None and parse_time(current.expires_at) <= parse_time(
                observed_value
            )
            status: Literal["EXPIRED", "FAILED", "QUEUED"] = (
                "EXPIRED" if expired else "FAILED" if current.attempts >= current.max_attempts else "QUEUED"
            )
            return JobClaimMutation(
                status=status,
                last_error=request.error_class,
                run_after=offset_schedule(request.observed_at, timedelta(seconds=request.schedule.seconds)),
                finished_at=(request.observed_at if status != "QUEUED" else Timestamp(None, None, "PENDING")),
            )

        return await self.store.transition_claimed_job(request.claim, fail_current)


class _ExpiryNotDue(Exception):
    """The freshly locked job has not reached its declared expiry."""
