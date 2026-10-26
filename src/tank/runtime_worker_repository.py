"""Queue wire admission for concrete worker execution and sparse dependency facts."""

from pydantic import ValidationError

from tank.contracts.job_repository import JobRepository, QueueStore
from tank.contracts.jobs import ClaimedJob, InvalidJobClaim, JobEnvelope, JobMetadata
from tank.runtime_contracts import replace_job_clock
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
)
from tank.time_precision import Timestamp
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
            metadata = JobMetadata.model_validate(
                {
                    field: value
                    for field, value in envelope.model_dump(mode="json", exclude_unset=True).items()
                    if field in JobMetadata.model_fields
                }
            )
            return ClaimedWork.model_validate(
                {
                    "job": metadata,
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

    @staticmethod
    def owns_running_claim(claim: RunningJobClaim, current: JobEnvelope) -> bool:
        return (
            current.job_id == claim.job_id
            and current.type == claim.type
            and current.status == "RUNNING"
            and current.attempts == claim.attempts
            and sha256(current.payload) == claim.payload_revision
        )

    async def requeue(self, request: RequeueRequest) -> bool:
        request = RequeueRequest.model_validate(request, by_name=True)
        claim = request.claim
        async with self.store.lock("job:" + claim.job_id):
            document = await self.store.get("ops.jobs", claim.job_id)
            if document is None:
                return False
            current = JobEnvelope.model_validate(document)
            if not self.owns_running_claim(claim, current):
                return False
            updated = replace_job_clock(document, "run_after", request.not_before)
            updated.update(status="QUEUED", attempts=current.attempts - 1)
            if request.reason == "SESSION_ADMISSION" and current.slot_waiting_since is None:
                created = Timestamp(
                    current.created_at,
                    current.created_at_prec,
                    current.created_at_prec_reason,
                    current.created_at_bucket_anchor,
                )
                updated = replace_job_clock(updated, "slot_waiting_since", created)
            JobEnvelope.model_validate(updated)
            await self.store.put("ops.jobs", current.job_id, updated)
            return True

    async def expire(self, claim: RunningJobClaim, observed_at: Timestamp) -> bool:
        claim = RunningJobClaim.model_validate(claim)
        if observed_at.value is None:
            raise ValueError("Worker expiry requires an actual controller observation clock")
        async with self.store.lock("job:" + claim.job_id):
            document = await self.store.get("ops.jobs", claim.job_id)
            if document is None:
                return False
            current = JobEnvelope.model_validate(document)
            if not self.owns_running_claim(claim, current):
                return False
            if current.expires_at is None or parse_time(current.expires_at) > parse_time(observed_at.value):
                return False
            updated = replace_job_clock(document, "finished_at", observed_at)
            updated["status"] = "EXPIRED"
            JobEnvelope.model_validate(updated)
            await self.store.put("ops.jobs", current.job_id, updated)
            return True
