"""Queue wire admission for concrete worker execution and sparse dependency facts."""

from pydantic import ValidationError

from tank.contracts.job_repository import JobRepository, QueueStore
from tank.contracts.jobs import InvalidJobClaim, JobMetadata
from tank.runtime_dispatch_contracts import ClaimedWork, DependencyState, RuntimeCommandRecord
from tank.util import sha256


class RepositoryWorkerQueue:
    def __init__(self, store: QueueStore) -> None:
        self.store = store
        self.jobs = JobRepository(store)

    async def claim(self) -> ClaimedWork | InvalidJobClaim | None:
        envelope = await self.jobs.claim()
        if envelope is None or isinstance(envelope, InvalidJobClaim):
            return envelope
        try:
            command = RuntimeCommandRecord.model_validate(
                {"type": envelope.type, "payload": envelope.payload}
            ).root
        except ValidationError as error:
            return InvalidJobClaim(
                job_id=envelope.job_id,
                kind=envelope.type,
                attempts=envelope.attempts,
                revision=sha256(envelope.model_dump(mode="json", exclude_unset=True)),
                error_codes=tuple(
                    sorted(
                        {
                            entry["type"]
                            for entry in error.errors(
                                include_input=False, include_context=False, include_url=False
                            )
                        }
                    )
                ),
                error_fields=("payload",),
            )
        metadata = JobMetadata.model_validate(
            {
                field: value
                for field, value in envelope.model_dump(mode="json", exclude_unset=True).items()
                if field in JobMetadata.model_fields
            }
        )
        return ClaimedWork.model_validate({"job": metadata, "command": command}, by_name=True)

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
