"""Concrete typed ops.jobs persistence adapter over the inward queue capability port."""

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager

from pydantic import ValidationError

from tank.contracts.job_repository import QueueStore
from tank.contracts.jobs import ClaimedJob, InvalidJobClaim, JobClaimRevision, JobEnvelope
from tank.contracts.repository import Collection
from tank.time_precision import Timestamp
from tank.util import JsonInput, TimeInput, sha256


class JobRepository:
    def __init__(self, store: QueueStore) -> None:
        self._store = store
        self._collection = Collection("ops.jobs", JobEnvelope)

    def transaction(self) -> AbstractAsyncContextManager[QueueStore]:
        return self._store.transaction()

    def lock(self, key: str) -> AbstractAsyncContextManager[QueueStore]:
        return self._store.lock(key)

    async def get(self, job_id: str) -> JobEnvelope | None:
        document = await self._store.get("ops.jobs", job_id)
        return None if document is None else self._collection.decode(document)

    async def put(self, job: JobEnvelope) -> JobEnvelope:
        document = self._collection.encode(job, exclude_unset=True)
        stored = await self._store.put("ops.jobs", job.job_id, document)
        return self._collection.decode(stored)

    async def enqueue(
        self,
        kind: str,
        payload: Mapping[str, JsonInput],
        idem_key: str,
        *,
        priority: int = 1,
        run_after: TimeInput | Timestamp | None = None,
        expires_at: TimeInput | Timestamp | None = None,
        max_attempts: int = 3,
    ) -> JobEnvelope:
        document = await self._store.enqueue(
            kind,
            payload,
            idem_key,
            priority=priority,
            run_after=run_after,
            expires_at=expires_at,
            max_attempts=max_attempts,
        )
        return self._collection.decode(document)

    async def claim(self) -> JobEnvelope | InvalidJobClaim | None:
        claimed = await self.claim_with_revision()
        return claimed.envelope if isinstance(claimed, ClaimedJob) else claimed

    async def claim_with_revision(self) -> ClaimedJob | InvalidJobClaim | None:
        document = await self._store.claim_job()
        if document is None:
            return None
        claim = JobClaimRevision.from_document(document)
        if claim.status != "RUNNING":
            raise ValueError("A queue claim must hold a RUNNING revision")
        revision = sha256(document)
        try:
            return ClaimedJob(envelope=self._collection.decode(document), revision=revision)
        except ValidationError as error:
            fields: set[str] = set()
            codes: set[str] = set()
            for detail in error.errors(include_input=False, include_context=False, include_url=False):
                codes.add(detail["type"])
                location = detail["loc"]
                if not location:
                    fields.add("__root__")
                elif isinstance(location[0], str) and location[0] in JobEnvelope.model_fields:
                    fields.add(location[0])
                else:
                    fields.add("extra_field")
            return InvalidJobClaim(
                claim_token=claim.claim_token,
                job_id=claim.job_id,
                kind=claim.type,
                attempts=claim.attempts,
                revision=revision,
                error_codes=tuple(sorted(codes)),
                error_fields=tuple(sorted(fields)),
            )

    async def reject_invalid(self, invalid: InvalidJobClaim) -> bool:
        invalid = InvalidJobClaim.model_validate(invalid)
        return await self._store.quarantine_job_claim(invalid)

    async def finish(self, job_id: str) -> None:
        await self._store.finish_job(job_id)

    async def fail(self, job_id: str, error: str | Exception, *, retry_after: float = 1) -> None:
        await self._store.fail_job(job_id, error, retry_after=retry_after)
