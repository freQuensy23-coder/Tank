"""Typed queue-envelope adapter; concrete application commands belong to dispatchers."""

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol, Self

from pydantic import JsonValue

from tank.contracts.jobs import JobEnvelope
from tank.contracts.repository import Collection, DocumentStore
from tank.time_precision import Timestamp
from tank.util import JsonInput, TimeInput


class QueueStore(DocumentStore, Protocol):
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...

    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...

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
    ) -> dict[str, JsonValue]: ...

    async def claim_job(self) -> dict[str, JsonValue] | None: ...

    async def finish_job(self, job_id: str) -> None: ...

    async def fail_job(self, job_id: str, error: str | Exception, *, retry_after: float = 1) -> None: ...


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

    async def claim(self) -> JobEnvelope | None:
        document = await self._store.claim_job()
        return None if document is None else self._collection.decode(document)

    async def finish(self, job_id: str) -> None:
        await self._store.finish_job(job_id)

    async def fail(self, job_id: str, error: str | Exception, *, retry_after: float = 1) -> None:
        await self._store.fail_job(job_id, error, retry_after=retry_after)
