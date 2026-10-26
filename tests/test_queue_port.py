"""The queue adapter requires only its actual persistence capabilities."""

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Self

import pytest
from pydantic import JsonValue

from tank.contracts.job_repository import JobRepository, QueueStore
from tank.contracts.jobs import JobEnvelope
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp
from tank.util import JsonInput, TimeInput


class QueueOnlyMemory:
    def __init__(self) -> None:
        self.source = MemoryRepository()

    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None:
        return await self.source.get(collection, key)

    async def put(
        self,
        collection: str,
        key: str,
        record: dict[str, JsonValue],
        *,
        immutable: bool = False,
    ) -> dict[str, JsonValue]:
        return await self.source.put(collection, key, record, immutable=immutable)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Self]:
        async with self.source.transaction():
            yield self

    @asynccontextmanager
    async def lock(self, key: str) -> AsyncIterator[Self]:
        async with self.source.lock(key):
            yield self

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
    ) -> dict[str, JsonValue]:
        return await self.source.enqueue(
            kind,
            payload,
            idem_key,
            priority=priority,
            run_after=run_after,
            expires_at=expires_at,
            max_attempts=max_attempts,
        )

    async def claim_job(self) -> dict[str, JsonValue] | None:
        return await self.source.claim_job()

    async def finish_job(self, job_id: str) -> None:
        await self.source.finish_job(job_id)

    async def fail_job(self, job_id: str, error: str | Exception, *, retry_after: float = 1) -> None:
        await self.source.fail_job(job_id, error, retry_after=retry_after)


async def test_queue_port_runs_real_lifecycle_without_document_query_capabilities() -> None:
    port: QueueStore = QueueOnlyMemory()
    jobs = JobRepository(port)
    queued = await jobs.enqueue("TEST", {"value": {"flag": True}}, "narrow-lifecycle")
    claimed = await jobs.claim()
    assert isinstance(claimed, JobEnvelope) and claimed.job_id == queued.job_id
    assert claimed.attempts == 1
    await jobs.fail(claimed.job_id, "temporary", retry_after=0)
    retried = await jobs.claim()
    assert isinstance(retried, JobEnvelope) and retried.attempts == 2
    await jobs.put(retried.model_copy(update={"slot_admitted": True}))
    await jobs.finish(retried.job_id)
    complete = await jobs.get(queued.job_id)
    assert complete is not None and complete.status == "DONE" and complete.slot_admitted is True
    assert complete.finished_at_prec == "COLLECTION_ONLY"
    assert await jobs.claim() is None


async def test_queue_port_keeps_actual_transaction_rollback() -> None:
    port: QueueStore = QueueOnlyMemory()
    jobs = JobRepository(port)
    key: str
    with pytest.raises(RuntimeError, match="abort queue transaction"):
        async with jobs.transaction():
            queued = await jobs.enqueue("TEST", {}, "narrow-rollback")
            key = queued.job_id
            raise RuntimeError("abort queue transaction")
    assert await jobs.get(key) is None
    assert await jobs.claim() is None
