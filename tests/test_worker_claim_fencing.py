"""A restarted controller must not inherit an older worker's completion authority."""

import asyncio
from collections.abc import Mapping

import pytest
from pydantic import JsonValue
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.jobs import JobEnvelope
from tank.runtime_dispatch_contracts import ClaimedWork
from tank.runtime_worker_repository import RepositoryWorkerQueue
from tank.storage import MemoryRepository, PostgresRepository, StorageBackend
from tank.time_precision import collection_clock
from tank.util import JsonInput


async def test_reclaimed_same_attempt_refuses_completion_from_previous_controller(
    repo: StorageBackend,
) -> None:
    job = await JobRepository(repo).enqueue("TELEGRAM_ANSWER", {"question_id": "question"}, "reclaim")
    queue = RepositoryWorkerQueue(repo)
    first = await queue.claim()
    assert isinstance(first, ClaimedWork)
    assert await repo.recover_jobs() == 1
    second = await queue.claim()
    assert isinstance(second, ClaimedWork)
    assert first.job.attempts == second.job.attempts == 1
    assert first.job.claim_token != second.job.claim_token
    assert second.job.recovery_count == 1
    before = await repo.get("ops.jobs", job.job_id)
    assert await queue.complete(first.scope(), {"stale": True}, collection_clock()) is None
    assert await repo.get("ops.jobs", job.job_id) == before
    assert await queue.complete(second.scope(), {"current": True}, collection_clock()) is not None


async def test_atomic_completion_serializes_recovery_until_real_write_commits(
    repo: StorageBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = await JobRepository(repo).enqueue("TELEGRAM_ANSWER", {"question_id": "question"}, "race")
    queue = RepositoryWorkerQueue(repo)
    first = await queue.claim()
    assert isinstance(first, ClaimedWork)
    writing = asyncio.Event()
    resume = asyncio.Event()
    original_put = repo.put
    completing: asyncio.Task[JobEnvelope | None] | None = None
    recovering: asyncio.Task[int] | None = None

    async def pause_before_write(
        collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        if asyncio.current_task() is completing and collection == "ops.jobs" and key == job.job_id:
            writing.set()
            await resume.wait()
        return await original_put(collection, key, record, immutable=immutable)

    monkeypatch.setattr(repo, "put", pause_before_write)
    completing = asyncio.create_task(queue.complete(first.scope(), {"authorized": True}, collection_clock()))
    try:
        await asyncio.wait_for(writing.wait(), timeout=3)
        recovering = asyncio.create_task(repo.recover_jobs())
        await asyncio.sleep(0)
        if isinstance(repo, PostgresRepository):
            pool = repo._pool
            assert pool is not None
            async with asyncio.timeout(3), pool.connection() as monitor:
                while True:
                    row = await (
                        await monitor.execute(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                            "WHERE datname=current_database() AND pid<>pg_backend_pid() "
                            "AND query LIKE 'UPDATE ops.jobs%' AND wait_event_type='Lock') AS waiting"
                        )
                    ).fetchone()
                    assert row is not None
                    observed = row["waiting"]
                    assert isinstance(observed, bool)
                    if observed:
                        break
                    await asyncio.sleep(0.01)
        else:
            assert isinstance(repo, MemoryRepository)
        assert not recovering.done()
        resume.set()
        completed = await completing
        assert completed is not None and completed.status == "DONE"
        assert await recovering == 0
        assert await queue.claim() is None
        stored = await repo.get("ops.jobs", job.job_id)
        assert stored is not None and stored["status"] == "DONE"
        assert stored["result"] == {"authorized": True}
    finally:
        resume.set()
        tasks = [task for task in (completing, recovering) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_non_consuming_deferral_does_not_restore_an_older_completion_authority(
    repo: StorageBackend,
) -> None:
    from tank.runtime_dispatch_contracts import RequeueRequest

    await JobRepository(repo).enqueue("TELEGRAM_ANSWER", {"question_id": "question"}, "deferral")
    queue = RepositoryWorkerQueue(repo)
    first = await queue.claim()
    assert isinstance(first, ClaimedWork)
    assert await queue.requeue(
        RequeueRequest(claim=first.scope(), not_before=collection_clock(), reason="DELIVERY_WAIT")
    )
    second = await queue.claim()
    assert isinstance(second, ClaimedWork)
    assert first.job.attempts == second.job.attempts == 1
    assert first.job.recovery_count is second.job.recovery_count is None
    assert first.job.claim_token != second.job.claim_token
    before = await repo.get("ops.jobs", second.job.job_id)
    assert await queue.complete(first.scope(), {"stale": True}, collection_clock()) is None
    assert await repo.get("ops.jobs", second.job.job_id) == before
