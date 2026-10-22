"""The migration lock serializes runners and never outlives a cancelled one."""

import asyncio

import pytest

from tank.storage import PostgresRepository

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture
async def blank_repo(postgres_template):
    import os

    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TANK_TEST_DATABASE_URL required; creates only an owned disposable database")
    async with postgres_template.repository(dsn, migrated=False) as repo:
        yield repo


async def test_migration_lock_serializes_concurrent_runners_and_releases_after_cancellation(blank_repo):
    repo = blank_repo
    other = await PostgresRepository(repo.database_url).open()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold():
        async with repo._migration_connection():
            entered.set()
            await release.wait()

    task = asyncio.create_task(hold())
    await entered.wait()
    waiting = asyncio.create_task(other.migrate())
    try:
        await asyncio.sleep(0.05)
        assert not waiting.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(waiting, timeout=300)

        await asyncio.wait_for(repo.migrate(), timeout=10)
    finally:
        release.set()
        for operation in (task, waiting):
            if not operation.done():
                operation.cancel()
        await asyncio.gather(task, waiting, return_exceptions=True)
        await other.close()


@pytest.mark.parametrize("lock_kind", ["session", "transaction"])
async def test_cancelled_waiting_migration_does_not_keep_session_lock(blank_repo, lock_kind):
    repo = blank_repo
    other = await PostgresRepository(repo.database_url).open()
    try:
        async with repo._migration_connection() if lock_kind == "session" else repo.lock("tank:migrations"):
            waiting = asyncio.create_task(other.migrate())
            await asyncio.sleep(0.05)
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
        await asyncio.wait_for(other.migrate(), timeout=300)
    finally:
        await other.close()
