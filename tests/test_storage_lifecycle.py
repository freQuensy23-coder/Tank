"""Actual PostgreSQL pool/transaction cleanup under iterator exit and cancellation."""

import asyncio
import logging
import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture
async def lifecycle_repo(postgres_template):
    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL CREATE DATABASE for owned lifecycle QA")
    async with postgres_template.repository(dsn, migrated=False) as repo:
        async with repo.transaction():
            await repo._conn().execute(
                "CREATE SCHEMA reg; CREATE TABLE reg.tokens(storage_key text PRIMARY KEY,document jsonb NOT NULL); INSERT INTO reg.tokens VALUES ('one','{\"token_id\":\"one\"}'),('two','{\"token_id\":\"two\"}')"
            )
        yield repo


async def test_pg_early_iterator_exit_releases_transaction(
    lifecycle_repo, caplog: pytest.LogCaptureFixture
) -> None:
    repo = lifecycle_repo
    caplog.set_level(logging.WARNING)
    iterator = repo.iter_all("reg.tokens", batch_size=1)
    async for row in iterator:
        assert row["token_id"] == "one"
        break

    assert repo._connection.get() is None
    assert repo._pool.get_stats()["pool_available"] == repo._pool.get_stats()["pool_size"]
    await iterator.aclose()
    assert await repo.get("reg.tokens", "two")
    assert not caplog.records


async def test_pg_repeated_cancel_during_transaction_exit(
    lifecycle_repo, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import psycopg

    repo = lifecycle_repo
    caplog.set_level(logging.WARNING)
    body_entered = asyncio.Event()
    exit_entered = asyncio.Event()
    release_exit = asyncio.Event()
    original = psycopg.AsyncTransaction.__aexit__
    target_connection = None

    async def delayed_exit(transaction, *args):
        if transaction._conn is target_connection:
            exit_entered.set()
            await release_exit.wait()
        return await original(transaction, *args)

    monkeypatch.setattr(psycopg.AsyncTransaction, "__aexit__", delayed_exit)

    async def worker() -> None:
        nonlocal target_connection
        async with repo.transaction():
            target_connection = repo._conn()
            await target_connection.execute("INSERT INTO reg.tokens VALUES ('cancelled','{}')")
            body_entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(worker())
    await body_entered.wait()
    task.cancel()
    await exit_entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    release_exit.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await repo.get("reg.tokens", "cancelled") is None
    assert target_connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    assert target_connection._num_transactions == 0
    assert repo._pool.get_stats().get("returns_bad", 0) == 0
    assert not caplog.records


async def test_pg_child_task_uses_own_connection_and_savepoints(lifecycle_repo) -> None:
    repo = lifecycle_repo
    async with repo.transaction():
        parent_connection = repo._conn()
        await parent_connection.execute("INSERT INTO reg.tokens VALUES ('parent-uncommitted','{}')")

        async def child() -> None:
            assert await repo.get("reg.tokens", "parent-uncommitted") is None
            async with repo.transaction():
                assert repo._conn() is not parent_connection
                await repo._conn().execute("INSERT INTO reg.tokens VALUES ('child-committed','{}')")

        await asyncio.create_task(child())
        try:
            async with repo.transaction():
                await repo._conn().execute("INSERT INTO reg.tokens VALUES ('savepoint-rollback','{}')")
                raise ValueError("nested failure")
        except ValueError:
            pass
        assert await repo.get("reg.tokens", "savepoint-rollback") is None
    assert await repo.get("reg.tokens", "child-committed") == {}


async def test_pg_cancel_during_enter_rolls_back_without_running_body(
    lifecycle_repo, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import psycopg

    entered = asyncio.Event()
    release = asyncio.Event()
    body = []
    connection = []
    original = psycopg.AsyncTransaction.__aenter__

    async def delayed_enter(transaction):
        result = await original(transaction)
        connection.append(transaction._conn)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(psycopg.AsyncTransaction, "__aenter__", delayed_enter)

    async def worker() -> None:
        async with lifecycle_repo.transaction():
            body.append(True)

    task = asyncio.create_task(worker())
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not body and connection[0]._num_transactions == 0
    assert connection[0].info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    assert lifecycle_repo._pool.get_stats().get("returns_bad", 0) == 0
    assert not caplog.records


async def test_pg_cancel_active_query_keeps_connection_usable(
    lifecycle_repo, caplog: pytest.LogCaptureFixture
) -> None:
    import psycopg

    repo = lifecycle_repo
    entered = asyncio.Event()
    connections = []

    async def worker() -> None:
        async with repo.transaction():
            connections.append(repo._conn())
            entered.set()
            await repo._conn().execute("SELECT pg_sleep(10)")

    task = asyncio.create_task(worker())
    await entered.wait()
    await asyncio.sleep(0.02)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert connections[0].info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    assert connections[0]._num_transactions == 0
    assert await repo.get("reg.tokens", "one")
    assert repo._pool.get_stats().get("returns_bad", 0) == 0
    assert not caplog.records


async def test_pg_child_outliving_parent_does_not_reuse_stale_binding(lifecycle_repo) -> None:
    repo = lifecycle_repo
    release = asyncio.Event()

    async def child() -> None:
        await release.wait()
        with pytest.raises(RuntimeError, match="current task"):
            repo._conn()
        assert await repo.get("reg.tokens", "after-parent") == {}

    async with repo.transaction():
        task = asyncio.create_task(child())
        await repo._conn().execute("INSERT INTO reg.tokens VALUES ('after-parent','{}')")
    release.set()
    await task
    assert repo._connection.get() is None


async def test_pg_repeated_cancel_nested_advisory_wait_keeps_parent_lock(
    lifecycle_repo, caplog: pytest.LogCaptureFixture
) -> None:
    import psycopg

    repo = lifecycle_repo
    started = asyncio.Event()
    connections = []
    acquired = []

    async def worker() -> None:
        async with repo.transaction():
            connections.append(repo._conn())
            started.set()
            async with repo.lock("parent-owned-lock"):
                acquired.append(True)

    async with repo.lock("parent-owned-lock"):
        task = asyncio.create_task(worker())
        await started.wait()
        await asyncio.sleep(0.02)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not acquired
        assert connections[0]._num_transactions == 0
        assert connections[0].info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        assert await repo.get("reg.tokens", "one")
    assert repo._pool.get_stats().get("returns_bad", 0) == 0
    assert not caplog.records
