import asyncio
import os
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from psycopg import AsyncTransaction

from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.util import parse_time


@pytest.fixture(
    params=["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def repo(request, postgres_template):
    if request.param == "memory":
        yield MemoryRepository()
        return
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repository:
        yield repository


async def test_minute_jobs_continue_during_blocked_maintenance(repo, monkeypatch):
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    stop = asyncio.Event()
    maintenance_started = asyncio.Event()
    now = parse_time("2026-11-22T00:00:50Z")
    waits = []
    wakeups = iter(["2026-11-22T00:01:02Z", "2026-11-22T00:02:00Z", "2026-11-22T00:05:09Z"])

    async def slow_expiry(tick_time):
        maintenance_started.set()
        await stop.wait()

    async def advance_clock(flag, seconds):
        nonlocal now
        if flag.is_set():
            return
        await maintenance_started.wait()
        waits.append(seconds)
        wake = next(wakeups, None)
        if wake is None:
            flag.set()
        else:
            now = parse_time(wake)

    scheduler.expire_calls = AsyncMock(side_effect=slow_expiry)
    monkeypatch.setattr(scheduler, "launch", lambda name, operation: None)
    monkeypatch.setattr("tank.scheduler.utcnow", lambda: now)
    monkeypatch.setattr(scheduler, "wait_for_tick", advance_clock, raising=False)
    async with asyncio.timeout(3):
        await scheduler.run(stop)

    scheduler.expire_calls.assert_awaited_once()
    assert waits == [10, 58, 60, 51]
    jobs = await repo.list("ops.jobs", filters={"type": "EVAL_CONDITIONS"})
    assert sorted(job["idem_key"] for job in jobs) == [
        "2026-11-22T00:00:00Z",
        "2026-11-22T00:01:00Z",
        "2026-11-22T00:02:00Z",
        "2026-11-22T00:05:00Z",
    ]
    for job in jobs:
        assert job["payload"]["cutoff"] == job["idem_key"]
        assert job["payload"]["cutoff_prec"] == "BUCKET(1m)"
        assert job["expires_at_prec"] == "COLLECTION_ONLY"
        assert job["priority"] == 2
        assert job["max_attempts"] == 2


async def test_minute_enqueue_retries_and_restart_do_not_replace_committed_job(repo, monkeypatch):
    now = parse_time("2026-11-22T00:00:10Z")
    monkeypatch.setattr("tank.scheduler.utcnow", lambda: now)
    enqueue = repo.enqueue
    attempts = 0

    async def flaky_enqueue(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("Failed before enqueue")
        job = await enqueue(*args, **kwargs)
        if attempts == 2:
            raise OSError("Enqueue committed but acknowledgement was lost")
        return job

    monkeypatch.setattr(repo, "enqueue", flaky_enqueue)
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    stop = asyncio.Event()
    waits = []

    async def retry_clock(flag, seconds):
        nonlocal now
        waits.append(seconds)
        if len(waits) < 3:
            now += timedelta(seconds=seconds)
        else:
            flag.set()

    monkeypatch.setattr(scheduler, "wait_for_tick", retry_clock)
    async with asyncio.timeout(3):
        await scheduler.run_minute_checks(stop)
    assert waits == [5, 5, 40]
    jobs = await repo.list("ops.jobs", filters={"type": "EVAL_CONDITIONS"})
    assert len(jobs) == 1
    assert jobs[0]["expires_at"] == "2026-11-22T00:05:15Z"
    original_job = jobs[0]

    restarted = Scheduler(repo, SimpleNamespace(), None, None)
    stop = asyncio.Event()

    async def stop_after_enqueue(flag, seconds):
        flag.set()

    monkeypatch.setattr(restarted, "wait_for_tick", stop_after_enqueue)
    async with asyncio.timeout(3):
        await restarted.run_minute_checks(stop)
    assert await repo.list("ops.jobs", filters={"type": "EVAL_CONDITIONS"}) == [original_job]


async def test_enqueue_still_waiting_at_boundary_is_cancelled_without_replay(monkeypatch):
    repo = MemoryRepository()
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    stop = asyncio.Event()
    now = parse_time("2026-11-22T00:00:59.999999Z")
    blocked_cutoff = "2026-11-22T00:00:00Z"
    enqueue = repo.enqueue
    old_enqueue_cancelled = asyncio.Event()

    async def blocked_enqueue(kind, payload, idem_key, **kwargs):
        nonlocal now
        if idem_key == blocked_cutoff:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                now = parse_time("2026-11-22T00:01:00Z")
                old_enqueue_cancelled.set()
                raise
        result = await enqueue(kind, payload, idem_key, **kwargs)
        stop.set()
        return result

    monkeypatch.setattr("tank.scheduler.utcnow", lambda: now)
    monkeypatch.setattr(repo, "enqueue", blocked_enqueue)
    async with asyncio.timeout(3):
        await scheduler.run_minute_checks(stop)
    assert old_enqueue_cancelled.is_set()
    jobs = await repo.list("ops.jobs", filters={"type": "EVAL_CONDITIONS"})
    assert [job["idem_key"] for job in jobs] == ["2026-11-22T00:01:00Z"]


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_postgres_commit_crossing_boundary_keeps_expiry_without_backfill(repo, monkeypatch, caplog):
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    stop = asyncio.Event()
    commit_started, release_commit = asyncio.Event(), asyncio.Event()
    deadlines = asyncio.Queue()
    now = parse_time("2026-11-22T00:00:45Z")
    first = "2026-11-22T00:00:00Z"
    lock, enqueue = repo.lock, repo.enqueue
    exit_transaction = AsyncTransaction.__aexit__
    delay_commit = False

    @asynccontextmanager
    async def mark_first_commit(key):
        nonlocal delay_commit
        async with lock(key):
            yield
            if key == f"queue:EVAL_CONDITIONS:{first}":
                delay_commit = True

    async def hold_commit(transaction, *exc):
        nonlocal delay_commit
        if delay_commit:
            delay_commit = False
            commit_started.set()
            await release_commit.wait()
        return await exit_transaction(transaction, *exc)

    def observe_deadline(seconds):
        deadline = asyncio.timeout(seconds)
        deadlines.put_nowait(deadline)
        return deadline

    async def stop_after_current(kind, payload, idem_key, **kwargs):
        job = await enqueue(kind, payload, idem_key, **kwargs)
        if idem_key != first:
            stop.set()
        return job

    monkeypatch.setattr(repo, "lock", mark_first_commit)
    monkeypatch.setattr(repo, "enqueue", stop_after_current)
    monkeypatch.setattr(AsyncTransaction, "__aexit__", hold_commit)
    monkeypatch.setattr("tank.scheduler.utcnow", lambda: now)
    monkeypatch.setattr(
        "tank.scheduler.asyncio", SimpleNamespace(timeout=observe_deadline, wait_for=asyncio.wait_for)
    )
    async with asyncio.timeout(3), asyncio.TaskGroup() as tasks:
        tasks.create_task(scheduler.run_minute_checks(stop))
        try:
            deadline = await deadlines.get()
            await commit_started.wait()
            now = parse_time("2026-11-22T00:03:00Z")
            deadline.reschedule(asyncio.get_running_loop().time())
        finally:
            release_commit.set()

    jobs = await repo.list("ops.jobs", filters={"type": "EVAL_CONDITIONS"})
    assert sorted((job["idem_key"], job["expires_at"]) for job in jobs) == [
        (first, "2026-11-22T00:05:45Z"),
        ("2026-11-22T00:03:00Z", "2026-11-22T00:08:00Z"),
    ]
    assert any(record.exc_info and record.exc_info[0] is TimeoutError for record in caplog.records)


async def test_minute_wait_exits_when_stop_is_set(monkeypatch):
    scheduler = Scheduler(MemoryRepository(), SimpleNamespace(), None, None)
    stop = asyncio.Event()
    waiting = asyncio.Event()
    original_wait = scheduler.wait_for_tick

    async def observe_wait(flag, seconds):
        waiting.set()
        await original_wait(flag, seconds)

    monkeypatch.setattr("tank.scheduler.utcnow", lambda: parse_time("2026-11-22T00:00:00Z"))
    monkeypatch.setattr(scheduler, "wait_for_tick", observe_wait)
    async with asyncio.timeout(3), asyncio.TaskGroup() as tasks:
        tasks.create_task(scheduler.run_minute_checks(stop))
        await waiting.wait()
        stop.set()


async def test_scheduler_cancel_joins_minute_enqueue_and_inline_maintenance(monkeypatch):
    scheduler = Scheduler(MemoryRepository(), SimpleNamespace(), None, None)
    enqueue_started, maintenance_started = asyncio.Event(), asyncio.Event()
    enqueue_exited, maintenance_exited = asyncio.Event(), asyncio.Event()

    async def block(started, exited):
        started.set()
        try:
            await asyncio.Future()
        finally:
            exited.set()

    async def blocked_enqueue(minute, now):
        await block(enqueue_started, enqueue_exited)

    async def blocked_expiry(now):
        await block(maintenance_started, maintenance_exited)

    monkeypatch.setattr("tank.scheduler.utcnow", lambda: parse_time("2026-11-22T00:00:00Z"))
    monkeypatch.setattr(scheduler, "enqueue_minute_check", blocked_enqueue)
    monkeypatch.setattr(scheduler, "expire_calls", blocked_expiry)
    async with asyncio.timeout(3), asyncio.TaskGroup() as tasks:
        task = tasks.create_task(scheduler.run(asyncio.Event()))
        await enqueue_started.wait()
        await maintenance_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert enqueue_exited.is_set() and maintenance_exited.is_set()
    assert scheduler.closing
