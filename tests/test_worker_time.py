import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

import tank.worker_time as timing
from tank.delivery import DeliveryDeferred
from tank.runtime import Runtime
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import utcnow
from tank.worker_time_report import events, report


@pytest.fixture
def clock(monkeypatch, caplog):
    epoch = datetime(2026, 1, 1, tzinfo=UTC)
    seconds = [0]
    monkeypatch.setattr(timing, "utcnow", lambda: epoch + timedelta(seconds=seconds[0]))
    monkeypatch.setattr(timing, "monotonic_ns", lambda: seconds[0] * 10**9)
    caplog.set_level(logging.INFO, logger="tank.worker_time")
    return epoch, seconds


def captured(caplog):
    return list(events(f" INFO {r.name} {r.getMessage()}" for r in caplog.records))


async def test_five_minutes_across_claims_workers_and_unclaimed_expiry(clock, caplog, monkeypatch):
    epoch, seconds = clock
    repo = MemoryRepository()
    expired = await repo.enqueue("DELIVER", {}, "unused", expires_at=utcnow() - timedelta(seconds=1))
    job = await repo.enqueue("DELIVER", {"secret": "private-payload"}, "private-key", run_after=epoch)
    actual_claim = repo.claim_job
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    stop = asyncio.Event()

    async def claim():
        seconds[0] += 5
        return await actual_claim()

    async def defer(payload):
        seconds[0] = 65
        stop.set()
        raise DeliveryDeferred(collection_clock(epoch + timedelta(seconds=70)))

    async def finish(payload):
        seconds[0] = 410
        stop.set()
        return {"result": "private-result"}

    monkeypatch.setattr(repo, "claim_job", claim)
    runtime.handlers = {"DELIVER": defer}
    await runtime.worker(stop, 2)
    seconds[0] = 95
    stop.clear()
    runtime.handlers = {"DELIVER": finish}
    await runtime.worker(stop, 5)
    assert (await repo.get("ops.jobs", expired["job_id"]))["status"] == "EXPIRED"
    assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "DONE"
    assert all(value not in caplog.text for value in ("private-payload", "private-key", "private-result"))

    start = epoch + timedelta(seconds=30)
    window = report(captured(caplog), start, start + timedelta(minutes=5))
    first, second = window["attempts"]
    assert [(a["worker"], a["attempt"], a["job_id"]) for a in (first, second)] == [
        (2, 1, job["job_id"]),
        (5, 1, job["job_id"]),
    ]
    assert first["run"] != second["run"]
    assert (first["eligible_wait_us"], second["eligible_wait_us"]) == (5000000, 30000000)
    assert (first["outcome"], first["elapsed_ns"]) == ("delivery_deferred", 60 * 10**9)
    assert (second["outcome"], second["elapsed_ns"]) == ("completed", 310 * 10**9)
    assert first["start"] == "2026-01-01T00:00:05Z" and first["end"] == "2026-01-01T00:01:05Z"
    assert second["start"] == "2026-01-01T00:01:40Z" and second["end"] == "2026-01-01T00:06:50Z"
    assert window["workers"][2]["total_us"] == dict(
        claimed=35000000, unknown=265000000, unclaimed=0, unfinished=0
    )
    assert window["workers"][5]["total_us"] == dict(
        claimed=230000000, unknown=65000000, unclaimed=5000000, unfinished=0
    )
    assert len(window["workers"]) == 8
    assert all(sum(worker["total_us"].values()) == 300000000 for worker in window["workers"].values())


async def test_open_claim_only_counts_until_observed_and_cancellation_closes_it(clock, caplog, monkeypatch):
    epoch, seconds = clock
    repo = MemoryRepository()
    await repo.enqueue("FILTER_ITEM", {}, "cancel")
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    running = asyncio.Event()
    callbacks = []
    call_later = asyncio.get_running_loop().call_later

    def schedule(delay, callback, *args, **kwargs):
        if isinstance(getattr(callback, "__self__", None), timing.WorkerTime):
            callbacks.append(callback)
            return Mock()
        return call_later(delay, callback, *args, **kwargs)

    async def handle(payload):
        running.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncio.get_running_loop(), "call_later", schedule)
    runtime.handlers = {"FILTER_ITEM": handle}
    task = asyncio.create_task(runtime.worker(asyncio.Event(), 0))
    try:
        await running.wait()
        seconds[0] = 30
        callbacks.pop()()
        before = report(captured(caplog), epoch, epoch + timedelta(minutes=5))
        assert before["workers"][0]["total_us"] == dict(
            claimed=0, unknown=270000000, unclaimed=0, unfinished=30000000
        )
        assert before["attempts"][0]["end"] is None
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    after = report(captured(caplog), epoch, epoch + timedelta(minutes=5))
    assert after["attempts"][0]["outcome"] == "cancelled"
    assert after["workers"][0]["total_us"]["claimed"] == 30000000
    rotated = report([r for r in captured(caplog) if r["event"] != "sample"], epoch, epoch + timedelta(minutes=5))
    assert rotated["workers"][0]["total_us"]["unknown"] == 300000000
    assert not runtime.active_jobs


async def test_empty_polls_do_not_emit_individual_messages(clock, caplog, monkeypatch):
    epoch, seconds = clock
    runtime = object.__new__(Runtime)
    runtime.repo = MemoryRepository()
    stop = asyncio.Event()

    async def pause(flag, delay):
        seconds[0] += 1
        if seconds[0] == 16:
            flag.set()

    monkeypatch.setattr(runtime, "pause", pause)
    await runtime.worker(stop, 7)
    records = captured(caplog)
    assert [r["event"] for r in records] == ["worker_start", "worker_stop"]
    window = report(records, epoch, epoch + timedelta(minutes=5))
    assert window["workers"][7]["total_us"]["unclaimed"] == 16000000
    assert not window["attempts"]


async def test_five_minute_burst_log_volume(caplog):
    caplog.set_level(logging.INFO, logger="tank.worker_time")
    workers = [timing.WorkerTime(i) for i in range(8)]
    job = {"job_id": "JOB-01M5ESBE4B2R5Z61GPT3634CK2", "attempts": 1, "run_after": utcnow()}
    for n in range(2796):
        worker = workers[n % 8]
        worker.start_claim(job, "REACTION_CAPTURE")
        worker.end_claim("dependency_deferred")
    for worker in workers:
        for _ in range(10):
            worker.timer.cancel()
            worker.sample()
        worker.stop()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    records = [r for r in caplog.records if r.name == "tank.worker_time"]
    size = 0
    for record in records:
        envelope = {"log": formatter.format(record) + "\n", "time": "2026-01-01T00:00:00.123456789Z", "stream": "stderr"}
        size += len(json.dumps(envelope, separators=(",", ":")).encode()) + 1
    assert len(records) == 2 * 2796 + 8 * 12
    assert size < 4 * 1024**2, size
