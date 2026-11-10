"""The market worker processes of a service generation, over real spawned processes."""

import asyncio
import os
import time
from types import SimpleNamespace

import market_worker_helpers
import pytest

from tank.analysis import market_workers

pytestmark = [pytest.mark.integration, pytest.mark.process]
SETTINGS = SimpleNamespace(config_version="cfg", code_version="code")
LANES = {market_workers.CONDITION: 2, market_workers.ASSESSMENT: 1}


@pytest.fixture
async def workers():
    pool = market_workers.MarketWorkers(SETTINGS, opener=market_worker_helpers.open_served, lanes=LANES)
    yield pool
    pool.close()


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def _gone(pid):
    for _ in range(100):
        if not _alive(pid):
            return True
        await asyncio.sleep(0.05)
    return False


async def test_a_token_keeps_its_worker_and_tokens_spread_over_the_lane(workers):
    first = await workers.worker(market_workers.CONDITION, "T").call("echo", 1)
    again = await workers.worker(market_workers.CONDITION, "T").call("echo", 2)
    other = await workers.worker(market_workers.CONDITION, "U").call("echo", 3)
    third = await workers.worker(market_workers.CONDITION, "V").call("echo", 4)
    assessment = await workers.worker(market_workers.ASSESSMENT, "T").call("echo", 5)
    assert first["pid"] == again["pid"] != os.getpid()
    assert (first["calls"], again["calls"]) == (1, 2)
    assert other["pid"] != first["pid"] and third["pid"] in {first["pid"], other["pid"]}
    assert assessment["pid"] not in {first["pid"], other["pid"]}
    assert first["worker"] == "market-worker-condition-0" and other["worker"] == "market-worker-condition-1"
    assert assessment["worker"] == "market-worker-assessment-0"


async def test_a_worker_serves_one_request_at_a_time_and_workers_serve_together(workers):
    same = workers.worker(market_workers.CONDITION, "T")
    other = workers.worker(market_workers.CONDITION, "U")
    await asyncio.gather(same.call("echo", 0), other.call("echo", 0))
    started = time.monotonic()
    results = await asyncio.gather(
        same.call("slow", 1, 0.4), same.call("slow", 2, 0.4), other.call("slow", 3, 0.4)
    )
    elapsed = time.monotonic() - started
    assert [result["value"] for result in results] == [1, 2, 3]
    assert not any(result["overlapped"] for result in results)
    assert 0.8 <= elapsed < 1.2


async def test_the_service_loop_keeps_running_while_a_worker_computes(workers):
    worker = workers.worker(market_workers.CONDITION, "T")
    await worker.call("echo", 0)
    gaps, last = [], time.monotonic()
    call = asyncio.create_task(worker.call("slow", 1, 0.5))
    while not call.done():
        await asyncio.sleep(0.01)
        gaps.append(time.monotonic() - last)
        last = time.monotonic()
    assert (await call)["value"] == 1
    assert max(gaps) < 0.2


async def test_a_cancelled_request_stops_in_its_worker_before_the_next_one_starts(workers):
    worker = workers.worker(market_workers.CONDITION, "T")
    before = await worker.call("echo", 0)
    call = asyncio.create_task(worker.call("stubborn", "first", 0.5))
    await asyncio.sleep(0.2)
    started = time.monotonic()
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert time.monotonic() - started < 0.2
    after = await worker.call("echo", 1)
    assert time.monotonic() - started >= 0.45
    assert after["pid"] == before["pid"] and after["calls"] == 2
    assert (await worker.call("seen"))["cancelled"] == ["first"]


async def test_a_request_cancelled_while_it_waits_for_its_turn_never_reaches_the_worker(workers):
    worker = workers.worker(market_workers.CONDITION, "T")
    running = asyncio.create_task(worker.call("slow", 1, 0.4))
    await asyncio.sleep(0.1)
    waiting = asyncio.create_task(worker.call("echo", "never"))
    await asyncio.sleep(0.1)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert (await running)["value"] == 1
    assert (await worker.call("seen"))["calls"] == 0


async def test_a_worker_failure_keeps_its_class_and_retry_time(workers):
    worker = workers.worker(market_workers.CONDITION, "T")
    with pytest.raises(market_worker_helpers.Retry) as raised:
        await worker.call("retry", 42)
    assert raised.value.retry_after == 42 and str(raised.value) == "try later"
    with pytest.raises(RuntimeError, match="RuntimeError in market-worker-condition-0"):
        await worker.call("unportable")
    with pytest.raises(RuntimeError, match="in market-worker-condition-0"):
        await worker.call("unportable_result")
    assert (await worker.call("echo", 1))["value"] == 1


async def test_a_lost_worker_fails_its_request_and_the_next_request_starts_a_new_one(workers):
    worker = workers.worker(market_workers.CONDITION, "T")
    before = await worker.call("echo", 0)
    queued = asyncio.create_task(worker.call("echo", 1))
    with pytest.raises(market_workers.MarketWorkerLost):
        await worker.call("die")
    after = await queued
    assert after["pid"] != before["pid"] and after["calls"] == 1
    assert await _gone(before["pid"])


async def test_closing_fails_waiting_requests_and_ends_every_worker_process():
    pool = market_workers.MarketWorkers(SETTINGS, opener=market_worker_helpers.open_served, lanes=LANES)
    worker = pool.worker(market_workers.CONDITION, "T")
    pid = (await worker.call("echo", 0))["pid"]
    other = (await pool.worker(market_workers.CONDITION, "U").call("echo", 0))["pid"]
    running = asyncio.create_task(worker.call("slow", 1, 30))
    queued = asyncio.create_task(worker.call("echo", 2))
    await asyncio.sleep(0.1)
    pool.close()
    for call in (running, queued):
        with pytest.raises(market_workers.MarketWorkerLost):
            await call
    with pytest.raises(market_workers.MarketWorkerLost):
        await worker.call("echo", 3)
    assert await _gone(pid) and await _gone(other)


async def test_workers_receive_the_generations_captured_configuration(tmp_path):
    from tank.settings import Settings, resolve_versions

    config = tmp_path / "config"
    config.mkdir()
    (config / "thresholds.yaml").write_text("version: one\n")
    settings = resolve_versions(Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None))
    (config / "thresholds.yaml").write_text("version: two\n")
    pool = market_workers.MarketWorkers(settings, opener=market_worker_helpers.open_served, lanes=LANES)
    try:
        served = await pool.worker(market_workers.CONDITION, "T").call("setting", "thresholds")
    finally:
        pool.close()
    assert served == {"version": "one"} == settings.load_config("thresholds")
