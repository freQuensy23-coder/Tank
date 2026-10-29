"""Full Market Truth computations run in per-lane worker processes, off the service's GIL."""

import asyncio
import os
import time
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
from types import SimpleNamespace

import pytest
from test_truth_cache_context import _spot_packet

import tank.analysis.truth as truth
from tank.analysis import compute_workers

SETTINGS = SimpleNamespace(config_version="cfg", code_version="code")
SPAWN = compute_workers.multiprocessing.get_context("spawn")


class Repo:
    def __init__(self):
        self.rows = {}

    async def get(self, table, key):
        return self.rows.get((table, key))

    async def put(self, table, key, value, **kwargs):
        self.rows[(table, key)] = value
        return value


class Market:
    def __init__(self, packets):
        self.packets = packets

    async def snapshot(self, token_id, cutoff, anchor=None):
        return dict(self.packets[token_id])


def _comparable(result):
    return {key: value for key, value in result.items() if key not in {"mta_id", "event_id"}}


def test_snapshot_parts_restore_every_row_and_value(monkeypatch):
    monkeypatch.setattr(compute_workers, "SLICE_ROWS", 4)
    packet = _spot_packet()
    parts = dict(compute_workers.pack(packet))
    assert len(parts["flow_bars"]) == 8
    assert isinstance(parts["anchor"], bytes)
    assert compute_workers.unpack(list(parts.items())) == packet


@pytest.mark.asyncio
async def test_assessment_keeps_each_tokens_baseline_cache_in_its_worker(monkeypatch):
    seen = []

    def compute(snapshot, *, config):
        cache = truth._BASELINE_CACHE.get()
        seen.append((snapshot["token_id"], sorted(cache)))
        cache[len(cache)] = snapshot["token_id"]
        return {"token_id": snapshot["token_id"], "cutoff": snapshot["cutoff"]}

    monkeypatch.setattr(truth, "_compute_market_truth", compute)
    packets = {token: {"token_id": token, "cutoff": "2026-10-16T12:00:00Z"} for token in ("T", "U")}
    first = truth.MarketTruthService(Repo(), SETTINGS, Market(packets))
    await first.assess(None, "T")
    await first.assess(None, "T")
    await first.assess(None, "U")
    await truth.MarketTruthService(Repo(), SETTINGS, Market(packets)).assess(None, "T")
    assert seen == [("T", []), ("T", [0]), ("U", []), ("T", [0, 1])]


@pytest.mark.asyncio
async def test_lost_worker_fails_its_call_and_the_next_call_starts_a_new_worker(monkeypatch):
    class Lost:
        def __init__(self):
            self.stopped = False

        def submit(self, *args, **kwargs):
            future = Future()
            future.set_exception(BrokenProcessPool("worker died"))
            return future

        def shutdown(self, wait=True, *, cancel_futures=False):
            self.stopped = cancel_futures

    lost = Lost()
    started = [lost]
    original = compute_workers.start_executor
    monkeypatch.setattr(
        compute_workers, "start_executor", lambda lane: started.pop(0) if started else original(lane)
    )
    packet = _spot_packet()
    with pytest.raises(BrokenProcessPool):
        await compute_workers.compute_market_truth(compute_workers.ASSESSMENT, packet, config={})
    assert lost.stopped and compute_workers.ASSESSMENT not in compute_workers._executors
    result = await compute_workers.compute_market_truth(compute_workers.ASSESSMENT, packet, config={})
    assert result == truth.compute_market_truth(_spot_packet(), config={})


@pytest.fixture
def worker_processes(monkeypatch):
    started = []

    def start_executor(lane):
        started.append(compute_workers.ProcessPoolExecutor(max_workers=1, mp_context=SPAWN))
        return started[-1]

    monkeypatch.setattr(compute_workers, "start_executor", start_executor)
    yield
    for pool in started:
        pool.shutdown(wait=True, cancel_futures=True)


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.asyncio
async def test_assessment_computes_in_a_worker_process_while_the_reaction_lane_is_busy(worker_processes):
    reaction = compute_workers.executor(compute_workers.REACTION)
    busy = reaction.submit(time.sleep, 30)
    repo = Repo()
    service = truth.MarketTruthService(repo, SETTINGS, Market({"T": _spot_packet()}))
    gaps, done = [], asyncio.Event()

    async def ticks():
        last = time.monotonic()
        while not done.is_set():
            await asyncio.sleep(0.01)
            gaps.append(time.monotonic() - last)
            last = time.monotonic()

    ticker = asyncio.create_task(ticks())
    started = time.monotonic()
    result = await service.assess(None, "T", "2026-10-16T12:00:00Z")
    elapsed = time.monotonic() - started
    done.set()
    await ticker
    assessment = compute_workers._executors[compute_workers.ASSESSMENT]
    workers = set(assessment._processes)
    assert workers and os.getpid() not in workers
    assert not busy.done() and elapsed < 25
    assert max(gaps) < 1
    assert repo.rows[("ana.market_truth", result["mta_id"])] is result
    assert _comparable(result) == _comparable(
        truth.compute_market_truth(_spot_packet(), config={"config_version": "cfg", "code_version": "code"})
    )
    for pid in set(reaction._processes):
        os.kill(pid, 9)
    with pytest.raises(BrokenProcessPool):
        busy.result(timeout=20)


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.asyncio
async def test_a_dead_worker_process_is_replaced_on_the_next_call(worker_processes):
    lane = compute_workers.ASSESSMENT
    crashed = compute_workers.executor(lane).submit(os._exit, 1)
    with pytest.raises(BrokenProcessPool):
        crashed.result(timeout=30)
    with pytest.raises(BrokenProcessPool):
        await compute_workers.compute_market_truth(lane, _spot_packet(), config={})
    result = await compute_workers.compute_market_truth(lane, _spot_packet(), config={})
    assert result == truth.compute_market_truth(_spot_packet(), config={})


def test_idle_release_never_starts_a_worker_and_spares_baselines_in_use(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(compute_workers.time, "monotonic", lambda: clock[0])
    compute_workers._used.update({(compute_workers.ASSESSMENT, "T"): 0.0})
    compute_workers.release_idle_baselines(compute_workers.ASSESSMENT, 10)
    assert compute_workers._executors == {} and compute_workers._used == {}
    compute_workers._baselines.update(T={"kept": 1}, U={"kept": 2}, V={"kept": 3})
    compute_workers._used.update(
        {
            (compute_workers.ASSESSMENT, "T"): 0.0,
            (compute_workers.ASSESSMENT, "U"): 0.0,
            (compute_workers.ASSESSMENT, "V"): 95.0,
            (compute_workers.REACTION, "T"): 0.0,
        }
    )
    compute_workers._active[(compute_workers.ASSESSMENT, "U")] = 1
    pool = compute_workers.executor(compute_workers.ASSESSMENT)
    compute_workers.release_idle_baselines(compute_workers.ASSESSMENT, 10)
    pool.submit(lambda: None).result(timeout=5)
    assert compute_workers._baselines == {"U": {"kept": 2}, "V": {"kept": 3}}
    assert set(compute_workers._used) == {
        (compute_workers.ASSESSMENT, "U"),
        (compute_workers.ASSESSMENT, "V"),
        (compute_workers.REACTION, "T"),
    }
