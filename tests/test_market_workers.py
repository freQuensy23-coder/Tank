"""Market reads and their Market Truth computations asked of the generation's worker processes."""

import asyncio
import pickle
import threading
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_truth_cache_context import _spot_packet

import tank.analysis.truth as truth
from tank.analysis import market_workers
from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.util import iso, utcnow

SETTINGS = SimpleNamespace(config_version="cfg", code_version="code")


class Market:
    def __init__(self, packets):
        self.packets, self.waits, self.scopes = packets, {}, []

    async def snapshot(self, token_id, cutoff, anchor=None, *, scope=None):
        self.scopes.append(scope)
        return dict(self.packets[token_id], capture_waits=dict(self.waits))

    async def condition_snapshot(self, token_id, cutoff, *, anchor=None, requirements):
        return dict(self.packets[token_id])

    async def recorded_capture_waits(self, token_id):
        return {"BF1": {"status": "INCOMPLETE"}}

    def record_capture_waits(self, waits):
        self.waits.update(waits)


class Loopback:
    """The worker processes' reader, served in this process."""

    def __init__(self, repo, market):
        self.reader = market_workers.MarketReader(truth.MarketTruthService(repo, SETTINGS, market), "reader")
        self.asked = []

    async def condition_values(self, *arguments):
        self.asked.append(("condition_values", *arguments))
        return await self.reader.condition_values(*arguments)

    async def assessment(self, *arguments):
        self.asked.append(("assessment", *arguments))
        return pickle.loads(pickle.dumps(await self.reader.assessment(*arguments)))

    async def reaction(self, *arguments):
        self.asked.append(("reaction", *arguments))
        return pickle.loads(pickle.dumps(await self.reader.reaction(*arguments)))


def _stored(result):
    return {key: value for key, value in result.items() if key != "mta_id"}


def test_a_token_keeps_the_least_loaded_worker_of_each_lane_it_first_met():
    pool = market_workers.MarketWorkers(SETTINGS, lanes={"A": 2, "B": 1})
    names = [pool.worker("A", token).name for token in ("T", "U", "V", "T", "W", "U")]
    assert names == [f"market-worker-a-{n}" for n in (0, 1, 0, 0, 1, 1)]
    assert pool.worker("B", "T") is pool.worker("B", "U")
    assert all(worker.process is None for workers in pool._workers.values() for worker in workers)
    pool.close()


def test_the_live_lanes_leave_postgres_connections_for_the_service_process():
    workers = sum(market_workers.LANE_WORKERS.values())
    assert market_workers.LANE_WORKERS == {"CONDITION": 4, "ASSESSMENT": 2, "REACTION": 1}
    assert workers * market_workers.WORKER_CONNECTIONS + 16 < 100


async def test_an_assessment_read_and_computed_by_a_worker_is_stored_by_the_service():
    packets = {"T": _spot_packet()}
    local_repo, repo = MemoryRepository(), MemoryRepository()
    local = await truth.MarketTruthService(local_repo, SETTINGS, Market(packets)).assess(
        None, "T", "2026-10-16T12:00:00Z"
    )
    workers = Loopback(MemoryRepository(), Market(packets))
    service = truth.MarketTruthService(repo, SETTINGS, Market({}), workers)
    result = await service.assess(None, "T", "2026-10-16T12:00:00Z")
    assert workers.asked == [
        ("assessment", None, "T", "2026-10-16T12:00:00Z", {"BF1": {"status": "INCOMPLETE"}})
    ]
    assert workers.reader.market.waits == {"BF1": {"status": "INCOMPLETE"}}
    assert _stored(result) == _stored(local)
    assert result["mta_id"].startswith("MTA") and result["mta_id"] != local["mta_id"]
    assert await repo.get("ana.market_truth", result["mta_id"]) == result
    assert await workers.reader.truth.repo.list("ana.market_truth") == []
    with pytest.raises(ValueError):
        await repo.put("ana.market_truth", result["mta_id"], {"mta_id": result["mta_id"]}, immutable=True)


async def test_a_worker_keeps_each_tokens_assessment_baselines_until_the_token_goes_idle(monkeypatch):
    seen = []

    def compute(snapshot, *, config):
        cache = truth._BASELINE_CACHE.get()
        seen.append((snapshot["token_id"], sorted(cache)))
        cache[len(cache)] = snapshot["token_id"]
        return {"token_id": snapshot["token_id"]}

    monkeypatch.setattr(truth, "_compute_market_truth", compute)
    monkeypatch.setattr(market_workers, "UPKEEP_S", 0.01)
    packets = {token: {"token_id": token, "cutoff": "2026-10-16T12:00:00Z"} for token in ("T", "U")}
    reader = Loopback(MemoryRepository(), Market(packets)).reader
    for token in ("T", "T", "U", "T"):
        await reader.assessment(None, token, "2026-10-16T12:00:00Z", {})
    assert seen == [("T", []), ("T", [0]), ("U", []), ("T", [0, 1])]
    reader.baselines["U"]["used"] -= truth.ASSESSMENT_BASELINE_IDLE_S + 1
    release = asyncio.create_task(reader.release_idle_baselines())
    await asyncio.sleep(0.05)
    release.cancel()
    assert sorted(reader.baselines) == ["T"]


async def test_condition_values_are_read_and_computed_by_a_worker():
    requirements = [{"metric": "spot.cohort.direction", "window": "W1H"}]
    packets = {"T": _spot_packet()}
    local = await truth.MarketTruthService(MemoryRepository(), SETTINGS, Market(packets)).condition_values(
        None, "T", "2026-10-16T12:00:00Z", requirements
    )
    workers = Loopback(MemoryRepository(), Market(packets))
    service = truth.MarketTruthService(MemoryRepository(), SETTINGS, Market({}), workers)
    assert await service.condition_values(None, "T", "2026-10-16T12:00:00Z", requirements) == local
    assert workers.asked == [("condition_values", None, "T", "2026-10-16T12:00:00Z", requirements)]


async def test_a_reaction_checkpoint_is_read_and_computed_by_a_worker_and_stored_by_the_service(monkeypatch):
    now = utcnow().replace(microsecond=0)
    monkeypatch.setattr(
        truth,
        "_compute_market_truth",
        lambda snapshot, config: {
            "relative": {"windows": {"EVENT": {"r_raw": 0.01}}},
            "seen": [snapshot["event_id"], snapshot["token_id"], snapshot["cutoff"], config["code_version"]],
        },
    )
    payload = {
        "event_id": "e",
        "token_id": "T",
        "checkpoint": "PRE_24H",
        "anchor": {"type": "T1", "ts": iso(now)},
        "retro": True,
    }
    stored = []
    for through_workers in (False, True):
        repo, market = MemoryRepository(), Market({"T": {"cutoff": iso(now)}})
        scheduler = Scheduler(repo, SETTINGS, market, None)
        if through_workers:
            scheduler.market = SimpleNamespace(snapshot=AsyncMock(side_effect=AssertionError))
            scheduler.market_workers = workers = Loopback(MemoryRepository(), market)
        await scheduler.reaction_capture(dict(payload))
        assert market.scopes == ["REACTION"]
        rows = await repo.list("out.reaction_checkpoints")
        stored.append(rows)
    assert len(stored[0]) == len(stored[1]) == 1
    differing = {key for key in stored[0][0] if stored[0][0][key] != stored[1][0][key]}
    assert differing == {"rwp_id", "captured_at"}
    assert stored[1][0]["market_truth"]["seen"] == ["e", "T", iso(now), "code"]
    name, event_id, token_id, end_stamp, anchor = workers.asked[0]
    assert (name, event_id, token_id) == ("reaction", "e", "T")
    assert end_stamp.value == iso(now) and anchor["ts"] == iso(now - timedelta(days=1))
    assert anchor["type"] == "T1"


def test_settings_reach_a_worker_process_with_the_configuration_they_captured(tmp_path):
    from tank.settings import Settings, resolve_versions

    config = tmp_path / "config"
    config.mkdir()
    (config / "thresholds.yaml").write_text("version: one\n")
    settings = resolve_versions(Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None))
    (config / "thresholds.yaml").write_text("version: two\n")
    received = pickle.loads(pickle.dumps(settings))
    assert received == settings and received.config_version == settings.config_version
    assert received.load_config("thresholds") == {"version": "one"}
    assert received.read_config_bytes("thresholds.yaml") == b"version: one\n"
    with pytest.raises(TypeError):
        received._config_snapshot["thresholds.yaml"] = b""
    plain = pickle.loads(
        pickle.dumps(Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None))
    )
    assert plain.load_config("thresholds") == {"version": "two"}


async def test_a_generation_gives_its_worker_processes_to_market_truth_and_the_scheduler_and_closes_them():
    from tank.runtime import Runtime, service_generation

    closed = []
    workers = SimpleNamespace(close=lambda: closed.append(True))
    service = SimpleNamespace()
    runtime = Runtime(
        MemoryRepository(),
        SimpleNamespace(configured_secrets=lambda: (), **vars(SETTINGS)),
        gateway=service,
        sources=service,
        market=service,
        pipeline=SimpleNamespace(filter_item=None, route_item=None),
        event_tools=SimpleNamespace(dispatch=None),
        sessions=SimpleNamespace(send_message=None),
        delivery=SimpleNamespace(deliver=None),
        inbox=SimpleNamespace(answer=None),
        market_workers=workers,
    )
    assert runtime.truth.workers is workers and runtime.scheduler.market_workers is workers
    await runtime.close()
    assert closed == [True]
    assert service_generation.__module__ == "tank.runtime"


async def test_tank_serve_runs_generations_with_market_worker_processes(monkeypatch):
    import tank.cli as cli
    import tank.runtime as runtime_module

    seen = {}

    class Supervisor:
        def __init__(self, repo, settings, *, factory):
            seen["factory"] = factory

        async def run(self, stop, reload_requested):
            seen["ran"] = True

    class Generation:
        def __init__(self, repo, settings, *, market_workers):
            seen["workers"] = market_workers

    async def launch_with(fn):
        repo = SimpleNamespace(migrate=AsyncMock(), clickhouse=None)
        return await fn(repo, SETTINGS)

    monkeypatch.setattr(runtime_module, "RuntimeSupervisor", Supervisor)
    monkeypatch.setattr(runtime_module, "Runtime", Generation)
    monkeypatch.setattr("tank.openma_sync.keep_in_sync", AsyncMock())
    launched = []
    monkeypatch.setattr(cli, "run_db", lambda fn, **kwargs: launched.append(fn))
    cli.serve.callback()
    assert await launch_with(launched[0]) == {"status": "stopped"}
    assert seen["ran"] and seen["factory"] is runtime_module.service_generation
    seen["factory"](None, SETTINGS)
    assert isinstance(seen["workers"], market_workers.MarketWorkers)
    seen["workers"].close()


async def test_a_stopped_loop_watchdog_leaves_no_thread_behind(tmp_path):
    from test_market_service import setup as market_setup

    from tank.market.service import MarketService

    repo, settings, _ = market_setup.__wrapped__(tmp_path)
    service = MarketService(repo, settings)
    service._stop = asyncio.Event()
    before = {thread for thread in threading.enumerate() if thread.name == "loop-watchdog"}
    watchdog = asyncio.create_task(service._loop_watchdog())
    await asyncio.sleep(0.05)
    started = {thread for thread in threading.enumerate() if thread.name == "loop-watchdog"} - before
    assert len(started) == 1
    watchdog.cancel()
    await asyncio.gather(watchdog, return_exceptions=True)
    await asyncio.to_thread(next(iter(started)).join, 3)
    assert not next(iter(started)).is_alive()
    await service.close()


async def test_a_service_process_hands_a_tokens_recorded_capture_waits_to_its_worker(tmp_path):
    from test_market_service import setup as market_setup

    from tank.market.service import MarketService

    repo, settings, inst = market_setup.__wrapped__(tmp_path)
    token = inst["token_id"]
    for backfill_id, owner in (("BF1", token), ("BF2", token), ("BF3", "bitcoin:BTC")):
        await repo.put(
            "mkt.backfill_runs",
            backfill_id,
            {
                "backfill_id": backfill_id,
                "event_id": "e",
                "token_id": owner,
                "started_at": iso(),
                "status": "RUNNING",
            },
        )
    service, worker = MarketService(repo, settings), MarketService(repo, settings)
    waited = await service.wait_capture("e", token, timeout_s=0)
    assert waited["status"] == "INCOMPLETE"
    waits = await service.recorded_capture_waits(token)
    assert list(waits) == [waited["backfill_id"]] and waits[waited["backfill_id"]] == waited
    assert await service.recorded_capture_waits("bitcoin:BTC") == {}
    worker.record_capture_waits(waits)
    assert worker._capture_waits == service._capture_waits
    await service.close()
    await worker.close()
