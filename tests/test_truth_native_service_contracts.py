import asyncio
import os
import time
from concurrent.futures.process import BrokenProcessPool
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from test_compute_workers import worker_processes as worker_processes
from test_truth_cache_context import _spot_packet
from test_truth_condition_values import REQUIREMENTS
from test_truth_sql_conditions import original_truth as original_truth
from test_truth_sql_engine import CountingNative, assert_complete, forbid_python_business
from test_truth_start_fixture import compact_start_snapshot
from test_truth_time_precision import packet as precision_packet
from truth_native_test_safety import owned_native_repository, require_loopback_native_url

from tank.analysis import compute_workers, truth
from tank.analysis import truth_sql_engine as native
from tank.market.service import MarketService
from tank.storage import ImmutableRecordError, MemoryRepository
from tank.time_precision import chosen_boundary
from tank.util import iso, parse_time

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

END = "2026-10-16T12:00:00Z"
VERSIONS = {"config_version": "test", "code_version": "test"}


@pytest.fixture
def postgres_template(request):
    require_loopback_native_url()
    return request.getfixturevalue("postgres_template")


def anchor(kind, stamp, precision=None):
    return {
        "type": kind,
        "ts": stamp,
        "ts_prec": precision,
        "ts_prec_reason": None if precision is not None else "INCOMPLETE",
    }


def public_context(stamp, precision=None):
    return {
        "event_public_at": stamp,
        "event_public_at_prec": precision,
        "event_public_at_prec_reason": None if precision is not None else "INCOMPLETE",
    }


async def assess_native(
    original,
    postgres_template,
    monkeypatch,
    *,
    event,
    token,
    cutoff,
    packet,
    expected_packet,
    edges=(),
    default=False,
    progress=False,
    config=None,
):
    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn, "owned disposable PostgreSQL is required"
    config = dict(VERSIONS if config is None else config)
    expected = original.compute_market_truth(deepcopy(expected_packet), config=deepcopy(config))
    expected["measurement_context"]["numeric_policy"] = native.NUMERIC_POLICY
    captured = []

    class Market:
        async def snapshot(self, token_id, supplied_cutoff, anchor=None):
            assert token_id == token
            assert supplied_cutoff == cutoff
            captured.append(deepcopy(anchor))
            return deepcopy(packet)

    async with owned_native_repository(postgres_template, dsn) as repo:
        assert getattr(repo, "clickhouse", None) is not None
        event_id = expected_packet["event_id"]
        if event_id:
            await repo.put("evt.events", event_id, dict(event, event_id=event_id))
        for version, role in edges:
            key = f"BFE-SERVICE-{version}"
            await repo.put(
                "evt.beneficiary_edges",
                key,
                {
                    "edge_id": key,
                    "event_id": event_id,
                    "token_id": token,
                    "map_version": version,
                    "role": role,
                },
            )
        counted = CountingNative(repo.clickhouse)
        repo.clickhouse = counted
        service = truth.MarketTruthService(repo, SimpleNamespace(**config), Market())
        forbid_python_business(monkeypatch)
        if progress:
            entered, release, heartbeat = asyncio.Event(), asyncio.Event(), asyncio.Event()
            real_rows = counted.rows

            async def awaited_rows(statement, params=None):
                pending = asyncio.create_task(real_rows(statement, params))
                entered.set()
                await release.wait()
                return await pending

            monkeypatch.setattr(counted, "rows", awaited_rows)
            pending = asyncio.create_task(service.assess(event_id, token, None if default else cutoff))
            await asyncio.wait_for(entered.wait(), 1)
            asyncio.get_running_loop().call_soon(heartbeat.set)
            await asyncio.wait_for(heartbeat.wait(), 1)
            assert not pending.done()
            release.set()
            result = await pending
        else:
            result = await service.assess(event_id, token, None if default else cutoff)
        assert len(counted.requests) == 1
        assert_complete({key: value for key, value in result.items() if key != "mta_id"}, expected)
        assert result["mta_id"].startswith("MTA")
        assert await repo.get("ana.market_truth", result["mta_id"]) == result
        assert await repo.put("ana.market_truth", result["mta_id"], result, immutable=True) == result
        with pytest.raises(ImmutableRecordError):
            await repo.put("ana.market_truth", result["mta_id"], dict(result, move_class="tampered"))
        assert await repo.get("ana.market_truth", result["mta_id"]) == result
        assert compute_workers._executors == {} and compute_workers._baselines == {}
        return result, captured


async def test_native_service_event_anchor_persistence(original_truth, postgres_template, monkeypatch):
    selected = anchor("T0", "2026-10-16T08:00:00Z", "EXACT")
    event = {
        "event_time": {"value": "2026-10-16T08:00:00Z", "precision": "EXACT", "verified": True},
        "state_verified": True,
        "known_to_us_at": "2026-10-16T08:10:00Z",
    }
    packet = {"token_id": "WLD", "cutoff": END, "anchor": selected}
    result, captured = await assess_native(
        original_truth,
        postgres_template,
        monkeypatch,
        event=event,
        token="WLD",
        cutoff=END,
        packet=packet,
        expected_packet=dict(deepcopy(packet), event_id="EVT-1"),
    )
    assert captured == [selected]
    assert result["relative"]["anchor"]["type"] == "T0"
    assert result["config_version"] == "test"


async def test_native_service_scheduled_t0_switch(original_truth, postgres_template, monkeypatch):
    event = {
        "event_time": "2026-10-30T06:00:00Z",
        "event_time_prec": "EXACT",
        "state_verified": True,
        "public_at": "2026-10-27T13:00:00Z",
        "public_at_prec": "EXACT",
    }
    before = chosen_boundary("2026-10-27T13:35:00Z", precision="BUCKET(1m)")
    results = []
    for cutoff, kind, stamp in (
        (before, "T1", "2026-10-27T13:00:00Z"),
        ("2026-10-30T07:00:00Z", "T0", "2026-10-30T06:00:00Z"),
    ):
        selected = anchor(kind, stamp, "EXACT")
        packet = {"token_id": "bitcoin:BTC", "cutoff": getattr(cutoff, "value", cutoff), "anchor": selected}
        expected = dict(deepcopy(packet), event_id="EVT-1", **public_context("2026-10-27T13:00:00Z", "EXACT"))
        result, captured = await assess_native(
            original_truth,
            postgres_template,
            monkeypatch,
            event=event,
            token="bitcoin:BTC",
            cutoff=cutoff,
            packet=packet,
            expected_packet=expected,
            config={"config_version": "t", "code_version": "t"},
        )
        assert captured == [selected]
        assert result["relative"]["anchor"]["type"] == kind
        results.append(result)
    assert results[0]["relative"]["anchor"]["ts"] == "2026-10-27T13:00:00Z"
    assert results[0]["mta_id"] != results[1]["mta_id"]


async def test_native_service_cutoff_and_async_wait_progress(original_truth, postgres_template, monkeypatch):
    real_iso = truth.iso
    monkeypatch.setattr(
        truth, "iso", lambda value=None: real_iso(value) if value is not None else "2026-10-16T12:02:37Z"
    )
    for cutoff, default in (("2026-10-16T12:02:00Z", True), ("2026-10-16T12:02:17Z", False)):
        packet = {"token_id": "T", "cutoff": cutoff, "anchor": None}
        result, captured = await assess_native(
            original_truth,
            postgres_template,
            monkeypatch,
            event={},
            token="T",
            cutoff=cutoff,
            packet=packet,
            expected_packet=dict(packet, event_id=None),
            default=default,
            progress=True,
        )
        assert captured == [None]
        assert result["cutoff"] == cutoff


@pytest.mark.parametrize(
    "event_time,precision,verified,kind",
    [
        ("2026-10-16T08:00:00Z", "EXACT", True, "T0"),
        ({"value": "2026-10-16T08:00:00Z", "precision": "EXACT"}, None, True, "T0"),
        ("2026-10-16T08:00:00Z", "EXACT", False, "T1"),
        ({"value": "2026-10-16T08:00:00Z", "precision": "BUCKET(1d)"}, None, True, "T1"),
    ],
    ids=["exact-verified", "nested-exact-verified", "unverified", "nested-bucket"],
)
async def test_native_service_canonical_anchor(
    original_truth,
    postgres_template,
    monkeypatch,
    event_time,
    precision,
    verified,
    kind,
):
    event = {
        "event_time": event_time,
        "event_time_prec": precision,
        "state_verified": verified,
        "public_at": "2026-10-16T09:00:00Z",
        "known_to_us_at": "2026-10-16T10:00:00Z",
    }
    selected = anchor(
        kind,
        "2026-10-16T08:00:00Z" if kind == "T0" else "2026-10-16T09:00:00Z",
        "EXACT" if kind == "T0" else None,
    )
    packet = {"token_id": "T", "cutoff": END, "anchor": selected}
    expected = dict(deepcopy(packet), event_id="EVT", **public_context("2026-10-16T09:00:00Z"))
    result, captured = await assess_native(
        original_truth,
        postgres_template,
        monkeypatch,
        event=event,
        token="T",
        cutoff=END,
        packet=packet,
        expected_packet=expected,
    )
    assert captured == [selected]
    assert captured[0] == original_truth.event_anchor(event)
    assert result["relative"]["anchor"]["type"] == kind
    assert result["relative"]["anchor"]["ts"] == selected["ts"]


async def test_native_service_does_not_retain_assessment_worker_baselines(
    original_truth, postgres_template, monkeypatch
):
    for token, cutoff in (("A", END), ("B", "2026-10-16T12:01:00Z"), ("B", "2026-10-16T12:31:00Z")):
        packet = {"token_id": token, "cutoff": cutoff, "anchor": None}
        await assess_native(
            original_truth,
            postgres_template,
            monkeypatch,
            event={},
            token=token,
            cutoff=cutoff,
            packet=packet,
            expected_packet=dict(packet, event_id=None),
        )
        assert compute_workers._executors == {} and compute_workers._baselines == {}


async def test_native_service_token_isolation(original_truth, postgres_template, monkeypatch):
    identities = []
    for token in ("T", "T", "U", "T"):
        packet = {"token_id": token, "cutoff": END}
        result, captured = await assess_native(
            original_truth,
            postgres_template,
            monkeypatch,
            event={},
            token=token,
            cutoff=END,
            packet=packet,
            expected_packet=dict(packet, event_id=None),
            config={"config_version": "cfg", "code_version": "code"},
        )
        assert captured == [None]
        identities.append(result["mta_id"])
    assert len(set(identities)) == 4


async def test_native_service_latest_loser_repricing(original_truth, postgres_template, monkeypatch):
    packet = compact_start_snapshot()
    assert original_truth.compute_market_truth(deepcopy(packet))["relative"]["repricing_band"] == "EARLY"
    expected = dict(
        deepcopy(packet),
        event_id="EVT",
        token_id="T",
        expected_direction="LOSER",
        **public_context("2026-10-16T08:00:00Z"),
    )
    result, _ = await assess_native(
        original_truth,
        postgres_template,
        monkeypatch,
        event={"public_at": "2026-10-16T08:00:00Z"},
        token="T",
        cutoff=END,
        packet=packet,
        expected_packet=expected,
        edges=((1, "PRIMARY"), (2, "LOSER")),
    )
    assert result["relative"]["expected_direction"] == "LOSER"
    assert result["relative"]["repricing_sign"] == -1
    assert result["relative"]["repricing_band"] == "UNPRICED"


async def test_native_service_explicit_and_legacy_precision(original_truth, postgres_template, monkeypatch):
    event = {"public_at": "2026-10-16T08:00:00Z", "public_at_prec": "BUCKET(1h)"}
    for precision in ("BUCKET(1h)", None):
        if precision is None:
            del event["public_at_prec"]
        selected = anchor("T1", "2026-10-16T08:00:00Z", precision)
        packet = precision_packet()
        packet["anchor"] = deepcopy(selected)
        expected = dict(
            deepcopy(packet), event_id="E", token_id="T", **public_context("2026-10-16T08:00:00Z", precision)
        )
        result, captured = await assess_native(
            original_truth,
            postgres_template,
            monkeypatch,
            event=event,
            token="T",
            cutoff=END,
            packet=packet,
            expected_packet=expected,
            config={"config_version": "t", "code_version": "t"},
        )
        assert captured == [selected]
        assert result["event_public_at_prec"] == precision
        assert result["relative"]["anchor"]["ts_prec"] == precision


async def condition_native(original, postgres_template, monkeypatch, *, requirements, loser=False):
    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn, "owned disposable PostgreSQL is required"
    packet = compact_start_snapshot()
    expected_packet = dict(deepcopy(packet), event_id="E", token_id="T")
    if loser:
        expected_packet["expected_direction"] = "LOSER"
    config = {"config_version": "t", "code_version": "t"}
    expected = original.compute_condition_values(
        deepcopy(expected_packet), deepcopy(requirements), config=config
    )
    calls, reads = [], []

    class Market:
        async def condition_snapshot(self, token_id, cutoff, *, anchor, requirements):
            assert token_id == "T" and cutoff == END
            assert anchor == {
                "type": "T1",
                "ts": "2026-10-16T08:00:00Z",
                "ts_prec": None,
                "ts_prec_reason": "INCOMPLETE",
            }
            calls.append(deepcopy(requirements))
            return deepcopy(packet)

        async def snapshot(self, *args, **kwargs):
            raise AssertionError("no full market snapshot")

    async with owned_native_repository(postgres_template, dsn) as repo:
        assert getattr(repo, "clickhouse", None) is not None
        await repo.put("evt.events", "E", {"event_id": "E", "public_at": "2026-10-16T08:00:00Z"})
        if loser:
            for version, role in ((1, "BENEFICIARY"), (2, "LOSER")):
                key = f"BFE-CONDITION-{version}"
                await repo.put(
                    "evt.beneficiary_edges",
                    key,
                    {"edge_id": key, "event_id": "E", "token_id": "T", "map_version": version, "role": role},
                )
        original_get, original_list = repo.get, repo.list
        before = await original_list("ana.market_truth", limit=10000)

        async def get(collection, key):
            assert (collection, key) == ("evt.events", "E")
            reads.append(collection)
            return await original_get(collection, key)

        async def listing(collection, **kwargs):
            assert loser and collection == "evt.beneficiary_edges"
            assert kwargs == {"filters": {"event_id": "E", "token_id": "T"}, "limit": 10000}
            return await original_list(collection, **kwargs)

        async def forbidden(*args, **kwargs):
            raise AssertionError("transient condition evaluation must not write")

        monkeypatch.setattr(repo, "get", get)
        monkeypatch.setattr(repo, "list", listing)
        monkeypatch.setattr(repo, "put", forbidden)
        counted = CountingNative(repo.clickhouse)
        repo.clickhouse = counted
        forbid_python_business(monkeypatch)
        actual = await truth.MarketTruthService(repo, SimpleNamespace(**config), Market()).condition_values(
            "E", "T", END, requirements
        )
        assert calls == [requirements] and reads == ["evt.events"]
        assert len(counted.requests) == 1
        assert_complete(actual, expected)
        assert "mta_id" not in actual
        assert await original_list("ana.market_truth", limit=10000) == before
        return actual, expected


async def test_native_service_light_conditions_transient(original_truth, postgres_template, monkeypatch):
    actual, _ = await condition_native(
        original_truth, postgres_template, monkeypatch, requirements=REQUIREMENTS[:2]
    )
    assert actual["spot"]["W1H"]["direction"] == "BUY"


async def test_native_service_selective_latest_loser(original_truth, postgres_template, monkeypatch):
    actual, expected = await condition_native(
        original_truth,
        postgres_template,
        monkeypatch,
        requirements=[{"metric": "price.repricing_band", "window": "EVENT"}],
        loser=True,
    )
    assert actual["relative"]["expected_direction"] == "LOSER"
    assert actual["relative"]["repricing_band"] == expected["relative"]["repricing_band"]


async def test_native_real_loader_selective_footprint_transient(
    original_truth,
    postgres_template,
    monkeypatch,
    tmp_path,
    record_property,
):
    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn, "owned disposable PostgreSQL is required"
    record_property("loader_backend", "MemoryRepository-original-legacy-packet")
    packet = compact_start_snapshot()
    registry = {"instruments": packet["instruments"], "tokens": [{"token_id": "T", "symbol": "T"}]}
    settings = SimpleNamespace(
        config_version="t",
        code_version="t",
        data_dir=tmp_path,
        user_agent="test",
        load_config=lambda name: (
            registry
            if name == "registry"
            else {"metrics": packet["semantics"]}
            if name == "semantics"
            else {}
        ),
    )
    req = [
        {"metric": "spot.cohort.direction", "window": "W1H"},
        {"metric": "price.close_vs_level", "window": "W1H"},
    ]
    captured, reads, event_reads = [], [], []
    repo = MemoryRepository()
    await repo.put_market_batch("mkt.flow_bars_1m", packet["flow_bars"])
    await repo.put_market_batch(
        "mkt.klines_1m", [row for row in packet["klines"] if row.get("interval_s") == 60]
    )
    original_range, original_get = repo.iter_range, repo.get

    async def ranged(collection, *args, **kwargs):
        reads.append(collection)
        async for row in original_range(collection, *args, **kwargs):
            yield row

    async def get(collection, key):
        event_reads.append((collection, key))
        if collection == "evt.events":
            return {"public_at": packet["anchor"]["ts"]}
        return await original_get(collection, key)

    async def forbidden(*args, **kwargs):
        raise AssertionError("selective evaluation cannot write/flush/persist full truth")

    monkeypatch.setattr(repo, "iter_range", ranged)
    monkeypatch.setattr(repo, "get", get)
    monkeypatch.setattr(repo, "put", forbidden)
    monkeypatch.setattr(repo, "put_market_batch", forbidden)
    async with owned_native_repository(postgres_template, dsn) as native_repo:
        assert getattr(native_repo, "clickhouse", None) is not None
        before = await native_repo.list("ana.market_truth", limit=10000)
        await native_repo.put("evt.events", "E", {"event_id": "E", "public_at": packet["anchor"]["ts"]})
        original_native_get = native_repo.get

        async def native_get(collection, key):
            assert (collection, key) == ("evt.events", "E")
            event_reads.append((collection, key))
            return await original_native_get(collection, key)

        monkeypatch.setattr(native_repo, "get", native_get)
        monkeypatch.setattr(native_repo, "put", forbidden)
        monkeypatch.setattr(native_repo, "put_market_batch", forbidden)
        counted = CountingNative(native_repo.clickhouse)
        native_repo.clickhouse = counted
        async with httpx.AsyncClient() as http:
            market = MarketService(repo, settings, http=http)
            for name in ("snapshot", "_put", "flush_market"):
                monkeypatch.setattr(market, name, forbidden)
            original_snapshot = market.condition_snapshot

            async def observed(*args, **kwargs):
                assert args == ("T", END) and kwargs["requirements"] == req
                assert kwargs["anchor"] == anchor("T1", "2026-10-16T08:00:00Z")
                value = await original_snapshot(*args, **kwargs)
                captured.append(deepcopy(value))
                return value

            monkeypatch.setattr(market, "condition_snapshot", observed)
            forbid_python_business(monkeypatch)
            actual = await truth.MarketTruthService(native_repo, settings, market).condition_values(
                "E", "T", END, req
            )
        assert len(captured) == 1
        expected_packet = dict(deepcopy(captured[0]), event_id="E", token_id="T")
        expected = original_truth.compute_condition_values(
            expected_packet, deepcopy(req), config={"config_version": "t", "code_version": "t"}
        )
        assert_complete(actual, expected)
        assert actual["spot"]["W1H"]["direction"] == "BUY"
        assert Decimal(actual["relative"]["close_1h_price"]) == Decimal("101.1")
        assert set(reads) == {"mkt.flow_bars_1m", "mkt.klines_1m"}
        assert event_reads == [("evt.events", "E")]
        assert len(counted.requests) == 1
        assert "mta_id" not in actual
        assert await native_repo.list("ana.market_truth", limit=10000) == before


@pytest.mark.process
async def test_native_service_is_independent_of_busy_reaction_process(
    original_truth,
    postgres_template,
    monkeypatch,
    worker_processes,
):
    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn and os.environ.get("CLICKHOUSE_TEST_URL")
    packet = _spot_packet()
    config = {"config_version": "cfg", "code_version": "code"}
    expected = original_truth.compute_market_truth(dict(deepcopy(packet), event_id=None), config=config)
    expected["measurement_context"]["numeric_policy"] = native.NUMERIC_POLICY

    class Market:
        async def snapshot(self, token_id, cutoff, anchor=None):
            assert (token_id, cutoff, anchor) == ("T", END, None)
            return deepcopy(packet)

    async with owned_native_repository(postgres_template, dsn) as repo:
        assert getattr(repo, "clickhouse", None) is not None
        counted = CountingNative(repo.clickhouse)
        repo.clickhouse = counted
        service = truth.MarketTruthService(repo, SimpleNamespace(**config), Market())
        forbid_python_business(monkeypatch)
        reaction = compute_workers.executor(compute_workers.REACTION)
        reaction_pid = await asyncio.wrap_future(reaction.submit(os.getpid))
        busy = reaction.submit(time.sleep, 30)
        reaction_pids = set(reaction._processes)
        assert reaction_pids == {reaction_pid} and os.getpid() not in reaction_pids
        gaps, done = [], asyncio.Event()

        async def ticks():
            last = time.monotonic()
            while not done.is_set():
                await asyncio.sleep(0.01)
                gaps.append(time.monotonic() - last)
                last = time.monotonic()

        ticker = asyncio.create_task(ticks())
        try:
            started = time.monotonic()
            result = await service.assess(None, "T", END)
            elapsed = time.monotonic() - started
            assert not busy.done() and elapsed < 25
            done.set()
            await ticker
            assert gaps and max(gaps) < 1
            assert set(compute_workers._executors) == {compute_workers.REACTION}
            assert compute_workers._baselines == {}
            assert len(counted.requests) == 1
            assert_complete({k: v for k, v in result.items() if k != "mta_id"}, expected)
            assert result["mta_id"].startswith("MTA")
            assert await repo.get("ana.market_truth", result["mta_id"]) == result
            assert await repo.put("ana.market_truth", result["mta_id"], result, immutable=True) == result
            with pytest.raises(ImmutableRecordError):
                await repo.put("ana.market_truth", result["mta_id"], dict(result, move_class="tampered"))
            assert await repo.get("ana.market_truth", result["mta_id"]) == result
        finally:
            done.set()
            await ticker
            for pid in reaction_pids:
                os.kill(pid, 9)
            with pytest.raises(BrokenProcessPool):
                busy.result(timeout=20)


async def test_native_physical_loader_independent_schema_valid_corpus(
    original_truth,
    postgres_template,
    monkeypatch,
    tmp_path,
    record_property,
):
    from tank.market_schema import MARKET_COLUMNS, MARKET_REQUIRED

    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn and os.environ.get("CLICKHOUSE_TEST_URL")
    record_property("loader_backend", "owned-ClickHouse-PostgreSQL-independent-synthetic-corpus")
    cutoff = "2026-09-12T15:00:00Z"
    iid = "SCHEMA:spot:SCHEMA"
    instrument = {
        "instrument_id": iid,
        "token_id": "SCHEMA",
        "venue": "SCHEMA",
        "market_type": "SPOT",
        "quote_asset": "USD",
    }
    registry = {"instruments": [instrument], "tokens": [{"token_id": "SCHEMA", "symbol": "SCHEMA"}]}
    settings = SimpleNamespace(
        config_version="physical-synthetic-v1",
        code_version="physical-synthetic-v1",
        data_dir=tmp_path,
        user_agent="test",
        load_config=lambda name: registry if name == "registry" else {},
    )
    flow = [
        {
            "instrument_id": iid,
            "token_id": "SCHEMA",
            "venue": "SCHEMA",
            "market_type": "SPOT",
            "quote_asset": "USD",
            "minute_start": iso(parse_time("2026-09-12T14:00:00Z") + timedelta(minutes=n)),
            "minute_start_prec": "BUCKET(1m)",
            "buy_qty": "12",
            "sell_qty": "3",
            "n_buy": 4,
            "n_sell": 1,
            "sum_sq_qty": "45",
            "o": "23",
            "h": "24",
            "l": "22",
            "c": "23",
            "buy_usd": "276",
            "sell_usd": "69",
            "volume_usd": "345",
            "quote_volume": "345",
            "size_buckets": {},
            "complete": True,
            "zero_trade_minute": False,
            "bar_version": 1,
            "trade_aggregation": "PER_FILL",
            "config_version": "physical-synthetic-v1",
            "code_version": "physical-synthetic-v1",
        }
        for n in range(60)
    ]
    klines = [
        {
            "instrument_id": iid,
            "token_id": "SCHEMA",
            "venue": "SCHEMA",
            "market_type": "SPOT",
            "quote_asset": "USD",
            "open_time": "2026-09-12T14:59:00Z",
            "open_time_prec": "BUCKET(1m)",
            "interval_s": 60,
            "o": "23",
            "h": "24",
            "l": "22",
            "c": "23",
            "volume_native": "15",
            "n_trades": 5,
            "quote_volume": "345",
            "quote_volume_usd": "345",
            "quote_usd_rate": "1",
            "complete": True,
            "config_version": "physical-synthetic-v1",
            "code_version": "physical-synthetic-v1",
        }
    ]
    admitted_populations = {"mkt.flow_bars_1m": deepcopy(flow), "mkt.klines_1m": deepcopy(klines)}
    excluded_flow = [
        {
            "instrument_id": control_iid,
            "token_id": control_token,
            "venue": "SCHEMA",
            "market_type": "SPOT",
            "quote_asset": "USD",
            "minute_start": stamp,
            "minute_start_prec": "BUCKET(1m)",
            "buy_qty": "1",
            "sell_qty": "9",
            "n_buy": 1,
            "n_sell": 3,
            "sum_sq_qty": "28",
            "o": "99",
            "h": "100",
            "l": "98",
            "c": "99",
            "buy_usd": "99",
            "sell_usd": "891",
            "volume_usd": "990",
            "quote_volume": "990",
            "size_buckets": {},
            "complete": True,
            "zero_trade_minute": False,
            "bar_version": 1,
            "trade_aggregation": "PER_FILL",
            "config_version": "physical-synthetic-v1",
            "code_version": "physical-synthetic-v1",
        }
        for control_iid, control_token, stamp in (
            (iid, "SCHEMA", "2026-09-12T15:00:00Z"),
            (iid, "SCHEMA", "2026-09-12T15:01:00Z"),
            ("FOREIGN:spot:FOREIGN", "FOREIGN", "2026-09-12T14:30:00Z"),
        )
    ]
    excluded_klines = [
        {
            "instrument_id": control_iid,
            "token_id": control_token,
            "venue": "SCHEMA",
            "market_type": "SPOT",
            "quote_asset": "USD",
            "open_time": stamp,
            "open_time_prec": "BUCKET(1m)",
            "interval_s": 60,
            "o": "99",
            "h": "100",
            "l": "98",
            "c": "99",
            "volume_native": "10",
            "n_trades": 4,
            "quote_volume": "990",
            "quote_volume_usd": "990",
            "quote_usd_rate": "1",
            "complete": True,
            "config_version": "physical-synthetic-v1",
            "code_version": "physical-synthetic-v1",
        }
        for control_iid, control_token, stamp in (
            (iid, "SCHEMA", "2026-09-12T14:58:00Z"),
            (iid, "SCHEMA", "2026-09-12T15:00:00Z"),
            (iid, "SCHEMA", "2026-09-12T15:01:00Z"),
            ("FOREIGN:spot:FOREIGN", "FOREIGN", "2026-09-12T14:59:00Z"),
        )
    ]
    populations = {"mkt.flow_bars_1m": flow + excluded_flow, "mkt.klines_1m": klines + excluded_klines}
    req = [
        {"metric": "spot.cohort.direction", "window": "W1H"},
        {"metric": "price.close_vs_level", "window": "W1H"},
    ]
    captured, reads, event_reads = [], [], []
    async with owned_native_repository(postgres_template, dsn) as repo:
        assert getattr(repo, "clickhouse", None) is not None
        for collection, rows in populations.items():
            assert all(set(row) <= set(MARKET_COLUMNS[collection]) for row in rows)
            assert all(
                all(row.get(field) is not None for field in MARKET_REQUIRED[collection]) for row in rows
            )
            await repo.put_market_batch(collection, deepcopy(rows))
        await repo.put(
            "evt.events", "E-SCHEMA", {"event_id": "E-SCHEMA", "public_at": "2026-09-12T14:00:00Z"}
        )
        before = await repo.list("ana.market_truth", limit=10000)
        stored = {}
        for collection, rows in populations.items():
            field = "minute_start" if collection == "mkt.flow_bars_1m" else "open_time"
            stored[collection] = [
                row
                async for row in repo.iter_market_projection(
                    collection, list(rows[0]), field, start="2026-09-12T13:59:00Z", end="2026-09-12T15:02:00Z"
                )
            ]
            assert len(stored[collection]) == len(rows)
            assert len({(row["instrument_id"], row[field]) for row in rows}) == len(rows)
            assert sorted(
                [{key: row[key] for key in rows[0]} for row in stored[collection]],
                key=lambda row: (row[field], row["instrument_id"]),
            ) == sorted(rows, key=lambda row: (row[field], row["instrument_id"]))
        original_get, original_list = repo.get, repo.list
        counted = CountingNative(repo.clickhouse)
        repo.clickhouse = counted

        async def forbidden(*args, **kwargs):
            raise AssertionError("physical selective READ phase cannot write or use full snapshot")

        async def get(collection, key):
            assert (collection, key) == ("evt.events", "E-SCHEMA")
            event_reads.append((collection, key))
            return await original_get(collection, key)

        def observed_iterator(name, original):
            async def observed(collection, *args, **kwargs):
                reads.append((name, collection))
                async for row in original(collection, *args, **kwargs):
                    yield row

            return observed

        async def latest(collection, *args, **kwargs):
            reads.append(("latest_before", collection))
            assert collection == "mkt.tickers_24h"
            result = await original_latest(collection, *args, **kwargs)
            assert result is None
            return result

        original_latest = repo.latest_before
        for name in ("iter_range", "iter_market_projection"):
            monkeypatch.setattr(repo, name, observed_iterator(name, getattr(repo, name)))
        monkeypatch.setattr(repo, "latest_before", latest)
        monkeypatch.setattr(repo, "get", get)
        for name in ("put", "put_market_batch", "list"):
            monkeypatch.setattr(repo, name, forbidden)
        monkeypatch.setattr(counted, "insert", forbidden)
        monkeypatch.setattr(counted, "execute", forbidden)
        async with httpx.AsyncClient() as http:
            market = MarketService(repo, settings, http=http)
            for name in ("snapshot", "_put", "flush_market"):
                monkeypatch.setattr(market, name, forbidden)
            original_snapshot = market.condition_snapshot

            async def snapshot(*args, **kwargs):
                assert args == ("SCHEMA", cutoff) and kwargs["requirements"] == req
                assert kwargs["anchor"] == anchor("T1", "2026-09-12T14:00:00Z")
                value = await original_snapshot(*args, **kwargs)
                captured.append(deepcopy(value))
                return value

            monkeypatch.setattr(market, "condition_snapshot", snapshot)
            forbid_python_business(monkeypatch)
            actual = await truth.MarketTruthService(repo, settings, market).condition_values(
                "E-SCHEMA", "SCHEMA", cutoff, req
            )
        assert len(captured) == 1
        for collection, family in (("mkt.flow_bars_1m", "flow_bars"), ("mkt.klines_1m", "klines")):
            rows = admitted_populations[collection]
            assert len(captured[0][family]) == len(rows)
            assert [{key: row[key] for key in rows[0]} for row in captured[0][family]] == rows
        expected_packet = dict(deepcopy(captured[0]), event_id="E-SCHEMA", token_id="SCHEMA")
        expected = original_truth.compute_condition_values(
            expected_packet,
            deepcopy(req),
            config={"config_version": "physical-synthetic-v1", "code_version": "physical-synthetic-v1"},
        )
        assert_complete(actual, expected)
        assert Decimal(actual["relative"]["close_1h_price"]) == Decimal("23")
        assert actual["spot"]["W1H"]["direction"] == "BUY"
        assert set(reads) == {
            ("iter_range", "mkt.flow_bars_1m"),
            ("iter_range", "mkt.klines_1m"),
            ("latest_before", "mkt.tickers_24h"),
        }
        analytical = [query for query in counted.requests if " AS eng_raw," in query]
        loader = [query for query in counted.requests if " AS eng_raw," not in query]
        assert len(analytical) == 1 and len(loader) >= 3
        assert all(
            any(collection in query for collection in (*populations, "mkt.tickers_24h")) for query in loader
        )
        assert event_reads == [("evt.events", "E-SCHEMA")]
        assert "mta_id" not in actual
        assert await original_list("ana.market_truth", limit=10000) == before
