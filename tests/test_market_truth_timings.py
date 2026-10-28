import asyncio
import json
import logging
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal

import httpx
import pytest

import tank.analysis.truth as truth
import tank.logging as telemetry
from tank.clickhouse_market import ClickHouse
from tank.util import iso, parse_time


def records(caplog):
    return [json.loads(r.message) for r in caplog.records if r.name == "tank.market_timing"]


def test_full_compute_logs_domains_and_preserves_report(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    packet = {"token_id": "T", "cutoff": "2026-10-28T12:00:00Z", "instruments": []}
    original = deepcopy(packet)
    result = truth.compute_market_truth(packet)
    assert result["token_id"] == "T"
    assert result["primary_window"] == "W4H"
    assert result["spot"]["EVENT"]["classification"] == "INSUFFICIENT_DATA"
    assert result["relative"]["windows"]["EVENT"]["reason"] == "INCOMPLETE"
    assert packet == original
    ended = [r for r in records(caplog) if r["status"] == "ok"]
    stages = {r["stage"] for r in ended}
    assert {
        "compute",
        "compute.lock_wait",
        "compute.prepare",
        "compute.spot",
        "compute.relative",
        "compute.derivatives",
        "compute.liquidity",
        "compute.supply",
        "compute.evidence",
        "compute.support",
        "compute.timestamps",
        "compute.metadata",
        "compute.jsonable",
    } <= stages
    windows = [r for r in ended if r["stage"] == "compute.window"]
    assert {r["window"] for r in windows} == {"W15M", "W1H", "W4H", "W24H", "EVENT"}
    for window in windows:
        children = {r["stage"] for r in ended if r["parent_id"] == window["span_id"]}
        assert {
            "compute.derivatives",
            "compute.liquidity",
            "compute.supply",
            "compute.evidence",
            "compute.support",
        } <= children
    assert len({r["trace_id"] for r in ended}) == 1
    assert all(r["wall_s"] >= 0 for r in ended)


@pytest.mark.parametrize("error", [ValueError("private payload"), asyncio.CancelledError()])
def test_failed_compute_closes_timing_and_preserves_exception(monkeypatch, caplog, error):
    caplog.set_level(logging.INFO, logger="tank.market_timing")

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(truth, "_compute_market_truth", fail)
    with pytest.raises(type(error)) as caught:
        truth.compute_market_truth({})
    assert caught.value is error
    ended = [r for r in records(caplog) if r["stage"] == "compute" and r["status"] != "started"]
    assert len(ended) == 1
    assert ended[0]["status"] == ("cancelled" if isinstance(error, asyncio.CancelledError) else "error")
    assert "private payload" not in caplog.text


@pytest.mark.parametrize("error", [None, ValueError("domain failure"), asyncio.CancelledError()])
def test_logging_failure_preserves_compute_and_context(monkeypatch, error):
    def unavailable(*args, **kwargs):
        raise RuntimeError("logging unavailable")

    monkeypatch.setattr(logging.getLogger("tank.market_timing"), "info", unavailable)
    if error is None:
        result = truth.compute_market_truth(
            {"token_id": "T", "cutoff": "2026-10-28T12:00:00Z", "instruments": []}
        )
        assert result["token_id"] == "T"
        assert result["primary_window"] == "W4H"
    else:

        def fail(*args, **kwargs):
            raise error

        monkeypatch.setattr(truth, "_compute_market_truth", fail)
        with pytest.raises(type(error)) as caught:
            truth.compute_market_truth({})
        assert caught.value is error
    assert telemetry._MARKET_STAGE.get() is None


async def test_parallel_traces_and_thread_handoff_are_isolated(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    ready = asyncio.Event()
    entered = 0

    async def run():
        nonlocal entered
        with telemetry.market_stage("request"):
            entered += 1
            if entered == 2:
                ready.set()
            await ready.wait()
            await asyncio.to_thread(child)

    def child():
        with telemetry.market_stage("worker", cpu=True):
            pass

    await asyncio.gather(run(), run())
    ended = [r for r in records(caplog) if r["status"] == "ok"]
    roots = [r for r in ended if r["stage"] == "request"]
    assert len(roots) == 2
    assert len({r["trace_id"] for r in roots}) == 2
    for root in roots:
        children = [r for r in ended if r["parent_id"] == root["span_id"]]
        assert len(children) == 1
        assert children[0]["trace_id"] == root["trace_id"]
        assert children[0]["thread_cpu_s"] >= 0
        assert root["thread_cpu_s"] is None
    with telemetry.market_stage("next"):
        pass
    assert records(caplog)[-1]["parent_id"] is None


@pytest.mark.parametrize("status", [200, 503])
async def test_sql_timing_owns_query_id_and_never_logs_sql_or_credentials(caplog, status):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    seen = []

    async def respond(request):
        seen.append(request.url.params["query_id"])
        return httpx.Response(status, content=b'{"value": "1.00000000000000000001"}\n')

    client = ClickHouse(
        "http://reader:private-password@localhost:8123",
        http=httpx.AsyncClient(transport=httpx.MockTransport(respond)),
    )
    try:
        if status == 200:
            assert await client.rows("SELECT 'private-sql'") == [{"value": "1.00000000000000000001"}]
        else:
            with pytest.raises(RuntimeError, match="ClickHouse 503"):
                await client.rows("SELECT 'private-sql'")
    finally:
        await client.close()
    ended = [r for r in records(caplog) if r["stage"] == "sql.http" and r["status"] != "started"]
    assert len(ended) == 1
    assert ended[0]["query_id"] == seen[0]
    assert ended[0]["status"] == ("ok" if status == 200 else "error")
    assert "private-sql" not in caplog.text
    assert "private-password" not in caplog.text
    if status == 200:
        assert any(r["stage"] == "sql.json" and r["status"] == "ok" for r in records(caplog))


def test_oi_lookback_prepared_once_preserves_exact_bounds(monkeypatch):
    start = parse_time("2026-10-28T12:00:00Z")
    rows = [
        {"ts": iso(start + timedelta(minutes=m)), "oi_native": str(n), "oi_usd": str(n * 2)}
        for m, n in [
            (-1441, 999),
            (-1440, 100),
            (-1439, 200),
            (-4, 100),
            (-3, 100),
            (-2, 100),
            (-1, 100),
            (0, 100),
            (1, 110),
            (2, 999),
        ]
    ]
    calls = []

    def lookback():
        calls.append(1)
        return 1

    monkeypatch.setattr(truth, "lookback_days", lookback)
    token = truth._PARAMETERS.set(
        {"history_min_days": 0, "oi_sync_tolerance_s": 0, "normal_range_quantiles": [0, 1]}
    )
    try:
        result = truth._oi_stats(rows, start, start + timedelta(minutes=1))
    finally:
        truth._PARAMETERS.reset(token)
    assert result["oi_native_start"] == Decimal(100)
    assert result["oi_native_end"] == Decimal(110)
    assert result["oi_native_chg_pct"] == 0.1
    assert result["native_normal_range"] == [0.0, 1.0]
    assert result["usd_normal_range"] == [0.0, 1.0]
    assert result["start_observed_at"] == iso(start)
    assert result["end_observed_at"] == iso(start + timedelta(minutes=1))
    assert len(calls) == 1


def test_volume_lookback_prepared_once_keeps_decimal_history(monkeypatch):
    end = parse_time("2026-10-28T12:00:00Z")
    tiny = "1.00000000000000000001"
    packet = {
        "token_id": "T",
        "klines": [
            {
                "instrument_id": "A:spot:T",
                "token_id": "T",
                "market_type": "SPOT",
                "open_time": iso(end - timedelta(minutes=m)),
                "interval_s": 60,
                "volume_native": tiny,
                "complete": True,
            }
            for m in range(8, 0, -1)
        ],
    }
    calls = []

    def lookback():
        calls.append(1)
        return 1

    monkeypatch.setattr(truth, "lookback_days", lookback)
    token = truth._PARAMETERS.set({"history_min_days": 0})
    try:
        result = truth.support_features(packet, end - timedelta(minutes=1), end, {}, {}, {}, {})
    finally:
        truth._PARAMETERS.reset(token)
    assert result["spot_volume"]["value"] == Decimal(tiny)
    assert result["spot_volume"]["previous"] == Decimal(tiny)
    assert result["spot_volume"]["normal_range"] == [Decimal(tiny), Decimal(tiny)]
    assert result["spot_volume_within_normal"] is True
    assert len(calls) == 1
