import asyncio
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from test_market_service import setup as setup

from tank import clickhouse_market as chm
from tank.market import normalize as norm
from tank.market.service import COLLECTIONS, MarketService
from tank.market_storage import decode
from tank.util import parse_time

T0 = parse_time("2026-06-17T00:00:00Z")
EPOCH = parse_time("1970-01-01T00:00:00Z")


class ColumnClient:
    database = "mkt"

    def __init__(self, raw):
        self.columns = AsyncMock(return_value=raw)


def columns(rows):
    return {
        "open_us": [(parse_time(row["open_time"]) - EPOCH) // timedelta(microseconds=1) for row in rows],
        "open_time_prec": [row.get("open_time_prec") for row in rows],
        "recorded_unknown": [row.get("recorded_at") is None for row in rows],
        "complete": [row["complete"] for row in rows],
        "interval_s": [row["interval_s"] for row in rows],
        "c": [row["c"] for row in rows],
        "volume_native": [row["volume_native"] for row in rows],
        "_cursor_key": [str(i) for i in range(len(rows))],
    }


@pytest.mark.parametrize("kind", ["price", "volume"])
@pytest.mark.parametrize("duplicate", [False, True])
async def test_bulk_series_matches_rows_including_exact_tiny_negative_and_partial_intervals(
    setup, kind, duplicate
):
    repo, settings, inst = setup
    rows = [
        dict(
            open_time=T0 + timedelta(minutes=i),
            open_time_prec="BUCKET(60s)" if i != 3 else None,
            recorded_at=T0,
            c=close,
            volume_native=volume,
            complete=complete,
            interval_s=interval,
        )
        for i, (close, volume, complete, interval) in enumerate(
            [
                ("1e+3", "3.000", True, 60),
                ("-0.00", "-1e-900", True, 60),
                (None, None, False, 60),
                ("2.50", "-0.000", True, 60),
                ("NaN", "NaN", False, 30),
                ("1e-900", "1e-900", True, 60),
            ]
        )
    ]
    if duplicate:
        rows.insert(1, dict(rows[0], c="1001.00", volume_native="4.000"))
    ch = ColumnClient(columns(rows))
    async with httpx.AsyncClient() as http:
        dense = MarketService(SimpleNamespace(clickhouse=ch), settings, http=http)
        reference = MarketService(repo, settings, http=http)
        dense._condition_heads = AsyncMock(return_value=["head"])
        options = dict(kind=kind, coverage_count=6, fixed_window=True)
        expected = await reference._condition_kline_series(
            inst, T0, T0 + timedelta(minutes=6), T0 + timedelta(minutes=7), captured_rows=rows, **options
        )
        actual = await dense._condition_kline_series(
            inst, T0, T0 + timedelta(minutes=6), T0 + timedelta(minutes=7), **options
        )
    assert actual == expected
    assert ch.columns.await_count == 1
    query = ch.columns.call_args.args[0]
    assert "FINAL" in query and "ORDER BY open_time, storage_key" in query
    assert "recorded_at IS NULL OR recorded_at <=" in query
    if kind == "volume":
        assert actual["values"][1] == "-1E-900"
        assert list(actual["complete"]) == [1, 0, 0, 1, 1]


async def test_dense_numeric_cells_parse_once_per_distinct_text_and_share_sql(monkeypatch):
    raw = {
        "open_us": [1, 2, 3, 4],
        "open_time_prec": ["BUCKET(60s)"] * 4,
        "recorded_unknown": [False] * 4,
        "complete": [True] * 4,
        "interval_s": [60] * 4,
        "c": ["1.00", "1.00", "2e3", None],
        "volume_native": ["-1e-900", "-1e-900", "0.00", None],
        "_cursor_key": ["a", "b", "c", "d"],
    }
    ch = ColumnClient(raw)
    calls = []

    def decimal(value):
        calls.append(value)
        return Decimal(value)

    monkeypatch.setattr(chm, "Decimal", decimal)
    options = dict(instrument_id="spot", start=T0, end=T0 + timedelta(days=1), assembled_at=T0)
    cache = {}
    price = await chm.dense_projection_page(ch, field="c", raw_cache=cache, **options)
    volume = await chm.dense_projection_page(ch, field="volume_native", raw_cache=cache, **options)
    assert price["value"] == ["1.00", "1.00", "2E+3", None]
    assert volume["value"] == ["-1E-900", "-1E-900", "0.00", None]
    assert volume["decimal_values"][0] < 0
    assert calls == ["1.00", "2e3", "-1e-900", "0.00"]
    assert ch.columns.await_count == 1
    assert price["_cursor_key"] == volume["_cursor_key"] == ["a", "b", "c", "d"]


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
async def test_bulk_complete_nonfinite_volume_keeps_semantic_error(setup, value):
    _, settings, inst = setup
    row = dict(open_time=T0, recorded_at=T0, complete=True, interval_s=60, c="1", volume_native=value)
    ch = ColumnClient(columns([row]))
    async with httpx.AsyncClient() as http:
        market = MarketService(SimpleNamespace(clickhouse=ch), settings, http=http)
        market._condition_heads = AsyncMock(return_value=["head"])
        with pytest.raises(norm.SemanticError, match="nonfinite decimal"):
            await market._condition_kline_series(
                inst,
                T0,
                T0 + timedelta(minutes=1),
                T0 + timedelta(minutes=2),
                kind="volume",
                coverage_count=1,
            )


def test_wide_decoder_matches_typed_decode_with_nulls_aliases_and_numeric_scale():
    row = {"ts": T0.isoformat(), "oi_native": "1e-900", "oi_usd": "3.000", "metric_key": "oi"}
    typed = {**row, "ts": T0, "oi_native": Decimal("1e-900"), "oi_usd": Decimal("3.000")}
    assert chm._record("mkt.oi_obs", row) == decode("mkt.oi_obs", typed)


async def test_wide_snapshot_reads_overlap_and_keep_collection_order(setup, monkeypatch):
    repo, settings, inst = setup
    seen = set()
    released = asyncio.Event()
    async with httpx.AsyncClient() as http:
        market = MarketService(repo, settings, http=http)
        original = market._window_rows

        async def window(token, collection, *args, **kwargs):
            seen.add(collection)
            if len(seen) == len(COLLECTIONS) - 1:
                released.set()
            async with asyncio.timeout(1):
                await released.wait()
            return await original(token, collection, *args, **kwargs)

        monkeypatch.setattr(market, "_window_rows", window)
        result = await market.snapshot(inst["token_id"], T0)
    assert seen == {collection for name, (collection, _) in COLLECTIONS.items() if name != "supply"}
    assert [name for name in result if name in COLLECTIONS] == list(COLLECTIONS)
    assert all(result[name] == [] for name in COLLECTIONS)


@pytest.mark.parametrize("cancel", [False, True])
async def test_wide_read_failure_or_cancellation_drains_sibling_tasks(setup, monkeypatch, cancel):
    repo, settings, inst = setup
    started = asyncio.Event()
    active = set()
    async with httpx.AsyncClient() as http:
        market = MarketService(repo, settings, http=http)

        async def window(token, collection, *args, **kwargs):
            active.add(collection)
            if len(active) == len(COLLECTIONS) - 1:
                started.set()
            try:
                await started.wait()
                if not cancel and collection == "mkt.klines_1m":
                    raise ValueError("read failed")
                await asyncio.Event().wait()
            finally:
                active.remove(collection)

        monkeypatch.setattr(market, "_window_rows", window)
        task = asyncio.create_task(market.snapshot(inst["token_id"], T0))
        async with asyncio.timeout(1):
            await started.wait()
            if cancel:
                task.cancel()
            with pytest.raises(asyncio.CancelledError if cancel else ValueError):
                await task
        assert active == set()


async def test_clickhouse_wide_reads_use_large_pages_and_preserve_storage_keys(setup, monkeypatch):
    _, settings, inst = setup
    calls = []

    async def ranged(repo, collection, field, ids, start, end, filters, **kwargs):
        calls.append(kwargs["batch_size"])
        kwargs["keys_out"][inst["instrument_id"]] = ["raw|key"]
        return {inst["instrument_id"]: [{"ts": T0.isoformat(), "oi_native": "3.000"}]}

    monkeypatch.setattr("tank.market.service.market_range_by_instrument", ranged)
    async with httpx.AsyncClient() as http:
        market = MarketService(SimpleNamespace(clickhouse=object()), settings, http=http)
        keys = {}
        rows = await market._ranged_read(
            "mkt.oi_obs", "ts", [inst["instrument_id"]], T0, T0 + timedelta(days=1), keys_out=keys
        )
    assert calls == [20000]
    assert keys == {inst["instrument_id"]: ["raw|key"]}
    assert rows[inst["instrument_id"]][0]["oi_native"] == "3.000"
