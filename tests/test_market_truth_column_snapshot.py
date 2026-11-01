import asyncio
import os
from datetime import timedelta

import httpx
import pytest
from test_clickhouse_market import IIDS, T0, chm, kline
from test_clickhouse_market import both as both

from tank.market.service import MarketService
from tank.util import parse_time

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("series", [False, True])
async def test_dense_columns_preserve_admitted_values_nulls_and_raw_pagination(both, series):
    _, repo, put = both
    rows = [
        dict(kline(IIDS[0], 0, "1e+3"), volume_native="3.000"),
        dict(
            kline(IIDS[0], 1, None, complete=False, recorded=T0 + timedelta(minutes=5)),
            volume_native=None,
        ),
        dict(kline(IIDS[0], 2, "2.50", recorded=T0 + timedelta(minutes=10)), volume_native="7"),
        dict(kline(IIDS[0], 3, "-0.00"), volume_native="9"),
        dict(kline(IIDS[0], 4, "1234"), volume_native="1234"),
        dict(kline(IIDS[1], 0, "999"), volume_native="999"),
    ]
    await put("mkt.klines_1m", rows)
    options = dict(
        instrument_id=IIDS[0],
        field="c",
        start=T0,
        end=T0 + timedelta(minutes=4),
        assembled_at=(T0 + timedelta(minutes=5)).isoformat(),
        limit=2,
        series=series,
    )
    selected = []
    cursor = None
    original_columns = repo.clickhouse.columns

    async def columns(query, params=None):
        q = chm.Query()
        chm._ids(q, [options["instrument_id"]])
        q.where("open_time IS NOT NULL")
        q.where(f"open_time >= {q.param(options['start'], chm.DT)}")
        q.where(f"open_time < {q.param(options['end'], chm.DT)}")
        availability = q.param(parse_time(options["assembled_at"]), chm.DT)
        q.where(f"(recorded_at IS NULL OR recorded_at <= {availability})")
        if cursor is not None:
            chm._after(
                q,
                ("open_time", cursor[0], chm.DT),
                ("storage_key", cursor[1], "String"),
            )
        common = (
            "SELECT toUnixTimestamp64Micro(open_time) AS open_us, open_time_prec, "
            "CAST(isNull(recorded_at), 'Bool') AS recorded_unknown, complete, interval_s, c, volume_native, "
        )
        if series:
            page = (
                "SELECT open_time, open_time_prec, recorded_at, complete, interval_s, c, volume_native, "
                f"storage_key FROM {chm._table('mkt.klines_1m')} WHERE {q.predicate()} "
                f"ORDER BY open_time, storage_key LIMIT {options['limit']}"
            )
            legacy = (
                common
                + "if(row_number() OVER (ORDER BY open_time DESC, storage_key DESC) = 1, "
                + f"storage_key, NULL) AS _cursor_key FROM ({page}) ORDER BY open_time, storage_key"
            )
        else:
            legacy = (
                common
                + f"storage_key AS _cursor_key FROM {chm._table('mkt.klines_1m')} "
                + f"WHERE {q.predicate()} ORDER BY open_time, storage_key LIMIT {options['limit']}"
            )
        actual = await original_columns(query, params)
        assert actual == await original_columns(legacy, q.params)
        selected.append(query)
        return actual

    repo.clickhouse.columns = columns
    first = await chm.dense_projection_page(repo.clickhouse, after=cursor, **options)
    epoch = parse_time("1970-01-01T00:00:00Z")
    start_us = (T0 - epoch) // timedelta(microseconds=1)
    assert first["open_us"] == [start_us, start_us + 60000000]
    assert first["value"] == ["1E+3", None]
    assert first["complete"] == [True, False]
    assert first["recorded_unknown"] == [False, False]
    if series:
        assert first["_cursor_key"][0] is None
        assert isinstance(first["_cursor_key"][-1], str)
    else:
        assert all(isinstance(key, str) for key in first["_cursor_key"])
    cursor = (T0 + timedelta(minutes=1), first["_cursor_key"][-1])
    second = await chm.dense_projection_page(repo.clickhouse, after=cursor, **options)
    assert second["open_us"] == [start_us + 180000000]
    assert second["value"] == ["-0.00"]
    assert len(second["_cursor_key"]) == 1
    cursor = (T0 + timedelta(minutes=3), second["_cursor_key"][-1])
    third = await chm.dense_projection_page(repo.clickhouse, after=cursor, **options)
    assert third["open_us"] == third["value"] == third["_cursor_key"] == []
    options["assembled_at"] = (T0 + timedelta(minutes=10)).isoformat()
    cursor = (T0 + timedelta(minutes=1), first["_cursor_key"][-1])
    admitted = await chm.dense_projection_page(repo.clickhouse, after=cursor, **options)
    assert admitted["open_us"] == [start_us + 120000000, start_us + 180000000]
    assert admitted["value"] == ["2.50", "-0.00"]
    assert admitted["_cursor_key"][-1] == second["_cursor_key"][-1]
    if series:
        assert all(" DESC" not in query for query in selected)
        assert all("UNBOUNDED FOLLOWING" in query for query in selected)


@pytest.mark.parametrize("kind", ["price", "volume"])
async def test_dense_column_reset_and_late_correction_match_row_repository(both, setup, kind):
    memory, repo, put = both
    _, settings, _ = setup
    instrument = dict(instrument_id=IIDS[0], token_id="bitcoin:BTC", venue="binance", market_type="SPOT")
    rows = [
        dict(kline(IIDS[0], 0, "1e+3"), volume_native="3.000", open_time_prec="BUCKET(60s)"),
        dict(kline(IIDS[0], 1, None, complete=False), volume_native=None, open_time_prec="BUCKET(60s)"),
        dict(kline(IIDS[0], 2, "2.50"), volume_native="7.00", open_time_prec=None),
    ]
    await put("mkt.klines_1m", rows)
    async with httpx.AsyncClient() as http:
        services = [MarketService(target, settings, http=http) for target in (memory, repo)]
        options = dict(kind=kind, coverage_count=3, fixed_window=True)
        initial = [
            await service._condition_kline_series(
                instrument, T0, T0 + timedelta(minutes=4), T0 + timedelta(minutes=5), **options
            )
            for service in services
        ]
        assert initial[0] == initial[1]
        assert initial[1]["changed_from_us"] == initial[0]["changed_from_us"]
        await put(
            "mkt.klines_1m",
            [dict(rows[0], c="1001.00", volume_native="4.000", recorded_at=T0 + timedelta(minutes=6))],
        )
        for service in services:
            service._condition_heads_memo.clear()
        corrected = [
            await service._condition_kline_series(
                instrument, T0, T0 + timedelta(minutes=4), T0 + timedelta(minutes=7), **options
            )
            for service in services
        ]
        assert corrected[0] == corrected[1]
        assert corrected[1]["dependency_fingerprint"] != initial[1]["dependency_fingerprint"]


async def test_snapshot_overlaps_independent_dense_reads_and_retains_each_lane(postgres_template, setup):
    _, settings, _ = setup
    token = "bitcoin:BTC"
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.put_market_batch(
            "mkt.klines_1m",
            [dict(kline(iid, minute, "1.5"), volume_native="3.00") for iid in IIDS for minute in range(3)],
        )
        seen, released = set(), asyncio.Event()
        original_post = repo.clickhouse._post

        async def post(query, params):
            value = await original_post(query, params)
            if query.endswith("FORMAT JSONColumns") and query.startswith(
                "SELECT toUnixTimestamp64Micro(open_time) AS open_us,"
            ):
                for iid in IIDS:
                    if f"instrument_id IN ('{iid}')" in query:
                        seen.add(iid)
                if len(seen) == len(IIDS):
                    released.set()
                async with asyncio.timeout(1):
                    await released.wait()
            return value

        repo.clickhouse._post = post
        async with httpx.AsyncClient() as http:
            market = MarketService(repo, settings, http=http)
            market.tokens = {token: {"token_id": token, "symbol": "BTC", "sector": "BENCHMARK"}}
            market.instruments = {
                iid: dict(instrument_id=iid, token_id=token, venue=venue, market_type="SPOT")
                for iid, venue in zip(IIDS, ("binance", "okx"), strict=True)
            }
            value = await market.snapshot(token, T0 + timedelta(minutes=3))
            assert seen == set(IIDS)
            assert set(value["stat_price_series"]) == set(IIDS)
            assert set(value["stat_volume_series"]) == set(IIDS)
            for iid in IIDS:
                assert list(value["stat_price_series"][iid]["values"]) == [1.5, 1.5, 1.5]
                assert value["stat_volume_series"][iid]["values"] == ["3.00", "3.00", "3.00"]
