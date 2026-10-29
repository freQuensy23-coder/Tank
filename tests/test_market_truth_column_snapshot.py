from datetime import timedelta

import httpx
import pytest
from test_clickhouse_market import IIDS, T0, chm, kline
from test_clickhouse_market import both as both

from tank.market.service import MarketService
from tank.util import parse_time

pytestmark = pytest.mark.integration


async def test_dense_columns_preserve_admitted_values_nulls_and_raw_pagination(both):
    _, repo, put = both
    rows = [
        dict(kline(IIDS[0], 0, "1e+3"), volume_native="3.000"),
        dict(kline(IIDS[0], 1, None, complete=False), volume_native=None),
        dict(kline(IIDS[0], 2, "2.50", recorded=T0 + timedelta(minutes=10)), volume_native="7"),
        dict(kline(IIDS[0], 3, "-0.00"), volume_native="9"),
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
    )
    first = await chm.dense_projection_page(repo.clickhouse, after=None, **options)
    epoch = parse_time("1970-01-01T00:00:00Z")
    start_us = (T0 - epoch) // timedelta(microseconds=1)
    assert first["open_us"] == [start_us, start_us + 60000000]
    assert first["value"] == ["1E+3", None]
    assert first["complete"] == [True, False]
    assert first["recorded_unknown"] == [False, False]
    second = await chm.dense_projection_page(
        repo.clickhouse,
        after=(T0 + timedelta(minutes=1), first["_cursor_key"][-1]),
        **options,
    )
    assert second["open_us"] == [start_us + 180000000]
    assert second["value"] == ["-0.00"]
    assert len(second["_cursor_key"]) == 1


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
