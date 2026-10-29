from datetime import timedelta

import pytest
from test_clickhouse_market import IIDS, T0, chm, kline
from test_clickhouse_market import both as both

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
        assembled_at=T0 + timedelta(minutes=5),
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
