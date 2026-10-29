from datetime import timedelta

import pytest
from test_clickhouse_market import IIDS, T0, ch_row
from test_clickhouse_market import both as both

from tank import clickhouse_market as chm
from tank import market_storage as ms
from tank.market_schema import MARKET_COLUMNS
from tank.util import iso

pytestmark = pytest.mark.integration


async def test_sampled_oi_state_suppresses_unchanged_payload_and_refreshes_interior_corrections(both):
    memory, routed, put = both
    collection = "mkt.oi_obs"
    rows = [
        dict(
            instrument_id=iid,
            metric_key=metric,
            ts=iso(T0 + timedelta(minutes=minute)),
            ts_prec="EXACT",
            oi_native=str(value) + ".00",
            oi_usd=None,
            recorded_at=iso(T0 + timedelta(minutes=10)),
        )
        for iid, metric, minute, value in (
            (IIDS[0], "native", 0, 1),
            (IIDS[0], "native", 2, 2),
            (IIDS[0], "native", 6, 3),
            (IIDS[1], "native", 1, 4),
            (IIDS[0], "other", 1, 5),
        )
    ]
    await put(collection, rows)
    args = collection, "ts", ("metric_key",), IIDS, T0, T0 + timedelta(minutes=10), 300
    expected = await ms.sampled_market_history(memory, *args)
    actual_rows, selected = routed.clickhouse.rows, []

    async def observed(query, params=None):
        result = await actual_rows(query, params)
        if "AS fingerprint" in query:
            selected.append(result)
        return result

    routed.clickhouse.rows = observed
    cache = {}
    first = await chm.sampled_market_history(routed.clickhouse, *args, cache=cache)
    second = await chm.sampled_market_history(routed.clickhouse, *args, cache=cache)
    assert first == second == expected
    assert len(first) == 4
    assert len(selected[-1]) == 1
    assert selected[-1][0]["records"] == []
    assert len(selected[-2][0]["records"]) == len(expected)
    previous_fingerprint = selected[-1][0]["fingerprint"]
    first[0]["gap_reason"] = "caller-local"
    assert await chm.sampled_market_history(routed.clickhouse, *args, cache=cache) == expected
    assert selected[-1][0]["records"] == []

    changed = dict(rows[1], oi_native="200.000", mark_price="17.00")
    await memory.put_market_batch(collection, [changed])
    prepared = ms._prepare_records(collection, [changed], None, MARKET_COLUMNS[collection], False)
    for key, record in prepared.items():
        await routed.clickhouse.insert(collection, [dict(ch_row(collection, record, key), _version=2)])
    refreshed = await chm.sampled_market_history(routed.clickhouse, *args, cache=cache)
    assert refreshed == await ms.sampled_market_history(memory, *args)
    assert selected[-1][0]["fingerprint"] != previous_fingerprint
    assert len(selected[-1][0]["records"]) == len(refreshed)
    assert any(row["oi_native"] == "200.000" and row["mark_price"] == "17.00" for row in refreshed)
    assert any(row["oi_native"] == "2.00" for row in first)
