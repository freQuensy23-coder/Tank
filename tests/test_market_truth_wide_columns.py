from copy import deepcopy
from datetime import timedelta
from decimal import Decimal

from tank import clickhouse_market as chm
from tank.util import parse_time

T0 = parse_time("2026-10-19T00:00:00Z")


class WideClient:
    def __init__(self, rows):
        self.data = rows
        self.row_queries = []
        self.column_queries = []

    async def rows(self, query, params=None):
        self.row_queries.append((query, deepcopy(params)))
        if query.startswith("SELECT storage_key,"):
            return [
                dict(storage_key=row["storage_key"], instrument_id=row["instrument_id"], _day=row["ts"])
                for row in self.data
            ]
        return deepcopy(self.data)

    async def columns(self, query, params=None):
        self.column_queries.append((query, deepcopy(params)))
        return {name: [row.get(name) for row in self.data] for name in self.data[0]}


async def test_wide_pages_decode_columns_directly_with_exact_cursor_and_mutable_values():
    rows = [
        dict(
            instrument_id="A:perp:T",
            storage_key=str(i),
            ts="2026-10-19T00:00:00Z",
            oi_native="1e-900",
            oi_usd="3.000",
            metric_key="oi",
        )
        for i in range(3)
    ]
    ch = WideClient(rows)
    actual = await chm.range_by_instrument_page(
        ch, "mkt.oi_obs", "ts", ["A:perp:T"], T0, T0 + timedelta(hours=1), {}, None, 3, False
    )
    expected = [
        (row["instrument_id"], chm._record("mkt.oi_obs", row), row["storage_key"], T0) for row in rows
    ]
    assert actual == expected
    assert len(ch.row_queries) == len(ch.column_queries) == 1
    query, params = ch.column_queries[0]
    assert "FINAL" in query and "storage_key IN" in query
    assert query.endswith("ORDER BY instrument_id, `ts`, storage_key")
    assert params
    actual[0][1]["oi_native"] = "caller-local"
    assert actual[1][1]["oi_native"] == str(Decimal("1e-900"))


async def test_empty_wide_page_skips_column_read():
    ch = WideClient([])
    assert (
        await chm.range_by_instrument_page(
            ch, "mkt.oi_obs", "ts", [], T0, T0, {}, None, 2, False
        )
        == []
    )
    assert len(ch.row_queries) == 1 and ch.column_queries == []
