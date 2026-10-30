from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, FloatOperation, InvalidOperation, localcontext

import pytest

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
        return {name: [row.get(name) for row in self.data] for name in self.data[0]} if self.data else {}


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
    assert ch.row_queries == [] and len(ch.column_queries) == 1
    query, params = ch.column_queries[0]
    assert "FINAL" in query and "storage_key IN" in query
    assert query.endswith("ORDER BY instrument_id, `ts`, storage_key")
    assert params
    actual[0][1]["oi_native"] = "caller-local"
    assert actual[1][1]["oi_native"] == str(Decimal("1e-900"))


async def test_empty_wide_page_uses_one_transport_read():
    ch = WideClient([])
    assert await chm.range_by_instrument_page(ch, "mkt.oi_obs", "ts", [], T0, T0, {}, None, 2, False) == []
    assert ch.row_queries == [] and len(ch.column_queries) == 1


@pytest.mark.parametrize("collection", list(chm.MARKET_TABLES))
def test_wide_column_decoder_matches_every_typed_schema_and_keeps_mutable_values_separate(collection):
    values = {
        "text": "x",
        "numeric": "3.000",
        "bigint": 1,
        "boolean": True,
        "timestamptz": "2026-10-19T00:00:00Z",
        "text[]": ["x"],
        "jsonb": '{"items":[]}',
    }
    row = {name: values[kind] for name, kind in chm.MARKET_COLUMNS[collection].items()}
    row["storage_key"] = "first"
    time_field = chm.MARKET_TABLES[collection]
    rows = [row, dict(row, storage_key="second")]
    columns = {name: [item[name] for item in rows] for name in row}
    actual = chm._wide_column_records(collection, columns, time_field)
    assert actual == [
        (item["instrument_id"], chm._record(collection, item), item["storage_key"], T0) for item in rows
    ]
    for name, kind in chm.MARKET_COLUMNS[collection].items():
        if kind == "jsonb":
            actual[0][1][name]["items"].append(1)
            assert actual[1][1][name] == {"items": []}
        elif kind == "text[]":
            actual[0][1][name].append("changed")
            assert actual[1][1][name] == ["x"]


def test_wide_column_decoder_refuses_ragged_columns():
    with pytest.raises(ValueError, match="Invalid wide page columns"):
        chm._wide_column_records("mkt.oi_obs", {"instrument_id": ["T"], "ts": []}, "ts")


def test_wide_schema_dispatch_is_once_per_column_instead_of_once_per_cell(monkeypatch):
    comparisons = []

    class Kind(str):
        __hash__ = str.__hash__

        def __eq__(self, other):
            comparisons.append(None)
            return super().__eq__(other)

    kinds = dict(chm.MARKET_COLUMNS["mkt.oi_obs"], oi_native=Kind("numeric"))
    monkeypatch.setitem(chm.MARKET_COLUMNS, "mkt.oi_obs", kinds)
    rows = [
        dict(instrument_id="A:perp:T", storage_key=str(i), ts="2026-10-19T00:00:00Z", oi_native="3.000")
        for i in range(20)
    ]
    expected = [(r["instrument_id"], chm._record("mkt.oi_obs", r), r["storage_key"], T0) for r in rows]
    comparisons.clear()
    columns = {name: [row[name] for row in rows] for name in rows[0]}
    assert chm._wide_column_records("mkt.oi_obs", columns, "ts") == expected
    assert len(comparisons) <= 4


@pytest.mark.parametrize("trap", [False, True])
def test_wide_column_batches_keep_first_row_error_and_decimal_flags(monkeypatch, trap):
    monkeypatch.setitem(chm.MARKET_COLUMNS, "mkt.oi_obs", {"oi_native": "numeric", "ts": "timestamptz"})
    rows = [
        dict(instrument_id="A:perp:T", storage_key="first", ts="not-a-date", oi_native=1.1),
        dict(instrument_id="A:perp:T", storage_key="second", ts="2026-10-19T00:00:00Z", oi_native="bad"),
    ]
    columns = {name: [row[name] for row in rows] for name in rows[0]}
    with localcontext() as context:
        context.traps[InvalidOperation] = trap
        context.traps[FloatOperation] = False
        context.clear_flags()
        with pytest.raises(ValueError) as expected:
            chm._record("mkt.oi_obs", rows[0])
        flags = dict(context.flags)
        context.clear_flags()
        with pytest.raises(type(expected.value)) as actual:
            chm._wide_column_records("mkt.oi_obs", columns, "ts")
        assert str(actual.value) == str(expected.value)
        assert dict(context.flags) == flags
