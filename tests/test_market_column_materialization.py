from copy import deepcopy

import pytest

from tank import clickhouse_market as chm


@pytest.mark.parametrize("collection", sorted(chm.MARKET_COLUMNS))
@pytest.mark.parametrize("shape", ["populated", "nulls", "empty"])
def test_column_materialization_preserves_legacy_fields_and_aliases(collection, shape):
    def value(kind):
        if shape == "nulls":
            return None
        if kind == "timestamptz":
            return "2026-10-28T12:00:00.123456Z"
        if kind == "numeric":
            return "-0.0000"
        if kind == "jsonb":
            return '{"proof":[false,null,0]}'
        if kind == "text[]":
            return ["proof", "retained"]
        if kind == "boolean":
            return False
        if kind in {"integer", "bigint"}:
            return 0
        return "retained"

    rows = []
    if shape != "empty":
        for _ in range(3):
            rows.append({name: value(kind) for name, kind in chm.MARKET_COLUMNS[collection].items()})
    columns = {name: [row[name] for row in rows] for name in chm.MARKET_COLUMNS[collection]}
    before = deepcopy(columns)
    expected = chm._records(collection, rows)
    actual = chm._column_records(collection, columns, len(rows))
    assert actual == expected
    assert chm._column_records(collection, columns, len(rows) + 1) == expected
    assert columns == before
    for result, reference in zip(actual, expected, strict=True):
        assert type(result) is dict
        assert list(result) == list(reference)
        for alias, target in chm.MARKET_ALIASES.get(collection, {}).items():
            assert result[alias] is result[target]


def test_column_materialization_keeps_custom_sequence_iteration_order():
    calls = []

    class Values(list):
        def __iter__(self):
            calls.append("iter")
            return super().__iter__()

        def __len__(self):
            calls.append("len")
            return super().__len__()

    columns = {"instrument_id": Values(["retained"] * 3)}
    expected = chm._records("mkt.oi_obs", [{"instrument_id": "retained"} for _ in range(3)])
    assert chm._column_records("mkt.oi_obs", columns, 3) == expected
    assert calls == ["iter"]


def test_column_materialization_keeps_strict_ragged_column_failure():
    with pytest.raises(ValueError, match="zip"):
        chm._column_records("mkt.oi_obs", {"instrument_id": ["retained"] * 3}, 2)
