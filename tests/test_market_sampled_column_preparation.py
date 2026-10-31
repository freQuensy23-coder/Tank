from datetime import timedelta
from decimal import Decimal, localcontext
from unittest.mock import AsyncMock

import pytest

from tank import clickhouse_market as chm
from tank.market_schema import MARKET_COLUMNS
from tank.util import parse_time

T0 = parse_time("2026-06-17T00:00:00Z")
IID = "binance:perp:ABCUSD"
COLLECTION = "mkt.oi_obs"


class Client:
    def __init__(self, *, database, rows):
        self.database, self.rows = database, rows


def population(count=64):
    names = [*MARKET_COLUMNS[COLLECTION], "storage_key", "_version"]
    rows = [
        {
            "instrument_id": IID,
            "token_id": "test:ABC",
            "metric_key": "BASE",
            "ts": (T0 + timedelta(minutes=i)).isoformat(),
            "ts_prec": "EXACT",
            "recorded_at": T0.isoformat(),
            "oi_native": "1.000",
            "oi_usd": "10.000",
            "storage_key": f"key-{i:04d}",
            "_version": i,
        }
        for i in range(count)
    ]
    return names, rows


async def sampled(client, cache):
    return await chm.sampled_market_history(
        client, COLLECTION, "ts", ("metric_key",), [IID], T0, T0 + timedelta(days=1), 300, cache=cache
    )


@pytest.mark.parametrize("count", [64, 1001])
async def test_sampled_population_normalizes_columns_without_per_row_decimal_temporaries(monkeypatch, count):
    names, rows = population(count)
    expected = chm._records(COLLECTION, rows)
    payload = [[row.get(name) for name in names] for row in rows]
    client = Client(
        database="sampled-test",
        rows=AsyncMock(
            side_effect=[
                [{"fingerprint": "A" * 64, "records": payload}],
                [{"fingerprint": "A" * 64, "records": []}],
            ]
        ),
    )
    calls = []

    def decimal(value):
        calls.append(value)
        return Decimal(value)

    monkeypatch.setattr(chm, "Decimal", decimal)
    cache = {}
    actual = await sampled(client, cache)
    assert actual == expected
    assert not calls
    actual[0]["oi_native"] = "changed"
    again = await sampled(client, cache)
    assert again == expected
    assert client.rows.await_count == 2
    assert not calls


@pytest.mark.parametrize("capitals", [0, 1])
@pytest.mark.parametrize("value", ["0E-8", "-0.00", "1e-900", "bad", 0.1])
async def test_sampled_column_preparation_preserves_legacy_context_and_errors(capitals, value):
    names, rows = population(2)
    rows[0]["oi_native"] = value
    if value == "bad":
        rows[1]["ts"] = "bad time after earlier numeric error"
    payload = [[row.get(name) for name in names] for row in rows]
    client = Client(
        database="sampled-test", rows=AsyncMock(return_value=[{"fingerprint": "B" * 64, "records": payload}])
    )
    with localcontext() as context:
        context.capitals = capitals
        context.clear_flags()
        try:
            expected = chm._records(COLLECTION, rows)
            expected_error = None
        except Exception as error:
            expected, expected_error = None, type(error)
        expected_flags = dict(context.flags)
    with localcontext() as context:
        context.capitals = capitals
        context.clear_flags()
        if expected_error is None:
            assert await sampled(client, {}) == expected
        else:
            with pytest.raises(expected_error):
                await sampled(client, {})
        assert dict(context.flags) == expected_flags


@pytest.mark.parametrize("missing", [True, False])
@pytest.mark.parametrize("uniform", [True, False])
async def test_sampled_ragged_payload_retains_strict_row_shape_failure(missing, uniform):
    names, rows = population(2)
    rows[0]["oi_native"] = "bad numeric before invalid shape"
    payload = [[row.get(name) for name in names] for row in rows]
    for row in payload if uniform else payload[1:]:
        if missing:
            row.pop()
        else:
            row.append("extra")
    client = Client(
        database="sampled-test", rows=AsyncMock(return_value=[{"fingerprint": "C" * 64, "records": payload}])
    )
    with localcontext() as context:
        context.clear_flags()
        with pytest.raises(ValueError):
            await sampled(client, {})
        assert not any(context.flags.values())


async def test_overridden_numeric_constructor_retains_row_major_callback_order(monkeypatch):
    names, rows = population(2)
    rows[0]["oi_native"], rows[0]["oi_usd"] = "1e+3", "2e+3"
    rows[1]["oi_native"], rows[1]["oi_usd"] = "3e+3", "4e+3"
    payload = [[row.get(name) for name in names] for row in rows]
    calls = []

    def decimal(value):
        calls.append(value)
        if value == "2e+3":
            raise ValueError("constructor callback")
        return Decimal(value)

    monkeypatch.setattr(chm, "Decimal", decimal)
    with pytest.raises(ValueError, match="constructor callback"):
        chm._records(COLLECTION, rows)
    expected = list(calls)
    calls.clear()
    client = Client(
        database="sampled-test", rows=AsyncMock(return_value=[{"fingerprint": "D" * 64, "records": payload}])
    )
    with pytest.raises(ValueError, match="constructor callback"):
        await sampled(client, {})
    assert calls == expected


async def test_sampled_json_population_and_aliases_remain_detached():
    collection, field = "mkt.flow_bars_1m", "minute_start"
    names = [*MARKET_COLUMNS[collection], "storage_key", "_version"]
    row = {
        "instrument_id": IID,
        "minute_start": T0.isoformat(),
        "size_buckets": '{"values": [1, 2]}',
        "storage_key": "flow-key",
        "_version": 1,
    }
    expected = chm._records(collection, [row])
    payload = [[row.get(name) for name in names]]
    client = Client(
        database="sampled-test",
        rows=AsyncMock(
            side_effect=[
                [{"fingerprint": "E" * 64, "records": payload}],
                [{"fingerprint": "E" * 64, "records": []}],
            ]
        ),
    )
    cache = {}
    first = await chm.sampled_market_history(
        client, collection, field, (), [IID], T0, T0 + timedelta(days=1), 300, cache=cache
    )
    assert first == expected
    first[0]["size_buckets"]["values"].append(3)
    again = await chm.sampled_market_history(
        client, collection, field, (), [IID], T0, T0 + timedelta(days=1), 300, cache=cache
    )
    assert again == expected


async def test_sampled_normalization_does_not_require_wide_metadata_time():
    names, rows = population(1)
    rows[0]["ts"] = None
    expected = chm._records(COLLECTION, rows)
    client = Client(
        database="sampled-test",
        rows=AsyncMock(
            return_value=[{"fingerprint": "F" * 64, "records": [[rows[0].get(name) for name in names]]}]
        ),
    )
    assert await sampled(client, {}) == expected
