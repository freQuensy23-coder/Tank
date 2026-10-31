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


async def test_sampled_population_normalizes_columns_without_per_row_decimal_temporaries(monkeypatch):
    names, rows = population()
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
async def test_sampled_ragged_payload_retains_strict_row_shape_failure(missing):
    names, rows = population(2)
    payload = [[row.get(name) for name in names] for row in rows]
    if missing:
        payload[1].pop()
    else:
        payload[1].append("extra")
    client = Client(
        database="sampled-test", rows=AsyncMock(return_value=[{"fingerprint": "C" * 64, "records": payload}])
    )
    with pytest.raises(ValueError):
        await sampled(client, {})
