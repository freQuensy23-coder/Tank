from datetime import timedelta
from decimal import InvalidOperation, localcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import numpy as np
import pytest
from test_market_service import setup as setup
from test_market_truth_column_batches import ColumnClient, columns

from tank import clickhouse_market as chm
from tank.market.service import MarketService
from tank.util import parse_time

T0 = parse_time("2026-06-17T00:00:00Z")


def page(values):
    return {
        "open_us": list(range(len(values))),
        "open_time_prec": ["BUCKET(60s)"] * len(values),
        "recorded_unknown": [False] * len(values),
        "complete": [True] * len(values),
        "interval_s": [60] * len(values),
        "c": values,
        "volume_native": values,
        "_cursor_key": [str(i) for i in range(len(values))],
    }


async def projection(values, field, **kwargs):
    client = SimpleNamespace(database="mkt", columns=AsyncMock(return_value=page(values)))
    return await chm.dense_projection_page(
        client,
        instrument_id="spot",
        field=field,
        start=T0,
        end=T0 + timedelta(days=1),
        assembled_at=T0,
        **kwargs,
    )


@pytest.mark.parametrize("field", ["c", "volume_native"])
async def test_series_projection_keeps_fixed_text_without_decimal_objects(monkeypatch, field):
    values = ["0.000", "1.2300", "999999999999999999999999.000001", None, "1.2300"]
    original = chm.Decimal
    calls = []

    def convert(value):
        calls.append(value)
        return original(value)

    monkeypatch.setattr(chm, "Decimal", convert)
    actual = await projection(values, field, series=True)
    assert actual["value"] == values
    assert "decimal_values" not in actual
    assert calls == []
    if field == "volume_native":
        assert np.asarray(actual["nonnegative"]).tolist() == [True, True, True, False, True]


@pytest.mark.parametrize("field", ["c", "volume_native"])
@pytest.mark.parametrize(
    "values",
    [["-0.00", "-1e-900", "1e3", None], ["NaN", "Infinity", "-Infinity"], ["01.200", "+3.00", "1E-900"]],
)
async def test_series_projection_unqualified_page_keeps_legacy_decimal_values(field, values):
    expected = await projection(values, field)
    actual = await projection(values, field, series=True)
    assert actual["value"] == expected["value"]
    assert [value.as_tuple() if value is not None else None for value in actual["decimal_values"]] == [
        value.as_tuple() if value is not None else None for value in expected["decimal_values"]
    ]
    assert "nonnegative" not in actual


async def test_series_projection_malformed_text_keeps_decimal_exception():
    with localcontext() as context:
        context.traps[InvalidOperation] = True
        with pytest.raises(InvalidOperation):
            await projection(["1.00", "malformed"], "volume_native", series=True)
    with localcontext() as context:
        context.traps[InvalidOperation] = False
        context.clear_flags()
        actual = await projection(["1.00", "malformed"], "volume_native", series=True)
        assert actual["value"] == ["1.00", "NaN"]
        assert context.flags[InvalidOperation]
        assert actual["decimal_values"][1].is_nan()


@pytest.mark.parametrize("field", ["c", "volume_native"])
@pytest.mark.parametrize(
    "values", [[], [None, None], ["0.000001", "0.000000"], ["0.0000001"], ["0.0000000"], [1, "2.0"]]
)
async def test_series_projection_formatting_boundaries_and_builtin_fallback(field, values):
    expected = await projection(values, field)
    actual = await projection(values, field, series=True)
    assert actual["value"] == expected["value"]
    if values and values[0] in ("0.0000001", "0.0000000", 1):
        assert "decimal_values" in actual
    else:
        assert "decimal_values" not in actual


@pytest.mark.parametrize("kind", ["price", "volume"])
async def test_real_series_uses_text_preparation_and_preserves_selected_masks(setup, monkeypatch, kind):
    repo, settings, inst = setup
    rows = [
        dict(
            open_time=T0 + timedelta(minutes=i),
            recorded_at=T0,
            open_time_prec="BUCKET(60s)",
            complete=complete,
            interval_s=interval,
            c=value,
            volume_native=value,
        )
        for i, (value, complete, interval) in enumerate(
            [
                ("1.200", True, 60),
                ("2.300", False, 60),
                ("3.400", True, 30),
                (None, True, 60),
                ("0.000", True, 60),
            ]
        )
    ]
    client = ColumnClient(columns(rows))
    async with httpx.AsyncClient() as http:
        reference = MarketService(repo, settings, http=http)
        options = dict(kind=kind, coverage_count=5, fixed_window=True)
        expected = await reference._condition_kline_series(
            inst, T0, T0 + timedelta(minutes=5), T0 + timedelta(minutes=6), captured_rows=rows, **options
        )
        calls = []
        original = chm.Decimal

        def convert(value):
            calls.append(value)
            return original(value)

        monkeypatch.setattr(chm, "Decimal", convert)
        market = MarketService(SimpleNamespace(clickhouse=client), settings, http=http)
        market._condition_heads = AsyncMock(return_value=["head"])
        actual = await market._condition_kline_series(
            inst, T0, T0 + timedelta(minutes=5), T0 + timedelta(minutes=6), **options
        )
    assert actual == expected
    assert calls == []


@pytest.mark.parametrize("field", ["c", "volume_native"])
@pytest.mark.parametrize("scale", [7, 8, 100])
async def test_canonical_zero_exponents_keep_dense_text_without_decimal_population(field, scale):
    values = [f"0E-{scale}", "1.000", None]
    expected = await projection(values, field)
    actual = await projection(values, field, series=True)
    assert actual["value"] == expected["value"]
    assert "decimal_values" not in actual
    if field == "volume_native":
        assert actual["nonnegative"] == [True, True, False]
