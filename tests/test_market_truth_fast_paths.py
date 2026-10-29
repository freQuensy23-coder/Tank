import builtins
from datetime import UTC, datetime
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Decimal, Inexact, Rounded, localcontext
from types import SimpleNamespace

import pytest

import tank.analysis.truth as truth
import tank.clickhouse_market as market
from tank.util import parse_time


@pytest.mark.parametrize("precision", [7, 128, 768])
@pytest.mark.parametrize("rounding", [ROUND_DOWN, ROUND_HALF_EVEN])
def test_single_decimal_keeps_context_sequence_without_redundant_reductions(
    monkeypatch, precision, rounding
):
    calls = []

    def maximum(*args, **kwargs):
        calls.append("max")
        return builtins.max(*args, **kwargs)

    def minimum(*args, **kwargs):
        calls.append("min")
        return builtins.min(*args, **kwargs)

    monkeypatch.setattr(truth, "max", maximum, raising=False)
    monkeypatch.setattr(truth, "min", minimum, raising=False)
    values = [Decimal("1." + "0" * 199 + "1"), Decimal("1.00")]
    with localcontext() as context:
        context.prec = precision
        context.rounding = rounding
        context.flags[Inexact] = context.flags[Rounded] = True
        before = context.copy()
        expected_precision = precision
        for value in values:
            span = value.adjusted() - int(value.as_tuple().exponent) + 1
            expected_precision = max(expected_precision, 2 * span + 32)
            assert truth.decimal(value) is value
            assert context.prec == expected_precision
            assert context.rounding == before.rounding
            assert context.flags == before.flags
            assert context.traps == before.traps
            assert (context.Emin, context.Emax, context.capitals, context.clamp) == (
                before.Emin,
                before.Emax,
                before.capitals,
                before.clamp,
            )
    assert calls == ["max", "max"]


def test_context_general_and_nonfinite_paths_keep_precision_and_flags():
    with localcontext() as context:
        context.prec = 7
        before = context.copy()
        truth.exact_context(None, "2", Decimal("NaN"), Decimal("Infinity"))
        assert context.prec == 7 and context.flags == before.flags
        large, small = Decimal("1E+200"), Decimal("1E-200")
        truth.exact_context(large, small)
        expected_span = large.adjusted() - int(small.as_tuple().exponent) + 1
        assert context.prec == 2 * expected_span + 32
        assert context.flags == before.flags and context.traps == before.traps


@pytest.mark.parametrize(
    "raw,expected_calls",
    [
        ("2026-10-28T14:06:00.123456+02:00", 2),
        ("2026-10-28 12:06:00.123456", 2),
        (datetime(2026, 10, 28, 12, 6, 0, 123456, tzinfo=UTC), 4),
    ],
)
async def test_projection_reuses_only_equal_timestamps_and_preserves_values(
    monkeypatch, raw, expected_calls
):
    available = "2026-10-28T12:07:00Z"
    source = {
        "opened": raw,
        "alias": raw,
        "available": available,
        "close": "1.00000000000000000001",
        "_cursor_time": raw,
        "_cursor_key": "venue:spot:T:minute",
    }
    queries, calls = [], []

    async def rows(query, params):
        queries.append(query)
        return [source]

    def counted(value):
        calls.append(value)
        return parse_time(value)

    monkeypatch.setattr(market, "parse_time", counted)
    result = await market.projection_page(
        SimpleNamespace(rows=rows),
        "mkt.klines_1m",
        {"opened": "open_time", "alias": "open_time", "available": "recorded_at", "close": "c"},
        "open_time",
        start=None,
        end=None,
        filters=None,
        instrument_ids=None,
        assembled_at=None,
        after=None,
        limit=2,
    )
    assert result == [
        (
            datetime(2026, 10, 28, 12, 6, 0, 123456, tzinfo=UTC),
            "venue:spot:T:minute",
            {
                "opened": "2026-10-28T12:06:00.123456Z",
                "alias": "2026-10-28T12:06:00.123456Z",
                "available": available,
                "close": "1.00000000000000000001",
            },
        )
    ]
    assert "ORDER BY `open_time`, storage_key LIMIT 2" in queries[0]
    assert len(calls) == expected_calls
