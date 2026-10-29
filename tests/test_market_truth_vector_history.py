from datetime import timedelta
from decimal import Decimal, Inexact, Rounded, localcontext

import pytest

from tank.analysis import truth
from tank.util import parse_time


def scalar_history(times, native, usd, window, tolerance):
    ordered = sorted(range(len(times)), key=times.__getitem__)
    hn, hu, native_ends, usd_ends = [], [], [], []
    for index, at in enumerate(times):
        target = at - window
        candidates = [
            (abs((times[old] - target).total_seconds()), times[old], old)
            for old in ordered
            if abs((times[old] - target).total_seconds()) <= tolerance
        ]
        if not candidates:
            continue
        old = min(candidates)[2]
        nn, oo = truth.decimal(native[index]), truth.decimal(native[old])
        uu, ou = truth.decimal(usd[index]), truth.decimal(usd[old])
        if nn is not None and oo:
            hn.append(float(nn / oo - 1))
            native_ends.append(at)
        if uu is not None and ou:
            hu.append(float(uu / ou - 1))
            usd_ends.append(at)
    return hn, hu, native_ends, usd_ends


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("rounding", ["ROUND_HALF_EVEN", "ROUND_DOWN"])
def test_vector_history_retains_endpoint_selection_decimal_values_and_flags(reverse, rounding):
    start = parse_time("2026-10-01T00:00:00Z")
    times = [start + timedelta(seconds=seconds) for seconds in (0, 60, 180, 240, 360)]
    native = ["3.000", "7.00", None, "11.0", "19"]
    usd = ["0", "13.00", "17.0", None, "29"]
    if reverse:
        times, native, usd = times[::-1], native[::-1], usd[::-1]
    with localcontext() as context:
        context.prec = 128
        context.rounding = rounding
        context.clear_flags()
        expected = scalar_history(times, native, usd, timedelta(seconds=90), 30.0)
        flags = dict(context.flags)
        context.clear_flags()
        actual = truth._oi_history_returns(times, native, usd, timedelta(seconds=90), 30.0)
        assert actual == expected
        assert context.flags == flags
        assert context.prec == 128


@pytest.mark.parametrize("signal", [Inexact, Rounded])
def test_vector_history_refuses_reordered_arithmetic_when_rounding_traps_are_enabled(signal):
    start = parse_time("2026-10-01T00:00:00Z")
    with localcontext() as context:
        context.prec = 128
        context.traps[signal] = True
        before = dict(context.flags)
        actual = truth._oi_history_returns(
            [start, start + timedelta(minutes=1)],
            [Decimal("3"), Decimal("7")],
            [None, None],
            timedelta(minutes=1),
            0,
        )
        assert actual is None
        assert context.flags == before


@pytest.mark.parametrize(
    "values",
    [
        ["NaN", "7"],
        ["Infinity", "7"],
        ["invalid", "7"],
        ["-3", "7"],
        ["1e101", "7"],
        ["1" * 49, "7"],
        [{"value": "3"}, "7"],
    ],
)
def test_vector_history_rejects_unsupported_values_without_context_changes(values):
    start = parse_time("2026-10-01T00:00:00Z")
    with localcontext() as context:
        context.prec = 128
        context.flags[Rounded] = True
        before = context.copy()
        assert truth._oi_history_returns(
            [start, start + timedelta(minutes=1)], values, [None, None], timedelta(minutes=1), 0
        ) is None
        assert context.flags == before.flags
        assert context.prec == before.prec


@pytest.mark.parametrize("rounding", ["ROUND_HALF_EVEN", "ROUND_DOWN"])
def test_composite_vector_preserves_cohorts_stored_usd_and_decimal_flags(monkeypatch, rounding):
    start = parse_time("2026-10-01T00:00:00Z")
    times = [start + timedelta(days=i) for i in range(10)]
    venues = [
        (
            times,
            [
                dict(oi_native=str(3 + i * 2) + ".000", oi_usd=str(13 + i * 3) + ".00")
                for i in range(10)
            ],
        ),
        (
            times[2:],
            [
                dict(
                    oi_native=None if i == 5 else str(i + 7),
                    oi_usd=str(i + 19),
                    oi_usd_source="DERIVED" if i % 2 else None,
                )
                for i in range(2, 10)
            ],
        ),
    ]
    with localcontext() as context:
        context.prec = 128
        context.rounding = rounding
        context.flags[Inexact] = True
        with monkeypatch.context() as scalar:
            scalar.setattr(truth, "_composite_history_vector", lambda *args: None)
            expected = truth._composite_history(venues, timedelta(days=1))
        flags = dict(context.flags)
        context.clear_flags()
        context.flags[Inexact] = True
        actual = truth._composite_history_vector(venues, timedelta(days=1))
        assert actual == expected
        assert context.flags == flags
        assert context.prec == 128


def test_vector_history_refuses_timestamp_overflow_and_excessive_span():
    start = parse_time("2026-10-01T00:00:00Z")
    with localcontext() as context:
        context.prec = 128
        assert truth._oi_history_returns(
            [start, start + timedelta(days=60000)], ["3", "7"], [None, None], timedelta(days=1), 0
        ) is None
        assert truth._oi_history_returns(
            [start.replace(year=1), start], ["3", "7"], [None, None], timedelta(days=1), 0
        ) is None


def test_composite_vector_keeps_arithmetic_flags_for_insufficient_measure_history(monkeypatch):
    start = parse_time("2026-10-01T00:00:00Z")
    times = [start + timedelta(days=i) for i in range(10)]
    rows = [dict(oi_native=None, oi_usd="3" if i == 0 else "7" if i == 1 else None) for i in range(10)]
    with localcontext() as context:
        context.prec = 128
        context.clear_flags()
        with monkeypatch.context() as scalar:
            scalar.setattr(truth, "_composite_history_vector", lambda *args: None)
            expected = truth._composite_history([(times, rows)], timedelta(days=1))
        flags = dict(context.flags)
        context.clear_flags()
        assert truth._composite_history_vector([(times, rows)], timedelta(days=1)) == expected == ([], [])
        assert context.flags == flags
        assert context.flags[Inexact]
