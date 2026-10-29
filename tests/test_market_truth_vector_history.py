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
