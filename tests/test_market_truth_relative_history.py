from bisect import bisect_right
from datetime import timedelta

import pytest

from tank.analysis import truth
from tank.util import parse_time


@pytest.mark.parametrize("benchmark", [False, True])
@pytest.mark.parametrize("length", [60, 90, 180])
def test_relative_vector_preserves_float_operations_endpoints_and_diagnostics(benchmark, length):
    start = parse_time("2026-10-01T00:10:00Z")
    origin = start - timedelta(minutes=10)
    times = [origin + timedelta(seconds=i) for i in (0, 60, 120, 240, 300, 360, 480)]
    prices = {t: 3.0 + i * 1.7 for i, t in enumerate(reversed(times))}
    btc = {t: 7.0 + i * 2.3 for i, t in enumerate(times) if i != 3}
    coefficient = 0.7312345678901234
    prepared = truth._relative_history_arrays(times, prices, btc, benchmark, coefficient)
    assert prepared is not None
    expected, durations, lags, ends = [], [], [], []
    for t in times:
        target = t - timedelta(seconds=length)
        index = bisect_right(times, target) - 1
        if index < 0:
            continue
        old = times[index]
        lag = (target - old).total_seconds()
        if lag >= 60 or not benchmark and (old not in btc or t not in btc):
            continue
        value = prices[t] / prices[old] - 1
        if not benchmark:
            value = value - coefficient * (btc[t] / btc[old] - 1)
        expected.append(value)
        durations.append((t - old).total_seconds())
        lags.append(lag)
        ends.append(t)
    assert truth._relative_history_returns(prepared, times, start, length, 60, benchmark, coefficient) == (
        expected,
        durations,
        lags,
        ends,
    )


@pytest.mark.parametrize("value", [0.0, -1.0, float("nan"), float("inf"), 1e-300, 1e300])
def test_relative_vector_refuses_unsafe_prices(value):
    start = parse_time("2026-10-01T00:00:00Z")
    times = [start, start + timedelta(minutes=1)]
    assert (
        truth._relative_history_arrays(times, dict(zip(times, [value, 7.0], strict=True)), {}, True, 1.0)
        is None
    )
