import random
from datetime import UTC, datetime, timedelta
from decimal import FloatOperation, Inexact, Rounded, localcontext
from math import log
from statistics import stdev

import pytest

from tank.analysis import truth


def corpus(kind):
    generator = random.Random(512)
    if kind == "random":
        return [generator.uniform(-0.05, 0.05) for _ in range(96)]
    if kind == "cancellation":
        return [1.0 + (index % 3) * 2**-52 for index in range(96)]
    if kind == "subnormal":
        return [(index % 7 - 3) * float.fromhex("0x0.0000000000001p-1022") for index in range(96)]
    if kind == "large":
        return [float.fromhex("0x1.ffffffffffff0p+900") + (index % 3) * 2.0**850 for index in range(96)]
    if kind == "overflow":
        return [(-1 if index % 2 else 1) * float.fromhex("0x1.fffffffffffffp+1023") for index in range(96)]
    return [-0.0 if index % 2 else 0.0 for index in range(96)]


@pytest.mark.parametrize("kind", ["random", "cancellation", "subnormal", "large", "overflow", "zero"])
@pytest.mark.parametrize("gaps", [False, True])
def test_exact_rolling_rv_matches_independent_stdlib_bits_errors_and_decimal_flags(kind, gaps):
    base = datetime(2026, 10, 20, tzinfo=UTC)
    ends = [base + timedelta(hours=index) for index in range(96)]
    returns = {
        at: value
        for index, (at, value) in enumerate(zip(ends, corpus(kind), strict=True))
        if not gaps or index % 11 != 0
    }
    expected_times, expected_values, expected_error = [], [], None
    try:
        for end in ends:
            selected = [
                value for at, value in sorted(returns.items()) if end - timedelta(hours=24) < at <= end
            ]
            if len(selected) == 24:
                expected_times.append(end)
                expected_values.append(stdev(selected).hex())
    except OverflowError as error:
        expected_error = type(error)
    with localcontext() as context:
        context.prec = 9
        context.traps[Inexact] = context.traps[Rounded] = context.traps[FloatOperation] = True
        context.flags[Inexact] = True
        before = dict(context.flags)
        if expected_error is not None:
            with pytest.raises(expected_error):
                truth._rv24_history(returns, ends)
        else:
            result = truth._rv24_history(returns, ends)
            assert result is not None
            assert result[0] == expected_times
            assert [value.hex() for value in result[1]] == expected_values
        assert context.prec == 9 and dict(context.flags) == before


def test_reference_rv_avoids_repeated_fractional_standard_deviations(monkeypatch):
    base = datetime(2026, 10, 20, tzinfo=UTC)
    prices = {base + timedelta(hours=index): 100.0 + (index % 17) / 8 for index in range(96)}
    returns = {
        at: log(value / prices[at - timedelta(hours=1)])
        for at, value in prices.items()
        if at - timedelta(hours=1) in prices
    }
    expected = []
    for end in prices:
        values = [value for at, value in sorted(returns.items()) if end - timedelta(hours=24) < at <= end]
        if len(values) == 24:
            expected.append((end, stdev(values).hex()))
    observed = []

    def counted(values):
        observed.append(len(values))
        return stdev(values)

    monkeypatch.setattr(truth, "stdev", counted)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        result = truth._reference_rv_series(prices)
        assert result is not None
        assert list(zip(result[1], [value.hex() for value in result[2]], strict=True)) == expected
        assert observed == []
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
