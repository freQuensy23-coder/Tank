from datetime import UTC, datetime, timedelta
from decimal import Rounded, localcontext

import pytest

from tank.analysis import truth


def observations(shape):
    first = datetime(2026, 10, 1, tzinfo=UTC)
    rows = [
        {"ts": first + timedelta(hours=i), "oi_native": str(100 + i), "oi_usd": str(300 + 3 * i)}
        for i in range(193)
    ]
    if shape == "duplicates":
        rows.insert(45, dict(rows[44], oi_native="155.00", oi_usd="465.000"))
    elif shape == "unsorted":
        rows[:100] = reversed(rows[:100])
    return rows, first + timedelta(hours=192)


def observe(rows, end, inputs, baseline):
    assessment = truth._ASSESSMENT_INPUTS.set(inputs)
    times = truth._TIMES.set(({}, inputs) if inputs is not None else None)
    cache = truth._BASELINE_CACHE.set(baseline)
    dependency = truth._BASELINE_DEPENDENCY.set("frozen-source")
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            context.flags[Rounded] = True
            result = truth._oi_stats(rows, end - timedelta(hours=2), end, history_identity="venue")
            return result, context.prec, dict(context.flags)
    finally:
        truth._BASELINE_DEPENDENCY.reset(dependency)
        truth._BASELINE_CACHE.reset(cache)
        truth._TIMES.reset(times)
        truth._ASSESSMENT_INPUTS.reset(assessment)


@pytest.mark.parametrize("shape", ["regular", "duplicates", "unsorted"])
def test_warm_oi_range_uses_temporal_index_without_rebuilding_row_dictionary(monkeypatch, shape):
    rows, end = observations(shape)
    expected = observe(rows, end, None, {})
    inputs = {}
    assessment = truth._ASSESSMENT_INPUTS.set(inputs)
    times = truth._TIMES.set(({}, inputs))
    cache = truth._BASELINE_CACHE.set({})
    dependency = truth._BASELINE_DEPENDENCY.set("frozen-source")
    original, calls = truth._time, []

    def counted(row):
        calls.append(row)
        return original(row)

    monkeypatch.setattr(truth, "_time", counted)
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            context.flags[Rounded] = True
            first = truth._oi_stats(rows, end - timedelta(hours=2), end, history_identity="venue")
            assert (first, context.prec, dict(context.flags)) == expected
            calls.clear()
            context.prec = 128
            context.clear_flags()
            context.flags[Rounded] = True
            second = truth._oi_stats(rows, end - timedelta(hours=2), end, history_identity="venue")
            assert (second, context.prec, dict(context.flags)) == expected
            assert len(calls) <= 8
    finally:
        truth._BASELINE_DEPENDENCY.reset(dependency)
        truth._BASELINE_CACHE.reset(cache)
        truth._TIMES.reset(times)
        truth._ASSESSMENT_INPUTS.reset(assessment)
