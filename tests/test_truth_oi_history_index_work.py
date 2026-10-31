import sys
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
def test_warm_oi_range_uses_temporal_index_without_rebuilding_row_dictionary(shape):
    rows, end = observations(shape)
    expected = observe(rows, end, None, {})
    inputs = {}
    assessment = truth._ASSESSMENT_INPUTS.set(inputs)
    times = truth._TIMES.set(({}, inputs))
    references = []

    class Ranges(dict):
        probe = rows[0]

        def get(self, key, default=None):
            references.append(sys.getrefcount(self.probe))
            return super().get(key, default)

    ranges = Ranges()
    cache = truth._BASELINE_CACHE.set(ranges)
    dependency = truth._BASELINE_DEPENDENCY.set("frozen-source")
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            context.flags[Rounded] = True
            first = truth._oi_stats(rows, end - timedelta(hours=2), end, history_identity="venue")
            assert (first, context.prec, dict(context.flags)) == expected
            references.clear()
            retained = sys.getrefcount(ranges.probe)
            context.prec = 128
            context.clear_flags()
            context.flags[Rounded] = True
            second = truth._oi_stats(rows, end - timedelta(hours=2), end, history_identity="venue")
            assert (second, context.prec, dict(context.flags)) == expected
            assert references == [retained]
    finally:
        truth._BASELINE_DEPENDENCY.reset(dependency)
        truth._BASELINE_CACHE.reset(cache)
        truth._TIMES.reset(times)
        truth._ASSESSMENT_INPUTS.reset(assessment)


def test_custom_time_callback_keeps_source_history_failure_order(monkeypatch):
    rows, end = observations("regular")

    def run(inputs):
        calls = []

        def callback(row):
            calls.append(id(row))
            if len(calls) == 250:
                raise RuntimeError("custom history timestamp")
            return row["ts"]

        monkeypatch.setattr(truth, "_time", callback)
        with pytest.raises(RuntimeError, match="custom history timestamp"):
            observe(rows, end, inputs, {})
        return calls

    assert run({}) == run(None)
