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


def observe(rows, end, inputs, baseline, *, window=timedelta(hours=2), sticky=True, precision=128):
    assessment = truth._ASSESSMENT_INPUTS.set(inputs)
    times = truth._TIMES.set(({}, inputs) if inputs is not None else None)
    cache = truth._BASELINE_CACHE.set(baseline)
    dependency = truth._BASELINE_DEPENDENCY.set("frozen-source")
    try:
        with localcontext() as context:
            context.prec = precision
            context.clear_flags()
            context.flags[Rounded] = sticky
            result = truth._oi_stats(rows, end - window, end, history_identity="venue")
            return result, context.prec, dict(context.flags)
    finally:
        truth._BASELINE_DEPENDENCY.reset(dependency)
        truth._BASELINE_CACHE.reset(cache)
        truth._TIMES.reset(times)
        truth._ASSESSMENT_INPUTS.reset(assessment)


@pytest.mark.parametrize("shape", ["regular", "duplicates", "unsorted"])
def test_cold_windows_share_one_request_owned_history_time_axis(shape, monkeypatch):
    rows, end = observations(shape)
    for row in rows:
        row["ts"] += timedelta(microseconds=row["ts"].hour % 17)
    windows = [timedelta(minutes=minutes) for minutes in (60, 120, 240, 1440)]
    expected = [observe(rows, end, None, {}, window=window) for window in windows]
    partition = truth.np.partition
    spans = []

    def partitioned(values, middle):
        spans.append(len(values))
        return partition(values, middle)

    monkeypatch.setattr(truth.np, "partition", partitioned)
    inputs = {}
    actual = [observe(rows, end, inputs, {}, window=window) for window in windows]
    assert actual == expected
    assert all(result[0] is not None for result in actual)
    assert len(inputs.get("oi_history_axes", {})) == 1
    assert len(spans) == len(windows)
    assert all(span > 0 for span in spans)


@pytest.mark.parametrize("shape", ["regular", "duplicates", "unsorted"])
def test_cold_ordered_history_slices_avoid_row_identity_gathers(shape, monkeypatch):
    rows, end = observations(shape)
    windows = [timedelta(hours=hours) for hours in (1, 2, 4, 8)]
    expected = [observe(rows, end, None, {}, window=window) for window in windows]
    fromiter, argsort = truth.np.fromiter, truth.np.argsort
    gathers, sorts = [], []

    def gathered(values, dtype, *args, **kwargs):
        if dtype is truth.np.intp:
            gathers.append(kwargs.get("count"))
        return fromiter(values, dtype, *args, **kwargs)

    def sorted_offsets(values, *args, **kwargs):
        sorts.append(len(values))
        return argsort(values, *args, **kwargs)

    monkeypatch.setattr(truth.np, "fromiter", gathered)
    monkeypatch.setattr(truth.np, "argsort", sorted_offsets)
    inputs = {}
    actual = [observe(rows, end, inputs, {}, window=window) for window in windows]
    assert actual == expected
    assert all(value[0] is not None for value in actual)
    expected_work = 0 if shape == "regular" else len(windows)
    assert len(gathers) == len(sorts) == expected_work


@pytest.mark.parametrize("sticky", [False, True])
@pytest.mark.parametrize("precision", [9, 128])
@pytest.mark.parametrize("case", ["gaps", "zero", "missing_usd", "derived_usd", "invalid_outside"])
def test_contiguous_history_preserves_decimal_state_and_boundary_values(case, precision, sticky):
    rows, end = observations("regular")
    if case == "gaps":
        for index, row in enumerate(rows):
            row["ts"] += timedelta(microseconds=index % 13)
        del rows[12:140:17]
    elif case == "zero":
        for index in (3, 5, 7):
            rows[index]["oi_native"] = "-0.000" if index == 5 else "0.00"
    elif case == "missing_usd":
        for row in rows[::3]:
            row["oi_usd"] = None
    elif case == "derived_usd":
        for row in rows[::3]:
            row["oi_usd_source"] = "DERIVED"
    else:
        rows.append({"ts": end + timedelta(days=1), "oi_native": "invalid", "oi_usd": "invalid"})
    options = {"precision": precision, "sticky": sticky}
    inputs = {}
    for hours in (1, 2, 4, 8):
        window = timedelta(hours=hours, microseconds=1)
        expected = observe(rows, end, None, {}, window=window, **options)
        actual = observe(rows, end, inputs, {}, window=window, **options)
        assert actual == expected
        for name in ("oi_native_start", "oi_native_end", "oi_usd_start", "oi_usd_end"):
            value, reference = actual[0][name], expected[0][name]
            assert (value.as_tuple() if value is not None else None) == (
                reference.as_tuple() if reference is not None else None
            )


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
