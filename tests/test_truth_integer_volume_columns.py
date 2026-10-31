from array import array
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext, localcontext

import pytest

from tank.analysis import truth


@pytest.mark.parametrize("mixed_quantum", [False, True, "large"])
def test_fixed_volume_columns_avoid_decimal_arrays_without_changing_money(monkeypatch, mixed_quantum):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    if mixed_quantum == "large":
        raw = [str(10**35 + i) + ".0000" for i in range(128)]
        width, end_index = 20, 120
    else:
        raw = ["1.00", "2", "3.0000", "4.0"] * 32 if mixed_quantum else [f"{i + 1}.0000" for i in range(128)]
        width, end_index = (1, 3) if mixed_quantum else (20, 120)
    begin_index = end_index - width
    projection = {
        "values": raw,
        "times_us": array("q", [int(start.timestamp()) * 1000000 + i * 60000000 for i in range(len(raw))]),
        "complete": [True] * len(raw),
        "interval_s": 60,
        "dependency_fingerprint": "fixed-volume-test",
    }
    calls = []
    original = truth._history_decimal_arrays

    def observe(columns, **kwargs):
        calls.append(tuple(len(column) for column in columns))
        return original(columns, **kwargs)

    monkeypatch.setattr(truth, "_history_decimal_arrays", observe)
    inputs = truth._ASSESSMENT_INPUTS.set({})
    parameters = truth._PARAMETERS.set({"history_min_days": 0})
    try:
        with localcontext() as context:
            context.prec = 128
            prefix = [Decimal(0)]
            for value in raw[:end_index]:
                prefix.append(prefix[-1] + Decimal(value))
            expected = (
                prefix[end_index] - prefix[begin_index],
                prefix[begin_index] - prefix[begin_index - width],
                [prefix[i] - prefix[i - width] for i in range(width, begin_index)],
            )
            expected_range = truth.normal(expected[2])
            flags = dict(getcontext().flags)
            index = truth._native_volume_arrays([], projection, start + timedelta(minutes=end_index), "test")
            assert index is not None
            actual = truth._native_volume_windows(
                [index], start + timedelta(minutes=begin_index), start + timedelta(minutes=end_index)
            )
            assert actual is not None
            assert [value.as_tuple() for value in actual[:2]] == [value.as_tuple() for value in expected[:2]]
            assert [value.as_tuple() for value in actual[2]] == [value.as_tuple() for value in expected[2]]
            assert [value.as_tuple() for value in truth.normal(actual[2])] == [
                value.as_tuple() for value in expected_range
            ]
            assert dict(getcontext().flags) == flags
            assert not calls
    finally:
        truth._PARAMETERS.reset(parameters)
        truth._ASSESSMENT_INPUTS.reset(inputs)


@pytest.mark.parametrize("shape", ["continuous", "gaps", "duplicate", "mixed", "floor"])
def test_integer_and_legacy_volume_columns_match_quantum_and_flags(shape):
    import numpy as np

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    raw = [["1.00", "2", "0.0000", "4.0"] * 32, ["0.1", "0", "5.000", "3.00"] * 32]
    times = [int(base.timestamp()) * 1000000 + i * 60000000 for i in range(128)]
    complete = [True] * 128
    if shape == "gaps":
        complete[4] = False
        raw[0][6] = None
    if shape == "duplicate":
        times[10] = times[9]
    inputs = truth._ASSESSMENT_INPUTS.set({})
    parameters = truth._PARAMETERS.set({"history_min_days": 0})
    cache = truth._BASELINE_CACHE.set(None)
    try:
        with localcontext() as context:
            context.prec = 128
            if shape == "floor":
                context.rounding = "ROUND_FLOOR"
            end = base + timedelta(minutes=120)

            def indexes(legacy):
                result = []
                for venue, values in enumerate(raw):
                    decimal_values = legacy or (shape == "mixed" and venue == 1)
                    projection = {
                        "times_us": array("q", times),
                        "values": [Decimal(v) if v is not None else None for v in values]
                        if decimal_values
                        else list(values),
                        "complete": complete,
                        "interval_s": 60,
                    }
                    index = truth._native_volume_arrays([], projection, end, str(venue))
                    assert index is not None
                    result.append(index)
                return result

            def fingerprint(result):
                if result is None:
                    return None
                current, previous, history = result
                normal = truth.normal(history)
                return (
                    current.as_tuple() if current is not None else None,
                    previous.as_tuple() if previous is not None else None,
                    [value.as_tuple() for value in history],
                    [value.as_tuple() for value in normal] if normal is not None else None,
                )

            legacy, candidate = indexes(True), indexes(False)
            context.clear_flags()
            expected = fingerprint(truth._native_volume_windows(legacy, end - timedelta(minutes=20), end))
            flags = dict(context.flags)
            context.clear_flags()
            actual = fingerprint(truth._native_volume_windows(candidate, end - timedelta(minutes=20), end))
            assert actual == expected
            assert dict(context.flags) == flags
            assert all(type(index[1]) is np.ndarray for index in legacy)
    finally:
        truth._BASELINE_CACHE.reset(cache)
        truth._PARAMETERS.reset(parameters)
        truth._ASSESSMENT_INPUTS.reset(inputs)
