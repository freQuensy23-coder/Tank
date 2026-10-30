from array import array
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext, localcontext

import pytest

from tank.analysis import truth


@pytest.mark.parametrize("mixed_quantum", [False, True])
def test_fixed_volume_columns_avoid_decimal_arrays_without_changing_money(monkeypatch, mixed_quantum):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
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
