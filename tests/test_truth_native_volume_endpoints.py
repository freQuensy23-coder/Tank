from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext

import numpy as np
import pytest

from tank.analysis import truth


def reference(rows, end):
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    previous_close, previous_valid, label, total, endpoints = None, False, 0, Decimal(0), {}
    for row in sorted(rows, key=lambda value: value["ts"]):
        at = row["ts"]
        close = at + timedelta(seconds=row["interval_s"])
        if close > end:
            continue
        valid = row["complete"] is True and row["volume_native"] is not None
        if previous_close is None or not valid or not previous_valid or at != previous_close:
            label += 1
        if valid:
            endpoints[(at - epoch) // timedelta(microseconds=1)] = (total, label)
            total += Decimal(row["volume_native"])
            endpoints[(close - epoch) // timedelta(microseconds=1)] = (total, label)
        previous_close, previous_valid = close, valid
    ordered = sorted(endpoints)
    return ordered, [endpoints[at][0].as_tuple() for at in ordered], [endpoints[at][1] for at in ordered]


@pytest.mark.parametrize("shape", ["continuous", "gaps", "empty", "overlap"])
def test_native_volume_endpoint_selection_preserves_last_wins_without_sorting_ordered_data(monkeypatch, shape):
    base = datetime(2026, 10, 28, tzinfo=UTC)
    rows = [
        {
            "ts": base + timedelta(minutes=index),
            "interval_s": 60,
            "volume_native": f"{index % 13}.00",
            "complete": True,
        }
        for index in range(512)
    ]
    if shape == "gaps":
        rows[8]["complete"] = False
        rows[14]["volume_native"] = None
    elif shape == "empty":
        rows.clear()
    elif shape == "overlap":
        rows[0]["interval_s"] = 180
        rows[1]["ts"] = base
    end = base + timedelta(minutes=600)
    with localcontext() as context:
        context.prec = 128
        context.clear_flags()
        expected = reference(rows, end)
        expected_flags = dict(context.flags)
    original_unique, selections = np.unique, []

    def observed(*args, **kwargs):
        selections.append(len(args[0]))
        return original_unique(*args, **kwargs)

    monkeypatch.setattr(np, "unique", observed)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            result = truth._native_volume_arrays(rows, None, end)
            assert result is not None
            assert result[0].tolist() == expected[0]
            assert [value.as_tuple() for value in result[1]] == expected[1]
            assert result[2].tolist() == expected[2]
            assert result[3] == len(rows)
            assert dict(context.flags) == expected_flags
        assert len(selections) == (1 if shape == "overlap" else 0)
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
