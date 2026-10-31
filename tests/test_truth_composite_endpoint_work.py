from datetime import UTC, datetime, timedelta
from decimal import (
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    Decimal,
    DecimalException,
    FloatOperation,
    Inexact,
    getcontext,
    localcontext,
)

import numpy as np
import pytest

from tank.analysis import truth
from tank.analysis.truth import (
    _history_nearest,
    _history_present,
    _history_time_array,
    _oi_history_arrays,
    parameter,
)


def reference(venue_history, window):
    prepared = []
    low, high = 0, 0
    for _, rows in venue_history:
        columns = _oi_history_arrays(rows)
        if columns is None:
            return None
        arrays, metadata = columns
        prepared.extend(arrays)
        low, high = min(low, metadata["low"]), max(high, metadata["high"])
    if high - low + len(str(max(len(venue_history), 1))) + 1 > getcontext().prec:
        return None
    if any(type(t) is not datetime for times, _ in venue_history for t in times):
        return None
    try:
        history = sorted({t for times, _ in venue_history for t in times})
    except TypeError:
        return None
    temporal = _history_time_array(history, window)
    if temporal is None:
        return None
    offsets, width, origin = temporal
    minimum = float(parameter("history_min_days", 7)) * 86400
    if (history[-1] - history[0]).total_seconds() < minimum:
        return [], []
    totals = [np.full(len(history), Decimal(0), dtype=object) for _ in range(4)]
    covered = [np.zeros(len(history), dtype=bool) for _ in range(2)]
    for venue, (times, rows) in enumerate(venue_history):
        if not times:
            continue
        if len(times) != len(rows) or any(type(t) is not datetime for t in times):
            return None
        delta = [t - origin for t in times]
        at = np.asarray(
            [d.days * 86400000000 + d.seconds * 1000000 + d.microseconds for d in delta],
            dtype=np.int64,
        )
        if np.any(at[1:] < at[:-1]):
            return None
        gaps = np.divide(np.diff(at), 1000000)
        gaps = np.sort(gaps[gaps > 0])
        tolerance = max(
            float(parameter("oi_sync_tolerance_s", 60)), gaps[len(gaps) // 2] / 2 if len(gaps) else 0
        )
        old, old_valid = _history_nearest(at, offsets - width, tolerance)
        new, new_valid = _history_nearest(at, offsets, tolerance)
        paired = old_valid & new_valid & (old != new)
        for measure in range(2):
            values = prepared[2 * venue + measure]
            present = _history_present(values)
            valid = paired & present[new] & present[old]
            valid &= np.not_equal(values[old], 0)
            covered[measure] |= valid
            for endpoint, indices in enumerate((old, new)):
                total = totals[2 * measure + endpoint]
                total[valid] = np.add(total[valid], values[indices[valid]])
    result = []
    for measure, valid in enumerate(covered):
        indices = np.flatnonzero(valid)
        changes = np.subtract(np.divide(totals[2 * measure + 1][valid], totals[2 * measure][valid]), 1)
        values = np.asarray(changes, dtype=np.float64).tolist()
        if (
            not len(indices)
            or (history[indices[-1]] - history[indices[0]] + window).total_seconds() < minimum
        ):
            result.append([])
        else:
            result.append(values)
    return result[0], result[1]



def venues(count=2):
    result = []
    for venue in range(count):
        times, rows = [], []
        for index in range(480):
            times.append(datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=index, seconds=venue * 10))
            rows.append(
                {
                    "oi_native": "-0.0000" if index % 37 == 0 else f"{1000 + index * 7}.{'00' * (venue + 1)}",
                    "oi_usd": None if index % 11 == 0 else f"{2000 + index * 13}E-{venue + 1}",
                    "oi_usd_source": "DERIVED" if index % 19 == 0 else None,
                }
            )
        result.append((times, rows))
    return result


def evaluate(function, history, monkeypatch, rounding=ROUND_HALF_EVEN, parameters=None, precision=128, trap=False):
    add, divide, unique = np.add, np.divide, np.unique
    additions, endpoints, signatures = [], [], []

    def observed_add(left, right):
        additions.append(len(left))
        return add(left, right)

    def observed_divide(left, right):
        if left.dtype == object:
            endpoints.append(
                (tuple(value.as_tuple() for value in left), tuple(value.as_tuple() for value in right))
            )
        return divide(left, right)

    def observed_unique(values, **kwargs):
        signatures.append(values.dtype == np.uint64)
        return unique(values, **kwargs)

    with localcontext() as context, monkeypatch.context() as patch:
        context.prec, context.rounding = precision, rounding
        context.clear_flags()
        context.clear_traps()
        context.flags[FloatOperation] = True
        context.traps[Inexact] = trap
        parameter_token = truth._PARAMETERS.set(parameters)
        inputs_token = truth._ASSESSMENT_INPUTS.set({})
        patch.setattr(np, "add", observed_add)
        patch.setattr(np, "divide", observed_divide)
        patch.setattr(np, "unique", observed_unique)
        try:
            try:
                result = function(history, timedelta(hours=1))
            except DecimalException as error:
                result = ("decimal_error", type(error).__name__, str(error))
            else:
                if result is not None:
                    result = [[float(value).hex() for value in component] for component in result]
            return result, dict(context.flags), context.prec, endpoints, sum(additions), signatures
        finally:
            truth._ASSESSMENT_INPUTS.reset(inputs_token)
            truth._PARAMETERS.reset(parameter_token)


@pytest.mark.parametrize("rounding", [ROUND_HALF_EVEN, ROUND_FLOOR])
@pytest.mark.parametrize("count", [2, 4])
def test_composite_endpoint_signatures_reduce_explicit_np_add_operands(monkeypatch, rounding, count):
    history = venues(count)
    expected = evaluate(reference, history, monkeypatch, rounding)
    actual = evaluate(truth._composite_history_vector, history, monkeypatch, rounding)
    assert actual[:4] == expected[:4]
    assert actual[4] < expected[4]
    assert actual[5] == [True, True]


def test_composite_endpoint_fold_keeps_wide_cohorts_on_the_reference_path(monkeypatch):
    history = venues(5)
    expected = evaluate(reference, history, monkeypatch)
    actual = evaluate(truth._composite_history_vector, history, monkeypatch)
    assert actual == expected


@pytest.mark.parametrize("mutation", ["precision", "trap"])
def test_composite_endpoint_fold_keeps_parameter_callbacks_in_reference_order(monkeypatch, mutation):
    class Parameters(dict):
        def __init__(self):
            super().__init__(history_min_days=7)
            self.calls = []

        def get(self, name, default=None):
            self.calls.append(name)
            if name == "history_min_days":
                if mutation == "precision":
                    getcontext().prec = 8
                else:
                    getcontext().traps[Inexact] = True
            return super().get(name, default)

    history = venues()
    expected_parameters, actual_parameters = Parameters(), Parameters()
    expected = evaluate(reference, history, monkeypatch, parameters=expected_parameters)
    actual = evaluate(truth._composite_history_vector, history, monkeypatch, parameters=actual_parameters)
    assert actual == expected
    assert actual_parameters.calls == expected_parameters.calls


@pytest.mark.parametrize("size", [65535, 65536])
def test_composite_prepared_index_boundary_preserves_unsigned_high_bits(monkeypatch, size):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=40)
    history = [([start, end], [{}, {}]) for _ in range(3)]
    history.append(([start] * (size - 2) + [end - timedelta(hours=2), end], [{}] * size))

    def arrays(rows):
        return [np.full(len(rows), Decimal("1.00"), dtype=object) for _ in range(2)], {"low": -2, "high": 1}

    monkeypatch.setattr(truth, "_oi_history_arrays", arrays)
    monkeypatch.setitem(reference.__globals__, "_oi_history_arrays", arrays)
    expected = evaluate(reference, history, monkeypatch)
    actual = evaluate(truth._composite_history_vector, history, monkeypatch)
    assert actual[:4] == expected[:4]
    assert actual[5] == ([True, True] if size == 65535 else [])


@pytest.mark.parametrize("settings", [{"precision": 8}, {"trap": True}])
def test_composite_unsupported_decimal_context_retains_reference_fallback(monkeypatch, settings):
    history = venues()
    expected = evaluate(reference, history, monkeypatch, **settings)
    actual = evaluate(truth._composite_history_vector, history, monkeypatch, **settings)
    assert actual == expected
