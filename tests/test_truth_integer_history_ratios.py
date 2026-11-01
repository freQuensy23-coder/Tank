from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Decimal, DecimalException, Inexact, Rounded, localcontext
from random import Random

import numpy as np
import pytest

from tank.analysis import truth

PAIRS = [
    (0, 1),
    (1, 1),
    (2, 1),
    (1, 3),
    (4, 3),
    (1, 64),
    (1, 5**12),
    (2**53 + 1, 2**53),
    (2**54 + 1, 2**53),
    (2**54 + 3, 2**53),
    (2**63 - 1, 2**63 - 2),
    (2**63 - 2, 2**63 - 1),
    (1, 2**63 - 1),
    (2**63 - 1, 1),
]


def decimal_changes(units, previous, positions):
    with localcontext() as context:
        context.prec = 128
        context.rounding = ROUND_HALF_EVEN
        context.clear_flags()
        context.clear_traps()
        values = [float(Decimal(int(units[i])) / Decimal(int(units[previous[i]])) - 1) for i in positions]
        return values, bool(context.flags[Inexact]), dict(context.flags)


@pytest.mark.parametrize("current,old", PAIRS)
def test_integer_history_kernel_matches_decimal_bits_and_termination(current, old):
    units = np.asarray([old, current], dtype=np.int64)
    previous = np.asarray([0, 0], dtype=np.intp)
    positions = np.asarray([1], dtype=np.intp)
    expected, inexact, _ = decimal_changes(units, previous, positions)
    with localcontext() as context:
        context.clear_flags()
        before = dict(context.flags)
        result = truth._integer_returns(units, previous, positions)
        assert result is not None
        values, observed_inexact = result
        assert [float(value).hex() for value in values] == [value.hex() for value in expected]
        assert observed_inexact is inexact
        assert dict(context.flags) == before


def test_integer_history_kernel_matches_seeded_full_width_population():
    random = Random(882141)
    units = np.asarray([random.randrange(1, 2**63) for _ in range(2048)], dtype=np.int64)
    previous = np.arange(len(units), dtype=np.intp) ^ 1
    positions = np.arange(len(units), dtype=np.intp)
    expected, inexact, _ = decimal_changes(units, previous, positions)
    result = truth._integer_returns(units, previous, positions)
    assert result is not None
    values, observed_inexact = result
    matching = [float(value).hex() for value in values] == [value.hex() for value in expected]
    assert matching
    assert observed_inexact is inexact


@pytest.mark.parametrize(
    "units,previous,positions",
    [
        ([-1, 2], [0, 0], [1]),
        ([0, 2], [0, 0], [1]),
        ([1, 2], [-1, 0], [0]),
        ([1, 2], [0, 2], [1]),
        ([1, 2], [0, 0], [2]),
    ],
)
def test_integer_history_kernel_refuses_unsafe_coefficients_and_positions(units, previous, positions):
    assert (
        truth._integer_returns(
            np.asarray(units, dtype=np.int64),
            np.asarray(previous, dtype=np.intp),
            np.asarray(positions, dtype=np.intp),
        )
        is None
    )


def reference_changes(times, prepared, old, paired):
    returns, ends = [], []
    for values in prepared:
        present = np.fromiter((value is not None for value in values), dtype=bool, count=len(values))
        valid = paired & present & present[old]
        valid &= np.not_equal(values[old], 0)
        result = np.subtract(np.divide(values[valid], values[old[valid]]), 1)
        returns.append(np.asarray(result, dtype=np.float64).tolist())
        ends.append([times[i] for i in np.flatnonzero(valid)])
    return returns[0], returns[1], ends[0], ends[1]


def observed_history(
    function,
    monkeypatch,
    *,
    precision=128,
    rounding=ROUND_HALF_EVEN,
    mutate=False,
    trap=None,
):
    count = 512
    times = [datetime(2026, 10, 19, tzinfo=UTC) + timedelta(minutes=5 * i) for i in range(count)]
    native = [None if i % 29 == 0 else f"{1000000 + 7 * i}.00000000" for i in range(count)]
    usd = [None if i % 31 == 0 else f"{2000000 + 11 * i}.00000000" for i in range(count)]
    old = np.maximum(np.arange(count, dtype=np.intp) - 17, 0)
    paired = np.arange(count) >= 17
    marker = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context, monkeypatch.context() as patch:
            context.prec = 128
            context.rounding = ROUND_HALF_EVEN
            context.clear_traps()
            context.clear_flags()
            prepared = truth._history_decimal_arrays((native, usd), metadata={})
            assert prepared is not None
            if mutate:
                prepared[0][100] = Decimal("3.0000000000000000000000000000000000000001")
            context.prec = precision
            context.rounding = rounding
            context.flags[Rounded] = True
            if trap is not None:
                context.traps[trap] = True
            original = np.divide
            object_operands = []

            def divide(left, right, *args, **kwargs):
                if np.asarray(left).dtype == object:
                    object_operands.append(np.asarray(left).size)
                return original(left, right, *args, **kwargs)

            patch.setattr(np, "divide", divide)
            try:
                result = function(times, prepared, old, paired)
            except DecimalException as error:
                result = type(error).__name__
            return result, dict(context.flags), sum(object_operands)
    finally:
        truth._ASSESSMENT_INPUTS.reset(marker)


def test_history_uses_native_integer_ratios_without_object_decimal_division(monkeypatch):
    expected = observed_history(reference_changes, monkeypatch)
    actual = observed_history(truth._oi_history_changes, monkeypatch)
    matching = actual[:2] == expected[:2]
    assert matching
    assert expected[2] > 0
    assert actual[2] == 0


@pytest.mark.parametrize(
    "options",
    [
        {"precision": 9},
        {"rounding": ROUND_DOWN},
        {"mutate": True},
        {"trap": Inexact},
        {"trap": Rounded},
    ],
)
def test_history_preserves_scalar_fallback_context_and_changed_values(monkeypatch, options):
    expected = observed_history(reference_changes, monkeypatch, **options)
    actual = observed_history(truth._oi_history_changes, monkeypatch, **options)
    matching = actual[:2] == expected[:2]
    assert matching
    if options.get("mutate"):
        assert 0 < actual[2] <= expected[2]
    else:
        assert actual[2] == expected[2] > 0


def test_integer_history_uses_native_dispatch_entries():
    required = {"_validated_integer_units", "_rounded_integer_change", "_integer_return_values"}
    assert required <= truth.__pyx_capi__.keys()
