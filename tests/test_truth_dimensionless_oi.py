import ctypes
import random
from datetime import UTC, datetime, timedelta
from decimal import (
    ROUND_05UP,
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    ROUND_UP,
    Decimal,
    FloatOperation,
    Inexact,
    Rounded,
    localcontext,
)

import numpy as np
import pytest

from tank.analysis import truth

ROUNDINGS = [
    ROUND_05UP,
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    ROUND_UP,
]


def signature(values):
    return [float(value).hex() for value in values]


def pairs():
    generator = random.Random(721)
    current, previous = [], []
    for index in range(257):
        exponent = (-100, -8, 0, 100)[index % 4]
        a, b = (generator.randrange(1, 2**53) for _ in range(2))
        current.append(Decimal((0, tuple(map(int, str(a))), exponent)))
        previous.append(Decimal((0, tuple(map(int, str(b))), exponent)))
    current.extend(map(Decimal, ("1.00", "-0.00", "1.00", "9007199254740993", "1e-100")))
    previous.extend(map(Decimal, ("1.00", "2.00", "3.00", "9007199254740992", "1e100")))
    current.extend(map(Decimal, ("9007199254740991", "9007199254740990")))
    previous.extend(map(Decimal, ("9007199254740990", "9007199254740991")))
    return np.asarray(current, dtype=object), np.asarray(previous, dtype=object)


@pytest.mark.parametrize("rounding", ROUNDINGS)
@pytest.mark.parametrize("sticky", [(False, False), (True, False), (False, True), (True, True)])
def test_dimensionless_batch_matches_decimal_bits_flags_and_context(rounding, sticky):
    current, previous = pairs()
    original = ([value.as_tuple() for value in current], [value.as_tuple() for value in previous])
    with localcontext() as context:
        context.prec = 128
        context.rounding = rounding
        context.clear_flags()
        context.flags[Inexact], context.flags[Rounded] = sticky
        context.traps[FloatOperation] = True
        expected = np.asarray(np.subtract(np.divide(current, previous), 1), dtype=np.float64)
        expected_flags = dict(context.flags)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.rounding = rounding
            context.clear_flags()
            context.flags[Inexact], context.flags[Rounded] = sticky
            context.traps[FloatOperation] = True
            result = truth._oi_dimensionless_changes(current, previous)
            assert result is not None
            assert signature(result) == signature(expected)
            assert context.prec == 128 and context.rounding == rounding
            assert dict(context.flags) == expected_flags
            assert context.traps[FloatOperation] is True
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
    assert original == ([value.as_tuple() for value in current], [value.as_tuple() for value in previous])


def test_oi_history_avoids_object_division_for_exact_dimensionless_pairs(monkeypatch):
    start = datetime(2026, 10, 28, tzinfo=UTC)
    times = [start + timedelta(minutes=index) for index in range(512)]
    native = [f"{100 + index % 19}.00" for index in range(512)]
    usd = [f"{300 + 3 * (index % 19)}.00" for index in range(512)]
    with localcontext() as context:
        context.prec = 128
        context.clear_flags()
        expected = [
            [float(Decimal(values[index]) / Decimal(values[index - 1]) - 1) for index in range(1, 512)]
            for values in (native, usd)
        ]
        expected_flags = dict(context.flags)
    original_divide, object_divisions = np.divide, []

    def observed(*args, **kwargs):
        if np.asarray(args[0]).dtype == object:
            object_divisions.append(len(args[0]))
        return original_divide(*args, **kwargs)

    monkeypatch.setattr(np, "divide", observed)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            result = truth._oi_history_returns(times, native, usd, timedelta(minutes=1), 0)
            assert result is not None
            assert [signature(result[index]) for index in range(2)] == list(map(signature, expected))
            assert result[2:] == (times[1:], times[1:])
            assert dict(context.flags) == expected_flags
        assert object_divisions == []
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


@pytest.mark.parametrize("bad", [Decimal("NaN"), Decimal("Infinity"), Decimal("-1"), Decimal("1e101")])
def test_dimensionless_batch_declines_late_unsupported_operands_without_flags(bad):
    current = np.asarray([Decimal("1.00"), bad], dtype=object)
    previous = np.asarray([Decimal("2.00"), Decimal("1.00")], dtype=object)
    with localcontext() as context:
        context.prec = 128
        context.clear_flags()
        assert truth._oi_dimensionless_changes(current, previous) is None
        assert not any(context.flags.values())


def test_dimensionless_batch_declines_non_nearest_binary_rounding():
    library = ctypes.CDLL(None)
    getter, setter = library.fegetround, library.fesetround
    getter.restype = ctypes.c_int
    setter.argtypes, setter.restype = [ctypes.c_int], ctypes.c_int
    initial = getter()
    try:
        for shift in range(32):
            if setter(1 << shift) == 0 and getter() != initial:
                break
        else:
            pytest.fail("No alternate binary rounding mode available")
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            values = np.asarray([Decimal("1.00")], dtype=object)
            assert truth._oi_dimensionless_changes(values, values) is None
            assert not any(context.flags.values())
    finally:
        assert setter(initial) == 0


@pytest.mark.parametrize("large", [False, True])
def test_composite_history_uses_exact_batches_and_original_large_ratio_fallback(monkeypatch, large):
    start = datetime(2026, 10, 20, tzinfo=UTC)
    times = [start + timedelta(hours=index) for index in range(193)]
    histories = []
    for venue in range(2):
        rows = []
        for index in range(193):
            amount = (10**40 if large else 100) + (venue + 1) * 10 + index % 19
            rows.append({"oi_native": f"{amount}.00", "oi_usd": f"{3 * amount}.00"})
        histories.append((times, rows))
    with localcontext() as context:
        context.prec = 128
        context.clear_flags()
        expected = []
        for field in ("oi_native", "oi_usd"):
            series = []
            for index in range(1, 193):
                previous = sum((Decimal(rows[index - 1][field]) for _, rows in histories), Decimal(0))
                current = sum((Decimal(rows[index][field]) for _, rows in histories), Decimal(0))
                series.append(float(current / previous - 1))
            expected.append(series)
        expected_flags = dict(context.flags)
    original_divide, object_divisions = np.divide, []

    def observed(*args, **kwargs):
        if np.asarray(args[0]).dtype == object:
            object_divisions.append(len(args[0]))
        return original_divide(*args, **kwargs)

    monkeypatch.setattr(np, "divide", observed)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            result = truth._composite_history_vector(histories, timedelta(hours=1))
            assert result is not None
            assert list(map(signature, result)) == list(map(signature, expected))
            assert dict(context.flags) == expected_flags
        assert object_divisions == ([192, 192] if large else [])
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
