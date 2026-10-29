from decimal import Decimal, InvalidOperation, localcontext

import pytest

from tank.analysis import truth


def representation(values):
    return [value.as_tuple() if value is not None else None for value in values]


def test_fixed_decimal_column_preserves_coefficients_without_retaining_per_scalar_cache_entries():
    column = [f"000.{index:06d}" for index in range(512)] + [None]
    inputs = {}
    token = truth._ASSESSMENT_INPUTS.set(inputs)
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            expected = [Decimal(value) if value is not None else None for value in column]
            metadata = {}
            actual = truth._history_decimal_arrays((column,), metadata=metadata)
            assert actual is not None
            assert representation(actual[0]) == representation(expected)
            numbers = [value for value in expected if value is not None]
            assert metadata["low"] == min(0, *(value.as_tuple().exponent for value in numbers))
            assert metadata["high"] == max(0, *(value.adjusted() for value in numbers))
            assert context.prec == 128 and not any(context.flags.values())
            assert not inputs.get("history_decimal_values")
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


@pytest.mark.parametrize("tail", ["-0.000", "1e-10", ".12", "12.", "+12", " 12.00 ", "１２.００"])
def test_fixed_decimal_column_keeps_scalar_fallback_representations(tail):
    column = ["1.00"] * 512 + [tail]
    with localcontext() as context:
        context.prec = 128
        context.clear_flags()
        expected = [Decimal(value) for value in column]
        actual = truth._history_decimal_arrays((column,))
        assert actual is not None
        assert representation(actual[0]) == representation(expected)
        assert not any(context.flags.values())


def test_fixed_decimal_column_rejects_nul_without_leaking_untrapped_invalid_flags():
    with localcontext() as context:
        context.prec = 128
        context.traps[InvalidOperation] = False
        context.clear_flags()
        assert truth._history_decimal_arrays((["1.00"] * 512 + ["2\x00"],)) is None
        assert not any(context.flags.values())
