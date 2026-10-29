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


def test_fixed_decimal_preparation_avoids_dense_character_array_passes(monkeypatch):
    column = ["000.12300", "0", ".12", "12.", "0000.000"] * 100 + [None]
    replace, calls = truth.np.strings.replace, []

    def counted(*args, **kwargs):
        calls.append(None)
        return replace(*args, **kwargs)

    monkeypatch.setattr(truth.np.strings, "replace", counted)
    with localcontext() as context:
        context.prec = 128
        context.clear_flags()
        expected = [Decimal(value) if value is not None else None for value in column]
        metadata = {}
        actual = truth._history_decimal_arrays((column,), metadata=metadata)
        assert representation(actual[0]) == representation(expected)
        assert metadata["low"] == -5 and metadata["high"] == 1
        assert not any(context.flags.values())
        assert calls == []


@pytest.mark.parametrize(
    ("tail", "admitted"),
    [
        ("9" * 48, True),
        ("9" * 49, False),
        ("0." + "0" * 99 + "1", True),
        ("0." + "0" * 100 + "1", False),
        ("0" * 160, True),
        ("0" * 161, False),
        (".", False),
        (".0", True),
        ("0.", True),
    ],
)
def test_fixed_decimal_metadata_admission_boundaries(tail, admitted):
    column = ["1.00"] * 256 + [tail]
    with localcontext() as context:
        context.clear_flags()
        result = truth._history_fixed_decimal_column(column)
        assert (result is not None) is admitted
        if admitted:
            expected = [Decimal(value) for value in column]
            assert representation(result[0]) == representation(expected)
            assert result[1] == min(0, *(value.as_tuple().exponent for value in expected))
            assert result[2] == max(0, *(value.adjusted() for value in expected))
        assert not any(context.flags.values())
