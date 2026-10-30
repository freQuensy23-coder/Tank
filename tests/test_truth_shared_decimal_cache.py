from decimal import Decimal, Rounded, localcontext

import pytest

from tank.analysis import truth


@pytest.mark.parametrize("length", [6, 64, 65, 160, 161])
def test_fixed_and_scalar_conversion_share_one_bounded_value_store(length):
    raw = "0" * (length - 1) + "1"
    inputs = {}
    token = truth._ASSESSMENT_INPUTS.set(inputs)
    try:
        with localcontext() as context:
            context.flags[Rounded] = True
            flags = dict(context.flags)
            for precision in (128, 160, 128):
                context.prec = precision
                result = truth._history_fixed_decimal_column([raw] * 256)
                shared = inputs["finite_text_decimals"]
                assert "fixed_text_decimals" not in inputs
                if length <= 160:
                    assert result is not None
                    assert all(value is shared[raw] for value in result[0])
                    assert shared[raw].as_tuple() == Decimal(raw).as_tuple()
                    assert inputs["finite_text_precisions"][raw] == 34
                else:
                    assert result is None and raw not in shared
                scalar = truth.decimal((" " + raw)[1:])
                assert scalar.as_tuple() == Decimal(raw).as_tuple()
                if length <= 64:
                    assert scalar is shared[raw]
                elif length <= 160:
                    assert scalar is not shared[raw]
                assert context.prec == precision and dict(context.flags) == flags
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_shared_values_still_require_fixed_lexical_metadata():
    inputs = {"finite_text_decimals": {"1e0": Decimal("1")}, "finite_text_precisions": {"1e0": 34}}
    token = truth._ASSESSMENT_INPUTS.set(inputs)
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            assert truth._history_fixed_decimal_column(["1e0"] * 256) is None
            raw = "001.2300"
            first = truth._history_fixed_decimal_column([raw, "0.0000"] * 128)
            inputs["fixed_text_metadata"].clear()
            second = truth._history_fixed_decimal_column([raw, "0.0000"] * 128)
            assert all(left is right for left, right in zip(first[0], second[0], strict=True))
            assert inputs["finite_text_precisions"][raw] == 42
            assert inputs["finite_text_precisions"]["0.0000"] == 34
            assert not any(context.flags.values())
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_shared_value_saturation_preserves_hits_without_another_value_dictionary():
    zero = Decimal(0)
    raw = "1.00"
    shared = {f"key{index}": zero for index in range(262143)}
    shared[raw] = Decimal(raw)
    inputs = {"finite_text_decimals": shared, "finite_text_precisions": {raw: 38}}
    token = truth._ASSESSMENT_INPUTS.set(inputs)
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            result = truth._history_fixed_decimal_column([raw, "9.00"] * 128)
            assert all(value is shared[raw] for value in result[0][::2])
            assert all(value.as_tuple() == Decimal("9.00").as_tuple() for value in result[0][1::2])
            assert len(shared) == 262144 and "9.00" not in shared
            assert len(inputs["fixed_text_metadata"]) <= 262144
            assert "fixed_text_decimals" not in inputs
            assert not any(context.flags.values())
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
