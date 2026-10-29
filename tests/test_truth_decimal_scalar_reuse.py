from decimal import Decimal, DecimalException, Inexact, InvalidOperation, Rounded, localcontext

import pytest

from tank.analysis import truth


def original_decimal(value):
    if isinstance(value, dict):
        value = value.get("value")
    if value is None:
        return None
    value = value if isinstance(value, Decimal) else Decimal(str(value))
    truth.exact_context(value)
    return value


def observe(function, value, precision, trap):
    with localcontext() as context:
        context.prec = precision
        context.Emin, context.Emax = -8, 8
        context.clear_flags()
        context.flags[Inexact] = True
        context.traps[InvalidOperation] = trap
        context.traps[Rounded] = trap
        try:
            result = function(value)
            result = None if result is None else result.as_tuple()
        except DecimalException as error:
            result = type(error)
        return result, context.prec, dict(context.flags), dict(context.traps)


@pytest.mark.parametrize(
    "value",
    [
        "3.000",
        "-0.000",
        "1e-900",
        "123456789012345678901234567890.00100",
        123,
        0.125,
        Decimal("1.200"),
        "NaN",
        "sNaN",
        "Infinity",
        "not-a-number",
        None,
    ],
)
@pytest.mark.parametrize("precision", [1, 28, 128])
@pytest.mark.parametrize("trap", [False, True])
def test_scalar_reuse_replays_precision_and_preserves_context_signals(value, precision, trap):
    expected = observe(original_decimal, value, precision, trap)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        assert observe(truth.decimal, value, precision, trap) == expected
        assert observe(truth.decimal, value, precision, trap) == expected
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_finite_scalar_is_reused_after_precision_reset_without_reusing_wrappers():
    raw = "123456789012345678901234567890.00100"
    source = {"value": raw}
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            first = truth.decimal(source)
            expected_precision = context.prec
            context.prec = 1
            second = truth.decimal(source)
            assert second is first
            assert second.as_tuple() == Decimal(raw).as_tuple()
            assert context.prec == expected_precision
            source["value"] = "2.00"
            assert truth.decimal(source).as_tuple() == Decimal("2.00").as_tuple()
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
    isolated = truth._ASSESSMENT_INPUTS.set({})
    try:
        assert truth.decimal(raw) is not first
        assert truth.decimal(raw).as_tuple() == first.as_tuple()
    finally:
        truth._ASSESSMENT_INPUTS.reset(isolated)


def test_custom_string_conversion_remains_observable_on_every_call():
    class ChangingText(str):
        calls = 0

        def __str__(self):
            self.calls += 1
            return str(self.calls)

    value = ChangingText("unused")
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        assert truth.decimal(value) == Decimal(1)
        assert truth.decimal(value) == Decimal(2)
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
