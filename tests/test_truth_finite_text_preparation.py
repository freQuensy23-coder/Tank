from decimal import Decimal, InvalidOperation, localcontext

import pytest

from tank.analysis import truth


@pytest.mark.parametrize("text", ["3.000", "-0.000", "1e-900", " １２.００ ", "+12."])
def test_finite_plain_text_reuses_preparation_but_reapplies_its_exact_precision(monkeypatch, text):
    calls = []
    original = truth.exact_context

    def observed(*values):
        calls.append(values)
        return original(*values)

    monkeypatch.setattr(truth, "exact_context", observed)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 7
            context.clear_flags()
            expected = Decimal(text)
            required = 2 * (expected.adjusted() - expected.as_tuple().exponent + 1) + 32
            first = truth.decimal(text)
            assert first.as_tuple() == expected.as_tuple()
            assert context.prec == max(7, required)
            context.prec = 9
            second = truth.decimal(text)
            assert second.as_tuple() == expected.as_tuple()
            assert context.prec == max(9, required)
            assert not any(context.flags.values())
            assert len(calls) == 1
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_scalar_preparation_does_not_cache_invalid_text_or_opaque_conversion():
    class Mutable:
        def __init__(self):
            self.value = "1"

        def __str__(self):
            return self.value

    value = Mutable()
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.traps[InvalidOperation] = False
            for _ in range(2):
                context.clear_flags()
                assert truth.decimal("invalid").is_nan()
                assert context.flags[InvalidOperation]
            context.traps[InvalidOperation] = True
            with pytest.raises(InvalidOperation):
                truth.decimal("invalid")
            assert truth.decimal(value) == Decimal(1)
            value.value = "2"
            assert truth.decimal(value) == Decimal(2)
            wrapped = {"value": "1.00"}
            assert truth.decimal(wrapped).as_tuple() == Decimal("1.00").as_tuple()
            wrapped["value"] = "2.000"
            assert truth.decimal(wrapped).as_tuple() == Decimal("2.000").as_tuple()
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
