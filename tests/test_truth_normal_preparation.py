from decimal import Decimal, localcontext

import pytest

from tank.analysis import truth


def scalar_percentile(values, quantile):
    ordered = sorted(value for value in values if value is not None)
    if not ordered:
        return None
    position = (len(ordered) - 1) * quantile
    index = int(position)
    fraction = truth.decimal(position - index) if isinstance(ordered[index], Decimal) else position - index
    return ordered[index] + (ordered[min(index + 1, len(ordered) - 1)] - ordered[index]) * fraction


def signature(values):
    return [value.as_tuple() if isinstance(value, Decimal) else value.hex() for value in values]


@pytest.mark.parametrize("kind", [Decimal, float])
@pytest.mark.parametrize("precision", [32, 128])
def test_normal_reuses_one_ordered_builtin_population(monkeypatch, kind, precision):
    values = [kind(f"{index % 19}.{index % 7:02d}00") for index in range(1024)]
    values.extend([None, kind("-0.00"), kind("0.000")])
    original, calls = truth.percentile, []

    def counted(*args):
        calls.append(None)
        return original(*args)

    monkeypatch.setattr(truth, "percentile", counted)
    with localcontext() as context:
        context.prec = precision
        context.clear_flags()
        expected = [scalar_percentile(values, quantile) for quantile in (0.025, 0.975)]
        expected_context = context.prec, dict(context.flags)
    with localcontext() as context:
        context.prec = precision
        context.clear_flags()
        actual = truth.normal(values)
        assert signature(actual) == signature(expected)
        assert (context.prec, dict(context.flags)) == expected_context
    assert calls == []


def test_normal_retains_custom_quantile_evaluation_order():
    def inputs():
        values, calls = [Decimal("1.00"), Decimal("2.000"), Decimal("3")], []

        class Quantile:
            def __init__(self, position, value):
                self.position, self.value = position, value

            def __float__(self):
                calls.append(self.position)
                values.append(Decimal("999") if self.position == 0 else Decimal("-5"))
                return self.value

        return values, [Quantile(0, 0.025), Quantile(1, 0.975)], calls

    expected_values, expected_quantiles, expected_calls = inputs()
    with localcontext():
        expected = [scalar_percentile(expected_values, float(quantile)) for quantile in expected_quantiles]
    values, quantiles, calls = inputs()
    parameter = truth._PARAMETERS.set({"normal_range_quantiles": quantiles})
    try:
        with localcontext():
            actual = truth.normal(values)
    finally:
        truth._PARAMETERS.reset(parameter)
    assert signature(actual) == signature(expected)
    assert values == expected_values and calls == expected_calls == [0, 1]
