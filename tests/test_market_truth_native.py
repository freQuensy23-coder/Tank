from decimal import Decimal, localcontext
from importlib.machinery import ExtensionFileLoader

from tank.analysis import truth


def test_truth_executes_native_code_without_changing_decimal_representation_or_precision():
    marker = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            first = truth.decimal("3.000")
            context.prec = 9
            second = truth.decimal("3.000")
            assert first.as_tuple() == second.as_tuple() == Decimal("3.000").as_tuple()
            assert context.prec == 40
        assert isinstance(truth.__loader__, ExtensionFileLoader)
    finally:
        truth._ASSESSMENT_INPUTS.reset(marker)
