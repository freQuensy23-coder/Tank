from decimal import Decimal, localcontext

from tank.analysis import truth


def test_assessment_caches_reuse_late_financial_values_beyond_the_initial_working_set():
    inputs = {}
    token = truth._ASSESSMENT_INPUTS.set(inputs)
    try:
        with localcontext() as context:
            context.prec = 128
            texts = [f"{index}.000" for index in range(65540)]
            first = None
            for text in texts:
                first = truth.decimal(text)
            context.prec = 9
            assert truth.decimal(texts[-1]) is first
            assert context.prec == 2 * len(first.as_tuple().digits) + 32
            context.prec = 128
            original = truth._history_fixed_decimal_column(texts)
            assert original is not None
            repeated = truth._history_fixed_decimal_column([texts[-1]] * 256)
            assert repeated is not None
            assert all(value is original[0][-1] for value in repeated[0])
            assert original[0][-1].as_tuple() == Decimal(texts[-1]).as_tuple()
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
