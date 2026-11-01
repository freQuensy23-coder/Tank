import gc
from decimal import localcontext

from tank.analysis import truth


def test_finite_text_values_do_not_retain_a_gc_container_per_cached_amount():
    inputs = {}
    token = truth._ASSESSMENT_INPUTS.set(inputs)
    try:
        with localcontext() as context:
            context.prec = 128
            amount = truth.decimal("123.0000")
            assert truth.decimal("123.0000") is amount
            assert not any(
                type(value) in (tuple, list, dict, set)
                for value in gc.get_referents(inputs["finite_text_decimals"])
            )
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
