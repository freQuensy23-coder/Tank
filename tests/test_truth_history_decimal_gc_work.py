import gc
from decimal import Decimal, Rounded, localcontext

from tank.analysis import truth


def test_history_decimal_preparation_reuses_values_without_per_amount_containers():
    inputs = {}
    token = truth._ASSESSMENT_INPUTS.set(inputs)
    values = [Decimal(f"{index}.000000") for index in range(256)]
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            context.flags[Rounded] = True
            flags = dict(context.flags)
            metadata = {}
            first = truth._history_decimal_arrays((values,), metadata=metadata)
            second = truth._history_decimal_arrays((values,))
            assert first is not None and second is not None
            assert all(actual is expected for actual, expected in zip(first[0], values, strict=True))
            assert all(actual is expected for actual, expected in zip(second[0], values, strict=True))
            assert metadata["low"] == -6 and metadata["high"] == 2
            assert context.prec == 128 and dict(context.flags) == flags
            assert not any(
                type(value) in (tuple, list, dict, set)
                for name, cache in inputs.items()
                if name.startswith("history_decimal")
                for value in gc.get_referents(cache)
            )
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
