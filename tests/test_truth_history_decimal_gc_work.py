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


def test_history_decimal_metadata_preserves_quantum_and_extreme_eligible_exponents():
    values = [Decimal("0E-100"), Decimal("-0E+100"), Decimal("9" * 48 + "E100"), "1e-100", 0.125, 42]
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 512
            context.clear_flags()
            for _ in range(2):
                metadata = {}
                prepared = truth._history_decimal_arrays((values,), metadata=metadata)
                assert prepared is not None
                assert [value.as_tuple() for value in prepared[0]] == [
                    Decimal(str(value)).as_tuple() for value in values
                ]
                assert metadata["low"] == -100 and metadata["high"] == 147
                assert context.prec == 512 and not any(context.flags.values())
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
