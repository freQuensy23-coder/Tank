import gc
from decimal import ROUND_UP, localcontext

from tank.analysis import truth


def test_context_signature_avoids_per_signal_container_allocation():
    with localcontext() as context:
        key = truth._history_context_key()
        assert len([part for part in gc.get_referents(key[-1]) if type(part) is tuple]) == 0
        for signal in context.traps:
            original = context.traps[signal]
            context.traps[signal] = not original
            assert truth._history_context_key() != key
            context.traps[signal] = original
            assert truth._history_context_key() == key
        for signal in context.flags:
            context.flags[signal] = not context.flags[signal]
        assert truth._history_context_key() == key
        for field, value in (
            ("prec", context.prec + 1),
            ("rounding", ROUND_UP),
            ("Emin", context.Emin - 1),
            ("Emax", context.Emax + 1),
            ("clamp", 1 - context.clamp),
        ):
            original = getattr(context, field)
            if value == original:
                continue
            setattr(context, field, value)
            assert truth._history_context_key() != key
            setattr(context, field, original)
            assert truth._history_context_key() == key
