import sys
from collections.abc import ValuesView
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, localcontext

import pytest

from tank.analysis import truth


@pytest.mark.parametrize("precision", [9, 128])
@pytest.mark.parametrize("rounding", [ROUND_DOWN, ROUND_HALF_EVEN])
def test_native_context_signature_avoids_python_signal_view_iteration(precision, rounding):
    calls = []
    iteration = ValuesView.__iter__.__code__

    def profile(frame, event, arg):
        if event == "call" and frame.f_code is iteration:
            calls.append(None)

    with localcontext() as context:
        context.prec = precision
        context.rounding = rounding
        context.Emin = -321
        context.Emax = 654
        context.clamp = 1
        signals = list(context.traps)
        for index, signal in enumerate(signals):
            context.traps[signal] = index % 2 == 0
        expected = (
            context.prec,
            context.rounding,
            context.Emin,
            context.Emax,
            context.clamp,
            tuple(context.traps[signal] for signal in signals),
        )
        original = sys.getprofile()
        sys.setprofile(profile)
        try:
            actual = truth._history_context_key()
        finally:
            sys.setprofile(original)
        assert actual == expected
        assert not calls
        for index, signal in enumerate(signals):
            context.traps[signal] = not context.traps[signal]
            changed = truth._history_context_key()
            assert changed[:-1] == actual[:-1]
            assert changed[-1][index] is not actual[-1][index]
            context.traps[signal] = not context.traps[signal]
        for signal in signals:
            context.flags[signal] = not context.flags[signal]
        assert truth._history_context_key() == actual
