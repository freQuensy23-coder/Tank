from datetime import timedelta
from decimal import Decimal, localcontext

import numpy as np

from tank.analysis import truth
from tank.util import parse_time


def test_history_presence_uses_identity_instead_of_decimal_equality(monkeypatch):
    start = parse_time("2026-10-19T00:00:00Z")
    times = [start + timedelta(minutes=i) for i in range(5)]
    native = ["1", "2", None, "4", "8"]
    usd = ["3", "6", None, "12", "24"]
    original = np.not_equal
    comparisons = []

    def observed(values, other):
        if other is None and any(type(value) is Decimal for value in values):
            comparisons.append(len(values))
        return original(values, other)

    monkeypatch.setattr(truth.np, "not_equal", observed)
    with localcontext() as context:
        context.prec = 128
        result = truth._oi_history_returns(times, native, usd, timedelta(minutes=1), 0)
    assert result == ([1.0, 1.0], [1.0, 1.0], [times[1], times[4]], [times[1], times[4]])
    assert comparisons == []
