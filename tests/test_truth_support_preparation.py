from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, Inexact, Rounded, localcontext

import pytest

from tank.analysis import truth
from tank.util import parse_time


def test_support_basis_is_prepared_once_across_windows_with_exact_results(monkeypatch):
    end = parse_time("2026-10-19T00:00:00Z")
    rows = [
        dict(
            instrument_id="A:perp:T",
            ts=end - timedelta(hours=i),
            mark_price=str(100 + i),
            index_price="103",
        )
        for i in range(6)
    ]
    rows.insert(2, dict(rows[1], mark_price="109"))
    packet = {"token_id": "T", "oi": rows}
    windows = [end - timedelta(hours=i) for i in (1, 2, 3)]
    config = truth._PARAMETERS.set({"history_min_days": 0})
    original = truth.decimal
    calls = []

    def decimal(value):
        if type(value) is str and value == "103":
            calls.append(value)
        return original(value)

    def evaluate(start):
        deriv = {"per_venue": [{"instrument_id": "A:perp:T", "venue": "A", "basis_bps": Decimal(1)}]}
        features = truth.support_features(packet, start, end, {}, deriv, {}, {})
        return features, deriv

    try:
        with localcontext() as context:
            context.prec = 128
            expected = [deepcopy(evaluate(start)) for start in windows]
            monkeypatch.setattr(truth, "decimal", decimal)
            token = truth._ASSESSMENT_INPUTS.set({})
            try:
                actual = [deepcopy(evaluate(start)) for start in windows]
            finally:
                truth._ASSESSMENT_INPUTS.reset(token)
        assert actual == expected
        assert len(calls) == sum(row["mark_price"] == "103" for row in rows) + len(rows)
    finally:
        truth._PARAMETERS.reset(config)


@pytest.mark.parametrize("preset", [False, True])
def test_basis_history_preserves_context_flags_on_miss_and_hit(preset):
    end = parse_time("2026-10-19T00:00:00Z")
    rows = [{"ts": end, "mark_price": "1", "index_price": "3"}]
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            context.flags[Rounded] = preset
            initial = dict(context.flags)
            expected = [(end, (truth.decimal("1") / truth.decimal("3") - 1) * 10000)]
            flags = dict(context.flags)
            context.clear_flags()
            context.flags.update(initial)
            actual = truth._basis_history(rows, end)
            assert actual == expected and dict(context.flags) == flags
            context.clear_flags()
            context.flags.update(initial)
            assert truth._basis_history(rows, end) is actual
            assert dict(context.flags) == flags and context.prec == 128
            context.traps[Inexact] = True
            with pytest.raises(Inexact):
                truth._basis_history(rows, end)
            context.traps[Inexact] = False
            context.prec = 160
            context.clear_flags()
            changed = truth._basis_history(rows, end)
            assert changed[0][1] != actual[0][1]
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_basis_history_does_not_memoize_precision_growth_or_custom_scalars():
    end = parse_time("2026-10-19T00:00:00Z")
    rows = [{"ts": end, "mark_price": "1" * 80, "index_price": "3"}]
    inputs = {}
    token = truth._ASSESSMENT_INPUTS.set(inputs)
    try:
        with localcontext() as context:
            context.prec = 128
            assert truth._basis_history(rows, end)
            assert context.prec > 128 and not inputs["basis_history"]
            assert truth._basis_history([dict(rows[0], mark_price={"value": "1"})], end) is None
            corrected = [dict(rows[0], mark_price="6")]
            assert truth._basis_history(corrected, end) == [(end, Decimal(10000))]
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
