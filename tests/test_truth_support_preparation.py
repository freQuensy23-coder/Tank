from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, localcontext

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
