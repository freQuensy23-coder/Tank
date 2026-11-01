import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import localcontext

import pytest

from tank.analysis import truth
from tank.util import jsonable


def _cohort(count):
    first = datetime(2026, 1, 1, tzinfo=UTC)
    sync = []
    for lane, venue in enumerate(("binance", "bybit"), 1):
        rows = [
            {
                "ts": first + timedelta(hours=i),
                "oi_native": f"{100 + lane * i}.00",
                "oi_usd": f"{1000 + 3 * lane * i}.0000",
            }
            for i in range(count)
        ]
        sync.append(({"venue": venue, "instrument_id": f"{venue}:perp:T"}, rows, "BASE"))
    return sync, first + timedelta(hours=count - 1)


def test_synchronized_history_materializes_only_requested_endpoints(monkeypatch):
    from test_truth_shared_oi_preparation import original_synchronized

    sync, end = _cohort(256)
    before = deepcopy(sync)
    widths = (1, 4, 24)

    def observe(function):
        variables = {
            "_PARAMETERS": {},
            "_ASSESSMENT_INPUTS": {},
            "_TIMES": {},
            "_BASELINE_CACHE": {},
            "_BASELINE_DEPENDENCY": None,
        }
        tokens = []
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            try:
                for name, value in variables.items():
                    variable = getattr(truth, name)
                    tokens.append((variable, variable.set(value)))
                results = [function(sync, end - timedelta(hours=width), end) for width in widths]
                return json.dumps(jsonable(results), sort_keys=True), dict(context.flags)
            finally:
                for variable, token in reversed(tokens):
                    variable.reset(token)

    expected = observe(original_synchronized)

    formatted = []
    original_iso = truth.iso

    def observed_iso(value):
        formatted.append(value)
        return original_iso(value)

    with monkeypatch.context() as patch:
        patch.setattr(truth, "iso", observed_iso)
        actual = observe(truth.synchronized_oi)
    assert actual == expected and sync == before
    assert len(formatted) <= 4 * len(widths)


@pytest.mark.parametrize(
    "case", ["unaligned", "duplicates", "missing_usd", "derived_usd", "missing_native", "short"]
)
def test_synchronized_endpoint_columns_preserve_scalar_oracle(case):
    from test_truth_shared_oi_preparation import original_synchronized

    sync, end = _cohort(64)
    rows = sync[1][1]
    if case == "unaligned":
        for row in rows:
            row["ts"] -= timedelta(seconds=30, microseconds=17)
    elif case == "duplicates":
        rows.insert(8, dict(rows[8], oi_native="1234567.8900"))
    elif case == "missing_usd":
        rows[5]["oi_usd"] = None
    elif case == "derived_usd":
        for row in rows:
            row.pop("oi_usd")
            row["multiplier"] = "0.0001"
            row["price"] = "2.500"
    elif case == "missing_native":
        rows[5]["oi_native"] = None
    elif case == "short":
        for _, values, _ in sync:
            del values[:-4]

    def observe(operation):
        slot = truth._ASSESSMENT_INPUTS.set({})
        try:
            with localcontext() as context:
                context.Emax, context.Emin = 512, -512
                result = []
                for precision in (128, 160, 128):
                    context.prec = precision
                    context.clear_flags()
                    for hours in (1, 4, 24):
                        value = operation(sync, end - timedelta(hours=hours), end)
                        result.append((json.dumps(jsonable(value), sort_keys=True), dict(context.flags)))
                return result
        finally:
            truth._ASSESSMENT_INPUTS.reset(slot)

    assert observe(truth.synchronized_oi) == observe(original_synchronized)
