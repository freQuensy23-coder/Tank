import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import localcontext

from tank.analysis import truth
from tank.util import jsonable


def test_synchronized_history_materializes_only_requested_endpoints(monkeypatch):
    from test_truth_shared_oi_preparation import original_synchronized

    first = datetime(2026, 1, 1, tzinfo=UTC)
    sync = []
    for lane, venue in enumerate(("binance", "bybit"), 1):
        rows = [
            {
                "ts": first + timedelta(hours=i),
                "oi_native": f"{100 + lane * i}.00",
                "oi_usd": f"{1000 + 3 * lane * i}.0000",
            }
            for i in range(256)
        ]
        sync.append(({"venue": venue, "instrument_id": f"{venue}:perp:T"}, rows, "BASE"))
    before = deepcopy(sync)
    end = first + timedelta(hours=255)
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
