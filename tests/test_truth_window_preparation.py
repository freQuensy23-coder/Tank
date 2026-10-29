from datetime import timedelta
from decimal import localcontext

from test_truth_shared_oi_preparation import cohort

from tank.analysis import truth
from tank.util import iso, parse_time


def test_synchronized_history_reuses_guarded_full_cohort_keys_across_assessments(monkeypatch):
    sync, end = cohort()
    start = end - timedelta(hours=12)
    original = truth._stored_usd
    converted = []

    def observed(row):
        converted.append(row)
        return original(row)

    monkeypatch.setattr(truth, "_stored_usd", observed)
    cache_token = truth._BASELINE_CACHE.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            token = truth._ASSESSMENT_INPUTS.set({})
            try:
                first_result = truth.synchronized_oi(sync, start, end)
            finally:
                truth._ASSESSMENT_INPUTS.reset(token)
            assert first_result is not None and first_result["oi_native_chg_pct"] is not None
            converted.clear()
            context.prec = 128
            token = truth._ASSESSMENT_INPUTS.set({})
            try:
                assert truth.synchronized_oi(sync, start, end) == first_result
                assert not converted
            finally:
                truth._ASSESSMENT_INPUTS.reset(token)
    finally:
        truth._BASELINE_CACHE.reset(cache_token)


def test_spot_subwindows_index_source_rows_once_and_preserve_unsorted_scalar_results(monkeypatch):
    first = parse_time("2026-10-01T00:00:00Z")
    instruments = [
        dict(instrument_id=f"i{index}", venue=f"v{index}", market_type="SPOT" if index < 2 else "PERP")
        for index in range(4)
    ]
    rows = [
        dict(
            instrument,
            minute_start=iso(first + timedelta(minutes=minute)),
            complete=True,
            buy_qty="2.00",
            sell_qty="1.00",
            sum_sq_qty="1.00",
        )
        for minute in reversed(range(360))
        for instrument in instruments
    ]
    windows = [
        (first + timedelta(minutes=minute), first + timedelta(minutes=minute + 15)) for minute in (0, 30)
    ]
    expected = [truth.spot_truth(rows, [], instruments, *window) for window in windows]
    original = truth._market
    visited = []

    def observed(row):
        visited.append(row)
        return original(row)

    monkeypatch.setattr(truth, "_market", observed)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        assert truth.spot_truth(rows, [], instruments, *windows[0]) == expected[0]
        visited.clear()
        assert truth.spot_truth(rows, [], instruments, *windows[1]) == expected[1]
        assert expected[1]["venues"]
        assert len(visited) < len(rows) // 10
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_freshness_uses_one_instrument_index_instead_of_rescanning_each_collection():
    end = parse_time("2026-10-01T12:00:00Z")
    visits = []

    class ObservedRow(dict):
        def get(self, key, default=None):
            if key == "instrument_id":
                visits.append(key)
            return super().get(key, default)

    instruments = [
        dict(instrument_id=f"i{index}", venue=f"v{index}", market_type="PERP", token_id="T")
        for index in range(12)
    ]
    rows = [
        ObservedRow(
            instrument,
            ts=iso(end - timedelta(minutes=minute)),
            ts_prec="EXACT",
            metric_key="OI_NATIVE",
            oi_native="100.000",
        )
        for minute in range(100)
        for instrument in instruments
    ]
    snapshot = dict(token_id="T", instruments=instruments, oi=rows, cutoff=iso(end), assembled_at=iso(end))
    spot = dict(venues=[], coverage=dict(n_complete=0, exact_majority=False))
    with localcontext() as context:
        context.prec = 128
        expected = truth.data_confidence(dict(snapshot, oi=[dict(row) for row in rows]), spot, {}, end)
        visits.clear()
        token = truth._ASSESSMENT_INPUTS.set({})
        try:
            assert truth.data_confidence(snapshot, spot, {}, end) == expected
            assert len(expected["freshness"]) == len(instruments)
            assert len(visits) < 8 * len(rows)
        finally:
            truth._ASSESSMENT_INPUTS.reset(token)
