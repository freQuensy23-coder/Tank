from datetime import timedelta

from tank.analysis import truth
from tank.util import iso, parse_time


def workload():
    first = parse_time("2026-10-01T00:00:00Z")
    end = first + timedelta(days=12)
    instruments, rows, semantics = [], [], []
    for venue in ("binance", "bybit"):
        iid = venue + ":perp:T"
        instruments.append(dict(instrument_id=iid, token_id="t", venue=venue, market_type="PERP"))
        semantics.append(
            dict(instrument_id=iid, metric_key="oi", sidedness="SINGLE_SIDED", aggregation_class="BASE")
        )
        rows.extend(
            dict(
                instrument_id=iid,
                token_id="t",
                venue=venue,
                market_type="PERP",
                ts=iso(first + timedelta(hours=6 * index)),
                ts_prec="EXACT",
                metric_key="oi",
                oi_native=str(100 + index),
                oi_usd=str(1000 + index),
                mark_price="101",
                index_price="100",
            )
            for index in range(49)
        )
    return dict(token_id="t", cutoff=iso(end), instruments=instruments, oi=rows, semantics=semantics), end


def test_derivative_windows_reuse_universe_classification_without_reusing_window_results(monkeypatch):
    snapshot, end = workload()
    starts = [end - timedelta(hours=12), end - timedelta(hours=6)]
    expected = [truth.derivatives_truth(snapshot, start, end) for start in starts]
    original, calls = truth._market, []

    def classified(row):
        calls.append(row)
        return original(row)

    monkeypatch.setattr(truth, "_market", classified)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        first = truth.derivatives_truth(dict(snapshot, _primary="W12H"), starts[0], end)
        first_calls = len(calls)
        second = truth.derivatives_truth(dict(snapshot, _primary="W6H"), starts[1], end)
        assert first == expected[0]
        assert second == expected[1]
        assert first["per_venue"][0]["oi_native_start"] != second["per_venue"][0]["oi_native_start"]
        assert first_calls > 0
        assert len(calls) == first_calls
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_derivative_preparation_tracks_replaced_sources_and_isolates_assessments():
    snapshot, end = workload()
    replacement = dict(snapshot, instruments=list(reversed(snapshot["instruments"])))
    replacement["oi"] = [dict(row, oi_native="200") for row in snapshot["oi"]]
    expected = truth.derivatives_truth(replacement, end - timedelta(hours=6), end)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        first = truth.derivatives_truth(snapshot, end - timedelta(hours=6), end)
        second = truth.derivatives_truth(replacement, end - timedelta(hours=6), end)
        assert second == expected
        assert second["per_venue"][0]["instrument_id"] != first["per_venue"][0]["instrument_id"]
        assert second["per_venue"][0]["oi_native_end"] != first["per_venue"][0]["oi_native_end"]
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
    fresh = truth._ASSESSMENT_INPUTS.set({})
    try:
        assert truth.derivatives_truth(replacement, end - timedelta(hours=6), end) == expected
    finally:
        truth._ASSESSMENT_INPUTS.reset(fresh)
