from bisect import bisect_right
from datetime import timedelta
from decimal import localcontext
from math import log
from statistics import stdev

import pytest

from tank.analysis import truth
from tank.util import parse_time


def test_oi_envelope_reuse_preserves_anchor_results_and_validates_each_timestamp(monkeypatch):
    start = parse_time("2026-10-19T00:00:00Z")
    rows = [
        dict(
            ts=(start + timedelta(minutes=i)).isoformat(),
            ts_prec="BUCKET(300s)",
            timestamp_precision="BUCKET(300s)",
            ts_bucket_anchor=anchor,
        )
        for anchor in ("START", None)
        for i in range(3)
    ]
    calls = []
    original = truth.propagate_timestamp

    def observed(*args, **kwargs):
        calls.append(args[0]["ts"])
        return original(*args, **kwargs)

    monkeypatch.setattr(truth, "propagate_timestamp", observed)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.clear_flags()
            before = dict(context.flags)
            assert [truth.oi_temporally_admissible(row) for row in rows] == [True] * 3 + [False] * 3
            assert len(calls) == len({row["ts_bucket_anchor"] for row in rows})
            assert dict(context.flags) == before
            with pytest.raises(ValueError):
                truth.oi_temporally_admissible(dict(rows[0], ts="not-a-timestamp"))
            with pytest.raises(ValueError):
                truth.oi_temporally_admissible(dict(rows[0], timestamp_precision="BUCKET(60s)"))
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


@pytest.mark.parametrize("gap", [False, True])
def test_prepared_reference_rv_preserves_every_statistics_stdev_result(gap):
    start = parse_time("2026-10-19T00:00:00Z")
    prices = {start + timedelta(hours=i): float(100 + i + i % 7) for i in range(96)}
    prices[start + timedelta(minutes=30)] = 145.0
    if gap:
        prices.pop(start + timedelta(hours=40))
    hourly = {t: p for t, p in prices.items() if t.minute == 0 and t.second == 0}
    returns = {
        t: log(p / hourly[t - timedelta(hours=1)])
        for t, p in hourly.items()
        if t - timedelta(hours=1) in hourly
    }
    return_times = sorted(returns)
    expected_times, expected_values = [], []
    for t in sorted(hourly):
        first = bisect_right(return_times, t - timedelta(hours=24))
        last = bisect_right(return_times, t)
        values = [returns[x] for x in return_times[first:last]]
        if len(values) == 24:
            expected_times.append(t)
            expected_values.append(stdev(values))
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        assert truth._reference_rv_series(prices) == (hourly, expected_times, expected_values)
        assert truth._reference_rv_series(dict(prices)) == (hourly, expected_times, expected_values)
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), 0.0, -1.0])
def test_nonfinite_or_nonpositive_reference_prices_keep_scalar_error_path(value):
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        assert truth._reference_rv_series({parse_time("2026-10-19T00:00:00Z"): value}) is None
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
