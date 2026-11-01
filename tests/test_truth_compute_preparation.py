from array import array
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, Inexact, Rounded, localcontext

import pytest

import tank.analysis.truth as truth
from tank.util import parse_time


def test_oi_bounded_history_preserves_duplicate_order_and_cutoffs(monkeypatch):
    start = parse_time("2026-10-19T00:00:00Z")
    end = start + timedelta(hours=1)
    rows = [
        {"ts": start - timedelta(hours=1), "oi_native": "30", "oi_usd": "300"},
        {"ts": start - timedelta(days=2), "oi_native": "10", "oi_usd": "100"},
        {"ts": start - timedelta(hours=1), "oi_native": "40", "oi_usd": "400"},
        {"ts": start - timedelta(days=2, seconds=1), "oi_native": "999"},
        {"ts": start, "oi_native": "50", "oi_usd": "500"},
        {"ts": end, "oi_native": "60", "oi_usd": "600"},
    ]
    captured = []

    def returns(times, native, usd, window, tolerance):
        captured.append((times, native, usd))
        return [], [], [], []

    monkeypatch.setattr(truth, "_oi_history_returns", returns)
    config = truth._PARAMETERS.set({"history_min_days": 0, "historical_lookback_days": 2})
    try:
        with localcontext() as context:
            context.prec = 128
            result = truth._oi_stats(rows, start, end)
    finally:
        truth._PARAMETERS.reset(config)
    assert result["oi_native_chg_pct"] == 0.2
    assert captured == [
        (
            [start - timedelta(hours=1), start - timedelta(days=2)],
            ["40", "10"],
            ["400", "100"],
        )
    ]


def test_oi_preparation_retains_values_and_rejects_changed_context():
    stamp = parse_time("2026-10-19T00:00:00Z")
    rows = [{"ts": stamp, "oi_native": "-0.00", "oi_usd": "1.2300"}]
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            first = truth._oi_cohort_key(rows)
            prepared = truth._history_decimal_arrays(([rows[0]["oi_native"]],))[0][0]
            assert prepared.as_tuple() == Decimal("-0.00").as_tuple()
            assert truth._oi_cohort_key(rows) == first
            assert not any(context.flags.values())
            context.traps[Rounded] = True
            assert truth._oi_cohort_key(rows) is None
            context.traps[Rounded] = False
            context.prec = 32
            assert truth._oi_cohort_key(rows) is None
            context.prec = 128
            corrected = [dict(rows[0], oi_usd="1.2301")]
            assert truth._oi_cohort_key(corrected) != first
            assert truth._ASSESSMENT_INPUTS.get()["oi_cohort_keys"]
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


@pytest.mark.parametrize("compact", [False, True])
def test_native_volume_batch_preserves_gaps_duplicate_overwrites_and_decimal_math(compact):
    start = parse_time("2026-10-19T00:00:00Z")
    epoch = parse_time("1970-01-01T00:00:00Z")
    opens = [start + timedelta(minutes=i) for i in (0, 1, 1, 2, 4, 5)]
    values = ["-0.00", "2.000", "3.00", None, "5.0000", "7.0"]
    rows = [
        dict(
            instrument_id="A:spot:T",
            token_id="T",
            market_type="SPOT",
            open_time=stamp,
            interval_s=60,
            volume_native=value,
            complete=True,
        )
        for stamp, value in zip(opens, values, strict=True)
    ]
    packet = {"token_id": "T", "klines": rows}
    if compact:
        packet = {
            "token_id": "T",
            "stat_volume_series": {
                "A:spot:T": dict(
                    token_id="T",
                    market_type="SPOT",
                    time_basis="OPEN_TIME",
                    interval_s=60,
                    times_us=array("q", [int((stamp - epoch).total_seconds() * 1000000) for stamp in opens]),
                    values=values,
                    complete=array("b", [1] * len(opens)),
                    dependency_fingerprint="original",
                )
            },
        }
    windows = [
        (0, 1, None if compact else "0.00"),
        (1, 2, "3.000"),
        (2, 3, None),
        (4, 6, "12.0000"),
    ]
    for a, z, amount in windows:
        first, last = start + timedelta(minutes=a), start + timedelta(minutes=z)
        scalar = truth.support_features(packet, first, last, {}, {}, {}, {})
        token = truth._ASSESSMENT_INPUTS.set({})
        try:
            actual = truth.support_features(packet, first, last, {}, {}, {}, {})
        finally:
            truth._ASSESSMENT_INPUTS.reset(token)
        assert actual == scalar
        value = actual["spot_volume"]["value"]
        if amount is None:
            assert value is None
        else:
            assert value.as_tuple() == Decimal(amount).as_tuple()


def test_native_volume_cache_rebuilds_append_and_correction():
    start = parse_time("2026-10-19T00:00:00Z")
    epoch = parse_time("1970-01-01T00:00:00Z")
    first = int((start - epoch).total_seconds() * 1000000)
    projection = dict(
        token_id="T",
        market_type="SPOT",
        time_basis="OPEN_TIME",
        interval_s=60,
        times_us=array("q", [first, first + 60000000]),
        values=["1.00", "2.0"],
        complete=array("b", [1, 1]),
        dependency_fingerprint="before",
    )
    baseline = truth._BASELINE_CACHE.set({})
    try:
        revisions = (
            ("before", ["1.00", "2.0"]),
            ("append", ["1.00", "2.0", "3"]),
            ("correct", ["9", "2.0", "3"]),
        )
        for head, values in revisions:
            updated = deepcopy(projection)
            updated.update(
                dependency_fingerprint=head,
                values=values,
                times_us=array("q", [first + i * 60000000 for i in range(len(values))]),
                complete=array("b", [1] * len(values)),
                append_only=head == "append",
                previous_dependency_fingerprint="before",
                changed_from_us=first if head == "correct" else first + 120000000,
            )
            packet = {"token_id": "T", "stat_volume_series": {"A:spot:T": updated}}
            token = truth._ASSESSMENT_INPUTS.set({})
            try:
                result = truth.support_features(packet, start, start + timedelta(minutes=2), {}, {}, {}, {})
            finally:
                truth._ASSESSMENT_INPUTS.reset(token)
            expected = Decimal("11.0") if head == "correct" else Decimal("3.00")
            assert result["spot_volume"]["value"] == expected
    finally:
        truth._BASELINE_CACHE.reset(baseline)


@pytest.mark.parametrize("signal", [Inexact, Rounded])
def test_native_volume_guard_keeps_trapping_context_on_scalar_path(signal):
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            context.traps[signal] = True
            assert truth._history_decimal_arrays((["1.00"],)) is None
            assert context.traps[signal]
            assert not context.flags[signal]
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
