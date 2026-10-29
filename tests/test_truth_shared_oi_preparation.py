from bisect import bisect_left
from copy import deepcopy
from datetime import timedelta
from decimal import DecimalException, Inexact, Rounded, localcontext

import numpy as np
import pytest

from tank.analysis import truth
from tank.util import iso, parse_time


def original_synchronized(sync, start, end):
    tolerance = float(truth.parameter("oi_sync_tolerance_s", 60))
    admissible = []
    for venue, rows, category in sync:
        if truth._venue(venue) == "hyperliquid":
            continue
        eligible = [r for r in rows if truth.oi_temporally_admissible(r) and truth._time(r) <= end]
        if all(
            any(abs((truth._time(row) - endpoint).total_seconds()) <= tolerance for row in eligible)
            for endpoint in (start, end)
        ):
            admissible.append((venue, eligible, category))
    if not admissible or len({s[2] for s in admissible}) != 1 or admissible[0][2] is None:
        return None
    timestamps = sorted({truth._time(r) for _, rows, _ in admissible for r in rows})
    indexed = []
    for _, rows, _ in admissible:
        ordered = sorted(rows, key=truth._time)
        indexed.append(([truth._time(r) for r in ordered], ordered))
    combined = []
    for at in timestamps:
        selected = []
        for times, rows in indexed:
            index = bisect_left(times, at)
            candidates = [
                rows[i]
                for i in (index - 1, index)
                if 0 <= i < len(rows)
                and abs((times[i] - at).total_seconds()) <= tolerance
                and times[i] <= end
            ]
            if not candidates:
                break
            selected.append(min(candidates, key=lambda row: abs((truth._time(row) - at).total_seconds())))
        if len(selected) == len(admissible) and all(
            truth.decimal(row.get("oi_native")) is not None for row in selected
        ):
            combined.append(
                dict(
                    ts=iso(at),
                    oi_native=truth._sum(selected, "oi_native"),
                    oi_usd=truth._sum(selected, "oi_usd"),
                )
            )
    result = truth._oi_stats(
        combined,
        start,
        end,
        history_identity=tuple(sorted(venue["instrument_id"] for venue, _, _ in admissible)),
        venue_history=indexed,
    )
    if result:
        result["venues"] = [venue["venue"] for venue, _, _ in admissible]
    return result


def cohort():
    first = parse_time("2026-10-01T00:00:00Z")
    sync = []
    for venue in ("binance", "bybit"):
        rows = [
            dict(
                ts=first + timedelta(hours=6 * i),
                oi_native=None if i == 7 else str(100 + i) + ".00",
                oi_usd=None if i == 8 else str(1000 + 3 * i) + ".000",
                oi_usd_source="OI_NATIVE_X_ROW_MARK" if i % 5 == 0 else None,
            )
            for i in range(49)
        ]
        rows.insert(13, dict(rows[12], oi_native="177.000", oi_usd="1999.00"))
        rows.append(dict(rows[-1], ts=first + timedelta(days=13), oi_native="1e2000"))
        rows = rows[::2] + rows[1::2]
        sync.append((dict(venue=venue, instrument_id=f"{venue}:perp:T"), rows, "BASE"))
    return sync, first + timedelta(days=12)


def observe(function, sync, start, end, precision, rounding, signal):
    with localcontext() as context:
        context.prec = precision
        context.rounding = rounding
        context.clear_flags()
        context.flags[Rounded] = True
        if signal is not None:
            context.traps[signal] = True
        try:
            result = function(sync, start, end)
        except DecimalException as error:
            result = type(error)
        return result, context.prec, dict(context.flags), dict(context.traps)


@pytest.mark.parametrize("precision", [32, 128])
@pytest.mark.parametrize("rounding", ["ROUND_HALF_EVEN", "ROUND_DOWN"])
@pytest.mark.parametrize("signal", [None, Inexact, Rounded])
def test_shared_timeline_matches_original_windows_and_decimal_effects(
    monkeypatch, precision, rounding, signal
):
    sync, end = cohort()
    before = deepcopy(sync)
    expected = []
    with monkeypatch.context() as scalar:
        scalar.setattr(truth, "_composite_history_vector", lambda *args: None)
        for hours in (6, 12, 24):
            expected.append(
                observe(
                    original_synchronized,
                    sync,
                    end - timedelta(hours=hours),
                    end,
                    precision,
                    rounding,
                    signal,
                )
            )
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        actual = [
            observe(
                truth.synchronized_oi, sync, end - timedelta(hours=hours), end, precision, rounding, signal
            )
            for hours in (6, 12, 24)
        ]
        assert actual == expected
        assert sync == before
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_shared_timeline_reuses_prepared_join_and_retains_source_rows(monkeypatch):
    sync, end = cohort()
    calls = []
    original = truth._oi_combined_vector

    def counted(*args):
        result = original(*args)
        assert result is not None
        calls.append(len(result))
        return result

    monkeypatch.setattr(truth, "_oi_combined_vector", counted)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            with localcontext(context):
                truth.synchronized_oi(sync, end - timedelta(hours=6), end)
            count = len(calls)
            assert count > 0
            context.clear_flags()
            with localcontext(context):
                truth.synchronized_oi(sync, end - timedelta(hours=12), end)
            assert len(calls) == count
            times, rows = truth._oi_cutoff_index(sync[0][1], end)
            assert times == sorted(times)
            assert all(any(row is source for source in sync[0][1]) for row in rows)
            assert max(times) == end
            assert truth._oi_cutoff_index(sync[0][1], end)[1] is rows
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_history_slices_share_numeric_storage_and_exclude_sourced_usd():
    sync, end = cohort()
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            times, rows = truth._oi_cutoff_index(sync[0][1], end)
            full, _ = truth._oi_history_arrays(rows)
            selected_times, selected = truth._history_slice(times, rows, end - timedelta(hours=12))
            arrays, _ = truth._oi_history_arrays(selected)
            assert all(np.shares_memory(a, b) for a, b in zip(full, arrays, strict=True))
            assert all(at < end - timedelta(hours=12) for at in selected_times)
            for row, value in zip(selected, arrays[1], strict=True):
                if row.get("oi_usd_source") or row.get("oi_usd") is None:
                    assert value is None
                else:
                    assert value.as_tuple() == truth.decimal(row["oi_usd"]).as_tuple()
            context.traps[Rounded] = True
            assert truth._oi_history_arrays(selected) is None
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_changed_sources_cutoffs_and_assessments_never_share_timeline():
    sync, end = cohort()
    corrected = deepcopy(sync)
    for row in corrected[0][1]:
        if row["ts"] == end:
            row["oi_native"] = "999.00"
    for _ in range(2):
        token = truth._ASSESSMENT_INPUTS.set({})
        try:
            with localcontext() as context:
                context.prec = 128
                for packet in (sync, corrected, sync):
                    actual = observe(
                        truth.synchronized_oi,
                        packet,
                        end - timedelta(hours=6),
                        end,
                        128,
                        "ROUND_HALF_EVEN",
                        None,
                    )
                    isolated = truth._ASSESSMENT_INPUTS.set(None)
                    try:
                        expected = observe(
                            original_synchronized,
                            packet,
                            end - timedelta(hours=6),
                            end,
                            128,
                            "ROUND_HALF_EVEN",
                            None,
                        )
                    finally:
                        truth._ASSESSMENT_INPUTS.reset(isolated)
                    assert actual == expected
                    earlier = end - timedelta(hours=6)
                    assert max(truth._oi_cutoff_index(packet[0][1], earlier)[0]) == earlier
        finally:
            truth._ASSESSMENT_INPUTS.reset(token)


def test_fractional_tolerance_does_not_round_an_ineligible_endpoint_into_cohort():
    end = parse_time("2026-10-01T01:00:00Z")
    start = end - timedelta(hours=1)
    rows = [dict(ts=at, oi_native="100", oi_usd="1000") for at in (start + timedelta(microseconds=1), end)]
    parameters = truth._PARAMETERS.set({"oi_sync_tolerance_s": 0.0000006})
    try:
        assert truth.synchronized_oi([(dict(venue="binance"), rows, "BASE")], start, end) is None
    finally:
        truth._PARAMETERS.reset(parameters)


@pytest.mark.parametrize("signal", [Inexact, Rounded])
def test_trapping_context_repeats_original_sum_operations(monkeypatch, signal):
    sync, end = cohort()
    calls = []
    original = truth._sum

    def counted(rows, measure):
        calls.append(measure)
        return original(rows, measure)

    monkeypatch.setattr(truth, "_sum", counted)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.traps[signal] = True
            context.clear_flags()
            indexed = [truth._oi_cutoff_index(rows, end) for _, rows, _ in sync]
            first = truth._oi_combined_timeline(indexed, 60)
            count = len(calls)
            assert count > 0
            assert truth._oi_combined_timeline(indexed, 60) == first
            assert len(calls) == 2 * count
            assert not any(context.flags.values())
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_unresolved_bucket_is_excluded_before_cutoff_indexing():
    sync, end = cohort()
    rows = sync[0][1]
    excluded = dict(
        ts=end,
        ts_prec="BUCKET(300s)",
        ts_bucket_anchor=None,
        oi_native="999999",
        oi_usd="999999",
    )
    rows.append(excluded)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        times, retained = truth._oi_cutoff_index(rows, end)
        assert max(times) == end
        assert all(row is not excluded for row in retained)
        assert truth._oi_split(rows)[1] == [excluded]
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_history_view_preserves_lower_bound_and_duplicate_source_order():
    sync, end = cohort()
    parameters = truth._PARAMETERS.set({"historical_lookback_days": 2})
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            times, rows = truth._oi_cutoff_index(sync[0][1], end)
            start = times[14]
            selected_times, selected = truth._history_slice(times, rows, start)
            expected = [
                (at, row)
                for at, row in zip(times, rows, strict=True)
                if start - timedelta(days=2) <= at < start
            ]
            assert selected_times == [at for at, _ in expected]
            assert all(a is b for a, (_, b) in zip(selected, expected, strict=True))
            arrays, _ = truth._oi_history_arrays(selected)
            assert [value.as_tuple() if value is not None else None for value in arrays[0]] == [
                truth.decimal(row["oi_native"]).as_tuple() if row["oi_native"] is not None else None
                for row in selected
            ]
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
        truth._PARAMETERS.reset(parameters)


def test_prepared_join_preserves_nearest_rows_with_offset_venues_and_duplicate_times(monkeypatch):
    sync, end = cohort()
    for row in sync[1][1]:
        if row["ts"] < end:
            row["ts"] += timedelta(seconds=30)
    start = end - timedelta(hours=12)
    with monkeypatch.context() as scalar:
        scalar.setattr(truth, "_composite_history_vector", lambda *args: None)
        expected = observe(original_synchronized, sync, start, end, 128, "ROUND_HALF_EVEN", None)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        actual = observe(truth.synchronized_oi, sync, start, end, 128, "ROUND_HALF_EVEN", None)
        assert actual == expected
        assert actual[0]["venues"] == ["binance", "bybit"]
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
