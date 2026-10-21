"""TDD§0/2.5: verified native units cannot resolve an undocumented bucket edge."""

from copy import deepcopy
from pathlib import Path
from runpy import run_path

import pytest

import tank.analysis.truth as truth
from tank.market import normalize
from tank.util import iso, parse_time


def packet_with_explicit_legacy_bybit_buckets():
    packet = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    old = "binance:perp:T"
    iid = "bybit:perp:T"
    instrument = dict(
        instrument_id=iid,
        token_id="T",
        venue="bybit",
        market_type="PERP_LINEAR",
        quote_asset="USD",
        semantic_verified_keys=["bybit.linear.open-interest"],
    )
    for collection in ("instruments", "funding", "klines"):
        for row in packet[collection]:
            if row.get("instrument_id") == old:
                row.update(instrument_id=iid, venue="bybit")
    for row in packet["semantics"]:
        if row.get("market_type") != "SPOT":
            row.update(venue="bybit", instrument_id=iid)
            if row["metric_family"] == "oi":
                row["metric_key"] = "bybit.linear.open-interest"
    packet["oi"] = [
        normalize.oi(
            instrument,
            {
                "singleOpenInterest": str(row["oi_native"]),
                "timestamp": int(parse_time(row["ts"]).timestamp() * 1000),
            },
            mark="100",
            mark_ts=row["ts"],
            mark_ts_prec="EXACT",
            received_ts=row["ts"],
        )
        for row in packet["oi"]
    ]
    for row in packet["oi"]:
        row.update(
            ts_prec="BUCKET(300s)",
            timestamp_precision="BUCKET(300s)",
            ts_bucket_size_s=300,
            ts_bucket_anchor="UNSPECIFIED",
        )
    return packet


def test_legacy_unit_verified_bybit_unknown_edge_is_supplemental_only():
    packet = packet_with_explicit_legacy_bybit_buckets()
    start, end = parse_time(packet["anchor"]["ts"]), parse_time(packet["cutoff"])
    assert all("semantic_confidence" not in r for r in packet["oi"])
    assert all(r["ts_bucket_anchor"] == "UNSPECIFIED" for r in packet["oi"])
    result = truth.derivatives_truth(packet, start, end)
    assert result["synchronized"] is None
    venue = result["per_venue"][0]
    assert venue["oi_native_chg_pct"] is None
    assert venue["reason"] == "INCOMPLETE"
    observed = venue["oi_temporal_exclusions"]["latest_observation"]
    assert observed["oi_native"] == 1000
    assert observed["ts"] == packet["cutoff"]
    assert observed["ts_bucket_anchor"] == "UNSPECIFIED"
    assert observed["scope"] == "SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT"
    full = truth.compute_market_truth(packet)
    selected = truth.compute_condition_values(
        packet,
        [dict(metric="dc.class", window="EVENT"), dict(metric="deriv.native_oi_chg_pct", window="EVENT")],
    )
    assert full["data_confidence"]["class"] == selected["data_confidence"]["class"] == "MEDIUM"
    assert "critical_semantics_verified" not in full["data_confidence"]
    assert any(f["condition"] == "derivatives_coverage" for f in full["data_confidence"]["failed"])
    assert selected["derivatives"]["windows"]["EVENT"]["synchronized"] is None


@pytest.mark.parametrize("edge", ["UNSPECIFIED", None])
def test_explicit_unknown_bucket_edges_cannot_train_normal_ranges(edge):
    packet = packet_with_explicit_legacy_bybit_buckets()
    start, end = parse_time(packet["anchor"]["ts"]), parse_time(packet["cutoff"])
    for row in packet["oi"]:
        row["ts_bucket_anchor"] = "END" if row["ts"] in (iso(start), iso(end)) else edge
    result = truth._oi_stats(packet["oi"], start, end)
    assert result["oi_native_start"] == result["oi_native_end"] == 1000
    assert result["native_normal_range"] is None
    assert result["native_oi_vs_normal"] is None


@pytest.mark.parametrize("kind", ["END", "START", "EXACT", "ABSENT_LEGACY"])
def test_known_edges_and_exact_or_absent_legacy_clocks_keep_requested_endpoints(kind):
    packet = packet_with_explicit_legacy_bybit_buckets()
    start, end = parse_time(packet["anchor"]["ts"]), parse_time(packet["cutoff"])
    for row in packet["oi"]:
        if kind in ("END", "START"):
            row["ts_bucket_anchor"] = kind
        else:
            row.pop("ts_bucket_anchor")
            if kind == "EXACT":
                row["ts_prec"] = row["timestamp_precision"] = "EXACT"
                row.pop("ts_bucket_size_s")
        row["synchronized_eligible"] = True
    result = truth.derivatives_truth(packet, start, end)
    sync = result["synchronized"]
    assert sync["venues"] == ["bybit"]
    assert sync["start_observed_at"] == iso(start)
    assert sync["end_observed_at"] == iso(end)
    assert sync["oi_native_start"] == sync["oi_native_end"] == 1000
    assert sync["native_normal_range"] == [0.0, 0.0]


def test_sync_eligibility_flag_does_not_hide_hyperliquid_per_venue_change():
    start, end = parse_time("2026-10-19T08:00:00Z"), parse_time("2026-10-19T12:00:00Z")
    instrument = dict(
        instrument_id="hyperliquid:perp:T",
        token_id="T",
        venue="hyperliquid",
        market_type="PERP_LINEAR",
        quote_asset="USD",
        semantic_verified_keys=["hyperliquid.perp.ctx"],
    )
    rows = [
        normalize.oi(instrument, {"openInterest": amount}, received_ts=iso(at))
        for amount, at in (("1000", start), ("1100", end))
    ]
    assert all(r["synchronized_eligible"] is False for r in rows)
    result = truth.derivatives_truth(dict(token_id="T", oi=rows, instruments=[instrument]), start, end)
    assert result["per_venue"][0]["oi_native_chg_pct"] == 0.1
    assert result["synchronized"] is None


def test_unresolved_venue_does_not_poison_known_synchronized_cohort():
    packet = packet_with_explicit_legacy_bybit_buckets()
    start, end = parse_time(packet["anchor"]["ts"]), parse_time(packet["cutoff"])
    bybit = packet["oi"]
    known = [
        dict(row, instrument_id="binance:perp:T", ts_bucket_anchor="END", synchronized_eligible=True)
        for row in deepcopy(bybit)
    ]
    sync = truth.synchronized_oi(
        [
            (dict(venue="binance", instrument_id="binance:perp:T"), known, "SINGLE"),
            (dict(venue="bybit", instrument_id="bybit:perp:T"), bybit, "SINGLE"),
        ],
        start,
        end,
    )
    assert sync["venues"] == ["binance"]
    assert sync["oi_native_end"] == 1000


def test_hyperliquid_cannot_enter_native_sum_even_with_legacy_eligibility_flag():
    packet = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    start, end = parse_time(packet["anchor"]["ts"]), parse_time(packet["cutoff"])
    rows = [dict(row, synchronized_eligible=True) for row in packet["oi"]]
    result = truth.synchronized_oi([({"venue": "hyperliquid"}, rows, "oi_native_single_sided")], start, end)
    assert result is None
