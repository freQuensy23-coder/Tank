"""Bybit historical OI is a source-timestamped stock sample, not a time bucket."""

from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from test_market_normalize import TS, instrument

from tank.analysis.truth import _oi_stats, oi_temporally_admissible, synchronized_oi
from tank.market.normalize import SemanticError, oi
from tank.time_precision import interval_bounds, propagate_timestamp
from tank.util import iso, parse_time


@pytest.mark.parametrize("period", [300, 900, 3600])
def test_sample_time_is_exact_without_rounding_or_request_bucket_invention(period):
    inst = instrument("bybit", "PERP_LINEAR")
    timestamp = TS + 1234  # Deliberately not a cadence boundary.
    raw = {"singleOpenInterest": "123.456789", "openInterest": "246.913578", "timestamp": str(timestamp)}
    original = deepcopy(raw)
    row = oi(inst, raw, bucket_size_s=period, received_ts=timestamp + 4000)
    stamp = propagate_timestamp(row, "ts")
    assert raw == original and stamp.precision == row["timestamp_precision"] == "EXACT"
    assert stamp.value == iso(timestamp) and interval_bounds(stamp) == (parse_time(timestamp),) * 2
    assert row["ts_bucket_size_s"] is None and "ts_bucket_anchor" not in row
    assert row["raw_value"] == row["oi_native"] == "123.456789"
    assert row["sidedness"] == "SINGLE_SIDED" and row["raw_unit"] == "BASE_QTY"
    assert "semantic_confidence" not in row and row["synchronized_eligible"] is True
    assert row["received_ts"] == iso(timestamp + 4000) and row["received_ts_prec"] == "COLLECTION_ONLY"


def test_current_period_sample_is_eligible_but_future_sample_is_not_selected():
    inst = instrument("bybit", "PERP_LINEAR")
    start = parse_time(TS)
    end = start + timedelta(minutes=4)
    rows = [
        oi(inst, {"singleOpenInterest": value, "timestamp": iso(ts)})
        for ts, value in [(start, "100"), (end, "105"), (end + timedelta(seconds=30), "99999")]
    ]
    assert all(oi_temporally_admissible(r) for r in rows)
    result = _oi_stats(rows, start, end)
    assert result["oi_native_start"] == Decimal("100") and result["oi_native_end"] == Decimal("105")
    assert Decimal(str(result["oi_native_chg_pct"])) == Decimal(".05")
    legacy = dict(
        rows[1],
        ts_prec="BUCKET(300s)",
        timestamp_precision="BUCKET(300s)",
        ts_bucket_size_s=300,
        ts_bucket_anchor="UNSPECIFIED",
    )
    original = deepcopy(legacy)
    assert not oi_temporally_admissible(legacy)
    assert _oi_stats([rows[0], legacy], start, end) is None and legacy == original


def test_missing_timestamp_bothsided_only_and_unaligned_mark_remain_unavailable():
    inst = instrument("bybit", "PERP_LINEAR")
    with pytest.raises(SemanticError, match="source timestamp absent"):
        oi(inst, {"singleOpenInterest": "100"})
    with pytest.raises(SemanticError, match="cannot substitute"):
        oi(inst, {"openInterest": "200", "timestamp": TS})
    row = oi(inst, {"singleOpenInterest": "100", "timestamp": TS}, mark="2", mark_ts=TS + 61000)
    assert row["mark_price"] is None and row["oi_usd"] is None
    assert "semantic_confidence" not in row


def test_actual_retained_f11_endpoint_values_reach_production_oi_math_without_replacing_rest():
    import hashlib
    import json

    source = Path(__file__).resolve().parents[1] / "fixture_check/data"
    binance_file, bybit_file = (
        source / "rest_openInterestHist_fixtures.json",
        source / "bybit_oi_5m_WLDUSDT.json",
    )
    assert (
        hashlib.sha256(binance_file.read_bytes()).hexdigest()
        == "67e4afd8ee1e43e348344668a933b66b3e2490e02bd62b1ce5244d8205a88f45"
    )
    binance_raw = json.loads(binance_file.read_text())["data"]["F-11:WLDUSDT:5m"]
    bybit_raw = json.loads(bybit_file.read_text())
    start, end = parse_time("2026-10-16T08:30:00Z"), parse_time("2026-10-16T12:30:00Z")
    groups = []
    for venue, raw in [("binance", binance_raw), ("bybit", bybit_raw)]:
        inst = dict(
            instrument(venue, "PERP_LINEAR"),
            instrument_id=venue + ":perp_linear:WLDUSDT",
            token_id="world:WLD",
            venue_symbol="WLDUSDT",
        )
        # Configured single-sided native units are immediately usable; no probe status.
        rows = [oi(inst, r, bucket_size_s=300) for r in raw if parse_time(r["timestamp"]) <= end]
        groups.append((inst, rows, "BASE_QTY_SINGLE_SIDED"))
    result = synchronized_oi(groups, start, end)
    assert result is not None
    native = result["oi_native_chg_pct"]
    assert Decimal(".0768") <= native <= Decimal(".0868")
    assert result["oi_native_start"] == Decimal("183464692") + Decimal("83937472.5")
    assert result["oi_native_end"] == Decimal("202487919") + Decimal("86777506")
    # USD/normal history is absent here, so this endpoint test does not certify REAL_NATIVE.
    assert result["oi_class"] is None
