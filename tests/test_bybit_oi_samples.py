"""Bybit historical OI is a source-timestamped stock sample, not a time bucket."""

from copy import deepcopy

import pytest
from test_market_normalize import TS, instrument

from tank.market.normalize import SemanticError, oi
from tank.time_precision import interval_bounds, propagate_timestamp
from tank.util import iso, parse_time


@pytest.mark.parametrize("period", [300, 900, 3600])
def test_sample_time_is_exact_without_rounding_or_request_bucket_invention(period):
    inst = instrument("bybit", "PERP_LINEAR")
    timestamp = TS + 1234
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


def test_missing_timestamp_bothsided_only_and_unaligned_mark_remain_unavailable():
    inst = instrument("bybit", "PERP_LINEAR")
    with pytest.raises(SemanticError, match="source timestamp absent"):
        oi(inst, {"singleOpenInterest": "100"})
    with pytest.raises(SemanticError, match="cannot substitute"):
        oi(inst, {"openInterest": "200", "timestamp": TS})
    row = oi(inst, {"singleOpenInterest": "100", "timestamp": TS}, mark="2", mark_ts=TS + 61000)
    assert row["mark_price"] is None and row["oi_usd"] is None
    assert "semantic_confidence" not in row
