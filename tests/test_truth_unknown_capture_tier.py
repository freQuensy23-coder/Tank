"""Historical source cadence cannot be inferred from an unproven capture tier."""

from pathlib import Path
from runpy import run_path

from tank.analysis.truth import capture_cadence, compute_market_truth


def test_unknown_historical_capture_tier_does_not_fall_back_to_base():
    fixture = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))
    snapshot = fixture["synthetic_start_snapshot"]()
    snapshot["capture_metadata"] = {
        "tier": "UNKNOWN",
        "collections": {
            "oi": {"cadence_s": None, "reason": "CAPTURE_TIER_HISTORY_UNAVAILABLE"},
            "funding": {"cadence_s": None, "reason": "CAPTURE_TIER_HISTORY_UNAVAILABLE"},
            "supply": {"by_kind": {"EXCHANGE_INVENTORY": None, "WALLET_FLOWS": None, "FUNDAMENTALS": 21600}},
        },
    }
    for collection in ("oi", "funding"):
        assert capture_cadence(snapshot, collection) == (None, "CAPTURE_TIER_HISTORY_UNAVAILABLE")
    for kind in ("EXCHANGE_INVENTORY", "WALLET_FLOWS"):
        assert capture_cadence(snapshot, "supply", {"kind": kind}) == (
            None,
            "CAPTURE_TIER_HISTORY_UNAVAILABLE",
        )
    assert capture_cadence(snapshot, "supply", {"kind": "FUNDAMENTALS"}) == (21600.0, "CAPTURE_METADATA")
    result = compute_market_truth(snapshot)
    assert result["data_confidence"]["class"] == "MEDIUM"
    unknown = [
        failure
        for failure in result["data_confidence"]["failed"]
        if failure.get("cadence_source") == "CAPTURE_TIER_HISTORY_UNAVAILABLE"
    ]
    assert {f["collection"] for f in unknown} == {"oi", "funding"}
    assert all(f["cadence_s"] is None for f in unknown)
