"""Missing liquidation FX is unknown USD, never a zero or a crash (TDD §0)."""

from decimal import Decimal
from pathlib import Path
from runpy import run_path

import pytest

from tank.analysis.truth import compute_market_truth

fixture = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))


@pytest.mark.parametrize("missing_side,other_side", [("LONG_LIQ", "SHORT_LIQ"), ("SHORT_LIQ", "LONG_LIQ")])
def test_actual_truth_keeps_liquidation_sides_independent_when_fx_missing(missing_side, other_side):
    snapshot = fixture["synthetic_start_snapshot"]()
    iid = "binance:perp:T"
    snapshot["capture_metadata"] = {
        "collections": {
            "liquidations": {
                "coverage_start": {iid: "2026-10-16T08:00:00Z"},
                "observed_through": {iid: snapshot["cutoff"]},
            }
        }
    }
    # Declare COMPLETE as the normalized source evidence; do not borrow Binance's sampled default.
    for row in snapshot["oi"]:
        row["liq_coverage"] = "COMPLETE"
    snapshot["liquidations"] = [
        dict(
            instrument_id=iid,
            ts="2026-10-16T09:00:00Z",
            side=missing_side,
            qty_native="2.0000000000000000001",
            notional_usd=None,
            null_reasons={"notional_usd": "INCOMPLETE"},
        ),
        dict(
            instrument_id=iid,
            ts="2026-10-16T09:00:01Z",
            side=missing_side,
            qty_native="3",
            notional_usd="300",
        ),
        dict(
            instrument_id=iid,
            ts="2026-10-16T09:00:02Z",
            side=other_side,
            qty_native="7",
            notional_usd="700.0000000000000000001",
        ),
    ]
    truth = compute_market_truth(snapshot)
    perp = truth["derivatives"]["per_venue"][0]
    missing = "long" if missing_side == "LONG_LIQ" else "short"
    other = "short" if missing == "long" else "long"
    assert perp[f"liq_{missing}_usd"] is None
    assert perp[f"liq_{missing}_usd_reason"] == "INCOMPLETE"
    assert perp[f"liq_{missing}_count"] == 2
    assert Decimal(perp[f"liq_{missing}_native"]) == Decimal("5.0000000000000000001")
    assert Decimal(perp[f"liq_{other}_usd"]) == Decimal("700.0000000000000000001")
    assert perp["liq_coverage"] == "COMPLETE"
    totals = truth["derivatives"]["liquidations"]
    assert totals["complete_venues_total"][missing] is None
    assert totals["complete_venues_total_reasons"][missing] == "INCOMPLETE"
    assert Decimal(totals["complete_venues_total"][other]) == Decimal("700.0000000000000000001")


@pytest.mark.parametrize("registry_present", [True, False])
def test_missing_oi_does_not_drop_independent_funding_and_liquidation_observations(registry_present):
    snapshot = fixture["synthetic_start_snapshot"]()
    snapshot["oi"] = []
    iid = "binance:perp:T"
    if not registry_present:
        snapshot["instruments"] = [r for r in snapshot["instruments"] if r["instrument_id"] != iid]
    snapshot["liquidations"] = [
        dict(
            instrument_id=iid,
            ts="2026-10-16T09:00:00Z",
            side="SHORT_LIQ",
            qty_native="7",
            notional_usd="700.01",
        )
    ]
    truth = compute_market_truth(snapshot)
    derivatives = truth["derivatives"]
    assert derivatives["synchronized"] is None
    perp = derivatives["per_venue"][0]
    assert perp["instrument_id"] == iid
    assert perp["oi_native_start"] is None
    assert perp["oi_native_chg_pct"] is None
    assert perp["reason"] == "INCOMPLETE"
    assert Decimal(perp["funding_settled_last"]) == 0
    assert perp["funding_start_observed_at"] is not None
    assert Decimal(perp["liq_short_usd"]) == Decimal("700.01")
    assert Decimal(perp["liq_short_native"]) == 7
    assert perp["liq_short_count"] == 1
    assert truth["data_confidence"]["derivatives_native_coverage_ready"] is False
