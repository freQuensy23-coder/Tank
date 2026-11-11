"""Missing liquidation FX is unknown USD, never a zero or a crash (TDD §0)."""

from decimal import Decimal
from pathlib import Path
from runpy import run_path

import pytest
from funding_helpers import steady

from tank.analysis.truth import compute_market_truth
from tank.util import parse_time

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

    for row in snapshot["instruments"]:
        if row["instrument_id"] == iid:
            row["liq_coverage"] = "COMPLETE"
    missing = "long" if missing_side == "LONG_LIQ" else "short"
    other = "short" if missing == "long" else "long"
    snapshot["funding_sql"] = steady(
        parse_time(snapshot["cutoff"]),
        [iid],
        liquidations={
            None: {
                iid: {
                    "observations": 3,
                    f"{missing}_count": 2,
                    f"{missing}_usd": 300.0,
                    f"{missing}_usd_missing": 1,
                    f"{missing}_native": 5.0,
                    f"{other}_count": 1,
                    f"{other}_usd": 700.0,
                    f"{other}_native": 7.0,
                    "total_usd": 1000.0,
                    "total_usd_missing": 1,
                }
            }
        },
    )
    truth = compute_market_truth(snapshot)
    perp = truth["derivatives"]["per_venue"][0]
    assert perp[f"liq_{missing}_usd"] is None
    assert perp[f"liq_{missing}_usd_reason"] == "INCOMPLETE"
    assert perp[f"liq_{missing}_count"] == 2
    assert Decimal(perp[f"liq_{missing}_native"]) == 5
    assert Decimal(perp[f"liq_{other}_usd"]) == 700
    assert perp["liq_coverage"] == "COMPLETE"
    totals = truth["derivatives"]["liquidations"]
    assert totals["complete_venues_total"][missing] is None
    assert totals["complete_venues_total_reasons"][missing] == "INCOMPLETE"
    assert Decimal(totals["complete_venues_total"][other]) == 700


def test_missing_oi_does_not_drop_independent_funding_and_liquidation_observations():
    snapshot = fixture["synthetic_start_snapshot"]()
    snapshot["oi"] = []
    snapshot.pop("oi_sql")
    iid = "binance:perp:T"
    snapshot["funding_sql"] = steady(
        parse_time(snapshot["cutoff"]),
        [iid],
        liquidations={
            None: {
                iid: dict(observations=1, short_count=1, short_usd=700.01, short_native=7.0, total_usd=700.01)
            }
        },
    )
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
