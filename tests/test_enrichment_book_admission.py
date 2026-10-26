from datetime import timedelta

import pytest
from pydantic import JsonValue
from truth_support import unselected_spot_sign_history

from tank.analysis.truth import enrich_evidence
from tank.market.books import OrderBook
from tank.util import iso, parse_time


def _enrich_book(book: dict[str, JsonValue]) -> None:
    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(minutes=15)
    enrich_evidence(
        {"token_id": "T", "books": [book], "cutoff": iso(end)},
        start,
        end,
        {
            "W1H": {
                "venues": [],
                "composite_max": {"volume": 0},
                "direction": "NEUTRAL",
                "composite_cohort": None,
                "cohort_consistent": {},
            }
        },
        "W1H",
        {},
        {
            "windows": {"W15M": {"normal_range": None}, "W1H": {"r_raw": None}},
            "context_inputs": {"rv24": None, "rv_normal_range": None},
            "ref_venue": None,
        },
        {"per_venue": []},
        sign_history=unselected_spot_sign_history("T", start, end),
    )


def test_actual_rejected_book_without_venue_remains_unconsumed() -> None:
    book = OrderBook(
        {
            "instrument_id": "coinbase:spot:T",
            "token_id": "T",
            "venue": "coinbase",
            "market_type": "SPOT",
            "venue_symbol": "T-USD",
            "quote_asset": "USD",
        }
    )
    book.snapshot([["102", "1"]], [["101", "1"]], ts="2026-10-16T11:59:00Z")
    summary = book.summary(received_ts="2026-10-16T11:59:01Z")
    assert "venue" not in summary
    assert summary["instrument_id"] == "coinbase:spot:T"
    assert summary["integrity_ok"] is False
    _enrich_book(summary)


@pytest.mark.parametrize("integrity", [False, None])
def test_rejected_unscoped_book_does_not_require_unused_venue(integrity: bool | None) -> None:
    _enrich_book({"market_type": "SPOT", "integrity_ok": integrity})


def test_eligible_unscoped_book_refuses_missing_venue() -> None:
    with pytest.raises(ValueError, match="actual nonblank venue"):
        _enrich_book({"market_type": "SPOT", "integrity_ok": True})
