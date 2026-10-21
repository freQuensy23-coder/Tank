"""Monetary precision through actual saved market rows and frozen review references."""

from datetime import timedelta
from decimal import Decimal

import pytest
from review_typed_fixtures import put_boundary, put_flow

from tank.review import ReviewService, price_return
from tank.storage import MemoryRepository
from tank.util import iso, parse_time


@pytest.mark.asyncio
@pytest.mark.parametrize("subminute", [False, True])
async def test_raw_large_prices_preserve_tiny_return_excursions_and_direction(subminute):
    repo = MemoryRepository()
    service = ReviewService(repo)
    start = parse_time("2026-10-01T00:00:30Z" if subminute else "2026-10-01T00:00:00Z")
    end = start + timedelta(hours=1)
    entry = "10000000000000000.01"
    endpoint = "10000000000000000.02"
    high = "10000000000000000.03"
    low = "9999999999999999.99"
    btc_entry = "10000000000000000.01"
    btc_endpoint = "10000000000000000.015"
    floor = start.replace(second=0)
    for minute in range(1 if subminute else 0, 60):
        stamp = iso(floor + timedelta(minutes=minute))
        await put_flow(
            repo,
            f"t-{minute}",
            {
                "instrument_id": "token",
                "open_time": stamp,
                "h": high,
                "l": low,
                "c": endpoint,
                "complete": True,
            },
        )
        await put_flow(
            repo,
            f"b-{minute}",
            {"instrument_id": "btc", "open_time": stamp, "c": btc_endpoint, "complete": True},
        )
    if subminute:
        for instrument, price in [("token", endpoint), ("btc", btc_endpoint)]:
            await put_boundary(
                repo,
                f"{instrument}-start",
                instrument,
                floor,
                start,
                floor + timedelta(minutes=1),
                start + timedelta(seconds=10),
                price,
            )
            await put_boundary(
                repo,
                f"{instrument}-end",
                instrument,
                end.replace(second=0),
                end.replace(second=0),
                end,
                end.replace(second=0) + timedelta(seconds=10),
                price,
            )
    truth = {
        "relative": {
            "ref_venue": "token",
            "entry_ref_price": entry,
            "beta_btc": "1",
            "normal_ranges": {"W1H": {"value": ["-0.00000000000000000049", "0.00000000000000000049"]}},
            "outcome_btc_reference": {"instrument_id": "btc", "entry_ref_price": btc_entry},
        }
    }
    decision = {
        "dec_id": "precision",
        "token_id": "T",
        "cutoff": iso(start),
        "outlook": {"best_horizon": "H1", "direction": "BULLISH"},
    }
    alert = {
        "alert_id": "precision-alert",
        "event_id": "precision-event",
        "version": 1,
        "generated_at": iso(start),
    }
    result = await service.outcome(alert, decision, truth, now=iso(end + timedelta(minutes=1)))
    assert result["status"] == "COMPLETED"
    assert result["realized_return"] == float(price_return(endpoint, entry)) > 0
    assert result["mfe"] == float(price_return(high, entry)) > 0
    assert result["mae"] == float(price_return(low, entry)) < 0
    expected = price_return(endpoint, entry) - price_return(btc_endpoint, btc_entry)
    assert result["btc_adjusted_return"] == float(expected) > 0
    assert result["direction_correct"] is True
    assert isinstance(result["realized_return"], float)
    assert Decimal(entry) != Decimal(endpoint) and float(entry) == float(endpoint)
