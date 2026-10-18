"""Monetary precision through actual saved market rows and frozen review references."""

from datetime import timedelta
from decimal import Decimal

import pytest

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
        await repo.put(
            "mkt.klines_1m",
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
        await repo.put(
            "mkt.klines_1m",
            f"b-{minute}",
            {"instrument_id": "btc", "open_time": stamp, "c": btc_endpoint, "complete": True},
        )
    if subminute:
        for instrument, price in [("token", endpoint), ("btc", btc_endpoint)]:
            for minute, second in [(0, 40), (60, 10)]:
                stamp = floor + timedelta(minutes=minute)
                key = f"{instrument}-{minute}"
                await repo.put(
                    "mkt.trades",
                    key,
                    {
                        "instrument_id": instrument,
                        "trade_id": key,
                        "ts": iso(stamp + timedelta(seconds=second)),
                        "price": price,
                    },
                )
                await repo.put(
                    "mkt.flow_bars_1m",
                    key,
                    {
                        "instrument_id": instrument,
                        "minute_start": iso(stamp),
                        "n_buy": 1,
                        "n_sell": 0,
                        "complete": True,
                    },
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
    assert isinstance(result["realized_return"], float)  # Existing dimensionless output contract.
    saved = await repo.get("qa.outcomes", result["outcome_id"])
    assert saved["realized_return"] > 0
    assert Decimal(entry) != Decimal(endpoint) and float(entry) == float(endpoint)
