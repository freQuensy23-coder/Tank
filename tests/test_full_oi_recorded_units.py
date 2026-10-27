"""Full derivative conclusions consume normalized USD, never an unvalued quote mark."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import JsonValue

from tank.analysis.truth import derivatives_truth
from tank.util import iso


def recorded_snapshot(
    *, native_start: int, native_end: int, recorded: bool, source_label: str | None
) -> tuple[dict[str, JsonValue], datetime, datetime]:
    end = datetime(2026, 10, 16, 12, tzinfo=UTC)
    start = end - timedelta(hours=4)
    instrument = "bybit:perp_linear:BTCUSDT"
    rows: list[JsonValue] = []
    tickers: list[JsonValue] = []
    for index in range(-8 * 288, 1):
        at = end + timedelta(minutes=5 * index)
        rows.append(
            {
                "instrument_id": instrument,
                "venue": "bybit",
                "ts": iso(at),
                "oi_native": native_start if at < end else native_end,
                "oi_usd": "1" if recorded else None,
                **({"oi_usd_source": source_label} if source_label is not None else {}),
            }
        )
        tickers.append({"instrument_id": instrument, "ts": iso(at), "mark_price": "10"})
    return (
        {
            "instruments": [
                {
                    "instrument_id": instrument,
                    "venue": "bybit",
                    "market_type": "perp_linear",
                    "aggregation_class": "native",
                }
            ],
            "oi": rows,
            "tickers": tickers,
        },
        start,
        end,
    )


def test_full_oi_missing_normalized_usd_does_not_bypass_quote_conversion() -> None:
    snapshot, start, end = recorded_snapshot(
        native_start=100, native_end=108, recorded=False, source_label=None
    )
    result = derivatives_truth(snapshot, start, end)["per_venue"][0]
    assert result["oi_native_start"] == Decimal(100)
    assert result["oi_native_end"] == Decimal(108)
    assert result["oi_native_chg_pct"] == pytest.approx(0.08)
    assert result["native_normal_range"] == [0, 0]
    assert result["oi_usd_start"] is None
    assert result["oi_usd_end"] is None
    assert result["oi_usd_chg_pct"] is None
    assert result["decomposition"]["pos_share"] is None
    assert result["oi_class"] is None


def test_full_oi_recorded_usd_extension_does_not_erase_historical_amounts() -> None:
    snapshot, start, end = recorded_snapshot(
        native_start=3, native_end=4, recorded=True, source_label="RETAINED_EXTENSION"
    )
    result = derivatives_truth(snapshot, start, end)["per_venue"][0]
    assert result["oi_usd_start"] == result["oi_usd_end"] == Decimal(1)
    assert result["oi_usd_chg_pct"] == 0
    assert result["usd_normal_range"] == [0, 0]


def test_full_oi_recorded_implied_price_classification_preserves_exact_half() -> None:
    snapshot, start, end = recorded_snapshot(native_start=3, native_end=4, recorded=True, source_label=None)
    result = derivatives_truth(snapshot, start, end)["per_venue"][0]
    assert result["native_normal_range"] == result["usd_normal_range"] == [0, 0]
    assert result["native_beyond"] is True
    assert result["oi_native_chg_pct"] == pytest.approx(1 / 3)
    assert result["decomposition"]["pos_share"] == 0.5
    assert result["oi_class"] == "MIXED"
