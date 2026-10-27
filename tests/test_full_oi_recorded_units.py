"""Full derivative conclusions consume normalized USD, never an unvalued quote mark."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import JsonValue

from tank.analysis.truth import derivatives_truth, measurement_metadata
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


@pytest.mark.parametrize("raw", [None, "NaN", "-1", "0"])
def test_full_oi_usd_component_availability_does_not_erase_native(raw: str | None) -> None:
    snapshot, start, end = recorded_snapshot(native_start=3, native_end=4, recorded=True, source_label=None)
    rows = snapshot["oi"]
    assert isinstance(rows, list) and isinstance(rows[-1], dict)
    rows[-1]["oi_usd"] = raw
    result = derivatives_truth(snapshot, start, end)["per_venue"][0]
    assert result["oi_native_start"] == Decimal(3)
    assert result["oi_native_end"] == Decimal(4)
    assert result["native_normal_range"] == [0, 0]
    if raw == "0":
        assert result["oi_usd_end"] == 0
        assert result["oi_usd_chg_pct"] == -1
        assert result["oi_usd_reason"] is None
        assert result["oi_usd_detail"] is None
    else:
        assert result["oi_usd_end"] is None
        assert result["oi_usd_chg_pct"] is None
        assert result["oi_usd_detail"] == (
            "RECORDED_USD_NOT_CAPTURED" if raw is None else "REPORTED_USD_INVALID"
        )


def test_full_oi_known_zero_usd_baseline_remains_observed() -> None:
    snapshot, start, end = recorded_snapshot(native_start=3, native_end=4, recorded=True, source_label=None)
    rows = snapshot["oi"]
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
        row["oi_usd"] = "0"
    result = derivatives_truth(snapshot, start, end)["per_venue"][0]
    assert result["oi_usd_start"] == result["oi_usd_end"] == 0
    assert result["oi_usd_reason"] is None
    assert result["oi_usd_detail"] is None
    assert result["oi_usd_chg_pct"] is None
    assert result["oi_usd_chg_pct_reason"] == "ZERO_BASELINE"


def test_full_oi_repeating_dollar_term_metadata_preserves_exact_cause() -> None:
    snapshot, start, end = recorded_snapshot(native_start=3, native_end=4, recorded=True, source_label=None)
    derivatives = derivatives_truth(snapshot, start, end)
    metadata = measurement_metadata(
        {"cutoff": iso(end), "primary_window": "W4H", "derivatives": derivatives}, spot_measurements=()
    )
    term = metadata["derivatives.per_venue[0].decomposition.position"]
    assert term["value"] is None
    assert term["unit"] == "USD"
    assert term["reason"] == "NOT_ESTIMABLE"
    assert term["reason_detail"] == "NON_TERMINATING_DECIMAL"
    markup = metadata["derivatives.per_venue[0].decomposition.markup"]
    assert markup["value"] == Decimal("-0.25")
    assert markup["reason"] is None


def test_full_synchronized_oi_cannot_hide_invalid_usd_inside_a_positive_sum() -> None:
    snapshot, start, end = recorded_snapshot(native_start=3, native_end=4, recorded=True, source_label=None)
    points = snapshot["oi"]
    assert isinstance(points, list)
    all_points: list[JsonValue] = []
    instruments: list[JsonValue] = []
    semantics: list[JsonValue] = []
    for venue in ("binance", "bybit"):
        iid = venue + ":perp:T"
        instruments.append({"instrument_id": iid, "venue": venue, "market_type": "PERP_LINEAR"})
        semantics.append(
            {
                "instrument_id": iid,
                "metric_key": venue + ".oi",
                "sidedness": "SINGLE_SIDED",
                "aggregation_class": "oi_native_single_sided",
            }
        )
        for point in points:
            assert isinstance(point, dict)
            row = deepcopy(point)
            row.update(instrument_id=iid, venue=venue, metric_key=venue + ".oi")
            row["oi_usd"] = "10" if venue == "binance" else "-1" if row["ts"] == iso(end) else "1"
            all_points.append(row)
    result = derivatives_truth(
        {"instruments": instruments, "oi": all_points, "semantics": semantics}, start, end
    )
    invalid = next(row for row in result["per_venue"] if row["venue"] == "bybit")
    assert invalid["oi_native_end"] == 4
    assert invalid["oi_usd_end"] is None
    assert invalid["oi_usd_detail"] == "REPORTED_USD_INVALID"
    synchronized = result["synchronized"]
    assert synchronized["oi_native_start"] == 6
    assert synchronized["oi_native_end"] == 8
    assert synchronized["oi_usd_start"] == 11
    assert synchronized["oi_usd_end"] is None
    assert synchronized["oi_usd_chg_pct"] is None
