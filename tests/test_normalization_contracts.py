from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import JsonValue, ValidationError

from tank.market import normalize
from tank.market.errors import SemanticError
from tank.market.normalization_contracts import NormalizationInstrument, NormalizationMarket
from tank.util import iso, parse_epoch

SOURCE_MS = 1781654400000
SOURCE_TIME = parse_epoch(SOURCE_MS, unit="ms")


def instrument() -> dict[str, JsonValue]:
    return {
        "venue": "binance",
        "market_type": "PERP_LINEAR",
        "instrument_id": "binance:perp:ABCUSDT",
        "token_id": "test:ABC",
        "quote_asset": "USDT",
    }


@pytest.mark.parametrize("value", [None, "", "spot", "UNDECLARED", True])
def test_declared_market_scope_is_required_for_routing(value: JsonValue) -> None:
    wire = instrument()
    wire["market_type"] = value
    with pytest.raises(ValidationError):
        normalize.venue_key(wire)


def test_absent_market_is_not_assumed_spot() -> None:
    wire = instrument()
    del wire["market_type"]
    with pytest.raises(ValidationError):
        normalize.venue_key(wire)


@pytest.mark.parametrize("field", ["instrument_id", "token_id"])
@pytest.mark.parametrize("value", [None, "", False, 1])
def test_actual_liquidation_cannot_emit_invalid_normalized_identity(field: str, value: JsonValue) -> None:
    wire = instrument()
    wire[field] = value
    with pytest.raises(ValidationError):
        normalize.liquidation(wire, {"S": "SELL", "z": "1", "ap": "2", "T": SOURCE_MS})


def test_consumed_scope_does_not_require_unrelated_registry_facts() -> None:
    scope = {"venue": "binance", "market_type": "PERP_LINEAR"}
    assert normalize.venue_key(scope) == "binance.usdm"
    assert NormalizationMarket.from_wire(scope).market_type == "PERP_LINEAR"
    with pytest.raises(ValidationError):
        NormalizationInstrument.from_wire(scope)
    wire = instrument()
    wire["unused_provider_extension"] = {"unchanged": [0, None, "raw"]}
    before = dict(wire)
    row = normalize.liquidation(wire, {"S": "SELL", "z": "0", "q": "3", "ap": "2", "T": SOURCE_MS})
    assert row["instrument_id"] == wire["instrument_id"]
    assert row["qty_native"] == "0"
    assert row["notional_usd"] is None
    assert wire == before


def test_declared_time_metadata_does_not_infer_precision_or_receipt() -> None:
    unknown = normalize.timestamp_fields("anchor", iso(SOURCE_TIME), None)
    assert unknown["anchor"] == iso(SOURCE_TIME)
    assert unknown["anchor_prec"] is None
    assert unknown["anchor_prec_reason"] == "INCOMPLETE"
    bucket = normalize.timestamp_fields("anchor", SOURCE_TIME, "BUCKET", bucket_size=(60, "s"))
    assert bucket["anchor_prec"] == "BUCKET(60s)"
    with pytest.raises(ValueError):
        normalize.timestamp_fields("anchor", "2026-06-17T00:00:00", "EXACT")


def test_captured_quote_candidate_keeps_unclocked_fallback_and_exact_history_policy() -> None:
    quote = "NORMALIZATION_TEST_QUOTE"
    wire = instrument()
    wire.update(quote_asset=quote, quote_usd_rate="0.99900")
    before = normalize.QUOTE_USD_HISTORY.get(quote)
    try:
        assert normalize.quote_usd_rate(wire, SOURCE_TIME) == ("0.99900", None)
        normalize.remember_quote_usd(quote, [(SOURCE_TIME, Decimal("1.00100"))])
        assert normalize.quote_usd_rate(wire, SOURCE_TIME) == ("1.00100", iso(SOURCE_TIME))
        assert normalize.quote_usd_rate(wire, SOURCE_TIME - timedelta(microseconds=1)) == (None, None)
        assert normalize.quote_usd_rate(wire, SOURCE_TIME + timedelta(hours=6)) == (
            "1.00100",
            iso(SOURCE_TIME),
        )
        assert normalize.quote_usd_rate(wire, SOURCE_TIME + timedelta(hours=6, microseconds=1)) == (
            None,
            None,
        )
        normalize.remember_quote_usd(quote, [(SOURCE_TIME + timedelta(days=5), "1.002")])
        assert normalize.QUOTE_USD_HISTORY[quote] == ((SOURCE_TIME + timedelta(days=5), "1.002"),)
    finally:
        if before is None:
            normalize.QUOTE_USD_HISTORY.pop(quote, None)
        else:
            normalize.QUOTE_USD_HISTORY[quote] = before


def test_wrong_provider_candle_container_refuses_before_financial_projection() -> None:
    with pytest.raises(SemanticError, match="field sequence"):
        normalize.kline(instrument(), {"0": SOURCE_MS}, now=SOURCE_TIME)
    wire = instrument()
    wire.update(venue="hyperliquid", market_type="PERP_LINEAR")
    with pytest.raises(SemanticError, match="requires an object"):
        normalize.kline(wire, [SOURCE_MS, "1", "1", "1", "1", "2"], now=SOURCE_TIME)


@pytest.mark.parametrize(
    "venue,market,provider_row,volume,quote",
    [
        (
            "binance",
            "SPOT",
            [SOURCE_MS, "1", "3", ".5", "2", "4", SOURCE_MS + 59999, "8", 2, "1", "2"],
            "4",
            "8",
        ),
        (
            "binance",
            "PERP_LINEAR",
            [SOURCE_MS, "1", "3", ".5", "2", "4", SOURCE_MS + 59999, "8", 2, "1", "2"],
            "4",
            "8",
        ),
        ("okx", "SPOT", [str(SOURCE_MS), "1", "3", ".5", "2", "4", "4", "8", "1"], "4", "8"),
        ("okx", "PERP_LINEAR", [str(SOURCE_MS), "1", "3", ".5", "2", "4", "4", "8", "1"], "8", "8"),
        ("bybit", "SPOT", [str(SOURCE_MS), "1", "3", ".5", "2", "4", "8"], "4", "8"),
        ("bybit", "PERP_LINEAR", [str(SOURCE_MS), "1", "3", ".5", "2", "4", "8"], "4", "8"),
        ("coinbase", "SPOT", [SOURCE_MS // 1000, ".5", "3", "1", "2", "4"], "4", None),
        ("kraken", "SPOT", [SOURCE_MS // 1000, "1", "3", ".5", "2", "2", "4", 2], "4", "8"),
        (
            "hyperliquid",
            "PERP_LINEAR",
            {
                "t": SOURCE_MS,
                "T": SOURCE_MS + 59999,
                "o": "1",
                "h": "3",
                "l": ".5",
                "c": "2",
                "v": "4",
                "n": 2,
            },
            "4",
            None,
        ),
    ],
)
def test_provider_candle_fields_reach_normalized_native_values_with_actual_declared_clocks(
    venue: str, market: str, provider_row: JsonValue, volume: str, quote: str | None
) -> None:
    assert isinstance(provider_row, (list, dict))
    wire = instrument()
    wire.update(venue=venue, market_type=market, contract_multiplier="2")
    receipt = SOURCE_TIME + timedelta(minutes=1)
    row = normalize.kline(wire, provider_row, now=receipt)
    assert (row["o"], row["h"], row["l"], row["c"]) == ("1", "3", "0.5", "2")
    assert row["volume_native"] == volume
    assert row["quote_volume"] == quote
    assert row["complete"] is True
    assert row["interval_s"] == 60
    assert (row["open_time"], row["open_time_prec"]) == (iso(SOURCE_TIME), "BUCKET(60s)")
    assert (row["received_ts"], row["received_ts_prec"]) == (iso(receipt), "COLLECTION_ONLY")
    assert row["quote_usd_rate"] is row["quote_volume_usd"] is None
