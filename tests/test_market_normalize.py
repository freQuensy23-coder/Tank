from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import JsonValue

from tank.contracts.registry import RegistryMarketType, RegistryVenue
from tank.market import normalize as n
from tank.market.books import OrderBook
from tank.market.flow import FlowAccumulator
from tank.market.trade_contracts import ActiveFlowBar, FlowInstrument, QuoteUsdValuation, TradeSide
from tank.market.trade_wire import TradeWire
from tank.util import iso, parse_epoch, parse_iso_time

TS = 1781654400000
RECEIPT = parse_epoch(TS + 100, unit="ms")
FIXTURE_FX = QuoteUsdValuation(
    rate=Decimal("1.00001"), as_of=None, basis="EXPLICIT_UNCLOCKED_INPUT", unavailable_reason=None
)
UNAVAILABLE_FX = QuoteUsdValuation(
    rate=None, as_of=None, basis="UNAVAILABLE", unavailable_reason="QUOTE_USD_RATE_UNAVAILABLE"
)


def trade_instrument(
    venue: RegistryVenue, market: RegistryMarketType = "SPOT", *, multiplier: Decimal | None = None
) -> FlowInstrument:
    return FlowInstrument(
        instrument_id=f"{venue}:{market}:ABC",
        token_id="test:ABC",
        venue=venue,
        market_type=market,
        venue_symbol="ABCUSDT",
        quote_asset="USDT",
        contract_multiplier=multiplier,
    )


def test_market_timestamp_precision_tracks_source_and_collection_clocks() -> None:
    row = n.liquidation(
        instrument("binance", "PERP_LINEAR"),
        {"S": "SELL", "z": "1", "ap": "2", "T": TS},
        received_ts=parse_epoch(TS + 12, unit="ms"),
    )
    assert row["ts_prec"] == "EXACT"
    assert row["received_ts_prec"] == "COLLECTION_ONLY"
    book = OrderBook(instrument("coinbase"))
    book.snapshot(
        [["1", "2"]], [["2", "2"]], ts=parse_epoch(TS, unit="ms"), sequence=0, ts_prec="COLLECTION_ONLY"
    )
    assert book.summary(received_ts=parse_epoch(TS + 12, unit="ms"))["exchange_ts_prec"] == "COLLECTION_ONLY"
    book.update([], [], sequence=1, ts=parse_epoch(TS + 15, unit="ms"))
    summary = book.summary(received_ts=parse_epoch(TS + 20, unit="ms"))
    assert summary["exchange_ts_prec"] == "EXACT"
    assert summary["received_ts_prec"] == "COLLECTION_ONLY"
    current = n.funding(
        instrument("bybit", "PERP_LINEAR"),
        {"fundingRate": "0.001"},
        kind="CURRENT_ESTIMATE",
        interval_h=8,
        received_ts=parse_epoch(TS, unit="ms"),
    )
    settled = n.funding(
        instrument("bybit", "PERP_LINEAR"),
        {"fundingRate": "0.001", "fundingRateTimestamp": TS},
        kind="SETTLED",
        interval_h=8,
        received_ts=parse_epoch(TS + 12, unit="ms"),
    )
    assert current["as_of_prec"] == "COLLECTION_ONLY"
    assert settled["as_of_prec"] == settled["settlement_time_prec"] == settled["funding_time_prec"] == "EXACT"
    assert current["received_ts_prec"] == settled["received_ts_prec"] == "COLLECTION_ONLY"
    candle = n.kline(
        instrument("binance"),
        [TS, "1", "1", "1", "1", "2", TS + 59999, "2", 2, "1", "1"],
        now=parse_epoch(TS + 60000, unit="ms"),
    )
    assert candle["open_time_prec"] == "BUCKET(60s)" and candle["received_ts_prec"] == "COLLECTION_ONLY"
    bucket = n.oi(
        instrument("bybit", "PERP_LINEAR"),
        {"singleOpenInterest": "1", "timestamp": TS},
        received_ts=parse_epoch(TS + 12, unit="ms"),
    )
    assert bucket["ts_prec"] == "EXACT" and bucket["ts_bucket_size_s"] is None
    flow = FlowAccumulator(now=lambda: RECEIPT)
    print_row = n.trade(
        trade_instrument("binance"),
        {"a": 1, "p": "1", "q": "2", "T": TS, "m": False},
        valuation=FIXTURE_FX,
        received_ts=RECEIPT,
        source="WS",
    )
    bar = flow.add(print_row)
    assert bar.minute_start_prec == "BUCKET(60s)" and bar.received_ts_prec == "COLLECTION_ONLY"


def instrument(venue: str, market: str = "SPOT", **kwargs: JsonValue) -> dict[str, JsonValue]:
    return dict(
        instrument_id=f"{venue}:{market}:ABC",
        token_id="test:ABC",
        venue=venue,
        market_type=market,
        venue_symbol="ABCUSDT",
        quote_asset="USDT",
        quote_usd_rate="1.00001",
        contract_multiplier="2",
        **kwargs,
    )


@pytest.mark.parametrize(
    "venue,market,raw,side,qty",
    [
        ("binance", "SPOT", {"a": 1, "p": "1.000000000000000001", "q": "3", "T": TS, "m": False}, "BUY", "3"),
        ("binance", "PERP_LINEAR", {"a": 1, "p": "2", "q": "3", "T": TS, "m": True}, "SELL", "3"),
        (
            "coinbase",
            "SPOT",
            {
                "trade_id": 1,
                "price": "2",
                "size": "3",
                "time": iso(parse_epoch(TS, unit="ms")),
                "side": "sell",
            },
            "BUY",
            "3",
        ),
        ("okx", "SPOT", {"tradeId": "1", "px": "2", "sz": "3", "ts": str(TS), "side": "buy"}, "BUY", "3"),
        (
            "okx",
            "PERP_LINEAR",
            {"tradeId": "1", "px": "2", "sz": "3", "ts": str(TS), "side": "sell"},
            "SELL",
            "6",
        ),
        ("bybit", "SPOT", {"i": "uuid", "p": "2", "v": "3", "T": TS, "S": "Buy"}, "BUY", "3"),
        (
            "bybit",
            "PERP_LINEAR",
            {"execId": "uuid", "price": "2", "size": "3", "time": TS, "side": "Sell"},
            "SELL",
            "3",
        ),
        ("kraken", "SPOT", ["2", "3", TS / 1000, "b", "m", "", 1], "BUY", "3"),
        (
            "hyperliquid",
            "PERP_LINEAR",
            {"tid": 1, "px": "2", "sz": "3", "time": TS, "side": "A"},
            "SELL",
            "3",
        ),
    ],
)
def test_nine_trade_paths_exact_native_and_aggressor(
    venue: RegistryVenue,
    market: RegistryMarketType,
    raw: TradeWire,
    side: TradeSide,
    qty: str,
) -> None:
    inst = trade_instrument(
        venue, market, multiplier=Decimal("2") if venue == "okx" and market == "PERP_LINEAR" else None
    )
    trade = n.trade(inst, raw, valuation=FIXTURE_FX, received_ts=RECEIPT, source="WS")
    expected_price = (
        Decimal("1.000000000000000001") if venue == "binance" and market == "SPOT" else Decimal("2")
    )
    assert trade.side == side and trade.qty_native == Decimal(qty) and trade.price == expected_price
    assert trade.notional_usd == Decimal(qty) * expected_price * Decimal("1.00001")
    assert trade.trade_id in {"1", "uuid"}


def test_missing_or_future_usd_rate_never_assumed() -> None:
    inst = trade_instrument("binance")
    raw: dict[str, JsonValue] = {"a": 1, "p": "2", "q": "3", "T": TS, "m": False}
    assert n.trade(inst, raw, valuation=UNAVAILABLE_FX, received_ts=RECEIPT, source="WS").notional_usd is None
    future = QuoteUsdValuation(
        rate=Decimal("1"),
        as_of=parse_epoch(TS + 1, unit="ms"),
        basis="HISTORICAL_SAMPLE",
        unavailable_reason=None,
    )
    with pytest.raises(n.SemanticError, match="newer than"):
        n.trade(inst, raw, valuation=future, received_ts=RECEIPT, source="WS")


def test_bybit_oi_no_division_fallback_and_immediate_registry_eligibility() -> None:
    i = instrument("bybit", "PERP_LINEAR")
    with pytest.raises(n.SemanticError, match="cannot substitute"):
        n.oi(i, {"openInterest": "200", "timestamp": TS})
    raw = {"openInterest": "200", "singleOpenInterest": "100", "timestamp": TS}
    row = n.oi(i, raw, mark="2", mark_ts=parse_epoch(TS, unit="ms"))
    assert row["oi_native"] == "100" and row["oi_usd"] == "200.00200"
    assert "semantic_confidence" not in row
    assert row["ts_prec"] == "EXACT" and row["ts_bucket_size_s"] is None
    assert "ts_bucket_anchor" not in row
    assert row["synchronized_eligible"]


def test_hyperliquid_sidedness_never_eligible() -> None:
    i = instrument("hyperliquid", "PERP_LINEAR")
    i["semantic_verified_keys"] = ["hyperliquid.perp.ctx"]
    row = n.oi(i, {"openInterest": "10"}, received_ts=parse_epoch(TS, unit="ms"))
    assert row["sidedness"] == "UNKNOWN" and not row["synchronized_eligible"]


def test_okx_contract_multiplier_mandatory() -> None:
    with pytest.raises(n.SemanticError, match="multiplier"):
        n.trade(
            trade_instrument("okx", "PERP_LINEAR"),
            {"tradeId": "1", "px": "2", "sz": "3", "ts": TS, "side": "buy"},
            valuation=FIXTURE_FX,
            received_ts=RECEIPT,
            source="WS",
        )


def test_binance_bool_not_string_and_coinbase_unknown_side() -> None:
    with pytest.raises(n.SemanticError, match="buyer-is-maker"):
        n.trade(
            trade_instrument("binance"),
            {"a": 1, "p": "2", "q": "3", "T": TS, "m": "false"},
            valuation=FIXTURE_FX,
            received_ts=RECEIPT,
            source="WS",
        )
    with pytest.raises(n.SemanticError, match="unknown Coinbase"):
        n.trade(
            trade_instrument("coinbase"),
            {"trade_id": 1, "side": "unknown", "price": "2", "size": "3", "time": iso(RECEIPT)},
            valuation=FIXTURE_FX,
            received_ts=RECEIPT,
            source="WS",
        )


def test_funding_hourly_eight_hour_equivalent() -> None:
    r = n.funding(
        instrument("hyperliquid", "PERP_LINEAR"),
        {"funding": "0.0000125"},
        kind="CURRENT_ESTIMATE",
        interval_h=1,
        received_ts=parse_epoch(TS, unit="ms"),
    )
    assert r["rate_8h_eq"] == "0.0001000"


def test_liquidations_position_side_and_sampled_limits() -> None:
    b = n.liquidation(
        instrument("binance", "PERP_LINEAR"), {"o": {"S": "SELL", "q": "5", "z": "3", "ap": "2", "T": TS}}
    )
    y = n.liquidation(instrument("bybit", "PERP_LINEAR"), {"S": "Buy", "v": "3", "p": "2", "T": TS})
    assert b["side"] == y["side"] == "LONG_LIQ" and b["coverage"] == "SAMPLED" and y["coverage"] == "COMPLETE"
    assert y["price_kind"] == "BANKRUPTCY" and b["qty_native"] == "3"


def test_flow_exact_sum_sq_size_buckets_and_gap() -> None:
    flow = FlowAccumulator(now=lambda: RECEIPT)
    inst = trade_instrument("binance")
    a = n.trade(
        inst,
        {"a": 1, "p": "100000", "q": "2", "T": TS, "m": False},
        valuation=FIXTURE_FX,
        received_ts=RECEIPT,
        source="WS",
    )
    bar = flow.add(a)
    assert bar.size_buckets is not None
    assert bar.size_buckets.B4.buy_qty == Decimal("2") and bar.sum_sq_qty == Decimal("4")
    b = n.trade(
        inst,
        {"a": 3, "p": "100001", "q": "1", "T": TS + 1, "m": True},
        valuation=FIXTURE_FX,
        received_ts=RECEIPT,
        source="WS",
    )
    bar = flow.add(b)
    assert bar.gap_reason == "TRADE_ID_GAP" and bar.sum_sq_qty == Decimal("5")
    assert not flow.record((inst.instrument_id, iso(bar.minute_start)), complete=True).complete


def test_book_snapshot_sequence_gap_and_incomplete_depth() -> None:
    book = OrderBook(instrument("binance"))
    book.snapshot([["99", "10"]], [["101", "10"]], sequence=10, ts=parse_epoch(TS, unit="ms"))
    row = book.summary(received_ts=parse_epoch(TS, unit="ms"))
    assert row["depth"]["bid"]["p2"] is None
    assert row["spread_bps"] == "200.00"
    with pytest.raises(n.SemanticError, match="sequence"):
        book.update([["99", "11"]], [], sequence=13, first=12, ts=parse_epoch(TS, unit="ms"))
    assert not book.integrity_ok


def test_okx_checksum_uses_original_decimal_strings() -> None:
    import zlib

    book = OrderBook(instrument("okx"))
    value = zlib.crc32(b"99.00:2.0:101.00:3.0")
    value = value if value < 2**31 else value - 2**32
    book.snapshot(
        [["99.00", "2.0"]], [["101.00", "3.0"]], sequence=1, ts=parse_epoch(TS, unit="ms"), checksum=value
    )
    assert book.integrity_ok
    with pytest.raises(n.SemanticError, match="checksum"):
        book.verify(0)


def test_kraken_checksum_ask_then_bid() -> None:
    import zlib

    book = OrderBook(instrument("kraken"))
    crc = zlib.crc32(b"1010030990020")
    book.snapshot(
        [["99.00", "2.0"]], [["101.00", "3.0"]], sequence=1, ts=parse_epoch(TS, unit="ms"), checksum=crc
    )
    assert book.integrity_ok


def test_kline_taker_fields_never_invented_and_ongoing_incomplete() -> None:
    r = n.kline(
        instrument("coinbase"), [TS / 1000, "1", "3", "2", "2.5", "4"], now=parse_epoch(TS + 30000, unit="ms")
    )
    assert not r["complete"] and r["taker_buy_native"] is None and r["n_trades"] is None


def test_current_okx_zero_checksum_uses_sequences_and_handles_sequence_reset() -> None:
    book = OrderBook(instrument("okx"))
    book.snapshot([["99", "2"]], [["101", "3"]], sequence=100, ts=parse_epoch(TS, unit="ms"), checksum=0)
    assert book.integrity_ok and "CHECKSUM_DEPRECATED" in book.integrity_method
    assert book.update(
        [["99", "4"]], [], sequence=101, previous=100, ts=parse_epoch(TS + 1, unit="ms"), checksum=0
    )
    assert book.update(
        [["99", "5"]], [], sequence=1, previous=101, ts=parse_epoch(TS + 2, unit="ms"), checksum=0
    )
    with pytest.raises(n.SemanticError, match="sequence"):
        book.update([], [], sequence=3, previous=2, ts=parse_epoch(TS + 3, unit="ms"), checksum=0)


def test_kraken_checksum_tiny_decimal_is_fixed_point() -> None:
    import zlib

    book = OrderBook(instrument("kraken"))
    zlib.crc32(b"20010100")

    book.snapshot(
        [{"price": Decimal("0.0000001"), "qty": Decimal("0.0100")}],
        [{"price": Decimal("0.0000002"), "qty": Decimal("0.0010")}],
        sequence=1,
        ts=parse_epoch(TS, unit="ms"),
    )
    expected = zlib.crc32(b"2101100")
    assert book.verify(expected)


def test_evm_precision_products_and_squared_flow_sizes_are_not_rounded_to28digits() -> None:
    from fractions import Fraction

    qty = "123456789012345678.123456789012345678"
    price = "12345.123456789012345678"
    normalized = n.trade(
        trade_instrument("binance"),
        {"a": 1, "p": price, "q": qty, "T": TS, "m": False},
        valuation=FIXTURE_FX,
        received_ts=RECEIPT,
        source="WS",
    )
    assert normalized.notional_usd is not None
    assert Fraction(normalized.notional_usd) == Fraction(qty) * Fraction(price) * Fraction("1.00001")
    bar = FlowAccumulator(now=lambda: RECEIPT).add(normalized)
    assert Fraction(bar.sum_sq_qty) == Fraction(qty) ** 2


@pytest.mark.parametrize(
    "venue,raw",
    [
        ("binance", {"S": "UNKNOWN", "q": "1", "ap": "2", "T": TS}),
        ("bybit", {"S": "UNKNOWN", "v": "1", "p": "2", "T": TS}),
        ("bybit", {"S": "Buy", "v": "-1", "p": "2", "T": TS}),
    ],
)
def test_unknown_liquidation_side_or_negative_quantity_is_not_guessed(venue, raw) -> None:
    with pytest.raises(n.SemanticError):
        n.liquidation(instrument(venue, "PERP_LINEAR"), raw)


def test_oi_canonical_precision_and_missing_exchange_timestamp_is_not_invented() -> None:
    inst = instrument("binance", "PERP_LINEAR")
    current = n.oi(inst, {"openInterest": "1", "time": TS})
    historical = n.oi(inst, {"sumOpenInterest": "1", "timestamp": TS})
    assert current["ts_prec"] == current["timestamp_precision"] == "EXACT"
    assert historical["ts_prec"] == historical["timestamp_precision"] == "BUCKET(300s)"
    with pytest.raises(n.SemanticError, match="source timestamp absent"):
        n.oi(inst, {"openInterest": "1"})


def test_native_only_flow_preserves_complete_native_evidence_and_explicit_null_reasons() -> None:
    inst = trade_instrument("binance", "PERP_LINEAR")
    raw: dict[str, JsonValue] = {"a": 1, "p": "2", "q": "3", "T": TS, "m": False}
    accumulator = FlowAccumulator(now=lambda: RECEIPT)
    snapshot = accumulator.add(n.trade(inst, raw, valuation=UNAVAILABLE_FX, received_ts=RECEIPT, source="WS"))
    bar = accumulator.record((inst.instrument_id, iso(snapshot.minute_start)), complete=True)
    assert isinstance(bar, ActiveFlowBar)
    assert bar.complete and bar.buy_qty == Decimal("3") and bar.sum_sq_qty == Decimal("9")
    assert bar.buy_notional is None and bar.sell_notional is None and bar.size_buckets is None
    assert (
        bar.null_reasons["buy_usd"]
        == bar.null_reasons["sell_usd"]
        == bar.null_reasons["size_buckets"]
        == "INCOMPLETE"
    )
    assert bar.notional_reason == "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"


@pytest.mark.parametrize(
    "venue,raw",
    [
        ("binance", {"sumOpenInterest": "123.456", "timestamp": TS}),
        ("bybit", {"singleOpenInterest": "123.456", "timestamp": TS}),
    ],
)
def test_oi_verified_request_bucket_metadata_is_not_hardcoded(venue, raw) -> None:
    inst = instrument(venue, "PERP_LINEAR")
    default = n.oi(inst, raw)
    hourly = n.oi(inst, raw, bucket_size_s=3600)
    if venue == "binance":
        assert default["ts_prec"] == "BUCKET(300s)"
        assert default["ts_bucket_size_s"] == 300
        assert hourly["ts_prec"] == "BUCKET(3600s)"
        assert hourly["ts_bucket_size_s"] == 3600
        assert hourly["ts_bucket_anchor"] == "END"
    else:
        assert default["ts_prec"] == hourly["ts_prec"] == "EXACT"
        assert default["ts_bucket_size_s"] is hourly["ts_bucket_size_s"] is None
        assert "ts_bucket_anchor" not in default and "ts_bucket_anchor" not in hourly
    assert hourly["ts"] == default["ts"] and hourly["oi_native"] == "123.456"
    for invalid in (0, -300, 0.5, True, 301):
        with pytest.raises(n.SemanticError, match="documented source request period"):
            n.oi(inst, raw, bucket_size_s=invalid)


def test_prints_are_valued_with_the_latest_quote_usd_sample_at_or_before_them() -> None:
    from datetime import UTC, timedelta

    from tank.market import normalize as norm
    from tank.util import iso

    inst = {
        "instrument_id": "bybit:spot:BTCUSDT",
        "token_id": "bitcoin:BTC",
        "venue": "bybit",
        "market_type": "SPOT",
        "venue_symbol": "BTCUSDT",
        "quote_asset": "USDT",
    }
    t0 = datetime(2026, 10, 21, 7, 0, tzinfo=UTC)
    norm.remember_quote_usd("USDT", [(iso(t0), "0.999"), (iso(t0 + timedelta(minutes=5)), "1.001")])

    inst.update(quote_usd_rate="1.001", quote_usd_rate_as_of=iso(t0 + timedelta(minutes=5)))

    identity = FlowInstrument(
        instrument_id="bybit:spot:BTCUSDT",
        token_id="bitcoin:BTC",
        venue="bybit",
        market_type="SPOT",
        venue_symbol="BTCUSDT",
        quote_asset="USDT",
        contract_multiplier=None,
    )

    def usd(at: datetime) -> Decimal | None:
        raw: dict[str, JsonValue] = {
            "S": "Buy",
            "v": "2",
            "p": "100",
            "T": int(at.timestamp() * 1000),
            "i": "1",
        }
        rate, as_of = norm.quote_usd_rate(inst, at)
        if rate is None:
            valuation = UNAVAILABLE_FX
        else:
            assert as_of is not None
            valuation = QuoteUsdValuation(
                rate=Decimal(rate),
                as_of=parse_iso_time(as_of),
                basis="HISTORICAL_SAMPLE",
                unavailable_reason=None,
            )
        return norm.trade(identity, raw, valuation=valuation, received_ts=at, source="WS").notional_usd

    assert usd(t0 + timedelta(minutes=4, seconds=59)) == Decimal("199.800")
    assert usd(t0 + timedelta(minutes=5)) == Decimal("200.200")
    assert usd(t0 - timedelta(seconds=1)) is None
    assert usd(t0 + timedelta(hours=7)) is None
    kline = norm.kline(
        inst, [int((t0 + timedelta(minutes=3)).timestamp() * 1000), "1", "1", "1", "1", "2", "10"]
    )
    assert kline["quote_usd_rate"] == "0.999" and kline["quote_usd_rate_as_of"] == iso(t0)


def test_declared_millisecond_wire_clock_preserves_zero_ids_and_raw_lexemes() -> None:
    raw: dict[str, JsonValue] = {
        "a": 0,
        "p": "2.000",
        "q": "3.00",
        "T": 1000,
        "m": False,
        "f": 0,
        "l": 0,
        "seq": 0,
        "provider_extra": {"retained": True},
    }
    row = n.trade(
        trade_instrument("binance"),
        raw,
        valuation=FIXTURE_FX,
        received_ts=parse_epoch(2000, unit="ms"),
        source="WS",
    )
    assert row.ts == parse_epoch(1, unit="s")
    assert row.trade_id == "0" and row.raw_first_id == row.raw_last_id == row.sequence == 0
    assert '"p":"2.000"' in row.source_payload_json and '"q":"3.00"' in row.source_payload_json
    assert '"provider_extra":{"retained":true}' in row.source_payload_json


@pytest.mark.parametrize("field", ["a", "f", "l", "seq"])
def test_boolean_wire_identifiers_are_rejected_instead_of_becoming_numbers(field: str) -> None:
    raw: dict[str, JsonValue] = {"a": 1, "p": "2", "q": "3", "T": TS, "m": False}
    raw[field] = True
    with pytest.raises(n.SemanticError):
        n.trade(trade_instrument("binance"), raw, valuation=FIXTURE_FX, received_ts=RECEIPT, source="WS")


def test_trade_identity_cannot_reinterpret_unsupported_market_as_spot() -> None:
    with pytest.raises(n.SemanticError, match="venue market"):
        n.trade(
            trade_instrument("coinbase", "PERP_LINEAR"),
            {"trade_id": 1, "side": "sell", "size": "3", "price": "2", "time": iso(RECEIPT)},
            valuation=FIXTURE_FX,
            received_ts=RECEIPT,
            source="WS",
        )


def test_book_without_provider_clock_preserves_unknown_exchange_time() -> None:
    book = OrderBook(instrument("binance"))
    book.snapshot([["99", "2"]], [["101", "3"]], sequence=1)
    receipt = parse_epoch(TS + 20, unit="ms")
    row = book.summary(received_ts=receipt)
    assert row["exchange_ts"] is None
    assert row["exchange_ts_prec"] is None
    assert row["exchange_ts_prec_reason"] is not None
    assert row["received_ts"] == iso(receipt)
    assert row["received_ts_prec"] == "COLLECTION_ONLY"
    book.update([], [], sequence=2, previous=1)
    assert book.summary(received_ts=receipt)["exchange_ts"] is None


def test_book_rejects_naive_provider_timestamp() -> None:
    book = OrderBook(instrument("binance"))
    with pytest.raises((TypeError, ValueError)):
        book.snapshot([["99", "2"]], [["101", "3"]], sequence=1, ts="2026-06-17T00:00:00")
