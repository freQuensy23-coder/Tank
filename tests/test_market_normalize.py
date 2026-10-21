from decimal import Decimal

import pytest

from tank.market import normalize as n
from tank.market.books import OrderBook
from tank.market.flow import FlowAccumulator

TS = 1781654400000


def test_market_timestamp_precision_tracks_source_and_collection_clocks():
    row = n.liquidation(
        instrument("binance", "PERP_LINEAR"), {"S": "SELL", "z": "1", "ap": "2", "T": TS}, received_ts=TS + 12
    )
    assert row["ts_prec"] == "EXACT"
    assert row["received_ts_prec"] == "COLLECTION_ONLY"
    book = OrderBook(instrument("coinbase"))
    book.snapshot([["1", "2"]], [["2", "2"]], ts=TS, sequence=0, ts_prec="COLLECTION_ONLY")
    assert book.summary(received_ts=TS + 12)["exchange_ts_prec"] == "COLLECTION_ONLY"
    book.update([], [], sequence=1, ts=TS + 15)
    summary = book.summary(received_ts=TS + 20)
    assert summary["exchange_ts_prec"] == "EXACT"
    assert summary["received_ts_prec"] == "COLLECTION_ONLY"
    current = n.funding(
        instrument("bybit", "PERP_LINEAR"),
        {"fundingRate": "0.001"},
        kind="CURRENT_ESTIMATE",
        interval_h=8,
        received_ts=TS,
    )
    settled = n.funding(
        instrument("bybit", "PERP_LINEAR"),
        {"fundingRate": "0.001", "fundingRateTimestamp": TS},
        kind="SETTLED",
        interval_h=8,
        received_ts=TS + 12,
    )
    assert current["as_of_prec"] == "COLLECTION_ONLY"
    assert settled["as_of_prec"] == settled["settlement_time_prec"] == settled["funding_time_prec"] == "EXACT"
    assert current["received_ts_prec"] == settled["received_ts_prec"] == "COLLECTION_ONLY"
    candle = n.kline(
        instrument("binance"), [TS, "1", "1", "1", "1", "2", TS + 59999, "2", 2, "1", "1"], now=TS + 60000
    )
    assert candle["open_time_prec"] == "BUCKET(60s)" and candle["received_ts_prec"] == "COLLECTION_ONLY"
    bucket = n.oi(
        instrument("bybit", "PERP_LINEAR"), {"singleOpenInterest": "1", "timestamp": TS}, received_ts=TS + 12
    )
    assert bucket["ts_prec"] == "EXACT" and bucket["ts_bucket_size_s"] is None
    flow = FlowAccumulator()
    print_row = n.trade(instrument("binance"), {"a": 1, "p": "1", "q": "2", "T": TS, "m": False})
    bar = flow.add(print_row)
    assert bar["minute_start_prec"] == "BUCKET(60s)" and bar["received_ts_prec"] == "COLLECTION_ONLY"


def instrument(venue, market="SPOT", **kwargs):
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
            {"trade_id": 1, "price": "2", "size": "3", "time": TS, "side": "sell"},
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
def test_nine_trade_paths_exact_native_and_aggressor(venue, market, raw, side, qty):
    t = n.trade(instrument(venue, market), raw, received_ts=TS)
    assert t["side"] == side and t["qty_native"] == qty
    expected = (
        Decimal(qty)
        * Decimal(raw.get("p", raw.get("px", raw.get("price", "2"))) if isinstance(raw, dict) else "2")
        * Decimal("1.00001")
    )
    assert Decimal(t["notional_usd"]) == expected
    assert t["trade_id"] in ("1", "uuid")


def test_missing_or_future_usd_rate_never_assumed():
    i = instrument("binance")
    i.pop("quote_usd_rate")
    raw = {"a": 1, "p": "2", "q": "3", "T": TS, "m": False}
    assert n.trade(i, raw)["notional_usd"] is None
    i.update(quote_usd_rate="1", quote_usd_rate_as_of=TS + 1)
    assert n.trade(i, raw)["notional_usd"] is None


def test_bybit_oi_no_division_fallback_and_immediate_registry_eligibility():
    i = instrument("bybit", "PERP_LINEAR")
    with pytest.raises(n.SemanticError, match="cannot substitute"):
        n.oi(i, {"openInterest": "200", "timestamp": TS})
    raw = {"openInterest": "200", "singleOpenInterest": "100", "timestamp": TS}
    row = n.oi(i, raw, mark="2", mark_ts=TS)
    assert row["oi_native"] == "100" and row["oi_usd"] == "200.00200"
    assert "semantic_confidence" not in row
    assert row["ts_prec"] == "EXACT" and row["ts_bucket_size_s"] is None
    assert "ts_bucket_anchor" not in row
    assert row["synchronized_eligible"]


def test_hyperliquid_sidedness_never_eligible():
    i = instrument("hyperliquid", "PERP_LINEAR")
    i["semantic_verified_keys"] = ["hyperliquid.perp.ctx"]
    row = n.oi(i, {"openInterest": "10"}, received_ts=TS)
    assert row["sidedness"] == "UNKNOWN" and not row["synchronized_eligible"]


def test_okx_contract_multiplier_mandatory():
    i = instrument("okx", "PERP_LINEAR")
    i.pop("contract_multiplier")
    with pytest.raises(n.SemanticError):
        n.trade(i, {"tradeId": "1", "px": "2", "sz": "3", "ts": TS, "side": "buy"})


def test_binance_bool_not_string_and_coinbase_unknown_side():
    with pytest.raises(n.SemanticError):
        n.trade(instrument("binance"), {"a": 1, "p": "2", "q": "3", "T": TS, "m": "false"})
    with pytest.raises(n.SemanticError):
        n.trade(
            instrument("coinbase"), {"trade_id": 1, "side": "unknown", "price": "2", "size": "3", "time": TS}
        )


def test_funding_hourly_eight_hour_equivalent():
    r = n.funding(
        instrument("hyperliquid", "PERP_LINEAR"),
        {"funding": "0.0000125"},
        kind="CURRENT_ESTIMATE",
        interval_h=1,
        received_ts=TS,
    )
    assert r["rate_8h_eq"] == "0.0001000"


def test_liquidations_position_side_and_sampled_limits():
    b = n.liquidation(
        instrument("binance", "PERP_LINEAR"), {"o": {"S": "SELL", "q": "5", "z": "3", "ap": "2", "T": TS}}
    )
    y = n.liquidation(instrument("bybit", "PERP_LINEAR"), {"S": "Buy", "v": "3", "p": "2", "T": TS})
    assert b["side"] == y["side"] == "LONG_LIQ" and b["coverage"] == "SAMPLED" and y["coverage"] == "COMPLETE"
    assert y["price_kind"] == "BANKRUPTCY" and b["qty_native"] == "3"


def test_flow_exact_sum_sq_size_buckets_and_gap():
    flow = FlowAccumulator()
    i = instrument("binance")
    a = n.trade(i, {"a": 1, "p": "100000", "q": "2", "T": TS, "m": False})
    bar = flow.add(a)
    assert bar["size_buckets"]["B4"]["buy_qty"] == "2" and bar["sum_sq_qty"] == "4"
    b = n.trade(i, {"a": 3, "p": "100001", "q": "1", "T": TS + 1, "m": True})
    bar = flow.add(b)
    assert bar["gap_reason"] == "TRADE_ID_GAP" and bar["sum_sq_qty"] == "5"
    bar = flow.record((i["instrument_id"], bar["minute_start"]), complete=True)
    assert not bar["complete"]


def test_book_snapshot_sequence_gap_and_incomplete_depth():
    book = OrderBook(instrument("binance"))
    book.snapshot([["99", "10"]], [["101", "10"]], sequence=10, ts=TS)
    row = book.summary(received_ts=TS)
    assert row["depth"]["bid"]["p2"] is None
    assert row["spread_bps"] == "200.00"
    with pytest.raises(n.SemanticError, match="sequence"):
        book.update([["99", "11"]], [], sequence=13, first=12, ts=TS)
    assert not book.integrity_ok


def test_okx_checksum_uses_original_decimal_strings():
    import zlib

    book = OrderBook(instrument("okx"))
    value = zlib.crc32(b"99.00:2.0:101.00:3.0")
    value = value if value < 2**31 else value - 2**32
    book.snapshot([["99.00", "2.0"]], [["101.00", "3.0"]], sequence=1, ts=TS, checksum=value)
    assert book.integrity_ok
    with pytest.raises(n.SemanticError, match="checksum"):
        book.verify(0)


def test_kraken_checksum_ask_then_bid():
    import zlib

    book = OrderBook(instrument("kraken"))
    crc = zlib.crc32(b"1010030990020")
    book.snapshot([["99.00", "2.0"]], [["101.00", "3.0"]], sequence=1, ts=TS, checksum=crc)
    assert book.integrity_ok


def test_kline_taker_fields_never_invented_and_ongoing_incomplete():
    r = n.kline(instrument("coinbase"), [TS / 1000, "1", "3", "2", "2.5", "4"], now=TS + 30000)
    assert not r["complete"] and r["taker_buy_native"] is None and r["n_trades"] is None


def test_current_okx_zero_checksum_uses_sequences_and_handles_sequence_reset():
    book = OrderBook(instrument("okx"))
    book.snapshot([["99", "2"]], [["101", "3"]], sequence=100, ts=TS, checksum=0)
    assert book.integrity_ok and "CHECKSUM_DEPRECATED" in book.integrity_method
    assert book.update([["99", "4"]], [], sequence=101, previous=100, ts=TS + 1, checksum=0)
    assert book.update([["99", "5"]], [], sequence=1, previous=101, ts=TS + 2, checksum=0)
    with pytest.raises(n.SemanticError, match="sequence"):
        book.update([], [], sequence=3, previous=2, ts=TS + 3, checksum=0)


def test_kraken_checksum_tiny_decimal_is_fixed_point():
    import zlib

    book = OrderBook(instrument("kraken"))
    zlib.crc32(b"20010100")

    book.snapshot(
        [{"price": Decimal("0.0000001"), "qty": Decimal("0.0100")}],
        [{"price": Decimal("0.0000002"), "qty": Decimal("0.0010")}],
        sequence=1,
        ts=TS,
    )
    expected = zlib.crc32(b"2101100")
    assert book.verify(expected)


def test_evm_precision_products_and_squared_flow_sizes_are_not_rounded_to28digits():
    from fractions import Fraction

    qty = "123456789012345678.123456789012345678"
    price = "12345.123456789012345678"
    normalized = n.trade(instrument("binance"), {"a": 1, "p": price, "q": qty, "T": TS, "m": False})
    assert Fraction(normalized["notional_usd"]) == Fraction(qty) * Fraction(price) * Fraction("1.00001")
    bar = FlowAccumulator().add(normalized)
    assert Fraction(bar["sum_sq_qty"]) == Fraction(qty) ** 2


@pytest.mark.parametrize(
    "venue,raw",
    [
        ("binance", {"S": "UNKNOWN", "q": "1", "ap": "2", "T": TS}),
        ("bybit", {"S": "UNKNOWN", "v": "1", "p": "2", "T": TS}),
        ("bybit", {"S": "Buy", "v": "-1", "p": "2", "T": TS}),
    ],
)
def test_unknown_liquidation_side_or_negative_quantity_is_not_guessed(venue, raw):
    with pytest.raises(n.SemanticError):
        n.liquidation(instrument(venue, "PERP_LINEAR"), raw)


def test_oi_canonical_precision_and_missing_exchange_timestamp_is_not_invented():
    inst = instrument("binance", "PERP_LINEAR")
    current = n.oi(inst, {"openInterest": "1", "time": TS})
    historical = n.oi(inst, {"sumOpenInterest": "1", "timestamp": TS})
    assert current["ts_prec"] == current["timestamp_precision"] == "EXACT"
    assert historical["ts_prec"] == historical["timestamp_precision"] == "BUCKET(300s)"
    with pytest.raises(n.SemanticError, match="source timestamp absent"):
        n.oi(inst, {"openInterest": "1"})


def test_native_only_flow_preserves_complete_native_evidence_and_explicit_null_reasons():
    inst = instrument("binance", "PERP_LINEAR")
    inst.pop("quote_usd_rate")
    raw = {"a": 1, "p": "2", "q": "3", "T": TS, "m": False}
    accumulator = FlowAccumulator()
    row = accumulator.add(n.trade(inst, raw))
    key = (inst["instrument_id"], row["minute_start"])
    bar = accumulator.record(key, complete=True)
    assert bar["complete"] and bar["buy_qty"] == "3" and bar["sum_sq_qty"] == "9"
    assert bar["buy_notional"] is None and bar["sell_notional"] is None and bar["size_buckets"] is None
    assert all(
        bar["null_reasons"][field] == "INCOMPLETE" for field in ("buy_usd", "sell_usd", "size_buckets")
    )
    assert bar["notional_reason"] == "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"


@pytest.mark.parametrize(
    "venue,raw",
    [
        ("binance", {"sumOpenInterest": "123.456", "timestamp": TS}),
        ("bybit", {"singleOpenInterest": "123.456", "timestamp": TS}),
    ],
)
def test_oi_verified_request_bucket_metadata_is_not_hardcoded(venue, raw):
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


def test_prints_are_valued_with_the_latest_quote_usd_sample_at_or_before_them():
    from datetime import UTC, datetime, timedelta

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

    def usd(at):
        raw = {"S": "Buy", "v": "2", "p": "100", "T": int(at.timestamp() * 1000), "i": "1"}
        return norm.trade(inst, raw)["notional_usd"]

    assert usd(t0 + timedelta(minutes=4, seconds=59)) == "199.800"
    assert usd(t0 + timedelta(minutes=5)) == "200.200"
    assert usd(t0 - timedelta(seconds=1)) is None
    assert usd(t0 + timedelta(hours=7)) is None
    kline = norm.kline(
        inst, [int((t0 + timedelta(minutes=3)).timestamp() * 1000), "1", "1", "1", "1", "2", "10"]
    )
    assert kline["quote_usd_rate"] == "0.999" and kline["quote_usd_rate_as_of"] == iso(t0)
