from decimal import Decimal

import pytest

from tank.market import normalize as n
from tank.market.books import OrderBook
from tank.market.flow import FlowAccumulator

TS = 1781654400000


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


def test_bybit_oi_no_division_fallback_and_runtime_semantics():
    i = instrument("bybit", "PERP_LINEAR")
    with pytest.raises(n.SemanticError, match="cannot substitute"):
        n.oi(i, {"openInterest": "200", "timestamp": TS})
    raw = {"openInterest": "200", "singleOpenInterest": "100", "timestamp": TS}
    row = n.oi(i, raw, mark="2", mark_ts=TS)
    assert row["oi_native"] == "100" and row["oi_usd"] == "200.00200"
    assert row["semantic_confidence"] == "UNKNOWN" and not row["synchronized_eligible"]
    i["semantic_verified_keys"] = ["bybit.linear.open-interest"]
    assert n.oi(i, raw)["synchronized_eligible"]


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
    # ask price0.0000002->2,size0.0010->10; bid0.0000001->1,size0.0100->100
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
