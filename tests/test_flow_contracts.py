from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.market.flow import FlowAccumulator
from tank.market.trade_contracts import (
    ActiveFlowBar,
    EmptyFlowBar,
    FlowIdentity,
    NormalizedTrade,
    TradeSide,
)
from tank.util import iso

AT = datetime(2026, 10, 24, 12, tzinfo=UTC)
OBSERVED = datetime(2026, 10, 25, 12, tzinfo=UTC)


def trade_fixture(
    number: int, second: int, price: Decimal, qty: Decimal, usd: Decimal | None, side: TradeSide
) -> NormalizedTrade:
    return NormalizedTrade(
        instrument_id="A",
        token_id="T",
        venue="binance",
        market_type="SPOT",
        quote_asset="USDT",
        venue_symbol="TUSDT",
        trade_id=str(number),
        source_payload_json='{"source":"fixture"}',
        ts=AT + timedelta(seconds=second),
        ts_prec="EXACT",
        ts_prec_reason=None,
        received_ts=OBSERVED,
        received_ts_prec="COLLECTION_ONLY",
        received_ts_prec_reason=None,
        side=side,
        qty_native=qty,
        price=price,
        quote_notional=qty * price,
        notional_usd=usd,
        usd_reason=None if usd is not None else "QUOTE_USD_RATE_UNAVAILABLE",
        metric_key="binance.spot.aggTrade",
        trade_aggregation="PER_TAKER_ORDER_SAME_PRICE",
        raw_first_id=number,
        raw_last_id=number,
        sequence=None,
        sequence_semantics="N_A",
        capture_source="ARCHIVE",
    )


def test_flow_preserves_sub_cent_financial_values_and_actual_trade_order() -> None:
    accumulator = FlowAccumulator(now=lambda: OBSERVED)
    accumulator.add(
        trade_fixture(
            1, 40, Decimal("10000000000000000.02"), Decimal(3), Decimal("30000000000000000.06"), "SELL"
        )
    )
    accumulator.add(
        trade_fixture(
            2, 10, Decimal("10000000000000000.01"), Decimal(2), Decimal("20000000000000000.02"), "BUY"
        )
    )
    bar = accumulator.record(("A", iso(AT)), complete=True)
    assert isinstance(bar, ActiveFlowBar)
    assert bar.open == Decimal("10000000000000000.01")
    assert bar.close == Decimal("10000000000000000.02")
    assert bar.vwap == Decimal("10000000000000000.016")
    assert bar.volume_usd == Decimal("50000000000000000.08")
    assert bar.sum_sq_qty == Decimal(13)
    assert bar.first_trade_id == "2" and bar.last_trade_id == "1"
    assert bar.first_trade is not None and bar.first_trade.ts == AT + timedelta(seconds=10)
    assert bar.last_trade is not None and bar.last_trade.ts == AT + timedelta(seconds=40)


def test_missing_historical_fx_preserves_native_flow_without_false_usd() -> None:
    accumulator = FlowAccumulator(now=lambda: OBSERVED)
    accumulator.add(trade_fixture(1, 10, Decimal("2.5"), Decimal(3), None, "BUY"))
    accumulator.add(trade_fixture(2, 20, Decimal("2.5"), Decimal(2), None, "SELL"))
    bar = accumulator.record(("A", iso(AT)), complete=True)
    assert isinstance(bar, ActiveFlowBar) and bar.complete
    assert (bar.buy_qty, bar.sell_qty, bar.n_buy, bar.n_sell, bar.sum_sq_qty) == (
        Decimal(3),
        Decimal(2),
        1,
        1,
        Decimal(13),
    )
    assert bar.buy_notional is None and bar.sell_notional is None and bar.size_buckets is None
    assert bar.volume_usd is None and bar.notional_reason == "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"
    assert bar.null_reasons == {
        "buy_usd": "INCOMPLETE",
        "buy_notional": "INCOMPLETE",
        "sell_usd": "INCOMPLETE",
        "sell_notional": "INCOMPLETE",
        "size_buckets": "INCOMPLETE",
    }


def test_trade_id_gap_cannot_be_upgraded_to_complete() -> None:
    accumulator = FlowAccumulator(now=lambda: OBSERVED)
    accumulator.add(trade_fixture(1, 1, Decimal(1), Decimal(1), Decimal(1), "BUY"))
    accumulator.add(trade_fixture(3, 2, Decimal(1), Decimal(1), Decimal(1), "BUY"))
    bar = accumulator.record(("A", iso(AT)), complete=True)
    assert not bar.complete and bar.gap_reason == "TRADE_ID_GAP"


def test_boundary_proofs_use_latest_overlap_and_strictly_earlier_trade() -> None:
    accumulator = FlowAccumulator(now=lambda: OBSERVED)
    for number, second in enumerate((10, 20, 50), 1):
        accumulator.add(trade_fixture(number, second, Decimal(number), Decimal(1), Decimal(number), "BUY"))
    cutoff = AT + timedelta(seconds=20)
    accumulator.register_boundaries("A", [cutoff, AT + timedelta(minutes=1)])
    accumulator.prove_range(
        "A", AT, AT + timedelta(minutes=1), complete=True, source="VERIFIED_ARCHIVE", observed_at=OBSERVED
    )
    accumulator.prove_range(
        "A",
        cutoff,
        AT + timedelta(seconds=40),
        complete=False,
        source="STREAM_GAP",
        observed_at=OBSERVED,
        reason="DISCONNECTED",
    )
    bar = accumulator.record(("A", iso(AT)), complete=True)
    before = next(
        segment for segment in bar.boundary_segments if segment.start == AT and segment.end == cutoff
    )
    after = next(segment for segment in bar.boundary_segments if segment.start == cutoff)
    assert before.complete and before.buy_qty == Decimal(1)
    assert not after.complete and after.coverage_reason == "INCOMPLETE"
    reference = next(point for point in bar.reference_points if point.boundary == cutoff)
    assert reference.complete and reference.last_trade is not None
    assert reference.last_trade.trade_id == "1"
    terminal = next(point for point in bar.reference_points if point.boundary == AT + timedelta(minutes=1))
    assert not terminal.complete and terminal.coverage_reason == "INCOMPLETE"


def test_empty_minute_keeps_proven_predecessor_without_inventing_prices() -> None:
    accumulator = FlowAccumulator(now=lambda: OBSERVED)
    accumulator.add(trade_fixture(1, -30, Decimal(100), Decimal(2), None, "BUY"))
    accumulator.prove_range(
        "A", AT, AT + timedelta(minutes=1), complete=True, source="VERIFIED_ARCHIVE", observed_at=OBSERVED
    )
    accumulator.empty_minute(
        FlowIdentity(
            instrument_id="A", token_id="T", venue="binance", market_type="SPOT", quote_asset="USDT"
        ),
        AT,
    )
    bar = accumulator.record(("A", iso(AT)), complete=True)
    assert isinstance(bar, EmptyFlowBar) and bar.complete
    assert bar.buy_qty == bar.sell_qty == Decimal(0)
    assert bar.open is None and bar.close is None and bar.vwap is None and bar.last_trade is None
    point = bar.reference_points[0]
    assert point.complete and point.last_trade is not None
    assert point.last_trade.ts == AT - timedelta(seconds=30)
    wire = bar.model_dump(mode="json")
    assert wire["zero_trade_minute"] is True
    assert "trade_aggregation" not in wire and "notional_reason" not in wire


def test_bucket_thresholds_and_no_snapshot_request_have_explicit_behavior() -> None:
    observations: list[datetime] = []

    def now() -> datetime:
        observations.append(OBSERVED)
        return OBSERVED

    accumulator = FlowAccumulator(now=now)
    for number, usd in enumerate((999, 1000, 10000, 100000, 1000000), 1):
        assert (
            accumulator.add(
                trade_fixture(number, number, Decimal(1), Decimal(1), Decimal(usd), "BUY"), emit_record=False
            )
            is None
        )
    assert observations == []
    bar = accumulator.record(("A", iso(AT)))
    assert bar.size_buckets is not None
    assert [
        bucket.n
        for bucket in (
            bar.size_buckets.B1,
            bar.size_buckets.B2,
            bar.size_buckets.B3,
            bar.size_buckets.B4,
            bar.size_buckets.B5,
        )
    ] == [1, 1, 1, 1, 1]
    assert observations == [OBSERVED]


def test_mutated_trade_and_naive_processing_clock_are_refused() -> None:
    accumulator = FlowAccumulator(now=lambda: OBSERVED)
    trade = trade_fixture(1, 1, Decimal(1), Decimal(1), Decimal(1), "BUY")
    object.__setattr__(trade, "qty_native", Decimal("NaN"))
    with pytest.raises(ValidationError):
        accumulator.add(trade)
    assert accumulator.bars == {} and accumulator.last == {}
    naive = FlowAccumulator(now=lambda: datetime(2026, 10, 25, 12))
    with pytest.raises(ValueError, match="explicit timezone"):
        naive.add(trade_fixture(1, 1, Decimal(1), Decimal(1), Decimal(1), "BUY"))


def test_late_trade_fills_an_empty_minute_with_its_declared_aggregation() -> None:
    accumulator = FlowAccumulator(now=lambda: OBSERVED)
    accumulator.empty_minute(
        FlowIdentity(
            instrument_id="A", token_id="T", venue="binance", market_type="SPOT", quote_asset="USDT"
        ),
        AT,
    )
    accumulator.add(trade_fixture(1, 10, Decimal(2), Decimal(1), Decimal(2), "BUY"))
    bar = accumulator.record(("A", iso(AT)), complete=True)
    assert isinstance(bar, ActiveFlowBar)
    assert bar.trade_aggregation == "PER_TAKER_ORDER_SAME_PRICE" and bar.buy_qty == Decimal(1)
    assert "zero_trade_minute" not in bar.model_dump(mode="json")


def test_coverage_proof_coalescing_keeps_actual_latest_observation() -> None:
    accumulator = FlowAccumulator(now=lambda: OBSERVED)
    accumulator.prove_range(
        "A", AT, AT + timedelta(seconds=20), complete=True, source="VERIFIED_ARCHIVE", observed_at=OBSERVED
    )
    newest = OBSERVED + timedelta(seconds=1)
    accumulator.prove_range(
        "A", AT, AT + timedelta(seconds=40), complete=True, source="VERIFIED_ARCHIVE", observed_at=newest
    )
    assert len(accumulator.coverage_ranges["A"]) == 1
    assert accumulator.coverage_ranges["A"][0].observed_at == newest
    with pytest.raises(ValidationError, match="end precedes"):
        accumulator.prove_range(
            "A", AT + timedelta(minutes=1), AT, complete=True, source="VERIFIED_ARCHIVE", observed_at=OBSERVED
        )
    with pytest.raises(ValueError, match="aligned minute boundary"):
        accumulator.empty_minute(
            FlowIdentity(
                instrument_id="A", token_id="T", venue="binance", market_type="SPOT", quote_asset="USDT"
            ),
            AT + timedelta(seconds=1),
        )
