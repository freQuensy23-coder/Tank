"""Trade normalization and deterministic flow domains, independent of acquisition and storage."""

from datetime import datetime
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.contracts.registry import RegistryMarketType, RegistryVenue
from tank.util import parse_iso_time

type TradeSide = Literal["BUY", "SELL"]
type TradeAggregation = Literal["PER_FILL", "PER_TAKER_ORDER_SAME_PRICE"]
type TradeCaptureSource = Literal["WS", "REST", "ARCHIVE"]
type RawTradeIdentifier = str | int
type FlowKey = tuple[str, str]
type NullFlowField = Literal["buy_usd", "sell_usd", "buy_notional", "sell_notional", "size_buckets"]


class FlowDomainModel(DomainModel):
    @field_validator(
        "ts",
        "received_ts",
        "start",
        "end",
        "observed_at",
        "minute_start",
        "boundary",
        "as_of",
        check_fields=False,
    )
    @classmethod
    def declared_timezone(cls, value: datetime | None) -> datetime | None:
        return None if value is None else parse_iso_time(value)


class FlowIdentity(FlowDomainModel):
    instrument_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    venue: RegistryVenue
    market_type: RegistryMarketType
    quote_asset: str = Field(min_length=1)


class FlowInstrument(FlowIdentity):
    venue_symbol: str | None
    contract_multiplier: Decimal | None = Field(gt=0)


class QuoteUsdValuation(FlowDomainModel):
    rate: Decimal | None = Field(gt=0)
    as_of: datetime | None
    basis: Literal["USD_UNIT", "HISTORICAL_SAMPLE", "EXPLICIT_UNCLOCKED_INPUT", "UNAVAILABLE"]
    unavailable_reason: Literal["QUOTE_USD_RATE_UNAVAILABLE"] | None

    @model_validator(mode="after")
    def availability(self) -> Self:
        if self.rate is None:
            if self.basis != "UNAVAILABLE" or self.unavailable_reason is None or self.as_of is not None:
                raise ValueError("Unavailable quote valuation requires its reason and no occurrence clock")
        elif self.unavailable_reason is not None or self.basis == "UNAVAILABLE":
            raise ValueError("Available quote valuation cannot carry an unavailable reason")
        elif self.basis == "HISTORICAL_SAMPLE" and self.as_of is None:
            raise ValueError("Historical quote valuation requires its actual sample clock")
        elif self.basis != "HISTORICAL_SAMPLE" and self.as_of is not None:
            raise ValueError("Only historical quote valuations carry a sample clock")
        elif self.basis == "USD_UNIT" and self.rate != Decimal(1):
            raise ValueError("USD unit valuation must equal one")
        return self


class NormalizedTrade(FlowIdentity):
    venue_symbol: str | None
    trade_id: str = Field(min_length=1)
    source_payload_json: str
    ts: datetime
    ts_prec: Literal["EXACT"]
    ts_prec_reason: None
    received_ts: datetime
    received_ts_prec: Literal["COLLECTION_ONLY"]
    received_ts_prec_reason: None
    side: TradeSide
    qty_native: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    quote_notional: Decimal = Field(gt=0)
    notional_usd: Decimal | None = Field(gt=0)
    usd_reason: Literal["QUOTE_USD_RATE_UNAVAILABLE"] | None
    metric_key: str = Field(min_length=1)
    trade_aggregation: TradeAggregation
    raw_first_id: RawTradeIdentifier | None
    raw_last_id: RawTradeIdentifier | None
    sequence: RawTradeIdentifier | None
    sequence_semantics: Literal["MONOTONIC", "N_A"]
    capture_source: TradeCaptureSource

    @field_validator("source_payload_json")
    @classmethod
    def valid_raw_json(cls, value: str) -> str:
        decode_json(value)
        return value

    @model_validator(mode="after")
    def valuation_availability(self) -> Self:
        if (self.notional_usd is None) != (self.usd_reason is not None):
            raise ValueError("Trade USD availability requires an explicit matching reason")
        return self


class TradePoint(FlowDomainModel):
    trade_id: str
    price: Decimal = Field(gt=0)
    ts: datetime
    ts_prec: Literal["EXACT"]
    ts_prec_reason: None


class FlowBucket(DomainModel):
    buy_qty: Decimal = Field(ge=0)
    sell_qty: Decimal = Field(ge=0)
    buy_notional: Decimal = Field(ge=0)
    sell_notional: Decimal = Field(ge=0)
    n: int = Field(ge=0)


class FlowBuckets(DomainModel):
    B1: FlowBucket
    B2: FlowBucket
    B3: FlowBucket
    B4: FlowBucket
    B5: FlowBucket


class FlowMetrics(FlowDomainModel):
    buy_qty: Decimal = Field(ge=0)
    sell_qty: Decimal = Field(ge=0)
    buy_notional: Decimal | None = Field(ge=0)
    sell_notional: Decimal | None = Field(ge=0)
    n_buy: int = Field(ge=0)
    n_sell: int = Field(ge=0)
    sum_sq_qty: Decimal = Field(ge=0)
    open: Decimal | None = Field(gt=0)
    high: Decimal | None = Field(gt=0)
    low: Decimal | None = Field(gt=0)
    close: Decimal | None = Field(gt=0)
    vwap: Decimal | None = Field(gt=0)
    size_buckets: FlowBuckets | None
    first_trade: TradePoint | None
    last_trade: TradePoint | None

    @model_validator(mode="after")
    def native_and_price_evidence(self) -> Self:
        has_trades = self.buy_qty > 0 or self.sell_qty > 0
        if (self.buy_qty == 0) != (self.n_buy == 0) or (self.sell_qty == 0) != (self.n_sell == 0):
            raise ValueError("Native flow quantities require matching positive trade counts")
        prices = (self.open, self.high, self.low, self.close, self.vwap)
        if not has_trades:
            if (
                any(price is not None for price in prices)
                or self.first_trade is not None
                or self.last_trade is not None
            ):
                raise ValueError("An empty interval cannot invent trade prices or endpoints")
        elif any(price is None for price in prices) or self.first_trade is None or self.last_trade is None:
            raise ValueError("A traded interval requires its prices and endpoint trades")
        if (
            self.high is not None
            and self.low is not None
            and self.open is not None
            and self.close is not None
            and (self.high < max(self.open, self.close) or self.low > min(self.open, self.close))
        ):
            raise ValueError("Flow OHLC extrema do not contain its endpoint prices")
        if (self.buy_notional is None) != (self.sell_notional is None):
            raise ValueError("Incomplete historical FX invalidates both USD side totals")
        if (self.buy_notional is None) != (self.size_buckets is None):
            raise ValueError("USD size buckets require complete historical FX")
        return self


class FlowSnapshotBase(FlowMetrics, FlowIdentity):
    minute_start: datetime
    minute_start_prec: Literal["BUCKET(60s)"]
    minute_start_prec_reason: None
    received_ts: datetime
    received_ts_prec: Literal["COLLECTION_ONLY"]
    received_ts_prec_reason: None
    first_trade_id: str | None
    last_trade_id: str | None
    complete: bool
    gap_reason: str | None
    quote_volume: Decimal = Field(ge=0)
    volume_usd: Decimal | None = Field(ge=0)
    null_reasons: dict[NullFlowField, Literal["INCOMPLETE"]]

    @field_validator("minute_start")
    @classmethod
    def minute_alignment(cls, value: datetime) -> datetime:
        if value.second != 0 or value.microsecond != 0:
            raise ValueError("Flow minute start must align to a minute boundary")
        return value

    @model_validator(mode="after")
    def missing_usd_reasons(self) -> Self:
        expected: set[NullFlowField] = set()
        if self.buy_notional is None:
            expected.update(("buy_usd", "buy_notional", "sell_usd", "sell_notional", "size_buckets"))
        if set(self.null_reasons) != expected:
            raise ValueError("Flow null reasons must identify precisely its missing USD fields")
        if (self.volume_usd is None) != (self.buy_notional is None):
            raise ValueError("Flow USD volume availability must match its side totals")
        return self


class ActiveFlowSnapshot(FlowSnapshotBase):
    trade_aggregation: TradeAggregation
    notional_reason: Literal["HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"] | None

    @model_validator(mode="after")
    def active_trade_evidence(self) -> Self:
        if self.buy_qty == 0 and self.sell_qty == 0:
            raise ValueError("An active flow snapshot requires at least one native trade")
        if (self.notional_reason is None) != (self.buy_notional is not None):
            raise ValueError("Historical FX absence requires its explicit flow reason")
        return self


class EmptyFlowSnapshot(FlowSnapshotBase):
    zero_trade_minute: Literal[True]

    @model_validator(mode="after")
    def no_trade_evidence(self) -> Self:
        if (
            self.buy_qty != 0
            or self.sell_qty != 0
            or self.first_trade_id is not None
            or self.last_trade_id is not None
        ):
            raise ValueError("A zero-trade minute cannot contain trade quantities or identities")
        return self


type FlowSnapshot = ActiveFlowSnapshot | EmptyFlowSnapshot


class CoverageProof(FlowDomainModel):
    start: datetime
    end: datetime
    complete: bool
    source: str = Field(min_length=1)
    reason: str | None
    observed_at: datetime
    observed_at_prec: Literal["COLLECTION_ONLY"]
    observed_at_prec_reason: None

    @model_validator(mode="after")
    def ordered_bounds(self) -> Self:
        if self.end < self.start:
            raise ValueError("Coverage proof end precedes its start")
        return self


class FlowBoundary(FlowDomainModel):
    start: datetime
    start_prec: Literal["EXACT"]
    start_prec_reason: None
    end: datetime
    end_prec: Literal["EXACT"]
    end_prec_reason: None
    complete: bool
    observed_at: datetime
    observed_at_prec: Literal["COLLECTION_ONLY"]
    observed_at_prec_reason: None
    coverage_reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def segment_bounds_and_coverage(self) -> Self:
        if self.end <= self.start:
            raise ValueError("A boundary segment requires a positive interval")
        if self.complete != (self.coverage_reason is None):
            raise ValueError("Boundary coverage requires its matching explicit reason")
        return self


class ActiveBoundarySegment(ActiveFlowSnapshot, FlowBoundary):
    pass


class EmptyBoundarySegment(FlowMetrics, FlowBoundary):
    instrument_id: str


type BoundarySegment = ActiveBoundarySegment | EmptyBoundarySegment


class FlowReferencePoint(FlowDomainModel):
    instrument_id: str
    boundary: datetime
    boundary_prec: Literal["EXACT"]
    boundary_prec_reason: None
    last_trade: TradePoint | None
    complete: bool
    observed_at: datetime
    observed_at_prec: Literal["COLLECTION_ONLY"]
    observed_at_prec_reason: None
    coverage_reason: Literal["INCOMPLETE"] | None

    @model_validator(mode="after")
    def reference_evidence(self) -> Self:
        if self.last_trade is not None and self.last_trade.ts >= self.boundary:
            raise ValueError("Reference trade must strictly precede its boundary")
        if self.complete and self.last_trade is None:
            raise ValueError("A complete reference requires an observed predecessor trade")
        if self.complete != (self.coverage_reason is None):
            raise ValueError("Reference coverage requires its matching explicit reason")
        return self


class ActiveFlowBar(ActiveFlowSnapshot):
    boundary_segments: list[BoundarySegment]
    reference_points: list[FlowReferencePoint]


class EmptyFlowBar(EmptyFlowSnapshot):
    boundary_segments: list[BoundarySegment]
    reference_points: list[FlowReferencePoint]


type FlowBar = ActiveFlowBar | EmptyFlowBar
