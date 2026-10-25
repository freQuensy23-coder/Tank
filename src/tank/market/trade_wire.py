"""Concrete provider trade wire projections; raw envelopes remain separate evidence."""

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import Field, JsonValue, ValidationError, field_validator

from tank.contracts import DomainModel
from tank.contracts.registry import RegistryVenue
from tank.market.errors import SemanticError
from tank.market.numbers import decimal
from tank.market.trade_contracts import RawTradeIdentifier, TradeAggregation, TradeSide
from tank.util import parse_epoch, parse_iso_time

type WireNumber = str | int | float
type TradeWire = dict[str, JsonValue] | list[JsonValue]


class BinanceTrade(DomainModel):
    m: bool
    q: WireNumber
    p: WireNumber
    T: WireNumber
    a: RawTradeIdentifier | None
    f: RawTradeIdentifier | None = None
    last_id: RawTradeIdentifier | None = Field(default=None, alias="l")


class CoinbaseTrade(DomainModel):
    side: Literal["buy", "sell"]
    size: WireNumber
    price: WireNumber
    time: str
    trade_id: RawTradeIdentifier | None


class OkxTrade(DomainModel):
    side: Literal["buy", "sell"]
    sz: WireNumber
    px: WireNumber
    ts: WireNumber
    tradeId: RawTradeIdentifier | None


class BybitWsTrade(DomainModel):
    S: Literal["Buy", "Sell"]
    v: WireNumber
    p: WireNumber
    T: WireNumber
    i: RawTradeIdentifier | None


class BybitRestTrade(DomainModel):
    side: Literal["Buy", "Sell"]
    size: WireNumber
    price: WireNumber
    time: WireNumber
    execId: RawTradeIdentifier | None


class KrakenWsTrade(DomainModel):
    price: WireNumber
    qty: WireNumber
    timestamp: str
    side: Literal["buy", "sell", "b", "s"]
    trade_id: RawTradeIdentifier | None


class KrakenRestTrade(DomainModel):
    price: WireNumber
    qty: WireNumber
    timestamp: WireNumber
    side: Literal["b", "s"]
    order_type: Literal["m", "l"]
    misc: str
    trade_id: RawTradeIdentifier | None


class HyperliquidTrade(DomainModel):
    side: Literal["B", "A"]
    sz: WireNumber
    px: WireNumber
    time: WireNumber
    tid: RawTradeIdentifier | None


class TradeSequence(DomainModel):
    seq: RawTradeIdentifier | None = None
    seqId: RawTradeIdentifier | None = None
    stream_sequence: RawTradeIdentifier | None = Field(default=None, alias="_stream_sequence")

    def value(self) -> RawTradeIdentifier | None:
        if "seq" in self.model_fields_set:
            return self.seq
        if "seqId" in self.model_fields_set:
            return self.seqId
        return self.stream_sequence


class DecodedTrade(DomainModel):
    trade_id: str = Field(min_length=1)
    reported_quantity: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    ts: datetime
    side: TradeSide
    aggregation: TradeAggregation
    raw_first_id: RawTradeIdentifier | None
    raw_last_id: RawTradeIdentifier | None
    sequence: RawTradeIdentifier | None

    @field_validator("ts")
    @classmethod
    def aware_source_clock(cls, value: datetime) -> datetime:
        return parse_iso_time(value)


def _project[T: DomainModel](
    schema: type[T],
    raw: dict[str, JsonValue],
    errors: dict[str, str] | None = None,
) -> T:
    document: dict[str, JsonValue] = {}
    for name, field in schema.model_fields.items():
        wire_name = field.alias if field.alias is not None else name
        if wire_name in raw:
            document[wire_name] = raw[wire_name]
    try:
        return schema.model_validate(document)
    except ValidationError as error:
        for issue in error.errors(include_input=False):
            location = issue["loc"]
            if location and isinstance(location[0], str):
                if issue["type"] == "missing":
                    raise KeyError(location[0]) from error
                if errors is not None and location[0] in errors:
                    raise SemanticError(errors[location[0]]) from error
        raise SemanticError(str(error)) from error


def _decoded(
    tid: RawTradeIdentifier | None,
    qty: WireNumber,
    price: WireNumber,
    timestamp: datetime,
    side: TradeSide,
    aggregation: TradeAggregation,
    metadata: TradeSequence,
    first: RawTradeIdentifier | None = None,
    last: RawTradeIdentifier | None = None,
) -> DecodedTrade:
    if tid is None:
        raise SemanticError("missing trade ID")
    quantity, value = decimal(qty), decimal(price)
    if quantity <= 0 or value <= 0:
        raise SemanticError("nonpositive trade")
    return DecodedTrade(
        trade_id=str(tid),
        reported_quantity=quantity,
        price=value,
        ts=timestamp,
        side=side,
        aggregation=aggregation,
        raw_first_id=first,
        raw_last_id=last,
        sequence=metadata.value(),
    )


def decode_trade(venue: RegistryVenue, raw: TradeWire) -> DecodedTrade:
    if isinstance(raw, list):
        if venue != "kraken":
            raise SemanticError("Trade payload must be a provider object")
        if len(raw) < 7:
            raise SemanticError("Kraken trade ID absent")
        record = KrakenRestTrade.model_validate(
            dict(
                zip(
                    ("price", "qty", "timestamp", "side", "order_type", "misc", "trade_id"),
                    raw[:7],
                    strict=True,
                )
            )
        )
        return _decoded(
            record.trade_id,
            record.qty,
            record.price,
            parse_epoch(record.timestamp, unit="s"),
            "BUY" if record.side == "b" else "SELL",
            "PER_FILL",
            TradeSequence(),
        )
    metadata = _project(TradeSequence, raw)
    match venue:
        case "binance":
            binance = _project(BinanceTrade, raw, {"m": "buyer-is-maker must be boolean"})
            return _decoded(
                binance.a,
                binance.q,
                binance.p,
                parse_epoch(binance.T, unit="ms"),
                "SELL" if binance.m else "BUY",
                "PER_TAKER_ORDER_SAME_PRICE",
                metadata,
                binance.f,
                binance.last_id,
            )
        case "coinbase":
            coinbase = _project(CoinbaseTrade, raw, {"side": "unknown Coinbase maker side"})
            return _decoded(
                coinbase.trade_id,
                coinbase.size,
                coinbase.price,
                parse_iso_time(coinbase.time),
                "BUY" if coinbase.side == "sell" else "SELL",
                "PER_FILL",
                metadata,
            )
        case "okx":
            okx = _project(OkxTrade, raw, {"side": "unknown OKX taker side"})
            return _decoded(
                okx.tradeId,
                okx.sz,
                okx.px,
                parse_epoch(okx.ts, unit="ms"),
                "BUY" if okx.side == "buy" else "SELL",
                "PER_TAKER_ORDER_SAME_PRICE",
                metadata,
            )
        case "bybit":
            if "S" in raw:
                bybit_ws = _project(BybitWsTrade, raw, {"S": "unknown Bybit taker side"})
                return _decoded(
                    bybit_ws.i,
                    bybit_ws.v,
                    bybit_ws.p,
                    parse_epoch(bybit_ws.T, unit="ms"),
                    "BUY" if bybit_ws.S == "Buy" else "SELL",
                    "PER_FILL",
                    metadata,
                )
            bybit_rest = _project(BybitRestTrade, raw, {"side": "unknown Bybit taker side"})
            return _decoded(
                bybit_rest.execId,
                bybit_rest.size,
                bybit_rest.price,
                parse_epoch(bybit_rest.time, unit="ms"),
                "BUY" if bybit_rest.side == "Buy" else "SELL",
                "PER_FILL",
                metadata,
            )
        case "kraken":
            kraken = _project(KrakenWsTrade, raw, {"side": "unknown Kraken taker side"})
            return _decoded(
                kraken.trade_id,
                kraken.qty,
                kraken.price,
                parse_iso_time(kraken.timestamp),
                "BUY" if kraken.side in {"buy", "b"} else "SELL",
                "PER_FILL",
                metadata,
            )
        case "hyperliquid":
            hyperliquid = _project(HyperliquidTrade, raw, {"side": "unknown Hyperliquid side"})
            return _decoded(
                hyperliquid.tid,
                hyperliquid.sz,
                hyperliquid.px,
                parse_epoch(hyperliquid.time, unit="ms"),
                "BUY" if hyperliquid.side == "B" else "SELL",
                "PER_FILL",
                metadata,
            )
        case _:
            raise SemanticError(f"unsupported venue {venue}")
