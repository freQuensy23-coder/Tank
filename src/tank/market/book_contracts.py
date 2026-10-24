"""Validated level-two quantities; checksum spellings are distinct from decimal values."""

from collections.abc import Mapping
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, JsonValue, field_validator, model_validator

from tank.contracts import DomainModel

type Number = str | int | float | Decimal


class BookInstrument(DomainModel):
    instrument_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    market_type: Literal["SPOT", "PERP_LINEAR"]
    venue: Literal["binance", "bybit", "okx", "coinbase", "kraken", "hyperliquid"]
    venue_symbol: str = Field(min_length=1)
    quote_asset: str | None = None
    quote_usd_rate: Decimal | None = Field(default=None, gt=0)
    contract_multiplier: Decimal | None = Field(default=None, gt=0)

    @field_validator("quote_usd_rate", "contract_multiplier", mode="before")
    @classmethod
    def decimal_from_wire(cls, value: Number | None) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError("boolean is not a decimal quantity")
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError("nonfinite instrument quantity")
        return result

    @classmethod
    def from_wire(cls, instrument: Mapping[str, JsonValue | Decimal]) -> Self:
        """Select book semantics from the larger instrument registry at its adapter boundary."""
        return cls.model_validate({key: instrument[key] for key in cls.model_fields if key in instrument})

    @model_validator(mode="after")
    def supported_market(self) -> Self:
        if self.venue in {"coinbase", "kraken"} and self.market_type != "SPOT":
            raise ValueError("venue has no supported perpetual order book")
        if self.venue == "hyperliquid" and self.market_type != "PERP_LINEAR":
            raise ValueError("Hyperliquid capture supports perpetual order books only")
        if self.venue == "okx" and self.market_type == "PERP_LINEAR" and self.contract_multiplier is None:
            raise ValueError("OKX perpetual book requires a verified base contract multiplier")
        return self

    @property
    def venue_key(self) -> str:
        venue = self.venue
        perp = self.market_type == "PERP_LINEAR"
        if venue.startswith("binance"):
            return "binance.usdm" if perp or venue.endswith("usdm") else "binance.spot"
        if venue.startswith("bybit"):
            return "bybit.linear" if perp else "bybit.spot"
        if venue.startswith("okx"):
            return "okx.swap" if perp else "okx.spot"
        if venue.startswith("hyperliquid"):
            return "hyperliquid.perp"
        return venue + ".spot"


class BookLevel(DomainModel):
    price: Decimal = Field(gt=0, allow_inf_nan=False)
    quantity: Decimal = Field(ge=0, allow_inf_nan=False)
    price_spelling: str
    quantity_spelling: str

    @classmethod
    def from_wire(cls, price: Number, quantity: Number) -> Self:
        if isinstance(price, bool) or isinstance(quantity, bool):
            raise ValueError("boolean is not a book quantity")
        p = format(price, "f") if isinstance(price, Decimal) else str(price)
        q = format(quantity, "f") if isinstance(quantity, Decimal) else str(quantity)
        return cls(price=Decimal(p), quantity=Decimal(q), price_spelling=p, quantity_spelling=q)


class DepthQuantity(DomainModel):
    native: Decimal = Field(ge=0, allow_inf_nan=False)
    usd: Decimal | None = Field(ge=0, allow_inf_nan=False)


class DepthBands(DomainModel):
    p0_5: DepthQuantity | None
    p1: DepthQuantity | None
    p2: DepthQuantity | None


class BookDepth(DomainModel):
    bid: DepthBands
    ask: DepthBands


class SideCoverage(DomainModel):
    bid: Decimal = Field(ge=0, allow_inf_nan=False)
    ask: Decimal = Field(ge=0, allow_inf_nan=False)


class BookSummary(DomainModel):
    instrument_id: str
    token_id: str
    exchange_ts: str
    exchange_ts_prec: str | None
    exchange_ts_prec_reason: str | None
    received_ts: str
    received_ts_prec: str
    received_ts_prec_reason: str | None
    mid: Decimal = Field(gt=0, allow_inf_nan=False)
    best_bid: Decimal = Field(gt=0, allow_inf_nan=False)
    best_ask: Decimal = Field(gt=0, allow_inf_nan=False)
    spread_bps: Decimal = Field(allow_inf_nan=False)
    depth: BookDepth
    raw_levels_range_pct: str
    raw_levels_scope: str
    quote_asset: str | None
    quote_usd_rate: Decimal | None
    range_covered_pct: SideCoverage
    levels_received: int = Field(gt=0)
    integrity_ok: bool
    integrity_method: str
    gap_reason: str | None
    bids: list[tuple[str, str]]
    asks: list[tuple[str, str]]
