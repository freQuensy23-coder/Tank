"""Validated level-two quantities; checksum spellings are distinct from decimal values."""

from collections.abc import Mapping
from decimal import Decimal
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

type Number = str | int | float | Decimal


class BookInstrument(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    instrument_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    market_type: str = Field(min_length=1)
    venue: str = Field(min_length=1)
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

    @property
    def venue_key(self) -> str:
        venue = self.venue.lower()
        perp = self.market_type.lower() != "spot"
        if venue.startswith("binance"):
            return "binance.usdm" if perp or venue.endswith("usdm") else "binance.spot"
        if venue.startswith("bybit"):
            return "bybit.linear" if perp else "bybit.spot"
        if venue.startswith("okx"):
            return "okx.swap" if perp else "okx.spot"
        if venue.startswith("hyperliquid"):
            return "hyperliquid.perp"
        return venue.split(".")[0] + ".spot"


class BookLevel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

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


class DepthQuantity(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    native: Decimal = Field(ge=0, allow_inf_nan=False)
    usd: Decimal | None = Field(ge=0, allow_inf_nan=False)


class DepthBands(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    p0_5: DepthQuantity | None
    p1: DepthQuantity | None
    p2: DepthQuantity | None


class BookDepth(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    bid: DepthBands
    ask: DepthBands


class SideCoverage(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    bid: Decimal = Field(ge=0, allow_inf_nan=False)
    ask: Decimal = Field(ge=0, allow_inf_nan=False)


class BookSummary(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

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
    bids: list[list[str]]
    asks: list[list[str]]
