"""Validate exchange catalog envelopes before making instrument observations."""

from typing import Annotated, Literal, Self

from pydantic import AfterValidator, Field, JsonValue, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.json import JsonDocument, decode_unique_json
from tank.contracts.source_checkpoints import (
    CoinbaseInstrument,
    CoinbaseRestrictionFlags,
    InstrumentRecord,
    InstrumentSnapshot,
    VenueInstrument,
)
from tank.contracts.sources import FeedParsingSpec


class BybitSpotResult(DomainModel):
    category: Literal["spot"]
    list: list[JsonValue]
    nextPageCursor: str = ""

    @field_validator("nextPageCursor")
    @classmethod
    def no_spot_pagination(cls, value: str) -> str:
        if value != "":
            raise ValueError("Bybit spot instruments do not support pagination")
        return value


class BybitPerpetualResult(DomainModel):
    category: Literal["linear"]
    list: list[JsonValue]
    nextPageCursor: str


class BybitSpotEnvelope(DomainModel):
    retCode: int
    result: BybitSpotResult


class BybitPerpetualEnvelope(DomainModel):
    retCode: int
    result: BybitPerpetualResult


class OkxEnvelope(DomainModel):
    code: str
    data: list[JsonValue]


class KrakenEnvelope(DomainModel):
    error: list[str]
    result: dict[str, JsonValue]


class BinanceEnvelope(DomainModel):
    symbols: list[JsonValue]


class HyperliquidEnvelope(DomainModel):
    universe: list[JsonValue]


class InstrumentPageResult(DomainModel):
    snapshot: InstrumentSnapshot
    next_cursor: str | None


def project(value: JsonValue, names: tuple[str, ...]) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise ValueError("Instrument protocol requires an object")
    return {name: value[name] for name in names if name in value}


def rows(value: JsonValue) -> list[JsonValue]:
    if not isinstance(value, list):
        raise ValueError("Instrument protocol requires an array")
    return value


def nonblank(value: str) -> str:
    if not value.strip():
        raise ValueError("Instrument protocol identity/status must be nonempty")
    return value


def protocol_identity(value: str) -> str:
    nonblank(value)
    if any(character.isspace() or not character.isprintable() for character in value):
        raise ValueError("Instrument protocol identity cannot contain whitespace or control characters")
    return value


type WireText = Annotated[str, Field(min_length=1), AfterValidator(nonblank)]
type WireIdentity = Annotated[str, Field(min_length=1), AfterValidator(protocol_identity)]
type Venue = Literal["binance", "coinbase", "kraken", "okx", "bybit", "hyperliquid"]
type MarketType = Literal["SPOT", "PERP_LINEAR"]


class BinanceSpotProjection(DomainModel):
    symbol: WireIdentity
    baseAsset: WireIdentity
    quoteAsset: WireIdentity
    status: WireText


class BinancePerpetualProjection(BinanceSpotProjection):
    contractType: WireIdentity


class BybitSpotProjection(DomainModel):
    symbol: WireIdentity
    baseCoin: WireIdentity
    quoteCoin: WireIdentity
    status: WireText


class BybitPerpetualProjection(BybitSpotProjection):
    contractType: WireIdentity


class KrakenProjection(DomainModel):
    base: WireIdentity
    quote: WireIdentity
    status: WireText


class OkxSpotProjection(DomainModel):
    instId: WireIdentity
    state: WireText
    baseCcy: WireIdentity
    quoteCcy: WireIdentity
    ctType: str | None = None

    @model_validator(mode="after")
    def declared_contract_fact(self) -> Self:
        if "ctType" in self.model_fields_set and self.ctType is None:
            raise ValueError("Declared OKX contract metadata cannot be null")
        return self


class OkxPerpetualProjection(DomainModel):
    instId: WireIdentity
    state: WireText
    ctType: WireIdentity
    baseCcy: str | None = None
    quoteCcy: str | None = None

    @model_validator(mode="after")
    def declared_currency_facts(self) -> Self:
        if ("baseCcy" in self.model_fields_set and self.baseCcy is None) or (
            "quoteCcy" in self.model_fields_set and self.quoteCcy is None
        ):
            raise ValueError("Declared OKX currency metadata cannot be null")
        return self


class CoinbaseProjection(DomainModel):
    id: WireIdentity
    base_currency: WireIdentity
    quote_currency: WireIdentity
    status: WireText
    post_only: bool
    limit_only: bool
    cancel_only: bool
    auction_mode: bool
    trading_disabled: bool | None = None

    @model_validator(mode="after")
    def actual_restriction_facts(self) -> Self:
        if "trading_disabled" in self.model_fields_set and self.trading_disabled is None:
            raise ValueError("Declared Coinbase trading restriction cannot be null")
        if (
            sum(
                value is True
                for value in (self.trading_disabled, self.cancel_only, self.post_only, self.limit_only)
            )
            > 1
        ):
            raise ValueError("Coinbase response declares contradictory trading restrictions")
        return self


class HyperliquidProjection(DomainModel):
    name: WireIdentity
    isDelisted: bool | None = None
    quote: str | None = None

    @model_validator(mode="after")
    def actual_declared_metadata(self) -> Self:
        if "isDelisted" in self.model_fields_set and self.isDelisted is None:
            raise ValueError("Declared Hyperliquid delisting flag cannot be null")
        if "quote" in self.model_fields_set and self.quote is None:
            raise ValueError("Declared Hyperliquid quote metadata cannot be null")
        return self


def normalise(
    venue: Venue, market_type: MarketType, raw: JsonValue, native_symbol: str | None
) -> InstrumentRecord:
    if venue == "binance":
        names = ("symbol", "baseAsset", "quoteAsset", "status")
        contract: str | None = None
        row: BinanceSpotProjection
        if market_type == "PERP_LINEAR":
            perpetual = BinancePerpetualProjection.model_validate(project(raw, (*names, "contractType")))
            row, contract = perpetual, perpetual.contractType
        else:
            row = BinanceSpotProjection.model_validate(project(raw, names))
        return VenueInstrument(
            venue=venue,
            market_type=market_type,
            symbol=row.symbol,
            base=row.baseAsset,
            quote=row.quoteAsset,
            status=row.status,
            contract_type=contract,
        )
    if venue == "bybit":
        names = ("symbol", "baseCoin", "quoteCoin", "status")
        bybit_contract: str | None = None
        bybit: BybitSpotProjection
        if market_type == "PERP_LINEAR":
            bybit_perpetual = BybitPerpetualProjection.model_validate(project(raw, (*names, "contractType")))
            bybit, bybit_contract = bybit_perpetual, bybit_perpetual.contractType
        else:
            bybit = BybitSpotProjection.model_validate(project(raw, names))
        return VenueInstrument(
            venue=venue,
            market_type=market_type,
            symbol=bybit.symbol,
            base=bybit.baseCoin,
            quote=bybit.quoteCoin,
            status=bybit.status,
            contract_type=bybit_contract,
        )
    if venue == "kraken":
        kraken = KrakenProjection.model_validate(project(raw, ("base", "quote", "status")))
        if native_symbol is None:
            raise ValueError("Kraken instrument requires its actual result key")
        return VenueInstrument(
            venue=venue,
            market_type=market_type,
            symbol=protocol_identity(native_symbol),
            base=kraken.base,
            quote=kraken.quote,
            status=kraken.status,
            contract_type=None,
        )
    if venue == "okx":
        okx: OkxSpotProjection | OkxPerpetualProjection
        if market_type == "SPOT":
            okx = OkxSpotProjection.model_validate(
                project(raw, ("instId", "state", "baseCcy", "quoteCcy", "ctType"))
            )
        else:
            okx = OkxPerpetualProjection.model_validate(
                project(raw, ("instId", "state", "baseCcy", "quoteCcy", "ctType"))
            )
        return VenueInstrument(
            venue=venue,
            market_type=market_type,
            symbol=okx.instId,
            base=okx.baseCcy,
            quote=okx.quoteCcy,
            status=okx.state,
            contract_type=okx.ctType,
        )
    if venue == "coinbase":
        coinbase = CoinbaseProjection.model_validate(
            project(
                raw,
                (
                    "id",
                    "base_currency",
                    "quote_currency",
                    "status",
                    "post_only",
                    "limit_only",
                    "cancel_only",
                    "auction_mode",
                    "trading_disabled",
                ),
            )
        )
        flags = CoinbaseRestrictionFlags(
            post_only=coinbase.post_only,
            limit_only=coinbase.limit_only,
            cancel_only=coinbase.cancel_only,
            auction_mode=coinbase.auction_mode,
        )
        if coinbase.trading_disabled is not None:
            flags = CoinbaseRestrictionFlags.model_validate(
                flags.model_copy(update={"trading_disabled": coinbase.trading_disabled})
            )
        return CoinbaseInstrument(
            venue=venue,
            market_type=market_type,
            symbol=coinbase.id,
            base=coinbase.base_currency,
            quote=coinbase.quote_currency,
            status="TRADING_DISABLED" if coinbase.trading_disabled is True else coinbase.status,
            contract_type=None,
            status_flags=flags,
        )
    hyperliquid = HyperliquidProjection.model_validate(project(raw, ("name", "isDelisted", "quote")))
    return VenueInstrument(
        venue=venue,
        market_type=market_type,
        symbol=hyperliquid.name,
        base=hyperliquid.name,
        quote=hyperliquid.quote,
        status="DELISTED" if hyperliquid.isDelisted is True else "TRADING",
        contract_type=None,
    )


def parse_instrument_page(payload: bytes | JsonValue, feed: FeedParsingSpec) -> InstrumentPageResult:
    data = (
        decode_unique_json(payload)
        if isinstance(payload, (bytes, str))
        else JsonDocument.validate_value(payload)
    )
    venue = feed.venue
    market_type = feed.market_type
    if market_type is None:
        raise ValueError("Instrument feed requires a market type")
    if venue in {"coinbase", "kraken"} and market_type != "SPOT":
        raise ValueError("This instrument adapter implements only the spot catalog")
    if venue == "hyperliquid" and market_type != "PERP_LINEAR":
        raise ValueError("This instrument adapter implements only the Hyperliquid perpetual catalog")
    cursor: str | None = None
    keyed: dict[str, JsonValue] | None = None
    if venue == "bybit":
        outer = project(data, ("retCode", "result"))
        if "result" in outer:
            outer["result"] = project(outer["result"], ("category", "list", "nextPageCursor"))
        envelope = (
            BybitSpotEnvelope.model_validate(outer)
            if market_type == "SPOT"
            else BybitPerpetualEnvelope.model_validate(outer)
        )
        if envelope.retCode != 0:
            raise ValueError("Bybit instrument API refused the request")
        entries = envelope.result.list
        cursor = envelope.result.nextPageCursor or None
    elif venue == "okx":
        okx = OkxEnvelope.model_validate(project(data, ("code", "data")))
        if okx.code != "0":
            raise ValueError("OKX instrument API refused the request")
        entries = okx.data
    elif venue == "kraken":
        kraken = KrakenEnvelope.model_validate(project(data, ("error", "result")))
        if kraken.error:
            raise ValueError("Kraken instrument API refused the request")
        keyed = kraken.result
        entries = list(keyed.values())
    elif venue == "binance":
        if isinstance(data, dict) and "code" in data:
            raise ValueError("Binance instrument response is an error envelope")
        entries = BinanceEnvelope.model_validate(project(data, ("symbols",))).symbols
    elif venue == "coinbase":
        entries = rows(data)
    elif venue == "hyperliquid":
        entries = HyperliquidEnvelope.model_validate(project(data, ("universe",))).universe
    else:
        raise ValueError("Unknown instrument venue")
    result: dict[str, InstrumentRecord] = {}
    external_rows = list(keyed.items()) if keyed is not None else [(None, entry) for entry in entries]
    for native_symbol, entry in external_rows:
        observed = normalise(venue, market_type, entry, native_symbol)
        if observed.symbol in result:
            raise ValueError("Instrument response contains duplicate symbols")
        result[observed.symbol] = observed
    return InstrumentPageResult(snapshot=InstrumentSnapshot(root=result), next_cursor=cursor)
