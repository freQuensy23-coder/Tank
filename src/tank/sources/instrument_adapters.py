"""Validate exchange catalog envelopes before making instrument observations."""

from pydantic import ConfigDict, JsonValue, TypeAdapter

from tank.contracts import DomainModel
from tank.contracts.json import JsonDocument, decode_json
from tank.contracts.source_checkpoints import InstrumentSnapshot
from tank.contracts.sources import FeedParsingSpec


class BybitSpotResult(DomainModel):
    list: list[JsonValue]
    nextPageCursor: str = ""


class BybitPerpetualResult(DomainModel):
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


def optional_text(row: dict[str, JsonValue], name: str) -> str | None:
    if name not in row:
        return None
    value = row[name]
    if not isinstance(value, str):
        raise ValueError("Declared instrument text must be a string")
    return value


def parse_instrument_page(payload: bytes | JsonValue, feed: FeedParsingSpec) -> InstrumentPageResult:
    data = decode_json(payload) if isinstance(payload, (bytes, str)) else JsonDocument.validate_value(payload)
    venue = feed.venue
    if feed.market_type is None:
        raise ValueError("Instrument feed requires a market type")
    cursor: str | None = None
    keyed: dict[str, JsonValue] | None = None
    if venue == "bybit":
        outer = project(data, ("retCode", "result"))
        if "result" in outer:
            outer["result"] = project(outer["result"], ("list", "nextPageCursor"))
        envelope = (
            BybitSpotEnvelope.model_validate(outer)
            if feed.market_type == "SPOT"
            else BybitPerpetualEnvelope.model_validate(outer)
        )
        if envelope.retCode != 0:
            raise ValueError("Bybit instrument API refused the request")
        entries = envelope.result.list
        cursor = envelope.result.nextPageCursor or None
        fields = ("symbol", "baseCoin", "quoteCoin", "status")
    elif venue == "okx":
        okx = OkxEnvelope.model_validate(project(data, ("code", "data")))
        if okx.code != "0":
            raise ValueError("OKX instrument API refused the request")
        entries = okx.data
        fields = ("instId", "baseCcy", "quoteCcy", "state")
    elif venue == "kraken":
        kraken = KrakenEnvelope.model_validate(project(data, ("error", "result")))
        if kraken.error:
            raise ValueError("Kraken instrument API refused the request")
        keyed = kraken.result
        entries = list(keyed.values())
        fields = ("symbol", "base", "quote", "status")
    elif venue == "binance":
        if isinstance(data, dict) and "code" in data:
            raise ValueError("Binance instrument response is an error envelope")
        entries = BinanceEnvelope.model_validate(project(data, ("symbols",))).symbols
        fields = ("symbol", "baseAsset", "quoteAsset", "status")
    elif venue == "coinbase":
        entries = rows(data)
        fields = ("id", "base_currency", "quote_currency", "status")
    elif venue == "hyperliquid":
        entries = HyperliquidEnvelope.model_validate(project(data, ("universe",))).universe
        fields = ("name", "name", "quote", "status")
    else:
        raise ValueError("Unknown instrument venue")
    result: dict[str, JsonValue] = {}
    boolean = TypeAdapter(bool, config=ConfigDict(strict=True, hide_input_in_errors=True))
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError("Instrument protocol requires object entries")
        row = dict(entry)
        if keyed is not None:
            row["symbol"] = tuple(keyed)[index]
        symbol = row[fields[0]]
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("Instrument symbol must be a nonempty string")
        status: JsonValue = None
        if venue != "hyperliquid":
            status = row[fields[3]]
        if venue == "hyperliquid":
            delisted = boolean.validate_python(row["isDelisted"]) if "isDelisted" in row else False
            status = "DELISTED" if delisted else "TRADING"
        flags: dict[str, JsonValue] = {}
        if venue == "coinbase":
            for name in ("trading_disabled", "cancel_only", "post_only", "limit_only", "auction_mode"):
                if name in row:
                    flags[name] = boolean.validate_python(row[name])
            if "trading_disabled" in flags and flags["trading_disabled"] is True:
                status = "TRADING_DISABLED"
        contract = optional_text(row, "contractType")
        alternate = optional_text(row, "ctType")
        if contract is None or contract == "":
            contract = alternate
        observation: dict[str, JsonValue] = {
            "venue": venue,
            "market_type": feed.market_type,
            "symbol": symbol,
            "base": optional_text(row, fields[1]),
            "quote": optional_text(row, fields[2]),
            "status": status,
            "contract_type": contract,
        }
        if venue == "coinbase":
            observation["status_flags"] = flags
        if symbol in result:
            raise ValueError("Instrument response contains duplicate symbols")
        result[symbol] = observation
    return InstrumentPageResult(snapshot=InstrumentSnapshot.model_validate(result), next_cursor=cursor)
