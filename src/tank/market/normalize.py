"""Strict exchange normalization. Missing semantic fields are errors, never estimates."""

from bisect import bisect_right
from datetime import datetime, timedelta

from tank.market.errors import SemanticError
from tank.market.funding import FundingObservation
from tank.market.numbers import decimal
from tank.market.precision import precise
from tank.market.trade_contracts import FlowInstrument, NormalizedTrade, QuoteUsdValuation, TradeCaptureSource
from tank.market.trade_wire import TradeWire, decode_trade
from tank.time_precision import collection_clock, propagate_timestamp
from tank.util import canonical_json, iso, jsonable, parse_iso_time, parse_time


def timestamp_fields(field, value, precision, *, bucket_size=None):
    """Emit caller-declared source precision through the shared time contract."""
    return propagate_timestamp(
        {field: iso(value) if value is not None else None, field + "_prec": precision},
        field,
        bucket_size=bucket_size,
    ).fields(field)


QUOTE_USD_HISTORY: dict[str, tuple[tuple[datetime, str], ...]] = {}
QUOTE_USD_REACH = timedelta(days=4)
QUOTE_USD_MAX_AGE = timedelta(hours=6)


def remember_quote_usd(quote, samples):
    """Add (as_of, rate) samples; those older than QUOTE_USD_REACH before the newest drop."""
    merged = dict(QUOTE_USD_HISTORY.get(quote, ()))
    merged.update((parse_time(as_of), str(rate)) for as_of, rate in samples)
    if not merged:
        return
    newest = max(merged)

    QUOTE_USD_HISTORY[quote] = tuple(
        sorted(item for item in merged.items() if item[0] >= newest - QUOTE_USD_REACH)
    )


def quote_usd_rate(instrument, at):
    """(rate, as_of) valuing quote units at time `at`, or (None, None)."""
    quote = instrument.get("quote_asset", "").upper()
    if quote == "USD":
        return "1", None
    at = parse_time(at)
    history = QUOTE_USD_HISTORY.get(quote)
    if history:
        index = bisect_right(history, at, key=lambda sample: sample[0])
        if index and at - history[index - 1][0] <= QUOTE_USD_MAX_AGE:
            return history[index - 1][1], iso(history[index - 1][0])
        return None, None
    rate, as_of = instrument.get("quote_usd_rate"), instrument.get("quote_usd_rate_as_of")
    if rate is None or as_of and parse_time(as_of) > at:
        return None, None
    return rate, as_of


def venue_key(instrument):
    venue = instrument["venue"].lower()
    perp = instrument.get("market_type", "SPOT").lower() not in ("spot",)
    if venue.startswith("binance"):
        return "binance.usdm" if perp or venue.endswith("usdm") else "binance.spot"
    if venue.startswith("bybit"):
        return "bybit.linear" if perp else "bybit.spot"
    if venue.startswith("okx"):
        return "okx.swap" if perp else "okx.spot"
    if venue.startswith("hyperliquid"):
        return "hyperliquid.perp"
    return venue.split(".")[0] + ".spot"


def _trade_metric(instrument: FlowInstrument, source: TradeCaptureSource) -> str:
    prefixes = {
        ("binance", "SPOT"): "binance.spot",
        ("binance", "PERP_LINEAR"): "binance.usdm",
        ("bybit", "SPOT"): "bybit.spot",
        ("bybit", "PERP_LINEAR"): "bybit.linear",
        ("okx", "SPOT"): "okx.spot",
        ("okx", "PERP_LINEAR"): "okx.swap",
        ("coinbase", "SPOT"): "coinbase.spot",
        ("kraken", "SPOT"): "kraken.spot",
        ("hyperliquid", "PERP_LINEAR"): "hyperliquid.perp",
    }
    identity = (instrument.venue, instrument.market_type)
    if identity not in prefixes:
        raise SemanticError("unsupported venue market type")
    if instrument.venue == "coinbase":
        if source == "ARCHIVE":
            raise SemanticError("Archived Coinbase trade requires its original metric provenance")
        return prefixes[identity] + (".trades" if source == "REST" else ".matches")
    suffixes = {
        "binance": ".aggTrade",
        "bybit": ".publicTrade",
        "okx": ".trades",
        "kraken": ".trade",
        "hyperliquid": ".trades",
    }
    return prefixes[identity] + suffixes[instrument.venue]


@precise
def trade(
    instrument: FlowInstrument,
    raw: TradeWire,
    *,
    valuation: QuoteUsdValuation,
    received_ts: datetime,
    source: TradeCaptureSource,
) -> NormalizedTrade:
    """Decode actual provider fields; caller owns quote valuation and receipt observation."""
    metric = _trade_metric(instrument, source)
    decoded = decode_trade(instrument.venue, raw)
    qty = decoded.reported_quantity
    if instrument.venue == "okx" and instrument.market_type == "PERP_LINEAR":
        if instrument.contract_multiplier is None:
            raise SemanticError("OKX ctVal base multiplier unverified")
        qty *= instrument.contract_multiplier
    if valuation.as_of is not None and valuation.as_of > decoded.ts:
        raise SemanticError("Quote USD valuation is newer than the trade")
    if valuation.basis == "USD_UNIT" and instrument.quote_asset.upper() != "USD":
        raise SemanticError("USD unit valuation requires a USD quote")
    quote_notional = qty * decoded.price
    usd = None if valuation.rate is None else quote_notional * valuation.rate
    return NormalizedTrade(
        instrument_id=instrument.instrument_id,
        token_id=instrument.token_id,
        venue=instrument.venue,
        venue_symbol=instrument.venue_symbol,
        market_type=instrument.market_type,
        quote_asset=instrument.quote_asset.upper(),
        trade_id=decoded.trade_id,
        source_payload_json=canonical_json(raw),
        ts=decoded.ts,
        ts_prec="EXACT",
        ts_prec_reason=None,
        received_ts=parse_iso_time(received_ts),
        received_ts_prec="COLLECTION_ONLY",
        received_ts_prec_reason=None,
        side=decoded.side,
        qty_native=qty,
        price=decoded.price,
        quote_notional=quote_notional,
        notional_usd=usd,
        usd_reason=valuation.unavailable_reason,
        metric_key=metric,
        trade_aggregation=decoded.aggregation,
        raw_first_id=decoded.raw_first_id,
        raw_last_id=decoded.raw_last_id,
        sequence=decoded.sequence,
        sequence_semantics="MONOTONIC" if instrument.venue in {"okx", "bybit"} else "N_A",
        capture_source=source,
    )


@precise
def kline(instrument, row, *, now=None):
    key = venue_key(instrument)
    extra = {}
    if key.startswith("binance"):
        ts, o, h, low, c, v, closed, q, n, b, bq = row[:11]
        extra = {"quote_volume": q, "n_trades": int(n), "taker_buy_native": b, "taker_buy_quote": bq}
        complete = parse_time(closed) < parse_time(now or iso())
    elif key.startswith("okx"):
        ts, o, h, low, c, v = row[:6]
        if key == "okx.swap":
            if instrument.get("contract_multiplier") is None:
                raise SemanticError("OKX multiplier absent")
            v = decimal(v) * decimal(instrument["contract_multiplier"])
        extra = {
            "quote_volume": row[7] if len(row) > 7 else None,
            "n_trades": None,
            "taker_buy_native": None,
            "taker_buy_quote": None,
        }
        complete = str(row[8]) == "1" if len(row) > 8 else False
    elif key.startswith("bybit"):
        ts, o, h, low, c, v, q = row[:7]
        complete = parse_time(ts) + timedelta(minutes=1) <= parse_time(now or iso())
        extra = {"quote_volume": q, "n_trades": None, "taker_buy_native": None, "taker_buy_quote": None}
    elif key.startswith("coinbase"):
        ts, low, h, o, c, v = row[:6]
        complete = parse_time(ts) + timedelta(minutes=1) <= parse_time(now or iso())
    elif key.startswith("kraken"):
        ts, o, h, low, c, vwap, v, n = row[:8]
        complete = parse_time(ts) + timedelta(minutes=1) <= parse_time(now or iso())
        extra = {
            "n_trades": int(n),
            "quote_volume": decimal(vwap) * decimal(v),
            "quote_volume_method": "REPORTED_VWAP_X_NATIVE_VOLUME",
        }
    elif key.startswith("hyperliquid"):
        ts, o, h, low, c, v = (row[x] for x in ("t", "o", "h", "l", "c", "v"))
        complete = parse_time(row["T"]) < parse_time(now or iso())
        extra = {"n_trades": int(row["n"])}
    else:
        raise SemanticError("unsupported kline")
    result = {
        "instrument_id": instrument["instrument_id"],
        "token_id": instrument["token_id"],
        **timestamp_fields("open_time", ts, "BUCKET(60s)"),
        "o": str(decimal(o)),
        "h": str(decimal(h)),
        "l": str(decimal(low)),
        "c": str(decimal(c)),
        "volume_native": str(decimal(v)),
        "complete": complete,
        **collection_clock(now).fields("received_ts"),
        "interval_s": 60,
    }
    for name in ("quote_volume", "n_trades", "taker_buy_native", "taker_buy_quote"):
        result[name] = extra.get(name)
    quote = result.get("quote_volume")

    fx, fx_asof = quote_usd_rate(instrument, parse_time(ts) + timedelta(minutes=1))
    result.update(
        quote_asset=instrument.get("quote_asset"),
        quote_volume_method=extra.get("quote_volume_method", "REPORTED" if quote is not None else None),
        quote_volume_usd=decimal(quote) * decimal(fx) if quote is not None and fx is not None else None,
        quote_volume_usd_reason="QUOTE_VOLUME_NOT_REPORTED"
        if quote is None
        else "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"
        if fx is None
        else None,
        quote_usd_rate=fx,
        **timestamp_fields("quote_usd_rate_as_of", fx_asof, "COLLECTION_ONLY" if fx_asof else None),
    )
    return jsonable(result)


@precise
def oi(instrument, raw, *, mark=None, mark_ts=None, mark_ts_prec=None, received_ts=None, bucket_size_s=300):
    received_ts = received_ts or raw.get("_capture_observed_at")
    key = venue_key(instrument)
    sidedness = "SINGLE_SIDED"
    reason = None
    if key == "binance.usdm":
        field = "sumOpenInterest" if "sumOpenInterest" in raw else "openInterest"
        value = raw[field]
        ts = raw.get("time", raw.get("timestamp"))
        metric = key + "." + ("openInterestHist" if field == "sumOpenInterest" else "openInterest")
    elif key == "okx.swap":
        value = raw["oiCcy"]
        ts = raw["ts"]
        metric = key + ".open-interest"
    elif key == "bybit.linear":
        if "singleOpenInterest" not in raw:
            raise SemanticError("Bybit singleOpenInterest absent; openInterest cannot substitute")
        value = raw["singleOpenInterest"]
        ts = raw.get("timestamp")
        metric = key + ".open-interest"
    elif key == "hyperliquid.perp":
        value = raw["openInterest"]
        ts = received_ts or iso()
        metric = key + ".ctx"
        sidedness = "UNKNOWN"
        reason = "SIDEDNESS_UNPUBLISHED"
    else:
        raise SemanticError("spot has no OI")
    if ts is None:
        raise SemanticError("OI source timestamp absent")
    bucketed = "Hist" in metric

    if bucketed or key == "bybit.linear":
        allowed = (
            {300, 900, 1800, 3600, 14400, 86400}
            if key == "bybit.linear"
            else {300, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400}
        )
        if isinstance(bucket_size_s, bool) or decimal(bucket_size_s) not in allowed:
            raise SemanticError("OI bucket must match a documented source request period in seconds")
        bucket_size_s = int(decimal(bucket_size_s))
    precision = (
        "COLLECTION_ONLY"
        if key == "hyperliquid.perp"
        else f"BUCKET({bucket_size_s}s)"
        if bucketed
        else "EXACT"
    )
    value = decimal(value)
    if value < 0:
        raise SemanticError("negative open interest")
    mark_aligned = (
        mark is not None
        and mark_ts is not None
        and abs((parse_time(mark_ts) - parse_time(ts)).total_seconds()) <= 60
    )
    fx = "1" if instrument.get("quote_asset") == "USD" else instrument.get("quote_usd_rate")
    usd = (
        decimal(raw["sumOpenInterestValue"])
        if raw.get("sumOpenInterestValue") is not None
        else decimal(raw["oiUsd"])
        if raw.get("oiUsd") is not None
        else value * decimal(mark) * decimal(fx)
        if mark_aligned and fx is not None
        else None
    )
    return jsonable(
        {
            "instrument_id": instrument["instrument_id"],
            "token_id": instrument["token_id"],
            **timestamp_fields("ts", ts, precision),
            **collection_clock(received_ts).fields("received_ts"),
            "ts_bucket_size_s": bucket_size_s if bucketed else None,
            **({"ts_bucket_anchor": "END" if key == "binance.usdm" else "UNSPECIFIED"} if bucketed else {}),
            "raw_value": str(value),
            "raw_unit": "BASE_QTY",
            "sidedness": sidedness,
            "metric_key": metric,
            "oi_native": value,
            "oi_usd": usd,
            "oi_usd_reason": None if usd is not None else "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE",
            "mark_price": str(mark) if mark_aligned else None,
            **timestamp_fields(
                "mark_as_of", mark_ts if mark_aligned else None, mark_ts_prec if mark_aligned else None
            ),
            "synchronized_eligible": sidedness == "SINGLE_SIDED",
            "gap_reason": reason,
            "timestamp_precision": precision,
        }
    )


@precise
def funding(instrument, raw, *, kind, interval_h, received_ts=None):
    received_ts = received_ts or raw.get("_capture_observed_at")
    key = venue_key(instrument)
    interval = decimal(interval_h)
    if interval <= 0:
        raise SemanticError("invalid funding interval")
    if key.startswith("binance"):
        rate = raw["fundingRate"] if kind == "SETTLED" else raw.get("lastFundingRate", raw.get("r"))
        timestamp = (
            raw["fundingTime"] if kind == "SETTLED" else raw.get("time", raw.get("E", received_ts or iso()))
        )
    elif key.startswith("okx"):
        rate = (
            raw["realizedRate"]
            if kind == "SETTLED" and raw.get("realizedRate") is not None
            else raw["fundingRate"]
        )
        timestamp = raw["fundingTime"] if kind == "SETTLED" else raw.get("ts", received_ts or iso())
    elif key.startswith("bybit"):
        rate = raw["fundingRate"]
        timestamp = raw["fundingRateTimestamp"] if kind == "SETTLED" else received_ts or iso()
    elif key.startswith("hyperliquid"):
        rate = raw["fundingRate"] if kind == "SETTLED" else raw["funding"]
        timestamp = raw.get("time", received_ts or iso())
    else:
        raise SemanticError("spot has no funding")
    rate = decimal(rate)
    source_clock = kind == "SETTLED" or (
        key.startswith("binance")
        and (raw.get("time") is not None or raw.get("E") is not None)
        or key.startswith("okx")
        and raw.get("ts") is not None
        or key.startswith("hyperliquid")
        and raw.get("time") is not None
    )
    observation = FundingObservation(
        instrument_id=instrument["instrument_id"],
        token_id=instrument["token_id"],
        kind=kind,
        rate=rate,
        interval_h=interval,
        timestamp=iso(timestamp),
        source_clock=source_clock,
        metric_key=key + ".funding",
    )
    return jsonable(
        dict(
            instrument_id=observation.instrument_id,
            token_id=observation.token_id,
            kind=observation.kind,
            rate=observation.rate,
            interval_h=observation.interval_h,
            rate_8h_eq=observation.rate_8h_eq,
            **timestamp_fields("as_of", timestamp, "EXACT" if source_clock else "COLLECTION_ONLY"),
            **(
                {
                    **timestamp_fields("funding_time", timestamp, "EXACT"),
                    **timestamp_fields("settlement_time", timestamp, "EXACT"),
                }
                if kind == "SETTLED"
                else {}
            ),
            **collection_clock(received_ts).fields("received_ts"),
            metric_key=key + ".funding",
        )
    )


@precise
def liquidation(instrument, raw, *, received_ts=None):
    key = venue_key(instrument)
    if key == "binance.usdm":
        x = raw.get("o", raw)
        if x.get("S") not in ("BUY", "SELL"):
            raise SemanticError("unknown Binance liquidation order side")
        side = "LONG_LIQ" if x["S"] == "SELL" else "SHORT_LIQ"
        qty = x["z"] if x.get("z") else x["q"]
        price = x["ap"]
        ts = x["T"]
        coverage = "SAMPLED"
    elif key == "bybit.linear":
        if raw.get("S") not in ("Buy", "Sell"):
            raise SemanticError("unknown Bybit liquidated position side")
        side = "LONG_LIQ" if raw["S"] == "Buy" else "SHORT_LIQ"
        qty = raw["v"]
        price = raw["p"]
        ts = raw["T"]
        coverage = "COMPLETE"
    elif key == "okx.swap":
        side = "LONG_LIQ" if raw["posSide"] == "long" else "SHORT_LIQ" if raw["posSide"] == "short" else None
        if side is None:
            raise SemanticError("OKX liquidation position side absent")
        if instrument.get("contract_multiplier") is None:
            raise SemanticError("OKX multiplier absent")
        qty = decimal(raw["sz"]) * decimal(instrument["contract_multiplier"])
        price = raw["bkPx"]
        ts = raw["ts"]
        coverage = "SAMPLED"
    else:
        raise SemanticError("liquidations unsupported")
    qty, price = decimal(qty), decimal(price)
    if qty < 0 or price <= 0:
        raise SemanticError("invalid liquidation quantity/price")
    fx = "1" if instrument.get("quote_asset") == "USD" else instrument.get("quote_usd_rate")
    return jsonable(
        {
            "instrument_id": instrument["instrument_id"],
            "token_id": instrument["token_id"],
            **timestamp_fields("ts", ts, "EXACT"),
            **collection_clock(received_ts).fields("received_ts"),
            "side": side,
            "qty_native": qty,
            "price": price,
            "notional_usd": qty * price * decimal(fx) if fx is not None else None,
            "null_reasons": {} if fx is not None else {"notional_usd": "INCOMPLETE"},
            "usd_reason": None if fx is not None else "QUOTE_USD_RATE_UNAVAILABLE",
            "coverage": coverage,
            "price_kind": "BANKRUPTCY" if key in ("okx.swap", "bybit.linear") else "EXECUTION_AVERAGE",
        }
    )
