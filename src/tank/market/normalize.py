"""Strict exchange normalization. Missing semantic fields are errors, never estimates."""

from bisect import bisect_right
from datetime import datetime, timedelta
from decimal import Decimal

from tank.market.errors import SemanticError
from tank.market.funding import FundingObservation
from tank.market.precision import precise
from tank.time_precision import collection_clock, propagate_timestamp
from tank.util import canonical_json, iso, jsonable, parse_time


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


D = Decimal


def decimal(value):
    if value is None or isinstance(value, bool):
        raise SemanticError("missing or invalid decimal")
    result = D(str(value))
    if not result.is_finite():
        raise SemanticError("nonfinite decimal")
    return result


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


@precise
def trade(instrument, raw, *, received_ts=None, source="WS"):
    key = venue_key(instrument)
    venue = key.split(".")[0]
    aggregation = "PER_FILL"
    if venue == "binance":
        if not isinstance(raw["m"], bool):
            raise SemanticError("buyer-is-maker must be boolean")
        side, qty, price, ts, tid = ("SELL" if raw["m"] else "BUY"), raw["q"], raw["p"], raw["T"], raw["a"]
        aggregation = "PER_TAKER_ORDER_SAME_PRICE"
    elif venue == "coinbase":
        if raw["side"] not in ("buy", "sell"):
            raise SemanticError("unknown Coinbase maker side")
        side, qty, price, ts, tid = (
            ("BUY" if raw["side"] == "sell" else "SELL"),
            raw["size"],
            raw["price"],
            raw["time"],
            raw["trade_id"],
        )
    elif venue == "okx":
        if raw["side"] not in ("buy", "sell"):
            raise SemanticError("unknown OKX taker side")
        side, qty, price, ts, tid = raw["side"].upper(), raw["sz"], raw["px"], raw["ts"], raw["tradeId"]
        if key == "okx.swap":
            multiplier = instrument.get("contract_multiplier")
            if multiplier is None:
                raise SemanticError("OKX ctVal base multiplier unverified")
            qty = decimal(qty) * decimal(multiplier)
        aggregation = "PER_TAKER_ORDER_SAME_PRICE"
    elif venue == "bybit":
        s = raw.get("S", raw.get("side"))
        if s not in ("Buy", "Sell"):
            raise SemanticError("unknown Bybit taker side")
        side, qty, price, ts, tid = (
            s.upper(),
            raw.get("v", raw.get("size")),
            raw.get("p", raw.get("price")),
            raw.get("T", raw.get("time")),
            raw.get("i", raw.get("execId")),
        )
    elif venue == "kraken":
        if isinstance(raw, list):
            if len(raw) < 7:
                raise SemanticError("Kraken trade ID absent")
            price, qty, ts, s, _, _, tid = raw[:7]
        else:
            price, qty, ts, s, tid = raw["price"], raw["qty"], raw["timestamp"], raw["side"], raw["trade_id"]
        if s not in ("buy", "sell", "b", "s"):
            raise SemanticError("unknown Kraken taker side")
        side = "BUY" if s in ("buy", "b") else "SELL"
    elif venue == "hyperliquid":
        if raw["side"] not in ("B", "A"):
            raise SemanticError("unknown Hyperliquid side")
        side, qty, price, ts, tid = (
            ("BUY" if raw["side"] == "B" else "SELL"),
            raw["sz"],
            raw["px"],
            raw["time"],
            raw["tid"],
        )
    else:
        raise SemanticError(f"unsupported venue {venue}")
    if tid is None:
        raise SemanticError("missing trade ID")
    qty, price = decimal(qty), decimal(price)
    if qty <= 0 or price <= 0:
        raise SemanticError("nonpositive trade")
    quote = instrument.get("quote_asset", "").upper()

    fx, _ = quote_usd_rate(instrument, ts)
    usd = qty * price * decimal(fx) if fx is not None else None
    return jsonable(
        {
            "instrument_id": instrument["instrument_id"],
            "token_id": instrument["token_id"],
            "venue": venue,
            "venue_symbol": instrument.get("venue_symbol"),
            "market_type": instrument["market_type"],
            "trade_id": str(tid),
            "source_payload_json": canonical_json(raw),
            **timestamp_fields("ts", ts, "EXACT"),
            **collection_clock(received_ts).fields("received_ts"),
            "side": side,
            "qty_native": qty,
            "price": price,
            "quote_notional": qty * price,
            "notional_usd": usd,
            "quote_asset": quote,
            "usd_reason": None if usd is not None else "QUOTE_USD_RATE_UNAVAILABLE",
            "metric_key": key
            + (
                ".aggTrade"
                if venue == "binance"
                else ".trades"
                if venue in ("okx", "hyperliquid")
                else (".trades" if source == "REST" else ".matches")
                if venue == "coinbase"
                else ".publicTrade"
                if venue == "bybit"
                else ".trade"
            ),
            "trade_aggregation": aggregation,
            "raw_first_id": raw.get("f") if isinstance(raw, dict) else None,
            "raw_last_id": raw.get("l") if isinstance(raw, dict) else None,
            "sequence": raw.get("seq", raw.get("seqId", raw.get("_stream_sequence")))
            if isinstance(raw, dict)
            else None,
            "sequence_semantics": "MONOTONIC" if venue in ("okx", "bybit") else "N_A",
            "capture_source": source,
        }
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
