"""Deterministic market evidence at one semantic market cutoff.

Exchange/settlement timestamps and completed-bar bucket ends bound quantitative
windows. Receipt time is collection latency, not a second market cutoff: archive
capture may occur later. When supplied, assembled_at bounds which received rows
were available to construct this snapshot. Missing evidence never votes true.
"""

import asyncio
import contextlib
import hashlib
import json
import threading
from array import array
from bisect import bisect_left, bisect_right
from collections import defaultdict
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, getcontext, localcontext
from functools import wraps
from statistics import mean, median
from types import SimpleNamespace

from tank.analysis import compute_workers, truth_sql
from tank.market.capture import event_anchor
from tank.time_precision import Timestamp, chosen_boundary, offset_schedule, propagate_timestamp
from tank.util import iso, jsonable, new_id, parse_time

WINDOWS = {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400}
RISK_ORDER = [
    "LIQUIDITY_VACUUM",
    "SHORT_SQUEEZE",
    "PERP_LED_CHASE",
    "DISTRIBUTION",
    "SELL_SIDE_ABSORPTION",
    "ORGANIC_MIXED",
    "SPOT_LED_ACCUMULATION",
    "REAL_MARKET_FRAGMENTATION",
    "PRICE_MARKUP_FAKE_OI",
]

_BASELINE_CACHE = ContextVar("truth_baseline_cache", default=None)
_BASELINE_DEPENDENCY = ContextVar("truth_baseline_dependency", default=None)

_TIMES = ContextVar("truth_observation_times", default=None)
_PARAMETERS = ContextVar("tank_truth_parameters", default=None)
_ASSESSMENT_INPUTS = ContextVar("truth_assessment_inputs", default=None)


def parameter(name, default):
    return (_PARAMETERS.get() or {}).get(name, default)


def lookback_days():
    return parameter("historical_lookback_days", parameter("base_retention_days", 90))


def decimal(v):
    if isinstance(v, dict):
        v = v.get("value")
    if v is None:
        return None
    value = v if isinstance(v, Decimal) else Decimal(str(v))
    exact_context(value)
    return value


def exact_context(*values):
    values = [v for v in values if isinstance(v, Decimal) and v.is_finite()]
    if values:
        span = max(v.adjusted() for v in values) - min(v.as_tuple().exponent for v in values) + 1
        getcontext().prec = max(getcontext().prec, 2 * span + 32)


def precise(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        with localcontext() as context:
            context.prec = 128
            return function(*args, **kwargs)

    return wrapped


def number(v):
    if isinstance(v, dict):
        v = v.get("value")
    return None if v is None else float(v)


def ratio(a, b):
    if a is None or not b:
        return None
    return float(decimal(a) / decimal(b))


@precise
def strict_majority(part, total):
    """The §0 predicate, independent of rounded dimensionless display statistics."""
    if part is None or total is None:
        return None
    part, total = decimal(part), decimal(total)
    if total <= 0:
        return None
    exact_context(part, total)
    return 2 * part > total


def spot_majority(spot, direction):
    key = direction.lower()
    flags = spot.get("exact_majority")
    if flags is not None and key in flags:
        return flags[key]
    value = (spot.get("cw_breadth") or {}).get(key)
    return strict_majority(value, 1) if value is not None else None


def missing(reason="INCOMPLETE", unit=None):
    return {
        "value": None,
        "unit": unit,
        "window": None,
        "observed_at": None,
        "precision": None,
        "coverage": None,
        "reason": reason,
    }


def remember(cache, key, build):
    if cache is None:
        return build()
    if key not in cache:
        cache[key] = build()
        if len(cache) > 128:
            cache.pop(next(iter(cache)))
    return cache[key]


def remember_latest(cache, key, head, build):
    """The entry built for `head` under one key: a new head replaces the previous entry, so
    the cache keeps only the newest state per key instead of one copy per head."""
    held = cache.get(key)
    if held is not None and held["head"] == head:
        return held
    built = build()
    cache.pop(key, None)
    cache[key] = built
    if len(cache) > 128:
        cache.pop(next(iter(cache)))
    return built


def percentile(values, p):
    s = sorted(v for v in values if v is not None)
    if not s:
        return None
    k = (len(s) - 1) * p
    i = int(k)
    return s[i] + (s[min(i + 1, len(s) - 1)] - s[i]) * (
        decimal(k - i) if isinstance(s[i], Decimal) else k - i
    )


def normal(values):
    qs = parameter("normal_range_quantiles", [0.025, 0.975])
    return [percentile(values, float(qs[0])), percentile(values, float(qs[1]))] if values else None


def beyond(v, r):
    return None if v is None or not r or None in r else v < r[0] or v > r[1]


def within(v, r):
    b = beyond(v, r)
    return None if b is None else not b


@precise
def decomposition(n0, n1, p0, p1):
    n0, n1, p0, p1 = (Decimal(str(v)) for v in (n0, n1, p0, p1))
    exact_context(n0, n1, p0, p1)
    position = (n1 - n0) * p0
    markup = n0 * (p1 - p0)
    interaction = (n1 - n0) * (p1 - p0)
    share = ratio(abs(position), abs(position) + abs(markup) + abs(interaction))
    return dict(position=position, markup=markup, interaction=interaction, pos_share=share)


def oi_class(dn, du, dec, nrange, urange):
    nb = beyond(dn, nrange)
    ub = beyond(du, urange)
    if nb is None:
        return None
    if nb:
        if dec.get("pos_share") is None:
            return None
        terms = [decimal(dec[k]) for k in ("position", "markup", "interaction")]
        exact_context(*terms)
        return "REAL_NATIVE" if 2 * abs(terms[0]) > sum(abs(v) for v in terms) else "MIXED"
    if ub is None:
        return None
    return "PRICE_MARKUP" if ub else "MIXED"


def _time(row):
    times = _TIMES.get()
    if times is not None:
        hit = times.get(id(row))
        if hit is not None:
            return hit[1]
    value = _observation_time(row)
    if times is not None:
        times[id(row)] = (row, value)
    return value


def _instrument_rows(snapshot, collection, instrument_id):
    """Rows of one instrument in their snapshot order, indexed once per assessment
    (windows re-filtered every row of the collection per instrument)."""
    rows = snapshot.get(collection, [])
    inputs = _ASSESSMENT_INPUTS.get()
    if inputs is None:
        return [r for r in rows if r.get("instrument_id") == instrument_id]
    memo = inputs.setdefault("by_instrument", {})
    hit = memo.get((collection, id(rows)))
    if hit is None or hit[0] is not rows:
        index = defaultdict(list)
        for row in rows:
            index[row.get("instrument_id")].append(row)
        hit = (rows, index)
        memo[(collection, id(rows))] = hit
    return hit[1].get(instrument_id, [])


def _observation_time(row):
    for k in (
        "minute_start",
        "open_time",
        "exchange_ts",
        "ts",
        "as_of",
        "settlement_time",
        "funding_time",
        "observed_at",
    ):
        if row.get(k) is not None:
            return parse_time(row[k])
    return None


def kline_close_time(row):
    """The bar's close boundary, open_time + interval_s.

    Binance closeTime (column [6], 03 §5.2.7) is the bar's last millisecond (hh:59:59.999);
    keying prices by it splits hourly archive bars from minute bars closing at hh:00 and
    drops them from every hourly series (F-16: 331 instead of ~2100 BTC 24h samples).
    """
    if row.get("open_time") is not None:
        return parse_time(row["open_time"]) + timedelta(seconds=int(row.get("interval_s", 60)))
    return parse_time(row["close_time"]) if row.get("close_time") else None


def _venue(row):
    return row.get("venue") or str(row.get("instrument_id", "")).split(":")[0]


def _market(row):
    return row.get("market_type") or ("SPOT" if ":spot:" in row.get("instrument_id", "") else "PERP_LINEAR")


@precise
def _sum(rows, key):
    vals = [decimal(r.get(key)) for r in rows]
    if not vals or None in vals:
        return None
    exact_context(*vals)
    return sum(vals, Decimal(0))


@precise
def _composite(venues):
    if not venues:
        return dict(
            cvd=None, volume=None, buy_share=None, bs_ratio=None, venues=[], sign=0, reason="INCOMPLETE"
        )
    exact_context(*(decimal(x[k]) for x in venues for k in ("volume_native", "cvd", "sum_sq_qty")))
    v = sum((decimal(x["volume_native"]) for x in venues), Decimal(0))
    cvd = sum((decimal(x["cvd"]) for x in venues), Decimal(0))
    q = sum((decimal(x["sum_sq_qty"]) for x in venues), Decimal(0))
    sign = (
        1
        if cvd > 0 and cvd * cvd >= decimal(parameter("significant_flow_sigma", 2)) ** 2 * q
        else -1
        if cvd < 0 and cvd * cvd >= decimal(parameter("significant_flow_sigma", 2)) ** 2 * q
        else 0
    )
    return dict(
        cvd=cvd,
        volume=v,
        buy_share=ratio(v + cvd, 2 * v),
        bs_ratio=ratio(v + cvd, v - cvd),
        venues=[x["venue"] for x in venues],
        sign=sign,
    )


def _available(row, snapshot):
    assembled = snapshot.get("assembled_at")
    received = row.get("received_ts")
    if not assembled or not received:
        return True
    inputs = _ASSESSMENT_INPUTS.get()
    if inputs is None:
        return parse_time(received) <= parse_time(assembled)
    memo = inputs.setdefault("available", {})
    hit = memo.get((id(row), assembled))
    if hit is None or hit[0] is not row:
        hit = (row, parse_time(received) <= parse_time(assembled))
        memo[(id(row), assembled)] = hit
    return hit[1]


def flow_segments(snapshot):
    """Bounded aggregate pieces extracted from typed minute bars, never individual prints.
    Built once per assessment per snapshot (callers only read it)."""
    inputs = _ASSESSMENT_INPUTS.get()
    sources = (snapshot.get("flow_segments", []), snapshot.get("flow_bars", []))
    if inputs is not None:
        memo = inputs.setdefault("flow_segments", {})
        hit = memo.get(tuple(map(id, sources)))
        if hit is not None and hit[0] is sources[0] and hit[1] is sources[1]:
            return hit[2]
    pieces = _flow_segments(snapshot)
    if inputs is not None:
        memo[tuple(map(id, sources))] = (*sources, pieces)
    return pieces


def _flow_segments(snapshot):
    result = {}
    for row in snapshot.get("flow_segments", []):
        result[(row["instrument_id"], row["start"], row["end"])] = row
    for bar in snapshot.get("flow_bars", []):
        for piece in bar.get("boundary_segments") or []:
            row = dict(
                piece,
                instrument_id=bar["instrument_id"],
                token_id=bar.get("token_id"),
                venue=_venue(bar),
                market_type=_market(bar),
            )
            result.setdefault((row["instrument_id"], row["start"], row["end"]), row)
    return list(result.values())


def segment_cover(pieces, iid, start, end, assembled_at=None):
    selected = [
        r
        for r in pieces
        if r.get("instrument_id") == iid
        and r.get("complete") is True
        and start <= parse_time(r["start"]) < parse_time(r["end"]) <= end
        and _available(r, {"assembled_at": assembled_at})
        and (
            assembled_at is None
            or r.get("observed_at") is None
            or parse_time(r["observed_at"]) <= parse_time(assembled_at)
        )
    ]
    cursor = start
    result = []
    while cursor < end:
        choices = [r for r in selected if parse_time(r["start"]) == cursor]
        if not choices:
            return None
        row = max(choices, key=lambda r: parse_time(r["end"]))
        result.append(row)
        cursor = parse_time(row["end"])
    return result


def flow_window(snapshot, iid, start, end):
    bars = [
        r
        for r in snapshot.get("flow_bars", [])
        if r.get("instrument_id") == iid and start <= _time(r) and _time(r) + timedelta(minutes=1) <= end
    ]
    first = start.replace(second=0, microsecond=0)
    first = first + timedelta(minutes=1) if first < start else first
    last = end.replace(second=0, microsecond=0)
    expected = max(0, int((last - first).total_seconds() / 60))
    if len({_time(r) for r in bars}) != expected or not all(r.get("complete") is True for r in bars):
        return None
    ranges = []
    if first > start:
        ranges.append((start, min(first, end)))
    if last < end and (not ranges or last >= ranges[0][1]):
        ranges.append((max(last, start), end))
    for left, right in ranges:
        pieces = segment_cover(flow_segments(snapshot), iid, left, right, snapshot.get("assembled_at"))
        if pieces is None:
            return None
        bars.extend(dict(piece, minute_start=piece["start"], segment_end=piece["end"]) for piece in pieces)
    return bars


@precise
def spot_truth(
    rows,
    tickers,
    instruments,
    start,
    end,
    *,
    flow_segments=None,
    assembled_at=None,
):
    pieces = flow_segments or []
    grouped = defaultdict(list)
    ids = defaultdict(set)
    for i in instruments:
        if _market(i) == "SPOT":
            ids[_venue(i)].add(i["instrument_id"])
    for r in rows:
        if _market(r) == "SPOT" and _time(r) < end and _time(r) + timedelta(minutes=1) > start:
            ids[_venue(r)].add(r["instrument_id"])
            if start <= _time(r) and _time(r) + timedelta(minutes=1) <= end:
                grouped[_venue(r)].append(r)
    registered = set(ids)
    venues = []
    excluded = []
    floor_start = start.replace(second=0, microsecond=0)
    floor_end = end.replace(second=0, microsecond=0)
    ceil_start = floor_start + timedelta(minutes=1) if start != floor_start else start
    expected = max(0, int((floor_end - ceil_start).total_seconds() / 60))
    partial = []
    if start != floor_start:
        partial.append((start, min(ceil_start, end)))
    if end != floor_end and not (start != floor_start and floor_end == floor_start):
        partial.append((max(start, floor_end), end))
    for venue in sorted(registered):
        rs = list(grouped[venue])
        minutes = {_time(r) for r in rs}
        boundary_failed = False
        segments = []
        for segment_start, segment_end in partial:
            selected = []
            for iid in ids[venue]:
                covered = segment_cover(pieces, iid, segment_start, segment_end, assembled_at)
                if covered is None:
                    boundary_failed = True
                    break
                selected.extend(covered)
            if boundary_failed:
                break
            rs.extend(selected)
            segments.append(
                dict(
                    start=iso(segment_start),
                    cutoff=iso(segment_end),
                    trade_count=sum((r.get("n_buy", 0) + r.get("n_sell", 0)) for r in selected),
                    aggregates=selected,
                )
            )
        if len(minutes) < expected or not all(r.get("complete") is True for r in rs) or boundary_failed:
            excluded.append(
                dict(
                    venue=venue,
                    reason="INCOMPLETE",
                    detail="BOUNDARY_AGGREGATE_UNAVAILABLE"
                    if boundary_failed
                    else "FULL_MINUTE_COVERAGE_INCOMPLETE",
                    source_reasons=sorted(
                        {
                            str(r.get("gap_reason") or r.get("coverage_reason"))
                            for r in list(grouped[venue]) + pieces
                            if r.get("instrument_id") in ids[venue]
                            and (r.get("gap_reason") or r.get("coverage_reason"))
                        }
                    ),
                )
            )
            continue
        b, s, q = (_sum(rs, k) for k in ("buy_qty", "sell_qty", "sum_sq_qty"))
        if None in (b, s, q):
            excluded.append(
                dict(
                    venue=venue,
                    reason="INCOMPLETE",
                    detail="FLOW_AGGREGATE_FIELDS_MISSING",
                    missing_metrics=[
                        k for k in ("buy_qty", "sell_qty", "sum_sq_qty") if any(r.get(k) is None for r in rs)
                    ],
                )
            )
            continue
        exact_context(b, s, q)
        v = b + s
        c = b - s
        sign = (
            1
            if c > 0 and c * c >= decimal(parameter("significant_flow_sigma", 2)) ** 2 * q
            else -1
            if c < 0 and c * c >= decimal(parameter("significant_flow_sigma", 2)) ** 2 * q
            else 0
        )
        venues.append(
            dict(
                venue=venue,
                complete=True,
                volume_native=v,
                buy_qty=b,
                sell_qty=s,
                cvd=c,
                buy_share=ratio(b, v),
                bs_ratio=ratio(b, s),
                sign=sign,
                sum_sq_qty=q,
                boundary_segments=segments,
            )
        )
    c = _composite(venues)
    v = c["volume"] or 0
    sign = c["sign"]
    estimated = []
    for ex in excluded:
        ts = [r for r in tickers if _venue(r) == ex["venue"] and _time(r) and _time(r) <= end]
        t = max(ts, key=_time) if ts else {}
        n = decimal(t.get("volume_native", t.get("volume")))
        estimated.append(n * decimal((end - start).total_seconds()) / 86400 if n is not None else None)
    covered = ratio(v, v + sum(estimated)) if None not in estimated else None
    cwb = {
        name: ratio(sum(x["volume_native"] for x in venues if x["sign"] == s), v)
        for name, s in [("buy", 1), ("sell", -1), ("neutral", 0)]
    }
    ranked = sorted(venues, key=lambda x: x["volume_native"], reverse=True)
    top = []
    vol = 0
    for x in ranked:
        top.append(x)
        vol += x["volume_native"]
        if vol > v / 2:
            break
    contributions = [(max(0, sign * x["cvd"]), x) for x in venues]
    total = sum(z[0] for z in contributions)
    tc = max(contributions, key=lambda x: x[0])[1] if contributions else None
    others = [x for x in venues if x is not tc]
    other_v = sum(x["volume_native"] for x in others)
    other_dir = ratio(sum(x["volume_native"] for x in others if x["sign"] == sign), other_v)
    top_share = ratio(max((z[0] for z in contributions), default=0), total)
    exact_majority = {
        name: strict_majority(sum((x["volume_native"] for x in venues if x["sign"] == s), Decimal(0)), v)
        for name, s in [("buy", 1), ("sell", -1), ("neutral", 0)]
    }
    exact_majority["covered"] = strict_majority(v, v + sum(estimated)) if None not in estimated else None
    direction_volume = sum(x["volume_native"] for x in venues if x["sign"] == sign)
    other_direction_volume = sum(x["volume_native"] for x in others if x["sign"] == sign)
    cls = (
        "INSUFFICIENT_DATA"
        if len(venues) < int(parameter("min_spot_venues", 2)) or covered is None or v <= sum(estimated)
        else "CONCENTRATED"
        if total
        and 2 * max(z[0] for z in contributions) > total
        and other_v
        and 2 * other_direction_volume < other_v
        else "BROAD"
        if 2 * direction_volume > v and ranked[0]["sign"] == sign
        else "DIVERGENT"
    )
    for x in venues:
        x["volume_share"] = ratio(x["volume_native"], v)
    return dict(
        venues=venues,
        composite_max=c,
        composite_cohort=None,
        breadth=dict(
            n_pos=sum(x["sign"] == 1 for x in venues),
            n_neg=sum(x["sign"] == -1 for x in venues),
            n_neutral=sum(x["sign"] == 0 for x in venues),
            pct_pos=ratio(sum(x["sign"] == 1 for x in venues), len(venues)),
            pct_neg=ratio(sum(x["sign"] == -1 for x in venues), len(venues)),
        ),
        cw_breadth=cwb,
        exact_majority=exact_majority,
        topn=dict(
            n=len(top),
            venues=[x["venue"] for x in top],
            agreement=ratio(sum(x["sign"] == sign for x in top), len(top)),
        ),
        concentration=dict(
            hhi=sum((ratio(x["volume_native"], v) or 0) ** 2 for x in venues) if v else None,
            top_volume_venue=ranked[0]["venue"] if ranked else None,
            top_volume_share=ratio(ranked[0]["volume_native"], v) if ranked else None,
            top_contrib_venue=tc["venue"] if tc else None,
            top_contrib_share=top_share,
            others_cwb_dir=other_dir,
        ),
        direction={1: "BUY", -1: "SELL", 0: "NEUTRAL"}[sign] if venues else None,
        classification=cls,
        coverage=dict(
            covered_share_est=covered,
            exact_majority=exact_majority["covered"],
            n_complete=len(venues),
            excluded=excluded,
        ),
        cohort_consistent={},
        size_profile=None,
    )


def fingerprint(value):
    return hashlib.sha256(
        json.dumps(
            jsonable(value),
            sort_keys=True,
            separators=(",", ":"),
            default=lambda v: v.tolist() if isinstance(v, array) else str(v),
        ).encode()
    ).hexdigest()


def oi_candidates(instruments, semantics):
    """1 for each perp instrument that may join the synchronised composite, else 0 (03 §5.4): a Binance, OKX
    or Bybit instrument whose open-interest definitions in the registry are all SINGLE_SIDED, the candidates
    sharing one aggregation class. The statements decide per window which of them are members."""
    definitions = [s for s in semantics if s.get("metric_family") == "oi"]
    flags, classes = [], set()
    for instrument in instruments:
        venue = _venue(instrument)
        own = [
            s
            for s in definitions
            if s.get("instrument_id") == instrument["instrument_id"]
            or s.get("instrument_id") is None
            and s.get("venue") == venue
        ]
        eligible = (
            venue in ("binance", "okx", "bybit")
            and bool(own)
            and all(s.get("sidedness") == "SINGLE_SIDED" for s in own)
        )
        flags.append(eligible)
        if eligible:
            classes.add(own[0].get("aggregation_class"))
    shared = len(classes) == 1 and None not in classes
    return [int(shared and eligible) for eligible in flags]


def oi_records(snapshot, window):
    """The endpoint and the history records of one window by instrument; the composite is 'composite'."""
    evidence = snapshot.get("oi_sql") or truth_sql.NO_OI
    return (
        {r.instrument_id: r for r in evidence.endpoints if r.window == window},
        {r.instrument_id: r for r in evidence.history if r.window == window},
    )


def oi_normal_ranges(history, length):
    """The native and the USD normal range of the change of open interest over `length` (03 §0): a history
    that reaches back less than the minimum days, or a range whose window ends span less than the minimum
    with the window, is None."""
    minimum = float(parameter("history_min_days", 7)) * 86400
    if (
        history is None
        or history.first_observation is None
        or (history.last_observation - history.first_observation).total_seconds() < minimum
    ):
        return None, None

    def spanning(low, high, first, last):
        if low is None or (last - first + length).total_seconds() < minimum:
            return None
        return [low, high]

    return (
        spanning(history.native_low, history.native_high, history.native_first_end, history.native_last_end),
        spanning(history.usd_low, history.usd_high, history.usd_first_end, history.usd_last_end),
    )


@precise
def oi_stats(endpoint, history, length):
    """The change of open interest of one venue, or of the composite, between the bounds of a window, its
    decomposition, its normal ranges and its class; None where a bound has no observation or the start has no
    positive native value."""
    if endpoint is None or endpoint.start_observed_at is None or endpoint.end_observed_at is None:
        return None
    n0, n1, u0, u1 = (
        endpoint.oi_native_start,
        endpoint.oi_native_end,
        endpoint.oi_usd_start,
        endpoint.oi_usd_end,
    )
    if n0 is None or n1 is None or n0 <= 0 or n1 < 0:
        return None
    p0 = u0 / n0 if u0 is not None else None
    p1 = u1 / n1 if u1 is not None and n1 else endpoint.end_mark
    usd_ready = u0 is not None and u1 is not None and u0 > 0
    dec = (
        decomposition(n0, n1, p0, p1)
        if p0 is not None and p1 is not None
        else dict(
            position=None,
            markup=None,
            interaction=None,
            pos_share=None,
            reason="INCOMPLETE",
            detail="SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE",
        )
    )
    nr, ur = oi_normal_ranges(history, length)
    dn, du = n1 / n0 - 1, u1 / u0 - 1 if usd_ready else None
    klass = oi_class(dn, du, dec, nr, ur)
    stamp = {
        "ts": endpoint.end_observed_at,
        "ts_prec": endpoint.end_ts_prec,
        "timestamp_precision": endpoint.end_timestamp_precision,
        "ts_bucket_size_s": endpoint.end_ts_bucket_size_s,
    }
    return dict(
        requested_window_duration_s=length.total_seconds(),
        observed_window_duration_s=(endpoint.end_observed_at - endpoint.start_observed_at).total_seconds(),
        native_oi_vs_normal=("ABOVE" if dn > nr[1] else "BELOW" if dn < nr[0] else "WITHIN") if nr else None,
        precision=propagate_timestamp(
            stamp, "ts", precision_aliases=("timestamp_precision", "precision")
        ).precision,
        oi_native_start=n0,
        oi_native_end=n1,
        oi_native_chg_pct=dn,
        oi_usd_start=u0,
        oi_usd_end=u1,
        oi_usd_chg_pct=du,
        oi_usd_reason=None if usd_ready else "INCOMPLETE",
        oi_usd_detail=None if usd_ready else "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE",
        decomposition=dec,
        oi_class=klass,
        classification_ready=klass is not None,
        native_normal_range=nr,
        usd_normal_range=ur,
        native_beyond=beyond(dn, nr),
        usd_beyond=beyond(du, ur),
        start_observed_at=iso(endpoint.start_observed_at),
        end_observed_at=iso(endpoint.end_observed_at),
    )


def oi_synchronized(snapshot, window, length, venues):
    """The composite of the venues that observed both bounds of the window, with `venues` as the (instrument,
    venue) pairs of the token in their order; None when there is none."""
    endpoints, history = oi_records(snapshot, window)
    composite = endpoints.get("composite")
    stats = oi_stats(composite, history.get("composite"), length) if composite else None
    if stats is not None:
        members = composite.cohort.split(",")
        stats["venues"] = [venue for iid, venue in venues if iid in members]
    return stats


def liquidation_side_evidence(rows, window_complete, coverage):
    """Missing conversion on one observed side cannot erase or zero its native evidence."""
    result = {}
    for side, classification in (("long", "LONG_LIQ"), ("short", "SHORT_LIQ")):
        selected = [row for row in rows if row.get("side") == classification]
        observed = bool(rows) or (window_complete and coverage in ("COMPLETE", "SAMPLED"))
        amount = _sum(selected, "notional_usd") if selected else Decimal(0) if observed else None
        prefix = "liq_" + side
        result[prefix + "_usd"] = amount
        result[prefix + "_usd_reason"] = "INCOMPLETE" if amount is None else None
        result[prefix + "_count"] = len(selected)
        result[prefix + "_native"] = (
            _sum(selected, "qty_native") if selected else Decimal(0) if observed else None
        )
    return result


@precise
def derivatives_truth(snapshot, start, end):
    groups = defaultdict(list)
    instruments = {}
    target = snapshot.get("token_id")
    for collection in ("instruments", "oi", "funding", "liquidations"):
        for row in snapshot.get(collection, []):
            iid = row.get("instrument_id")
            if iid and _market(row) != "SPOT" and row.get("token_id", target) == target:
                instruments.setdefault(iid, row)
                groups.setdefault(iid, [])
    for iid in groups:
        groups[iid] = _instrument_rows(snapshot, "oi", iid)
    endpoints, history = oi_records(snapshot, snapshot.get("_primary"))
    per = []
    for instrument, rs in groups.items():
        endpoint = endpoints.get(instrument)
        stats = oi_stats(endpoint, history.get(instrument), end - start)
        unresolved = endpoint.excluded if endpoint else 0
        venue = _venue(instruments[instrument])
        if stats is None:
            stats = {
                "oi_class": None,
                "oi_native_start": None,
                "oi_native_end": None,
                "oi_native_chg_pct": None,
                "oi_usd_start": None,
                "oi_usd_end": None,
                "oi_usd_chg_pct": None,
                "reason": "INCOMPLETE",
                "detail": "OI_BUCKET_EDGE_UNSPECIFIED" if unresolved else None,
            }
        fs = [f for f in _instrument_rows(snapshot, "funding", instrument) if _time(f) and _time(f) <= end]
        settled = sorted([f for f in fs if f.get("kind") == "SETTLED"], key=_time)
        est = sorted([f for f in fs if f.get("kind") == "CURRENT_ESTIMATE"], key=_time)
        last = settled[-1] if settled else {}
        interval = decimal(last.get("interval_h"))
        rate = decimal(last.get("rate"))
        eq = rate * 8 / interval if rate is not None and interval else None
        funding_history = [
            f
            for f in settled
            if _time(f) < start
            and decimal(f.get("interval_h")) is not None
            and decimal(f["interval_h"]) > 0
            and decimal(f.get("rate")) is not None
        ]
        fh = [
            decimal(f.get("rate_8h_eq"))
            if f.get("rate_8h_eq") is not None
            else decimal(f.get("rate")) * 8 / decimal(f["interval_h"])
            for f in funding_history
        ]
        settled_eq = eq
        estimate = est[-1] if est else {}
        estimate_interval = decimal(estimate.get("interval_h"))
        estimate_rate = decimal(estimate.get("rate"))
        estimate_eq = (
            estimate_rate * 8 / estimate_interval if estimate_rate is not None and estimate_interval else None
        )
        if estimate_eq is not None:
            eq = estimate_eq
            interval = estimate_interval
        history_ready = (
            bool(funding_history)
            and (_time(funding_history[-1]) - _time(funding_history[0])).total_seconds()
            >= float(parameter("history_min_days", 7)) * 86400
        )
        baseline_cache = _BASELINE_CACHE.get()
        funding_key = (
            (
                "funding",
                history_ready,
                tuple(parameter("normal_range_quantiles", [0.025, 0.975])),
                (_BASELINE_DEPENDENCY.get(), instrument)
                if _BASELINE_DEPENDENCY.get() is not None
                else fingerprint(fh),
            )
            if baseline_cache is not None
            else None
        )
        if baseline_cache is not None and funding_key in baseline_cache:
            fr = baseline_cache[funding_key][0]
        else:
            fr = normal(fh) if history_ready else None
            if baseline_cache is not None:
                baseline_cache[funding_key] = (fr,)
                if len(baseline_cache) > 128:
                    baseline_cache.pop(next(iter(baseline_cache)))
        liqs = [r for r in _instrument_rows(snapshot, "liquidations", instrument) if start <= _time(r) < end]
        cov = (rs[-1] if rs else instruments[instrument]).get("liq_coverage") or (
            "COMPLETE" if venue == "bybit" else "SAMPLED" if venue in ("binance", "okx") else "NOT_SUPPORTED"
        )
        liq_source_coverage = cov
        liq_metadata = ((snapshot.get("capture_metadata") or {}).get("collections") or {}).get(
            "liquidations", {}
        )
        liq_start = (liq_metadata.get("coverage_start") or {}).get(instrument)
        liq_through = (liq_metadata.get("observed_through") or {}).get(instrument)
        liq_window_complete = bool(
            liq_start and liq_through and parse_time(liq_start) <= start and parse_time(liq_through) >= end
        )
        if cov == "COMPLETE" and not liq_window_complete:
            cov = "UNKNOWN"
        startfund = next((f for f in reversed(settled) if _time(f) <= start), {})
        p = dict(
            venue=venue,
            instrument_id=instrument,
            coverage={"venue": venue},
            **stats,
            funding_settled_last=rate,
            funding_settled_interval_h=decimal(last.get("interval_h")),
            funding_settled_last_observed_at=iso(_time(last)) if last else None,
            funding_estimate_observed_at=iso(_time(est[-1])) if est else None,
            funding_estimate=decimal(est[-1].get("rate")) if est else None,
            funding_estimate_interval_h=estimate_interval,
            interval_h=interval,
            funding_8h_eq=eq,
            funding_8h_kind="CURRENT_ESTIMATE" if estimate_eq is not None else "SETTLED",
            funding_settled_8h_eq=settled_eq,
            funding_estimate_8h_eq=estimate_eq,
            funding_vs_normal=("BELOW" if eq < fr[0] else "ABOVE" if eq > fr[1] else "WITHIN")
            if eq is not None and fr
            else None,
            funding_start=decimal(startfund.get("rate")),
            funding_start_interval_h=decimal(startfund.get("interval_h")),
            funding_start_observed_at=iso(_time(startfund)) if startfund else None,
            **liquidation_side_evidence(liqs, liq_window_complete, cov),
            liq_coverage=cov,
            liq_source_coverage=liq_source_coverage,
            liq_capture_window={
                "start": liq_start,
                "observed_through": liq_through,
                "complete": liq_window_complete,
                "reason": None if liq_window_complete else "INCOMPLETE",
            },
        )
        if unresolved:
            stamp = {
                "ts": endpoint.excluded_at,
                "ts_prec": endpoint.excluded_ts_prec,
                "timestamp_precision": endpoint.excluded_timestamp_precision,
                "ts_bucket_size_s": endpoint.excluded_ts_bucket_size_s,
                "ts_bucket_anchor": endpoint.excluded_ts_bucket_anchor,
            }
            p["oi_temporal_exclusions"] = {
                "count": unresolved,
                "reason": "INCOMPLETE",
                "detail": "OI_BUCKET_EDGE_UNSPECIFIED",
                "latest_observation": {
                    **propagate_timestamp(
                        stamp, "ts", precision_aliases=("timestamp_precision", "precision")
                    ).fields("ts"),
                    "oi_native": endpoint.excluded_oi_native,
                    "oi_usd": endpoint.excluded_oi_usd,
                    "scope": "SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT",
                },
            }
        p["basis_bps"] = endpoint.basis_bps if endpoint else None
        per.append(p)
    synchronized = oi_synchronized(
        snapshot, snapshot.get("_primary"), end - start, [(p["instrument_id"], p["venue"]) for p in per]
    )
    eqs = [p["funding_8h_eq"] for p in per if p["funding_8h_eq"] is not None]
    totals = {}
    for key, cov in [("complete_venues_total", "COMPLETE"), ("sampled_venues_min", "SAMPLED")]:
        eligible = [p for p in per if p["liq_coverage"] == cov]
        totals[key] = {
            side: sum(p["liq_" + side + "_usd"] for p in eligible)
            if eligible and all(p["liq_" + side + "_usd"] is not None for p in eligible)
            else None
            for side in ("long", "short")
        }
    for key in tuple(totals):
        totals[key + "_reasons"] = {
            side: "INCOMPLETE" if amount is None else None for side, amount in totals[key].items()
        }
    volumes = {}
    volume_coverage = {}
    evidence = snapshot.get("volume_sql") or truth_sql.NO_VOLUME
    usd = {(r.window, r.instrument_id): r for r in evidence.windows}
    for market in ("SPOT", "PERP"):
        instruments = sorted(i for i in evidence.instruments if (":spot:" in i) == (market == "SPOT"))
        rows = [usd.get((snapshot.get("_primary"), i)) for i in instruments]
        current = [r.current_usd if r else None for r in rows]
        previous = [r.previous_usd if r else None for r in rows]
        complete = bool(instruments) and None not in current and None not in previous
        volumes[market] = max(0.0, sum(current) - sum(previous)) if complete else None
        volume_coverage[market.lower()] = {
            "instruments": instruments,
            "complete": complete,
            "unit": "USD",
            "reason": None if complete else "INCOMPLETE",
        }
    inc = (
        {k.lower(): ratio(v, sum(volumes.values())) for k, v in volumes.items()}
        if None not in volumes.values() and sum(volumes.values())
        else None
    )
    if inc is not None:
        inc["increment_usd"] = {k.lower(): v for k, v in volumes.items()}
    return dict(
        per_venue=per,
        synchronized=synchronized,
        funding_dispersion_8h=max(eqs) - min(eqs) if len(eqs) > 1 else None,
        liquidations=totals,
        incremental_share=inc,
        incremental_volume_coverage=volume_coverage,
        lead_lag=dict(first_mover="UNKNOWN", spot_confirmed=None, sequence=[]),
    )


AS_OF_ALL = datetime(2299, 12, 31, tzinfo=UTC)
DEFI_SECTORS = (
    "DEX_EXECUTION",
    "LENDING_CREDIT",
    "PERP_DEX",
    "RWA",
    "INTEROPERABILITY",
    "SYNTHETIC_DOLLAR",
)


def _knob(config, name, default):
    return (config or {}).get(name, default)


def relative_registry(snapshot):
    """The registry facts the price-relative rules need, from the snapshot's instruments, klines,
    tokens and sector baskets: which token an instrument belongs to, each token's symbol and sector,
    and the instrument chosen for a token (the spot instrument with the most one-minute closes in
    the look-back, `series_coverage_counts`)."""
    token_id = snapshot.get("token_id", "")
    token_symbols = {
        r["token_id"]: r["symbol"].upper()
        for r in snapshot.get("tokens", [])
        if r.get("token_id") and r.get("symbol")
    }
    identities = {}
    spot = set()
    for r in [*snapshot.get("instruments", []), *snapshot.get("klines", [])]:
        iid = r.get("instrument_id")
        if iid and r.get("token_id"):
            identities[iid] = r["token_id"]
            if _market(r) == "SPOT":
                spot.add(iid)
    for r in snapshot.get("instruments", []):
        if r.get("token_id") and (r.get("base_asset") or r.get("base_symbol")):
            token_symbols.setdefault(r["token_id"], str(r.get("base_asset", r.get("base_symbol"))).upper())
    token_symbols.setdefault(token_id, str(token_id).split(":")[-1].upper())
    for identity in identities.values():
        if identity:
            token_symbols.setdefault(identity, str(identity).split(":")[-1].upper())
    counts = snapshot.get("series_coverage_counts")
    counts = counts if isinstance(counts, dict) else None

    def chosen(ids):
        ids = sorted(ids)
        if not ids:
            return None
        best = max(ids, key=lambda iid: counts.get(iid, 0) if counts is not None else 1)
        return best if counts is None or counts.get(best, 0) > 0 else None

    def of_token(identity):
        return [iid for iid in spot if identities.get(iid) == identity]

    def of_symbol(symbol):
        return [iid for iid in spot if token_symbols.get(identities.get(iid)) == symbol]

    sector_members = defaultdict(list)
    token_sectors = {}
    for row in snapshot.get("tokens", []):
        identity, sector = row.get("token_id"), row.get("sector")
        if identity and sector:
            token_sectors[identity] = sector
            sector_members[sector].append(identity)
    for basket in snapshot.get("sector_baskets", []):
        if basket.get("sector") and basket.get("weighting") == "EQUAL":
            sector_members[basket["sector"]] = list(basket.get("constituents", []))
            for identity in sector_members[basket["sector"]]:
                token_sectors[identity] = basket["sector"]
    sector = token_sectors.get(token_id)
    peers = [identity for identity in sector_members.get(sector, []) if identity != token_id]
    defi = [
        identity for name in DEFI_SECTORS for identity in sector_members.get(name, []) if identity != token_id
    ]
    return SimpleNamespace(
        token_id=token_id,
        token_symbols=token_symbols,
        candidates=sorted(of_token(token_id)),
        btc=chosen(of_symbol("BTC")),
        eth=chosen(of_symbol("ETH")),
        sector=sector,
        peers={identity: chosen(of_token(identity)) for identity in dict.fromkeys(peers + defi)},
        sector_peers=peers,
        defi_peers=defi,
        benchmark=token_symbols.get(token_id) == "BTC",
    )


def reference_choice(references, candidates):
    """The reference venue: the candidate instrument of the token with a price and the largest native
    volume over a complete 24 hours of flow (the first by instrument id on equal volume)."""
    volumes = {
        r.instrument_id: r.flow_volume_native
        for r in references
        if r.instrument_id in candidates and r.last_close is not None and r.flow_volume_native is not None
    }
    return max(volumes, key=volumes.get) if volumes else None


def _trade(raw, iid):
    return dict(json.loads(raw), instrument_id=iid) if raw else None


def event_raw_return(reference):
    """The EVENT raw return from the trade prices at the anchor and at the cutoff (the last close
    stands in for a missing cutoff trade); None without an anchor price."""
    if reference is None:
        return None
    anchor, entry = (
        _trade(reference.anchor_trade, reference.instrument_id),
        _trade(reference.entry_trade, reference.instrument_id),
    )
    anchor_price = Decimal(str(anchor["price"])) if anchor else None
    entry_price = Decimal(str(entry["price"])) if entry else reference.last_close
    if not anchor_price or not entry_price:
        return None
    return float(entry_price / anchor_price - 1)


OUTCOME_HORIZONS = (("W12H", 43200), ("W3D", 259200), ("W7D", 604800))


def _horizon_plan(start, end, include_outcome):
    plan = [(name, seconds, end) for name, seconds in WINDOWS.items()]
    plan.append(("EVENT", (end - start).total_seconds(), end))
    plan.append(("PRE24H", 86400, start))
    if include_outcome:
        plan.extend((name, seconds, end) for name, seconds in OUTCOME_HORIZONS)
    return plan


def _time_param(value):
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


async def relative_evidence(ch, snapshot, start, end, config, *, include_outcome=True, baselines=True):
    """Run the price-relative statements of a snapshot (03 section 5.7) and return their records.

    `baselines=False` reads only the reference prices (the hourly close of the reference venue)."""
    registry = relative_registry(snapshot)
    assembled = parse_time(snapshot["assembled_at"]) if snapshot.get("assembled_at") else AS_OF_ALL
    lookback_s = (
        int(_knob(config, "historical_lookback_days", _knob(config, "base_retention_days", 90))) * 86400
    )
    ids = list(dict.fromkeys([*registry.candidates, *([registry.btc] if registry.btc else [])]))
    common = dict(cutoff=end, assembled=assembled, lookback_s=lookback_s)
    jobs = [truth_sql.fetch(ch, truth_sql.ReferencePrice, instrument_ids=ids, start=start, **common)]
    if baselines:
        peer_ids = {
            registry.peers[identity] for identity in registry.sector_peers if registry.peers[identity]
        }
        if len(peer_ids) < 2:
            peer_ids |= {
                registry.peers[identity] for identity in registry.defi_peers if registry.peers[identity]
            }
        names = [name for name, _ in sorted(WINDOWS.items(), key=lambda item: item[1])] + ["EVENT"]
        lengths = dict(WINDOWS, EVENT=(end - start).total_seconds())
        if peer_ids:
            jobs.append(
                truth_sql.fetch(
                    ch,
                    truth_sql.SectorReturn,
                    peer_ids=sorted(peer_ids),
                    names=names,
                    starts=[_time_param(end - timedelta(seconds=lengths[name])) for name in names],
                    ends=[_time_param(end)] * len(names),
                    **common,
                )
            )
    references, *rest = await asyncio.gather(*jobs)
    peers = rest[0] if rest else []
    ref = reference_choice(references, registry.candidates)
    evidence = truth_sql.RelativeEvidence(
        ref=ref, btc=registry.btc, eth=registry.eth, references=tuple(references), peers=tuple(peers)
    )
    if not baselines:
        return evidence
    by_instrument = {r.instrument_id: r for r in references}
    plan = _horizon_plan(start, end, include_outcome)
    raw = event_raw_return(by_instrument.get(ref))
    horizons = await truth_sql.fetch(
        ch,
        truth_sql.RelativeHorizon,
        ref=ref or "",
        btc=registry.btc or "",
        eth=registry.eth or "",
        is_btc=int(registry.benchmark),
        start=start,
        blume=float(_knob(config, "beta_blume_weight", 2 / 3)),
        min_pairs=int(_knob(config, "history_min_days", 7)) * 24,
        event_raw=float("nan") if raw is None else raw,
        names=[name for name, _, _ in plan],
        lens_us=[str(round(seconds * 1_000_000)) for _, seconds, _ in plan],
        ends=[_time_param(at) for _, _, at in plan],
        **common,
    )
    return replace(evidence, horizons=tuple(horizons))


async def volume_evidence(ch, snapshot, end, config, windows):
    """Run the volume statements of a snapshot (03 section 5.4 and 5.7) for these windows, a mapping of
    window name to length in seconds, and return their records."""
    target = snapshot.get("token_id")
    instruments = sorted(
        {i["instrument_id"] for i in snapshot.get("instruments", []) if i.get("token_id", target) == target}
    )
    if not instruments:
        return truth_sql.NO_VOLUME
    assembled = parse_time(snapshot["assembled_at"]) if snapshot.get("assembled_at") else AS_OF_ALL
    names = list(windows)
    lengths = [str(round(seconds * 1_000_000)) for seconds in windows.values()]
    common = dict(cutoff=end, assembled=assembled)
    rows, normals, buckets = await asyncio.gather(
        truth_sql.fetch(
            ch, truth_sql.WindowVolume, instrument_ids=instruments, names=names, lens_us=lengths, **common
        ),
        truth_sql.fetch(
            ch,
            truth_sql.VolumeNormal,
            instrument_ids=instruments,
            names=names,
            lens_us=lengths,
            lookback_s=int(
                _knob(config, "historical_lookback_days", _knob(config, "base_retention_days", 90))
            )
            * 86400,
            min_days=float(_knob(config, "history_min_days", 7)),
            **common,
        ),
        truth_sql.fetch(
            ch,
            truth_sql.VolumeBucket,
            instrument_ids=[i for i in instruments if ":spot:" in i],
            **common,
        ),
    )
    return truth_sql.VolumeEvidence(
        instruments=tuple(instruments), windows=tuple(rows), normals=tuple(normals), buckets=tuple(buckets)
    )


async def oi_evidence(ch, snapshot, end, config, windows, recent_start, *, history=True):
    """Run the open-interest statements of a snapshot (03 section 5.4 and 5.7) for these windows, a mapping of
    window name to length in seconds, and return their records; `history=False` reads only the endpoints.
    `recent_start` is where the history stops being sampled."""
    target = snapshot.get("token_id")
    instruments = sorted(
        (
            i
            for i in snapshot.get("instruments", [])
            if i.get("token_id", target) == target and _market(i) != "SPOT"
        ),
        key=lambda i: i["instrument_id"],
    )
    if not instruments:
        return truth_sql.NO_OI
    ids = [i["instrument_id"] for i in instruments]
    common = dict(
        iids=ids,
        cand=[str(flag) for flag in oi_candidates(instruments, snapshot.get("semantics", []))],
        cutoff=end,
        assembled=parse_time(snapshot["assembled_at"]) if snapshot.get("assembled_at") else AS_OF_ALL,
        names=list(windows),
        lens_us=[str(round(seconds * 1_000_000)) for seconds in windows.values()],
        sync_tol_s=int(_knob(config, "oi_sync_tolerance_s", 60)),
    )
    jobs = [truth_sql.fetch(ch, truth_sql.OiEndpoint, **common)]
    if history:
        lookback_s = (
            int(_knob(config, "historical_lookback_days", _knob(config, "base_retention_days", 90))) * 86400
        )
        jobs.append(
            truth_sql.fetch(
                ch, truth_sql.OiHistory, recent_start=recent_start, lookback_s=lookback_s, **common
            )
        )
    endpoints, *rest = await asyncio.gather(*jobs)
    return truth_sql.OiEvidence(
        instruments=tuple(ids), history=tuple(rest[0]) if rest else (), endpoints=tuple(endpoints)
    )


def float_range(low, high):
    """The 90-day normal range of a measure built with float arithmetic: None when the history is
    missing or when a zero-width range (identical history, float noise alone could exceed it) makes
    it unavailable (DEGENERATE_HISTORY)."""
    if low is None or high is None:
        return None
    return None if abs(high - low) <= 1e-12 * max(1.0, abs(low), abs(high)) else [low, high]


def history_usable(first, last):
    """The history needs at least history_min_days between its first and last close."""
    return bool(
        first and last and (last - first).total_seconds() >= float(parameter("history_min_days", 7)) * 86400
    )


def beta_flag(pairs, ols):
    if pairs < int(parameter("history_min_days", 7)) * 24:
        return "INSUFFICIENT_HISTORY"
    return "NOT_ESTIMABLE" if ols is None else None


def repricing(value, event, day, sign):
    """The repricing band of a signed EVENT measure against the 90-day history of the EVENT window and of
    the 24 h window: each history is (q025, q25, q75, q975), None where it is unavailable; with
    sign -1 (a loser) the histories are mirrored."""

    def mirrored(history):
        low, lower, upper, high = history[:4]
        if sign == 1:
            return float_range(low, high), upper
        return float_range(None if high is None else -high, None if low is None else -low), (
            None if lower is None else -lower
        )

    er, upper = mirrored(event)
    dr, _ = mirrored(day)
    if value is None:
        return None
    if dr and value > dr[1]:
        return "HEAVY"
    if not er:
        return None
    if value < er[0]:
        return "NEGATIVE"
    if value > er[1]:
        return "SUBSTANTIAL"
    return "EARLY" if value > upper else "UNPRICED"


def market_context(r, rv, returns, vols):
    """The BTC context: returns = (q025, q25) of the 24 h return and vols = (q025, q975) of the 24 h
    realised volatility over the 90-day history."""
    if None in returns or None in vols or r is None or rv is None:
        return None
    return "STRESS" if r < returns[0] or rv > vols[1] else "HEADWIND" if r < returns[1] else "NEUTRAL"


def relative_truth(snapshot, start, end, *, include_outcome=True):
    evidence = snapshot.get("relative_sql") or truth_sql.NO_RELATIVE
    registry = relative_registry(snapshot)
    ref, benchmark = evidence.ref, registry.benchmark
    rows = {h.name: h for h in evidence.horizons}
    by_instrument = {r.instrument_id: r for r in evidence.references}
    reference = by_instrument.get(ref)
    ref_coverage = {}
    for r in evidence.references:
        if r.instrument_id not in registry.candidates or r.last_close is None:
            continue
        complete = r.flow_volume_native is not None
        ref_coverage[r.instrument_id] = dict(
            complete=complete,
            expected_duration_s=86400,
            boundary_segments=[],
            excluded=[]
            if complete
            else [
                dict(
                    venue=r.instrument_id.split(":")[0],
                    reason="INCOMPLETE",
                    detail=r.flow_detail,
                )
            ],
            volume_unit="NATIVE_TOKEN",
            covered_volume_native=r.flow_volume_native,
        )
    cutoff_row = _trade(reference.entry_trade, ref) if reference else None
    anchor_row = _trade(reference.anchor_trade, ref) if reference else None
    anchor_price = decimal(anchor_row.get("price")) if anchor_row else None
    entry_price = (
        decimal(cutoff_row.get("price")) if cutoff_row else reference.last_close if reference else None
    )
    last_close_at = reference.last_close_time if reference else None
    any_horizon = next(iter(rows.values()), None)
    bb = any_horizon.beta_btc if any_horizon else 1.0
    be = any_horizon.beta_eth if any_horizon else 1.0
    bflag = (
        beta_flag(any_horizon.beta_btc_pairs, any_horizon.beta_btc_ols)
        if any_horizon
        else "INSUFFICIENT_HISTORY"
    )
    eflag = (
        beta_flag(any_horizon.beta_eth_pairs, any_horizon.beta_eth_ols)
        if any_horizon
        else "INSUFFICIENT_HISTORY"
    )
    sample = any_horizon
    resolution = (
        int(sample.history_resolution_s) if sample and sample.history_resolution_s is not None else None
    )
    usable = bool(sample) and history_usable(sample.price_first_close, sample.price_last_close)
    interval = next(
        (
            r.get("interval_s", 60)
            for r in reversed(snapshot.get("klines", []))
            if r.get("instrument_id") == ref
        ),
        60,
    )

    def history(h, length):
        """The usable part of one horizon's history: its range, its sample count and diagnostics."""
        n = h.sample_count if h is not None and usable else 0
        diagnostic = None
        if h is not None and usable:
            k = -(-int(round(length * 1_000_000)) // 60_000_000)
            duration, lag = float(k * 60), float(k * 60 - length)
            diagnostic = dict(
                target_window_duration_s=float(length),
                observed_duration_bounds_s=[duration, duration] if n else None,
                endpoint_staleness_bounds_s=[lag, lag] if n else None,
                exact=bool(n) and lag == 0,
                resolution_s=resolution,
                alignment="AS_OF_AT_EXACT_TARGET_BOUNDS",
                sample_count=n,
                latest_observed_at=iso(h.history_last_end) if n else None,
            )
        return n, diagnostic

    windows = {}
    diagnostics = {}
    lengths = dict(WINDOWS, EVENT=(end - start).total_seconds())
    for name, length in lengths.items():
        h = rows.get(name)
        n, diagnostic = history(h, length)
        diagnostics[name] = diagnostic
        r = h.raw_return if h else None
        adj = None if benchmark else h.btc_adjusted_return if h else None
        measure = r if benchmark else adj
        rng = float_range(h.history_q025, h.history_q975) if n else None
        windows[name] = dict(
            window=name,
            start=iso(end - timedelta(seconds=length)),
            cutoff=iso(end),
            observed_at=iso(last_close_at) if last_close_at else None,
            precision=f"BUCKET({int(interval)}s)" if last_close_at else None,
            coverage={"reference_venue": ref, "complete_24h": bool(ref)},
            r_raw=r,
            r_btc_adj=adj,
            r_btc_adj_reason="NOT_APPLICABLE_BENCHMARK" if benchmark else None,
            relative_measure="R_RAW" if benchmark else "R_BTC_ADJ",
            relative_return=measure,
            r_eth_adj=h.eth_adjusted_return if h else None,
            r_sector_adj=None,
            sector_reason="NOT_ESTIMABLE",
            pctile=h.percentile if n and measure is not None else None,
            normal_range=rng,
            normal_range_reason="INCOMPLETE" if not n else "DEGENERATE_HISTORY" if rng is None else None,
            beyond_normal=beyond(measure, rng),
            historical_window_resolution_s=resolution,
            historical_window_duration_s=length,
            requested_window_duration_s=length,
            historical_alignment="AS_OF_AT_EXACT_TARGET_BOUNDS",
            historical_measurement=diagnostic
            or {"exact": False, "reason": "INCOMPLETE", "target_window_duration_s": length},
        )

    sector = registry.sector
    returns = {(r.instrument_id, r.name): r.peer_return for r in evidence.peers}
    with_series = {r.instrument_id for r in evidence.peers}
    tradable = [identity for identity in registry.sector_peers if registry.peers[identity] in with_series]
    if len(tradable) < 2:
        tradable = [identity for identity in registry.defi_peers if registry.peers[identity] in with_series]
    not_applicable = sector == "IDENTITY_PAYMENTS"
    for name in lengths:
        peer_returns = [
            returns.get((registry.peers[identity], name))
            for identity in tradable
            if returns.get((registry.peers[identity], name)) is not None
        ]
        raw = windows[name]["r_raw"]
        windows[name]["r_sector_adj"] = (
            raw - mean(peer_returns)
            if not not_applicable and sector and raw is not None and len(peer_returns) >= 2
            else None
        )
        windows[name]["sector_reason"] = (
            "NOT_APPLICABLE"
            if not_applicable
            else None
            if windows[name]["r_sector_adj"] is not None
            else "NOT_ESTIMABLE"
        )
    sign = -1 if snapshot.get("expected_direction") in ("BEARISH", "LOSER", -1) else 1
    eventr = windows["EVENT"]["relative_return"]
    pre = rows.get("PRE24H")
    pre_n = pre.sample_count if pre is not None and usable else 0
    n_event = diagnostics["EVENT"]["sample_count"] if diagnostics.get("EVENT") else 0

    def quantiles(h, n):
        return (
            (h.history_q025, h.history_q25, h.history_q75, h.history_q975)
            if h is not None and n
            else (None,) * 4
        )

    ar = pre.measure if pre is not None else None
    event_row = rows.get("EVENT")
    ctx_usable = bool(sample) and history_usable(sample.btc_first_close, sample.btc_last_close)
    ctx = rows.get("W24H") or sample
    br = ctx.btc_return if ctx else None
    rv = ctx.btc_realized_vol_24h if ctx else None
    return_q = (
        (ctx.btc_return_24h_q025, ctx.btc_return_24h_q25, ctx.btc_return_24h_q975)
        if ctx and ctx_usable and ctx.btc_return_24h_samples
        else (None,) * 3
    )
    vol_q = (
        (ctx.btc_realized_vol_24h_q025, ctx.btc_realized_vol_24h_q975)
        if ctx and ctx_usable and ctx.btc_realized_vol_24h_samples
        else (None,) * 2
    )
    context = market_context(br, rv, (return_q[0], return_q[1]), vol_q)
    horizon_ranges = {}
    for name, length in (
        [("W1H", 3600), ("W4H", 14400), ("W12H", 43200), ("W24H", 86400), ("W3D", 259200), ("W7D", 604800)]
        if include_outcome
        else []
    ):
        h = rows.get(name)
        n, diagnostic = history(h, length)
        horizon_ranges[name] = dict(
            value=float_range(h.history_q025, h.history_q975) if n else None,
            unit="DECIMAL_RETURN",
            window=name,
            start=iso(start - timedelta(days=lookback_days())),
            cutoff=iso(start),
            observed_at=diagnostic["latest_observed_at"] if diagnostic else None,
            precision=f"BUCKET({resolution}s)" if resolution else None,
            coverage={"sample_count": n},
            reason=None if n else "NOT_ESTIMABLE",
            historical_measurement=diagnostic or {},
        )
    btc_reference = by_instrument.get(registry.btc) if registry.btc else None
    btc_last = _trade(btc_reference.entry_trade, registry.btc) if btc_reference else None
    btc_entry = (
        decimal(btc_last.get("price")) if btc_last else btc_reference.last_close if btc_reference else None
    )
    outcome_btc = dict(
        instrument_id=registry.btc,
        entry_ref_price=btc_entry,
        observed_at=iso(parse_time(btc_last["ts"]))
        if btc_last
        else iso(btc_reference.last_close_time)
        if btc_reference and btc_reference.last_close_time
        else None,
        price_source="LAST_TRADE" if btc_last else "KLINE_CLOSE" if btc_entry is not None else None,
        reason=None if btc_entry is not None else "INCOMPLETE",
    )
    return dict(
        expected_direction=snapshot.get("expected_direction"),
        repricing_sign=sign,
        outcome_btc_reference=outcome_btc,
        normal_ranges=horizon_ranges,
        entry_ref_price=entry_price,
        entry_price_source="LAST_TRADE" if cutoff_row else "KLINE_CLOSE",
        entry_price_observed_at=iso(parse_time(cutoff_row["ts"]))
        if cutoff_row
        else iso(last_close_at)
        if last_close_at
        else None,
        close_1h_price=reference.hourly_close if reference else None,
        ref_venue=ref,
        reference_coverage=ref_coverage,
        reference_reason=None if ref else "INCOMPLETE",
        anchor=dict(
            type=snapshot.get("anchor", {}).get("type", "KNOWN_TO_US_AT")
            if isinstance(snapshot.get("anchor"), dict)
            else "KNOWN_TO_US_AT",
            ts=iso(start),
            ref_price=anchor_price,
            price_observed_at=iso(parse_time(anchor_row["ts"])) if anchor_row else None,
            evidence=anchor_row,
            price_reason=None if anchor_row else "INCOMPLETE",
            price_source="LAST_TRADE" if anchor_row else None,
        ),
        windows=windows,
        beta_btc=bb,
        beta_eth=be,
        beta_flags=dict(btc=bflag, eth=eflag),
        pre_event_runup=dict(
            window="PRE_EVENT_W24H",
            start=iso(start - timedelta(days=1)),
            cutoff=iso(start),
            ar=ar,
            pctile=pre.percentile if pre_n and ar is not None else None,
            flag=beyond(ar, float_range(pre.history_q025, pre.history_q975) if pre_n else None),
        ),
        repricing_band=repricing(
            eventr * sign if eventr is not None else None,
            quantiles(event_row, n_event),
            quantiles(pre, pre_n),
            sign,
        ),
        market_context=context,
        context_inputs=dict(
            btc_return_24h=br,
            rv24=rv,
            rv24_observed_cutoff=iso(end.replace(minute=0, second=0, microsecond=0)),
            rv24_resolution_s=3600,
            return_normal_range=[return_q[0], return_q[2]] if None not in return_q else None,
            return_lower_quartile=return_q[1],
            rv_normal_range=list(vol_q) if None not in vol_q else None,
            rule="normal_range",
        ),
    )


@precise
def walk_book(levels, notional, mid, side="ask"):
    remaining = decimal(notional)
    mid = decimal(mid)
    native = Decimal(0)
    spent = Decimal(0)
    for level in levels:
        p, q = (
            (decimal(level[k]) for k in ("price", "qty"))
            if isinstance(level, dict)
            else map(decimal, level[:2])
        )
        take = min(remaining, p * q)
        native += take / p
        spent += take
        remaining -= take
        if remaining <= 1e-8:
            break
    return float(((spent / native / mid) - 1) * 10000) if native and remaining <= 1e-8 else None


def depth_at_band(book, side, unit="usd"):
    covered = book.get("range_covered_pct") or {}
    coverage = number(covered.get(side)) if isinstance(covered, dict) else number(covered)
    band = float(parameter("depth_band_pct", 1))
    return (
        decimal(
            (
                ((book.get("depth") or {}).get(side) or {}).get({0.5: "p0_5", 1.0: "p1", 2.0: "p2"}[band])
                or {}
            ).get(unit)
        )
        if coverage is not None and coverage >= band
        else None
    )


@precise
def liquidity_truth(snapshot, start, end, spot, relative):
    books = [
        b
        for b in snapshot.get("books", [])
        if _market(b) == "SPOT"
        and b.get("token_id", snapshot.get("token_id")) == snapshot.get("token_id")
        and _time(b) <= end
        and b.get("integrity_ok") is True
    ]
    groups = defaultdict(list)
    for b in books:
        groups[_venue(b)].append(b)
    eligible_venues = {v["venue"] for v in spot.get("venues", [])}
    flow = [
        r
        for r in snapshot.get("flow_bars", [])
        if _market(r) == "SPOT"
        and _venue(r) in eligible_venues
        and start <= _time(r)
        and _time(r) + timedelta(minutes=1) <= end
        and r.get("complete") is True
    ]
    for v in spot.get("venues", []):
        for segment in v.get("boundary_segments", []):
            for row in segment.get("aggregates", []):
                flow.append(dict(row, minute_start=row["start"], segment_end=row["end"], venue=v["venue"]))
    by_bucket = defaultdict(dict)
    for b in (snapshot.get("volume_sql") or truth_sql.NO_VOLUME).buckets:
        by_bucket[b.bucket_start][b.venue] = b
    expected_venues = {v["venue"] for v in spot["venues"]}
    covered_volumes = []
    for venues in by_bucket.values():
        selected = [venues.get(venue) for venue in expected_venues]
        if expected_venues and all(
            b is not None and b.minutes == 15 and not b.incomplete and not b.missing for b in selected
        ):
            covered_volumes.append(sum(b.notional for b in selected))
    impact_tiers = parameter("impact_tiers_usd", [100000, 1000000, 5000000])
    k = (
        min(Decimal(1), decimal(mean(covered_volumes)) / decimal(impact_tiers[2]))
        if covered_volumes
        else None
    )
    tiers = {
        name: decimal(size) * k if k is not None else None
        for name, size in list(zip(("T1", "T2", "T3"), impact_tiers, strict=False))
    }
    per = []
    predepth = []
    nowdepth = []
    side_now = {"bid": [], "ask": []}
    side_pre = {"bid": [], "ask": []}

    venue_now = {"bid": {}, "ask": {}}
    venue_pre = {"bid": {}, "ask": {}}
    spreads = []
    prespreads = []
    burst_results = []

    depth = depth_at_band

    pre_anchor = snapshot.get("_event_anchor", start)
    book_interval = timedelta(seconds=float(parameter("book_interval_s", 10)))
    refill = timedelta(seconds=float(parameter("refill_window_s", 60)))
    resilience = timedelta(minutes=float(parameter("resilience_window_min", 15)))
    venue_flow = defaultdict(list)
    for f in flow:
        venue_flow[_venue(f)].append(f)
    for venue, rs in groups.items():
        rs.sort(key=_time)
        times = [_time(x) for x in rs]
        flows = venue_flow.get(venue, [])
        by_time = sorted(flows, key=_time)
        flow_times = [_time(x) for x in by_time]
        b = rs[-1]
        bid, ask = depth(b, "bid"), depth(b, "ask")
        exact_context(bid, ask)
        d = bid + ask if bid is not None and ask is not None else None
        for side, value in [("bid", bid), ("ask", ask)]:
            if value is not None:
                side_now[side].append(value)
                venue_now[side][venue] = (value, depth(b, side, "native"))
        pre = (
            rs[bisect_left(times, pre_anchor - timedelta(hours=1)) : bisect_left(times, pre_anchor)]
            if pre_anchor is not None
            else []
        )
        for side in ("bid", "ask"):
            vals = [depth(x, side) for x in pre if depth(x, side) is not None]
            if vals:
                side_pre[side].append(mean(vals))
                natives = [depth(x, side, "native") for x in pre]
                venue_pre[side][venue] = (
                    mean(vals),
                    mean(natives) if natives and None not in natives else None,
                )
        pd = [
            depth(x, "bid") + depth(x, "ask")
            for x in pre
            if depth(x, "bid") is not None and depth(x, "ask") is not None
        ]
        if pd:
            predepth.append(mean(pd))
        if d is not None:
            nowdepth.append(d)
        spread = number(b.get("spread_bps"))
        spreads.append(spread) if spread is not None else None
        ps = [number(x["spread_bps"]) for x in pre if x.get("spread_bps") is not None]
        if ps:
            prespreads.append(mean(ps))
        fx = Decimal(1) if b.get("quote_asset") == "USD" else decimal(b.get("quote_usd_rate"))
        mid = decimal(b.get("mid"))
        levels = b.get("asks", b.get("levels", {}).get("asks", []))
        per.append(
            dict(
                venue=venue,
                coverage={
                    "venue": venue,
                    "range_covered_pct": b.get("range_covered_pct"),
                    "integrity_ok": b.get("integrity_ok"),
                },
                observed_at=iso(_time(b)),
                precision=propagate_timestamp(
                    b, "exchange_ts", precision_aliases=("timestamp_precision",)
                ).precision,
                spread_bps=spread,
                depth_p1_usd=d,
                bid_depth_p1_usd=bid,
                ask_depth_p1_usd=ask,
                range_covered_pct=b.get("range_covered_pct"),
                tier_notional=tiers,
                slippage_bps={
                    name: walk_book(levels, n / fx, mid) if n and mid and levels and fx else None
                    for name, n in tiers.items()
                },
            )
        )
        for f in flows:
            t = _time(f)
            k = bisect_left(times, t) - 1
            if k < 0 or times[k] < t - book_interval:
                continue
            baseline = rs[k]
            bd = depth(baseline, "bid")
            sell, buy = decimal(f.get("sell_notional")), decimal(f.get("buy_notional"))
            if sell is None or buy is None:
                continue
            net = sell - buy
            if bd is None or net < bd:
                continue
            after = [
                x
                for x in rs[bisect_right(times, t) : bisect_right(times, min(t + refill, end))]
                if depth(x, "bid") is not None and depth(x, "bid") >= bd
            ]
            recovered = bool(after)
            future = [
                decimal(x["low"])
                for x in by_time[bisect_right(flow_times, t) : bisect_right(flow_times, t + resilience)]
                if x.get("low") is not None
            ]
            low = decimal(f.get("low"))
            resilient = (min(future) >= low) if future and low is not None and t + resilience <= end else None
            burst_results.append(
                dict(
                    venue=venue,
                    ts=iso(t),
                    replenished=recovered if t + refill <= end else None,
                    recovery_s=(_time(after[0]) - t).total_seconds() if after else None,
                    resilient=resilient,
                )
            )
    aggregate = sum(nowdepth) if nowdepth and len(nowdepth) == len(groups) else None
    pre = sum(predepth) if predepth and len(predepth) == len(groups) else None
    drop = float(1 - aggregate / pre) if aggregate is not None and pre else None
    bn, sn = _sum(flow, "buy_notional"), _sum(flow, "sell_notional")
    net_usd = bn - sn if bn is not None and sn is not None else None

    bq, sq = _sum(flow, "buy_qty"), _sum(flow, "sell_qty")
    if bq is not None and sq is not None:
        net, unit, k = bq - sq, "NATIVE", 1
    else:
        net, unit, k = net_usd, "USD", 0
    raw = relative["windows"].get(snapshot.get("_primary", "W4H"), {}).get("r_raw")
    beyondprice = relative["windows"].get(snapshot.get("_primary", "W4H"), {}).get("beyond_normal")
    side_totals = {
        side: sum(values) if values and len(values) == len(groups) else None
        for side, values in side_now.items()
    }
    pre_totals = {
        side: sum(values) if values and len(values) == len(groups) else None
        for side, values in side_pre.items()
    }
    hit_side = "ask" if net is not None and net > 0 else "bid" if net is not None and net < 0 else None
    hit_depth = side_totals.get(hit_side)
    pre_hit_depth = pre_totals.get(hit_side)
    all_venues = set(groups)

    def lower_bound(table, side, index):
        """Sum over the venues whose band is covered: a lower bound of the depth (exact when
        every venue is covered); venues without the band are never extrapolated."""
        rows = {v: t[index] for v, t in table.get(side, {}).items() if t[index] is not None}
        return (
            (sum(rows.values()) if rows else None),
            sorted(rows),
            bool(all_venues) and set(rows) == all_venues,
        )

    bound, covered, complete = lower_bound(venue_now, hit_side, k) if hit_side else (None, [], False)
    pre_bound, pre_covered, pre_complete = (
        lower_bound(venue_pre, hit_side, k) if hit_side else (None, [], False)
    )
    average_hit_depth = []
    if hit_side:
        for rs in groups.values():
            values = [
                depth(b, hit_side) for b in rs if start <= _time(b) <= end and depth(b, hit_side) is not None
            ]
            if values:
                average_hit_depth.append(mean(values))
    avg = sum(average_hit_depth) if average_hit_depth and len(average_hit_depth) == len(groups) else None
    small_price = (
        (raw if net > 0 else -raw) < float(parameter("depth_band_pct", 1)) / 100
        if net and raw is not None
        else None
    )
    absorbed = None if not net or bound is None else False if abs(net) < bound else True if complete else None
    weak = (
        None
        if not net
        else False
        if absorbed is False or small_price is False
        else True
        if absorbed and small_price
        else None
    )
    excess = (
        False
        if beyondprice is False
        else (True if abs(net) < pre_bound else False if pre_complete else None)
        if beyondprice is True and net is not None and pre_bound is not None
        else None
    )
    cohorts = {
        side: sorted(
            v for v in set(venue_now[side]) & set(venue_pre[side]) if venue_pre[side][v][0] is not None
        )
        for side in ("bid", "ask")
    }
    cohort_now = {side: sum(venue_now[side][v][0] for v in cohorts[side]) for side in cohorts}
    cohort_pre = {side: sum(venue_pre[side][v][0] for v in cohorts[side]) for side in cohorts}

    side_drops = {
        side: float(1 - cohort_now[side] / cohort_pre[side]) if cohorts[side] and cohort_pre[side] else None
        for side in ("bid", "ask")
    }
    drop = side_drops.get(hit_side) if hit_side else None
    depth_majority_lost = (
        bool(cohort_now[hit_side] * 2 < cohort_pre[hit_side])
        if hit_side and cohorts[hit_side] and cohort_pre[hit_side]
        else None
    )
    vacuum = (
        bool(beyondprice and depth_majority_lost and excess)
        if drop is not None and excess is not None and beyondprice is not None
        else None
    )
    thin_sides = []
    for side in ("bid", "ask"):
        low, _, full = lower_bound(venue_now, side, 0)
        thin_sides.append(
            None
            if low is None or tiers["T2"] is None
            else low < tiers["T2"]
            if full
            else False
            if low >= tiers["T2"]
            else None
        )
    thin = True if True in thin_sides else False if thin_sides == [False, False] else None
    return dict(
        depth_measurement_band_pct=parameter("depth_band_pct", 1),
        per_venue=per,
        aggregate_depth_p1_usd=aggregate,
        aggregate_bid_depth_p1_usd=side_totals["bid"],
        aggregate_ask_depth_p1_usd=side_totals["ask"],
        pre_event_depth_p1_usd=pre,
        pre_event_depth_window={
            "start": iso(pre_anchor - timedelta(hours=1)),
            "cutoff": iso(pre_anchor),
            "anchor_type": "EVENT",
        }
        if pre_anchor is not None
        else {"start": None, "cutoff": None, "reason": "INCOMPLETE"},
        pre_event_side_depth_p1_usd=pre_totals,
        depth_drop_by_side=side_drops,
        depth_drop=drop,
        depth_majority_lost=depth_majority_lost,
        depth_drop_cohort=cohorts,
        spread_ratio=ratio(mean(spreads), mean(prespreads)) if spreads and prespreads else None,
        replenishment=dict(
            available=bool(books and flow),
            bursts=len(burst_results) if books and flow else None,
            replenished=sum(x["replenished"] is True for x in burst_results) if books and flow else None,
            resilient_bursts=sum(x["replenished"] is True and x["resilient"] is True for x in burst_results)
            if books and flow
            else None,
            median_recovery_s=median([x["recovery_s"] for x in burst_results if x["recovery_s"] is not None])
            if any(x["recovery_s"] is not None for x in burst_results)
            else None,
            observations=burst_results,
        ),
        elasticity=dict(
            bps_per_depth_unit=raw * 10000 / float(abs(net_usd) / avg)
            if raw is not None and net_usd and avg
            else None,
            weak_response=weak,
            excess_response=excess,
            hit_side=hit_side,
            depth_unit=unit,
            net_flow=net,
            hit_side_depth_lower_bound=bound,
            hit_side_covered_venues=covered,
            hit_side_coverage_complete=complete,
            pre_event_hit_side_depth_lower_bound=pre_bound,
            pre_event_hit_side_covered_venues=pre_covered,
            hit_side_depth_p1_usd=hit_depth,
            pre_event_hit_side_depth_p1_usd=pre_hit_depth,
            avg_hit_side_depth_p1_usd=avg,
        ),
        vol_compression=dict(rv24=None, normal_range=None, state=None, reason="INCOMPLETE"),
        liquidity_state="VACUUM" if vacuum else ("THIN" if thin else "NORMAL") if thin is not None else None,
    )


def _count(n, noun):
    """'1 complete venue', '3 complete venues'."""
    return f"{n} {noun}" + ("" if n == 1 else "s")


def _quote(row):
    return str(row.get("quote_asset") or "").upper()


def _usd_mid(row, mid):
    """A mid in USD: USD quotes as is, others by the row's stored quote_usd_rate (None without)."""
    if mid is None:
        return None
    if _quote(row) == "USD":
        return mid
    rate = decimal(row.get("quote_usd_rate"))
    return mid * rate if rate else None


def enrich_evidence(snapshot, start, end, spots, primary, deriv, relative, liquidity):
    """Compute cross-window claims only on pairwise common, complete venues."""
    for name, left in spots.items():
        left["cohort_comparisons"] = {}
        for other, right in spots.items():
            if name == other:
                continue
            cohort = set(x["venue"] for x in left["venues"]) & set(x["venue"] for x in right["venues"])
            a = _composite([x for x in left["venues"] if x["venue"] in cohort])
            b = _composite([x for x in right["venues"] if x["venue"] in cohort])
            share = ratio(a["volume"], left["composite_max"]["volume"])
            share_majority = strict_majority(a["volume"], left["composite_max"]["volume"])
            valid = len(cohort) >= int(parameter("min_spot_venues", 2)) and share_majority is True
            left["cohort_consistent"][other] = valid
            left["cohort_comparisons"][other] = dict(
                cohort=sorted(cohort),
                current=a if valid else None,
                other=b if valid else None,
                share_of_max=share,
                share_majority=share_majority,
                reason=None if valid else "NOT_COHORT_CONSISTENT",
            )
    sem = snapshot.get("semantics", [])
    aggregation = {
        s.get("venue"): s.get("trade_aggregation")
        for s in sem
        if s.get("trade_aggregation") not in (None, "N_A")
    }
    classes = defaultdict(list)
    for v in spots[primary]["venues"]:
        if v["venue"] in aggregation:
            classes[aggregation[v["venue"]]].append(v["venue"])
    large_threshold = decimal(parameter("large_ticket_usd", 100000))
    bucket_edges = [decimal(v) for v in parameter("size_buckets_usd", [1000, 10000, 100000, 1000000])]
    large_buckets = [f"B{i + 2}" for i, edge in enumerate(bucket_edges) if edge >= large_threshold]
    if classes and large_threshold in bucket_edges:
        compatible = max(classes.values(), key=len)
        venue_ids = {
            r["instrument_id"]
            for r in snapshot.get("flow_bars", [])
            if _venue(r) in compatible and _market(r) == "SPOT"
        }
        current_rows = [flow_window(snapshot, iid, start, end) for iid in venue_ids]
        bars = (
            [row for rows in current_rows if rows is not None for row in rows]
            if all(rows is not None for rows in current_rows)
            else []
        )
        buy = sell = Decimal(0)
        valid = True
        for r in bars:
            buckets = r.get("size_buckets")
            if not buckets:
                valid = False
                break
            for k in large_buckets:
                b = buckets.get(k)
                if not b or b.get("buy_notional") is None or b.get("sell_notional") is None:
                    valid = False
                    break
                buy += decimal(b["buy_notional"])
                sell += decimal(b["sell_notional"])
        if valid and bars:
            total = _sum(bars, "buy_notional")
            sells = _sum(bars, "sell_notional")
            total = total + sells if total is not None and sells is not None else None
            previous_rows = [flow_window(snapshot, iid, start - (end - start), start) for iid in venue_ids]
            previous = (
                [row for rows in previous_rows if rows is not None for row in rows]
                if all(rows is not None for rows in previous_rows)
                else []
            )
            prev_buy = prev_sell = Decimal(0)
            prev_valid = bool(previous)
            for r in previous:
                for k in large_buckets:
                    bucket = (r.get("size_buckets") or {}).get(k) or {}
                    if bucket.get("buy_notional") is None or bucket.get("sell_notional") is None:
                        prev_valid = False
                        break
                    prev_buy += decimal(bucket["buy_notional"])
                    prev_sell += decimal(bucket["sell_notional"])
            old_share = ratio(prev_sell, prev_buy + prev_sell) if prev_valid else None
            new_share = ratio(sell, buy + sell)
            spots[primary]["size_profile"] = dict(
                large_ticket_buy_share=ratio(buy, buy + sell),
                large_ticket_sell_share=new_share,
                large_ticket_buy_majority=strict_majority(buy, buy + sell),
                large_tickets_minority=(2 * (buy + sell) < total) if total else None,
                large_ticket_buy_notional=buy,
                large_ticket_sell_notional=sell,
                sell_share_rising=(sell * (prev_buy + prev_sell) > prev_sell * (buy + sell))
                if old_share is not None and new_share is not None
                else None,
                compatible_venues=compatible,
            )

    nr = relative["windows"]["W15M"]["normal_range"]
    sequence = []
    groups = defaultdict(list)
    for r in snapshot.get("klines", []):
        if (
            r.get("token_id", snapshot.get("token_id")) == snapshot.get("token_id")
            and snapshot.get("_lead_start", start) <= _time(r) < end
        ):
            groups[r["instrument_id"]].append(r)
    complete = True
    for _instrument, rows in groups.items():
        rows.sort(key=_time)
        if not all(r.get("complete") is True for r in rows):
            complete = False
            continue
        if any((_time(b) - _time(a)).total_seconds() != 60 for a, b in zip(rows, rows[1:], strict=False)):
            complete = False
            continue
        p0 = number(rows[0].get("o"))
        if not p0 or nr is None:
            continue
        for r in rows:
            p1 = number(r.get("c"))
            if p1 is not None and beyond(p1 / p0 - 1, nr):
                sequence.append(
                    dict(
                        venue=_venue(r),
                        market="SPOT" if _market(r) == "SPOT" else "PERP",
                        t_first=iso(_time(r)),
                    )
                )
                break
    first = {
        m: min((parse_time(r["t_first"]) for r in sequence if r["market"] == m), default=None)
        for m in ("SPOT", "PERP")
    }
    mover = "UNKNOWN"
    if complete and first["SPOT"] and first["PERP"]:
        delta = (first["PERP"] - first["SPOT"]).total_seconds()
        mover = "SPOT" if delta >= 60 else "PERP" if delta <= -60 else "SIMULTANEOUS"
    elif (
        complete
        and sequence
        and {"SPOT", "PERP"}
        <= {("SPOT" if _market(r) == "SPOT" else "PERP") for rows in groups.values() for r in rows}
    ):
        mover = "SPOT" if first["SPOT"] else "PERP"
    lead_window = "EVENT" if snapshot.get("_has_event") else primary
    pr = relative["windows"][lead_window]["r_raw"]
    direction = spots[lead_window]["direction"]
    cohort = spots[lead_window]["composite_cohort"]
    deriv["lead_lag"] = dict(
        window=lead_window,
        start=iso(snapshot.get("_lead_start", start)),
        cutoff=iso(end),
        first_mover=mover,
        spot_confirmed=(direction == ("BUY" if pr > 0 else "SELL" if pr < 0 else "NEUTRAL"))
        if pr is not None and cohort
        else None,
        sequence=sequence,
    )
    tiers = liquidity["per_venue"][0]["tier_notional"] if liquidity["per_venue"] else {}
    best = {
        tier: min(
            (v["slippage_bps"][tier] for v in liquidity["per_venue"] if v["slippage_bps"][tier] is not None),
            default=None,
        )
        for tier in ("T1", "T2", "T3")
    }
    split = {}
    latest = {}
    for b in snapshot.get("books", []):
        if _market(b) != "SPOT" or b.get("token_id", snapshot.get("token_id")) != snapshot.get("token_id"):
            continue
        if b.get("integrity_ok") is True and (_venue(b) not in latest or _time(b) > _time(latest[_venue(b)])):
            latest[_venue(b)] = b
    askdepth = {
        v: decimal((b.get("depth", {}).get("ask", {}).get("p1") or {}).get("usd")) for v, b in latest.items()
    }
    total = sum(x for x in askdepth.values() if x is not None)
    for tier in ("T1", "T2", "T3"):
        fills = []
        if tiers.get(tier) and total and all(x is not None for x in askdepth.values()):
            for venue, b in latest.items():
                allocation = tiers[tier] * askdepth[venue] / total
                levels = b.get("asks", b.get("levels", {}).get("asks", []))
                mid = decimal(b.get("mid"))
                fx = Decimal(1) if b.get("quote_asset") == "USD" else decimal(b.get("quote_usd_rate"))
                impact = (
                    walk_book(levels, allocation / fx, mid) if levels and mid and allocation and fx else None
                )
                fills.append((allocation, impact))
        split[tier] = (
            sum(float(n / tiers[tier]) * i for n, i in fills)
            if fills and all(i is not None for _, i in fills)
            else None
        )
    liquidity["best_single_venue_slippage_bps"] = best
    liquidity["split_slippage_bps"] = split
    ci = relative["context_inputs"]
    liquidity["vol_compression"] = dict(
        rv24=ci["rv24"] if snapshot.get("token_id", "").upper() == "BTC" else None,
        normal_range=ci["rv_normal_range"] if snapshot.get("token_id", "").upper() == "BTC" else None,
        state=None,
        reason="NOT_ESTIMABLE",
    )

    books = [
        b
        for b in snapshot.get("books", [])
        if _market(b) == "SPOT"
        and b.get("token_id", snapshot.get("token_id")) == snapshot.get("token_id")
        and b.get("integrity_ok") is True
        and _time(b) <= end
    ]
    grouped = defaultdict(list)
    for book in books:
        grouped[_venue(book)].append(book)
    for rows in grouped.values():
        rows.sort(key=_time)
    grouped_times = {venue: [_time(r) for r in rows] for venue, rows in grouped.items()}
    frag_start = end - timedelta(minutes=15)
    cadence = float(parameter("book_interval_s", 10))
    pairs = []
    persistent = False
    validated = False
    closed = False
    capture = spot_truth(
        snapshot.get("flow_bars", []),
        snapshot.get("tickers", []),
        [
            i
            for i in snapshot.get("instruments", [])
            if i.get("token_id", snapshot.get("token_id")) == snapshot.get("token_id")
        ],
        frag_start,
        end,
        flow_segments=flow_segments(snapshot),
        assembled_at=snapshot.get("assembled_at"),
    )
    complete_venues = {v["venue"] for v in capture["venues"]}

    conversion_bps = decimal(parameter("quote_conversion_uncertainty_bps", 2))
    unevaluated = []
    for index, a in enumerate(sorted(grouped)):
        for b in sorted(grouped)[index + 1 :]:
            ar, br = grouped[a], grouped[b]
            at, bt = grouped_times[a], grouped_times[b]
            times = sorted({frag_start, end} | {t for t in at + bt if frag_start <= t < end})
            gaps = []
            second = False
            for t in times:
                states = []
                for rows, stamps in ((ar, at), (br, bt)):
                    k = bisect_right(stamps, t) - 1
                    states.append(rows[k] if k >= 0 and (t - stamps[k]).total_seconds() <= cadence else None)
                if None in states:
                    gaps = []
                    break
                x, y = states
                xm, ym = decimal(x.get("mid")), decimal(y.get("mid"))
                xs, ys = decimal(x.get("spread_bps")), decimal(y.get("spread_bps"))
                if None in (xm, ym, xs, ys) or not ym:
                    gaps = []
                    break
                same_quote = _quote(x) == _quote(y)
                cost = Decimal(0) if same_quote else conversion_bps
                xm, ym = (xm, ym) if same_quote else (_usd_mid(x, xm), _usd_mid(y, ym))
                if xm is None or not ym:
                    unevaluated.append([a, b])
                    gaps = []
                    break
                gap = abs(xm - ym) * 10000 > (xs + ys + cost) * ym
                if t < end:
                    gaps.append(gap)
                elif gaps and gaps[-1] and not gap:
                    closed = True
                rest = []
                for stream in (x, y):
                    independent = stream.get("independent_rest") or {}
                    rt = independent.get("ts")
                    rm = decimal(independent.get("mid"))
                    rs = decimal(independent.get("spread_bps"))
                    good = (
                        stream.get("second_path_confirmed") is True
                        and independent.get("source") == "PUBLIC_REST"
                        and rt is not None
                        and parse_time(rt) <= end
                        and _available(independent, snapshot)
                        and abs((parse_time(rt) - _time(stream)).total_seconds()) <= cadence
                        and rm is not None
                        and rm > 0
                        and rs is not None
                    )
                    rest.append((rm if same_quote else _usd_mid(stream, rm), rs) if good else None)
                second = second or (
                    None not in rest
                    and None not in (rest[0][0], rest[1][0])
                    and abs(rest[0][0] - rest[1][0]) * 10000 > (rest[0][1] + rest[1][1] + cost) * rest[1][0]
                )
            if gaps and all(gaps):
                persistent = True
                pairs.append([a, b])
                validated = validated or (second and {a, b} <= complete_venues)

    funding_groups = {
        p["instrument_id"]: [] for p in deriv.get("per_venue", []) if p.get("funding_8h_eq") is not None
    }
    historical = []
    states = {}
    funding_rows = sorted(
        [
            r
            for r in snapshot.get("funding", [])
            if r.get("instrument_id") in funding_groups
            and r.get("kind") == "SETTLED"
            and _time(r) < start
            and _time(r) >= start - timedelta(days=lookback_days())
        ],
        key=_time,
    )
    for row in funding_rows:
        rate, interval = decimal(row.get("rate")), decimal(row.get("interval_h"))
        if rate is None or not interval:
            continue
        states[row["instrument_id"]] = rate * 8 / interval
        if len(states) == len(funding_groups) and len(states) >= 2:
            historical.append((_time(row), max(states.values()) - min(states.values())))
    normal_dispersion = (
        normal([value for _, value in historical])
        if historical
        and (historical[-1][0] - historical[0][0]).total_seconds()
        >= float(parameter("history_min_days", 7)) * 86400
        else None
    )
    dispersion = deriv.get("funding_dispersion_8h")
    funding_beyond = beyond(dispersion, normal_dispersion)
    confirmations = snapshot.get("funding_confirmations", [])
    secondary = {}
    for row in confirmations:
        if (
            row.get("instrument_id") not in funding_groups
            or _time(row) is None
            or _time(row) > end
            or not _available(row, snapshot)
            or row.get("independent_path_verified") is not True
            or not row.get("source_path")
            or not row.get("primary_source_path")
            or row["source_path"] == row["primary_source_path"]
        ):
            continue
        rate = decimal(row.get("rate_8h_eq"))
        if rate is not None:
            secondary[row["instrument_id"]] = rate
    secondary_dispersion = (
        max(secondary.values()) - min(secondary.values())
        if len(secondary) >= 2 and set(secondary) == set(funding_groups)
        else None
    )
    perp_ids = set(funding_groups)
    perp_complete = True
    for iid in perp_ids:
        if flow_window(snapshot, iid, frag_start, end) is None:
            perp_complete = False
    funding_validated = (
        funding_beyond is True and beyond(secondary_dispersion, normal_dispersion) is True and perp_complete
    )
    deriv["funding_dispersion_normal_range_8h"] = normal_dispersion
    deriv["funding_dispersion_beyond_normal"] = funding_beyond

    top = set(spots[primary]["topn"]["venues"]) if spots[primary].get("topn") else set()
    signs = {v: [] for v in top}
    cursor = start
    while top and cursor + timedelta(minutes=15) <= end:
        sub = spot_truth(
            snapshot.get("flow_bars", []),
            [],
            [
                i
                for i in snapshot.get("instruments", [])
                if _venue(i) in top
                and i.get("token_id", snapshot.get("token_id")) == snapshot.get("token_id")
            ],
            cursor,
            cursor + timedelta(minutes=15),
            flow_segments=flow_segments(snapshot),
            assembled_at=snapshot.get("assembled_at"),
        )
        found = {v["venue"]: v["sign"] for v in sub["venues"]}
        for venue in top:
            signs[venue].append(found.get(venue))
        if any(venue not in found for venue in top):
            break
        cursor += timedelta(minutes=15)
    flips = []
    for values in signs.values():
        if None in values or len(values) < 3:
            continue
        directional = [v for v in values if v != 0]
        flips.append(sum(a != b for a, b in zip(directional, directional[1:], strict=False)))
    top_flips = (
        any(n > 1 for n in flips)
        if signs and all(len(v) >= 3 and None not in v for v in signs.values())
        else None
    )
    liquidity["fragmentation"] = dict(
        window="W15M",
        cutoff=iso(end),
        persistent_gap=persistent if books else None,
        validated=validated or funding_validated,
        price_path_validated=validated,
        funding_path_validated=funding_validated,
        funding_beyond_normal=funding_beyond,
        funding_normal_range_8h=normal_dispersion,
        top_sign_flips=top_flips,
        gap_closed_quickly=closed,
        venues=pairs,
        cross_quote_not_evaluated=unevaluated,
        quote_conversion_uncertainty_bps=conversion_bps,
        complete_venues=sorted(complete_venues),
        reason=None if books or dispersion is not None else "INCOMPLETE",
    )

    horizons = {h.name: h for h in (snapshot.get("relative_sql") or truth_sql.NO_RELATIVE).horizons}
    sample = horizons.get(primary)
    current = sample.ref_realized_vol_24h if sample else None
    rr = (
        [sample.ref_realized_vol_24h_q025, sample.ref_realized_vol_24h_q975]
        if sample
        and sample.ref_realized_vol_24h_samples
        and history_usable(sample.ref_hourly_first, sample.ref_hourly_last)
        else None
    )
    liquidity["vol_compression"] = dict(
        window="W24H",
        cutoff=iso(end),
        rv24=current,
        normal_range=rr,
        state=("COMPRESSED" if current < rr[0] else "EXPANDED" if current > rr[1] else "NORMAL")
        if current is not None and rr
        else None,
        reason=None if current is not None and rr else "INCOMPLETE",
    )


@precise
def supply_truth(snapshot, start, end):
    """Interpret captured supply wrappers; unknown wallets and APY never become inferred sales/APR."""
    rows = sorted(
        [r for r in snapshot.get("supply", []) if _time(r) is not None and _time(r) <= end], key=_time
    )

    def kind(r):
        return r.get("kind", r.get("type"))

    def latest(k):
        return next((r for r in reversed(rows) if kind(r) in k), None)

    def asof(rs, target, maxage=21600):
        candidates = [r for r in rs if _time(r) <= target and (target - _time(r)).total_seconds() <= maxage]
        return max(candidates, key=_time) if candidates else None

    unavailable = {}
    flows = []
    unlocks = []
    inventory = None
    lending = None
    chains = []
    available = False
    evidence = False
    inventories = [r for r in rows if kind(r) in ("INVENTORY", "EXCHANGE_INVENTORY")]
    current = inventories[-1] if inventories else None
    if current and current.get("available") is not False:
        available = True
        if "balances" not in current:
            inventory = dict(current)
            if (
                beyond(number(current.get("chg_pct")), current.get("normal_range")) is True
                and number(current.get("chg_pct")) > 0
            ):
                evidence = True
        else:

            def balances(r):
                return {
                    (b.get("venue"), b.get("chainid"), b.get("contract"), b.get("wallet_list_commit")): b
                    for b in r.get("balances", [])
                    if b.get("complete") is True and decimal(b.get("qty_native")) is not None
                }

            now = balances(current)
            previous = asof(inventories, end - timedelta(days=1))
            old = balances(previous) if previous and previous.get("available") is not False else {}
            cohort = set(now) & set(old)
            per = []
            history = []

            def total(r, keys):
                bs = balances(r)
                return (
                    sum((decimal(bs[k]["qty_native"]) for k in keys), Decimal(0))
                    if keys and all(k in bs for k in keys)
                    else None
                )

            for key in sorted(cohort, key=str):
                n, o = decimal(now[key]["qty_native"]), decimal(old[key]["qty_native"])
                per.append(
                    dict(
                        venue=key[0],
                        chainid=key[1],
                        contract=key[2],
                        qty_native=n,
                        chg_native_24h=n - o,
                        chg_pct=ratio(n - o, o),
                    )
                )
            n0, n1 = total(previous, cohort) if previous else None, total(current, cohort)
            if (
                inventories
                and (_time(current) - _time(inventories[0])).total_seconds()
                >= float(parameter("history_min_days", 7)) * 86400
            ):
                for r in inventories:
                    if not end - timedelta(days=lookback_days()) <= _time(r) < start:
                        continue
                    past = asof(inventories, _time(r) - timedelta(days=1))
                    n = total(r, cohort)
                    o = total(past, cohort) if past else None
                    if n is not None and o:
                        history.append(float(n / o - 1))
            change = n1 - n0 if n1 is not None and n0 is not None else None
            pct = ratio(change, n0)
            nr = normal(history)
            inventory = dict(
                chg_native_24h=change,
                chg_pct=pct,
                normal_range=nr,
                per_venue=per,
                coverage=dict(
                    complete_balances=len(now),
                    observed_balances=len(current.get("balances", [])),
                    comparable_balances=len(cohort),
                    coverage_errors=current.get("coverage_errors", []),
                    unsupported_venues=current.get("unsupported_venues", []),
                ),
                reason=None if change is not None else "INCOMPLETE",
                observed_at=iso(_time(current)),
            )
            if pct is not None and nr and pct > nr[1]:
                evidence = True
    else:
        unavailable["exchange_inventory"] = current.get("reason", "INCOMPLETE") if current else "INCOMPLETE"
    wallet = latest(("WALLET_FLOWS",))
    if wallet and wallet.get("available") is not False:
        available = True
        for f in wallet.get("flows", []):
            t = parse_time(f["ts"]) if f.get("ts") else None
            if t is None or not start <= t < end:
                continue
            flow = dict(f)
            flow["class"] = f.get("classification", "AMBIGUOUS")
            flow["qty"] = decimal(f.get("qty_native"))
            flows.append(flow)
            if (
                flow["class"] in ("UNLOCK_DISTRIBUTION", "TREASURY_INTERNAL", "MM_INVENTORY")
                and f.get("reaches_exchange") is True
            ):
                evidence = True
    elif wallet:
        unavailable["flows"] = wallet.get("reason", "INCOMPLETE")
    elif not any(kind(r) in ("FLOW", "WALLET_FLOW") for r in rows):
        unavailable["flows"] = "INCOMPLETE"
    for r in rows:
        if kind(r) in ("FLOW", "WALLET_FLOW") and r.get("available") is not False and start <= _time(r) < end:
            flow = dict(r)
            flow["class"] = r.get("class", "AMBIGUOUS")
            flows.append(flow)
            available = True
            if (
                flow["class"] in ("UNLOCK_DISTRIBUTION", "TREASURY_INTERNAL", "MM_INVENTORY")
                and r.get("to_exchange") is True
            ):
                evidence = True
    unlock = latest(("UNLOCKS",))
    schedules = []
    if unlock and unlock.get("available") is not False:
        available = True
        schedules = unlock.get("unlocks", [])
    elif unlock:
        unavailable["unlocks"] = unlock.get("reason", "INCOMPLETE")
    elif not any(kind(r) == "UNLOCK" for r in rows):
        unavailable["unlocks"] = "INCOMPLETE"
    schedules += [r for r in rows if kind(r) == "UNLOCK" and r.get("available") is not False]
    for u in schedules:
        stamp = u.get("date", u.get("ts"))
        if stamp and end <= parse_time(stamp) <= end + timedelta(days=14):
            available = True
            unlocks.append(
                dict(
                    u,
                    inference="POTENTIAL_SUPPLY_IS_NOT_A_SALE",
                    pct_circulating=u.get("pct_circulating"),
                    recipient_class=u.get("recipient_class"),
                    transferable=u.get("transferable"),
                )
            )
    lend = latest(("LENDING",))
    if lend and lend.get("available") is not False:
        available = True
        lending = dict(lend)
        if "pools" in lend:
            prior = asof(
                [r for r in rows if kind(r) == "LENDING" and r.get("available") is not False],
                end - timedelta(days=1),
                1800,
            )
            oldp = {p["pool_id"]: p for p in prior.get("pools", [])} if prior else {}
            pools = []
            for p in lend["pools"]:
                old = oldp.get(p["pool_id"], {})
                u, ou = number(p.get("utilization")), number(old.get("utilization"))
                pools.append(
                    dict(
                        p,
                        borrow_apr=None,
                        borrow_apr_reason="NOT_ESTIMABLE",
                        chg_24h=u - ou if u is not None and ou is not None else None,
                    )
                )
            lending["pools"] = pools
            lending["borrow_apr"] = None
            lending["reason"] = "APY_IS_NOT_APR"
    elif lend:
        unavailable["lending"] = lend.get("reason", "INCOMPLETE")
    else:
        unavailable["lending"] = "INCOMPLETE"
    chain_latest = {}
    for r in rows:
        if kind(r) in ("CHAIN_LIQUIDITY", "STABLECOIN_CHAIN"):
            chain_latest[r.get("chain")] = r
    if not chain_latest:
        unavailable["stablecoin_chain"] = "INCOMPLETE"
    for chain, r in chain_latest.items():
        if r.get("available") is False:
            unavailable["stablecoin_chain"] = r.get("reason", "INCOMPLETE")
            continue
        available = True
        if "series" not in r:
            chains.append(r)
            continue
        series = sorted(
            [p for p in r["series"] if parse_time(p["ts"]) <= end], key=lambda p: parse_time(p["ts"])
        )

        def point(target):
            ps = [
                p
                for p in series
                if parse_time(p["ts"]) <= target and (target - parse_time(p["ts"])).total_seconds() <= 86400
            ]
            return decimal(ps[-1].get("supply_usd")) if ps else None

        n = point(end)
        p1 = point(end - timedelta(days=1))
        p7 = point(end - timedelta(days=7))
        chains.append(
            dict(
                chain=chain,
                supply_usd=n,
                supply_chg_24h=n - p1 if n is not None and p1 is not None else None,
                supply_chg_7d=n - p7 if n is not None and p7 is not None else None,
                reason=None if n is not None else "INCOMPLETE",
            )
        )

    def fundamental_available(row):
        if isinstance(row.get("ts"), bool):
            return False
        receipt = row.get("received_ts")
        if receipt is None:
            return True
        try:
            return not isinstance(receipt, bool) and parse_time(receipt) <= end
        except (ValueError, TypeError, OverflowError):
            return False

    fundamentals = next(
        (r for r in reversed(rows) if kind(r) == "FUNDAMENTALS" and fundamental_available(r)), None
    )
    holders_revenue = dict(status="UNAVAILABLE", protocols=[], reason="NO_CAPTURED_FUNDAMENTALS")
    if fundamentals:
        cadence, cadence_basis = capture_cadence(snapshot, "supply", fundamentals)
        age = (end - _time(fundamentals)).total_seconds()
        status = "STALE" if cadence is not None and age > cadence else "OBSERVED"
        protocols = []
        for slug, metrics in sorted((fundamentals.get("protocols") or {}).items()):
            metric = metrics.get("dailyHoldersRevenue") or {}
            periods = []
            for field, period in (("total24h", "24h"), ("total30d", "30d")):
                try:
                    value = decimal(metric.get(field))
                    if value is not None and not value.is_finite():
                        value = None
                except (ArithmeticError, ValueError):
                    value = None
                if metric.get("available") is False or fundamentals.get("available") is False:
                    value = None
                periods.append(
                    dict(
                        provider_period=period,
                        value=str(value) if value is not None else None,
                        status="OBSERVED" if value is not None else "UNAVAILABLE",
                    )
                )
            chart_points = []
            if metric.get("available") is not False and fundamentals.get("available") is not False:
                for point in metric.get("totalDataChart") or []:
                    try:
                        stamp, raw_value = point
                        if isinstance(stamp, bool) or isinstance(raw_value, bool):
                            continue
                        value = decimal(raw_value)
                        point_time = parse_time(stamp)
                        if point_time <= end and value is not None and value.is_finite():
                            chart_points.append(
                                (
                                    point_time,
                                    dict(
                                        provider_timestamp=str(stamp),
                                        value=str(value),
                                        provider_period="UNSPECIFIED",
                                    ),
                                )
                            )
                    except (ArithmeticError, ValueError, TypeError, OverflowError):
                        continue
            chart_point = max(chart_points, key=lambda point: point[0])[1] if chart_points else None
            protocols.append(
                dict(
                    protocol=slug,
                    provider="DefiLlama",
                    unit="USD",
                    source_url=metric.get("source_url"),
                    periods=periods,
                    chart_available=bool(metric.get("totalDataChart")),
                    latest_chart_point=chart_point,
                    reason=metric.get("reason"),
                )
            )
        coverage = {
            period: dict(
                observed=sum(p["periods"][i]["value"] is not None for p in protocols),
                expected=len(protocols),
                complete=bool(protocols) and all(p["periods"][i]["value"] is not None for p in protocols),
            )
            for i, period in enumerate(("24h", "30d"))
        }
        holders_revenue = dict(
            status=status if protocols else "UNAVAILABLE",
            availability=(
                "COMPLETE"
                if all(c["complete"] for c in coverage.values())
                else "PARTIAL"
                if any(
                    p["latest_chart_point"] or any(v["value"] is not None for v in p["periods"])
                    for p in protocols
                )
                else "UNAVAILABLE"
            ),
            protocols=protocols,
            coverage=coverage,
            as_of=iso(_time(fundamentals)),
            capture_received_at=fundamentals.get("received_ts"),
            capture_availability="KNOWN" if fundamentals.get("received_ts") is not None else "UNKNOWN",
            age_s=age,
            cadence_s=cadence,
            cadence_basis=cadence_basis,
            identity_evidence=deepcopy(fundamentals.get("identity_evidence")),
            source_observation_sha256=hashlib.sha256(
                json.dumps(jsonable(fundamentals), sort_keys=True).encode()
            ).hexdigest(),
            reason=fundamentals.get("reason"),
            inference_limits=[
                "PROVIDER_REVENUE_IS_NOT_PROOF_OF_TOKEN_CAPTURE",
                "PER_PROTOCOL_VALUES_NOT_SUMMED_PARENT_CHILD_SCOPE_MAY_OVERLAP",
            ],
        )
    elif any(
        r.get("kind", r.get("type")) == "FUNDAMENTALS" and _time(r) is not None and _time(r) > end
        for r in snapshot.get("supply", [])
    ):
        holders_revenue["reason"] = "ONLY_FUTURE_FUNDAMENTALS"
    return dict(
        holders_revenue=holders_revenue,
        exchange_inventory=dict(inventory, window="W24H") if inventory else missing(),
        flows=flows,
        unlocks_next_14d=unlocks,
        lending=dict(lending, window="W24H") if lending else missing(),
        stablecoin_chain=chains[0] if chains else missing(),
        chain_contexts=chains,
        unavailable=unavailable,
        supply_in_evidence="TRUE" if evidence else "FALSE" if available else "UNKNOWN",
    )


def benchmark_reaction(record):
    """Reaction checkpoint values for a BTC/ETH benchmark outside the analysis set (03 §8.1).

    Lead ruling 2026-10-20: the raw return comes from base-tier 1m kline closes of the
    benchmark's largest-volume spot venue (trailing 24 h before the window end); spot CVD,
    OI and funding are n/a NOT_APPLICABLE. No escalation, so no trades are needed. `record` is
    the typed result of benchmark_reaction.sql (analysis/truth_sql.py).
    """
    return {
        "r_raw": record.r_raw,
        "r_raw_reason": None if record.r_raw is not None else "INCOMPLETE",
        "r_raw_method": "BASE_KLINE_CLOSE_LARGEST_SPOT_VENUE",
        "r_raw_venue": record.venue,
        "price_start": record.price_start,
        "price_start_observed_at": iso(record.price_start_observed_at)
        if record.price_start_observed_at
        else None,
        "price_end": record.price_end,
        "price_end_observed_at": iso(record.price_end_observed_at) if record.price_end_observed_at else None,
        "not_applicable": ["r_btc_adj", "pctile", "spot_cvd", "oi", "funding", "liquidations"],
        "reason": "NOT_APPLICABLE",
    }


def capture_cadence(snapshot, collection, row=None):
    row = row or {}
    metadata = snapshot.get("capture_metadata") or {}
    collections = metadata.get("collections") or {}
    entry = collections.get(collection, {})
    if isinstance(entry, (int, float)):
        return float(entry), "CAPTURE_METADATA"
    kind = row.get("kind", row.get("type"))
    if str(metadata.get("tier", snapshot.get("capture_tier", ""))).upper() == "UNKNOWN" and (
        collection in ("oi", "funding")
        or collection == "supply"
        and kind in ("EXCHANGE_INVENTORY", "WALLET_FLOWS")
    ):
        return None, "CAPTURE_TIER_HISTORY_UNAVAILABLE"
    bymetric = entry.get("cadence_s_by_metric", {}) if isinstance(entry, dict) else {}
    if row.get("metric_key") in (bymetric or {}) and number(bymetric[row["metric_key"]]):
        return float(bymetric[row["metric_key"]]), "CAPTURE_METADATA_BY_METRIC"
    bykind = entry.get("by_kind", {}) if isinstance(entry, dict) else {}
    explicit = (
        bykind.get(kind) if kind in bykind else entry.get("cadence_s") if isinstance(entry, dict) else None
    )
    if isinstance(explicit, dict):
        explicit = explicit.get("cadence_s")
    if explicit is not None and float(explicit) > 0:
        return float(explicit), "CAPTURE_METADATA"
    for sem in snapshot.get("semantics", []):
        if (
            row.get("metric_key")
            and sem.get("metric_key") == row["metric_key"]
            and (not sem.get("instrument_id") or sem["instrument_id"] == row.get("instrument_id"))
            and sem.get("cadence_s") is not None
        ):
            return float(sem["cadence_s"]), "SCOPED_METRIC_SEMANTICS"
    iid = row.get("instrument_id")
    instrument = next((r for r in snapshot.get("instruments", []) if r.get("instrument_id") == iid), {})
    tier = str(
        metadata.get(
            "tier",
            snapshot.get("capture_tier", instrument.get("capture_tier", instrument.get("tier", "BASE"))),
        )
    ).upper()
    if collection in ("oi", "funding"):
        return (60.0 if tier == "HOT" else 300.0), "TDD_5_1_" + tier
    if collection in ("flow_bars", "klines"):
        return 60.0, "TDD_5_1"
    if collection == "books":
        return 10.0, "TDD_5_1"
    if collection == "tickers":
        return 60.0, "BASE_TICKER_POLL"
    if collection == "supply":
        cadences = {
            "EXCHANGE_INVENTORY": 900 if tier == "HOT" else 21600,
            "WALLET_FLOWS": 900 if tier == "HOT" else 21600,
            "UNLOCKS": 86400,
            "LENDING": 1800,
            "CHAIN_LIQUIDITY": 21600,
            "STABLECOIN_CHAIN": 21600,
            "FUNDAMENTALS": 21600,
        }
        return (
            (float(cadences[kind]), "TDD_5_6_" + tier)
            if kind in cadences
            else (None, "CADENCE_NOT_CONFIGURED")
        )
    return None, "CADENCE_NOT_CONFIGURED"


def source_intervals(snapshot):
    """Series the provider publishes once per interval (metric semantics source_interval_s,
    publish_lag_s), keyed (metric_key, instrument_id or None)."""
    return {
        (sem["metric_key"], sem.get("instrument_id")): sem
        for sem in snapshot.get("semantics", [])
        if sem.get("metric_key") and number(sem.get("source_interval_s"))
    }


def interval_deadline(intervals, row):
    """Freshness deadline of a row of a once-per-interval series, else None. The next point
    is due one interval after this one and visible publish_lag_s later, so the series is
    within its own cadence (03 §5.10) while age from the interval start <= 2 x interval + lag.
    Returns (deadline, allowed age in s, interval start)."""
    key = row.get("metric_key")
    sem = intervals.get((key, row.get("instrument_id"))) or intervals.get((key, None))
    if sem is None:
        return None
    interval = float(sem["source_interval_s"])
    allowed = 2 * interval + float(sem.get("publish_lag_s") or 0)
    start = (
        _time(row) - timedelta(seconds=interval)
        if sem.get("timestamp_semantics") == "BUCKET_END"
        else _time(row)
    )
    return start + timedelta(seconds=allowed), allowed, start


def observed_through(snapshot, collection, instrument_id):
    """Capture watermark: the source was polled successfully through this time (e.g. a venue
    that publishes no candle for a minute without trades). Never a value, only availability."""
    entry = ((snapshot.get("capture_metadata") or {}).get("collections") or {}).get(collection) or {}
    marks = entry.get("observed_through", {}) if isinstance(entry, dict) else {}
    mark = marks.get(instrument_id) if isinstance(marks, dict) else marks
    return parse_time(mark) if mark else None


def available_field_freshness(snapshot, end):
    """Latest point per source field; old history does not make a fresh series stale.
    Only the assessed token's fields count (§5.10): benchmark and sector-peer closes in the
    snapshot are inputs of the relative block, not fields of this token."""
    checks = []
    metadata = (snapshot.get("capture_metadata") or {}).get("collections", {})
    intervals = source_intervals(snapshot)
    token = snapshot.get("token_id")
    owners = {
        r["instrument_id"]: r["token_id"]
        for r in snapshot.get("instruments", [])
        if r.get("instrument_id") and r.get("token_id")
    }
    for collection in ("flow_bars", "klines", "books", "oi", "funding", "liquidations", "tickers", "supply"):
        groups = defaultdict(list)
        for r in snapshot.get(collection, []):
            if (
                r.get("token_id", owners.get(r.get("instrument_id"), token)) != token
                or r.get("available") is False
                or _time(r) is None
                or _time(r) > end
                or not _available(r, snapshot)
            ):
                continue
            key = (r.get("instrument_id"), r.get("metric_key"), r.get("kind", r.get("type")), r.get("chain"))
            groups[key].append(r)

        for key in sorted(groups, key=lambda k: tuple("" if v is None else str(v) for v in k)):
            rows = groups[key]
            row = max(rows, key=_time)
            stamp = _time(row)
            cadence, source = capture_cadence(snapshot, collection, row)
            if collection in ("flow_bars", "klines"):
                completed = [
                    r
                    for r in rows
                    if r.get("complete") is True
                    and _time(r) + timedelta(seconds=int(r.get("interval_s", 60))) <= end
                ]
                if not completed:
                    continue
                row = max(completed, key=lambda r: _time(r) + timedelta(seconds=int(r.get("interval_s", 60))))
                stamp = _time(row) + timedelta(seconds=int(row.get("interval_s", 60)))
                mark = observed_through(snapshot, collection, row.get("instrument_id"))
                if mark is not None and stamp < mark <= end:
                    stamp = mark
            if collection == "funding" and row.get("kind") == "SETTLED" and number(row.get("interval_h")):
                cadence = number(row["interval_h"]) * 3600
                source = "SETTLEMENT_INTERVAL"
            stream = None
            if collection == "liquidations":
                entry = metadata.get(collection, {}) or {}
                watermarks = entry.get("observed_through", {}) if isinstance(entry, dict) else {}
                watermark = (
                    watermarks.get(row.get("instrument_id")) if isinstance(watermarks, dict) else watermarks
                )
                states = entry.get("coverage_status_by_instrument", {}) if isinstance(entry, dict) else {}
                stream = dict(
                    freshness_basis="STREAM_WATERMARK" if watermark is not None else "LAST_EVENT",
                    coverage_status=states.get(row.get("instrument_id"))
                    if isinstance(states, dict)
                    else None,
                )
                if watermark is not None:
                    stamp = parse_time(watermark)
            field = next(
                name
                for name in (
                    "minute_start",
                    "open_time",
                    "exchange_ts",
                    "ts",
                    "as_of",
                    "settlement_time",
                    "funding_time",
                    "observed_at",
                )
                if row.get(name) is not None
            )
            observed = propagate_timestamp(row, field, precision_aliases=("timestamp_precision", "precision"))
            if collection in ("flow_bars", "klines"):
                observed = offset_schedule(observed, timedelta(seconds=int(row.get("interval_s", 60))))
            if observed.value != iso(stamp):
                observed = propagate_timestamp({"observed_at": iso(stamp)}, "observed_at")
            age = (end - stamp).total_seconds()
            published = (
                interval_deadline(intervals, row) if collection not in ("flow_bars", "klines") else None
            )
            if published is not None:
                _, cadence, start = published
                source = "SOURCE_INTERVAL_PLUS_PUBLISH_LAG"
                age = (end - start).total_seconds()
            fresh = cadence is not None and 0 <= age <= cadence
            checks.append(
                dict(
                    collection=collection,
                    instrument_id=key[0],
                    metric_key=key[1],
                    kind=key[2],
                    chain=key[3],
                    **observed.fields("observed_at"),
                    cadence_s=cadence,
                    cadence_source=source,
                    age_s=age,
                    fresh=fresh,
                    reason=None if fresh else "NOT_ESTIMABLE" if cadence is None else "INCOMPLETE",
                    **(
                        dict(
                            stream,
                            detail=None
                            if fresh or stream["freshness_basis"] == "STREAM_WATERMARK"
                            else "STREAM_CONNECTION_UNPROVEN",
                        )
                        if stream
                        else {}
                    ),
                )
            )
    return checks


def data_confidence(snapshot, spot, derivatives, end):
    """§5.10: grade spot coverage, derivatives coverage, then field freshness."""
    failed = []
    low = False
    complete_venues = {v["venue"] for v in spot.get("venues", [])}
    instruments = {
        r["instrument_id"]: r
        for r in snapshot.get("instruments", [])
        if r.get("instrument_id") and r.get("token_id", snapshot.get("token_id")) == snapshot.get("token_id")
    }
    owners = {
        r["instrument_id"]: r.get("token_id")
        for r in snapshot.get("instruments", [])
        if r.get("instrument_id")
    }
    for collection in ("flow_bars", "books", "oi", "funding", "klines"):
        for r in snapshot.get(collection, []):
            iid = r.get("instrument_id")
            owner = r.get("token_id", owners.get(iid) or snapshot.get("token_id"))
            if iid and owner == snapshot.get("token_id"):
                instruments.setdefault(iid, r)
    required = []
    for iid, inst in instruments.items():
        venue = _venue(inst)
        market = _market(inst)
        if market == "SPOT" and venue in complete_venues:
            required.extend(
                (iid, venue, market, f, c)
                for f, c in [("aggressor", "flow_bars"), ("depth", "books"), ("price", "klines")]
            )
        elif market != "SPOT":
            required.extend((iid, venue, market, f, c) for f, c in [("oi", "oi"), ("funding", "funding")])
    cov = spot["coverage"]
    low = low or cov["n_complete"] < int(parameter("min_spot_venues", 2))
    if (
        cov.get("exact_majority") is not True
        if "exact_majority" in cov
        else cov["covered_share_est"] is None or decimal(cov["covered_share_est"]) <= Decimal(".5")
    ) or cov["n_complete"] < int(parameter("min_spot_venues", 2)):
        failed.append(dict(condition="spot_coverage", reason="INCOMPLETE"))
    perp_ids = {iid for iid, r in instruments.items() if _market(r) != "SPOT"}
    if perp_ids:
        synced = derivatives.get("synchronized")
        synvenues = set(synced["venues"]) if synced else set()
        perpvol = defaultdict(Decimal)
        for r in snapshot.get("klines", []):
            if (
                r.get("instrument_id") in perp_ids
                and end - timedelta(hours=24) <= _time(r) < end
                and r.get("quote_volume_usd") is not None
            ):
                perpvol[_venue(r)] += decimal(r["quote_volume_usd"])
        if (
            not synced
            or not perpvol
            or sum(v for k, v in perpvol.items() if k in synvenues) <= sum(perpvol.values()) / 2
        ):
            failed.append(dict(condition="derivatives_coverage", reason="INCOMPLETE"))
    native_coverage_ready = not perp_ids or not any(f["condition"] == "derivatives_coverage" for f in failed)
    intervals = source_intervals(snapshot)
    class_ready = not perp_ids or bool(
        derivatives.get("synchronized") and derivatives["synchronized"].get("classification_ready")
    )
    for iid, _unused_venue, _unused_market, family, collection in required:
        rows = [
            r
            for r in snapshot.get(collection, [])
            if r.get("instrument_id") == iid
            and _time(r) is not None
            and _time(r) <= end
            and _available(r, snapshot)
        ]
        valid = []
        for r in rows:
            stamp = _time(r)
            if collection in ("klines", "flow_bars"):
                if r.get("complete") is not True:
                    continue
                stamp += timedelta(seconds=int(r.get("interval_s", 60)))
                if stamp > end:
                    continue
            if collection == "books" and (
                r.get("integrity_ok") is not True
                or not number(r.get("mid"))
                or any(number(r.get("range_covered_pct", {}).get(side)) is None for side in ("bid", "ask"))
            ):
                continue
            if collection == "klines" and not number(r.get("c", r.get("close"))):
                continue
            if collection == "oi" and decimal(r.get("oi_native")) is None:
                continue
            if collection == "funding" and (number(r.get("rate")) is None or not number(r.get("interval_h"))):
                continue
            valid.append((stamp, r))
        mark = observed_through(snapshot, collection, iid) if collection == "klines" and valid else None
        if mark is not None and max(t for t, _ in valid) < mark <= end:
            valid.append((mark, None))
        cadence, cadence_source = capture_cadence(
            snapshot, collection, rows[-1] if rows else {"instrument_id": iid}
        )
        deadlines = []
        for stamp, r in valid:
            published = interval_deadline(intervals, r) if r is not None and collection == "oi" else None
            if published is not None:
                deadlines.append(published[0])
            elif cadence is not None:
                deadlines.append(stamp + timedelta(seconds=cadence))
        if not deadlines or max(deadlines) < end:
            failed.append(
                dict(
                    condition="freshness",
                    reason="INCOMPLETE",
                    collection=collection,
                    instrument_id=iid,
                    metric_family=family,
                    cadence_s=cadence,
                    cadence_source=cadence_source,
                )
            )
    freshness = available_field_freshness(snapshot, end)
    for check in freshness:
        if not check["fresh"]:
            failed.append(dict(condition="freshness", **{k: v for k, v in check.items() if k != "fresh"}))
    return dict(
        class_="LOW" if low else "MEDIUM" if failed else "HIGH",
        failed=failed,
        limiting=failed[0] if failed else None,
        derivatives_native_coverage_ready=native_coverage_ready,
        derivatives_classification_ready=class_ready,
        freshness=freshness,
    )


@precise
def support_features(snapshot, start, end, spot, deriv, liquidity, supply):
    """§5.8 support evidence. Unknown inputs remain None and never add support."""
    length = end - start
    evidence = snapshot.get("volume_sql") or truth_sql.NO_VOLUME
    window = snapshot.get("_primary")
    volume = {(n.market, n.window): n for n in evidence.normals}
    usd = {(r.window, r.instrument_id): r for r in evidence.windows}
    oi_endpoints, oi_history = oi_records(snapshot, window)
    features = {}
    for market in ("SPOT", "PERP"):
        row = volume.get((market, window))
        current = row.value if row else None
        previous = row.previous if row else None
        nr = [row.normal_low, row.normal_high] if row and row.normal_low is not None else None
        up = bool(current > previous and current > nr[1]) if None not in (current, previous) and nr else None
        features[market.lower() + "_volume_up_beyond_normal"] = up
        features[market.lower() + "_volume_within_normal"] = within(current, nr)
        features[market.lower() + "_volume"] = {
            "value": current,
            "previous": previous,
            "normal_range": nr,
            "unit": "NATIVE_TOKEN",
            "window": window,
            "reason": None if current is not None and nr else "NOT_ESTIMABLE",
        }
    perp_buys = perp_sells = perp_q = Decimal(0)
    perp_complete = False
    perp_flags = []
    for p in deriv.get("per_venue", []):
        iid = p["instrument_id"]
        rows = flow_window(snapshot, iid, start, end)
        configured = any(
            s.get("metric_family") == "aggressor"
            and s.get("venue") == p["venue"]
            and s.get("market_type") != "SPOT"
            and (not s.get("instrument_id") or s["instrument_id"] == iid)
            for s in snapshot.get("semantics", [])
        )
        complete = configured and rows is not None
        if complete:
            buy, sell, q = (_sum(rows, k) for k in ("buy_qty", "sell_qty", "sum_sq_qty"))
            complete = None not in (buy, sell, q)
            if complete:
                perp_buys += buy
                perp_sells += sell
                perp_q += q
                perp_complete = True
            p["perp_taker_buy_share"] = ratio(buy, buy + sell) if complete else None
        else:
            p["perp_taker_buy_share"] = None
        perp_flags.append(complete)
        p["perp_volume_usd"] = usd[(window, iid)].current_usd if (window, iid) in usd else None
        p["perp_volume_reason"] = None if p["perp_volume_usd"] is not None else "INCOMPLETE"
        endpoint, record = oi_endpoints.get(iid), oi_history.get(iid)
        br = (
            [record.basis_low, record.basis_high]
            if record is not None
            and record.basis_low is not None
            and (record.basis_last - record.basis_first).total_seconds()
            >= float(parameter("history_min_days", 7)) * 86400
            else None
        )
        p["basis_normal_range"] = br
        p["basis_vs_normal"] = (
            "WITHIN"
            if within(p.get("basis_bps"), br) is True
            else "ABOVE"
            if br and p.get("basis_bps") is not None and p["basis_bps"] > br[1]
            else "BELOW"
            if br and p.get("basis_bps") is not None
            else None
        )
        initial_value = endpoint.basis_start_bps if endpoint else None
        p["basis_widening"] = (
            p["basis_bps"] > initial_value
            if p.get("basis_bps") is not None and initial_value is not None
            else None
        )
        first = p.get("funding_start")
        interval = p.get("funding_start_interval_h")
        p["funding_rising"] = (
            p["funding_8h_eq"] > decimal(first) * 8 / decimal(interval)
            if first is not None and interval and p.get("funding_8h_eq") is not None
            else None
        )
    perp_complete = bool(perp_flags) and all(perp_flags)
    cvd = perp_buys - perp_sells
    features["perp_taker_flow_buy"] = (
        bool(cvd > 0 and cvd * cvd >= decimal(parameter("significant_flow_sigma", 2)) ** 2 * perp_q)
        if perp_complete
        else None
    )
    features["basis_within_normal"] = (
        bool(deriv.get("per_venue")) and all(p.get("basis_vs_normal") == "WITHIN" for p in deriv["per_venue"])
        if all(p.get("basis_vs_normal") is not None for p in deriv.get("per_venue", []))
        else None
    )
    features["basis_widening"] = (
        any(p.get("basis_widening") is True for p in deriv.get("per_venue", []))
        if any(p.get("basis_widening") is not None for p in deriv.get("per_venue", []))
        else None
    )
    features["funding_rising"] = (
        any(p.get("funding_rising") is True for p in deriv.get("per_venue", []))
        if any(p.get("funding_rising") is not None for p in deriv.get("per_venue", []))
        else None
    )
    liqs = [
        r
        for r in snapshot.get("liquidations", [])
        if start <= _time(r) < end
        and r.get("instrument_id")
        in {p["instrument_id"] for p in deriv.get("per_venue", []) if p.get("liq_coverage") == "COMPLETE"}
    ]
    total = _sum(liqs, "notional_usd")
    first = _sum([r for r in liqs if _time(r) < start + length / 2], "notional_usd")
    features["liquidations_first_half_majority"] = (
        strict_majority(first or Decimal(0), total) if total is not None else None
    )
    profile = spot.get("size_profile") or {}
    features["large_ticket_buy_majority"] = profile.get("large_ticket_buy_majority")
    features["large_tickets_minority"] = profile.get("large_tickets_minority")
    features["large_ticket_sell_share_rising"] = profile.get("sell_share_rising")
    inventory = supply.get("exchange_inventory") or {}
    change = decimal(inventory.get("chg_native_24h"))
    features["exchange_inflows"] = (
        bool(change > 0)
        if change is not None
        else any(
            f.get("class") in ("TREASURY_INTERNAL", "MM_INVENTORY", "UNLOCK_DISTRIBUTION")
            and (f.get("reaches_exchange") is True or f.get("to_exchange") is True)
            for f in supply.get("flows", [])
        )
        if supply.get("flows")
        else None
    )
    features["localized_inventory_transfer"] = (
        any(f.get("class") == "CEX_CUSTODY_MIGRATION" for f in supply.get("flows", []))
        if supply.get("flows")
        else None
    )
    features["ask_depth_building"] = None
    features["ask_replenishment"] = None
    books = defaultdict(list)
    for b in snapshot.get("books", []):
        if _market(b) == "SPOT" and b.get("integrity_ok") is True:
            books[_venue(b)].append(b)
    builds = []
    recovered = False
    testable = False
    book_interval = timedelta(seconds=float(parameter("book_interval_s", 10)))
    refill = timedelta(seconds=float(parameter("refill_window_s", 60)))
    window_flows = defaultdict(list)
    for flow in snapshot.get("flow_bars", []):
        if _market(flow) == "SPOT" and start <= _time(flow) < end and flow.get("complete") is True:
            window_flows[_venue(flow)].append(flow)

    def ask(b):
        return decimal((b.get("depth", {}).get("ask", {}).get("p1") or {}).get("usd"))

    for venue, rows in books.items():
        rows.sort(key=_time)
        times = [_time(r) for r in rows]
        i = bisect_right(times, start) - 1
        j = bisect_right(times, end) - 1
        before = rows[i] if i >= 0 and times[i] >= start - book_interval else None
        after = rows[j] if j >= 0 else None
        if before is not None and after is not None and ask(before) is not None and ask(after) is not None:
            builds.append(ask(after) > ask(before))
        for flow in window_flows.get(venue, []):
            buy, sell = decimal(flow.get("buy_notional")), decimal(flow.get("sell_notional"))
            t = _time(flow)
            k = bisect_left(times, t) - 1
            pre = rows[k] if k >= 0 and times[k] >= t - book_interval else None
            if buy is None or sell is None or pre is None or ask(pre) is None or buy - sell < ask(pre):
                continue
            if t + refill > end:
                continue
            testable = True
            recovered = recovered or any(
                ask(r) is not None and ask(r) >= ask(pre)
                for r in rows[bisect_right(times, t) : bisect_right(times, t + refill)]
            )
    if builds:
        features["ask_depth_building"] = any(builds)
    if testable:
        features["ask_replenishment"] = recovered
    return features


def select_move(spot, deriv, liq, supply, relative, confidence, window):
    p = relative["windows"][window]
    r = p.get("relative_return", p["r_btc_adj"])
    rng = p["normal_range"]
    up = r is not None and rng is not None and r > rng[1]
    notdown = r is not None and rng is not None and r >= rng[0]
    moved = beyond(r, rng) is True
    oi = deriv["synchronized"] or {}
    dn = oi.get("oi_native_chg_pct")
    nr = oi.get("native_normal_range")
    nb = beyond(dn, nr)
    nup = nb is True and dn > 0
    ndown = nb is True and dn < 0
    nwithin = within(dn, nr) is True
    funding = [x.get("funding_vs_normal") for x in deriv["per_venue"]]
    fundok = bool(funding) and all(x in ("WITHIN", "BELOW") for x in funding)
    fs = [x.get("funding_start") for x in deriv["per_venue"]]
    fundnegative = bool(fs) and None not in fs and all(x <= 0 for x in fs)
    inc = deriv.get("incremental_share") or {}
    increments = inc.get("increment_usd")
    if increments:
        exact_context(*increments.values())
    perpmost = (
        (2 * increments["perp"] > sum(increments.values())) if increments else (inc.get("perp") or 0) > 0.5
    )
    spotmost = (
        (2 * increments["spot"] > sum(increments.values())) if increments else (inc.get("spot") or 0) > 0.5
    )
    lead = deriv["lead_lag"]["first_mover"]
    weak = liq["elasticity"]["weak_response"]
    excess = liq["elasticity"]["excess_response"]
    drop = liq["depth_drop"]
    depth_majority_lost = liq.get("depth_majority_lost", drop is not None and drop > 0.5)
    direction = spot["direction"]
    broad = spot["classification"] == "BROAD"
    supplyyes = supply["supply_in_evidence"] == "TRUE"
    liqs = deriv["liquidations"]["complete_venues_total"]
    shortmore = liqs["short"] is not None and liqs["long"] is not None and liqs["short"] > liqs["long"]
    features = deriv.get("support_features") or {}
    native_not_up = nb is False or (nb is True and dn < 0)

    def count(*values):
        return sum(value is True for value in values)

    duration = WINDOWS.get(window) or (
        (parse_time(p["cutoff"]) - parse_time(p["start"])).total_seconds()
        if p.get("cutoff") and p.get("start")
        else None
    )
    cohort_buy_to_sell = any(
        (c.get("current") or {}).get("sign") == -1
        and (c.get("other") or {}).get("sign") == 1
        and c.get("share_majority") is True
        for name, c in spot.get("cohort_comparisons", {}).items()
        if duration is not None and WINDOWS.get(name, 0) > duration
    )
    eligible = {}
    divergence = dict(state="NONE", triggered=False, candidates=[], unresolved=[], confirmed=False)
    if direction == "BUY" and broad and notdown and weak is False and nwithin and fundok:
        eligible["SPOT_LED_ACCUMULATION"] = count(
            features.get("spot_volume_up_beyond_normal"),
            lead == "SPOT",
            spotmost,
            features.get("large_ticket_buy_majority"),
            drop is not None and drop <= 0,
        )
    if (
        direction == "BUY"
        and spot["classification"] in ("BROAD", "CONCENTRATED")
        and dn is not None
        and dn > 0
        and nwithin
        and oi.get("oi_class") in ("REAL_NATIVE", "MIXED")
        and fundok
    ):
        eligible["ORGANIC_MIXED"] = count(
            features.get("spot_volume_up_beyond_normal") is True
            and features.get("perp_volume_up_beyond_normal") is True,
            features.get("basis_within_normal"),
        )
    if up and nup and (perpmost or lead == "PERP") and not (direction == "BUY" and broad):
        eligible["PERP_LED_CHASE"] = count(
            any(x == "ABOVE" for x in funding),
            features.get("basis_widening"),
            direction in ("SELL", "NEUTRAL"),
            features.get("perp_taker_flow_buy"),
        )
    if up and (ndown or shortmore) and fundnegative:
        eligible["SHORT_SQUEEZE"] = count(
            features.get("liquidations_first_half_majority"), features.get("perp_taker_flow_buy")
        )
    if direction == "BUY" and weak is True:
        eligible["SELL_SIDE_ABSORPTION"] = count(features.get("ask_replenishment"), supplyyes, native_not_up)
    if (
        notdown
        and (cohort_buy_to_sell or (direction == "SELL" and spot_majority(spot, "SELL") is True))
        and supplyyes
    ):
        eligible["DISTRIBUTION"] = count(
            features.get("large_ticket_sell_share_rising"),
            features.get("ask_depth_building"),
            features.get("exchange_inflows"),
        )
    if moved and depth_majority_lost is True and excess is True:
        eligible["LIQUIDITY_VACUUM"] = count(
            features.get("large_tickets_minority"), features.get("spot_volume_within_normal")
        )
    if liq.get("fragmentation", {}).get("validated") is True:
        eligible["REAL_MARKET_FRAGMENTATION"] = count(
            len({v["sign"] for v in spot["venues"] if v["sign"] != 0}) > 1,
            liq.get("fragmentation", {}).get("gap_closed_quickly"),
        )
    if (
        not eligible
        and oi.get("oi_class") == "PRICE_MARKUP"
        and oi.get("oi_usd_chg_pct", 0) > 0
        and oi.get("usd_beyond") is True
    ):
        eligible["PRICE_MARKUP_FAKE_OI"] = 0
    eligible_before_divergence = sorted(eligible)
    eligible_support_before_divergence = dict(eligible)
    if notdown and direction == "SELL":
        divergence["triggered"] = True
        candidates = {}
        refill = liq["replenishment"]
        bursts = refill["bursts"]
        rep = refill["replenished"]
        res = refill["resilient_bursts"]
        if nup or perpmost:
            candidates["PERP_LED_MARKUP"] = count(
                lead == "PERP",
                features.get("funding_rising") is True or features.get("basis_widening") is True,
                spot["classification"] in ("CONCENTRATED", "DIVERGENT"),
                features.get("perp_taker_flow_buy"),
            )
        refuted = (
            bursts is not None
            and bursts >= int(parameter("absorption_min_bursts", 2))
            and rep is not None
            and rep < bursts / 2
        ) or depth_majority_lost is True
        if not refuted:
            confirmed = (
                bursts is not None
                and bursts >= int(parameter("absorption_min_bursts", 2))
                and rep == bursts
                and res == bursts
                and native_not_up
            )
            candidates["ABSORPTION_CONFIRMED" if confirmed else "ABSORPTION_CANDIDATE"] = count(
                liq["spread_ratio"] is not None and liq["spread_ratio"] <= 1,
                (spot.get("size_profile") or {}).get("large_ticket_sell_share") is not None
                and (spot["size_profile"]["large_ticket_sell_share"] > 0.5)
                and weak is True,
                native_not_up,
            )
        if depth_majority_lost is True or excess is True:
            candidates["LIQUIDITY_VACUUM"] = count(liq["liquidity_state"] == "THIN", excess is True)
        if (
            liq.get("fragmentation", {}).get("persistent_gap") is True
            or liq.get("fragmentation", {}).get("funding_beyond_normal") is True
            or (
                spot["classification"] == "DIVERGENT"
                and liq.get("fragmentation", {}).get("top_sign_flips") is True
            )
        ) and not broad:
            candidates["FRAGMENTATION"] = count(
                features.get("localized_inventory_transfer"),
                liq.get("fragmentation", {}).get("gap_closed_quickly"),
            )
        for state in ("PERP_LED_MARKUP", "ABSORPTION_CANDIDATE", "LIQUIDITY_VACUUM", "FRAGMENTATION"):
            actual = (
                "ABSORPTION_CONFIRMED"
                if "ABSORPTION_CONFIRMED" in candidates and state == "ABSORPTION_CANDIDATE"
                else state
            )
            divergence["candidates"].append(
                {
                    "state": actual,
                    "eligible": actual in candidates,
                    "refuted": refuted
                    if state == "ABSORPTION_CANDIDATE"
                    else broad
                    if state == "FRAGMENTATION"
                    else False,
                    "testable": True
                    if state == "ABSORPTION_CANDIDATE"
                    else (nb is not None or bool(inc))
                    if state == "PERP_LED_MARKUP"
                    else drop is not None or excess is not None
                    if state == "LIQUIDITY_VACUUM"
                    else bool(liq.get("fragmentation", {})),
                    "support": candidates.get(actual, 0),
                }
            )
        if candidates:
            chosen = max(
                candidates,
                key=lambda x: (
                    candidates[x],
                    {
                        "LIQUIDITY_VACUUM": 4,
                        "PERP_LED_MARKUP": 3,
                        "ABSORPTION_CONFIRMED": 2,
                        "ABSORPTION_CANDIDATE": 1,
                        "FRAGMENTATION": 0,
                    }[x],
                ),
            )
            divergence.update(
                state=chosen,
                unresolved=[x for x in candidates if x != chosen],
                confirmed=chosen == "ABSORPTION_CONFIRMED",
            )
            move = {
                "PERP_LED_MARKUP": "PERP_LED_CHASE",
                "LIQUIDITY_VACUUM": "LIQUIDITY_VACUUM",
                "ABSORPTION_CONFIRMED": "PASSIVE_SPOT_ABSORPTION",
                "ABSORPTION_CANDIDATE": "PASSIVE_SPOT_ABSORPTION",
                "FRAGMENTATION": "REAL_MARKET_FRAGMENTATION"
                if liq.get("fragmentation", {}).get("validated")
                else "UNKNOWN",
            }[chosen]
            eligible = {move: candidates[chosen]}
        else:
            eligible = {}
            divergence["state"] = "UNKNOWN"
    ordered = sorted(eligible, key=lambda x: (-eligible[x], RISK_ORDER.index(x) if x in RISK_ORDER else 99))
    move = ordered[0] if ordered else "UNKNOWN"
    if confidence == "LOW":
        move = "UNKNOWN"
        divergence["state"] = "UNKNOWN" if divergence["triggered"] else "NONE"
    return dict(
        eligible_before_divergence=eligible_before_divergence,
        eligible_support_before_divergence=eligible_support_before_divergence,
        move_class=move,
        support=eligible.get(move, 0),
        alternatives=ordered if move == "UNKNOWN" else ordered[1:],
        divergence=divergence,
        class_confidence="LOW"
        if move == "UNKNOWN"
        else "MEDIUM"
        if len(ordered) > 1 or divergence["unresolved"]
        else "HIGH",
        flags=(
            ["PRICE_MARKUP_OI"]
            if any(
                row.get("oi_class") == "PRICE_MARKUP"
                and row.get("oi_usd_chg_pct") is not None
                and row["oi_usd_chg_pct"] > 0
                for row in [oi, *deriv["per_venue"]]
            )
            else []
        )
        + (
            ["NOT_COHORT_CONSISTENT"]
            if any(v is False for v in spot.get("cohort_consistent", {}).values())
            else []
        )
        + (["PRE_EVENT_RUNUP"] if relative.get("pre_event_runup", {}).get("flag") is True else [])
        + (
            ["FRAGMENTATION_INVESTIGATE"]
            if ("FRAGMENTATION" in divergence.get("unresolved", []) or divergence["state"] == "FRAGMENTATION")
            and liq.get("fragmentation", {}).get("validated") is not True
            else []
        ),
    )


def _signed_native(value, token_id):
    """A signed native quantity with thousands separators and its ticker (03 L8), e.g.
    "+506,057.72 AERO"; below one unit it keeps three significant digits."""
    amount = decimal(value)
    text = format(amount, "+,.2f") if abs(amount) >= 1 else format(amount, "+.3g")
    return text + " " + str(token_id).rsplit(":", 1)[-1]


def decisive_facts(
    spot, derivatives, liquidity, relative, supply, move_class, window, start, end, *, token_id
):
    """Card-ready measured facts with the exact evidence scope behind the class."""
    facts = []
    coverage = spot["coverage"]
    sync = derivatives.get("synchronized") or {}

    def add(metric, value, unit, text, collection, instruments, covered):
        if value is None:
            return
        facts.append(
            dict(
                metric=metric,
                value=value,
                unit=unit,
                window=window,
                start=iso(start),
                cutoff=iso(end),
                text=text,
                evidence=[
                    dict(collection=collection, instrument_id=iid, start=iso(start), cutoff=iso(end))
                    for iid in instruments
                ],
                coverage=covered,
            )
        )

    cvd = spot["composite_max"].get("cvd")
    direction = spot.get("direction")
    venues = spot["composite_max"].get("venues", [])
    spot_instruments = [v.get("instrument_id", v["venue"]) for v in spot.get("venues", [])]
    add(
        "spot.cvd",
        cvd,
        "NATIVE_TOKEN",
        f"{window} spot flow {direction or 'unknown'}: CVD {_signed_native(cvd, token_id)}, {spot['classification']}; {_count(coverage['n_complete'], 'complete venue')}."
        if cvd is not None
        else "",
        "mkt.flow_bars_1m",
        spot_instruments,
        coverage,
    )
    if facts:
        facts[-1]["evidence"] = [
            dict(collection="mkt.flow_bars_1m", venue=v, start=iso(start), cutoff=iso(end)) for v in venues
        ]
    oi = sync.get("oi_native_chg_pct")
    normal_state = sync.get("native_oi_vs_normal")
    oi_instruments = [
        p["instrument_id"] for p in derivatives.get("per_venue", []) if p["venue"] in sync.get("venues", [])
    ]
    oi_class_text = sync.get("oi_class")
    if oi_class_text is None:
        dec = sync.get("decomposition") or {}
        why = dec.get("detail") or dec.get("reason") or sync.get("oi_usd_detail") or "INCOMPLETE"
        venues_text = []
        for p in derivatives.get("per_venue", []):
            vdec = p.get("decomposition") or {}
            if p.get("oi_class") is None or vdec.get("pos_share") is None:
                continue
            chg = p.get("oi_native_chg_pct")
            markup = decimal(vdec.get("markup"))
            venues_text.append(
                f"{p['venue']} {p['oi_class']}"
                + (f" (native {chg:+.2%}" if chg is not None else " (")
                + f", position share {vdec['pos_share']:.1%}"
                + (f", price-markup component {markup:+,.0f} USD" if markup is not None else "")
                + ")"
            )
        oi_class_text = f"unavailable ({why})" + (
            "; by venue: " + ", ".join(venues_text) if venues_text else ""
        )
    add(
        "derivatives.oi_native_chg_pct",
        oi,
        "DECIMAL_RETURN",
        f"{window} synchronized native OI {oi:+.2%}, {normal_state or 'normal range unavailable'}; class {oi_class_text}."
        if oi is not None
        else "",
        "mkt.oi_obs",
        oi_instruments,
        dict(venues=sync.get("venues", []), classification_ready=sync.get("classification_ready")),
    )
    rel = relative["windows"][window]
    raw_measure = rel.get("relative_measure") == "R_RAW"
    r = rel.get("relative_return", rel.get("r_btc_adj"))
    ref = relative.get("ref_venue")
    span = rel.get("normal_range")
    add(
        "relative.r_raw" if raw_measure else "relative.r_btc_adj",
        r,
        "DECIMAL_RETURN",
        f"{window} {'price move (benchmark, raw)' if raw_measure else 'BTC-adjusted price move'} {r:+.2%}; "
        + (
            f"normal range {span[0]:+.2%} to {span[1]:+.2%}."
            if span
            else f"normal range unavailable ({rel.get('normal_range_reason') or 'INCOMPLETE'})."
        )
        if r is not None
        else "",
        "mkt.klines_1m",
        [ref] if ref else [],
        relative.get("reference_coverage", {}),
    )
    drop = liquidity.get("depth_drop")
    elasticity = liquidity.get("elasticity", {})
    hit = elasticity.get("hit_side")
    if move_class in ("LIQUIDITY_VACUUM", "SELL_SIDE_ABSORPTION", "PASSIVE_SPOT_ABSORPTION"):
        liquid = []
        if drop is not None:
            liquid.append(
                dict(
                    metric="liquidity.depth_drop",
                    value=drop,
                    unit="DECIMAL_FRACTION",
                    window=window,
                    start=iso(start),
                    cutoff=iso(end),
                    text=f"{hit or 'unknown'} depth within 1% changed {(-drop):+.2%}; excess response {elasticity.get('excess_response')}; weak response {elasticity.get('weak_response')}.",
                    evidence=[
                        dict(
                            collection="mkt.book_summaries",
                            venue=v["venue"],
                            start=iso(start),
                            cutoff=iso(end),
                        )
                        for v in liquidity.get("per_venue", [])
                    ],
                    coverage=dict(hit_side=hit, depth_usd=elasticity.get("hit_side_depth_p1_usd")),
                )
            )
        if liquid:
            facts = liquid + facts
    if move_class == "REAL_MARKET_FRAGMENTATION":
        f = liquidity.get("fragmentation", {})
        if f.get("validated") is True:
            facts.insert(
                0,
                dict(
                    metric="liquidity.fragmentation",
                    value=True,
                    unit="BOOLEAN",
                    window="W15M",
                    start=iso(end - timedelta(minutes=15)),
                    cutoff=iso(end),
                    text="Venue price gap exceeded both spreads for 15 minutes and was confirmed by independent public REST books.",
                    evidence=[
                        dict(collection="mkt.book_summaries", venues=pair) for pair in f.get("venues", [])
                    ],
                    coverage=dict(independent_path="PUBLIC_REST", validated=True),
                ),
            )
    if move_class == "SHORT_SQUEEZE":
        rates = {
            p["venue"]: dict(
                rate=p.get("funding_start"),
                interval_h=p.get("funding_start_interval_h"),
                observed_at=p.get("funding_start_observed_at"),
            )
            for p in derivatives.get("per_venue", [])
            if p.get("funding_start") is not None
        }
        if rates:
            readable = "; ".join(
                f"{venue} {r['rate']:+.4%} per {r['interval_h']}h" for venue, r in sorted(rates.items())
            )
            facts.insert(
                0,
                dict(
                    metric="derivatives.funding_start",
                    value=rates,
                    unit="RATE_PER_INTERVAL",
                    window=window,
                    start=iso(start),
                    cutoff=iso(end),
                    text=f"Funding at window start: {readable}.",
                    evidence=[
                        dict(
                            collection="mkt.funding_obs",
                            instrument_id=p["instrument_id"],
                            ts=p.get("funding_start_observed_at"),
                        )
                        for p in derivatives.get("per_venue", [])
                        if p.get("funding_start") is not None
                    ],
                    coverage=dict(venues=list(rates)),
                ),
            )
    if move_class == "DISTRIBUTION" and supply.get("supply_in_evidence") == "TRUE":
        inventory = supply.get("exchange_inventory", {})
        classified = [
            f
            for f in supply.get("flows", [])
            if f.get("class") in ("UNLOCK_DISTRIBUTION", "TREASURY_INTERNAL", "MM_INVENTORY")
        ]
        facts.insert(
            0,
            dict(
                metric="supply.supply_in_evidence",
                value=True,
                unit="BOOLEAN",
                window=window,
                start=iso(start),
                cutoff=iso(end),
                text="Supply in evidence: inventory change "
                + (
                    f"{float(inventory['chg_pct']):+.2%}"
                    if inventory.get("chg_pct") is not None
                    else "unknown"
                )
                + f"; labeled exchange transfers {len(classified)}.",
                evidence=[
                    dict(
                        collection="mkt.supply_context",
                        observed_at=inventory.get("observed_at"),
                        flows=[f.get("tx_hash") for f in classified],
                    )
                ],
                coverage=inventory.get("coverage", {"classified_flows": len(classified)}),
            ),
        )
    if move_class in ("SHORT_SQUEEZE", "PERP_LED_CHASE", "PRICE_MARKUP_FAKE_OI"):
        facts = sorted(
            facts,
            key=lambda f: (
                0 if f["metric"].startswith("derivatives") else 1 if f["metric"].startswith("relative") else 2
            ),
        )
    return facts[:3]


def canonical_fact_text(fact):
    """TDD decisive_facts is text[]; structured audit evidence lives separately."""
    coverage = fact.get("coverage") or {}
    if "n_complete" in coverage:
        share = coverage.get("covered_share_est")
        label = _count(coverage["n_complete"], "complete spot venue")
        if share is not None:
            label += f", {share:.1%} estimated volume coverage"
    elif "venues" in coverage:
        label = _count(len(coverage["venues"]), "covered venue")
    elif "hit_side" in coverage:
        depth_usd = coverage.get("depth_usd")
        label = f"{coverage.get('hit_side') or 'unknown side'} depth " + (
            f"{float(depth_usd):,.0f} USD" if depth_usd is not None else "unknown"
        )
    elif "complete_balances" in coverage:
        label = (
            f"{_count(coverage['complete_balances'], 'complete balance')}, "
            f"{coverage.get('comparable_balances', 0)} comparable"
        )
    elif coverage.get("validated") is True:
        label = "independent public REST confirmation"
    elif coverage and all(isinstance(v, dict) and "complete" in v for v in coverage.values()):
        label = _count(
            sum(v["complete"] is True for v in coverage.values()), "complete trailing-24h reference venue"
        )
    elif "classified_flows" in coverage:
        label = _count(coverage["classified_flows"], "classified flow")
    else:
        label = "coverage unavailable"
    return f"{fact['text']} [{fact['window']}; {fact['unit']}; {label}]"


def measurement_metadata(result):
    """Companion metadata preserves the literal §2 bare-field shapes.

    Every quantitative or null derived leaf inherits its nearest explicit window and analysis cutoff;
    observation time, precision and coverage stay unknown unless actually supplied.
    No inferred zero or fabricated observation timestamp is introduced.
    """
    missing_fields = {}
    skip = {
        "reason",
        "detail",
        "limiting",
        "evidence",
        "decisive_fact_details",
        "data_confidence",
        "measurement_metadata",
        "measurement_context",
        "config_version",
        "code_version",
        "event_id",
        "mta_id",
        "price_reason",
        "sector_reason",
        "precision_reason",
    }

    def unit(key, path):
        if key == "sum_sq_qty":
            return "NATIVE_TOKEN_SQUARED"
        if key in ("sign",):
            return "DIRECTION_SIGN"
        if key in ("hhi",):
            return "DIMENSIONLESS"
        if key in ("agreement", "velocity"):
            return "DIMENSIONLESS_RATIO"
        if key in ("support", "n", "trade_count", "expected_minutes", "complete_minutes"):
            return "COUNT"
        if "slippage_bps" in path:
            return "BPS"
        if "tier_notional" in path or "liquidations" in path:
            return "USD"
        if key in ("weak_response", "excess_response", "spot_confirmed", "flag"):
            return "BOOLEAN"
        if key == "bps_per_depth_unit":
            return "BPS_PER_DEPTH_UNIT"
        if key in ("rv24", "rv_24h") or ("vol_compression" in path and key == "normal_range"):
            return "LOG_RETURN_STDDEV"
        if key.endswith("normal_range") or key == "pctile":
            return "DECIMAL_FRACTION"
        if key == "funding_8h_eq":
            return "RATE_8H_EQ"
        if key.startswith("r_") or any(
            word in key for word in ("pct", "share", "ratio", "beta", "utilization", "drop")
        ):
            return "DECIMAL_FRACTION"
        if "bps" in key or key.startswith("basis"):
            return "BPS"
        if key.endswith("_s"):
            return "SECONDS"
        if key.endswith("_h"):
            return "HOURS"
        if any(word in key for word in ("usd", "notional", "position", "markup", "interaction")):
            return "USD"
        if any(word in key for word in ("native", "qty", "cvd")) or key == "volume":
            return "NATIVE_TOKEN"
        if "rate" in key or "funding" in key:
            return "RATE_PER_INTERVAL"
        if "price" in key or key == "mid":
            return "QUOTE_PRICE"
        if key.endswith("_at") or key in ("ts", "start", "cutoff"):
            return "UTC_TIMESTAMP"
        if key.startswith(("n_", "bursts", "replenished", "resilient")):
            return "COUNT"
        return "CLASSIFICATION_OR_STATE"

    def walk(value, path, window, context):
        if isinstance(value, dict):
            scoped = value.get("window", window)
            local = dict(context)
            if value.get("end_observed_at") is not None:
                local["observed_at"] = value["end_observed_at"]
            for field in ("observed_at", "precision", "coverage", "reason", "instrument_id", "venue"):
                if value.get(field) is not None:
                    local[field] = value[field]
            for key, child in value.items():
                if key in skip:
                    continue
                child_window = key if key in (*WINDOWS, "EVENT") else "EVENT" if key == "anchor" else scoped
                p = f"{path}.{key}" if path else key
                if child is None or isinstance(child, (int, float, Decimal)) and not isinstance(child, bool):
                    leaf_context = dict(local)
                    if key.startswith("funding_"):
                        field = (
                            "funding_start_observed_at"
                            if key.startswith("funding_start")
                            else "funding_estimate_observed_at"
                            if key
                            in ("funding_estimate", "funding_estimate_8h_eq", "funding_estimate_interval_h")
                            or (key == "funding_8h_eq" and value.get("funding_8h_kind") == "CURRENT_ESTIMATE")
                            else "funding_settled_last_observed_at"
                        )
                        leaf_context["observed_at"] = value.get(field)
                        leaf_context["precision"] = value.get("funding_precision")
                    if key in ("r_btc_adj", "r_eth_adj", "r_sector_adj"):
                        leaf_context["precision"] = None
                    if key.startswith("liq_"):
                        leaf_context["precision"] = None
                        leaf_context["observed_at"] = (value.get("liq_capture_window") or {}).get(
                            "observed_through"
                        )
                    reason = value.get(key + "_reason")
                    if key == "r_sector_adj":
                        reason = reason or value.get("sector_reason")
                    reason = (
                        reason
                        or local.get("reason")
                        or (
                            "NOT_ESTIMABLE"
                            if key
                            in (
                                "normal_range",
                                "native_normal_range",
                                "usd_normal_range",
                                "pctile",
                                "funding_vs_normal",
                                "funding_dispersion_8h",
                            )
                            else "INCOMPLETE"
                        )
                    )
                    if reason not in (
                        "NOT_SUPPORTED",
                        "SOURCE_FAILED",
                        "INCOMPLETE",
                        "INCOMPLETE",
                        "NOT_APPLICABLE",
                        "NOT_ESTIMABLE",
                        "PENDING",
                    ):
                        reason = "INCOMPLETE"
                    missing_fields[p] = dict(
                        value=child,
                        unit=value.get("unit") or unit(key, p),
                        window=child_window,
                        cutoff=result["cutoff"],
                        observed_at=leaf_context.get("observed_at"),
                        observation_reason=None if leaf_context.get("observed_at") else "INCOMPLETE",
                        precision=leaf_context.get("precision") if child is not None else None,
                        precision_reason=None
                        if child is not None and leaf_context.get("precision")
                        else "NOT_ESTIMABLE",
                        context_precision=leaf_context.get("precision"),
                        coverage=leaf_context.get("coverage"),
                        coverage_reason=None if leaf_context.get("coverage") is not None else "INCOMPLETE",
                        reason=reason if child is None else None,
                        representation="DECIMAL_STRING"
                        if isinstance(child, Decimal)
                        else "INTEGER"
                        if isinstance(child, int)
                        else "FLOAT64"
                        if isinstance(child, float)
                        else None,
                        evidence_scope={k: local[k] for k in ("instrument_id", "venue") if k in local},
                    )
                else:
                    if isinstance(child, list):
                        local = dict(local, unit_hint=value.get("unit") or unit(key, p))
                    walk(child, p, child_window, local)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                if isinstance(child, (dict, list)):
                    walk(child, f"{path}[{index}]", window, context)
                elif isinstance(child, (int, float, Decimal)) and not isinstance(child, bool):
                    missing_fields[f"{path}[{index}]"] = dict(
                        value=child,
                        unit=context.get("unit_hint", "DIMENSIONLESS"),
                        window=window,
                        cutoff=result["cutoff"],
                        observed_at=context.get("observed_at"),
                        observation_reason=None if context.get("observed_at") else "INCOMPLETE",
                        precision=context.get("precision"),
                        precision_reason=None if context.get("precision") else "NOT_ESTIMABLE",
                        coverage=context.get("coverage"),
                        coverage_reason=None if context.get("coverage") is not None else "INCOMPLETE",
                        reason=None,
                        representation="DECIMAL_STRING" if isinstance(child, Decimal) else "FLOAT64",
                    )

    walk(result, "", result["primary_window"], {})
    return missing_fields


def timestamp_companions(result, snapshot):
    """Declare selected bounds; propagate source precision without reading ISO formatting."""
    result = deepcopy(result)
    source_times = defaultdict(dict)
    temporal = {
        "received_ts",
        "exchange_ts",
        "open_time",
        "minute_start",
        "as_of",
        "settlement_time",
        "funding_time",
        "public_at",
        "known_to_us_at",
        "ts",
        "start",
        "end",
        "cutoff",
        "requested_cutoff",
        "assembled_at",
        "observed_at",
        "observed_through",
        "started_at",
        "finished_at",
        "latest_observed_at",
        "end_observed_at",
        "start_observed_at",
        "entry_price_observed_at",
        "price_observed_at",
        "funding_start_observed_at",
        "funding_settled_last_observed_at",
        "funding_estimate_observed_at",
        "rv24_observed_cutoff",
        "event_public_at",
    }

    needed = set()

    def collect(value, iid=None):
        if isinstance(value, list):
            for child in value:
                collect(child, iid)
        elif isinstance(value, dict):
            iid = value.get("instrument_id", value.get("ref_venue", iid))
            for key, child in value.items():
                if key in temporal and isinstance(child, str):
                    with contextlib.suppress(TypeError, ValueError):
                        needed.add((iid, parse_time(child)))
                else:
                    collect(child, iid)

    collect(result)
    time_fields = (
        "minute_start",
        "open_time",
        "exchange_ts",
        "ts",
        "as_of",
        "settlement_time",
        "funding_time",
        "observed_at",
    )
    for collection in (
        "flow_bars",
        "klines",
        "books",
        "oi",
        "funding",
        "liquidations",
        "supply",
    ):
        rows = snapshot.get(collection, [])
        rows = rows.values() if isinstance(rows, dict) else rows
        for row in rows:
            observed = _time(row)
            if observed is None:
                continue
            interval = row.get("interval_s")
            if (row.get("instrument_id"), observed) not in needed and not (
                interval is not None
                and (row.get("instrument_id"), observed + timedelta(seconds=int(interval))) in needed
            ):
                continue
            for field in time_fields:
                if row.get(field) is None:
                    continue
                stamp = propagate_timestamp(row, field, precision_aliases=("timestamp_precision",))
                source_times[(row.get("instrument_id"), stamp.value)].setdefault(collection, []).append(stamp)
                if field in ("open_time", "minute_start") and row.get("interval_s") is not None:
                    closed = offset_schedule(stamp, timedelta(seconds=int(row["interval_s"])))
                    source_times[(row.get("instrument_id"), closed.value)].setdefault(collection, []).append(
                        closed
                    )
                break
    for row in snapshot.get("flow_bars", []):
        for point in row.get("reference_points") or []:
            trade = point.get("last_trade")
            if trade:
                stamp = propagate_timestamp(trade, "ts")
                source_times[(row["instrument_id"], stamp.value)].setdefault("reference_points", []).append(
                    stamp
                )
        for name in ("first_trade", "last_trade"):
            trade = row.get(name)
            if trade:
                stamp = propagate_timestamp(trade, "ts")
                source_times[(row["instrument_id"], stamp.value)].setdefault("reference_points", []).append(
                    stamp
                )

    cutoff_stamp = (
        propagate_timestamp(snapshot, "cutoff")
        if "cutoff_prec" in snapshot or "cutoff_precision" in snapshot
        else chosen_boundary(snapshot["cutoff"], precision="EXACT")
    )
    anchor_stamp = propagate_timestamp(snapshot.get("anchor") or {}, "ts")

    def bound(child, path, key, value):
        if key == "requested_cutoff" and (
            "requested_cutoff_prec" in snapshot or "requested_cutoff_precision" in snapshot
        ):
            return propagate_timestamp(snapshot, "requested_cutoff")
        base = cutoff_stamp
        if key == "start" and (
            path.endswith(("pre_event_runup", ".EVENT"))
            or value.get("window") == "EVENT"
            or path == "analysis_window"
            and result["primary_window"] == "EVENT"
        ):
            base = anchor_stamp
        return (
            offset_schedule(base, parse_time(child) - parse_time(base.value))
            if base.value
            else propagate_timestamp({key: child}, key)
        )

    def walk(value, path="", iid=None):
        if isinstance(value, list):
            for child in value:
                walk(child, path, iid)
        elif isinstance(value, dict):
            iid = value.get("instrument_id", value.get("ref_venue", iid))
            for key, child in list(value.items()):
                if key in temporal and (child is None or isinstance(child, str)):
                    chosen = (
                        key in ("cutoff", "requested_cutoff")
                        or key in ("start", "end")
                        and (
                            "window" in value
                            or ".windows." in path
                            or path.endswith(("pre_event_runup", "lead_lag"))
                        )
                    )
                    if child is not None and chosen:
                        stamp = bound(child, path, key, value)
                    elif path == "relative.anchor" and key == "ts":
                        stamp = propagate_timestamp(snapshot.get("anchor") or {}, "ts")
                    elif path == "" and key in ("assembled_at", "event_public_at"):
                        stamp = propagate_timestamp(snapshot, key)
                    else:
                        stamp = propagate_timestamp(value, key)
                        candidates = (
                            [
                                stamp
                                for collection, stamps in source_times.get((iid, child), {}).items()
                                if (not key.startswith("funding_") or collection == "funding")
                                and (not path.startswith("relative.windows.") or collection == "klines")
                                and (
                                    key != "entry_price_observed_at"
                                    or collection in ("reference_points", "klines")
                                )
                                and (key != "price_observed_at" or collection == "reference_points")
                                for stamp in stamps
                            ]
                            if iid
                            else []
                        )
                        if (
                            child is not None
                            and stamp.precision is None
                            and candidates
                            and all(t.precision == candidates[0].precision for t in candidates)
                        ):
                            stamp = candidates[0]
                    value.update(stamp.fields(key))
                elif isinstance(child, (dict, list)) and key != "measurement_metadata":
                    walk(child, f"{path}.{key}" if path else key, iid)

    walk(result)
    return result


def bounded_market_snapshot(snapshot, end):
    snapshot = dict(snapshot)
    snapshot["flow_segments"] = [
        r for r in flow_segments(snapshot) if parse_time(r["end"]) <= end and _available(r, snapshot)
    ]
    snapshot["reference_points"] = list(snapshot.get("reference_points", [])) + [
        dict(point, instrument_id=bar["instrument_id"])
        for bar in snapshot.get("flow_bars", [])
        for point in (bar.get("reference_points") or [])
        if point.get("boundary") and parse_time(point["boundary"]) <= end
    ]
    for name in ("flow_bars", "klines", "books", "oi", "funding", "liquidations", "tickers", "supply"):
        snapshot[name] = [
            r
            for r in snapshot.get(name, [])
            if _time(r) is not None
            and _time(r) <= end
            and (
                name not in ("klines", "flow_bars")
                or _time(r) + timedelta(seconds=int(r.get("interval_s", 60))) <= end
            )
            and _available(r, snapshot)
        ]
    return snapshot


def spot_window_graph(snapshot, start, end, av):
    instruments = [
        i for i in snapshot.get("instruments", []) if i.get("token_id") in (None, snapshot.get("token_id"))
    ]
    primary = "EVENT" if av and 900 <= (end - start).total_seconds() <= 86400 else "W4H"
    snapshot["_primary"] = primary
    windows = dict(WINDOWS, EVENT=(end - start).total_seconds())
    spots = {
        name: spot_truth(
            snapshot["flow_bars"],
            snapshot["tickers"],
            instruments,
            end - timedelta(seconds=seconds),
            end,
            flow_segments=flow_segments(snapshot),
            assembled_at=snapshot.get("assembled_at"),
        )
        for name, seconds in windows.items()
    }
    cohort = set.intersection(*(set(v["venue"] for v in s["venues"]) for s in spots.values()))
    for name, s in spots.items():
        s.update(
            window=name,
            start=iso(end - timedelta(seconds=windows[name])),
            cutoff=iso(end),
            observed_at=iso(end) if s["venues"] else None,
            precision="BUCKET(60s)" if s["venues"] else None,
        )
        selected = [v for v in s["venues"] if v["venue"] in cohort]
        c = _composite(selected)
        share = ratio(c["volume"], s["composite_max"]["volume"])
        share_majority = strict_majority(c["volume"], s["composite_max"]["volume"])
        valid = len(cohort) >= int(parameter("min_spot_venues", 2)) and share_majority is True
        c.update(
            cohort=sorted(cohort),
            share_of_max=share,
            share_majority=share_majority,
            velocity=None,
            reason=None if valid else "NOT_COHORT_CONSISTENT",
        )
        s["composite_cohort"] = c if valid else None
        s["cohort_consistent"] = {other: valid for other in spots if other != name}
        prev = spot_truth(
            snapshot["flow_bars"],
            snapshot["tickers"],
            instruments,
            end - timedelta(seconds=2 * windows[name]),
            end - timedelta(seconds=windows[name]),
            flow_segments=flow_segments(snapshot),
            assembled_at=snapshot.get("assembled_at"),
        )
        pv = [v for v in prev["venues"] if v["venue"] in cohort]
        if valid and len(pv) == len(cohort):
            c["velocity"] = ratio(c["volume"], sum(v["volume_native"] for v in pv))
    return spots, windows, primary, instruments


@precise
def _compute_market_truth(snapshot, *, config=None):
    config = config or {}
    snapshot = dict(snapshot)
    end = parse_time(snapshot["cutoff"])
    anchor = snapshot.get("anchor")
    av = anchor.get("ts") if isinstance(anchor, dict) else anchor
    start = parse_time(av) if av else end - timedelta(hours=4)
    if start >= end:
        raise ValueError("anchor must precede cutoff")

    snapshot = bounded_market_snapshot(snapshot, end)

    spots, windows, primary, instruments = spot_window_graph(snapshot, start, end, av)
    snapshot["_has_event"] = bool(av)
    snapshot["_lead_start"] = start
    snapshot["_event_anchor"] = start if av else None
    relative = relative_truth(snapshot, start, end)
    derivative_windows = {}
    liquidity_windows = {}
    supply_windows = {}
    for name, seconds in windows.items():
        wstart = end - timedelta(seconds=seconds)
        scoped = dict(snapshot, _primary=name)
        d = derivatives_truth(scoped, wstart, end)
        window_liquidity = liquidity_truth(scoped, wstart, end, spots[name], relative)
        su = supply_truth(scoped, wstart, end)
        for record in (d, window_liquidity, su):
            record.update(window=name, start=iso(wstart), cutoff=iso(end))
        if d.get("synchronized"):
            d["synchronized"]["coverage"] = {"venues": d["synchronized"]["venues"]}
        if window_liquidity.get("per_venue"):
            window_liquidity["coverage"] = {"venues": [v["venue"] for v in window_liquidity["per_venue"]]}
            window_liquidity["observed_at"] = min(v["observed_at"] for v in window_liquidity["per_venue"])
        window_liquidity["elasticity"].update(window=name, start=iso(wstart), cutoff=iso(end))
        window_liquidity["replenishment"].update(window=name, start=iso(wstart), cutoff=iso(end))
        enrich_evidence(scoped, wstart, end, spots, name, d, relative, window_liquidity)
        d["support_features"] = support_features(scoped, wstart, end, spots[name], d, window_liquidity, su)
        if name == "EVENT" and not av:
            d = derivatives_truth(
                dict(scoped, oi=[], oi_sql=truth_sql.NO_OI, funding=[], liquidations=[], klines=[]),
                wstart,
                end,
            )
            d.update(window=name, start=None, cutoff=iso(end), reason="INCOMPLETE")
            window_liquidity = {"window": name, "start": None, "cutoff": iso(end), "reason": "INCOMPLETE"}
            su = {"window": name, "start": None, "cutoff": iso(end), "reason": "INCOMPLETE"}
        derivative_windows[name] = d
        liquidity_windows[name] = window_liquidity
        supply_windows[name] = su
    deriv = dict(derivative_windows[primary], windows=derivative_windows)
    liquidity = dict(liquidity_windows[primary], windows=liquidity_windows)
    supply = dict(supply_windows[primary], windows=supply_windows)
    snapshot["_relative_reference"] = relative
    conf = data_confidence(snapshot, spots[primary], derivative_windows[primary], end)
    conf["class"] = conf.pop("class_")
    if not av:
        relative["anchor"] = {"type": None, "ts": None, "ref_price": None, "reason": "INCOMPLETE"}
        relative["repricing_band"] = None
        relative["windows"]["EVENT"] = {k: None for k in relative["windows"]["EVENT"]}
        relative["windows"]["EVENT"]["reason"] = "INCOMPLETE"
        spots["EVENT"]["reason"] = "INCOMPLETE"
        spots["EVENT"]["classification"] = "INSUFFICIENT_DATA"
    selected = select_move(spots[primary], deriv, liquidity, supply, relative, conf["class"], primary)
    pstart = end - timedelta(seconds=windows[primary])
    facts = decisive_facts(
        spots[primary],
        deriv,
        liquidity,
        relative,
        supply,
        selected["move_class"],
        primary,
        pstart,
        end,
        token_id=snapshot.get("token_id"),
    )
    for fact in facts:
        fact["text"] = canonical_fact_text(fact)
    capture = snapshot.get("capture_status") or {"status": "UNKNOWN", "reason": "CAPTURE_STATUS_NOT_PROVIDED"}
    has_perps = any(_market(i) != "SPOT" for i in instruments)
    window_readiness = {
        name: dict(
            spot_ready=spots[name]["classification"] != "INSUFFICIENT_DATA",
            derivatives_native_ready=not has_perps
            or derivative_windows[name].get("synchronized") is not None,
            derivatives_classification_ready=not has_perps
            or bool((derivative_windows[name].get("synchronized") or {}).get("classification_ready")),
            book_depth_ready=bool(liquidity_windows[name].get("per_venue"))
            and all(
                v.get("bid_depth_p1_usd") is not None and v.get("ask_depth_p1_usd") is not None
                for v in liquidity_windows[name].get("per_venue", [])
            ),
            reference_ready=relative.get("ref_venue") is not None
            and relative.get("anchor", {}).get("ref_price") is not None,
        )
        for name in windows
    }
    incomplete = [name for name, ready in window_readiness.items() if not all(ready.values())]
    readiness = dict(
        capture_status=capture,
        backfill_runs=[
            {k: r.get(k) for k in ("backfill_id", "status", "started_at", "finished_at", "anchor")}
            for r in snapshot.get("backfill_runs", [])
        ],
        windows=window_readiness,
        incomplete_windows=incomplete,
        primary_window=primary,
        primary_window_ready=primary not in incomplete,
        prerequisite_pending=capture.get("status") in ("PENDING", "RUNNING", "PARTIAL", "FAILED"),
    )
    conf["capture_readiness"] = readiness
    result = dict(
        assembled_at=snapshot.get("assembled_at"),
        time_policy="SEMANTIC_MARKET_TIME_WITH_ASSEMBLY_AVAILABILITY",
        token_id=snapshot.get("token_id"),
        event_id=snapshot.get("event_id"),
        cutoff=iso(end),
        requested_cutoff=snapshot.get("requested_cutoff", snapshot.get("cutoff")),
        event_public_at=snapshot.get("event_public_at"),
        primary_window=primary,
        analysis_window=dict(window=primary, start=iso(start), end=iso(end)),
        decisive_facts=[f["text"] for f in facts],
        decisive_fact_details=facts,
        decisive_facts_reason=None if len(facts) >= 2 else "INCOMPLETE",
        detail=readiness,
        spot=spots,
        derivatives=deriv,
        liquidity=liquidity,
        supply=supply,
        relative=relative,
        data_confidence=conf,
        **selected,
        config_version=config.get("config_version", snapshot.get("config_version", "unversioned")),
        code_version=config.get("code_version", snapshot.get("code_version", "unversioned")),
    )
    result["measurement_context"] = {
        "cutoff": iso(end),
        "window_inheritance": "Nearest enclosing explicit window; canonical map keys override; anchor is EVENT",
        "shape_policy": "Literal TDD §2 bare fields retained; all quantitative and null derived leaves described in measurement_metadata",
        "unknown_precision": "Null with NOT_ESTIMABLE; no fabricated timestamp precision",
        "time_policy": result["time_policy"],
        "numeric_policy": "Native quantities, USD amounts and funding rates use adaptive Decimal arithmetic (128-digit minimum), serialized as decimal strings; dimensionless returns, beta, percentiles and ratios use FLOAT64 statistics; significance compares CVD squared against four times sum of squares exactly",
    }
    result = timestamp_companions(result, snapshot)
    result["measurement_metadata"] = measurement_metadata(result)
    return jsonable(result)


_COMPUTE_SLOT = threading.RLock()


def compute_market_truth(snapshot, *, config=None, baseline_cache=None):
    """baseline_cache: a dict the caller keeps per token across assessments. Its keys carry
    exact history fingerprints (and config), so a reused entry is the same computation."""
    with _COMPUTE_SLOT:
        return _compute_market_truth_slot(snapshot, config=config, baseline_cache=baseline_cache)


def _compute_market_truth_slot(snapshot, *, config=None, baseline_cache=None):
    config = config or {}
    if config.get("market_context_rule", "normal_range") != "normal_range":
        raise ValueError("Only the approved §5.7 normal_range market context rule is supported")
    token = _PARAMETERS.set(config)
    baselines = _BASELINE_CACHE.set(baseline_cache if baseline_cache is not None else {})
    dependency = _BASELINE_DEPENDENCY.set(None)
    inputs = _ASSESSMENT_INPUTS.set({})
    times = _TIMES.set({})
    try:
        return _compute_market_truth(snapshot, config=config)
    finally:
        _TIMES.reset(times)
        _ASSESSMENT_INPUTS.reset(inputs)
        _BASELINE_DEPENDENCY.reset(dependency)
        _BASELINE_CACHE.reset(baselines)
        _PARAMETERS.reset(token)


SPOT_CONDITION_METRICS = {
    "spot.cohort.direction",
    "spot.cohort.cwb_buy",
    "spot.cohort.cwb_sell",
    "spot.classification",
}
DERIVATIVE_CONDITION_METRICS = {
    "deriv.native_oi_chg_pct",
    "deriv.native_oi_vs_normal",
    "deriv.oi_class",
    "deriv.funding_vs_normal",
}


def condition_reference_close(snapshot, end):
    evidence = snapshot.get("relative_sql") or truth_sql.NO_RELATIVE
    reference = next((r for r in evidence.references if r.instrument_id == evidence.ref), None)
    selected = (
        reference
        if reference is not None
        and reference.hourly_close_time == end.replace(minute=0, second=0, microsecond=0)
        else None
    )
    observed = propagate_timestamp({}, "observed_at")
    if selected is not None:
        observed = offset_schedule(
            propagate_timestamp(
                {
                    "open_time": selected.hourly_close_time - timedelta(seconds=60),
                    "open_time_prec": selected.hourly_close_prec,
                    "open_time_prec_reason": selected.hourly_close_prec_reason,
                },
                "open_time",
            ),
            timedelta(seconds=60),
        )
    return dict(
        ref_venue=evidence.ref,
        close_1h_price=selected.hourly_close if selected is not None else None,
        **observed.fields("close_1h_observed_at"),
    )


def condition_primary_bounds(snapshot, end):
    anchor = snapshot.get("anchor") or {}
    start = parse_time(anchor["ts"]) if anchor.get("ts") else end - timedelta(hours=4)
    return (
        (start, "EVENT")
        if anchor.get("ts") and 900 <= (end - start).total_seconds() <= 86400
        else (end - timedelta(hours=4), "W4H")
    )


def condition_confidence(snapshot, end):
    start, window = condition_primary_bounds(snapshot, end)
    instruments = [
        r for r in snapshot.get("instruments", []) if r.get("token_id") in (None, snapshot.get("token_id"))
    ]
    spot = spot_truth(
        snapshot.get("flow_bars", []),
        snapshot.get("tickers", []),
        instruments,
        start,
        end,
        flow_segments=flow_segments(snapshot),
        assembled_at=snapshot.get("assembled_at"),
    )
    derivatives = {
        "synchronized": oi_synchronized(
            snapshot,
            window,
            end - start,
            [(i["instrument_id"], _venue(i)) for i in instruments if _market(i) != "SPOT"],
        )
    }
    value = data_confidence(snapshot, spot, derivatives, end)
    value["class"] = value.pop("class_")
    value["derivatives_classification_ready"] = None
    value["derivatives_classification_ready_reason"] = "NOT_ESTIMABLE"
    value["analysis_window"] = {"window": window, "start": iso(start), "cutoff": iso(end)}
    return value


def condition_depth(snapshot, end):
    grouped = defaultdict(list)
    for book in snapshot.get("books", []):
        if (
            _market(book) == "SPOT"
            and book.get("token_id", snapshot.get("token_id")) == snapshot.get("token_id")
            and _time(book) <= end
            and book.get("integrity_ok") is True
        ):
            grouped[_venue(book)].append(book)
    anchor = snapshot.get("anchor") or {}
    start = parse_time(anchor["ts"]) if anchor.get("ts") else None
    current = []
    spreads = []
    previous_spreads = []
    previous = []
    observations = []
    for venue, rows in grouped.items():
        latest = max(rows, key=_time)
        bid, ask = depth_at_band(latest, "bid"), depth_at_band(latest, "ask")
        if bid is not None and ask is not None:
            current.append(bid + ask)
        spread = number(latest.get("spread_bps"))
        if spread is not None:
            spreads.append(spread)
        before = [
            row for row in rows if start is not None and start - timedelta(hours=1) <= _time(row) < start
        ]
        old_spreads = [number(row["spread_bps"]) for row in before if row.get("spread_bps") is not None]
        if old_spreads:
            previous_spreads.append(mean(old_spreads))
        depths = [
            depth_at_band(row, "bid") + depth_at_band(row, "ask")
            for row in before
            if depth_at_band(row, "bid") is not None and depth_at_band(row, "ask") is not None
        ]
        if depths:
            baseline = _BASELINE_CACHE.get()
            key = (
                "depth_pre",
                venue,
                iso(start),
                parameter("depth_band_pct", 1),
                fingerprint(
                    [
                        (iso(_time(row)), depth_at_band(row, "bid"), depth_at_band(row, "ask"))
                        for row in before
                    ]
                ),
            )
            if baseline is not None and key in baseline:
                previous.append(baseline[key])
            else:
                value = mean(depths)
                previous.append(value)
                if baseline is not None:
                    baseline[key] = value
                    if len(baseline) > 128:
                        baseline.pop(next(iter(baseline)))
        cadence, source = capture_cadence(snapshot, "books", latest)
        observations.append(
            dict(
                venue=venue,
                observed_at=iso(_time(latest)),
                cadence_s=cadence,
                cadence_source=source,
                fresh=cadence is not None and (end - _time(latest)).total_seconds() <= cadence,
            )
        )
    return {
        "aggregate_depth_p1_usd": sum(current) if current and len(current) == len(grouped) else None,
        "pre_event_depth_p1_usd": sum(previous) if previous and len(previous) == len(grouped) else None,
        "observations": observations,
        "spread_ratio": ratio(mean(spreads), mean(previous_spreads))
        if spreads and previous_spreads
        else None,
    }


COMPOSITE_CONDITION_METRICS = {"mt.move_class", "mt.divergence_state"}
RELATIVE_CONDITION_METRICS = {"price.repricing_band", "price.vs_anchor_pct"}
OI_CONDITION_METRICS = DERIVATIVE_CONDITION_METRICS - {"deriv.funding_vs_normal"}


def condition_oi_windows(snapshot, requirements):
    """The windows (name to seconds) whose open-interest records a condition evaluation reads, and whether
    their history is needed: the requested window of an open-interest metric, the primary window of a state
    or of a data confidence (whose endpoints alone are enough)."""
    end = parse_time(snapshot["cutoff"])
    lengths = dict(WINDOWS)
    anchor = (snapshot.get("anchor") or {}).get("ts")
    if anchor:
        lengths["EVENT"] = (end - parse_time(anchor)).total_seconds()
    start, primary = condition_primary_bounds(snapshot, end)
    windows, history = {}, False
    for requirement in requirements:
        metric = requirement["metric"]
        if metric in OI_CONDITION_METRICS:
            name = requirement.get("window", "W1H")
            if lengths.get(name, 0) > 0:
                windows[name] = lengths[name]
                history = True
        elif metric in COMPOSITE_CONDITION_METRICS or metric == "dc.class":
            windows[primary] = (end - start).total_seconds()
            history = history or metric in COMPOSITE_CONDITION_METRICS
    return windows, history


def condition_market_state(snapshot, end, cache):
    """The required primary-window state graph, without MTA/outcome/card persistence work."""
    av = (snapshot.get("anchor") or {}).get("ts")
    start = parse_time(av) if av else end - timedelta(hours=4)
    if start >= end:
        return {
            "move_class": "UNKNOWN",
            "divergence": {"state": "UNKNOWN"},
            "data_confidence": {"class": "LOW"},
        }

    def node(name, fields, build):
        head = (snapshot.get("domain_dependency_fingerprints") or {}).get(name)
        if head is None:
            head = fingerprint({field: snapshot.get(field) for field in fields})
        elif not isinstance(head, str):
            head = fingerprint(head)
        for evidence in ("relative_sql", "volume_sql", "oi_sql"):
            if evidence in fields:
                head = fingerprint([head, snapshot.get(evidence)])
        key = (
            "condition-state-node",
            name,
            iso(start),
            iso(end),
            head,
            fingerprint(_PARAMETERS.get()),
            fingerprint(snapshot.get("expected_direction")),
        )

        return deepcopy(remember(cache, key, build))

    spots, windows, primary, instruments = node(
        "spot",
        ("flow_bars", "flow_segments", "tickers", "instruments", "anchor"),
        lambda: spot_window_graph(snapshot, start, end, av),
    )
    scoped = dict(
        snapshot,
        _primary=primary,
        _has_event=bool(av),
        _lead_start=start,
        _event_anchor=start if av else None,
    )
    relative = node(
        "relative",
        (
            "klines",
            "relative_sql",
            "flow_bars",
            "flow_segments",
            "reference_points",
            "tokens",
            "sector_baskets",
            "instruments",
            "expected_direction",
        ),
        lambda: relative_truth(scoped, start, end, include_outcome=False),
    )
    wstart = end - timedelta(seconds=windows[primary])
    derivatives = node(
        "derivatives",
        ("oi", "oi_sql", "funding", "liquidations", "semantics", "instruments", "klines", "volume_sql"),
        lambda: derivatives_truth(scoped, wstart, end),
    )
    liquidity = liquidity_truth(scoped, wstart, end, spots[primary], relative)
    supply = node("supply", ("supply", "anchor"), lambda: supply_truth(scoped, wstart, end))
    enrich_evidence(scoped, wstart, end, spots, primary, derivatives, relative, liquidity)
    derivatives["support_features"] = support_features(
        scoped, wstart, end, spots[primary], derivatives, liquidity, supply
    )
    conf = data_confidence(snapshot, spots[primary], derivatives, end)
    conf["class"] = conf.pop("class_")
    if not av:
        relative["repricing_band"] = None
        relative["windows"]["EVENT"]["r_raw"] = None
    selected = select_move(spots[primary], derivatives, liquidity, supply, relative, conf["class"], primary)
    return dict(
        spot=spots,
        derivatives={"windows": {primary: derivatives}},
        liquidity=liquidity,
        supply=supply,
        relative=relative,
        data_confidence=conf,
        primary_window=primary,
        **selected,
    )


@precise
def compute_condition_values(snapshot, requirements, *, config=None, cache=None, baseline_cache=None):
    """Transient requested-domain graph; no full MTA orchestration or persistence."""
    config = config or {}
    cache = cache if cache is not None else {}
    baseline_cache = baseline_cache if baseline_cache is not None else {}
    parameters = _PARAMETERS.set(config)
    baselines = _BASELINE_CACHE.set(baseline_cache)
    try:
        end = parse_time(snapshot["cutoff"])
        snapshot = bounded_market_snapshot(snapshot, end)
        result = {"cutoff": iso(end), "spot": {}, "derivatives": {"windows": {}}, "relative": {}}
        requested = sorted({(r["metric"], r.get("window", "W1H")) for r in requirements})
        pending = []
        proofs = []
        for metric, window in requested:
            seconds = WINDOWS.get(window)
            if window == "EVENT" and snapshot.get("anchor"):
                seconds = (end - parse_time(snapshot["anchor"]["ts"])).total_seconds()
            if not seconds or seconds <= 0:
                pending.append(dict(metric=metric, window=window, reason="NOT_SUPPORTED"))
                continue
            if metric in SPOT_CONDITION_METRICS:
                domain = "spot"
                fields = ("flow_bars", "flow_segments", "tickers", "instruments")
            elif metric in DERIVATIVE_CONDITION_METRICS:
                domain = "funding" if metric == "deriv.funding_vs_normal" else "oi"
                fields = (domain, "instruments", "semantics", "capture_metadata", "oi_sql")
            elif metric == "price.close_vs_level":
                domain = "close"
                fields = ("relative_sql", "instruments")
            elif metric == "dc.class":
                domain = "dc"
                fields = (
                    "flow_bars",
                    "flow_segments",
                    "tickers",
                    "instruments",
                    "semantics",
                    "capture_metadata",
                    "oi",
                    "oi_sql",
                    "funding",
                    "klines",
                    "books",
                    "liquidations",
                    "supply",
                    "anchor",
                )
                seconds = (end - condition_primary_bounds(snapshot, end)[0]).total_seconds()
            elif metric in ("liq.depth_p1_vs_pre", "liq.spread_ratio"):
                domain = "depth"
                fields = ("books", "instruments", "anchor", "capture_metadata")
            elif metric in COMPOSITE_CONDITION_METRICS:
                domain = "state"
                fields = (
                    "flow_bars",
                    "flow_segments",
                    "reference_points",
                    "tickers",
                    "instruments",
                    "tokens",
                    "sector_baskets",
                    "semantics",
                    "capture_metadata",
                    "oi",
                    "oi_sql",
                    "funding",
                    "klines",
                    "books",
                    "liquidations",
                    "supply",
                    "anchor",
                    "expected_direction",
                    "funding_confirmations",
                    "relative_sql",
                    "volume_sql",
                    "series_coverage_counts",
                )
                seconds = (end - condition_primary_bounds(snapshot, end)[0]).total_seconds()
            elif metric in RELATIVE_CONDITION_METRICS:
                domain = "relative"
                fields = (
                    "flow_bars",
                    "flow_segments",
                    "reference_points",
                    "instruments",
                    "tokens",
                    "sector_baskets",
                    "klines",
                    "anchor",
                    "expected_direction",
                    "funding_confirmations",
                    "relative_sql",
                    "series_coverage_counts",
                )
            else:
                pending.append(dict(metric=metric, window=window, reason="NOT_SUPPORTED"))
                continue
            dependency = (snapshot.get("domain_dependency_fingerprints") or {}).get(domain)
            if dependency is None:
                dependency = fingerprint({field: snapshot.get(field) for field in fields})
            elif not isinstance(dependency, str):
                dependency = fingerprint(dependency)
            for evidence in ("relative_sql", "volume_sql", "oi_sql"):
                if evidence in fields:
                    dependency = fingerprint([dependency, snapshot.get(evidence)])
            key = (
                domain,
                window,
                seconds,
                iso(end),
                dependency,
                fingerprint(config),
                fingerprint(snapshot.get("expected_direction")),
            )
            hit = key in cache
            if not hit:
                selected = {field: snapshot.get(field, []) for field in fields}
                selected.update(
                    token_id=snapshot.get("token_id"),
                    cutoff=iso(end),
                    assembled_at=snapshot.get("assembled_at"),
                    domain_dependency_fingerprints=snapshot.get("domain_dependency_fingerprints", {}),
                )
                start = end - timedelta(seconds=seconds)
                if domain == "spot":
                    value = spot_truth(
                        selected.get("flow_bars", []),
                        selected.get("tickers", []),
                        selected.get("instruments", []),
                        start,
                        end,
                        flow_segments=flow_segments(selected),
                        assembled_at=selected.get("assembled_at"),
                    )
                elif domain == "close":
                    value = condition_reference_close(selected, end)
                elif domain == "dc":
                    value = condition_confidence(selected, end)
                elif domain == "depth":
                    value = condition_depth(selected, end)
                elif domain == "state":
                    value = condition_market_state(selected, end, cache)
                elif domain == "relative":
                    anchor = (selected.get("anchor") or {}).get("ts")
                    value = relative_truth(
                        selected,
                        parse_time(anchor) if anchor else end - timedelta(hours=4),
                        end,
                        include_outcome=False,
                    )
                    if not anchor:
                        value["repricing_band"] = None
                        value["windows"]["EVENT"]["r_raw"] = None
                else:
                    selected["_primary"] = window
                    history_head = (snapshot.get("history_dependency_fingerprints") or {}).get(domain, {})
                    history_head = history_head.get(window) if isinstance(history_head, dict) else None
                    if history_head is not None and not isinstance(history_head, str):
                        history_head = fingerprint(history_head)
                    history_token = _BASELINE_DEPENDENCY.set(history_head)
                    try:
                        value = derivatives_truth(selected, start, end)
                    finally:
                        _BASELINE_DEPENDENCY.reset(history_token)
                cache[key] = value
                if len(cache) > 128:
                    cache.pop(next(iter(cache)))
            value = deepcopy(cache[key])
            fresh = True
            observations = []
            if domain in ("oi", "funding"):
                for instrument in value.get("per_venue", []):
                    if domain == "oi" and instrument["venue"] not in (value.get("synchronized") or {}).get(
                        "venues", []
                    ):
                        continue
                    rows = [
                        r
                        for r in snapshot.get(domain, [])
                        if r.get("instrument_id") == instrument["instrument_id"]
                    ]
                    latest = max(rows, key=_time) if rows else None
                    cadence, source = capture_cadence(snapshot, domain, latest)
                    field = next(
                        (
                            name
                            for name in (
                                "minute_start",
                                "open_time",
                                "exchange_ts",
                                "ts",
                                "as_of",
                                "settlement_time",
                                "funding_time",
                                "observed_at",
                            )
                            if latest and latest.get(name) is not None
                        ),
                        "ts",
                    )
                    observed = propagate_timestamp(
                        latest or {}, field, precision_aliases=("timestamp_precision", "precision")
                    )
                    observations.append(
                        dict(
                            instrument_id=instrument["instrument_id"],
                            **observed.fields("observed_at"),
                            cadence_s=cadence,
                            cadence_source=source,
                        )
                    )
                    if latest is None or cadence is None or (end - _time(latest)).total_seconds() > cadence:
                        fresh = False
                        if domain == "funding":
                            instrument["funding_vs_normal"] = None
                if domain == "oi" and not fresh:
                    value["synchronized"] = None
            if domain == "depth":
                observations = value.pop("observations")
                fresh = bool(observations) and all(row["fresh"] for row in observations)
                if not fresh:
                    value["aggregate_depth_p1_usd"] = None
                    value["spread_ratio"] = None
            if domain == "spot":
                result["spot"][window] = value
            elif domain == "close":
                result["relative"].update(value)
            elif domain == "dc":
                result["data_confidence"] = value
            elif domain == "depth":
                result["liquidity"] = value
            elif domain == "state":
                for state_window, pack in value["derivatives"]["windows"].items():
                    prior = result["derivatives"]["windows"].get(state_window, {})
                    if any(
                        m in DERIVATIVE_CONDITION_METRICS
                        and m != "deriv.funding_vs_normal"
                        and w == state_window
                        for m, w in requested
                    ):
                        pack["synchronized"] = prior.get("synchronized")
                    if ("deriv.funding_vs_normal", state_window) in requested:
                        pack["per_venue"] = prior.get("per_venue", [])
                value["derivatives"]["windows"] = {
                    **result["derivatives"]["windows"],
                    **value["derivatives"]["windows"],
                }
                result.update(value)
            elif domain == "relative":
                result["relative"].update(value)
            else:
                pack = result["derivatives"]["windows"].setdefault(
                    window, {"per_venue": [], "synchronized": None}
                )
                if domain == "oi":
                    pack["synchronized"] = value.get("synchronized")
                else:
                    pack["per_venue"] = value.get("per_venue", [])
            if domain == "spot":
                available = value.get("direction") is not None
            elif domain == "close":
                available = value.get("close_1h_price") is not None
            elif domain == "dc":
                available = value.get("class") is not None
            elif domain == "depth":
                available = (
                    value.get("spread_ratio") is not None
                    if metric == "liq.spread_ratio"
                    else value.get("aggregate_depth_p1_usd") is not None
                    and value.get("pre_event_depth_p1_usd") not in (None, 0)
                )
            elif domain == "state":
                available = value.get("move_class") is not None
            elif domain == "relative":
                available = (
                    value.get("repricing_band") is not None
                    if metric == "price.repricing_band"
                    else value.get("windows", {}).get("EVENT", {}).get("r_raw") is not None
                )
            elif domain == "funding":
                available = bool(value.get("per_venue")) and all(
                    p.get("funding_vs_normal") is not None for p in value["per_venue"]
                )
            else:
                field = {
                    "deriv.native_oi_chg_pct": "oi_native_chg_pct",
                    "deriv.native_oi_vs_normal": "native_oi_vs_normal",
                    "deriv.oi_class": "oi_class",
                }[metric]
                available = (value.get("synchronized") or {}).get(field) is not None
            if not available:
                pending.append(dict(metric=metric, window=window, reason="INCOMPLETE"))
            proofs.append(
                dict(
                    metric=metric,
                    window=window,
                    dependency_fingerprint=dependency,
                    reused=hit,
                    reason=None if fresh and available else "INCOMPLETE",
                    observations=observations,
                    available=available,
                    coverage=value.get("coverage")
                    if domain == "spot"
                    else {"venues": (value.get("synchronized") or {}).get("venues", [])}
                    if domain == "oi"
                    else None,
                )
            )
        result["_condition_evaluation"] = {
            "requires_market_truth": [],
            "unavailable_metrics": pending,
            "request_key": fingerprint(
                dict(
                    token_id=snapshot.get("token_id"),
                    requirements=requested,
                    dependencies=snapshot.get(
                        "dependency_fingerprint", [p["dependency_fingerprint"] for p in proofs]
                    ),
                    anchor=snapshot.get("anchor"),
                    config=config,
                )
            ),
            "proofs": proofs,
            "transient": True,
        }
        result.update(propagate_timestamp(snapshot, "cutoff").fields("cutoff"))
        return jsonable(result)
    finally:
        _BASELINE_CACHE.reset(baselines)
        _PARAMETERS.reset(parameters)


def _cutoff_value(cutoff):
    """The instant of a Market Truth cutoff given as a Timestamp or a plain time."""
    return cutoff.value if isinstance(cutoff, Timestamp) else cutoff


async def with_relative(repo, snapshot, config, *, include_outcome=True, baselines=True):
    """The snapshot with the records of the price-relative statements (03 §5.7) as `relative_sql`.

    `baselines=False` reads only the reference prices; a snapshot whose anchor does not precede its
    cutoff is returned as it is."""
    ch = _market_client(repo)
    end = parse_time(snapshot["cutoff"])
    anchor = snapshot.get("anchor")
    anchor_at = anchor.get("ts") if isinstance(anchor, dict) else anchor
    start = parse_time(anchor_at) if anchor_at else end - timedelta(hours=4)
    if start >= end:
        return snapshot
    evidence = await relative_evidence(
        ch, snapshot, start, end, config, include_outcome=include_outcome, baselines=baselines
    )
    return dict(snapshot, relative_sql=evidence)


async def with_volumes(repo, snapshot, config, *, primary_only=False):
    """The snapshot with the records of the volume statements (03 §5.4, §5.7) as `volume_sql`.

    The windows are those of the assessment (W15M, W1H, W4H, W24H and EVENT), or with `primary_only` the
    primary window of a condition; a snapshot whose anchor does not precede its cutoff is returned as it
    is."""
    ch = _market_client(repo)
    end = parse_time(snapshot["cutoff"])
    anchor = snapshot.get("anchor")
    anchor_at = anchor.get("ts") if isinstance(anchor, dict) else anchor
    start = parse_time(anchor_at) if anchor_at else end - timedelta(hours=4)
    if start >= end:
        return snapshot
    windows = dict(WINDOWS, EVENT=(end - start).total_seconds())
    if primary_only:
        name = condition_primary_bounds(snapshot, end)[1]
        windows = {name: windows[name]}
    return dict(snapshot, volume_sql=await volume_evidence(ch, snapshot, end, config, windows))


async def with_oi(repo, snapshot, config, *, windows=None, history=True):
    """The snapshot with the records of the open-interest statements (03 §5.4, §5.7) as `oi_sql`.

    The windows are those of the assessment (W15M, W1H, W4H, W24H and EVENT) unless given; the history is
    sampled before `recent_start`, twice the longer of a day and the event window (and a minute) before the
    cutoff, the same for every path. A snapshot whose anchor does not precede its cutoff is returned as it
    is."""
    ch = _market_client(repo)
    end = parse_time(snapshot["cutoff"])
    anchor = snapshot.get("anchor")
    anchor_at = anchor.get("ts") if isinstance(anchor, dict) else anchor
    start = parse_time(anchor_at) if anchor_at else end - timedelta(hours=4)
    if start >= end:
        return snapshot
    if windows is None:
        windows = dict(WINDOWS, EVENT=(end - start).total_seconds())
    lookback = timedelta(
        days=int(_knob(config, "historical_lookback_days", _knob(config, "base_retention_days", 90)))
    )
    duration = max(timedelta(days=1), end - start) if anchor_at else timedelta(days=1)
    recent_start = max(end - lookback, end - 2 * duration - timedelta(minutes=1))
    return dict(
        snapshot,
        oi_sql=await oi_evidence(ch, snapshot, end, config, windows, recent_start, history=history),
    )


def _market_client(repo):
    from tank import clickhouse_market

    ch = clickhouse_market.client(repo, "mkt.klines_1m")
    if ch is None:
        raise RuntimeError("mkt.klines_1m is stored in ClickHouse only; TANK_CLICKHOUSE_URL is not set")
    return ch


ASSESSMENT_BASELINE_IDLE_S = 1800


class MarketTruthService:
    def __init__(self, repo, settings, market):
        self.repo, self.settings, self.market = repo, settings, market
        self._condition_cache = {}
        self._condition_baselines = {}

        self._assessment_locks = {}

    def _release_idle_assessment_baselines(self):
        """Drop the baselines of tokens with no assessment for ASSESSMENT_BASELINE_IDLE_S.

        Their entries are keyed by exact history heads that move every minute, so after half
        an hour without an assessment they serve at most a repeat of an old cutoff; kept, they
        would hold the token's 90-day indexes for the lifetime of the assessment worker."""
        compute_workers.release_idle_baselines(compute_workers.ASSESSMENT, ASSESSMENT_BASELINE_IDLE_S)

    async def condition_values(self, event_id, token_id, cutoff, requirements):
        self._release_idle_assessment_baselines()
        event = await self.repo.get("evt.events", event_id) if event_id else None
        anchor = event_anchor(event, at=_cutoff_value(cutoff)) if event else None
        snapshot = await self.market.condition_snapshot(
            token_id, cutoff, anchor=anchor, requirements=requirements
        )
        snapshot = dict(snapshot, event_id=event_id, token_id=token_id)
        snapshot.setdefault("anchor", anchor)
        if any(r["metric"] in COMPOSITE_CONDITION_METRICS | RELATIVE_CONDITION_METRICS for r in requirements):
            beneficiaries = (
                await self.repo.list(
                    "evt.beneficiary_edges", filters={"event_id": event_id, "token_id": token_id}, limit=10000
                )
                if event_id and hasattr(self.repo, "list")
                else []
            )
            if beneficiaries:
                edge = max(
                    beneficiaries, key=lambda b: (int(b.get("map_version", 0)), str(b.get("edge_id", "")))
                )
                snapshot["expected_direction"] = "LOSER" if edge.get("role") == "LOSER" else "BULLISH"
        config = (
            self.settings.load_config("thresholds")
            if callable(getattr(self.settings, "load_config", None))
            else {}
        )
        config = dict(
            config, config_version=self.settings.config_version, code_version=self.settings.code_version
        )
        wanted = {r["metric"] for r in requirements}
        if wanted & ({"price.close_vs_level"} | COMPOSITE_CONDITION_METRICS | RELATIVE_CONDITION_METRICS):
            snapshot = await with_relative(
                self.repo,
                snapshot,
                config,
                include_outcome=False,
                baselines=bool(wanted & (COMPOSITE_CONDITION_METRICS | RELATIVE_CONDITION_METRICS)),
            )
        if wanted & COMPOSITE_CONDITION_METRICS:
            snapshot = await with_volumes(self.repo, snapshot, config, primary_only=True)
        windows, history = condition_oi_windows(snapshot, requirements)
        if windows:
            snapshot = await with_oi(self.repo, snapshot, config, windows=windows, history=history)
        return await asyncio.to_thread(
            compute_condition_values,
            snapshot,
            requirements,
            config=config,
            cache=self._condition_cache,
            baseline_cache=self._condition_baselines,
        )

    async def assess(self, event_id, token_id, cutoff=None):
        event = await self.repo.get("evt.events", event_id) if event_id else None
        event = event or {}
        cutoff = cutoff or iso(parse_time(iso()).replace(second=0, microsecond=0))
        anchor = None
        if event:
            anchor = event_anchor(event, at=_cutoff_value(cutoff))
        snapshot = await self.market.snapshot(token_id, cutoff, anchor=anchor)
        snapshot.update(event_id=event_id, token_id=token_id)
        if event.get("public_at") is not None:
            snapshot.update(propagate_timestamp(event, "public_at").fields("event_public_at"))
        beneficiaries = (
            await self.repo.list(
                "evt.beneficiary_edges", filters={"event_id": event_id, "token_id": token_id}, limit=10000
            )
            if event_id and hasattr(self.repo, "list")
            else []
        )
        if beneficiaries:
            edge = max(beneficiaries, key=lambda b: (int(b.get("map_version", 0)), str(b.get("edge_id", ""))))
            snapshot["expected_direction"] = "LOSER" if edge.get("role") == "LOSER" else "BULLISH"
        knobs = (
            self.settings.load_config("thresholds")
            if callable(getattr(self.settings, "load_config", None))
            else {}
        )
        knobs = dict(
            knobs, config_version=self.settings.config_version, code_version=self.settings.code_version
        )
        self._release_idle_assessment_baselines()
        lock = self._assessment_locks.setdefault(token_id, asyncio.Lock())
        async with lock:
            snapshot = await with_oi(
                self.repo,
                await with_volumes(self.repo, await with_relative(self.repo, snapshot, knobs), knobs),
                knobs,
            )
            result = await compute_workers.compute_market_truth(
                compute_workers.ASSESSMENT, snapshot, config=knobs, baseline_key=token_id
            )
        result["mta_id"] = new_id("MTA")
        await self.repo.put("ana.market_truth", result["mta_id"], result, immutable=True)
        return result
