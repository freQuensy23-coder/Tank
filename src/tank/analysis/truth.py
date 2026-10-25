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
import pickle
import threading
from array import array
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Callable, Sequence
from contextvars import ContextVar
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, getcontext, localcontext
from functools import wraps
from math import log
from statistics import mean, median, stdev

from pydantic import JsonValue

from tank.analysis.execution import ExecutionLevelWire, ExecutionSide, execute_book_walk, execution_request
from tank.analysis.positioning import (
    MissingOIDecomposition,
)
from tank.analysis.positioning import (
    decomposition as decomposition,
)
from tank.analysis.positioning import (
    oi_class as oi_class,
)
from tank.analysis.truth_contracts import MarketTruthAssessmentRef
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.market.capture import event_anchor
from tank.time_precision import Timestamp, chosen_boundary, offset_schedule, propagate_timestamp
from tank.util import iso, jsonable, new_id, parse_time, utcnow

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


def precise[**P, R](function: Callable[P, R]) -> Callable[P, R]:
    @wraps(function)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
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


class StatisticalPopulation:
    """Compact immutable float statistics with one exact ordering, never monetary storage."""

    def __init__(self, values, ordered=None):
        self.values = array("d", values)
        self.ordered = array("d", sorted(self.values) if ordered is None else ordered)
        self._negative = None

    def __len__(self):
        return len(self.values)

    def __iter__(self):
        return iter(self.values)

    def __getitem__(self, index):
        return self.values[index]

    def scaled(self, sign):
        if sign == 1:
            return self
        if self._negative is None:
            self._negative = StatisticalPopulation(
                (-v for v in self.values), (-v for v in reversed(self.ordered))
            )
        return self._negative


def remember(cache, key, build):
    if cache is None:
        return build()
    if key not in cache:
        cache[key] = build()
        if len(cache) > 128:
            cache.pop(next(iter(cache)))
    return cache[key]


def percentile(values, p):
    s = (
        values.ordered
        if isinstance(values, StatisticalPopulation)
        else sorted(v for v in values if v is not None)
    )
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


def float_normal(values):
    """normal() for measures built with float arithmetic (beta-adjusted returns): a zero-width
    range there ([x, x] up to 1e-12) comes from identical history and float noise alone could
    exceed it (BTC-adjusted returns of BTC are ~1e-16 around 0), so it is unavailable
    (DEGENERATE_HISTORY). Exact Decimal measures (OI, funding, volume) keep normal()."""
    r = normal(values)
    if r is None or None in r:
        return r
    lo, hi = float(r[0]), float(r[1])
    return None if abs(hi - lo) <= 1e-12 * max(1.0, abs(lo), abs(hi)) else r


def range_reason(values):
    """Why float_normal(values) is unavailable: INCOMPLETE (no history) or DEGENERATE_HISTORY."""
    if not values:
        return "INCOMPLETE"
    return "DEGENERATE_HISTORY" if float_normal(values) is None else None


def beyond(v, r):
    return None if v is None or not r or None in r else v < r[0] or v > r[1]


def within(v, r):
    b = beyond(v, r)
    return None if b is None else not b


def beta(token, benchmark):
    pairs = [(benchmark[t], token[t]) for t in token.keys() & benchmark.keys()]
    if len(pairs) < int(parameter("history_min_days", 7)) * 24:
        return 1.0, "INSUFFICIENT_HISTORY"
    x, y = zip(*pairs, strict=False)
    xm, ym = mean(x), mean(y)
    den = sum((v - xm) ** 2 for v in x)
    weight = float(parameter("beta_blume_weight", 2 / 3))
    return (
        (weight * sum((a - xm) * (b - ym) for a, b in pairs) / den + (1 - weight), None)
        if den
        else (1.0, "NOT_ESTIMABLE")
    )


def market_context(r, rv, returns, vols):
    rr, vr = normal(returns), normal(vols)
    if not rr or not vr or r is None or rv is None:
        return None
    return "STRESS" if r < rr[0] or rv > vr[1] else "HEADWIND" if r < percentile(returns, 0.25) else "NEUTRAL"


def repricing(value, event_history, day_history):
    er, dr = float_normal(event_history), float_normal(day_history)
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
    return "EARLY" if value > percentile(event_history, 0.75) else "UNPRICED"


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


def flow_reference(snapshot, iid, boundary):
    points = list(snapshot.get("reference_points", []))
    for bar in snapshot.get("flow_bars", []):
        if bar.get("instrument_id") != iid:
            continue
        points.extend(dict(point, instrument_id=iid) for point in (bar.get("reference_points") or []))
        if bar.get("complete") is True and _time(bar) + timedelta(minutes=1) == boundary:
            points.append(
                dict(
                    instrument_id=iid, boundary=iso(boundary), complete=True, last_trade=bar.get("last_trade")
                )
            )
    eligible = [
        p
        for p in points
        if p.get("instrument_id") == iid
        and p.get("boundary")
        and parse_time(p["boundary"]) == boundary
        and p.get("complete") is True
        and _available(p, snapshot)
        and (
            snapshot.get("assembled_at") is None
            or p.get("observed_at") is None
            or parse_time(p["observed_at"]) <= parse_time(snapshot["assembled_at"])
        )
    ]
    if not eligible:
        return None
    trades = [
        p["last_trade"]
        for p in eligible
        if p.get("last_trade")
        and _time(p["last_trade"]) is not None
        and _time(p["last_trade"]) < boundary
        and _available(p["last_trade"], snapshot)
        and decimal(p["last_trade"].get("price")) is not None
    ]
    return dict(max(trades, key=_time), instrument_id=iid) if trades else None


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


def history_fingerprint(items):
    """Exact in-process cache key for history rows (datetime/Decimal tuples).

    fingerprint() serializes through iso()/jsonable/json and cost ~3 s per assessment on
    90 days of prod history; pickling the same values is equality-preserving and C-fast.
    Keys never leave the process.
    """
    return hashlib.blake2b(pickle.dumps(items, protocol=5), digest_size=32).hexdigest()


def fingerprint(value):
    return hashlib.sha256(
        json.dumps(
            jsonable(value),
            sort_keys=True,
            separators=(",", ":"),
            default=lambda v: v.tolist() if isinstance(v, array) else str(v),
        ).encode()
    ).hexdigest()


def oi_temporally_admissible(row):
    """An explicitly unresolved provider bucket edge proves no endpoint window.

    Missing legacy edge fields retain their existing contract. Canonical nullable
    bucket edges are resolved by the shared timestamp helper, never guessed here.
    Unit/sidedness synchronization flags do not govern supplemental venue math.
    """
    raw = row.get("ts")
    if "ts_bucket_anchor" not in row and not (isinstance(raw, dict) and "bucket_anchor" in raw):
        return True

    inputs = _ASSESSMENT_INPUTS.get()
    memo = inputs.setdefault("oi_admissible", {}) if inputs is not None else None
    identity = id(row)
    if memo is not None and identity in memo:
        return memo[identity][1]
    value = (
        propagate_timestamp(row, "ts", precision_aliases=("timestamp_precision", "precision")).bucket_anchor
        != "UNSPECIFIED"
    )
    if memo is not None:
        memo[identity] = (row, value)
    return value


@precise
def _oi_rows(snapshot, instrument_id):
    """The instrument's OI rows; a row without oi_usd gets oi_native x mark (03 §5.4 table:
    Binance oi_native x mark, Bybit singleOpenInterest x markPrice) from its own mark or the
    nearest ticker mark of the instrument within the sync tolerance, marked with
    oi_usd_source. A derived USD value serves only the window endpoints a decomposition uses;
    normal-range history uses stored oi_usd only (_stored_usd). Input rows are not modified;
    the derived list is built once per assessment and shared by all windows."""
    rows = _instrument_rows(snapshot, "oi", instrument_id)
    inputs = _ASSESSMENT_INPUTS.get()
    memo = inputs.setdefault("oi_rows", {}) if inputs is not None else None
    if memo is not None and memo.get(id(rows), (None,))[0] is rows:
        return memo[id(rows)][1]
    tolerance = float(parameter("oi_sync_tolerance_s", 60))
    marks = sorted(
        (_time(t), decimal(t["mark_price"]))
        for t in _instrument_rows(snapshot, "tickers", instrument_id)
        if t.get("mark_price") is not None and _time(t) is not None and decimal(t["mark_price"])
    )
    mark_times = [t for t, _ in marks]
    out = []
    for row in rows:
        native = decimal(row.get("oi_native"))
        if row.get("oi_usd") is not None or native is None:
            out.append(row)
            continue
        mark, source = decimal(row.get("mark_price")), "OI_NATIVE_X_ROW_MARK"
        if not mark:
            t = _time(row)
            i = bisect_left(mark_times, t)
            near = [
                (abs((mark_times[j] - t).total_seconds()), j)
                for j in (i - 1, i)
                if 0 <= j < len(marks) and abs((mark_times[j] - t).total_seconds()) <= tolerance
            ]
            mark, source = (marks[min(near)[1]][1], "OI_NATIVE_X_TICKER_MARK") if near else (None, None)
        if not mark:
            out.append(row)
            continue
        exact_context(native, mark)
        out.append(dict(row, oi_usd=native * mark, oi_usd_reason=None, oi_usd_source=source))
    if memo is not None:
        memo[id(rows)] = (rows, out)
    return out


def _stored_usd(row):
    """oi_usd as stored by capture; None for a value derived from a mark at assessment time."""
    return None if row.get("oi_usd_source") else decimal(row.get("oi_usd"))


def _oi_split(rows):
    """(admissible rows, inadmissible rows, admissible (time, index) order) of one row list,
    built once per assessment for a list shared across windows."""
    inputs = _ASSESSMENT_INPUTS.get()
    memo = inputs.setdefault("oi_split", {}) if inputs is not None else None
    hit = memo.get(id(rows)) if memo is not None else None
    if hit is None or hit[0] is not rows:
        admissible = [row for row in rows if oi_temporally_admissible(row)]
        excluded = [row for row in rows if not oi_temporally_admissible(row)]
        ordered = sorted((_time(r), i) for i, r in enumerate(admissible))
        hit = (rows, admissible, excluded, ordered, [t for t, _ in ordered])
        if memo is not None:
            memo[id(rows)] = hit
    return hit[1:]


def _oi_stats(rows, start, end, *, history_identity=None, venue_history=None):
    rows, _, ordered, ordered_times = _oi_split(rows)
    tolerance = float(parameter("oi_sync_tolerance_s", 60))

    def nearest(t):

        lo = bisect_left(ordered_times, t - timedelta(seconds=tolerance))
        hi = bisect_right(ordered_times, min(t + timedelta(seconds=tolerance), end))
        best = None
        for when, index in ordered[lo:hi]:
            if abs((when - t).total_seconds()) > tolerance:
                continue
            rank = (abs((when - t).total_seconds()), index)
            if best is None or rank < best:
                best = rank
        return rows[best[1]] if best else None

    a, z = nearest(start), nearest(end)
    if not a or not z:
        return None
    n0, n1, u0, u1 = (
        decimal(r.get(k)) for r, k in [(a, "oi_native"), (z, "oi_native"), (a, "oi_usd"), (z, "oi_usd")]
    )
    if n0 is None or n1 is None or n0 <= 0 or n1 < 0:
        return None
    exact_context(n0, n1, u0, u1)
    p0 = u0 / n0 if u0 is not None else None
    p1 = u1 / n1 if u1 is not None and n1 else decimal(z.get("mark_price", z.get("mark")))
    usd_ready = u0 is not None and u1 is not None and u0 > 0
    dec = decomposition(n0, n1, p0, p1) if p0 is not None and p1 is not None else MissingOIDecomposition()
    length = _time(z) - _time(a)

    window = end - start
    bytime = {_time(r): r for r in rows if start - timedelta(days=lookback_days()) <= _time(r) < start}
    if venue_history is not None:
        venue_history = [_history_slice(times, venue_rows, start) for times, venue_rows in venue_history]
    history_times = sorted(bytime)
    gaps = sorted(
        (b - a).total_seconds() for a, b in zip(history_times, history_times[1:], strict=False) if b > a
    )
    pair_tolerance = max(float(parameter("oi_sync_tolerance_s", 60)), gaps[len(gaps) // 2] / 2 if gaps else 0)
    cache = _BASELINE_CACHE.get()
    dependency = _BASELINE_DEPENDENCY.get()
    history_key = (
        (
            "oi" if venue_history is None else "oi_composite",
            window.total_seconds(),
            pair_tolerance,
            parameter("history_min_days", 7),
            tuple(parameter("normal_range_quantiles", [0.025, 0.975])),
            (dependency, history_identity or tuple(sorted({r.get("instrument_id", "") for r in rows})))
            if dependency is not None
            else history_fingerprint(
                [(t, r.get("oi_native"), _stored_usd(r)) for t, r in sorted(bytime.items())]
                if venue_history is None
                else [
                    [(t, r.get("oi_native"), _stored_usd(r)) for t, r in zip(*v, strict=False)]
                    for v in venue_history
                ]
            ),
        )
        if cache is not None
        else None
    )
    ranges = cache.get(history_key) if cache is not None else None
    if ranges is None:
        hn = []
        hu = []
        if venue_history is not None:
            hn, hu = _composite_history(venue_history, window)
        elif (
            bytime
            and (max(bytime) - min(bytime)).total_seconds() >= float(parameter("history_min_days", 7)) * 86400
        ):
            native_ends, usd_ends = [], []
            for t, r in bytime.items():
                target = t - window
                i = bisect_left(history_times, target)
                near = [
                    (abs((history_times[j] - target).total_seconds()), j)
                    for j in (i - 1, i)
                    if 0 <= j < len(history_times)
                    and abs((history_times[j] - target).total_seconds()) <= pair_tolerance
                ]
                old = bytime[history_times[min(near)[1]]] if near else None
                if old:
                    nn, oo = decimal(r.get("oi_native")), decimal(old.get("oi_native"))
                    uu, ou = _stored_usd(r), _stored_usd(old)
                    if nn is not None and oo:
                        hn.append(float(nn / oo - 1))
                        native_ends.append(t)
                    if uu is not None and ou:
                        hu.append(float(uu / ou - 1))
                        usd_ends.append(t)

            minimum = float(parameter("history_min_days", 7)) * 86400
            if not usd_ends or (max(usd_ends) - min(usd_ends) + window).total_seconds() < minimum:
                hu = []
            if not native_ends or (max(native_ends) - min(native_ends) + window).total_seconds() < minimum:
                hn = []
        ranges = (normal(hn), normal(hu))
        if cache is not None:
            cache[history_key] = ranges
            if len(cache) > 128:
                cache.pop(next(iter(cache)))
    dn, du = float(n1 / n0 - 1), float(u1 / u0 - 1) if usd_ready else None
    nr, ur = ranges
    return dict(
        requested_window_duration_s=(end - start).total_seconds(),
        observed_window_duration_s=length.total_seconds(),
        native_oi_vs_normal=("ABOVE" if dn > nr[1] else "BELOW" if dn < nr[0] else "WITHIN") if nr else None,
        precision=propagate_timestamp(
            z, "ts", precision_aliases=("timestamp_precision", "precision")
        ).precision,
        oi_native_start=n0,
        oi_native_end=n1,
        oi_native_chg_pct=dn,
        oi_usd_start=u0,
        oi_usd_end=u1,
        oi_usd_chg_pct=du,
        oi_usd_reason=None if usd_ready else "INCOMPLETE",
        oi_usd_detail=None if usd_ready else "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE",
        decomposition=dec.model_dump(mode="python"),
        oi_class=oi_class(dn, du, dec, nr, ur),
        classification_ready=oi_class(dn, du, dec, nr, ur) is not None,
        native_normal_range=nr,
        usd_normal_range=ur,
        native_beyond=beyond(dn, nr),
        usd_beyond=beyond(du, ur),
        start_observed_at=iso(_time(a)),
        end_observed_at=iso(_time(z)),
    )


@precise
def window_usd_volume(snapshot, instrument, start, end):
    """Observed USD volume, with complete-print flow fallback; never candle-price estimates."""
    index = snapshot.get("_kline_volume_index")
    if index is not None:
        times, rows = index.get(instrument, ((), ()))
        lo, hi = bisect_left(times, start), bisect_left(times, end)
        points = zip(times[lo:hi], rows[lo:hi], strict=False)
    else:
        points = sorted(
            [
                (_time(r), r)
                for r in snapshot.get("klines", [])
                if r.get("instrument_id") == instrument and start <= _time(r) < end
            ],
            key=lambda point: point[0],
        )
    cursor = start
    values = []
    for at, row in points:
        z = at + timedelta(seconds=int(row.get("interval_s", 60)))
        if at != cursor or z > end or row.get("complete") is not True or row.get("quote_volume_usd") is None:
            break
        values.append(decimal(row["quote_volume_usd"]))
        cursor = z
    if cursor == end and values:
        return sum(values, Decimal(0))
    rows = flow_window(snapshot, instrument, start, end)
    if rows is None:
        return None
    buy, sell = _sum(rows, "buy_notional"), _sum(rows, "sell_notional")
    return buy + sell if buy is not None and sell is not None else None


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


def _history_slice(times, rows, start):
    lo = bisect_left(times, start - timedelta(days=lookback_days()))
    hi = bisect_left(times, start)
    return times[lo:hi], rows[lo:hi]


def _composite_history(venue_history, window):
    """Past same-length changes of a synchronized composite (03 §0, §5.4).

    venue_history holds each cohort venue's (times, rows) before the window. Each past window
    sums, per measure, the venues observed at both of its bounds, the rule the event window
    follows: native OI where the native value was observed, USD OI where the USD value was
    (Bybit base-tier rows carry no mark before ticker capture). Demanding every current venue
    at every past point cut the history to the youngest venue's: OKX OI capture began on
    18 Oct 2026 against Binance and Bybit base tiers from July-August, so the three-venue BTC
    composite stayed under the 7 days §0 needs. A venue joining or leaving the cohort is never
    counted as an OI change.
    """
    series = []
    for times, rows in venue_history:
        gaps = sorted((b - a).total_seconds() for a, b in zip(times, times[1:], strict=False) if b > a)

        tolerance = max(float(parameter("oi_sync_tolerance_s", 60)), gaps[len(gaps) // 2] / 2 if gaps else 0)
        values = [(decimal(r.get("oi_native")), _stored_usd(r)) for r in rows]
        series.append((times, values, tolerance))
    history = sorted({t for times, _, _ in series for t in times})

    minimum = float(parameter("history_min_days", 7)) * 86400
    if not history or (history[-1] - history[0]).total_seconds() < minimum:
        return [], []

    def nearest(times, values, tolerance, t):
        i = bisect_left(times, t)
        near = [
            (abs((times[j] - t).total_seconds()), j)
            for j in (i - 1, i)
            if 0 <= j < len(times) and abs((times[j] - t).total_seconds()) <= tolerance
        ]
        return values[min(near)[1]] if near else None

    hn, hu, native_ends, usd_ends = [], [], [], []
    for t in history:
        n0 = n1 = u0 = u1 = Decimal(0)
        native = usd = 0
        for times, values, tolerance in series:
            old, new = nearest(times, values, tolerance, t - window), nearest(times, values, tolerance, t)
            if old is None or new is None or old is new:
                continue
            if new[0] is not None and old[0]:
                native += 1
                n0, n1 = n0 + old[0], n1 + new[0]
            if new[1] is not None and old[1]:
                usd += 1
                u0, u1 = u0 + old[1], u1 + new[1]
        if native:
            native_ends.append(t)
            hn.append(float(n1 / n0 - 1))
        if usd:
            usd_ends.append(t)
            hu.append(float(u1 / u0 - 1))

    def stored(ends):

        return bool(ends) and (ends[-1] - ends[0] + window).total_seconds() >= minimum

    return (hn if stored(native_ends) else []), (hu if stored(usd_ends) else [])


def synchronized_oi(sync, start, end):
    admissible = []
    tolerance = float(parameter("oi_sync_tolerance_s", 60))
    for p, rows, category in sync:
        if _venue(p) == "hyperliquid":
            continue
        eligible = [r for r in rows if oi_temporally_admissible(r) and _time(r) <= end]
        if all(
            any(abs((_time(row) - endpoint).total_seconds()) <= tolerance for row in eligible)
            for endpoint in (start, end)
        ):
            admissible.append((p, eligible, category))
    sync = admissible
    synchronized = None
    if sync and len({s[2] for s in sync}) == 1 and sync[0][2] is not None:
        timestamps = sorted({_time(r) for _, rs, _ in sync for r in rs})
        combined = []
        indexed = []
        for _, rs, _ in sync:
            ordered = sorted(rs, key=_time)
            indexed.append(([_time(r) for r in ordered], ordered))
        for t in timestamps:
            selected = []
            for times, rs in indexed:
                idx = bisect_left(times, t)
                candidates = [
                    rs[i]
                    for i in (idx - 1, idx)
                    if 0 <= i < len(rs)
                    and abs((times[i] - t).total_seconds()) <= float(parameter("oi_sync_tolerance_s", 60))
                    and times[i] <= end
                ]
                if not candidates:
                    break
                selected.append(min(candidates, key=lambda r: abs((_time(r) - t).total_seconds())))
            if len(selected) == len(sync) and all(decimal(r.get("oi_native")) is not None for r in selected):
                combined.append(
                    dict(ts=iso(t), oi_native=_sum(selected, "oi_native"), oi_usd=_sum(selected, "oi_usd"))
                )
        synchronized = _oi_stats(
            combined,
            start,
            end,
            history_identity=tuple(sorted(p[0]["instrument_id"] for p in sync)),
            venue_history=indexed,
        )
        if synchronized:
            synchronized["venues"] = [p["venue"] for p, _, _ in sync]
    return synchronized


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
        groups[iid] = _oi_rows(snapshot, iid)
    sem = {(s.get("instrument_id"), s["metric_key"]): s for s in snapshot.get("semantics", [])}
    per = []
    sync = []
    for instrument, rs in groups.items():
        stats = _oi_stats(rs, start, end)
        unresolved = [r for r in _oi_split(rs)[1] if _time(r) <= end]
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
            latest_unresolved = max(unresolved, key=_time)
            p["oi_temporal_exclusions"] = {
                "count": len(unresolved),
                "reason": "INCOMPLETE",
                "detail": "OI_BUCKET_EDGE_UNSPECIFIED",
                "latest_observation": {
                    **propagate_timestamp(
                        latest_unresolved, "ts", precision_aliases=("timestamp_precision", "precision")
                    ).fields("ts"),
                    "oi_native": decimal(latest_unresolved.get("oi_native")),
                    "oi_usd": decimal(latest_unresolved.get("oi_usd")),
                    "scope": "SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT",
                },
            }
        lastoi = max(rs, key=_time) if rs else {}
        mark = decimal(lastoi.get("mark_price", lastoi.get("mark")))
        index = decimal(lastoi.get("index_price", lastoi.get("index")))
        p["basis_bps"] = (mark / index - 1) * 10000 if mark is not None and index else None
        per.append(p)
        semantics = [
            sem.get((instrument, r.get("metric_key")), sem.get((None, r.get("metric_key")), {}))
            for r in rs
            if start <= _time(r) <= end
        ]
        if (
            venue in ("binance", "okx", "bybit")
            and semantics
            and all(s.get("sidedness") == "SINGLE_SIDED" for s in semantics)
            and stats.get("oi_native_start") is not None
        ):
            sync.append((p, rs, semantics[0].get("aggregation_class")))
    synchronized = synchronized_oi(sync, start, end)
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
    target = snapshot.get("token_id")
    for market in ("SPOT", "PERP"):
        instruments = {
            r["instrument_id"]
            for r in snapshot.get("instruments", [])
            if r.get("token_id", target) == target and ((_market(r) == "SPOT") == (market == "SPOT"))
        }
        instruments |= {
            r["instrument_id"]
            for collection in ("klines", "flow_bars")
            for r in snapshot.get(collection, [])
            if r.get("token_id", target) == target and ((_market(r) == "SPOT") == (market == "SPOT"))
        }
        current = []
        previous = []
        for iid in instruments:
            current.append(window_usd_volume(snapshot, iid, start, end))
            previous.append(window_usd_volume(snapshot, iid, start - (end - start), start))
        complete = bool(instruments) and None not in current and None not in previous
        volumes[market] = (
            max(Decimal(0), sum(current, Decimal(0)) - sum(previous, Decimal(0))) if complete else None
        )
        volume_coverage[market.lower()] = {
            "instruments": sorted(instruments),
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


def _prices(snapshot, token=None, *, exact=False, reference_only=False):
    inputs = _ASSESSMENT_INPUTS.get()
    if exact or inputs is None:
        return _price_groups(snapshot, token, exact=exact, reference_only=reference_only)
    candles = snapshot.get("klines")
    projected = snapshot.get("stat_price_series")
    key = (id(candles), id(projected), snapshot["cutoff"], token)
    prices = inputs.setdefault("prices", {})
    if key not in prices:
        prices[key] = (candles, projected, _price_groups(snapshot, token))
    return prices[key][2]


def _price_groups(snapshot, token=None, *, exact=False, reference_only=False):
    groups = defaultdict(dict)
    references = {}
    cutoff = parse_time(snapshot["cutoff"])
    for r in snapshot.get("klines", []):
        if r.get("complete") is not True or _market(r) != "SPOT":
            continue
        if token and r.get("token_id") != token and token not in r.get("instrument_id", "").upper():
            continue
        t = kline_close_time(r) or _time(r) + timedelta(seconds=int(r.get("interval_s", 60)))
        raw = r.get("c", r.get("close"))
        if exact and reference_only:
            raw = raw.get("value") if isinstance(raw, dict) else raw
            price = None if raw is None else raw if isinstance(raw, Decimal) else Decimal(str(raw))
        else:
            price = (decimal if exact else number)(raw)
        if price and t <= cutoff:
            if exact and reference_only:
                latest, hourly = references.setdefault(r["instrument_id"], [None, None])
                if latest is None or t >= latest[0]:
                    references[r["instrument_id"]][0] = (t, price)
                if t.minute == 0 and t.second == 0 and (hourly is None or t >= hourly[0]):
                    references[r["instrument_id"]][1] = (t, price)
            else:
                groups[r["instrument_id"]][t] = price
    if exact and reference_only:
        return {
            iid: {point[0]: decimal(point[1]) for point in points if point is not None}
            for iid, points in references.items()
        }
    if not exact:
        epoch = parse_time("1970-01-01T00:00:00Z")
        for iid, projected in (snapshot.get("stat_price_series") or {}).items():
            if projected.get("time_basis") != "CLOSE_TIME":
                continue
            if token and projected.get("token_id") != token:
                continue
            for micros, value in zip(
                projected.get("times_us", ()), projected.get("values", ()), strict=False
            ):
                t = epoch + timedelta(microseconds=int(micros))
                price = number(value)
                if price and price > 0 and t <= cutoff:
                    groups[iid][t] = price
    return groups


def relative_truth(snapshot, start, end, *, include_outcome=True):
    groups = _prices(snapshot)
    exact_groups = _prices(snapshot, exact=True, reference_only=True)
    token_id = snapshot.get("token_id", "")
    token_symbols = {
        r["token_id"]: r["symbol"].upper()
        for r in snapshot.get("tokens", [])
        if r.get("token_id") and r.get("symbol")
    }
    identities = {
        r["instrument_id"]: r.get("token_id")
        for r in snapshot.get("instruments", [])
        if r.get("instrument_id")
    }
    identities.update(
        {
            r["instrument_id"]: r["token_id"]
            for r in snapshot.get("klines", [])
            if r.get("instrument_id") and r.get("token_id")
        }
    )
    identities.update(
        {iid: row.get("token_id") for iid, row in (snapshot.get("stat_price_series") or {}).items()}
    )
    coverage_counts = dict(snapshot.get("series_coverage_counts") or {})
    for iid, projected in (snapshot.get("stat_price_series") or {}).items():
        coverage_counts.setdefault(iid, projected.get("coverage_count", len(groups.get(iid, {}))))
    for r in snapshot.get("instruments", []):
        if r.get("token_id") and (r.get("base_asset") or r.get("base_symbol")):
            token_symbols.setdefault(r["token_id"], str(r.get("base_asset", r.get("base_symbol"))).upper())
    token_symbols.setdefault(token_id, str(token_id).split(":")[-1].upper())
    for identity in identities.values():
        if identity:
            token_symbols.setdefault(identity, str(identity).split(":")[-1].upper())
    candidates = {iid: p for iid, p in groups.items() if identities.get(iid) == token_id}
    ref_coverage = {}
    volumes = {}
    for iid in candidates:
        instrument = next(
            (i for i in snapshot.get("instruments", []) if i.get("instrument_id") == iid),
            dict(instrument_id=iid, market_type="SPOT"),
        )
        bars = [r for r in snapshot.get("flow_bars", []) if r.get("instrument_id") == iid]
        coverage = spot_truth(
            bars,
            [],
            [instrument],
            end - timedelta(days=1),
            end,
            flow_segments=flow_segments(snapshot),
            assembled_at=snapshot.get("assembled_at"),
        )
        observed = coverage["venues"][0] if coverage["venues"] else None
        ref_coverage[iid] = dict(
            complete=observed is not None,
            expected_duration_s=86400,
            boundary_segments=observed.get("boundary_segments", []) if observed else [],
            excluded=coverage["coverage"]["excluded"],
            volume_unit="NATIVE_TOKEN",
            covered_volume_native=observed["volume_native"] if observed else None,
        )
        if observed is not None:
            volumes[iid] = observed["volume_native"]
    ref = max(volumes, key=volumes.get) if volumes else None
    prices = candidates.get(ref, {})

    def series(symbol):
        matches = {iid: p for iid, p in groups.items() if token_symbols.get(identities.get(iid)) == symbol}
        return (
            matches[max(matches, key=lambda iid: coverage_counts.get(iid, len(matches[iid])))]
            if matches
            else {}
        )

    cutoff_row = flow_reference(snapshot, ref, end)
    anchor_row = flow_reference(snapshot, ref, start)
    anchor_price = decimal(anchor_row.get("price")) if anchor_row else None
    btc = series("BTC")
    eth = series("ETH")
    history_end = start
    cache = _BASELINE_CACHE.get()
    history_head = snapshot.get("relative_history_dependency_fingerprint")
    if history_head is None and cache is not None:
        history_head = history_fingerprint(
            [(iid, [(t, v) for t, v in rows.items() if t < start]) for iid, rows in groups.items()]
        )
    elif history_head is not None and not isinstance(history_head, str):
        history_head = fingerprint(history_head)
    scope = (
        history_head,
        iso(start),
        lookback_days(),
        parameter("history_min_days", 7),
        parameter("beta_blume_weight", 2 / 3),
    )

    def series_id(p):
        return tuple(iid for iid, rows in groups.items() if rows is p)

    def hourly_values(p):
        return {
            t: log(p[t] / p[t - timedelta(hours=1)])
            for t in p
            if t.minute == 0
            and t.second == 0
            and t < history_end
            and t - timedelta(hours=1) in p
            and t >= history_end - timedelta(days=lookback_days())
        }

    def hourly(p):
        return remember(cache, ("relative_hourly", scope, series_id(p)), lambda: hourly_values(p))

    bb, bflag = remember(
        cache,
        ("relative_beta", scope, series_id(prices), series_id(btc)),
        lambda: beta(hourly(prices), hourly(btc)),
    )
    be, eflag = remember(
        cache,
        ("relative_beta", scope, series_id(prices), series_id(eth)),
        lambda: beta(hourly(prices), hourly(eth)),
    )
    price_times = {}

    def price(p, t):
        times = price_times.setdefault(id(p), sorted(p)) if id(p) not in price_times else price_times[id(p)]
        index = bisect_right(times, t) - 1
        return p[times[index]] if index >= 0 else None

    def ret(p, a, z):
        pa, pz = price(p, a), price(p, z)
        return pz / pa - 1 if pa and pz else None

    def fresh_price(p, t):
        """A benchmark/peer price at t only if its last point is within that series' own bar
        spacing: a base-tier outage must not turn two copies of one old close into a 0 return."""
        times = price_times.setdefault(id(p), sorted(p)) if id(p) not in price_times else price_times[id(p)]
        index = bisect_right(times, t) - 1
        if index < 0:
            return None
        step = (times[index] - times[index - 1]).total_seconds() if index > 0 else 60
        return p[times[index]] if (t - times[index]).total_seconds() <= max(step, 60) else None

    def bench_ret(p, a, z):
        pa, pz = fresh_price(p, a), fresh_price(p, z)
        return pz / pa - 1 if pa and pz else None

    benchmark = token_symbols.get(token_id) == "BTC"
    historical_times = sorted(t for t in prices if t < start)
    deltas = [
        (b - a).total_seconds() for a, b in zip(historical_times, historical_times[1:], strict=False) if b > a
    ]
    history_resolution_s = int(median(deltas)) if deltas else None
    history_diagnostics = {}

    def hist_values(length):
        if (
            not historical_times
            or (historical_times[-1] - historical_times[0]).total_seconds()
            < float(parameter("history_min_days", 7)) * 86400
        ):
            return []
        out = []
        durations = []
        stalenesses = []
        ends = []

        for t in historical_times:
            target = t - timedelta(seconds=length)
            i = bisect_left(historical_times, target)
            if i == len(historical_times) or historical_times[i] != target:
                i -= 1
            if i < 0:
                continue
            a = historical_times[i]
            lag = (target - a).total_seconds()
            if (
                a < start - timedelta(days=lookback_days())
                or lag >= history_resolution_s
                or not benchmark
                and (t not in btc or a not in btc)
            ):
                continue
            out.append(
                prices[t] / prices[a] - 1
                if benchmark
                else prices[t] / prices[a] - 1 - bb * (btc[t] / btc[a] - 1)
            )
            durations.append((t - a).total_seconds())
            stalenesses.append(lag)
            ends.append(t)
        history_diagnostics[length] = dict(
            target_window_duration_s=length,
            observed_duration_bounds_s=[min(durations), max(durations)] if durations else None,
            endpoint_staleness_bounds_s=[min(stalenesses), max(stalenesses)] if stalenesses else None,
            exact=bool(stalenesses) and max(stalenesses) == 0,
            resolution_s=history_resolution_s,
            alignment="AS_OF_AT_EXACT_TARGET_BOUNDS",
            sample_count=len(out),
            latest_observed_at=iso(max(ends)) if ends else None,
        )
        return out

    def hist(length):
        key = ("relative_hist", scope, ref, series_id(btc), bb, benchmark, length)

        def build():
            values = hist_values(length)
            return StatisticalPopulation(values) if cache is not None else values, deepcopy(
                history_diagnostics.get(length, {})
            )

        values, diagnostic = remember(cache, key, build)
        if diagnostic:
            history_diagnostics[length] = diagnostic
        return values

    windows = {}
    for name, length in dict(WINDOWS, EVENT=(end - start).total_seconds()).items():
        a = end - timedelta(seconds=length)
        r = (
            (
                float(
                    (
                        decimal(cutoff_row.get("price", cutoff_row.get("p")))
                        if cutoff_row
                        else price(exact_groups.get(ref, {}), end)
                    )
                    / anchor_price
                    - 1
                )
                if anchor_price and (cutoff_row or price(prices, end))
                else None
            )
            if name == "EVENT"
            else ret(prices, a, end)
        )
        rb = bench_ret(btc, a, end)
        re = bench_ret(eth, a, end)
        adj = r - bb * rb if r is not None and rb is not None and not benchmark else None
        measure = r if benchmark else adj
        h = hist(length)
        windows[name] = dict(
            window=name,
            start=iso(a),
            cutoff=iso(end),
            observed_at=iso(max(prices)) if prices else None,
            precision=f"BUCKET({int(next((r.get('interval_s', 60) for r in reversed(snapshot.get('klines', [])) if r.get('instrument_id') == ref), 60))}s)"
            if prices
            else None,
            coverage={"reference_venue": ref, "complete_24h": bool(ref)},
            r_raw=r,
            r_btc_adj=adj,
            r_btc_adj_reason="NOT_APPLICABLE_BENCHMARK" if benchmark else None,
            relative_measure="R_RAW" if benchmark else "R_BTC_ADJ",
            relative_return=measure,
            r_eth_adj=r - be * re if r is not None and re is not None else None,
            r_sector_adj=None,
            sector_reason="NOT_ESTIMABLE",
            pctile=ratio(sum(x <= measure for x in h), len(h)) if measure is not None and h else None,
            normal_range=float_normal(h),
            normal_range_reason=range_reason(h),
            beyond_normal=beyond(measure, float_normal(h)),
            historical_window_resolution_s=history_resolution_s,
            historical_window_duration_s=length,
            requested_window_duration_s=length,
            historical_alignment="AS_OF_AT_EXACT_TARGET_BOUNDS",
            historical_measurement=history_diagnostics.get(
                length, {"exact": False, "reason": "INCOMPLETE", "target_window_duration_s": length}
            ),
        )

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

    defi_sectors = (
        "DEX_EXECUTION",
        "LENDING_CREDIT",
        "PERP_DEX",
        "RWA",
        "INTEROPERABILITY",
        "SYNTHETIC_DOLLAR",
    )

    def peer_series(identity):
        matches = {iid: p for iid, p in groups.items() if identities.get(iid) == identity}
        return (
            matches[max(matches, key=lambda iid: coverage_counts.get(iid, len(matches[iid])))]
            if matches
            else {}
        )

    peers = [identity for identity in sector_members.get(sector, []) if identity != token_id]
    tradable = [identity for identity in peers if peer_series(identity)]
    if len(tradable) < 2:
        tradable = [
            identity
            for name in defi_sectors
            for identity in sector_members.get(name, [])
            if identity != token_id and peer_series(identity)
        ]
    not_applicable = sector == "IDENTITY_PAYMENTS"
    for name, length in dict(WINDOWS, EVENT=(end - start).total_seconds()).items():
        peer_returns = [
            bench_ret(peer_series(identity), end - timedelta(seconds=length), end) for identity in tradable
        ]
        peer_returns = [v for v in peer_returns if v is not None]
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
    eh = hist((end - start).total_seconds())
    dh = hist(86400)
    sign = -1 if snapshot.get("expected_direction") in ("BEARISH", "LOSER", -1) else 1
    eventr = windows["EVENT"]["relative_return"]
    br = bench_ret(btc, end - timedelta(days=1), end)
    bh = []
    rvhist = []
    rv = None
    btc_hourly = {t: p for t, p in btc.items() if t.minute == 0 and t.second == 0 and t.microsecond == 0}

    def btc_baseline():
        bh = []
        rvhist = []
        for t in sorted(btc_hourly):
            if t >= start or t < start - timedelta(days=lookback_days()):
                continue
            a = t - timedelta(days=1)
            if a in btc:
                bh.append(btc[t] / btc[a] - 1)
            lr = [
                log(btc_hourly[x] / btc_hourly[x - timedelta(hours=1)])
                for x in btc_hourly
                if a < x <= t and x - timedelta(hours=1) in btc_hourly
            ]
            if len(lr) == 24:
                rvhist.append(stdev(lr))
        return StatisticalPopulation(bh) if cache is not None else bh, StatisticalPopulation(
            rvhist
        ) if cache is not None else rvhist

    bh, rvhist = remember(cache, ("btc_context_history", scope, series_id(btc)), btc_baseline)
    lr = [
        log(btc_hourly[t] / btc_hourly[t - timedelta(hours=1)])
        for t in btc_hourly
        if end.replace(minute=0, second=0, microsecond=0) - timedelta(days=1)
        < t
        <= end.replace(minute=0, second=0, microsecond=0)
        and t - timedelta(hours=1) in btc_hourly
    ]
    if len(lr) == 24:
        rv = stdev(lr)
    if not btc or (max(btc) - min(btc)).total_seconds() < float(parameter("history_min_days", 7)) * 86400:
        bh = []
        rvhist = []
    context = market_context(br, rv, bh, rvhist)
    pre = ret(prices, start - timedelta(days=1), start)
    preb = bench_ret(btc, start - timedelta(days=1), start)
    ar = pre if benchmark else pre - bb * preb if pre is not None and preb is not None else None
    horizon_ranges = {}
    for name, length in (
        [
            ("W1H", 3600),
            ("W4H", 14400),
            ("W12H", 43200),
            ("W24H", 86400),
            ("W3D", 259200),
            ("W7D", 604800),
        ]
        if include_outcome
        else []
    ):
        history = hist(length)
        diagnostic = history_diagnostics.get(length, {})
        horizon_ranges[name] = dict(
            value=float_normal(history),
            unit="DECIMAL_RETURN",
            window=name,
            start=iso(start - timedelta(days=lookback_days())),
            cutoff=iso(start),
            observed_at=diagnostic.get("latest_observed_at"),
            precision=f"BUCKET({history_resolution_s}s)" if history_resolution_s else None,
            coverage={"sample_count": len(history)},
            reason=None if history else "NOT_ESTIMABLE",
            historical_measurement=diagnostic,
        )
    btc_ids = {iid: p for iid, p in groups.items() if token_symbols.get(identities.get(iid)) == "BTC"}
    btc_iid = max(btc_ids, key=lambda iid: coverage_counts.get(iid, len(btc_ids[iid]))) if btc_ids else None
    btc_last = flow_reference(snapshot, btc_iid, end)
    btc_entry = decimal(btc_last.get("price")) if btc_last else price(exact_groups.get(btc_iid, {}), end)
    outcome_btc = dict(
        instrument_id=btc_iid,
        entry_ref_price=btc_entry,
        observed_at=iso(_time(btc_last)) if btc_last else iso(max(btc)) if btc else None,
        price_source="LAST_TRADE" if btc_last else "KLINE_CLOSE" if btc else None,
        reason=None if btc_entry is not None else "INCOMPLETE",
    )
    hourly_closes = [
        (t, p) for t, p in exact_groups.get(ref, {}).items() if t <= end and t.minute == 0 and t.second == 0
    ]
    return dict(
        expected_direction=snapshot.get("expected_direction"),
        repricing_sign=sign,
        outcome_btc_reference=outcome_btc,
        normal_ranges=horizon_ranges,
        entry_ref_price=decimal(cutoff_row.get("price", cutoff_row.get("p")))
        if cutoff_row
        else price(exact_groups.get(ref, {}), end),
        entry_price_source="LAST_TRADE" if cutoff_row else "KLINE_CLOSE",
        entry_price_observed_at=iso(_time(cutoff_row))
        if cutoff_row
        else iso(max(prices))
        if prices
        else None,
        close_1h_price=max(hourly_closes, key=lambda x: x[0])[1] if hourly_closes else None,
        ref_venue=ref,
        reference_coverage=ref_coverage,
        reference_reason=None if ref else "INCOMPLETE",
        anchor=dict(
            type=snapshot.get("anchor", {}).get("type", "KNOWN_TO_US_AT")
            if isinstance(snapshot.get("anchor"), dict)
            else "KNOWN_TO_US_AT",
            ts=iso(start),
            ref_price=anchor_price,
            price_observed_at=iso(_time(anchor_row)) if anchor_row else None,
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
            pctile=ratio(sum(x <= ar for x in dh), len(dh)) if ar is not None else None,
            flag=beyond(ar, float_normal(dh)),
        ),
        repricing_band=repricing(
            eventr * sign if eventr is not None else None,
            eh.scaled(sign) if isinstance(eh, StatisticalPopulation) else [x * sign for x in eh],
            dh.scaled(sign) if isinstance(dh, StatisticalPopulation) else [x * sign for x in dh],
        ),
        market_context=context,
        context_inputs=dict(
            btc_return_24h=br,
            rv24=rv,
            rv24_observed_cutoff=iso(end.replace(minute=0, second=0, microsecond=0)),
            rv24_resolution_s=3600,
            return_normal_range=normal(bh),
            return_lower_quartile=percentile(bh, 0.25),
            rv_normal_range=normal(rvhist),
            rule="normal_range",
        ),
    )


def walk_book(
    levels: Sequence[ExecutionLevelWire],
    notional: JsonValue | Decimal,
    mid: JsonValue | Decimal,
    side: ExecutionSide = "ask",
) -> float | None:
    return execute_book_walk(execution_request(levels, notional, mid, side)).slippage_bps


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
    volume_buckets = defaultdict(list)
    for r in snapshot.get("flow_bars", []):
        if _market(r) == "SPOT" and end - timedelta(days=1) <= _time(r) < end:
            volume_buckets[int(_time(r).timestamp()) // 900].append(r)
    covered_volumes = []
    expected_venues = {v["venue"] for v in spot["venues"]}
    for _bucket, rs in volume_buckets.items():
        selected = [r for r in rs if _venue(r) in expected_venues]
        if (
            expected_venues
            and all(r.get("complete") is True for r in selected)
            and all(len({_time(r) for r in selected if _venue(r) == v}) == 15 for v in expected_venues)
        ):
            buy, sell = _sum(selected, "buy_notional"), _sum(selected, "sell_notional")
            if buy is not None and sell is not None:
                covered_volumes.append(buy + sell)
    impact_tiers = parameter("impact_tiers_usd", [100000, 1000000, 5000000])
    k = min(Decimal(1), mean(covered_volumes) / decimal(impact_tiers[2])) if covered_volumes else None
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

    token = relative["ref_venue"]
    prices = _prices(snapshot).get(token, {})
    hourly = {t: p for t, p in prices.items() if t.minute == 0 and t.second == 0}
    hourly_returns = {
        t: log(p / hourly[t - timedelta(hours=1)])
        for t, p in hourly.items()
        if t - timedelta(hours=1) in hourly
    }
    return_times = sorted(hourly_returns)
    rvs = []
    current = None
    for t in sorted(hourly):
        left = bisect_right(return_times, t - timedelta(hours=24))
        right = bisect_right(return_times, t)
        lr = [hourly_returns[x] for x in return_times[left:right]]
        if len(lr) == 24:
            if start - timedelta(days=lookback_days()) <= t < start:
                rvs.append(stdev(lr))
            if t <= end:
                current = stdev(lr)
    if (
        hourly
        and (max(hourly) - min(hourly)).total_seconds() < float(parameter("history_min_days", 7)) * 86400
    ):
        rvs = []
    rr = normal(rvs)
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


def benchmark_reaction_values(snapshot, start, end):
    """Reaction checkpoint values for a BTC/ETH benchmark outside the analysis set (03 §8.1).

    Lead ruling 2026-10-20: the raw return comes from base-tier 1m kline closes of the
    benchmark's largest-volume spot venue (trailing 24 h before the window end); spot CVD,
    OI and funding are n/a NOT_APPLICABLE. No escalation, so no trades are needed.
    """
    token = snapshot.get("token_id")
    rows = [
        r
        for r in snapshot.get("klines", [])
        if _market(r) == "SPOT"
        and r.get("complete") is True
        and r.get("token_id", token) == token
        and number(r.get("c", r.get("close")))
    ]

    def close_time(r):
        return kline_close_time(r) or _time(r) + timedelta(seconds=int(r.get("interval_s", 60)))

    volume = defaultdict(Decimal)
    for r in rows:
        if end - timedelta(hours=24) < close_time(r) <= end:
            v = decimal(r.get("quote_volume_usd"))
            volume[r["instrument_id"]] += v if v is not None else Decimal(0)
    venue = max(volume, key=volume.get) if volume and max(volume.values()) > 0 else None

    def last_close(bound):
        eligible = [r for r in rows if r["instrument_id"] == venue and close_time(r) <= bound]
        latest = max(eligible, key=close_time) if eligible else None

        if latest is not None and (bound - close_time(latest)).total_seconds() > int(
            latest.get("interval_s", 60)
        ):
            return None
        return latest

    first, last = (last_close(start), last_close(end)) if venue else (None, None)
    p0 = decimal((first or {}).get("c", (first or {}).get("close")))
    p1 = decimal((last or {}).get("c", (last or {}).get("close")))
    r_raw = float(p1 / p0 - 1) if p0 and p1 else None
    return {
        "r_raw": r_raw,
        "r_raw_reason": None if r_raw is not None else "INCOMPLETE",
        "r_raw_method": "BASE_KLINE_CLOSE_LARGEST_SPOT_VENUE",
        "r_raw_venue": venue,
        "price_start": p0,
        "price_start_observed_at": iso(close_time(first)) if first else None,
        "price_end": p1,
        "price_end_observed_at": iso(close_time(last)) if last else None,
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
    token = snapshot.get("token_id")
    features = {}
    for market in ("SPOT", "PERP"):
        grouped = defaultdict(list)
        for row in snapshot.get("klines", []):
            if row.get("token_id", token) == token and ((_market(row) == "SPOT") == (market == "SPOT")):
                grouped[row["instrument_id"]].append(row)
        market_indexes = {}
        projected = {
            iid: row
            for iid, row in (snapshot.get("stat_volume_series") or {}).items()
            if row.get("token_id") == token
            and row.get("market_type") == market
            and row.get("time_basis") == "OPEN_TIME"
        }
        for iid in set(grouped) | set(projected):
            projection = projected.get(iid)

            def build_index():
                index_cache = _BASELINE_CACHE.get()
                previous_head = projection.get("previous_dependency_fingerprint") if projection else None
                prior = (
                    index_cache.get(("native-volume-index", iid, previous_head))
                    if index_cache is not None and previous_head
                    else None
                )
                append = bool(
                    projection
                    and projection.get("append_only") is True
                    and prior is not None
                    and projection.get("changed_from_us") is not None
                    and projection["changed_from_us"] > prior["last_open_us"]
                )
                bounds = dict(prior["bounds"]) if append else {}
                running = prior["running"] if append else Decimal(0)
                segment = prior["segment"] if append else 0
                last = prior["last"] if append else None
                last_open_us = prior["last_open_us"] if append else -(2**63)
                if projection is not None:
                    epoch = parse_time("1970-01-01T00:00:00Z")
                    times = projection.get("times_us", ())
                    begin = bisect_left(times, projection["changed_from_us"]) if append else 0
                    source = (
                        (
                            epoch + timedelta(microseconds=int(times[j])),
                            int(projection.get("interval_s", 60)),
                            projection["values"][j],
                            bool(projection["complete"][j]),
                            int(times[j]),
                        )
                        for j in range(begin, len(times))
                    )
                else:
                    source = (
                        (
                            _time(row),
                            int(row.get("interval_s", 60)),
                            row.get("volume_native"),
                            row.get("complete") is True,
                            None,
                        )
                        for row in sorted(grouped[iid], key=_time)
                    )
                for a, interval, raw, complete, micros in source:
                    z = a + timedelta(seconds=interval)
                    if projection is None and z > end:
                        continue
                    if micros is not None:
                        last_open_us = micros
                    value = decimal(raw)
                    if not complete or value is None:
                        segment += 1
                        last = None
                        continue
                    if last != a:
                        segment += 1
                    bounds[a] = (running, segment)
                    running += value
                    bounds[z] = (running, segment)
                    last = z
                return dict(
                    bounds=bounds, running=running, segment=segment, last=last, last_open_us=last_open_us
                )

            head = projection.get("dependency_fingerprint") if projection is not None else None
            index = (
                remember(_BASELINE_CACHE.get(), ("native-volume-index", iid, head), build_index)
                if head is not None and _BASELINE_CACHE.get() is not None
                else build_index()
            )
            market_indexes[iid] = index["bounds"]

        def total(a, z):
            values = []
            for bounds in market_indexes.values():
                if a not in bounds or z not in bounds or bounds[a][1] != bounds[z][1]:
                    return None
                values.append(bounds[z][0] - bounds[a][0])
            return sum(values, Decimal(0)) if values else None

        current, previous = total(start, end), total(start - length, start)
        history = []
        history_windows = []
        endpoints = set.intersection(*(set(v) for v in market_indexes.values())) if market_indexes else set()
        for t in endpoints:
            if start - timedelta(days=lookback_days()) <= t - length < t < start:
                value = total(t - length, t)
                if value is not None:
                    history.append(value)
                    history_windows.append((t - length, t))

        if (
            not history_windows
            or (max(z for _, z in history_windows) - min(a for a, _ in history_windows)).total_seconds()
            < float(parameter("history_min_days", 7)) * 86400
        ):
            history = []
        nr = normal(history)
        up = (
            bool(current > previous and current > nr[1])
            if current is not None and previous is not None and nr
            else None
        )
        features[market.lower() + "_volume_up_beyond_normal"] = up
        features[market.lower() + "_volume_within_normal"] = within(current, nr)
        features[market.lower() + "_volume"] = {
            "value": current,
            "previous": previous,
            "normal_range": nr,
            "unit": "NATIVE_TOKEN",
            "window": snapshot.get("_primary"),
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
        p["perp_volume_usd"] = window_usd_volume(snapshot, iid, start, end)
        p["perp_volume_reason"] = None if p["perp_volume_usd"] is not None else "INCOMPLETE"
        oi = [r for r in _instrument_rows(snapshot, "oi", iid) if _time(r) <= end]
        historical = []
        initial = []
        tolerance = float(parameter("oi_sync_tolerance_s", 60))
        for row in oi:
            mark, index = decimal(row.get("mark_price")), decimal(row.get("index_price"))
            if mark is None or not index:
                continue
            basis = (mark / index - 1) * 10000
            if _time(row) < start:
                historical.append((_time(row), basis))
            if abs((_time(row) - start).total_seconds()) <= tolerance:
                initial.append((_time(row), basis))
        br = (
            normal([v for _, v in historical])
            if historical
            and (max(t for t, _ in historical) - min(t for t, _ in historical)).total_seconds()
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
        initial_value = (
            min(initial, key=lambda x: abs((x[0] - start).total_seconds()))[1] if initial else None
        )
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


def decisive_facts(spot, derivatives, liquidity, relative, supply, move_class, window, start, end):
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
        f"{window} spot flow {direction or 'unknown'}: CVD {cvd:g}, {spot['classification']}; {_count(coverage['n_complete'], 'complete venue')}."
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

    indexed = defaultdict(list)
    for row in snapshot.get("klines", []):
        indexed[row.get("instrument_id")].append((_time(row), row))
    snapshot["_kline_volume_index"] = {}
    for iid, points in indexed.items():
        points.sort(key=lambda point: point[0])
        snapshot["_kline_volume_index"][iid] = (
            [point[0] for point in points],
            [point[1] for point in points],
        )
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
            d = derivatives_truth(dict(scoped, oi=[], funding=[], liquidations=[], klines=[]), wstart, end)
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
        spots[primary], deriv, liquidity, relative, supply, selected["move_class"], primary, pstart, end
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
    price_groups = _prices(snapshot, exact=True)
    candidates = {}
    for instrument in snapshot.get("instruments", []):
        iid = instrument.get("instrument_id")
        if _market(instrument) != "SPOT" or instrument.get("token_id") != snapshot.get("token_id"):
            continue
        spot = spot_truth(
            [r for r in snapshot.get("flow_bars", []) if r.get("instrument_id") == iid],
            [],
            [instrument],
            end - timedelta(days=1),
            end,
            flow_segments=flow_segments(snapshot),
            assembled_at=snapshot.get("assembled_at"),
        )
        if spot["venues"]:
            candidates[iid] = spot["venues"][0]["volume_native"]
    ref = max((iid for iid in price_groups if iid in candidates), key=candidates.get, default=None)
    values = price_groups.get(ref, {})
    closed = [(t, v) for t, v in values.items() if t <= end and t.minute == t.second == t.microsecond == 0]
    selected = max(closed, key=lambda row: row[0]) if closed else None
    expected = end.replace(minute=0, second=0, microsecond=0)
    if selected and selected[0] != expected:
        selected = None
    source = next(
        (
            row
            for row in reversed(snapshot.get("klines", []))
            if row.get("instrument_id") == ref
            and row.get("complete") is True
            and selected
            and (kline_close_time(row) or _time(row) + timedelta(seconds=int(row.get("interval_s", 60))))
            == selected[0]
        ),
        None,
    )
    observed = propagate_timestamp({}, "observed_at")
    if source:
        if source.get("close_time") and source.get("open_time") is None:
            observed = propagate_timestamp(source, "close_time")
        else:
            observed = offset_schedule(
                propagate_timestamp(source, "open_time", precision_aliases=("timestamp_precision",)),
                timedelta(seconds=int(source.get("interval_s", 60))),
            )
    return dict(
        ref_venue=ref,
        close_1h_price=selected[1] if selected else None,
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
    tolerance = timedelta(seconds=float(parameter("oi_sync_tolerance_s", 60)))
    endpoints = dict(
        snapshot,
        oi=[
            r
            for r in snapshot.get("oi", [])
            if abs((_time(r) - start).total_seconds()) <= tolerance.total_seconds()
            or end - tolerance <= _time(r) <= end
        ],
    )
    groups = defaultdict(list)
    for row in endpoints["oi"]:
        groups[row["instrument_id"]].append(row)
    semantics = {(row.get("instrument_id"), row["metric_key"]): row for row in snapshot.get("semantics", [])}
    sync = []
    for iid, rows in groups.items():
        stats = _oi_stats(rows, start, end)
        definitions = [
            semantics.get((iid, row.get("metric_key")), semantics.get((None, row.get("metric_key")), {}))
            for row in rows
            if start <= _time(row) <= end
        ]
        venue = _venue(rows[0])
        if (
            venue in ("binance", "okx", "bybit")
            and definitions
            and all(row.get("sidedness") == "SINGLE_SIDED" for row in definitions)
            and stats
            and stats.get("oi_native_start") is not None
        ):
            sync.append((dict(venue=venue, instrument_id=iid), rows, definitions[0].get("aggregation_class")))
    derivatives = {"synchronized": synchronized_oi(sync, start, end)}
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
            "stat_price_series",
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
        ("oi", "funding", "liquidations", "semantics", "instruments", "klines"),
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
                fields = (domain, "instruments", "semantics", "capture_metadata")
            elif metric == "price.close_vs_level":
                domain = "close"
                fields = ("flow_bars", "flow_segments", "klines", "instruments")
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
                    "funding",
                    "klines",
                    "books",
                    "liquidations",
                    "supply",
                    "anchor",
                    "expected_direction",
                    "funding_confirmations",
                    "stat_price_series",
                    "stat_volume_series",
                    "series_coverage_counts",
                    "relative_history_dependency_fingerprint",
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
                    "stat_price_series",
                    "series_coverage_counts",
                    "relative_history_dependency_fingerprint",
                )
            else:
                pending.append(dict(metric=metric, window=window, reason="NOT_SUPPORTED"))
                continue
            dependency = (snapshot.get("domain_dependency_fingerprints") or {}).get(domain)
            if dependency is None:
                dependency = fingerprint({field: snapshot.get(field) for field in fields})
            elif not isinstance(dependency, str):
                dependency = fingerprint(dependency)
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


class MarketTruthService:
    def __init__(self, repo, settings, market, *, versions: ResolvedVersionIdentity):
        self.repo, self.settings, self.market = repo, settings, market
        self.versions = versions
        self._condition_cache = {}
        self._condition_baselines = {}

        self._assessment_baselines = {}
        self._assessment_locks = {}

    async def condition_values(self, event_id, token_id, cutoff, requirements):
        event = await self.repo.get("evt.events", event_id) if event_id else None
        anchor = event_anchor(event).to_wire() if event else None
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
            config, config_version=self.versions.config_version, code_version=self.versions.code_version
        )
        return await asyncio.to_thread(
            compute_condition_values,
            snapshot,
            requirements,
            config=config,
            cache=self._condition_cache,
            baseline_cache=self._condition_baselines,
        )

    async def assess_for_job(
        self, event_id: str, token_id: str, cutoff: Timestamp | None = None
    ) -> MarketTruthAssessmentRef:
        result = await self.assess(event_id, token_id, cutoff)
        return MarketTruthAssessmentRef.from_record(result)

    async def assess(self, event_id, token_id, cutoff=None):
        event = await self.repo.get("evt.events", event_id) if event_id else None
        event = event or {}
        cutoff = iso(utcnow().replace(second=0, microsecond=0)) if cutoff is None else cutoff
        anchor = None
        if event:
            anchor = event_anchor(event).to_wire()
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
            knobs, config_version=self.versions.config_version, code_version=self.versions.code_version
        )
        lock = self._assessment_locks.setdefault(token_id, asyncio.Lock())
        async with lock:
            result = await asyncio.to_thread(
                compute_market_truth,
                snapshot,
                config=knobs,
                baseline_cache=self._assessment_baselines.setdefault(token_id, {}),
            )
        result["mta_id"] = new_id("MTA")
        await self.repo.put("ana.market_truth", result["mta_id"], result, immutable=True)
        return result
