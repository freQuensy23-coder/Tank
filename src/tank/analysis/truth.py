"""Deterministic market evidence at one semantic market cutoff.

Exchange/settlement timestamps and completed-bar bucket ends bound quantitative
windows. Receipt time is collection latency, not a second market cutoff: archive
capture may occur later. When supplied, assembled_at bounds which received rows
were available to construct this snapshot. Missing evidence never votes true.
"""

from __future__ import annotations

import asyncio
from bisect import bisect_left
from collections import defaultdict
from contextvars import ContextVar
from datetime import timedelta
from decimal import Decimal, getcontext, localcontext
from functools import wraps
from math import log
from statistics import mean, median, stdev

from tank.market.capture import event_anchor
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

_PARAMETERS = ContextVar("tank_truth_parameters", default={})


def parameter(name, default):
    return _PARAMETERS.get().get(name, default)


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


def beta(token, benchmark):
    pairs = [(benchmark[t], token[t]) for t in token.keys() & benchmark.keys()]
    if len(pairs) < int(parameter("history_min_days", 7)) * 24:
        return 1.0, "INSUFFICIENT_HISTORY"
    x, y = zip(*pairs)
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
    er, dr = normal(event_history), normal(day_history)
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
    return not assembled or not received or parse_time(received) <= parse_time(assembled)


@precise
def spot_truth(
    rows, tickers, instruments, start, end, *, boundary_trades=None, boundary_coverage=None, assembled_at=None
):
    boundary_trades = boundary_trades or []
    boundary_coverage = boundary_coverage or []
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
            selected_ids = ids[venue]
            proofs = []
            for iid in selected_ids:
                candidates = []
                for proof in boundary_coverage:
                    if proof.get("instrument_id") != iid or proof.get("complete") is not True:
                        continue
                    pstart = proof.get("start", proof.get("minute_start"))
                    pend = proof.get("end", proof.get("observed_through"))
                    if pend is None and proof.get("minute_start"):
                        pend = iso(parse_time(proof["minute_start"]) + timedelta(minutes=1))
                    observed = proof.get("observed_at", proof.get("received_ts"))
                    if (
                        pstart
                        and pend
                        and parse_time(pstart) <= segment_start
                        and parse_time(pend) >= segment_end
                        and observed is not None
                        and (assembled_at is None or parse_time(observed) <= parse_time(assembled_at))
                    ):
                        candidates.append(proof)
                if not candidates:
                    boundary_failed = True
                    break
                proofs.extend(candidates)
            if boundary_failed:
                break
            trades = [
                r
                for r in boundary_trades
                if r.get("instrument_id") in selected_ids
                and _time(r) is not None
                and segment_start <= _time(r) < segment_end
                and _available(r, {"assembled_at": assembled_at})
            ]
            unique = {(r.get("instrument_id"), r.get("trade_id")): r for r in trades}
            if any(
                r.get("trade_id") is None
                or r.get("side") not in ("BUY", "SELL")
                or decimal(r.get("qty_native")) is None
                for r in unique.values()
            ):
                boundary_failed = True
                break
            trades = list(unique.values())
            buys = [decimal(r["qty_native"]) for r in trades if r["side"] == "BUY"]
            sells = [decimal(r["qty_native"]) for r in trades if r["side"] == "SELL"]
            rs.append(
                dict(
                    buy_qty=sum(buys),
                    sell_qty=sum(sells),
                    sum_sq_qty=sum(decimal(r["qty_native"]) ** 2 for r in trades),
                    complete=True,
                )
            )
            segments.append(
                dict(
                    start=iso(segment_start),
                    cutoff=iso(segment_end),
                    trade_count=len(trades),
                    coverage_proof=proofs,
                )
            )
        if len(minutes) < expected or not all(r.get("complete") is True for r in rs) or boundary_failed:
            excluded.append(
                dict(
                    venue=venue,
                    reason="INCOMPLETE",
                    detail="BOUNDARY_TRADES_OR_CONTINUITY_PROOF_UNAVAILABLE"
                    if boundary_failed
                    else "FULL_MINUTE_COVERAGE_INCOMPLETE",
                )
            )
            continue
        b, s, q = (_sum(rs, k) for k in ("buy_qty", "sell_qty", "sum_sq_qty"))
        if None in (b, s, q):
            excluded.append(dict(venue=venue, reason="INCOMPLETE"))
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


@precise
def _oi_stats(rows, start, end):
    def nearest(t):
        eligible = [
            r
            for r in rows
            if _time(r) <= end
            and abs((_time(r) - t).total_seconds()) <= float(parameter("oi_sync_tolerance_s", 60))
        ]
        return min(eligible, key=lambda r: abs((_time(r) - t).total_seconds())) if eligible else None

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
    length = _time(z) - _time(a)
    bytime = {_time(r): r for r in rows if start - timedelta(days=lookback_days()) <= _time(r) < start}
    hn = []
    hu = []
    if (
        bytime
        and (max(bytime) - min(bytime)).total_seconds() >= float(parameter("history_min_days", 7)) * 86400
    ):
        for t, r in bytime.items():
            old = bytime.get(t - length)
            if old:
                nn, oo = decimal(r.get("oi_native")), decimal(old.get("oi_native"))
                uu, ou = decimal(r.get("oi_usd")), decimal(old.get("oi_usd"))
                if nn is not None and oo:
                    hn.append(float(nn / oo - 1))
                if uu is not None and ou:
                    hu.append(float(uu / ou - 1))
    dn, du = float(n1 / n0 - 1), float(u1 / u0 - 1) if usd_ready else None
    nr, ur = normal(hn), normal(hu)
    return dict(
        requested_window_duration_s=(end - start).total_seconds(),
        observed_window_duration_s=length.total_seconds(),
        native_oi_vs_normal=("ABOVE" if dn > nr[1] else "BELOW" if dn < nr[0] else "WITHIN") if nr else None,
        precision=z.get("timestamp_precision", z.get("precision")),
        oi_native_start=n0,
        oi_native_end=n1,
        oi_native_chg_pct=dn,
        oi_usd_start=u0,
        oi_usd_end=u1,
        oi_usd_chg_pct=du,
        oi_usd_reason=None if usd_ready else "INCOMPLETE",
        oi_usd_detail=None if usd_ready else "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE",
        decomposition=dec,
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
    rows = sorted(
        [
            r
            for r in snapshot.get("klines", [])
            if r.get("instrument_id") == instrument and start <= _time(r) < end
        ],
        key=_time,
    )
    cursor = start
    values = []
    for row in rows:
        z = _time(row) + timedelta(seconds=int(row.get("interval_s", 60)))
        if (
            _time(row) != cursor
            or z > end
            or row.get("complete") is not True
            or row.get("quote_volume_usd") is None
        ):
            break
        values.append(decimal(row["quote_volume_usd"]))
        cursor = z
    if cursor == end and values:
        return sum(values, Decimal(0))
    if any(t.second or t.microsecond for t in (start, end)):
        return None
    rows = [
        r
        for r in snapshot.get("flow_bars", [])
        if r.get("instrument_id") == instrument and start <= _time(r) < end
    ]
    if len({_time(r) for r in rows}) != int((end - start).total_seconds() / 60) or not all(
        r.get("complete") is True for r in rows
    ):
        return None
    buy, sell = _sum(rows, "buy_notional"), _sum(rows, "sell_notional")
    return buy + sell if buy is not None and sell is not None else None


@precise
def derivatives_truth(snapshot, start, end):
    groups = defaultdict(list)
    for r in snapshot.get("oi", []):
        groups[r["instrument_id"]].append(r)
    sem = {(s.get("instrument_id"), s["metric_key"]): s for s in snapshot.get("semantics", [])}
    per = []
    sync = []
    for instrument, rs in groups.items():
        stats = _oi_stats(rs, start, end)
        venue = _venue(rs[0])
        if stats is None:
            stats = {"oi_class": None, "reason": "INCOMPLETE"}
        fs = [
            f
            for f in snapshot.get("funding", [])
            if f.get("instrument_id") == instrument and _time(f) and _time(f) <= end
        ]
        settled = sorted([f for f in fs if f.get("kind") == "SETTLED"], key=_time)
        est = sorted([f for f in fs if f.get("kind") == "CURRENT_ESTIMATE"], key=_time)
        last = settled[-1] if settled else {}
        interval = decimal(last.get("interval_h"))
        rate = decimal(last.get("rate"))
        eq = rate * 8 / interval if rate is not None and interval else None
        fh = [
            decimal(f.get("rate_8h_eq"))
            if f.get("rate_8h_eq") is not None
            else decimal(f.get("rate")) * 8 / decimal(f["interval_h"])
            for f in settled
            if _time(f) < start and f.get("interval_h") and f.get("rate") is not None
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
        fr = (
            normal(fh)
            if settled
            and (_time(settled[-1]) - _time(settled[0])).total_seconds()
            >= float(parameter("history_min_days", 7)) * 86400
            else None
        )
        liqs = [
            r
            for r in snapshot.get("liquidations", [])
            if r.get("instrument_id") == instrument and start <= _time(r) < end
        ]
        cov = rs[-1].get("liq_coverage") or (
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
            funding_settled_last_observed_at=iso(_time(last)) if last else None,
            funding_estimate_observed_at=iso(_time(est[-1])) if est else None,
            funding_estimate=decimal(est[-1].get("rate")) if est else None,
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
            liq_long_usd=sum(
                (decimal(r["notional_usd"]) for r in liqs if r.get("side") == "LONG_LIQ"), Decimal(0)
            )
            if liqs or (liq_window_complete and cov in ("COMPLETE", "SAMPLED"))
            else None,
            liq_short_usd=sum(
                (decimal(r["notional_usd"]) for r in liqs if r.get("side") == "SHORT_LIQ"), Decimal(0)
            )
            if liqs or (liq_window_complete and cov in ("COMPLETE", "SAMPLED"))
            else None,
            liq_coverage=cov,
            liq_source_coverage=liq_source_coverage,
            liq_capture_window={
                "start": liq_start,
                "observed_through": liq_through,
                "complete": liq_window_complete,
                "reason": None if liq_window_complete else "INCOMPLETE",
            },
        )
        lastoi = max(rs, key=_time)
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
            and all(
                s.get("semantic_confidence") == "VERIFIED" and s.get("sidedness") == "SINGLE_SIDED"
                for s in semantics
            )
            and stats.get("oi_native_start") is not None
        ):
            sync.append((p, rs, semantics[0].get("aggregation_class")))
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
        synchronized = _oi_stats(combined, start, end)
        if synchronized:
            synchronized["venues"] = [p["venue"] for p, _, _ in sync]
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
        if None not in volumes.values()
        and sum(volumes.values())
        and all(t.second == 0 and t.microsecond == 0 for t in (start, end))
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


def _prices(snapshot, token=None, *, exact=False):
    groups = defaultdict(dict)
    for r in snapshot.get("klines", []):
        if r.get("complete") is not True or _market(r) != "SPOT":
            continue
        if token and r.get("token_id") != token and token not in r.get("instrument_id", "").upper():
            continue
        t = (
            parse_time(r["close_time"])
            if r.get("close_time")
            else _time(r) + timedelta(seconds=int(r.get("interval_s", 60)))
        )
        price = (decimal if exact else number)(r.get("c", r.get("close")))
        if price and t <= parse_time(snapshot["cutoff"]):
            groups[r["instrument_id"]][t] = price
    return groups


def relative_truth(snapshot, start, end):
    groups = _prices(snapshot)
    exact_groups = _prices(snapshot, exact=True)
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
    for r in snapshot.get("instruments", []):
        if r.get("token_id") and (r.get("base_asset") or r.get("base_symbol")):
            token_symbols.setdefault(r["token_id"], str(r.get("base_asset", r.get("base_symbol"))).upper())
    token_symbols.setdefault(token_id, str(token_id).split(":")[-1].upper())
    for identity in identities.values():
        if identity:
            token_symbols.setdefault(identity, str(identity).split(":")[-1].upper())
    token = token_symbols[token_id]
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
            boundary_trades=snapshot.get("boundary_trades"),
            boundary_coverage=snapshot.get("boundary_coverage"),
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
        return max(matches.values(), key=len) if matches else {}

    def anchor_trade(iid):
        anchors = snapshot.get("anchor_trades", [])
        trades = list(snapshot.get("trades", [])) + list(
            anchors.values() if isinstance(anchors, dict) else anchors
        )
        eligible = [
            r
            for r in trades
            if r.get("instrument_id") == iid
            and _time(r) is not None
            and _time(r) < start
            and _available(r, snapshot)
            and number(r.get("price", r.get("p"))) is not None
        ]
        return max(eligible, key=_time) if eligible else None

    cutoff_points = snapshot.get("cutoff_trades", {})
    cutoff_candidates = list(
        cutoff_points.values() if isinstance(cutoff_points, dict) else cutoff_points
    ) + list(snapshot.get("trades", []))
    cutoff_candidates = [
        r
        for r in cutoff_candidates
        if r.get("instrument_id") == ref
        and _time(r) is not None
        and _time(r) < end
        and _available(r, snapshot)
        and number(r.get("price", r.get("p"))) is not None
    ]
    cutoff_row = max(cutoff_candidates, key=_time) if cutoff_candidates else None
    anchor_row = anchor_trade(ref)
    anchor_price = decimal(anchor_row.get("price", anchor_row.get("p"))) if anchor_row else None
    btc = series("BTC")
    eth = series("ETH")
    history_end = start

    def hourly(p):
        return {
            t: log(p[t] / p[t - timedelta(hours=1)])
            for t in p
            if t.minute == 0
            and t.second == 0
            and t < history_end
            and t - timedelta(hours=1) in p
            and t >= history_end - timedelta(days=lookback_days())
        }

    bb, bflag = beta(hourly(prices), hourly(btc))
    be, eflag = beta(hourly(prices), hourly(eth))

    def price(p, t):
        ts = [x for x in p if x <= t]
        return p[max(ts)] if ts else None

    def ret(p, a, z):
        pa, pz = price(p, a), price(p, z)
        return pz / pa - 1 if pa and pz else None

    historical_times = sorted(t for t in prices if t < start)
    deltas = [(b - a).total_seconds() for a, b in zip(historical_times, historical_times[1:]) if b > a]
    history_resolution_s = int(median(deltas)) if deltas else None
    history_diagnostics = {}

    def hist(length):
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
        # Fixed target duration; lookup the last observable price at each bound, never ceil the window.
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
                or t not in btc
                or a not in btc
            ):
                continue
            out.append(prices[t] / prices[a] - 1 - bb * (btc[t] / btc[a] - 1))
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
        rb = ret(btc, a, end)
        re = ret(eth, a, end)
        adj = r - bb * rb if r is not None and rb is not None else None
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
            r_eth_adj=r - be * re if r is not None and re is not None else None,
            r_sector_adj=None,
            sector_reason="NOT_ESTIMABLE",
            pctile=ratio(sum(x <= adj for x in h), len(h)) if adj is not None else None,
            normal_range=normal(h),
            beyond_normal=beyond(adj, normal(h)),
            historical_window_resolution_s=history_resolution_s,
            historical_window_duration_s=length,
            requested_window_duration_s=length,
            historical_alignment="AS_OF_AT_EXACT_TARGET_BOUNDS",
            historical_measurement=history_diagnostics.get(
                length, {"exact": False, "reason": "INCOMPLETE", "target_window_duration_s": length}
            ),
        )
    sectors = [
        ["UNI", "CAKE", "AERO", "CRV", "1INCH", "ZRX", "COW"],
        ["AAVE", "MORPHO", "EUL"],
        ["AVAX", "NEAR", "XPL"],
        ["HYPE", "LIT"],
        ["ONDO"],
        ["ZRO", "QNT"],
        ["ENA"],
    ]
    peers = next((s for s in sectors if token in s), [])
    peers = [x for x in peers if x != token]
    tradable = [symbol for symbol in peers if series(symbol)]
    if len(tradable) < 2:
        tradable = [
            symbol
            for sector in sectors
            if sector != sectors[2]
            for symbol in sector
            if symbol != token and series(symbol)
        ]
    for name, length in dict(WINDOWS, EVENT=(end - start).total_seconds()).items():
        peer_returns = [ret(series(symbol), end - timedelta(seconds=length), end) for symbol in tradable]
        peer_returns = [v for v in peer_returns if v is not None]
        raw = windows[name]["r_raw"]
        windows[name]["r_sector_adj"] = (
            raw - mean(peer_returns)
            if token != "WLD" and raw is not None and len(peer_returns) >= 2
            else None
        )
        windows[name]["sector_reason"] = (
            "NOT_APPLICABLE"
            if token == "WLD"
            else None
            if windows[name]["r_sector_adj"] is not None
            else "NOT_ESTIMABLE"
        )
    eh = hist((end - start).total_seconds())
    dh = hist(86400)
    sign = -1 if snapshot.get("expected_direction") in ("BEARISH", "LOSER", -1) else 1
    eventr = windows["EVENT"]["r_btc_adj"]
    br = ret(btc, end - timedelta(days=1), end)
    bh = []
    rvhist = []
    rv = None
    btc_hourly = {t: p for t, p in btc.items() if t.minute == 0 and t.second == 0 and t.microsecond == 0}
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
    preb = ret(btc, start - timedelta(days=1), start)
    ar = pre - bb * preb if pre is not None and preb is not None else None
    horizon_ranges = {}
    for name, length in [
        ("W1H", 3600),
        ("W4H", 14400),
        ("W12H", 43200),
        ("W24H", 86400),
        ("W3D", 259200),
        ("W7D", 604800),
    ]:
        history = hist(length)
        diagnostic = history_diagnostics.get(length, {})
        horizon_ranges[name] = dict(
            value=normal(history),
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
    btc_iid = max(btc_ids, key=lambda iid: len(btc_ids[iid])) if btc_ids else None
    btc_points = list(cutoff_points.values() if isinstance(cutoff_points, dict) else cutoff_points) + list(
        snapshot.get("trades", [])
    )
    btc_points = [
        r
        for r in btc_points
        if r.get("instrument_id") == btc_iid
        and _time(r) is not None
        and _time(r) < end
        and _available(r, snapshot)
        and number(r.get("price", r.get("p"))) is not None
    ]
    btc_last = max(btc_points, key=_time) if btc_points else None
    btc_entry = (
        decimal(btc_last.get("price", btc_last.get("p")))
        if btc_last
        else price(exact_groups.get(btc_iid, {}), end)
    )
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
            flag=beyond(ar, normal(dh)),
        ),
        repricing_band=repricing(
            eventr * sign if eventr is not None else None, [x * sign for x in eh], [x * sign for x in dh]
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
            ss, se = parse_time(segment["start"]), parse_time(segment["cutoff"])
            instrument_ids = {p.get("instrument_id") for p in segment["coverage_proof"]}
            trades = {
                (r["instrument_id"], r.get("trade_id")): r
                for r in snapshot.get("boundary_trades", [])
                if r.get("instrument_id") in instrument_ids
                and ss <= _time(r) < se
                and _available(r, snapshot)
            }

            def amount(side):
                selected = [r for r in trades.values() if r.get("side") == side]
                return _sum(selected, "notional_usd") if selected else 0.0

            flow.append(
                dict(
                    instrument_id=next(iter(instrument_ids)),
                    venue=v["venue"],
                    minute_start=iso(ss),
                    segment_end=iso(se),
                    buy_notional=amount("BUY"),
                    sell_notional=amount("SELL"),
                    complete=True,
                )
            )
    volume_buckets = defaultdict(list)
    for r in snapshot.get("flow_bars", []):
        if _market(r) == "SPOT" and end - timedelta(days=1) <= _time(r) < end:
            volume_buckets[int(_time(r).timestamp()) // 900].append(r)
    covered_volumes = []
    expected_venues = {v["venue"] for v in spot["venues"]}
    for bucket, rs in volume_buckets.items():
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
        for name, size in list(zip(("T1", "T2", "T3"), impact_tiers))
    }
    per = []
    predepth = []
    nowdepth = []
    side_now = {"bid": [], "ask": []}
    side_pre = {"bid": [], "ask": []}
    spreads = []
    prespreads = []
    burst_results = []

    def depth(b, side):
        covered = b.get("range_covered_pct", {})
        coverage = number(covered.get(side)) if isinstance(covered, dict) else number(covered)
        return (
            decimal(
                (
                    b.get("depth", {})
                    .get(side, {})
                    .get({0.5: "p0_5", 1.0: "p1", 2.0: "p2"}[float(parameter("depth_band_pct", 1))])
                    or {}
                ).get("usd")
            )
            if coverage is not None and coverage >= float(parameter("depth_band_pct", 1))
            else None
        )

    pre_anchor = snapshot.get("_event_anchor", start)
    for venue, rs in groups.items():
        rs.sort(key=_time)
        b = rs[-1]
        bid, ask = depth(b, "bid"), depth(b, "ask")
        exact_context(bid, ask)
        d = bid + ask if bid is not None and ask is not None else None
        for side, value in [("bid", bid), ("ask", ask)]:
            if value is not None:
                side_now[side].append(value)
        pre = (
            [x for x in rs if pre_anchor - timedelta(hours=1) <= _time(x) < pre_anchor]
            if pre_anchor is not None
            else []
        )
        for side in ("bid", "ask"):
            vals = [depth(x, side) for x in pre if depth(x, side) is not None]
            if vals:
                side_pre[side].append(mean(vals))
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
                precision=b.get("exchange_ts_precision", b.get("timestamp_precision")),
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
        for f in flow:
            if _venue(f) != venue:
                continue
            t = _time(f)
            before = [
                x
                for x in rs
                if t - timedelta(seconds=float(parameter("book_interval_s", 10))) <= _time(x) < t
            ]
            if not before:
                continue
            baseline = before[-1]
            bd = depth(baseline, "bid")
            sell, buy = decimal(f.get("sell_notional")), decimal(f.get("buy_notional"))
            if sell is None or buy is None:
                continue
            net = sell - buy
            if bd is None or net < bd:
                continue
            after = [
                x
                for x in rs
                if t < _time(x) <= min(t + timedelta(seconds=float(parameter("refill_window_s", 60))), end)
                and depth(x, "bid") is not None
                and depth(x, "bid") >= bd
            ]
            recovered = bool(after)
            future = [
                decimal(x["low"])
                for x in flow
                if _venue(x) == venue
                and t < _time(x) <= t + timedelta(minutes=float(parameter("resilience_window_min", 15)))
                and x.get("low") is not None
            ]
            low = decimal(f.get("low"))
            resilient = (
                (min(future) >= low)
                if future
                and low is not None
                and t + timedelta(minutes=float(parameter("resilience_window_min", 15))) <= end
                else None
            )
            burst_results.append(
                dict(
                    venue=venue,
                    ts=iso(t),
                    replenished=recovered
                    if t + timedelta(seconds=float(parameter("refill_window_s", 60))) <= end
                    else None,
                    recovery_s=(_time(after[0]) - t).total_seconds() if after else None,
                    resilient=resilient,
                )
            )
    aggregate = sum(nowdepth) if nowdepth and len(nowdepth) == len(groups) else None
    pre = sum(predepth) if predepth and len(predepth) == len(groups) else None
    drop = float(1 - aggregate / pre) if aggregate is not None and pre else None
    bn, sn = _sum(flow, "buy_notional"), _sum(flow, "sell_notional")
    net = bn - sn if bn is not None and sn is not None else None
    raw = relative["windows"].get(snapshot.get("_primary", "W4H"), {}).get("r_raw")
    beyondprice = relative["windows"].get(snapshot.get("_primary", "W4H"), {}).get("beyond_normal")
    avg = mean(nowdepth + predepth) if nowdepth and predepth else None
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
    average_hit_depth = []
    if hit_side:
        for rs in groups.values():
            values = [
                depth(b, hit_side) for b in rs if start <= _time(b) <= end and depth(b, hit_side) is not None
            ]
            if values:
                average_hit_depth.append(mean(values))
    avg = sum(average_hit_depth) if average_hit_depth and len(average_hit_depth) == len(groups) else None
    weak = (
        (abs(net) >= hit_depth and (raw if net > 0 else -raw) < float(parameter("depth_band_pct", 1)) / 100)
        if net and hit_depth is not None and raw is not None
        else None
    )
    excess = (
        bool(beyondprice and abs(net) < pre_hit_depth)
        if beyondprice is not None and net is not None and pre_hit_depth is not None
        else None
    )
    side_drops = {
        side: float(1 - side_totals[side] / pre_totals[side])
        if side_totals[side] is not None and pre_totals[side]
        else None
        for side in ("bid", "ask")
    }
    drop = side_drops.get(hit_side) if hit_side else None
    depth_majority_lost = (
        bool(hit_depth * 2 < pre_hit_depth) if hit_depth is not None and pre_hit_depth is not None else None
    )
    vacuum = (
        bool(beyondprice and depth_majority_lost and excess)
        if drop is not None and excess is not None and beyondprice is not None
        else None
    )
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
            bps_per_depth_unit=raw * 10000 / float(abs(net) / avg)
            if raw is not None and net and avg
            else None,
            weak_response=weak,
            excess_response=excess,
            hit_side=hit_side,
            hit_side_depth_p1_usd=hit_depth,
            pre_event_hit_side_depth_p1_usd=pre_hit_depth,
            avg_hit_side_depth_p1_usd=avg,
        ),
        vol_compression=dict(rv24=None, normal_range=None, state=None, reason="INCOMPLETE"),
        liquidity_state="VACUUM"
        if vacuum
        else ("THIN" if min(side_totals.values()) < tiers["T2"] else "NORMAL")
        if None not in side_totals.values() and tiers["T2"] is not None
        else None,
    )


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
        bars = [
            r
            for r in snapshot.get("flow_bars", [])
            if _venue(r) in compatible
            and _market(r) == "SPOT"
            and start <= _time(r) < end
            and r.get("complete") is True
        ]
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
            previous = [
                r
                for r in snapshot.get("flow_bars", [])
                if _venue(r) in compatible
                and _market(r) == "SPOT"
                and start - (end - start) <= _time(r) < start
                and r.get("complete") is True
            ]
            prev_buy = prev_sell = Decimal(0)
            prev_valid = bool(previous)
            for r in previous:
                for k in large_buckets:
                    bucket = r.get("size_buckets", {}).get(k, {})
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
    # First crossing uses only complete, contiguous event bars and the token's historical 15m range.
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
    for instrument, rows in groups.items():
        rows.sort(key=_time)
        if not all(r.get("complete") is True for r in rows):
            complete = False
            continue
        if any((_time(b) - _time(a)).total_seconds() != 60 for a, b in zip(rows, rows[1:])):
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
    # Continuous as-of book states, not exact wall-clock bucket membership.
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
        boundary_trades=snapshot.get("boundary_trades"),
        boundary_coverage=snapshot.get("boundary_coverage"),
        assembled_at=snapshot.get("assembled_at"),
    )
    complete_venues = {v["venue"] for v in capture["venues"]}
    for index, a in enumerate(sorted(grouped)):
        for b in sorted(grouped)[index + 1 :]:
            ar, br = grouped[a], grouped[b]
            times = sorted({frag_start, end} | {_time(r) for r in ar + br if frag_start <= _time(r) < end})
            gaps = []
            second = False
            for t in times:
                states = []
                for rows in (ar, br):
                    before = [r for r in rows if _time(r) <= t]
                    states.append(
                        before[-1] if before and (t - _time(before[-1])).total_seconds() <= cadence else None
                    )
                if None in states:
                    gaps = []
                    break
                x, y = states
                xm, ym = decimal(x.get("mid")), decimal(y.get("mid"))
                xs, ys = decimal(x.get("spread_bps")), decimal(y.get("spread_bps"))
                if None in (xm, ym, xs, ys) or not ym:
                    gaps = []
                    break
                gap = abs(xm - ym) * 10000 > (xs + ys) * ym
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
                    rest.append((rm, rs) if good else None)
                second = second or (
                    None not in rest
                    and abs(rest[0][0] - rest[1][0]) * 10000 > (rest[0][1] + rest[1][1]) * rest[1][0]
                )
            if gaps and all(gaps):
                persistent = True
                pairs.append([a, b])
                validated = validated or (second and {a, b} <= complete_venues)
    # Funding dispersion uses settled 8h-equivalent historical observations on the same venue cohort.
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
        bars = [
            r
            for r in snapshot.get("flow_bars", [])
            if r.get("instrument_id") == iid and frag_start <= _time(r) < end
        ]
        if len({_time(r) for r in bars}) != 15 or not all(r.get("complete") is True for r in bars):
            perp_complete = False
    funding_validated = (
        funding_beyond is True and beyond(secondary_dispersion, normal_dispersion) is True and perp_complete
    )
    deriv["funding_dispersion_normal_range_8h"] = normal_dispersion
    deriv["funding_dispersion_beyond_normal"] = funding_beyond
    # The discriminator's top-venue sign-flip route does not pretend to be independent confirmation.
    top = set(spots[primary]["topn"]["venues"]) if spots[primary].get("topn") else set()
    signs = {v: [] for v in top}
    cursor = start
    while cursor + timedelta(minutes=15) <= end:
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
            boundary_trades=snapshot.get("boundary_trades"),
            boundary_coverage=snapshot.get("boundary_coverage"),
            assembled_at=snapshot.get("assembled_at"),
        )
        found = {v["venue"]: v["sign"] for v in sub["venues"]}
        for venue in top:
            signs[venue].append(found.get(venue))
        cursor += timedelta(minutes=15)
    flips = []
    for values in signs.values():
        if None in values or len(values) < 3:
            continue
        directional = [v for v in values if v != 0]
        flips.append(sum(a != b for a, b in zip(directional, directional[1:])))
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
        complete_venues=sorted(complete_venues),
        reason=None if books or dispersion is not None else "INCOMPLETE",
    )
    # Token RV distribution, using hourly samples and sample standard deviation.
    token = relative["ref_venue"]
    prices = _prices(snapshot).get(token, {})
    hourly = {t: p for t, p in prices.items() if t.minute == 0 and t.second == 0}
    rvs = []
    current = None
    for t in sorted(hourly):
        lr = [
            log(hourly[x] / hourly[x - timedelta(hours=1)])
            for x in hourly
            if t - timedelta(hours=24) < x <= t and x - timedelta(hours=1) in hourly
        ]
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
    elif current:
        unavailable["exchange_inventory"] = current.get("reason", "INCOMPLETE")
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
    chain_latest = {}
    for r in rows:
        if kind(r) in ("CHAIN_LIQUIDITY", "STABLECOIN_CHAIN"):
            chain_latest[r.get("chain")] = r
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
    return dict(
        exchange_inventory=dict(inventory, window="W24H") if inventory else missing(),
        flows=flows,
        unlocks_next_14d=unlocks,
        lending=dict(lending, window="W24H") if lending else missing(),
        stablecoin_chain=chains[0] if chains else missing(),
        chain_contexts=chains,
        unavailable=unavailable,
        supply_in_evidence="TRUE" if evidence else "FALSE" if available else "UNKNOWN",
    )


def capture_cadence(snapshot, collection, row=None):
    row = row or {}
    metadata = snapshot.get("capture_metadata") or {}
    collections = metadata.get("collections") or {}
    entry = collections.get(collection, {})
    if isinstance(entry, (int, float)):
        return float(entry), "CAPTURE_METADATA"
    kind = row.get("kind", row.get("type"))
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


def available_field_freshness(snapshot, end):
    """Latest point per source field; old history does not make a fresh series stale."""
    checks = []
    metadata = (snapshot.get("capture_metadata") or {}).get("collections", {})
    for collection in ("flow_bars", "klines", "books", "oi", "funding", "liquidations", "tickers", "supply"):
        groups = defaultdict(list)
        for r in snapshot.get(collection, []):
            if (
                r.get("available") is False
                or _time(r) is None
                or _time(r) > end
                or not _available(r, snapshot)
            ):
                continue
            key = (r.get("instrument_id"), r.get("metric_key"), r.get("kind", r.get("type")), r.get("chain"))
            groups[key].append(r)
        for key, rows in groups.items():
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
            if collection == "funding" and row.get("kind") == "SETTLED" and number(row.get("interval_h")):
                cadence = number(row["interval_h"]) * 3600
                source = "SETTLEMENT_INTERVAL"
            if collection == "liquidations":
                entry = metadata.get(collection, {}) or {}
                watermarks = entry.get("observed_through", {}) if isinstance(entry, dict) else {}
                watermark = (
                    watermarks.get(row.get("instrument_id")) if isinstance(watermarks, dict) else watermarks
                )
                if watermark is not None:
                    stamp = parse_time(watermark)
            age = (end - stamp).total_seconds()
            fresh = cadence is not None and 0 <= age <= cadence
            checks.append(
                dict(
                    collection=collection,
                    instrument_id=key[0],
                    metric_key=key[1],
                    kind=key[2],
                    chain=key[3],
                    observed_at=iso(stamp),
                    cadence_s=cadence,
                    cadence_source=source,
                    age_s=age,
                    fresh=fresh,
                    reason=None if fresh else "NOT_ESTIMABLE" if cadence is None else "INCOMPLETE",
                )
            )
    return checks


def data_confidence(snapshot, spot, derivatives, end):
    """Certify actual instrument evidence, never descriptive text or absent feeds."""
    failed = []
    low = False
    sem = snapshot.get("semantics", [])
    complete_venues = {v["venue"] for v in spot.get("venues", [])}
    instruments = {
        r["instrument_id"]: r
        for r in snapshot.get("instruments", [])
        if r.get("instrument_id") and r.get("token_id", snapshot.get("token_id")) == snapshot.get("token_id")
    }
    for collection in ("flow_bars", "books", "oi", "funding", "klines"):
        for r in snapshot.get(collection, []):
            iid = r.get("instrument_id")
            if iid and (r.get("token_id", snapshot.get("token_id")) == snapshot.get("token_id")):
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
    if complete_venues - {_venue(r) for r in instruments.values() if _market(r) == "SPOT"}:
        failed.append(
            dict(
                condition="semantics",
                reason="INCOMPLETE",
                detail="Participating spot venue lacks actual instrument evidence",
            )
        )
    if not required:
        failed.append(
            dict(condition="semantics", reason="INCOMPLETE", detail="No instrument-scoped critical evidence")
        )
    for iid, venue, market, family, collection in required:
        candidates = [
            s
            for s in sem
            if s.get("metric_family") == family
            and s.get("venue") == venue
            and s.get("market_type") == market
            and (not s.get("instrument_id") or s["instrument_id"] == iid)
        ]
        if any(s.get("semantic_confidence") in ("UNKNOWN", "INFERRED") for s in candidates):
            low = True
        if not candidates or not all(s.get("semantic_confidence") == "VERIFIED" for s in candidates):
            failed.append(
                dict(
                    condition="semantics",
                    reason="UNVERIFIED_SEMANTICS",
                    instrument_id=iid,
                    metric_family=family,
                )
            )
    verified = bool(required) and not any(f["condition"] == "semantics" for f in failed)
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
    class_ready = not perp_ids or bool(
        derivatives.get("synchronized") and derivatives["synchronized"].get("classification_ready")
    )
    for iid, venue, market, family, collection in required:
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
            valid.append(stamp)
        cadence, cadence_source = capture_cadence(
            snapshot, collection, rows[-1] if rows else {"instrument_id": iid}
        )
        if not valid or cadence is None or (end - max(valid)).total_seconds() > cadence:
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
        critical_semantics_verified=verified,
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
        for iid, rows in grouped.items():
            bounds = {}
            running = Decimal(0)
            segment = 0
            last = None
            for row in sorted(rows, key=_time):
                a = _time(row)
                z = a + timedelta(seconds=int(row.get("interval_s", 60)))
                value = decimal(row.get("volume_native"))
                if row.get("complete") is not True or value is None:
                    segment += 1
                    last = None
                    continue
                if last != a:
                    segment += 1
                bounds[a] = (running, segment)
                running += value
                bounds[z] = (running, segment)
                last = z
            market_indexes[iid] = bounds

        def total(a, z):
            values = []
            for bounds in market_indexes.values():
                if a not in bounds or z not in bounds or bounds[a][1] != bounds[z][1]:
                    return None
                values.append(bounds[z][0] - bounds[a][0])
            return sum(values, Decimal(0)) if values else None

        current, previous = total(start, end), total(start - length, start)
        history = []
        endpoints = set.intersection(*(set(v) for v in market_indexes.values())) if market_indexes else set()
        for t in endpoints:
            if start - timedelta(days=lookback_days()) <= t - length < t < start:
                value = total(t - length, t)
                if value is not None:
                    history.append(value)
        if (
            not endpoints
            or (max(endpoints) - min(endpoints)).total_seconds()
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
        rows = [
            r
            for r in snapshot.get("flow_bars", [])
            if r.get("instrument_id") == iid and start <= _time(r) and _time(r) + timedelta(minutes=1) <= end
        ]
        verified = any(
            s.get("metric_family") == "aggressor"
            and s.get("semantic_confidence") == "VERIFIED"
            and s.get("venue") == p["venue"]
            and s.get("market_type") != "SPOT"
            and (not s.get("instrument_id") or s["instrument_id"] == iid)
            for s in snapshot.get("semantics", [])
        )
        complete = (
            verified
            and start.second == end.second == 0
            and not start.microsecond
            and not end.microsecond
            and len({_time(r) for r in rows}) == int(length.total_seconds() / 60)
            and all(r.get("complete") is True for r in rows)
        )
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
        oi = [r for r in snapshot.get("oi", []) if r.get("instrument_id") == iid and _time(r) <= end]
        historical = []
        initial = []
        for row in oi:
            mark, index = decimal(row.get("mark_price")), decimal(row.get("index_price"))
            if mark is None or not index:
                continue
            basis = (mark / index - 1) * 10000
            if _time(row) < start:
                historical.append((_time(row), basis))
            if abs((_time(row) - start).total_seconds()) <= float(parameter("oi_sync_tolerance_s", 60)):
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
    for venue, rows in books.items():
        rows.sort(key=_time)

        def ask(b):
            return decimal((b.get("depth", {}).get("ask", {}).get("p1") or {}).get("usd"))

        before = [
            r
            for r in rows
            if start - timedelta(seconds=float(parameter("book_interval_s", 10))) <= _time(r) <= start
        ]
        after = [r for r in rows if _time(r) <= end]
        if before and after and ask(before[-1]) is not None and ask(after[-1]) is not None:
            builds.append(ask(after[-1]) > ask(before[-1]))
        for flow in snapshot.get("flow_bars", []):
            if (
                _venue(flow) != venue
                or _market(flow) != "SPOT"
                or not start <= _time(flow) < end
                or flow.get("complete") is not True
            ):
                continue
            buy, sell = decimal(flow.get("buy_notional")), decimal(flow.get("sell_notional"))
            t = _time(flow)
            pre = [
                r
                for r in rows
                if t - timedelta(seconds=float(parameter("book_interval_s", 10))) <= _time(r) < t
            ]
            if buy is None or sell is None or not pre or ask(pre[-1]) is None or buy - sell < ask(pre[-1]):
                continue
            if t + timedelta(seconds=float(parameter("refill_window_s", 60))) > end:
                continue
            testable = True
            recovered = recovered or any(
                t < _time(r) <= t + timedelta(seconds=float(parameter("refill_window_s", 60)))
                and ask(r) is not None
                and ask(r) >= ask(pre[-1])
                for r in rows
            )
    if builds:
        features["ask_depth_building"] = any(builds)
    if testable:
        features["ask_replenishment"] = recovered
    return features


def select_move(spot, deriv, liq, supply, relative, confidence, window):
    p = relative["windows"][window]
    r = p["r_btc_adj"]
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
            if oi.get("oi_class") == "PRICE_MARKUP" and oi.get("oi_usd_chg_pct", 0) > 0
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
        f"{window} spot flow {direction or 'unknown'}: CVD {cvd:g}, {spot['classification']}; {coverage['n_complete']} complete venues."
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
    add(
        "derivatives.oi_native_chg_pct",
        oi,
        "DECIMAL_RETURN",
        f"{window} synchronized native OI {oi:+.2%}, {normal_state or 'normal range unavailable'}; class {sync.get('oi_class') or 'unavailable'}."
        if oi is not None
        else "",
        "mkt.oi_obs",
        oi_instruments,
        dict(venues=sync.get("venues", []), classification_ready=sync.get("classification_ready")),
    )
    r = relative["windows"][window].get("r_btc_adj")
    ref = relative.get("ref_venue")
    add(
        "relative.r_btc_adj",
        r,
        "DECIMAL_RETURN",
        f"{window} BTC-adjusted price move {r:+.2%}; normal range {relative['windows'][window].get('normal_range')}."
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
                text=f"Supply in evidence: inventory change {inventory.get('chg_pct')}; labeled exchange transfers {len(classified)}.",
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
        label = f"{coverage['n_complete']} complete spot venues"
        if share is not None:
            label += f", {share:.1%} estimated volume coverage"
    elif "venues" in coverage:
        label = f"{len(coverage['venues'])} covered venues"
    elif "hit_side" in coverage:
        label = f"{coverage.get('hit_side') or 'unknown side'} depth {coverage.get('depth_usd')} USD"
    elif "complete_balances" in coverage:
        label = f"{coverage['complete_balances']} complete balances, {coverage.get('comparable_balances', 0)} comparable"
    elif coverage.get("validated") is True:
        label = "independent public REST confirmation"
    elif coverage and all(isinstance(v, dict) and "complete" in v for v in coverage.values()):
        label = (
            f"{sum(v['complete'] is True for v in coverage.values())} complete trailing-24h reference venues"
        )
    elif "classified_flows" in coverage:
        label = f"{coverage['classified_flows']} classified flows"
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
                            if key in ("funding_estimate", "funding_estimate_8h_eq")
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
                        "UNVERIFIED_SEMANTICS",
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
    # Semantic market time bounds values; collection availability is bounded by assembly time.
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
    snapshot["boundary_trades"] = [
        r
        for r in snapshot.get("boundary_trades", [])
        if _time(r) is not None and _time(r) < end and _available(r, snapshot)
    ]
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
            boundary_trades=snapshot.get("boundary_trades"),
            boundary_coverage=snapshot.get("boundary_coverage"),
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
            boundary_trades=snapshot.get("boundary_trades"),
            boundary_coverage=snapshot.get("boundary_coverage"),
            assembled_at=snapshot.get("assembled_at"),
        )
        pv = [v for v in prev["venues"] if v["venue"] in cohort]
        if valid and len(pv) == len(cohort):
            c["velocity"] = ratio(c["volume"], sum(v["volume_native"] for v in pv))
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
        primary_window=primary,
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
    result["measurement_metadata"] = measurement_metadata(result)
    return jsonable(result)


def compute_market_truth(snapshot, *, config=None):
    config = config or {}
    if config.get("market_context_rule", "normal_range") != "normal_range":
        raise ValueError("Only the approved §5.7 normal_range market context rule is supported")
    token = _PARAMETERS.set(config)
    try:
        return _compute_market_truth(snapshot, config=config)
    finally:
        _PARAMETERS.reset(token)


class MarketTruthService:
    def __init__(self, repo, settings, market):
        self.repo, self.settings, self.market = repo, settings, market

    async def assess(self, event_id, token_id, cutoff=None):
        event = await self.repo.get("evt.events", event_id) if event_id else None
        event = event or {}
        cutoff = cutoff or iso(parse_time(iso()).replace(second=0, microsecond=0))
        anchor = None
        if event:
            anchor = event_anchor(event)
        snapshot = await self.market.snapshot(token_id, cutoff, anchor=anchor)
        snapshot.update(event_id=event_id, token_id=token_id)
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
        result = await asyncio.to_thread(compute_market_truth, snapshot, config=knobs)
        result["mta_id"] = new_id("MTA")
        await self.repo.put("ana.market_truth", result["mta_id"], result, immutable=True)
        return result
