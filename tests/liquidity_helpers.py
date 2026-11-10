"""Records of the order-book statements (analysis/sql/book_liquidity.sql, book_tiers.sql and book_fragmentation.sql)
for tests of the rules that read them, made from the books and bars a test writes down.

A test that describes a market with `books` (spot order-book summaries) and `flow_bars` gets the records the
statements return for it: per venue and window the latest book, the means of the pre-event hour and of the window,
the sell bursts and the ask-side flags, per venue the slippage of the impact tiers, and per venue pair whether the
prices differ by more than the spreads. The statements themselves are tested against a real ClickHouse server in
tests/test_truth_liquidity_sql.py."""

import json
from datetime import timedelta

from flow_helpers import flow_evidence

from tank.analysis import truth, truth_sql
from tank.util import iso, parse_time

BANDS = {0.5: "p0_5", 1.0: "p1", 2.0: "p2"}
MINUTE = timedelta(minutes=1)


def _number(value):
    if isinstance(value, dict):
        value = value.get("value")
    return None if value is None else float(value)


def _when(row):
    return truth._time(row)


def _venue(row):
    return row.get("venue") or str(row["instrument_id"]).split(":")[0]


def _market(row):
    return row.get("market_type") or ("SPOT" if ":spot:" in row.get("instrument_id", "") else "PERP_LINEAR")


def _usable(row, assembled):
    for key in ("received_ts", "recorded_at"):
        stamp = row.get(key)
        if assembled and stamp and parse_time(stamp) > assembled:
            return False
    return True


def depth(book, side, band, unit="usd"):
    covered = book.get("range_covered_pct") or {}
    coverage = _number(covered.get(side)) if isinstance(covered, dict) else _number(covered)
    if coverage is None or coverage < band:
        return None
    return _number((((book.get("depth") or {}).get(side) or {}).get(BANDS[band]) or {}).get(unit))


def ask_any(book):
    return _number((((book.get("depth") or {}).get("ask") or {}).get("p1") or {}).get("usd"))


def _levels(book):
    levels = book.get("asks") or (book.get("levels") or {}).get("asks") or []
    return [
        (_number(x["price"]), _number(x["qty"])) if isinstance(x, dict) else (_number(x[0]), _number(x[1]))
        for x in levels
    ]


def walk(levels, notional, mid):
    """The slippage in bps of taking `notional` (quote currency) from the best level up, None where the levels do
    not fill it (what book_tiers.sql computes)."""
    if not notional or notional <= 0 or not mid or not levels:
        return None
    remaining, native, spent = notional, 0.0, 0.0
    for price, quantity in levels:
        take = min(remaining, price * quantity)
        native += take / price
        spent += take
        remaining -= take
        if remaining <= 1e-8:
            break
    return ((spent / native / mid) - 1) * 10000 if native and remaining <= 1e-8 else None


def _books(snapshot, assembled):
    token = snapshot.get("token_id")
    groups = {}
    for row in snapshot.get("books", []):
        if (
            _market(row) == "SPOT"
            and row.get("token_id", token) == token
            and row.get("integrity_ok") is True
            and _usable(row, assembled)
        ):
            groups.setdefault(_venue(row), []).append(row)
    for rows in groups.values():
        rows.sort(key=_when)
    return groups


def _bars(snapshot, assembled):
    token = snapshot.get("token_id")
    groups = {}
    for row in snapshot.get("flow_bars", []):
        if (
            _market(row) == "SPOT"
            and row.get("token_id", token) == token
            and row.get("complete") is True
            and _usable(row, assembled)
        ):
            groups.setdefault(_venue(row), []).append(row)
    for rows in groups.values():
        rows.sort(key=lambda r: parse_time(r["minute_start"]))
    return groups


def _mean(values):
    return sum(values) / len(values) if values else None


def _bar_value(bar, name):
    return _number(bar.get(name, bar.get(name.replace("_notional", "_usd"))))


def venue_record(window, venue, books, bars, begin, end, anchor, since, get):
    band = float(get("depth_band_pct", 1))
    interval = timedelta(seconds=float(get("book_interval_s", 10)))
    refill = timedelta(seconds=float(get("refill_window_s", 60)))
    resilience = timedelta(minutes=float(get("resilience_window_min", 15)))
    times = [_when(b) for b in books]
    marks = [(t, b) for t, b in zip(times, books, strict=True) if since - interval <= t <= end]
    inside = [
        (parse_time(r["minute_start"]), r) for r in bars if parse_time(r["minute_start"]) + MINUTE <= end
    ]
    latest = max(marks, key=lambda x: x[0], default=None)
    pre = [b for t, b in marks if anchor is not None and anchor - timedelta(hours=1) <= t < anchor]
    bid_pre = [depth(b, "bid", band) for b in pre]
    ask_pre = [depth(b, "ask", band) for b in pre]
    native_bid = [depth(b, "bid", band, "native") for b in pre]
    native_ask = [depth(b, "ask", band, "native") for b in pre]
    both = [d + a for d, a in zip(bid_pre, ask_pre, strict=True) if d is not None and a is not None]
    spreads = [_number(b.get("spread_bps")) for b in pre if b.get("spread_bps") is not None]
    span = [b for t, b in marks if begin <= t <= end]

    def before(moment):
        found = [(t, b) for t, b in marks if t < moment]
        return found[-1] if found and found[-1][0] >= moment - interval else None

    bursts, ask_testable, ask_recovered = [], 0, 0
    for t, bar in inside:
        if t < begin:
            continue
        base = before(t)
        buy, sell = _bar_value(bar, "buy_notional"), _bar_value(bar, "sell_notional")
        if base is None or buy is None or sell is None:
            continue
        bid_base, ask_base = depth(base[1], "bid", band), ask_any(base[1])
        if bid_base is not None and sell - buy >= bid_base:
            later = [
                (u, b)
                for u, b in marks
                if t < u <= min(t + refill, end) and depth(b, "bid", band) is not None
            ]
            found = [u for u, b in later if depth(b, "bid", band) >= bid_base]
            lows = [
                _number(x.get("low"))
                for u, x in inside
                if t < u <= t + resilience and x.get("low") is not None
            ]
            low = _number(bar.get("low"))
            bursts.append(
                (
                    t,
                    int(bool(found)) if t + refill <= end else None,
                    (found[0] - t).total_seconds() if found else None,
                    int(min(lows) >= low) if lows and low is not None and t + resilience <= end else None,
                )
            )
        if ask_base is not None and buy - sell >= ask_base and t + refill <= end:
            ask_testable += 1
            ask_recovered += int(
                any(
                    ask_any(b) is not None and ask_any(b) >= ask_base for u, b in marks if t < u <= t + refill
                )
            )
    start_book = next(((t, b) for t, b in reversed(marks) if t <= begin), None)
    start_book = start_book if start_book and start_book[0] >= begin - interval else None
    building = (
        int(ask_any(latest[1]) > ask_any(start_book[1]))
        if latest and start_book and ask_any(latest[1]) is not None and ask_any(start_book[1]) is not None
        else None
    )
    pre_depth = _mean(both)
    return truth_sql.BookLiquidity(
        window=window,
        venue=venue,
        books=len(marks),
        latest_at=latest[0] if latest else None,
        latest_prec=latest[1].get("exchange_ts_prec") if latest else None,
        latest_prec_reason=latest[1].get("exchange_ts_prec_reason") if latest else None,
        latest_spread=_number(latest[1].get("spread_bps")) if latest else None,
        latest_bid=depth(latest[1], "bid", band) if latest else None,
        latest_ask=depth(latest[1], "ask", band) if latest else None,
        latest_bid_native=depth(latest[1], "bid", band, "native") if latest else None,
        latest_ask_native=depth(latest[1], "ask", band, "native") if latest else None,
        latest_covered=json.dumps(latest[1].get("range_covered_pct"))
        if latest and latest[1].get("range_covered_pct") is not None
        else None,
        pre_books=len(pre),
        pre_bid=_mean([x for x in bid_pre if x is not None]),
        pre_ask=_mean([x for x in ask_pre if x is not None]),
        pre_bid_native=_mean([x for x in native_bid if x is not None]),
        pre_ask_native=_mean([x for x in native_ask if x is not None]),
        pre_native_missing=(int(None in native_bid) | 2 * int(None in native_ask)) if pre else 0,
        pre_depth=pre_depth,
        pre_spread=_mean(spreads),
        window_bid=_mean([x for x in (depth(b, "bid", band) for b in span) if x is not None]),
        window_ask=_mean([x for x in (depth(b, "ask", band) for b in span) if x is not None]),
        bars=sum(1 for t, _ in inside if t >= begin),
        bursts=len(bursts),
        replenished=sum(x[1] == 1 for x in bursts),
        resilient_bursts=sum(x[1] == 1 and x[3] == 1 for x in bursts),
        burst_at=tuple(x[0] for x in bursts),
        burst_replenished=tuple(x[1] for x in bursts),
        burst_recovery_s=tuple(x[2] for x in bursts),
        burst_resilient=tuple(x[3] for x in bursts),
        ask_building=building,
        ask_testable_bars=ask_testable,
        ask_recovered_bars=ask_recovered,
    )


def tier_records(snapshot, books, windows, end, tiers, assembled):
    found = []
    for name, notionals in dict(tiers).items():
        latest = {}
        for venue, rows in books.items():
            rows = [b for b in rows if end - timedelta(hours=1) <= _when(b) <= end]
            if rows:
                latest[venue] = rows[-1]
        asks = {venue: ask_any(b) for venue, b in latest.items()}
        total = sum(x for x in asks.values() if x is not None)
        for venue, book in sorted(latest.items()):
            fx = 1.0 if book.get("quote_asset") == "USD" else _number(book.get("quote_usd_rate"))
            mid = _number(book.get("mid"))
            levels = _levels(book)

            def slippage(notional):
                if not notional or notional <= 0 or not fx or fx <= 0:
                    return None
                return walk(levels, notional / fx, mid)

            parts = [
                n * asks[venue] / total if n and asks[venue] is not None and total > 0 else None
                for n in notionals
            ]
            found.append(
                truth_sql.BookTier(
                    window=name,
                    venue=venue,
                    observed_at=_when(book),
                    ask_p1=asks[venue],
                    levels=len(levels),
                    **{f"slippage_t{i + 1}": slippage(n) for i, n in enumerate(notionals)},
                    **{f"split_t{i + 1}": slippage(n) for i, n in enumerate(parts)},
                )
            )
    return found


def _usd_mid(book, mid):
    if mid is None:
        return None
    if str(book.get("quote_asset") or "").upper() == "USD":
        return mid
    rate = _number(book.get("quote_usd_rate"))
    return mid * rate if rate else None


def pair_records(books, end, assembled, get):
    cadence = timedelta(seconds=float(get("book_interval_s", 10)))
    cost_bps = float(get("quote_conversion_uncertainty_bps", 2))
    begin = end - timedelta(minutes=15)
    window = {
        venue: [(_when(b), b) for b in rows if begin - cadence <= _when(b) <= end]
        for venue, rows in books.items()
    }
    window = {venue: rows for venue, rows in window.items() if rows}
    found = []
    names = sorted(window)
    for index, a in enumerate(names):
        for b in names[index + 1 :]:
            ar, br = window[a], window[b]
            moments = sorted({begin, end} | {t for t, _ in ar + br if begin <= t < end})
            problem, gaps, closed, confirmed = 0, [], False, False
            for t in moments:
                states = []
                for rows in (ar, br):
                    last = [(u, x) for u, x in rows if u <= t]
                    states.append(last[-1][1] if last and (t - last[-1][0]) <= cadence else None)
                x, y = states
                if x is None or y is None:
                    problem = 1
                    break
                xm, ym = _number(x.get("mid")), _number(y.get("mid"))
                xs, ys = _number(x.get("spread_bps")), _number(y.get("spread_bps"))
                if None in (xm, ym, xs, ys) or not ym:
                    problem = 1
                    break
                same = str(x.get("quote_asset") or "").upper() == str(y.get("quote_asset") or "").upper()
                cost = 0.0 if same else cost_bps
                xm, ym = (xm, ym) if same else (_usd_mid(x, xm), _usd_mid(y, ym))
                if xm is None or not ym:
                    problem = 2
                    break
                gap = abs(xm - ym) * 10000 > (xs + ys + cost) * ym
                if t < end:
                    gaps.append(gap)
                elif gaps and gaps[-1] and not gap:
                    closed = True
                rest = []
                for stream in (x, y):
                    independent = stream.get("independent_rest") or {}
                    stamp = independent.get("ts")
                    mid = _number(independent.get("mid"))
                    spread = _number(independent.get("spread_bps"))
                    good = (
                        stream.get("second_path_confirmed") is True
                        and independent.get("source") == "PUBLIC_REST"
                        and stamp is not None
                        and parse_time(stamp) <= end
                        and (
                            not independent.get("received_ts")
                            or not assembled
                            or parse_time(independent["received_ts"]) <= assembled
                        )
                        and abs((parse_time(stamp) - _when(stream)).total_seconds())
                        <= cadence.total_seconds()
                        and mid is not None
                        and mid > 0
                        and spread is not None
                    )
                    rest.append((mid if same else _usd_mid(stream, mid), spread) if good else None)
                confirmed = confirmed or (
                    None not in rest
                    and None not in (rest[0][0], rest[1][0])
                    and abs(rest[0][0] - rest[1][0]) * 10000 > (rest[0][1] + rest[1][1] + cost) * rest[1][0]
                )
            persistent = not problem and bool(gaps) and all(gaps)
            found.append(
                truth_sql.BookPair(
                    a_venue=a,
                    b_venue=b,
                    problem=problem,
                    persistent=int(persistent),
                    closed=int(closed and not problem),
                    confirmed=int(confirmed),
                )
            )
    return found


def liquidity_evidence(snapshot, windows=None, *, state=True, config=None, anchor=..., end=None, venues=None):
    """The `LiquidityEvidence` a snapshot of hand-written books and bars would have: the windows of an assessment
    unless given, the anchor of the snapshot unless given (None for no pre-event hour). The impact tiers are scaled
    by the volume of the venues whose flow the windows cover, or by `venues` when given."""
    config = config or {}
    end = end or parse_time(snapshot["cutoff"])
    if anchor is ...:
        value = snapshot.get("anchor")
        value = value.get("ts") if isinstance(value, dict) else value
        anchor = parse_time(value) if value else None
    start = anchor or end - timedelta(hours=4)
    if windows is None:
        windows = dict(truth.WINDOWS, EVENT=(end - start).total_seconds())

    def get(name, default):
        return config.get(name, default)

    assembled = parse_time(snapshot["assembled_at"]) if snapshot.get("assembled_at") else None
    books, bars = _books(snapshot, assembled), _bars(snapshot, assembled)
    begins = {name: end - timedelta(seconds=seconds) for name, seconds in windows.items()}
    since = min(
        [end - timedelta(hours=1), *begins.values(), *([anchor - timedelta(hours=1)] if anchor else [])]
    )
    venues = sorted(set(books) | set(bars))
    records = [
        venue_record(name, venue, books.get(venue, []), bars.get(venue, []), begin, end, anchor, since, get)
        for name, begin in begins.items()
        for venue in venues
    ]
    if not state:
        return truth_sql.LiquidityEvidence(books=tuple(records))
    with_flow = dict(snapshot, flow_sql=snapshot.get("flow_sql") or flow_evidence(snapshot, windows=windows))
    tiers = (
        tuple((name, truth.tier_usd(with_flow, venues, get)) for name in windows)
        if venues is not None
        else truth.tier_notionals(with_flow, windows, end, get)
    )
    return truth_sql.LiquidityEvidence(
        books=tuple(records),
        tiers=tuple(tier_records(snapshot, books, windows, end, tiers, assembled)),
        pairs=tuple(pair_records(books, end, assembled, get)),
        tier_usd=tiers,
    )


def with_liquidity(snapshot, **kwargs):
    """The snapshot with the liquidity evidence of its books as `liquidity_sql`."""
    return dict(snapshot, liquidity_sql=liquidity_evidence(snapshot, **kwargs))


def window_liquidity(snapshot, start, end, *, name="W4H", **kwargs):
    """The snapshot with the liquidity evidence (and the flow evidence) of the one window [start, end] named
    `name`, whose pre-event hour ends at its start unless `anchor` is given (what `truth.liquidity_truth` assumes
    of a snapshot without an event anchor)."""
    kwargs.setdefault("anchor", snapshot.get("_event_anchor", start))
    windows = {name: (end - start).total_seconds()}
    timed = dict(snapshot, cutoff=iso(end))
    flow = snapshot.get("flow_sql") or flow_evidence(timed, windows=windows, flips={})
    evidence = liquidity_evidence(dict(timed, flow_sql=flow), windows=windows, end=end, **kwargs)
    return dict(snapshot, flow_sql=flow, liquidity_sql=evidence)
