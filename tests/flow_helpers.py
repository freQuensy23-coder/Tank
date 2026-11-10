"""Records of the trade-flow statements (analysis/sql/flow_windows.sql and flow_flips.sql) for tests of the
rules that read them, made from the minute bars a test writes down.

A test that describes a market minute by minute (`flow_bars`, with boundary segments in the bars or in
`flow_segments`) gets the records the statements return for it: per venue (spot) or instrument (perp) and window
the full minutes found, those not complete, whether every partial minute at a bound is covered by a chain of
complete segments, the sums, and the 15-minute sub-windows of the longer windows. The statements themselves are
tested against a real ClickHouse server in tests/test_truth_flow_sql.py."""

import json
from datetime import timedelta
from decimal import Decimal

from tank.analysis import truth, truth_sql
from tank.util import parse_time

LARGE = ("B4", "B5")
STEP = timedelta(minutes=15)


def _number(value):
    if isinstance(value, dict):
        value = value.get("value")
    return None if value is None else Decimal(str(value))


def _usable(row, assembled):
    stamp = row.get("received_ts")
    return not assembled or not stamp or parse_time(stamp) <= assembled


def _venue(row):
    return row.get("venue") or str(row["instrument_id"]).split(":")[0]


def _market(row):
    return row.get("market_type") or ("SPOT" if ":spot:" in row.get("instrument_id", "") else "PERP_LINEAR")


def _pieces(snapshot, assembled):
    found = {}
    for piece in snapshot.get("flow_segments", []):
        found[(piece["instrument_id"], piece["start"], piece["end"])] = piece
    for bar in snapshot.get("flow_bars", []):
        for piece in bar.get("boundary_segments") or []:
            found.setdefault(
                (bar["instrument_id"], piece["start"], piece["end"]),
                dict(piece, instrument_id=bar["instrument_id"]),
            )
    return [
        piece
        for piece in found.values()
        if piece.get("complete") is True
        and _usable(piece, assembled)
        and (
            not assembled or piece.get("observed_at") is None or parse_time(piece["observed_at"]) <= assembled
        )
    ]


def _chain(pieces, iid, start, end):
    cursor, chosen = start, []
    while cursor < end:
        options = [
            p
            for p in pieces
            if p["instrument_id"] == iid
            and parse_time(p["start"]) == cursor
            and parse_time(p["end"]) <= end
            and parse_time(p["start"]) < parse_time(p["end"])
        ]
        if not options:
            return None
        piece = max(options, key=lambda p: parse_time(p["end"]))
        chosen.append(piece)
        cursor = parse_time(piece["end"])
    return chosen


def _large(buckets, side):
    return sum((_number((buckets.get(name) or {}).get(side)) or Decimal(0) for name in LARGE), Decimal(0))


def _row_flow(row):
    buckets = row.get("size_buckets")
    lacking = not buckets or any(
        _number((buckets.get(name) or {}).get(side)) is None
        for name in LARGE
        for side in ("buy_notional", "sell_notional")
    )
    missing = sum(
        bit
        for bit, key in (
            (1, "buy_qty"),
            (2, "sell_qty"),
            (4, "sum_sq_qty"),
            (8, "buy_notional"),
            (16, "sell_notional"),
        )
        if _number(row.get(key)) is None
    )
    return dict(
        buy=_number(row.get("buy_qty")) or Decimal(0),
        sell=_number(row.get("sell_qty")) or Decimal(0),
        sq=_number(row.get("sum_sq_qty")) or Decimal(0),
        buy_usd=_number(row.get("buy_notional")) or Decimal(0),
        sell_usd=_number(row.get("sell_notional")) or Decimal(0),
        large_buy=_large(buckets or {}, "buy_notional"),
        large_sell=_large(buckets or {}, "sell_notional"),
        missing=missing | (32 if lacking else 0),
    )


def _record(window, previous, group, ids, bars, pieces, start, end, *, with_pieces=True, flip=None):
    floor_start = start.replace(second=0, microsecond=0)
    floor_end = end.replace(second=0, microsecond=0)
    first = floor_start + timedelta(minutes=1) if start != floor_start else start
    expected = max(0, int((floor_end - first).total_seconds() / 60))
    inside = [
        r
        for minute, r in bars
        if start <= minute and minute + timedelta(minutes=1) <= end and r["instrument_id"] in ids
    ]
    partial = truth.partial_minutes(start, end)
    left = right = ()
    ok = True
    summed = [_row_flow(r) for r in inside]
    reasons = {str(r["gap_reason"]) for r in inside if r.get("gap_reason")}
    reasons |= {
        str(p.get("gap_reason") or p.get("coverage_reason"))
        for p in pieces
        if p["instrument_id"] in ids and (p.get("gap_reason") or p.get("coverage_reason"))
    }
    for edge_start, edge_end, side in partial:
        chosen = []
        for iid in ids:
            chain = _chain(pieces, iid, edge_start, edge_end)
            if chain is None:
                ok = False
                break
            chosen.extend(chain)
        if not ok:
            break
        summed.extend(_row_flow(p) for p in chosen)
        text = tuple(json.dumps(p, default=str) for p in chosen)
        if side == "left":
            left = text
        else:
            right = text
    total = {
        key: float(sum((s[key] for s in summed), Decimal(0)))
        for key in ("buy", "sell", "sq", "buy_usd", "sell_usd", "large_buy", "large_sell")
    }
    base = dict(
        window=window,
        venue=group,
        expected=expected,
        minutes=len({r["minute_start"] for r in inside}),
        incomplete=sum(r.get("complete") is not True for r in inside),
        boundary_ok=int(ok),
        buy=total["buy"],
        sell=total["sell"],
        missing=sum(1 << i for i in range(6) if any(s["missing"] & (1 << i) for s in summed)),
    )
    if flip is not None:
        return truth_sql.FlowFlip(k=flip, sum_sq=total["sq"], **{**base, "missing": base["missing"] & 7})
    return truth_sql.FlowWindow(
        previous=int(previous),
        sum_sq=total["sq"],
        buy_usd=total["buy_usd"],
        sell_usd=total["sell_usd"],
        large_buy=total["large_buy"],
        large_sell=total["large_sell"],
        reasons=tuple(sorted(reasons)),
        left_pieces=left if with_pieces else (),
        right_pieces=right if with_pieces else (),
        **base,
    )


def _bars(snapshot):
    assembled = parse_time(snapshot["assembled_at"]) if snapshot.get("assembled_at") else None
    return [
        (parse_time(b["minute_start"]), b) for b in snapshot.get("flow_bars", []) if _usable(b, assembled)
    ], _pieces(snapshot, assembled)


def window_records(
    snapshot, start, end, *, name="W", previous=False, market="SPOT", by_instrument=False, parsed=None
):
    """The records of one window [start, end) of the bars of `snapshot`: per venue of the spot instruments and of
    the spot bars overlapping it (or per instrument with `by_instrument`)."""
    bars, pieces = parsed or _bars(snapshot)
    groups = {}
    token = snapshot.get("token_id")
    found = [r for r in snapshot.get("instruments", [])]
    found += [row for minute, row in bars if minute < end and minute + timedelta(minutes=1) > start]
    for row in found:
        if (_market(row) == "SPOT") == (market == "SPOT") and (
            token is None or row.get("token_id", token) == token
        ):
            groups.setdefault(row["instrument_id"] if by_instrument else _venue(row), set()).add(
                row["instrument_id"]
            )
    return [
        _record(name, previous, group, ids, bars, pieces, start, end) for group, ids in sorted(groups.items())
    ]


def flow_evidence(snapshot, windows=None, flips=None):
    """The `FlowEvidence` a snapshot of hand-written bars would have: the windows of an assessment unless given, the
    sub-windows of the windows with three or more of them unless `flips` names the windows."""
    end = parse_time(snapshot["cutoff"])
    anchor = snapshot.get("anchor")
    anchor_at = anchor.get("ts") if isinstance(anchor, dict) else anchor
    start = parse_time(anchor_at) if anchor_at else end - timedelta(hours=4)
    if windows is None:
        windows = dict(truth.WINDOWS, EVENT=(end - start).total_seconds())
    flips = windows if flips is None else flips
    spot, perp, steps = [], [], []
    parsed = _bars(snapshot)
    bars, pieces = parsed
    for name, seconds in windows.items():
        begin = end - timedelta(seconds=seconds)
        before = begin - timedelta(seconds=seconds)
        spot += window_records(snapshot, begin, end, name=name, parsed=parsed)
        spot += window_records(snapshot, before, begin, name=name, previous=True, parsed=parsed)
        perp += window_records(
            snapshot, begin, end, name=name, market="PERP", by_instrument=True, parsed=parsed
        )
    for name, seconds in flips.items():
        begin = end - timedelta(seconds=seconds)
        count = int(seconds // STEP.total_seconds())
        if count < 3:
            continue
        for k in range(count):
            sub_start, sub_end = begin + k * STEP, begin + (k + 1) * STEP
            for row in window_records(snapshot, sub_start, sub_end, name=name, parsed=parsed):
                ids = {b["instrument_id"] for _, b in bars if _venue(b) == row.venue and _market(b) == "SPOT"}
                steps.append(_record(name, False, row.venue, ids, bars, pieces, sub_start, sub_end, flip=k))
    return truth_sql.FlowEvidence(large=LARGE, spot=tuple(spot), perp=tuple(perp), flips=tuple(steps))


def spot_truth(rows, tickers, instruments, start, end, *, flow_segments=None, assembled_at=None):
    """`truth.spot_truth` of the window [start, end) of hand-written bars."""
    snapshot = dict(
        flow_bars=rows, flow_segments=flow_segments or [], instruments=instruments, assembled_at=assembled_at
    )
    return truth.spot_truth(window_records(snapshot, start, end), tickers, start, end)


def with_flow(snapshot, **kwargs):
    """The snapshot with the flow evidence of its bars as `flow_sql`."""
    return dict(snapshot, flow_sql=flow_evidence(snapshot, **kwargs))
