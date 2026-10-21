"""Explicit synthetic typed flow aggregates; never reconstruct production raw data."""

from datetime import timedelta

from tank.util import iso, parse_time


async def put_flow(repo, key, row):
    row = dict(row)
    minute = row.pop("open_time", row.get("minute_start"))
    row["minute_start"] = minute
    reasons = dict(row.get("null_reasons") or {})
    for field in ("buy_usd", "sell_usd", "size_buckets"):
        if row.get(field) is None:
            reasons[field] = "INCOMPLETE"
    row["null_reasons"] = reasons
    count = 3 if row.get("h") is not None and row.get("l") is not None else 1
    for field, value in {
        "buy_qty": str(count),
        "sell_qty": "0",
        "n_buy": count,
        "n_sell": 0,
        "sum_sq_qty": str(count),
    }.items():
        if row.get(field) is None:
            row[field] = value
    if row.get("c") is not None:
        if "close" in row:
            row["close"] = row["c"]
        row["last_trade"] = {
            "trade_id": str(key),
            "price": str(row["c"]),
            "ts": iso(parse_time(minute) + timedelta(seconds=59)),
            "ts_prec": "EXACT",
        }
    await repo.put("mkt.flow_bars_1m", key, row)


async def put_boundary(repo, key, instrument, minute, lo, hi, trade_at, price):
    point = {"trade_id": key, "price": str(price), "ts": iso(trade_at), "ts_prec": "EXACT"}
    piece = {
        "start": iso(lo),
        "end": iso(hi),
        "complete": True,
        "observed_at": iso(hi),
        "n_buy": 1,
        "n_sell": 0,
        "high": str(price),
        "low": str(price),
        "first_trade": point,
        "last_trade": point,
        "coverage_reason": None,
    }
    await repo.put(
        "mkt.flow_bars_1m",
        key,
        {
            "instrument_id": instrument,
            "minute_start": iso(minute),
            "complete": False,
            "null_reasons": {"buy_usd": "INCOMPLETE", "sell_usd": "INCOMPLETE", "size_buckets": "INCOMPLETE"},
            "buy_qty": "1",
            "sell_qty": "0",
            "n_buy": 1,
            "n_sell": 0,
            "sum_sq_qty": "1",
            "h": "99999999999999999",
            "l": "0.0001",
            "boundary_segments": [piece],
            "reference_points": [],
        },
    )
