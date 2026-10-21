"""Shadow comparison: tank-capture's shadow flow bars against the live ones.

Per (instrument, minute) of [--since, --until), mkt_shadow.flow_bars_1m (tank-capture in
shadow mode) is compared with mkt.flow_bars_1m (the in-process capture): buy and sell
quantity, trade counts, sum of squared quantities, size buckets and completeness, and
summarized per venue. Numbers compare as decimals ("1.50" equals "1.5"), size buckets as
JSON with their numbers as decimals.

The rollout gate is a minute complete in both schemas whose compared fields differ: any
such minute exits with status 1. Minutes complete in one schema only are reported but do
not fail the gate (the two writers seal and repair on their own schedules). The USD
notionals inside the size buckets are valued with each writer's own stablecoin USD sample,
fetched on its own schedule, so they agree within --usd-tolerance (relative, default
0.001); quantities and counts agree exactly.

  --since/--until ISO bounds, default the 24 h ending SETTLE_MARGIN before now
  --venue V        only this venue (repeatable)
  --json           machine-readable report
  --show N         print up to N differing minutes (default 20)
  --usd-tolerance  relative tolerance of size-bucket USD notionals (0: exact)

Run: uv run python scripts/capture/shadow_compare.py [--since ISO] [--until ISO] [--venue binance]
"""

import argparse
import json
import sys
from datetime import timedelta
from decimal import Decimal, InvalidOperation

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from tank.util import iso, parse_time, utcnow

FIELDS = ("buy_qty", "sell_qty", "n_buy", "n_sell", "sum_sq_qty", "size_buckets", "complete")
# Minutes this close to now may still be open or awaiting a head check in either writer.
SETTLE_MARGIN = timedelta(minutes=5)
COLUMNS = ("instrument_id", "minute_start", "venue", "market_type", "gap_reason", *FIELDS)
USD_FIELDS = ("buy_notional", "sell_notional")
USD_TOLERANCE = Decimal("0.001")


def normalized(value):
    """Numbers and numeric strings as Decimal, recursively; other values unchanged."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    if isinstance(value, str):
        try:
            number = Decimal(value)
        except InvalidOperation:
            return value
        return number if number.is_finite() else value
    if isinstance(value, dict):
        return {key: normalized(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalized(item) for item in value]
    return value


def jsonb(value):
    """A jsonb column read as text, or already parsed."""
    return json.loads(value, parse_float=Decimal) if isinstance(value, str) else value


def buckets_differ(a, b, tolerance):
    """Size buckets differ: any count or quantity, or a USD notional beyond the tolerance."""
    a, b = normalized(jsonb(a)), normalized(jsonb(b))
    if not isinstance(a, dict) or not isinstance(b, dict) or set(a) != set(b):
        return a != b
    for name in a:
        x, y = a[name], b[name]
        if not isinstance(x, dict) or not isinstance(y, dict):
            if x != y:
                return True
            continue
        for key in set(x) | set(y):
            u, v = x.get(key), y.get(key)
            if key in USD_FIELDS and isinstance(u, Decimal) and isinstance(v, Decimal):
                if abs(u - v) > tolerance * max(abs(u), abs(v)):
                    return True
            elif u != v:
                return True
    return False


def differing_fields(live, shadow, tolerance=USD_TOLERANCE):
    """Compared fields whose values differ between two rows of the same minute."""
    differ = []
    for field in FIELDS:
        a, b = live.get(field), shadow.get(field)
        if field == "size_buckets":
            if buckets_differ(a, b, tolerance):
                differ.append(field)
        elif normalized(a) != normalized(b):
            differ.append(field)
    return differ


def key_of(row):
    return row["instrument_id"], parse_time(row["minute_start"])


def venue_of(row):
    return f"{row.get('venue')}/{row.get('market_type')}"


def keyed(rows):
    for row in rows:
        yield key_of(row), row


def pairs(live_rows, shadow_rows):
    """(live, shadow) per minute, None where a schema has no row; both inputs are in
    (instrument_id, minute_start) order."""
    end = (None, None)
    live_rows, shadow_rows = keyed(live_rows), keyed(shadow_rows)
    (live_key, live), (shadow_key, shadow) = next(live_rows, end), next(shadow_rows, end)
    while live is not None or shadow is not None:
        if shadow is None or (live is not None and live_key < shadow_key):
            yield live, None
            live_key, live = next(live_rows, end)
        elif live is None or shadow_key < live_key:
            yield None, shadow
            shadow_key, shadow = next(shadow_rows, end)
        else:
            yield live, shadow
            (live_key, live), (shadow_key, shadow) = next(live_rows, end), next(shadow_rows, end)


def empty_summary():
    return {
        "compared": 0,
        "equal": 0,
        "live_only": 0,
        "shadow_only": 0,
        "complete_both": 0,
        "complete_live_only": 0,
        "complete_shadow_only": 0,
        "gate_failures": 0,
        "mismatched": dict.fromkeys(FIELDS, 0),
    }


def add(summary, live, shadow, differ):
    live_complete = bool(live and live.get("complete") is True)
    shadow_complete = bool(shadow and shadow.get("complete") is True)
    if live is None:
        summary["shadow_only"] += 1
    elif shadow is None:
        summary["live_only"] += 1
    else:
        summary["compared"] += 1
        summary["equal"] += not differ
        for field in differ:
            summary["mismatched"][field] += 1
    if live_complete and shadow_complete:
        summary["complete_both"] += 1
        summary["gate_failures"] += bool(differ)
    elif live_complete:
        summary["complete_live_only"] += 1
    elif shadow_complete:
        summary["complete_shadow_only"] += 1


def compare(live_rows, shadow_rows, *, show=20, tolerance=USD_TOLERANCE):
    """The report of two (instrument_id, minute_start)-ordered row streams."""
    venues, total, examples = {}, empty_summary(), []
    for live, shadow in pairs(live_rows, shadow_rows):
        row = live or shadow
        differ = differing_fields(live, shadow, tolerance) if live is not None and shadow is not None else []
        for summary in (venues.setdefault(venue_of(row), empty_summary()), total):
            add(summary, live, shadow, differ)
        if differ and len(examples) < show:
            examples.append(
                {
                    "instrument_id": row["instrument_id"],
                    "minute_start": iso(key_of(row)[1]),
                    "gate": live.get("complete") is True and shadow.get("complete") is True,
                    "fields": {
                        field: {"live": live.get(field), "shadow": shadow.get(field)} for field in differ
                    },
                    "gap_reason": {"live": live.get("gap_reason"), "shadow": shadow.get("gap_reason")},
                }
            )
    return {
        "venues": dict(sorted(venues.items())),
        "total": total,
        "examples": examples,
        "verdict": "FAIL" if total["gate_failures"] else "PASS",
    }


def read(connection, schema, since, until, venues, name):
    """Bars of one schema in (instrument_id, minute_start) order through a server-side cursor.
    COLLATE "C" orders instrument IDs by code point, as Python compares them."""
    clauses = [sql.SQL("minute_start >= %s"), sql.SQL("minute_start < %s")]
    args = [since, until]
    if venues:
        clauses.append(sql.SQL("venue = ANY(%s)"))
        args.append(list(venues))
    query = sql.SQL('SELECT {} FROM {} WHERE {} ORDER BY instrument_id COLLATE "C", minute_start').format(
        sql.SQL(",").join(map(sql.Identifier, COLUMNS)),
        sql.Identifier(schema, "flow_bars_1m"),
        sql.SQL(" AND ").join(clauses),
    )
    cursor = connection.cursor(name=name, row_factory=dict_row)
    cursor.itersize = 5000
    cursor.execute(query, args)
    return cursor


def table_exists(connection, schema):
    row = connection.execute("SELECT to_regclass(%s) IS NOT NULL", (schema + ".flow_bars_1m",)).fetchone()
    return bool(row[0])


def render(report, args):
    lines = [
        f"shadow comparison {iso(args.since)} -> {iso(args.until)} ({args.live_schema} vs {args.shadow_schema})"
    ]
    header = ["venue", "compared", "equal", "live_only", "shadow_only", "complete_both"]
    header += ["complete_live_only", "complete_shadow_only", "gate_failures", *FIELDS]
    rows = [header]
    for venue, summary in [*report["venues"].items(), ("TOTAL", report["total"])]:
        rows.append(
            [venue, *(str(summary[name]) for name in header[1:9])]
            + [str(summary["mismatched"][field]) for field in FIELDS]
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    lines += ["  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)) for row in rows]
    for example in report["examples"]:
        lines.append(
            ("GATE " if example["gate"] else "     ")
            + f"{example['instrument_id']} {example['minute_start']} "
            + json.dumps(example["fields"], default=str, sort_keys=True)
        )
    lines.append(
        "verdict: "
        + report["verdict"]
        + (
            f" ({report['total']['gate_failures']} minutes complete in both schemas differ)"
            if report["verdict"] == "FAIL"
            else " (no minute complete in both schemas differs)"
        )
    )
    return "\n".join(lines)


def parse(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--since")
    parser.add_argument("--until")
    parser.add_argument("--venue", action="append", default=[])
    parser.add_argument("--database-url")
    parser.add_argument("--live-schema", default="mkt")
    parser.add_argument("--shadow-schema", default="mkt_shadow")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--show", type=int, default=20)
    parser.add_argument("--usd-tolerance", type=Decimal, default=USD_TOLERANCE)
    args = parser.parse_args(argv)
    now = utcnow().replace(second=0, microsecond=0)
    args.until = parse_time(args.until) if args.until else now - SETTLE_MARGIN
    args.since = parse_time(args.since) if args.since else args.until - timedelta(hours=24)
    if args.since >= args.until:
        parser.error("--since must be before --until")
    if args.database_url is None:
        from tank.settings import Settings

        args.database_url = Settings().database_url
    return args


def main(argv=None):
    args = parse(argv)
    with psycopg.connect(args.database_url) as connection:
        for schema in (args.live_schema, args.shadow_schema):
            if not table_exists(connection, schema):
                print(f"{schema}.flow_bars_1m does not exist", file=sys.stderr)
                return 2
        live = read(connection, args.live_schema, args.since, args.until, args.venue, "live_bars")
        shadow = read(connection, args.shadow_schema, args.since, args.until, args.venue, "shadow_bars")
        report = compare(live, shadow, show=args.show, tolerance=args.usd_tolerance)
    report.update(
        since=iso(args.since),
        until=iso(args.until),
        live_schema=args.live_schema,
        shadow_schema=args.shadow_schema,
        venue_filter=args.venue,
    )
    print(json.dumps(report, default=str, indent=1, sort_keys=True) if args.json else render(report, args))
    return 1 if report["verdict"] == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
