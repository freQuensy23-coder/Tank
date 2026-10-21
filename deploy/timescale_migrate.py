"""Move the mkt.* market series from daily declarative partitions to TimescaleDB hypertables.

Run inside the tank container (it has the database settings):
    docker exec -i tank-tank-1 /opt/tank/venv/bin/python - [--tables a,b] < deploy/timescale_migrate.py

Per table, smallest first, and resumable:
1. `mkt.<t>__ts` is created like the partitioned parent, made a hypertable (1-day chunks)
   with the parent's indexes.
2. Partitions older than 8 days are copied oldest first and their chunks compressed at
   once (~10% of the original size); the old partitions stay readable meanwhile.
3. Swap, in one transaction holding the parent's lock (writers wait, nothing is lost):
   the last 8 days are upserted, the parent is renamed away and dropped, `__ts` takes
   its name, and grants are copied.
4. Compression (segment by instrument, after 7 days: repairs of 96 h event windows and
   late bar revisions touch only uncompressed chunks) and, for book_summaries, a 72 h
   retention policy (03 §14).
"""

import sys
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import sql

from tank.settings import Settings

# table -> (time column, segment-by columns); smallest first.
TABLES = {
    "liq_obs": ("ts", "instrument_id"),
    "flow_bars_1m": ("minute_start", "instrument_id"),
    "tickers_24h": ("ts", "instrument_id"),
    "book_summaries": ("exchange_ts", "instrument_id"),
    "oi_obs": ("ts", "instrument_id, metric_key"),
    "klines_1m": ("open_time", "instrument_id"),
}
# Columns not carried over (order-book levels are not stored any more, 03 §14).
DROPPED = {"book_summaries": {"bids", "asks"}}
COMPRESS_AFTER = "7 days"
# Partitions this recent are copied in the swap itself (revisions and repairs may still
# change them); older ones are copied and compressed beforehand.
SWAP_WINDOW = timedelta(days=8)


def log(*args):
    print(*args, flush=True)


def children(cur, table):
    cur.execute(
        "SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) FROM pg_inherits i "
        "JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = %s::regclass ORDER BY c.relname",
        (f"mkt.{table}",),
    )
    return [r[0] for r in cur.fetchall()]


def is_hypertable(cur, table):
    cur.execute(
        "SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema='mkt' AND hypertable_name=%s",
        (table,),
    )
    return cur.fetchone() is not None


def columns(cur, table):
    cur.execute(
        "SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        (f"mkt.{table}",),
    )
    return [r[0] for r in cur.fetchall()]


def copy_rows(cur, table, source, upsert):
    cols = columns(cur, table)
    target = sql.Identifier("mkt", table + "__ts")
    select = sql.SQL(",").join(sql.NULL if c in DROPPED.get(table, ()) else sql.Identifier(c) for c in cols)
    names = sql.SQL(",").join(map(sql.Identifier, cols))
    conflict = sql.SQL("ON CONFLICT DO NOTHING")
    if upsert:
        # Rows the source changed after an earlier copy win (a revised bar, a late repair).
        cur.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname='mkt' AND tablename=%s AND indexdef ILIKE 'CREATE UNIQUE%%'",
            (table + "__ts",),
        )
        key = cur.fetchone()[0].split("(", 1)[1].rsplit(")", 1)[0]
        keys = [k.strip() for k in key.split(",")]
        conflict = sql.SQL("ON CONFLICT ({}) DO UPDATE SET {}").format(
            sql.SQL(",").join(map(sql.Identifier, keys)),
            sql.SQL(",").join(
                sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in cols
                if c not in keys
            ),
        )
    cur.execute(
        sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {} {}").format(
            target, names, select, sql.Identifier("mkt", source), conflict
        )
    )
    return cur.rowcount


def unvalidated_checks(cur, table):
    """The parent's NOT VALID CHECK constraints: (name, definition)."""
    cur.execute(
        "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conrelid = %s::regclass AND contype = 'c' AND NOT convalidated",
        (f"mkt.{table}",),
    )
    return cur.fetchall()


def prepare(conn, table, ts, segment):
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"mkt.{table}__ts",))
        if cur.fetchone()[0] is not None:
            return
        cur.execute(
            sql.SQL(
                "CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE)"
            ).format(sql.Identifier("mkt", table + "__ts"), sql.Identifier("mkt", table))
        )
        # NOT VALID legacy checks would reject old rows on copy: dropped here, added back
        # NOT VALID after the swap (unvalidated_checks).
        for name, _ in unvalidated_checks(cur, table):
            cur.execute(
                sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
                    sql.Identifier("mkt", table + "__ts"), sql.Identifier(name)
                )
            )
        cur.execute(
            "SELECT create_hypertable(%s, by_range(%s, INTERVAL '1 day'), create_default_indexes => false)",
            (f"mkt.{table}__ts", ts),
        )
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='mkt' AND tablename=%s", (table,)
        )
        for name, definition in cur.fetchall():
            definition = definition.replace(" ON ONLY mkt.", " ON mkt.").replace(
                f" ON mkt.{table} ", f" ON mkt.{table}__ts "
            )
            definition = definition.replace(f"INDEX {name} ", f"INDEX {name}__ts ", 1)
            cur.execute(definition)
        cur.execute(
            sql.SQL(
                "ALTER TABLE {} SET (timescaledb.compress, timescaledb.compress_segmentby = {}, "
                "timescaledb.compress_orderby = {})"
            ).format(sql.Identifier("mkt", table + "__ts"), sql.Literal(segment), sql.Literal(ts + " DESC"))
        )
    conn.commit()
    log(table, "hypertable created")


def part_day(name):
    """The UTC day of a daily partition (mkt.<t>_YYYYMMDD), None for the DEFAULT one."""
    digits = name.rsplit("_", 1)[-1]
    if len(digits) == 8 and digits.isdigit():
        return datetime(int(digits[:4]), int(digits[4:6]), int(digits[6:]), tzinfo=timezone.utc)
    return None


def backfill(conn, table, done):
    """Copy every partition older than the swap window and compress its chunks at once,
    so the copy costs ~10% of the original; the old partitions stay readable until the
    swap (no history hole for Market Truth meanwhile)."""
    cutoff = datetime.now(timezone.utc) - SWAP_WINDOW
    with conn.cursor() as cur:
        parts = children(cur, table)
    for part in parts:
        day = part_day(part)
        if part in done or (day is not None and day >= cutoff):
            continue
        with conn.cursor() as cur:
            n = copy_rows(cur, table, part, upsert=False)
            lo = day or datetime(1970, 1, 1, tzinfo=timezone.utc)
            hi = (day + timedelta(days=1)) if day else cutoff
            cur.execute(
                "SELECT count(compress_chunk(c, if_not_compressed => true)) "
                "FROM show_chunks(%s, older_than => %s, newer_than => %s) c",
                (f"mkt.{table}__ts", min(hi, cutoff), lo),
            )
        conn.commit()
        done.add(part)
        log(table, part, "copied", n, "compressed")


def swap(conn, table, segment, ts):
    """Under the parent's lock: upsert the recent partitions (they may have changed since),
    rename, copy grants, drop the old table (its disk is returned)."""
    with conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '30s'")
        cur.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(sql.Identifier("mkt", table)))
        cutoff = datetime.now(timezone.utc) - SWAP_WINDOW
        for part in children(cur, table):
            day = part_day(part)
            if day is None or day < cutoff:
                continue  # copied and compressed by backfill (DEFAULT holds only old rows)
            n = copy_rows(cur, table, part, upsert=True)
            log(table, part, "upserted", n)
        cur.execute(
            "SELECT grantee, privilege_type FROM information_schema.role_table_grants "
            "WHERE table_schema='mkt' AND table_name=%s AND grantee <> current_user",
            (table,),
        )
        grants = cur.fetchall()
        checks = unvalidated_checks(cur, table)
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier("mkt", table), sql.Identifier(table + "__old")
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier("mkt", table + "__ts"), sql.Identifier(table)
            )
        )
        for grantee, privilege in grants:
            cur.execute(
                sql.SQL("GRANT {} ON {} TO {}").format(
                    sql.SQL(privilege), sql.Identifier("mkt", table), sql.Identifier(grantee)
                )
            )
        # Sequences of serial columns (liq_obs.id) belong to the old table: hand them over
        # before the drop, or the drop takes them and every insert fails.
        cur.execute(
            "SELECT s.relname, a.attname FROM pg_depend d JOIN pg_class s ON s.oid = d.objid "
            "JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid "
            "WHERE s.relkind = %s AND d.refobjid = %s::regclass",
            ("S", f"mkt.{table}__old"),
        )
        for seq, column in cur.fetchall():
            cur.execute(
                sql.SQL("ALTER SEQUENCE {} OWNED BY {}.{}").format(
                    sql.Identifier("mkt", seq), sql.Identifier("mkt", table), sql.Identifier(column)
                )
            )
        cur.execute(sql.SQL("DROP TABLE {} CASCADE").format(sql.Identifier("mkt", table + "__old")))
    conn.commit()
    for name, definition in checks:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} {} NOT VALID").format(
                        sql.Identifier("mkt", table),
                        sql.Identifier(name),
                        sql.SQL(definition.replace(" NOT VALID", "")),
                    )
                )
                conn.commit()
            except psycopg.Error as exc:
                conn.rollback()
                log(table, "check", name, "not re-added:", type(exc).__name__, str(exc).splitlines()[0])
    log(table, "swapped")
    finish(conn, table, segment, ts)


def finish(conn, table, segment, ts):
    """Compression and retention policies (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT compression_enabled FROM timescaledb_information.hypertables "
            "WHERE hypertable_schema='mkt' AND hypertable_name=%s",
            (table,),
        )
        if not cur.fetchone()[0]:
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {} SET (timescaledb.compress, timescaledb.compress_segmentby = {}, "
                    "timescaledb.compress_orderby = {})"
                ).format(sql.Identifier("mkt", table), sql.Literal(segment), sql.Literal(ts + " DESC"))
            )
        cur.execute(
            "SELECT add_compression_policy(%s, %s::interval, if_not_exists => true)",
            (f"mkt.{table}", COMPRESS_AFTER),
        )
        if table == "book_summaries":
            cur.execute(
                "SELECT add_retention_policy('mkt.book_summaries', '72 hours'::interval, if_not_exists => true)"
            )
    conn.commit()
    log(table, "compression policy set")


def main():
    only = None
    if "--tables" in sys.argv:
        only = set(sys.argv[sys.argv.index("--tables") + 1].split(","))
    with psycopg.connect(Settings().database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        conn.commit()
        for table, (ts, segment) in TABLES.items():
            if only and table not in only:
                continue
            with conn.cursor() as cur:
                if is_hypertable(cur, table):
                    log(table, "already a hypertable")
                    finish(conn, table, segment, ts)
                    continue
            prepare(conn, table, ts, segment)
            backfill(conn, table, set())
            swap(conn, table, segment, ts)


main()
