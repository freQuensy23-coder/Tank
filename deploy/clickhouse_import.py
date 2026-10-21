"""One-time move of the exchange market history into ClickHouse (HLD §7.1).

Run in the tank container once ClickHouse serves the live writes:
    docker exec -i tank-tank-1 /opt/tank/venv/bin/python - [--cleanup] < deploy/clickhouse_import.py

- Series: ClickHouse reads each PostgreSQL table itself (postgresql() over tank-net) through
  a temporary view that gives numbers and JSON as text, so every digit is kept. Imported
  rows carry _version 1: a row tank-capture writes live always wins over its history copy.
- Prints: ClickHouse reads the hourly trade files (all text columns) and the open hour
  journals from the tank-data volume (mounted read-only into it).
- Each table's count is checked (ClickHouse FINAL >= PostgreSQL); with --cleanup the
  PostgreSQL tables are then emptied (the trade files are removed by the caller).
"""

import asyncio
import sys
from pathlib import Path

import psycopg
from psycopg import sql

from tank import clickhouse_market as chm
from tank.market_schema import MARKET_COLUMNS
from tank.settings import Settings

VIEW_SCHEMA = "chexport"


def view_sql(collection):
    schema, table = collection.split(".")
    columns = [sql.Identifier("storage_key")]
    for name, kind in MARKET_COLUMNS[collection].items():
        column = sql.Identifier(name)
        if kind in ("numeric", "jsonb"):
            columns.append(sql.SQL("{}::text AS {}").format(column, column))
        else:
            columns.append(column)
    return sql.SQL("CREATE OR REPLACE VIEW {}.{} AS SELECT {} FROM {}.{}").format(
        sql.Identifier(VIEW_SCHEMA),
        sql.Identifier(table),
        sql.SQL(",").join(columns),
        sql.Identifier(schema),
        sql.Identifier(table),
    )


def insert_series(collection, password, since=None):
    table = collection.split(".")[1]
    names = ["storage_key", *MARKET_COLUMNS[collection]]
    target = ", ".join(f"`{n}`" for n in names) + ", `_version`"
    source = ", ".join(
        f"ifNull(`{n}`, [])" if MARKET_COLUMNS[collection].get(n) == "text[]" else f"`{n}`" for n in names
    )
    pg = f"postgresql('postgres:5432', 'tank', '{table}', 'tank', '{password}', '{VIEW_SCHEMA}')"
    # --since: only rows the old writer recorded after the first copy (a handover's tail).
    where = f" WHERE recorded_at >= parseDateTime64BestEffort('{since}', 6, 'UTC')" if since else ""
    return (
        f"INSERT INTO {collection} ({target}) SELECT {source}, 1 FROM {pg}{where} "
        "SETTINGS max_partitions_per_insert_block = 1000"
    )


TRADE_SELECT = """
SELECT instrument_id, trade_id, parseDateTime64BestEffort(ts, 6, 'UTC') AS ts,
  ifNull(token_id, ''), ifNull(venue, ''), ifNull(market_type, ''), ifNull(venue_symbol, ''), ifNull(side, ''),
  toDecimal128OrZero(price, 18), toDecimal128OrZero(qty_native, 18),
  toDecimal128OrNull(quote_notional, 18), toDecimal128OrNull(notional_usd, 18),
  ifNull(quote_asset, ''), usd_reason, ifNull(metric_key, ''), ifNull(trade_aggregation, ''),
  raw_first_id, raw_last_id, sequence, sequence_semantics, ifNull(capture_source, ''),
  ts_prec, ts_prec_reason, parseDateTime64BestEffortOrNull(received_ts, 6, 'UTC'), received_ts_prec,
  received_ts_prec_reason, ifNull(code_version, ''), ifNull(config_version, ''), 1
FROM {source}
WHERE instrument_id IS NOT NULL AND trade_id IS NOT NULL AND ts IS NOT NULL
"""
TEXT_COLUMNS = [
    "instrument_id",
    "trade_id",
    "ts",
    "token_id",
    "venue",
    "market_type",
    "venue_symbol",
    "side",
    "price",
    "qty_native",
    "quote_notional",
    "notional_usd",
    "quote_asset",
    "usd_reason",
    "metric_key",
    "trade_aggregation",
    "raw_first_id",
    "raw_last_id",
    "sequence",
    "sequence_semantics",
    "capture_source",
    "ts_prec",
    "ts_prec_reason",
    "received_ts",
    "received_ts_prec",
    "received_ts_prec_reason",
    "code_version",
    "config_version",
]
STRUCTURE = ", ".join(f"{c} Nullable(String)" for c in TEXT_COLUMNS)


async def main():
    cleanup = "--cleanup" in sys.argv
    since = sys.argv[sys.argv.index("--since") + 1] if "--since" in sys.argv else None
    only = set(sys.argv[sys.argv.index("--tables") + 1].split(",")) if "--tables" in sys.argv else None
    trade_files = sys.argv[sys.argv.index("--trades") + 1] if "--trades" in sys.argv else "*/*/*/*/*"
    settings = Settings()
    password = (Path(settings.secrets_dir) / "tank_db_password").read_text().strip()
    ch = chm.ClickHouse(settings.clickhouse_url, password=password)
    for statement in chm.ddl():
        await ch.execute(statement)
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(VIEW_SCHEMA)))
        for collection in chm.MARKET_TABLES:
            conn.execute(view_sql(collection))
        pg_counts = {
            c: conn.execute(
                sql.SQL("SELECT count(*) FROM {}.{}").format(*map(sql.Identifier, c.split(".")))
            ).fetchone()[0]
            for c in chm.MARKET_TABLES
        }
        for collection in chm.MARKET_TABLES:
            if only is not None and collection not in only:
                continue
            await ch.execute(insert_series(collection, password.replace("'", "\\'"), since))
            rows = await ch.rows(f"SELECT count() AS n FROM {collection} FINAL")
            print(collection, "postgres", pg_counts[collection], "clickhouse", rows[0]["n"], flush=True)
            if since is None and int(rows[0]["n"]) < pg_counts[collection]:
                raise SystemExit(f"{collection}: ClickHouse has fewer rows than PostgreSQL")
        # Prints: each hourly file (and open hour journal) goes in as it is; ClickHouse converts
        # the text columns while inserting (input()).
        files = sorted(
            Path(settings.data_dir, "raw", "trades").glob(
                trade_files.removeprefix("tank-data/raw/trades/") + ".parquet"
            )
        )
        files += sorted(
            Path(settings.data_dir, "raw", "trades").glob(
                trade_files.removeprefix("tank-data/raw/trades/") + ".capture.jsonl"
            )
        )
        for n, path in enumerate(files, 1):
            fmt = "Parquet" if path.suffix == ".parquet" else "JSONEachRow"
            query = (
                f"INSERT INTO {chm.TRADES} "
                + TRADE_SELECT.format(source=f"input('{STRUCTURE}')")
                + f" FORMAT {fmt}"
            )
            response = await ch.http.post(
                ch.base,
                params={
                    "query": query,
                    "input_format_parquet_allow_missing_columns": "1",
                    "input_format_skip_unknown_fields": "1",
                    "input_format_allow_errors_ratio": "0.001",
                    "max_partitions_per_insert_block": "1000",
                },
                content=path.read_bytes(),
                headers={"X-ClickHouse-User": ch.user, "X-ClickHouse-Key": ch.password},
            )
            if response.status_code != 200:
                raise SystemExit(f"{path}: {response.text[:300]}")
            if n % 200 == 0:
                print("trade files", n, "of", len(files), flush=True)
        rows = await ch.rows(
            f"SELECT count() AS n, min(ts) AS first, max(ts) AS last FROM {chm.TRADES} FINAL"
        )
        print(chm.TRADES, rows[0], flush=True)
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(VIEW_SCHEMA)))
        if cleanup:
            for collection in chm.MARKET_TABLES:
                conn.execute(sql.SQL("TRUNCATE {}.{}").format(*map(sql.Identifier, collection.split("."))))
                print(collection, "emptied in PostgreSQL", flush=True)
    await ch.close()


asyncio.run(main())
