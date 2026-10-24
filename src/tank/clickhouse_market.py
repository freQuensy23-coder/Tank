"""Exchange market series and prints in ClickHouse (HLD §7.1): schema, reads and writes.

tank-capture writes these tables in production (capture/internal/store). Core reads them
through the repository methods; every collection of MARKET_TABLES is served here
(`repo.clickhouse`), rows decoded by market_storage.decode. A repository without a
ClickHouse client keeps them only in memory (tests and replay).

Tables are ReplacingMergeTree on the collection's identity, the newest `_version` wins, and
every read selects FINAL, so a row reads as its last write. Numbers are stored as their
exact decimal text (some carry 100 digits), timestamps as DateTime64(6, UTC), JSON as text.
"""

import asyncio
import json
import re
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal, Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import JsonValue

from tank.contracts.json import decode_json
from tank.market_schema import MARKET_ALIASES, MARKET_COLUMNS, MARKET_IDENTITIES
from tank.settings import Settings
from tank.util import canonical_json, iso, parse_time

MARKET_TABLES = {
    "mkt.klines_1m": "open_time",
    "mkt.flow_bars_1m": "minute_start",
    "mkt.book_summaries": "exchange_ts",
    "mkt.oi_obs": "ts",
    "mkt.funding_obs": "as_of",
    "mkt.liq_obs": "ts",
    "mkt.tickers_24h": "ts",
}

TRADES = "mkt.trades"
TRADE_COLUMNS = {
    "instrument_id": "String",
    "trade_id": "String",
    "ts": "DateTime64(6, 'UTC')",
    "token_id": "LowCardinality(String)",
    "venue": "LowCardinality(String)",
    "market_type": "LowCardinality(String)",
    "venue_symbol": "LowCardinality(String)",
    "side": "LowCardinality(String)",
    "price": "Decimal(38, 18)",
    "qty_native": "Decimal(38, 18)",
    "quote_notional": "Nullable(Decimal(38, 18))",
    "notional_usd": "Nullable(Decimal(38, 18))",
    "quote_asset": "LowCardinality(String)",
    "usd_reason": "LowCardinality(Nullable(String))",
    "metric_key": "LowCardinality(String)",
    "trade_aggregation": "LowCardinality(String)",
    "raw_first_id": "Nullable(String)",
    "raw_last_id": "Nullable(String)",
    "sequence": "Nullable(String)",
    "sequence_semantics": "LowCardinality(Nullable(String))",
    "capture_source": "LowCardinality(String)",
    "ts_prec": "LowCardinality(Nullable(String))",
    "ts_prec_reason": "LowCardinality(Nullable(String))",
    "received_ts": "Nullable(DateTime64(6, 'UTC'))",
    "received_ts_prec": "LowCardinality(Nullable(String))",
    "received_ts_prec_reason": "LowCardinality(Nullable(String))",
    "code_version": "LowCardinality(String)",
    "config_version": "LowCardinality(String)",
}
TYPES = {
    "text": "String",
    "numeric": "String",
    "timestamptz": "DateTime64(6, 'UTC')",
    "jsonb": "String",
    "boolean": "Bool",
    "bigint": "Int64",
    "text[]": "Array(String)",
}

ZSTD = "CODEC(ZSTD(3))"
TIME_CODEC = "CODEC(DoubleDelta, ZSTD(3))"
VERSION_CODEC = "CODEC(Delta, ZSTD(3))"
CODECS = {"timestamptz": TIME_CODEC, "boolean": "", "bigint": "CODEC(Delta, ZSTD(3))"}

TRADES_TTL_DAYS = 30


def ddl() -> list[str]:
    """Idempotent statements creating the mkt database and its tables."""
    statements = ["CREATE DATABASE IF NOT EXISTS mkt"]
    for collection, part in MARKET_TABLES.items():
        identity = MARKET_IDENTITIES[collection]
        columns = ["`storage_key` String"]
        for name, kind in MARKET_COLUMNS[collection].items():
            ch = TYPES[kind]
            if name not in identity and kind != "text[]":
                ch = f"Nullable({ch})"
            columns.append(f"`{name}` {ch} {CODECS.get(kind, ZSTD)}")
        columns.append(f"`_version` UInt64 {VERSION_CODEC}")
        columns.append("INDEX storage_key_bf storage_key TYPE bloom_filter GRANULARITY 4")
        columns.append("INDEX recorded_at_mm recorded_at TYPE minmax GRANULARITY 1")
        order = ", ".join(f"`{n}`" for n in identity)
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {collection} ({', '.join(columns)}) "
            f"ENGINE = ReplacingMergeTree(_version) PARTITION BY toYYYYMMDD(`{part}`) ORDER BY ({order})"
        )
    columns = [
        f"`{name}` {kind} {TIME_CODEC if 'DateTime64' in kind else '' if 'LowCardinality' in kind else ZSTD}"
        for name, kind in TRADE_COLUMNS.items()
    ] + [f"`_version` UInt64 {VERSION_CODEC}"]
    statements.append(
        f"CREATE TABLE IF NOT EXISTS {TRADES} ({', '.join(columns)}) ENGINE = ReplacingMergeTree(_version) "
        "PARTITION BY toYYYYMMDD(ts) ORDER BY (instrument_id, ts, trade_id)"
    )
    return statements


class ClickHouse:
    """A small async client of the ClickHouse HTTP interface (server-side parameters)."""

    def __init__(
        self,
        url: str,
        *,
        password: str | None = None,
        http: httpx.AsyncClient | None = None,
        database: str = "mkt",
    ) -> None:
        self.database = database
        parts = urlsplit(url)
        self.user = "default" if parts.username is None else parts.username
        self.password = (
            password if password is not None else ("" if parts.password is None else parts.password)
        )
        if parts.scheme not in {"http", "https"} or parts.hostname is None:
            raise ValueError("ClickHouse URL requires an HTTP(S) scheme and host")
        netloc = parts.hostname + (f":{parts.port}" if parts.port else "")
        self.base = urlunsplit((parts.scheme, netloc, "/", "", ""))
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(120, connect=10)) if http is None else http

    async def close(self) -> None:
        await self.http.aclose()

    async def _post(self, query: str, params: dict[str, tuple[str, str]] | None) -> bytes:
        request = {
            "date_time_output_format": "iso",
            "output_format_json_quote_64bit_integers": "0",
            "output_format_json_quote_decimals": "0",
            "do_not_merge_across_partitions_select_final": "1",
            "use_skip_indexes_if_final": "0",
            "max_query_size": str(256 << 20),
        }
        for name, (value, _) in (params or {}).items():
            request["param_" + name] = value
        response = await self.http.post(
            self.base,
            params=request,
            content=(query if self.database == "mkt" else re.sub(r"\bmkt\b", self.database, query)).encode(),
            headers={"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password},
        )
        if response.status_code != 200:
            raise RuntimeError(f"ClickHouse {response.status_code}: {response.text[:500]}")
        return response.content

    async def rows(
        self, query: str, params: dict[str, tuple[str, str]] | None = None
    ) -> list[dict[str, JsonValue]]:
        body = await self._post(query + " FORMAT JSONEachRow", params)
        if len(body) > 1_000_000:
            return await asyncio.to_thread(_json_lines, body)
        return _json_lines(body)

    async def execute(self, query: str, params: dict[str, tuple[str, str]] | None = None) -> None:
        await self._post(query, params)

    async def insert(self, table: str, rows: list[dict[str, JsonValue]]) -> None:
        if rows:
            body = "\n".join(json.dumps(row) for row in rows)
            await self._post(f"INSERT INTO {table} FORMAT JSONEachRow\n{body}", None)


def _json_lines(body: bytes) -> list[dict[str, JsonValue]]:
    rows: list[dict[str, JsonValue]] = []
    for line in body.splitlines():
        if not line:
            continue
        value = decode_json(line)
        if not isinstance(value, dict):
            raise ValueError("ClickHouse JSONEachRow must contain objects")
        rows.append(value)
    return rows


def _literal(value):
    """A value as ClickHouse parameter text."""
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return (
            "[" + ",".join("'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'" for v in value) + "]"
        )
    return str(value)


class Query:
    """Clauses and server-side parameters of one statement."""

    def __init__(self):
        self.clauses, self.params = [], {}

    def param(self, value, kind):
        if isinstance(value, (list, tuple)):
            return "(" + (_literal(value)[1:-1] or "NULL") + ")"
        name = f"p{len(self.params)}"
        self.params[name] = (_literal(value), kind)
        return "{" + name + ":" + kind + "}"

    def where(self, clause):
        self.clauses.append(clause)

    def predicate(self):
        return " AND ".join(self.clauses) if self.clauses else "1"


DT = "DateTime64(6, 'UTC')"


def _column(collection, name):
    name = MARKET_ALIASES.get(collection, {}).get(name, name)
    if name != "storage_key" and name not in MARKET_COLUMNS[collection]:
        raise ValueError(f"Unknown typed market filter {collection}.{name}")
    return name


def _filters(q, collection, filters):
    """_exact_predicate: typed equality, None as IS NULL."""
    for key, value in (filters or {}).items():
        name = _column(collection, key)
        kind = "text" if name == "storage_key" else MARKET_COLUMNS[collection][name]
        if value is None:
            q.where(f"`{name}` IS NULL")
        elif kind == "timestamptz":
            q.where(f"`{name}` = {q.param(parse_time(value), DT)}")
        elif kind == "jsonb":
            q.where(f"`{name}` = {q.param(canonical_json(value), 'String')}")
        elif kind == "numeric":
            q.where(f"toDecimal256OrNull(`{name}`, 30) = toDecimal256({q.param(value, 'String')}, 30)")
        else:
            q.where(f"`{name}` = {q.param(value, TYPES[kind])}")


def _after(q, *keys):
    """Rows after a keyset cursor, spelled out per column: ClickHouse prunes by the sort key on
    this form and reads the whole range again for every page on a tuple comparison."""
    q.where(_after_clause(q, *keys))


def _after_clause(q, *keys):
    column, value, kind = keys[0]
    here = q.param(value, kind)
    if len(keys) == 1:
        return f"{column} > {here}"
    return f"({column} > {here} OR ({column} = {here} AND {_after_clause(q, *keys[1:])}))"


def _ids(q, instrument_ids):
    q.where(f"instrument_id IN {q.param(list(instrument_ids), 'Array(String)')}")


def _record(collection, row):
    """A ClickHouse row as the typed values a PostgreSQL row carries (then decode)."""
    from tank.market_codec import decode

    out = {}
    for name, kind in MARKET_COLUMNS[collection].items():
        value = row.get(name)
        if value is None:
            out[name] = None
        elif kind == "timestamptz":
            out[name] = parse_time(value)
        elif kind == "jsonb":
            out[name] = json.loads(value)
        elif kind == "text[]":
            out[name] = list(value) or None
        elif kind == "numeric":
            out[name] = Decimal(value)
        else:
            out[name] = value
    return decode(collection, out)


def _records(collection, rows):
    return [_record(collection, r) for r in rows]


async def _decode(collection, rows):
    if len(rows) > 1000:
        return await asyncio.to_thread(_records, collection, rows)
    return _records(collection, rows)


def _table(collection):
    return f"{collection} FINAL"


async def _select(ch, collection, q, order, tail="", selected="*"):
    day = MARKET_TABLES[collection]
    keys = await ch.rows(
        f"SELECT storage_key, instrument_id, `{day}` AS _day FROM {_table(collection)} "
        f"WHERE {q.predicate()} ORDER BY {order} {tail}",
        q.params,
    )
    if not keys:
        return []
    days = [parse_time(r["_day"]) for r in keys]
    k = Query()
    k.where(f"instrument_id IN {k.param(sorted({r['instrument_id'] for r in keys}), 'Array(String)')}")
    k.where(f"`{day}` >= {k.param(min(days), DT)}")
    k.where(f"`{day}` <= {k.param(max(days), DT)}")
    k.where(f"storage_key IN {k.param([r['storage_key'] for r in keys], 'Array(String)')}")
    return await ch.rows(
        f"SELECT {selected} FROM {_table(collection)} WHERE {k.predicate()} ORDER BY {order}", k.params
    )


async def get(ch, collection, key):
    q = Query()
    q.where(f"storage_key = {q.param(str(key), 'String')}")
    rows = await _select(ch, collection, q, "storage_key", "LIMIT 1")
    return _record(collection, rows[0]) if rows else None


async def range_page(
    ch, collection, time_field, *, start=None, end=None, filters=None, limit=1000, after=None
):
    time_field = _column(collection, time_field)
    q = Query()
    q.where(f"`{time_field}` IS NOT NULL")
    if start:
        q.where(f"`{time_field}` >= {q.param(parse_time(start), DT)}")
    if end:
        q.where(f"`{time_field}` < {q.param(parse_time(end), DT)}")
    if after is not None:
        _after(q, (f"`{time_field}`", parse_time(after[0]), DT), ("storage_key", str(after[1]), "String"))
    _filters(q, collection, filters)
    rows = await _select(ch, collection, q, f"`{time_field}`, storage_key", f"LIMIT {int(limit)}")
    records = await _decode(collection, rows)
    return [(rec, (rec[time_field], row["storage_key"])) for rec, row in zip(records, rows, strict=True)]


async def latest_before(ch, collection, time_field, cutoff, *, filters=None):
    time_field = _column(collection, time_field)
    q = Query()
    q.where(f"`{time_field}` < {q.param(parse_time(cutoff), DT)}")
    _filters(q, collection, filters)
    rows = await _select(ch, collection, q, f"`{time_field}` DESC, storage_key DESC", "LIMIT 1")
    return _record(collection, rows[0]) if rows else None


async def all_page(ch, collection, *, filters=None, after=None, limit=1000):
    q = Query()
    _filters(q, collection, filters)
    if after is not None:
        q.where(f"storage_key > {q.param(str(after), 'String')}")
    rows = await _select(ch, collection, q, "storage_key", f"LIMIT {int(limit)}")
    records = await _decode(collection, rows)
    return [(row["storage_key"], rec) for row, rec in zip(rows, records, strict=True)]


async def list_latest(ch, collection, time_field, *, filters=None, limit=100, start=None, end=None):
    time_field = _column(collection, time_field)
    q = Query()
    _filters(q, collection, filters)
    q.where(f"`{time_field}` IS NOT NULL")
    if start is not None:
        q.where(f"`{time_field}` >= {q.param(parse_time(start), DT)}")
    if end is not None:
        q.where(f"`{time_field}` < {q.param(parse_time(end), DT)}")
    rows = await _select(ch, collection, q, f"`{time_field}` DESC, storage_key DESC", f"LIMIT {int(limit)}")
    return await _decode(collection, rows)


async def late_page(
    ch, collection, arrival_field, market_field, *, since, end, market_end, filters, after, limit
):
    q = Query()
    _filters(q, collection, filters)
    q.where(f"`{arrival_field}` IS NOT NULL")
    q.where(f"`{market_field}` <= {q.param(market_end, DT)}")
    if since is not None:
        q.where(f"`{arrival_field}` >= {q.param(since, DT)}")
    if end is not None:
        q.where(f"`{arrival_field}` < {q.param(end, DT)}")
    if after is not None:
        _after(q, (f"`{arrival_field}`", after[0], DT), ("storage_key", str(after[1]), "String"))
    rows = await _select(ch, collection, q, f"`{arrival_field}`, storage_key", f"LIMIT {int(limit)}")
    records = await _decode(collection, rows)
    return [
        (rec, (parse_time(rec[arrival_field]), row["storage_key"]))
        for rec, row in zip(records, rows, strict=True)
    ]


RECENT_LATEST = timedelta(days=1)


async def latest_market_records(
    ch, collection, *, group_by, time_field, filters, cutoff, assembled_at, start, instrument_ids
):
    """Latest row per group. Grouped by instrument alone, the day before the cutoff is read first
    and only instruments without a row there read the rest of the range: the same rows, without
    merging months of every instrument under FINAL for the last minute of each."""
    groups = ", ".join(f"`{g}`" for g in group_by)

    async def latest(lo, below, ids):
        q = Query()
        _filters(q, collection, filters)
        if lo is not None:
            q.where(f"`{time_field}` >= {q.param(lo, DT)}")
        if below is not None:
            q.where(f"`{time_field}` < {q.param(below, DT)}")
        if cutoff is not None:
            q.where(f"`{time_field}` <= {q.param(cutoff, DT)}")
        if assembled_at is not None:
            q.where(f"(recorded_at IS NULL OR recorded_at <= {q.param(assembled_at, DT)})")
        if ids is not None:
            _ids(q, ids)
        return await _select(
            ch,
            collection,
            q,
            f"{groups}, `{time_field}` DESC NULLS LAST, storage_key DESC",
            f"LIMIT 1 BY {groups}",
        )

    recent = None if cutoff is None else parse_time(cutoff) - RECENT_LATEST
    if (
        tuple(group_by) != ("instrument_id",)
        or instrument_ids is None
        or recent is None
        or (start is not None and parse_time(start) >= recent)
    ):
        return await _decode(collection, await latest(start, None, instrument_ids))
    rows = await latest(recent, None, instrument_ids)
    found = {row["instrument_id"] for row in rows}
    rest = [i for i in instrument_ids if i not in found]
    if rest:
        rows += await latest(start, recent, rest)
        rows.sort(key=lambda row: row["instrument_id"])
    return await _decode(collection, rows)


async def market_dependency_head(
    ch, collection, semantic_field, cutoff, *, start, filters, instrument_ids, assembled_at, wanted
):
    q = Query()
    _filters(q, collection, filters)
    q.where("recorded_at IS NOT NULL")
    q.where(f"`{semantic_field}` <= {q.param(cutoff, DT)}")
    if start is not None:
        q.where(f"`{semantic_field}` >= {q.param(start, DT)}")
    if assembled_at is not None:
        q.where(f"recorded_at <= {q.param(assembled_at, DT)}")
    if instrument_ids is not None:
        _ids(q, instrument_ids)
    picks = {
        "availability": "argMax",
        "semantic_latest": "argMax",
        "semantic_earliest": "argMin",
    }
    row = f"tuple(storage_key, recorded_at, semantic_fingerprint, `{semantic_field}`)"
    heads = ", ".join(
        f"{picks[order]}({row}, tuple({'recorded_at' if order == 'availability' else f'`{semantic_field}`'}, "
        f"storage_key)) AS `{order}`"
        for order in wanted
    )
    rows = await ch.rows(
        f"SELECT count() AS n, {heads} FROM {_table(collection)} WHERE {q.predicate()}", q.params
    )
    if not wanted or not rows or not int(rows[0]["n"]):
        return {}
    found = {}
    for order in wanted:
        key, recorded, fingerprint, semantic = rows[0][order]
        found[order] = {
            "storage_key": key,
            "recorded_at": iso(parse_time(recorded)),
            "semantic_fingerprint": fingerprint,
            semantic_field: iso(parse_time(semantic)),
        }
    return found


async def projection_page(
    ch, collection, selected, time_field, *, start, end, filters, instrument_ids, assembled_at, after, limit
):
    q = Query()
    _filters(q, collection, filters)
    q.where(f"`{time_field}` IS NOT NULL")
    if start is not None:
        q.where(f"`{time_field}` >= {q.param(start, DT)}")
    if end is not None:
        q.where(f"`{time_field}` < {q.param(end, DT)}")
    if assembled_at is not None:
        q.where(f"(recorded_at IS NULL OR recorded_at <= {q.param(assembled_at, DT)})")
    if instrument_ids is not None:
        _ids(q, instrument_ids)
    if after is not None:
        _after(q, (f"`{time_field}`", after[0], DT), ("storage_key", str(after[1]), "String"))
    expressions = ", ".join(f"`{field}` AS `{requested}`" for requested, field in selected.items())
    rows = await ch.rows(
        f"SELECT {expressions}, `{time_field}` AS _cursor_time, storage_key AS _cursor_key "
        f"FROM {_table(collection)} WHERE {q.predicate()} ORDER BY `{time_field}`, storage_key LIMIT {int(limit)}",
        q.params,
    )
    columns = MARKET_COLUMNS[collection]

    def convert():
        page = []
        for row in rows:
            result = {}
            for requested, field in selected.items():
                value = row[requested]
                kind = columns.get(field)
                if value is not None and kind == "timestamptz":
                    value = iso(parse_time(value))
                elif value is not None and kind == "jsonb":
                    value = json.loads(value)
                elif value is not None and kind == "numeric":
                    value = str(Decimal(value))
                elif kind == "text[]":
                    value = list(value) or None
                result[requested] = value
            page.append((parse_time(row["_cursor_time"]), row["_cursor_key"], result))
        return page

    return await asyncio.to_thread(convert) if len(rows) > 1000 else convert()


CLOSE_OK = "isFinite(toFloat64OrNull(c)) AND toFloat64OrNull(c) > 0"


async def market_series_counts(ch, instrument_ids, start, cutoff, assembled_at):
    q = Query()
    _ids(q, instrument_ids)
    q.where(f"open_time >= {q.param(start, DT)}")
    q.where(f"open_time < {q.param(cutoff, DT)}")
    q.where(f"open_time + INTERVAL 60 SECOND <= {q.param(cutoff, DT)}")
    q.where("interval_s = 60 AND complete = true AND " + CLOSE_OK)
    if assembled_at is not None:
        q.where(f"(recorded_at IS NULL OR recorded_at <= {q.param(assembled_at, DT)})")
    rows = await ch.rows(
        f"SELECT instrument_id, count() AS n FROM {_table('mkt.klines_1m')} WHERE {q.predicate()} GROUP BY instrument_id",
        q.params,
    )
    return {row["instrument_id"]: int(row["n"]) for row in rows}


async def latest_market_trades(ch, instrument_ids, start):
    q = Query()
    _ids(q, instrument_ids)
    q.where(f"minute_start >= {q.param(start, DT)}")
    q.where("last_trade IS NOT NULL")
    rows = await ch.rows(
        f"SELECT instrument_id, last_trade FROM {_table('mkt.flow_bars_1m')} WHERE {q.predicate()} "
        "ORDER BY instrument_id, minute_start DESC LIMIT 1 BY instrument_id",
        q.params,
    )
    return [{"instrument_id": r["instrument_id"], "last_trade": json.loads(r["last_trade"])} for r in rows]


async def market_series_day_counts(ch, instrument_ids, start, end, assembled_at):
    q = Query()
    _ids(q, instrument_ids)
    q.where(f"open_time >= {q.param(start, DT)}")
    q.where(f"open_time < {q.param(end, DT)}")
    q.where("interval_s = 60 AND complete = true AND " + CLOSE_OK)
    if assembled_at is not None:
        q.where(f"(recorded_at IS NULL OR recorded_at <= {q.param(assembled_at, DT)})")
    rows = await ch.rows(
        f"SELECT instrument_id, toString(toDate(open_time)) AS day, count() AS n "
        f"FROM {_table('mkt.klines_1m')} WHERE {q.predicate()} GROUP BY instrument_id, day",
        q.params,
    )
    return {(r["instrument_id"], _day(r["day"])): int(r["n"]) for r in rows}


def _day(text):
    return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=UTC)


async def market_kline_days_recorded_since(ch, since, end):
    q = Query()
    q.where(f"recorded_at >= {q.param(since, DT)}")
    q.where(f"open_time < {q.param(end, DT)}")
    rows = await ch.rows(
        f"SELECT DISTINCT instrument_id, toString(toDate(open_time)) AS day FROM {_table('mkt.klines_1m')} "
        f"WHERE {q.predicate()}",
        q.params,
    )
    return {(r["instrument_id"], _day(r["day"])) for r in rows}


async def market_rows_recorded_since(ch, collection, time_field, instrument_ids, since, start, end, filters):
    q = Query()
    _ids(q, instrument_ids)
    q.where(f"recorded_at >= {q.param(since, DT)}")
    q.where(f"`{time_field}` >= {q.param(start, DT)}")
    q.where(f"`{time_field}` < {q.param(end, DT)}")
    _filters(q, collection, filters)
    rows = await _select(ch, collection, q, f"instrument_id, `{time_field}`, storage_key")
    records = await _decode(collection, rows)
    return [(row["instrument_id"], rec, row["storage_key"]) for row, rec in zip(rows, records, strict=True)]


async def sampled_market_history(
    ch, collection, time_field, group_fields, instrument_ids, start, end, bucket_s
):
    q = Query()
    _ids(q, instrument_ids)
    q.where(f"`{time_field}` >= {q.param(start, DT)}")
    q.where(f"`{time_field}` < {q.param(end, DT)}")
    groups = ", ".join(["instrument_id", *(f"`{g}`" for g in group_fields)])
    bucket = f"intDiv(toUnixTimestamp(`{time_field}`), {int(bucket_s)})"
    rows = await _select(
        ch,
        collection,
        q,
        f"{groups}, {bucket}, `{time_field}` DESC, storage_key DESC",
        f"LIMIT 1 BY {groups}, {bucket}",
    )
    records = await _decode(collection, rows)
    return sorted(records, key=lambda r: r[time_field])


async def range_by_instrument_page(
    ch, collection, time_field, instrument_ids, start, end, filters, after, limit, levels
):
    q = Query()
    q.where(f"`{time_field}` IS NOT NULL")
    _ids(q, instrument_ids)
    if start is not None:
        q.where(f"`{time_field}` >= {q.param(parse_time(start), DT)}")
    if end is not None:
        q.where(f"`{time_field}` < {q.param(parse_time(end), DT)}")
    _filters(q, collection, filters)
    if after is not None:
        _after(
            q,
            ("instrument_id", after[0], "String"),
            (f"`{time_field}`", after[1], DT),
            ("storage_key", after[2], "String"),
        )
    selected = "*" if levels else "* EXCEPT (bids, asks)" if collection == "mkt.book_summaries" else "*"
    rows = await _select(
        ch, collection, q, f"instrument_id, `{time_field}`, storage_key", f"LIMIT {int(limit)}", selected
    )
    records = await _decode(collection, rows)
    return [
        (row["instrument_id"], rec, row["storage_key"], parse_time(row[time_field]))
        for row, rec in zip(rows, records, strict=True)
    ]


async def prune(ch, cutoff, preserved_days, *, book_cutoff=None, trades_cutoff=None):
    """Drop whole daily partitions older than the cutoffs, except preserved (MAJOR+) days;
    book summaries go at their own cutoff with no exception."""
    cutoffs = {collection: cutoff for collection in MARKET_TABLES}
    if book_cutoff is not None:
        cutoffs["mkt.book_summaries"] = book_cutoff
    if trades_cutoff is not None:
        cutoffs[TRADES] = trades_cutoff
    rows = await ch.rows(
        "SELECT DISTINCT database, table, partition FROM system.parts WHERE database = 'mkt' AND active"
    )
    keep = {day.strftime("%Y%m%d") for day in preserved_days}
    dropped = {}
    for row in rows:
        collection = "mkt." + row["table"]
        limit = cutoffs.get(collection)
        if limit is None:
            continue
        day = datetime.strptime(row["partition"], "%Y%m%d").replace(tzinfo=UTC)
        exempt = collection != "mkt.book_summaries" and row["partition"] in keep
        if day + timedelta(days=1) <= limit and not exempt:
            await ch.execute(f"ALTER TABLE {collection} DROP PARTITION {int(row['partition'])}")
            dropped[collection] = dropped.get(collection, 0) + 1
    return dropped


class MarketClientOwner(Protocol):
    @property
    def backend(self) -> Literal["memory", "postgres"]: ...

    clickhouse: ClickHouse | None


def install(repo: MarketClientOwner, settings: Settings) -> ClickHouse | None:
    """Give a production repository its ClickHouse client (never a replay database)."""
    url = settings.clickhouse_url
    if url is None or settings.replay:
        return None
    repo.clickhouse = ClickHouse(url, password=settings.tank_db_password)
    return repo.clickhouse


def client(repo: MarketClientOwner, collection: str) -> ClickHouse | None:
    """The ClickHouse client serving `collection` for this repository, else None.

    A database repository has no other store for the exchange series."""
    if collection not in MARKET_TABLES:
        return None
    ch = repo.clickhouse
    if ch is None and repo.backend == "postgres":
        raise RuntimeError(f"{collection} is stored in ClickHouse only; TANK_CLICKHOUSE_URL is not set")
    return ch


def _row(collection, record, key, version):
    row = {"storage_key": key, "_version": version}
    for name, kind in MARKET_COLUMNS[collection].items():
        value = record.get(name)
        if value is None:
            if kind == "text[]":
                row[name] = []
            continue
        if kind == "timestamptz":
            row[name] = parse_time(value).astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
        elif kind == "jsonb":
            row[name] = canonical_json(value)
        elif kind == "numeric":
            row[name] = str(value)
        elif kind == "text[]":
            row[name] = list(value)
        else:
            row[name] = value
    return row


NULL_REASONS = frozenset(
    {
        "NOT_SUPPORTED",
        "SOURCE_FAILED",
        "INCOMPLETE",
        "UNVERIFIED_SEMANTICS",
        "NOT_APPLICABLE",
        "NOT_ESTIMABLE",
        "PENDING",
    }
)
REASONED = {"mkt.flow_bars_1m": ("buy_usd", "sell_usd", "size_buckets")}


class RequiredFieldError(ValueError):
    def __init__(self, collection, field):
        super().__init__(f"{collection}: required_{field}")
        self.constraint = "required_" + field


def _check_required(collection, value):
    """Every required field is set; a missing USD conversion carries its own valid reason."""
    from tank.market_schema import MARKET_REQUIRED

    for field in MARKET_REQUIRED.get(collection, ()):
        if value.get(field) is None:
            raise RequiredFieldError(collection, field)
    reasons = value.get("null_reasons") or {}
    for field in REASONED.get(collection, ()):
        if value.get(field) is None and (
            not isinstance(reasons.get(field), str) or reasons[field] not in NULL_REASONS
        ):
            raise RequiredFieldError(collection, field)


async def put_batch(ch, collection, records, keys=None):
    """Write typed rows the way tank-capture does; return the rows that changed.

    A complete flow bar is never replaced by an unproven one, funding and ticker points
    merge into the stored row, and an unchanged fingerprint is not written again."""
    from tank.market_codec import _prepare_records, _unproves, decode, merge_point

    unique = _prepare_records(collection, records, keys, MARKET_COLUMNS[collection], False)
    if not unique:
        return []
    identity = MARKET_IDENTITIES[collection]
    values = {key: decode(collection, prepared) for key, prepared in unique.items()}

    def ident(key, value):
        return tuple(key if name == "storage_key" else value.get(name) for name in identity)

    def literal(name, value):
        if name != "storage_key" and MARKET_COLUMNS[collection][name] == "timestamptz":
            return f"toDateTime64('{_literal(parse_time(value))}', 6, 'UTC')"
        return _literal([value])[1:-1]

    tuples = ",".join(
        "("
        + ",".join(literal(name, part) for name, part in zip(identity, ident(key, value), strict=True))
        + ")"
        for key, value in values.items()
    )
    q = Query()
    q.where(f"({','.join(f'`{name}`' for name in identity)}) IN ({tuples})")
    stored = {}
    for row in await _select(ch, collection, q, "storage_key"):
        record = _record(collection, row)
        stored[ident(row["storage_key"], record)] = record
    changed, rows = [], []
    version = time.time_ns()
    for value in values.values():
        _check_required(collection, value)
    for key, value in values.items():
        old = stored.get(ident(key, value), {})
        if old and collection in ("mkt.funding_obs", "mkt.tickers_24h"):
            value = merge_point(collection, old, value)
        if old.get("semantic_fingerprint") == value["semantic_fingerprint"] or _unproves(
            collection, old, value
        ):
            continue
        changed.append(value)
        rows.append(_row(collection, value, key, version))
    await ch.insert(collection, rows)
    return changed


async def delete(ch, collection, key):
    q = Query()
    q.where(f"storage_key = {q.param(str(key), 'String')}")
    await ch.execute(f"DELETE FROM {collection} WHERE {q.predicate()}", q.params)
