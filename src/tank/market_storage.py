"""Typed market encoding and batched SQL writes (§14)."""

import asyncio
import copy
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Protocol, Self, TypeIs, overload

from psycopg import AsyncConnection, AsyncCursor, sql
from psycopg.types.json import Jsonb

from tank import clickhouse_market as chm
from tank.contracts.json import decode_json, encode_json
from tank.market_codec import (
    IdentityValue,
    StoredRow,
    StoredValue,
    _prepare_records,
    _timestamp,
    _unproves,
    decode,
    merge_point,
)
from tank.market_codec import encode as encode
from tank.market_codec import semantic_fingerprint as semantic_fingerprint
from tank.market_schema import MARKET_ALIASES, MARKET_COLUMNS, MARKET_IDENTITIES
from tank.models import Record
from tank.util import JsonInput, TimeInput, iso, jsonable, parse_time, utcnow

type SQLParameter = StoredValue | Jsonb | Sequence[SQLParameter]


class MarketReader(Protocol):
    @property
    def backend(self) -> Literal["memory", "postgres"]: ...

    clickhouse: chm.ClickHouse | None
    _market_write_clock: dict[tuple[str, str | None, str | None], datetime]

    def transaction(self) -> AbstractAsyncContextManager[Self]: ...

    def _exact_predicate(
        self, collection: str, filters: Record | None
    ) -> tuple[list[sql.Composable], list[SQLParameter]]: ...

    def iter_all(
        self, collection: str, *, filters: Record | None = None, batch_size: int = 1000
    ) -> AsyncIterator[Record]: ...

    def page_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        limit: int = 1000,
        after: tuple[TimeInput, str] | None = None,
    ) -> Awaitable[list[tuple[Record, tuple[str, str]]]]: ...

    def iter_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        batch_size: int = 10000,
    ) -> AsyncIterator[Record]: ...

    def iter_late_arrivals(
        self,
        collection: str,
        arrival_field: str,
        market_field: str,
        *,
        since: TimeInput | None = None,
        end: TimeInput | None = None,
        market_end: TimeInput,
        filters: Record | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[Record]: ...

    def iter_market_projection(
        self,
        collection: str,
        fields: Sequence[str],
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: Record | None = None,
        instrument_ids: Collection[str] | None = None,
        assembled_at: TimeInput | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[Record]: ...


class MemoryMarketStore(MarketReader, Protocol):
    @property
    def backend(self) -> Literal["memory"]: ...

    _data: dict[str, dict[str, Record]]


class PostgresMarketStore(MarketReader, Protocol):
    @property
    def backend(self) -> Literal["postgres"]: ...

    def _conn(self) -> AsyncConnection[StoredRow]: ...


type MarketStore = MemoryMarketStore | PostgresMarketStore


def _is_memory(store: MarketStore) -> TypeIs[MemoryMarketStore]:
    return store.backend == "memory"


async def put_market_batch(
    self: MarketStore,
    collection: str,
    records: Iterable[Mapping[str, JsonInput]],
    *,
    keys: Iterable[str] | None = None,
) -> list[Record]:
    records = list(records)
    keys = None if keys is None else list(keys)

    if (ch := chm.client(self, collection)) is not None:
        changed = []
        for at in range(0, len(records), WRITE_CHUNK):
            part = None if keys is None else keys[at : at + WRITE_CHUNK]
            changed += await chm.put_batch(ch, collection, records[at : at + WRITE_CHUNK], part)
        _note_writes(self, collection, changed)
        return changed
    if _is_memory(self) or len(records) <= WRITE_CHUNK:
        return await _put_market_batch(self, collection, records, keys=keys)

    changed = []
    for at in range(0, len(records), WRITE_CHUNK):
        part = None if keys is None else keys[at : at + WRITE_CHUNK]
        changed += await _put_market_batch(self, collection, records[at : at + WRITE_CHUNK], keys=part)
    return changed


YIELD_ROWS = 1000

WRITE_CHUNK = 500

LEVELS = ("bids", "asks")


def _note_writes(self: MarketReader, collection: str, rows: Sequence[Record]) -> None:
    """When rows of a scope last changed: a dependency head that saw every row recorded by
    its assembly clock stays valid for a later clock until its scope is written again."""
    if not rows:
        return
    clock = self._market_write_clock
    now = utcnow()
    clock[collection, None, None] = now
    for row in rows:
        for scope in ("instrument_id", "token_id"):
            identity = row.get(scope)
            if identity is not None:
                if not isinstance(identity, str):
                    raise ValueError(f"{scope} requires text")
                clock[collection, scope, identity] = now


def _json_as_text(cursor: AsyncCursor[StoredRow]) -> AsyncCursor[StoredRow]:
    """A cursor whose json/jsonb columns arrive as text, to be parsed off the event loop."""
    from psycopg.types.string import TextLoader

    for name in ("json", "jsonb"):
        cursor.adapters.register_loader(name, TextLoader)
    return cursor


def _load_json(collection: str, row: StoredRow) -> StoredRow:
    """Worker thread: parse the jsonb columns of a row read through _json_as_text."""
    for name, kind in MARKET_COLUMNS[collection].items():
        value = row.get(name)
        if kind == "jsonb" and isinstance(value, str):
            row[name] = decode_json(value)
    return row


def _instrument_rows(collection: str, rows: Sequence[StoredRow]) -> list[tuple[str, Record, str]]:
    return [
        (
            _text(row, "instrument_id"),
            decode(collection, _load_json(collection, row)),
            _text(row, "storage_key"),
        )
        for row in rows
    ]


async def _put_market_batch(
    self: MarketStore,
    collection: str,
    records: Sequence[Mapping[str, JsonInput]],
    *,
    keys: Sequence[str] | None = None,
) -> list[Record]:

    columns = MARKET_COLUMNS[collection]

    records = list(records)
    if not records:
        return []
    if _is_memory(self):
        unique = _prepare_records(collection, records, keys)
        changed = []
        async with self.transaction():
            memory_rows = self._data.setdefault(collection, {})
            for key, row in unique.items():
                value = decode(collection, row)
                previous = key if key in memory_rows else None
                if key not in memory_rows:
                    identity = tuple(value.get(n) for n in MARKET_IDENTITIES[collection])
                    previous = (
                        next(
                            (
                                k
                                for k, v in memory_rows.items()
                                if tuple(v.get(n) for n in MARKET_IDENTITIES[collection]) == identity
                            ),
                            None,
                        )
                        if "storage_key" not in MARKET_IDENTITIES[collection]
                        else None
                    )
                old = None if previous is None else memory_rows[previous]
                if old is not None and collection in ("mkt.funding_obs", "mkt.tickers_24h"):
                    value = merge_point(collection, old, value)
                if old is None or (
                    old.get("semantic_fingerprint") != value["semantic_fingerprint"]
                    and not _unproves(collection, old, value)
                ):
                    if previous is not None and previous != key:
                        memory_rows.pop(previous)
                    memory_rows[key] = value
                    changed.append(value.copy())
        _note_writes(self, collection, changed)
        return changed
    names = ["storage_key", *columns]

    def prepare() -> list[list[StoredValue]]:

        unique = _prepare_records(collection, records, keys)
        arrays = []
        for name in names:
            kind = "text" if name == "storage_key" else columns[name]
            values = []
            for key, row in unique.items():
                value = key if name == "storage_key" else row.get(name)
                values.append(
                    encode_json(jsonable(value)).decode()
                    if kind in ("jsonb", "text[]") and value is not None
                    else value
                )
            arrays.append(values)
        return arrays

    arrays = await asyncio.to_thread(prepare)
    selection = sql.SQL(",").join(
        sql.SQL(
            "CASE WHEN u.{} IS NULL THEN NULL ELSE ARRAY(SELECT jsonb_array_elements_text(u.{})) END"
        ).format(sql.Identifier(n), sql.Identifier(n))
        if columns.get(n) == "text[]"
        else sql.SQL("u.{}").format(sql.Identifier(n))
        for n in names
    )
    unnest_args = sql.SQL(",").join(
        sql.SQL("%s::{}[]").format(
            sql.SQL("jsonb" if columns.get(n) == "text[]" else "text" if n == "storage_key" else columns[n])
        )
        for n in names
    )
    source = sql.SQL("SELECT {} FROM unnest({}) AS u({})").format(
        selection, unnest_args, sql.SQL(",").join(map(sql.Identifier, names))
    )
    table = sql.Identifier(*collection.split("."))
    identity = MARKET_IDENTITIES[collection]
    assignments = sql.SQL(",").join(
        sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(n), sql.Identifier(n))
        for n in names
        if n not in identity
    )
    query = sql.SQL(
        "INSERT INTO {} AS current ({}) {} ON CONFLICT ({}) DO UPDATE SET {} WHERE current.semantic_fingerprint IS DISTINCT FROM EXCLUDED.semantic_fingerprint RETURNING *"
    ).format(
        table,
        sql.SQL(",").join(map(sql.Identifier, names)),
        source,
        sql.SQL(",").join(map(sql.Identifier, identity)),
        assignments,
    )
    async with self.transaction():
        cursor = _json_as_text(self._conn().cursor())
        await cursor.execute(query, arrays)
        rows = await cursor.fetchall()
    changed = await asyncio.to_thread(lambda: [decode(collection, _load_json(collection, r)) for r in rows])
    _note_writes(self, collection, changed)
    return changed


def _text(row: Mapping[str, StoredValue], field: str) -> str:
    value = row[field]
    if not isinstance(value, str):
        raise ValueError(f"{field} requires text")
    return value


def _group_value(value: StoredValue) -> IdentityValue | None:
    if value is None or isinstance(value, (str, int, float, datetime, Decimal)):
        return value
    raise ValueError("Market groups require scalar values")


def _decimal(value: StoredValue) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Market numeric evidence requires a decimal scalar")
    return Decimal(str(value))


async def latest_market_records(
    self: MarketStore,
    collection: str,
    *,
    group_by: Sequence[str] = ("instrument_id",),
    time_field: str = "ts",
    filters: Record | None = None,
    cutoff: TimeInput | None = None,
    assembled_at: TimeInput | None = None,
    start: TimeInput | None = None,
    instrument_ids: Collection[str] | None = None,
) -> list[Record]:
    """Latest point per typed group after applying semantic/availability bounds.

    Unknown legacy recorded_at remains visible for consumers to classify as unknown/stale;
    this read does not invent historical availability or precision.
    """

    columns = MARKET_COLUMNS.get(collection)
    if columns is None or not group_by:
        raise ValueError("Latest market records require a market collection and typed grouping")
    for field in (*group_by, time_field):
        if field not in columns:
            raise ValueError(f"Unknown market column {field}")
    if columns[time_field] != "timestamptz":
        raise ValueError("Latest market time_field must be a typed timestamp")
    if instrument_ids is not None:
        if isinstance(instrument_ids, (str, bytes)) or "instrument_id" not in columns:
            raise ValueError("instrument_ids requires a typed instrument scope")
        instrument_ids = list(instrument_ids)
        if any(not isinstance(value, str) for value in instrument_ids):
            raise ValueError("instrument_ids must contain instrument identifier strings")
        instrument_ids = sorted(set(instrument_ids))
        if not instrument_ids:
            return []
    cutoff = parse_time(cutoff) if cutoff is not None else None
    start = parse_time(start) if start is not None else None
    assembled_at = parse_time(assembled_at) if assembled_at is not None else None

    if (ch := chm.client(self, collection)) is not None:
        return await chm.latest_market_records(
            ch,
            collection,
            group_by=group_by,
            time_field=time_field,
            filters=filters,
            cutoff=cutoff,
            assembled_at=assembled_at,
            start=start,
            instrument_ids=instrument_ids,
        )
    clauses, args = self._exact_predicate(collection, filters)
    for bound, operator in ((start, ">="), (cutoff, "<=")):
        if bound is not None:
            clauses.append(sql.SQL("{} " + operator + " %s").format(sql.Identifier(time_field)))
            args.append(bound)
    if assembled_at is not None:
        clauses.append(sql.SQL("(recorded_at IS NULL OR recorded_at<=%s)"))
        args.append(assembled_at)
    if _is_memory(self):
        grouped: dict[tuple[IdentityValue | None, ...], Record] = {}
        async for row in self.iter_all(collection, filters=filters):
            if instrument_ids is not None and row.get("instrument_id") not in instrument_ids:
                continue
            at = _timestamp(row[time_field]) if row.get(time_field) is not None else None
            arrival = _timestamp(row["recorded_at"]) if row.get("recorded_at") is not None else None
            if (
                cutoff is not None
                and (at is None or at > cutoff)
                or start is not None
                and (at is None or at < start)
                or assembled_at is not None
                and arrival is not None
                and arrival > assembled_at
            ):
                continue
            key = tuple(_group_value(row.get(k)) for k in group_by)
            previous = grouped.get(key)

            if (
                previous is None
                or (
                    at is not None
                    and (previous.get(time_field) is None or at >= _timestamp(previous[time_field]))
                )
                or at is None
                and previous.get(time_field) is None
            ):
                grouped[key] = row
        return list(grouped.values())
    groups = sql.SQL(",").join(map(sql.Identifier, group_by))
    if instrument_ids is not None and tuple(group_by) == ("instrument_id",):
        clauses.append(sql.SQL("instrument_id = scope.instrument_id"))
        bounded = start is not None or cutoff is not None
        unique_time = bounded and set(MARKET_IDENTITIES[collection]) <= {*group_by, time_field}
        tie_breaker = sql.SQL("") if unique_time else sql.SQL(",storage_key DESC")
        query = sql.SQL(
            "SELECT point.* FROM unnest(%s::text[]) AS scope(instrument_id) "
            "CROSS JOIN LATERAL (SELECT * FROM {} WHERE {} "
            "ORDER BY {} {}{} LIMIT 1) AS point"
        ).format(
            sql.Identifier(*collection.split(".")),
            sql.SQL(" AND ").join(clauses),
            sql.Identifier(time_field),
            sql.SQL("DESC" if bounded else "DESC NULLS LAST"),
            tie_breaker,
        )
        args.insert(0, instrument_ids)
    else:
        if instrument_ids is not None:
            clauses.append(sql.SQL("instrument_id = ANY(%s::text[])"))
            args.append(instrument_ids)
        query = sql.SQL(
            "SELECT DISTINCT ON ({}) * FROM {} WHERE {} ORDER BY {},{} DESC NULLS LAST,storage_key DESC"
        ).format(
            groups,
            sql.Identifier(*collection.split(".")),
            sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE"),
            groups,
            sql.Identifier(time_field),
        )
    async with self.transaction():
        rows = await (await self._conn().execute(query, args)).fetchall()
    return [decode(collection, row) for row in rows]


@overload
async def market_dependency_head(
    self: MarketStore,
    collection: str,
    semantic_field: str,
    cutoff: TimeInput,
    *,
    start: TimeInput | None = None,
    filters: Record | None = None,
    instrument_ids: Collection[str] | None = None,
    assembled_at: TimeInput | None = None,
    order: str = "availability",
    orders: None = None,
) -> Record | None: ...


@overload
async def market_dependency_head(
    self: MarketStore,
    collection: str,
    semantic_field: str,
    cutoff: TimeInput,
    *,
    start: TimeInput | None = None,
    filters: Record | None = None,
    instrument_ids: Collection[str] | None = None,
    assembled_at: TimeInput | None = None,
    order: str = "availability",
    orders: Collection[str],
) -> dict[str, Record | None]: ...


async def market_dependency_head(
    self: MarketStore,
    collection: str,
    semantic_field: str,
    cutoff: TimeInput,
    *,
    start: TimeInput | None = None,
    filters: Record | None = None,
    instrument_ids: Collection[str] | None = None,
    assembled_at: TimeInput | None = None,
    order: str = "availability",
    orders: Collection[str] | None = None,
) -> Record | dict[str, Record | None] | None:
    """Availability or semantic-boundary head within a scope, returning metadata only.
    With `orders`, every listed head comes from one round trip as {order: head}.

    A correction to an old point participates by its actual recorded_at, not by its
    position in semantic history. Unknown legacy availability is explicitly excluded.
    Bounds are inclusive; an empty instrument scope never broadens into all instruments.
    """

    if orders is not None and not orders:
        return {}
    if any(
        o not in ("availability", "semantic_latest", "semantic_earliest") for o in (order,) if orders is None
    ) or (
        orders is not None
        and any(o not in ("availability", "semantic_latest", "semantic_earliest") for o in orders)
    ):
        raise ValueError("Unsupported market dependency head order")
    if orders is not None and _is_memory(self):
        return {
            o: await market_dependency_head(
                self,
                collection,
                semantic_field,
                cutoff,
                start=start,
                filters=filters,
                instrument_ids=instrument_ids,
                assembled_at=assembled_at,
                order=o,
            )
            for o in orders
        }
    ascending = order == "semantic_earliest"
    columns = MARKET_COLUMNS.get(collection)
    semantic_field = MARKET_ALIASES.get(collection, {}).get(semantic_field, semantic_field)
    if (
        columns is None
        or columns.get(semantic_field) != "timestamptz"
        or columns.get("recorded_at") != "timestamptz"
    ):
        raise ValueError("Dependency heads require typed market semantic and availability timestamps")
    if instrument_ids is not None:
        if isinstance(instrument_ids, (str, bytes)):
            raise ValueError("instrument_ids must contain instrument identifier strings")
        instrument_ids = list(instrument_ids)
        if any(not isinstance(value, str) for value in instrument_ids):
            raise ValueError("instrument_ids must contain instrument identifier strings")
        instrument_ids = sorted(set(instrument_ids))
        if not instrument_ids:
            return None if orders is None else dict.fromkeys(orders)
        if "instrument_id" not in columns:
            raise ValueError(f"{collection} has no typed instrument_id")
    cutoff = parse_time(cutoff)
    start = parse_time(start) if start is not None else None
    assembled_at = parse_time(assembled_at) if assembled_at is not None else None

    def metadata(row: Mapping[str, StoredValue]) -> Record:
        return {
            "storage_key": _text(row, "storage_key"),
            "recorded_at": iso(_timestamp(row["recorded_at"])),
            "semantic_fingerprint": None
            if row.get("semantic_fingerprint") is None
            else _text(row, "semantic_fingerprint"),
            semantic_field: iso(_timestamp(row[semantic_field])),
        }

    if _is_memory(self):
        self._exact_predicate(collection, filters)
        selected: tuple[tuple[datetime, str], Record] | None = None
        async with self.transaction():
            for key, row in self._data.get(collection, {}).items():
                raw_arrival, raw_semantic = row.get("recorded_at"), row.get(semantic_field)
                if raw_arrival is None or raw_semantic is None:
                    continue
                arrival, semantic = _timestamp(raw_arrival), _timestamp(raw_semantic)
                if (
                    semantic > cutoff
                    or start is not None
                    and semantic < start
                    or assembled_at is not None
                    and arrival > assembled_at
                ):
                    continue
                if instrument_ids is not None and row.get("instrument_id") not in instrument_ids:
                    continue
                if any(
                    row.get(MARKET_ALIASES.get(collection, {}).get(k, k)) != v
                    for k, v in (filters or {}).items()
                ):
                    continue
                candidate = (arrival if order == "availability" else semantic, key)
                if selected is None or (candidate < selected[0] if ascending else candidate > selected[0]):
                    selected = (candidate, metadata(dict(row, storage_key=key)))
        return selected[1] if selected else None

    if (ch := chm.client(self, collection)) is not None:
        wanted = [order] if orders is None else list(orders)
        found = await chm.market_dependency_head(
            ch,
            collection,
            semantic_field,
            cutoff,
            start=start,
            filters=filters,
            instrument_ids=instrument_ids,
            assembled_at=assembled_at,
            wanted=wanted,
        )
        return found.get(order) if orders is None else {o: found.get(o) for o in wanted}
    clauses, args = self._exact_predicate(collection, filters)
    clauses.extend(
        [sql.SQL("recorded_at IS NOT NULL"), sql.SQL("{} <= %s").format(sql.Identifier(semantic_field))]
    )
    args.append(cutoff)
    if start is not None:
        clauses.append(sql.SQL("{} >= %s").format(sql.Identifier(semantic_field)))
        args.append(start)
    if assembled_at is not None:
        clauses.append(sql.SQL("recorded_at <= %s"))
        args.append(assembled_at)
    if instrument_ids is not None:
        if len(instrument_ids) == 1:
            clauses.append(sql.SQL("instrument_id = %s"))
            args.append(instrument_ids[0])
        else:
            clauses.append(sql.SQL("instrument_id = ANY(%s::text[])"))
            args.append(instrument_ids)
    wanted = [order] if orders is None else list(orders)

    def head(o: str) -> sql.Composed:
        direction = sql.SQL("ASC" if o == "semantic_earliest" else "DESC")
        return sql.SQL(
            "(SELECT {} AS head,storage_key,recorded_at,semantic_fingerprint,{} FROM {} WHERE {}"
            " ORDER BY {} {},storage_key {} LIMIT 1)"
        ).format(
            sql.Literal(o),
            sql.Identifier(semantic_field),
            sql.Identifier(*collection.split(".")),
            sql.SQL(" AND ").join(clauses),
            sql.Identifier("recorded_at" if o == "availability" else semantic_field),
            direction,
            direction,
        )

    query = sql.SQL(" UNION ALL ").join(head(o) for o in wanted)
    async with self.transaction():
        rows = await (await self._conn().execute(query, args * len(wanted))).fetchall()
    found = {_text(row, "head"): metadata(row) for row in rows}
    return found.get(order) if orders is None else {o: found.get(o) for o in wanted}


async def iter_market_projection(
    self: MarketStore,
    collection: str,
    fields: Sequence[str],
    time_field: str,
    *,
    start: TimeInput | None = None,
    end: TimeInput | None = None,
    filters: Record | None = None,
    instrument_ids: Collection[str] | None = None,
    assembled_at: TimeInput | None = None,
    batch_size: int = 1000,
) -> AsyncIterator[Record]:
    """Stream explicitly selected typed columns, never full market records.

    Semantic bounds are [start,end). Unknown legacy availability is retained explicitly.
    Each keyset page releases its connection before yielding any projected row.
    """

    columns = MARKET_COLUMNS.get(collection)
    aliases = MARKET_ALIASES.get(collection, {})
    time_field = aliases.get(time_field, time_field)
    fields = list(fields)
    if columns is None or columns.get(time_field) != "timestamptz":
        raise ValueError("Market projection requires a typed market time field")
    if not fields or len(fields) > 100 or len(set(fields)) != len(fields) or batch_size < 1:
        raise ValueError("Projection requires unique fields and a positive batch size")
    selected = {field: aliases.get(field, field) for field in fields}
    if any(field != "storage_key" and field not in columns for field in selected.values()):
        raise ValueError("Unknown typed market projection field")
    if instrument_ids is not None:
        if isinstance(instrument_ids, (str, bytes)):
            raise ValueError("instrument_ids must contain identifier strings")
        instrument_ids = list(instrument_ids)
        if any(not isinstance(value, str) for value in instrument_ids):
            raise ValueError("instrument_ids must contain identifier strings")
        if not instrument_ids:
            return
        if "instrument_id" not in columns:
            raise ValueError("Collection lacks instrument_id")
    start = parse_time(start) if start is not None else None
    end = parse_time(end) if end is not None else None
    assembled_at = parse_time(assembled_at) if assembled_at is not None else None

    self._exact_predicate(collection, filters)

    def project(row: Record, key: str) -> Record:
        result = {}
        for requested, field in selected.items():
            value = key if field == "storage_key" else row.get(field)
            kind = columns.get(field)
            if value is not None and kind == "timestamptz":
                value = iso(_timestamp(value))
            elif value is not None and kind == "numeric":
                value = str(value)
            result[requested] = jsonable(value)
        return result

    after = None

    ch = chm.client(self, collection)
    while True:
        if ch is not None:
            page = await chm.projection_page(
                ch,
                collection,
                selected,
                time_field,
                start=start,
                end=end,
                filters=filters,
                instrument_ids=instrument_ids,
                assembled_at=assembled_at,
                after=after,
                limit=batch_size,
            )
        elif not _is_memory(self):
            clauses, args = self._exact_predicate(collection, filters)
            clauses.append(sql.SQL("{} IS NOT NULL").format(sql.Identifier(time_field)))
            for bound, operator in ((start, ">="), (end, "<")):
                if bound is not None:
                    clauses.append(sql.SQL("{} " + operator + " %s").format(sql.Identifier(time_field)))
                    args.append(bound)
            if assembled_at is not None:
                clauses.append(sql.SQL("(recorded_at IS NULL OR recorded_at<=%s)"))
                args.append(assembled_at)
            if instrument_ids is not None:
                clauses.append(sql.SQL("instrument_id=ANY(%s::text[])"))
                args.append(instrument_ids)
            if after is not None:
                clauses.append(sql.SQL("({},storage_key)>(%s,%s)").format(sql.Identifier(time_field)))
                args.extend(after)
            expressions = sql.SQL(",").join(
                sql.SQL("{} AS {}").format(sql.Identifier(field), sql.Identifier(requested))
                for requested, field in selected.items()
            )
            query = sql.SQL(
                "SELECT {},{} AS _cursor_time,storage_key AS _cursor_key FROM {} WHERE {} ORDER BY {},storage_key LIMIT %s"
            ).format(
                expressions,
                sql.Identifier(time_field),
                sql.Identifier(*collection.split(".")),
                sql.SQL(" AND ").join(clauses),
                sql.Identifier(time_field),
            )
            async with self.transaction():
                raw = await (await self._conn().execute(query, [*args, batch_size])).fetchall()

            def convert(raw: Sequence[StoredRow] = raw) -> list[tuple[datetime, str, Record]]:
                page = []
                for row in raw:
                    result: Record = {}
                    for requested, field in selected.items():
                        value = row[requested]
                        kind = columns.get(field)
                        if value is not None and kind == "timestamptz":
                            value = iso(_timestamp(value))
                        elif value is not None and kind == "numeric":
                            value = str(value)
                        result[requested] = jsonable(value)
                    page.append((_timestamp(row["_cursor_time"]), _text(row, "_cursor_key"), result))
                return page

            page = await asyncio.to_thread(convert) if len(raw) > YIELD_ROWS else convert()
        else:
            page = []
            async with self.transaction():
                for key, row in self._data.get(collection, {}).items():
                    at = _timestamp(row[time_field]) if row.get(time_field) is not None else None
                    arrival = _timestamp(row["recorded_at"]) if row.get("recorded_at") is not None else None
                    if (
                        at is None
                        or start is not None
                        and at < start
                        or end is not None
                        and at >= end
                        or assembled_at is not None
                        and arrival is not None
                        and arrival > assembled_at
                    ):
                        continue
                    if after and (at, key) <= after:
                        continue
                    if instrument_ids is not None and row.get("instrument_id") not in instrument_ids:
                        continue
                    if any(row.get(aliases.get(k, k)) != v for k, v in (filters or {}).items()):
                        continue
                    page.append((at, key, project(row, key)))
            page.sort(key=lambda value: value[:2])
            page = page[:batch_size]
        for _, _, row in page:
            yield row
        if len(page) < batch_size:
            return
        after = page[-1][:2]


async def market_series_counts(
    self: MarketStore,
    instrument_ids: Collection[str],
    start: TimeInput,
    cutoff: TimeInput,
    assembled_at: TimeInput | None = None,
) -> dict[str, int]:
    """Count complete finite positive closes of closed native 1m bars per instrument."""
    if isinstance(instrument_ids, (str, bytes)):
        raise TypeError("instrument_ids must be identifiers")
    instrument_ids = list(instrument_ids)
    if any(not isinstance(value, str) for value in instrument_ids):
        raise TypeError("instrument_ids must be identifiers")
    instrument_ids = sorted(set(instrument_ids))
    counts = dict.fromkeys(instrument_ids, 0)
    if not counts:
        return counts
    start, cutoff = parse_time(start), parse_time(cutoff)
    assembled_at = parse_time(assembled_at) if assembled_at is not None else None

    if (ch := chm.client(self, "mkt.klines_1m")) is not None:
        counts.update(await chm.market_series_counts(ch, instrument_ids, start, cutoff, assembled_at))
        return counts
    async for row in self.iter_market_projection(
        "mkt.klines_1m",
        ["instrument_id", "open_time", "c", "interval_s"],
        "open_time",
        start=start,
        end=cutoff,
        filters={"complete": True, "interval_s": 60},
        instrument_ids=instrument_ids,
        assembled_at=assembled_at,
    ):
        close = _decimal(row["c"]) if row["c"] is not None else None
        if (
            close is not None
            and close.is_finite()
            and close > 0
            and _timestamp(row["open_time"]) + timedelta(seconds=60) <= cutoff
        ):
            counts[_text(row, "instrument_id")] += 1
    return counts


async def latest_market_trades(
    repo: MarketStore, instrument_ids: Collection[str], *, start: TimeInput
) -> list[Record]:
    """Newest flow bar holding a print, per instrument: its `last_trade` resumes capture."""
    instrument_ids = sorted(set(instrument_ids))
    start = parse_time(start)
    if not instrument_ids:
        return []

    if (ch := chm.client(repo, "mkt.flow_bars_1m")) is not None:
        return await chm.latest_market_trades(ch, instrument_ids, start)
    latest: dict[str, Record] = {}
    async for row in repo.iter_all("mkt.flow_bars_1m"):
        iid = _text(row, "instrument_id")
        if iid not in instrument_ids or not row.get("last_trade") or _timestamp(row["minute_start"]) < start:
            continue
        if iid not in latest or _timestamp(row["minute_start"]) > _timestamp(latest[iid]["minute_start"]):
            latest[iid] = row
    return [{"instrument_id": iid, "last_trade": row["last_trade"]} for iid, row in sorted(latest.items())]


async def market_series_day_counts(
    repo: MarketStore,
    instrument_ids: Collection[str],
    start: TimeInput,
    end: TimeInput,
    assembled_at: TimeInput | None = None,
) -> dict[tuple[str, datetime], int]:
    """Complete finite positive 1m closes per (instrument, UTC day) for whole days in [start, end)."""
    instrument_ids = sorted(set(instrument_ids))
    start, end = parse_time(start), parse_time(end)
    counts: dict[tuple[str, datetime], int] = {}
    if not instrument_ids or start >= end:
        return counts

    if (ch := chm.client(repo, "mkt.klines_1m")) is not None:
        return await chm.market_series_day_counts(
            ch, instrument_ids, start, end, parse_time(assembled_at) if assembled_at is not None else None
        )
    async for row in repo.iter_market_projection(
        "mkt.klines_1m",
        ["instrument_id", "open_time", "c", "interval_s"],
        "open_time",
        start=start,
        end=end,
        filters={"complete": True, "interval_s": 60},
        instrument_ids=instrument_ids,
        assembled_at=assembled_at,
    ):
        close = _decimal(row["c"]) if row["c"] is not None else None
        opened = _timestamp(row["open_time"])
        if close is not None and close.is_finite() and close > 0 and opened < end:
            day = opened.replace(hour=0, minute=0, second=0, microsecond=0)
            counts[_text(row, "instrument_id"), day] = counts.get((_text(row, "instrument_id"), day), 0) + 1
    return counts


async def market_kline_days_recorded_since(
    repo: MarketStore, since: TimeInput, end: TimeInput
) -> set[tuple[str, datetime]]:
    """(instrument, UTC day) of every 1m kline in [.., end) recorded at or after `since`."""
    since, end = parse_time(since), parse_time(end)
    days: set[tuple[str, datetime]] = set()

    if (ch := chm.client(repo, "mkt.klines_1m")) is not None:
        return await chm.market_kline_days_recorded_since(ch, since, end)
    async for row in repo.iter_late_arrivals(
        "mkt.klines_1m", "recorded_at", "open_time", since=since, market_end=end
    ):
        opened = _timestamp(row["open_time"])
        if opened < end:
            days.add((_text(row, "instrument_id"), opened.replace(hour=0, minute=0, second=0, microsecond=0)))
    return days


async def market_rows_recorded_since(
    repo: MarketStore,
    collection: str,
    time_field: str,
    instrument_ids: Collection[str],
    since: TimeInput,
    start: TimeInput,
    end: TimeInput,
    filters: Record | None = None,
) -> list[tuple[str, Record, str]]:
    """Rows of the instruments in [start, end) recorded at or after `since` (rewritten or
    late), as (instrument_id, record, key), JSON decoded in a worker thread."""

    instrument_ids = sorted(set(instrument_ids))
    since, start, end = parse_time(since), parse_time(start), parse_time(end)
    if not instrument_ids or start >= end:
        return []

    if (ch := chm.client(repo, collection)) is not None:
        return await chm.market_rows_recorded_since(
            ch, collection, time_field, instrument_ids, since, start, end, filters
        )
    if _is_memory(repo):
        found: list[tuple[str, Record, str]] = []
        async with repo.transaction():
            for key, row in repo._data.get(collection, {}).items():
                iid = row.get("instrument_id")
                arrival = row.get("recorded_at")
                semantic = row.get(time_field)
                if iid is None or arrival is None or semantic is None:
                    continue
                if not isinstance(iid, str):
                    raise ValueError("instrument_id requires text")
                if iid not in instrument_ids or _timestamp(arrival) < since:
                    continue
                if any(name not in row or row[name] != value for name, value in (filters or {}).items()):
                    continue
                if start <= _timestamp(semantic) < end:
                    found.append((iid, copy.deepcopy(row), key))
        return sorted(found, key=lambda entry: (entry[0], _timestamp(entry[1][time_field]), entry[2]))

    clauses = [
        sql.SQL("instrument_id = ANY(%s::text[])"),
        sql.SQL("recorded_at >= %s"),
        sql.SQL("{} >= %s").format(sql.Identifier(time_field)),
        sql.SQL("{} < %s").format(sql.Identifier(time_field)),
    ]
    args: list[SQLParameter] = [instrument_ids, since, start, end]
    if filters:
        extra, values = repo._exact_predicate(collection, filters)
        clauses.extend(extra)
        args.extend(values)
    query = sql.SQL("SELECT * FROM {} WHERE {} ORDER BY instrument_id, {}, storage_key").format(
        sql.Identifier(*collection.split(".")), sql.SQL(" AND ").join(clauses), sql.Identifier(time_field)
    )
    async with repo.transaction():
        cursor = _json_as_text(repo._conn().cursor())
        await cursor.execute(query, args)
        rows = await cursor.fetchall()
    return await asyncio.to_thread(
        lambda: [
            (
                _text(row, "instrument_id"),
                decode(collection, _load_json(collection, row)),
                _text(row, "storage_key"),
            )
            for row in rows
        ]
    )


async def sampled_market_history(
    repo: MarketStore,
    collection: str,
    time_field: str,
    group_fields: Sequence[str],
    instrument_ids: Collection[str],
    start: TimeInput,
    end: TimeInput,
    bucket_s: int,
) -> list[Record]:
    """The last point per (instrument, group, bucket_s bucket) in [start, end).

    Older history behind normal ranges is read at the base-tier cadence: denser hot
    points matter only near the event, which the caller reads in full.
    """

    instrument_ids = sorted(set(instrument_ids))
    start, end = parse_time(start), parse_time(end)
    if not instrument_ids or start >= end:
        return []

    if (ch := chm.client(repo, collection)) is not None:
        return await chm.sampled_market_history(
            ch, collection, time_field, group_fields, instrument_ids, start, end, bucket_s
        )
    if _is_memory(repo):
        latest: dict[tuple[IdentityValue | None, ...], Record] = {}
        for iid in instrument_ids:
            async for row in repo.iter_range(
                collection, time_field, start=start, end=end, filters={"instrument_id": iid}
            ):
                at = _timestamp(row[time_field])
                if not start <= at < end:
                    continue
                key = (
                    iid,
                    *(_group_value(row.get(g)) for g in group_fields),
                    int(at.timestamp()) // bucket_s,
                )
                if key not in latest or at >= _timestamp(latest[key][time_field]):
                    latest[key] = row
        return sorted(latest.values(), key=lambda row: _timestamp(row[time_field]))
    groups = [sql.Identifier("instrument_id"), *map(sql.Identifier, group_fields)]
    bucket = sql.SQL("floor(extract(epoch from {}) / {})").format(
        sql.Identifier(time_field), sql.Literal(int(bucket_s))
    )
    query = sql.SQL(
        "SELECT DISTINCT ON ({groups}, {bucket}) * FROM {table} WHERE instrument_id = ANY(%s::text[]) "
        "AND {ts} >= %s AND {ts} < %s ORDER BY {groups}, {bucket}, {ts} DESC"
    ).format(
        groups=sql.SQL(",").join(groups),
        bucket=bucket,
        table=sql.Identifier(*collection.split(".")),
        ts=sql.Identifier(time_field),
    )
    args = [instrument_ids, start, end]
    async with repo.transaction():
        cursor = _json_as_text(repo._conn().cursor())
        await cursor.execute(query, args)
        rows = await cursor.fetchall()

    return await asyncio.to_thread(
        lambda: sorted(
            (decode(collection, _load_json(collection, row)) for row in rows),
            key=lambda row: _timestamp(row[time_field]),
        )
    )


async def market_range_by_instrument(
    repo: MarketStore,
    collection: str,
    time_field: str,
    instrument_ids: Collection[str],
    start: TimeInput | None,
    end: TimeInput | None,
    filters: Record | None = None,
    *,
    batch_size: int = 1000,
    keep: Callable[[str, list[Record]], None] | None = None,
    level_rows: int | None = None,
    keys_out: dict[str, list[str]] | None = None,
) -> dict[str, list[Record]]:
    """Rows of several instruments in [start, end), paged on one key for all of them.

    Per instrument the rows come in the order iter_range returns them for that instrument
    alone ((time_field, storage_key)), so callers can replace a per-instrument loop of range
    reads (e.g. 9 collections x the instruments of a token) without changing any output.
    Pages are keyset-paged like iter_range; `keep(instrument_id, rows)` runs after each
    appended row so a caller can shrink older rows while they stream (book levels).
    1k-row pages (wide rows: 500) bound the transient of one page and the loop time of its fetch.
    `keys_out`, a dict, receives each instrument's row keys in the order of its rows.
    """

    instrument_ids = sorted(set(instrument_ids))
    out: dict[str, list[Record]] = {iid: [] for iid in instrument_ids}
    if not instrument_ids:
        return out

    if (ch := chm.client(repo, collection)) is not None:
        after = None
        while True:
            page = await chm.range_by_instrument_page(
                ch,
                collection,
                time_field,
                instrument_ids,
                start,
                end,
                filters,
                after,
                batch_size,
                levels=False,
            )
            for n, (iid, record, key, _) in enumerate(page, 1):
                if level_rows is not None:
                    for name in LEVELS:
                        record.pop(name, None)
                out[iid].append(record)
                if keys_out is not None:
                    keys_out.setdefault(iid, []).append(key)
                if keep is not None:
                    keep(iid, out[iid])
                if n % YIELD_ROWS == 0:
                    await asyncio.sleep(0)
            if len(page) < batch_size:
                return out
            iid, _, key, at = page[-1]
            after = (iid, at, key)
    if _is_memory(repo):
        for iid in instrument_ids:
            page_cursor: tuple[TimeInput, str] | None = None
            while True:
                memory_page = await repo.page_range(
                    collection,
                    time_field,
                    start=start,
                    end=end,
                    filters=dict(filters or {}, instrument_id=iid),
                    limit=batch_size,
                    after=page_cursor,
                )
                for row, position in memory_page:
                    out[iid].append(row)
                    if keys_out is not None:
                        keys_out.setdefault(iid, []).append(position[1])
                    if keep is not None:
                        keep(iid, out[iid])
                if len(memory_page) < batch_size:
                    break
                page_cursor = memory_page[-1][1]
        return out

    clauses = [
        sql.SQL("{} IS NOT NULL").format(sql.Identifier(time_field)),
        sql.SQL("instrument_id = ANY(%s::text[])"),
    ]
    args: list[SQLParameter] = [instrument_ids]
    for op, bound in ((">=", start), ("<", end)):
        if bound is not None:
            clauses.append(sql.SQL("{} " + op + " %s").format(sql.Identifier(time_field)))
            args.append(parse_time(bound))
    if filters:
        extra, values = repo._exact_predicate(collection, filters)
        clauses.extend(extra)
        args.extend(values)

    selected = (
        sql.SQL("*")
        if level_rows is None
        else sql.SQL(",").join(
            map(sql.Identifier, ["storage_key", *(c for c in MARKET_COLUMNS[collection] if c not in LEVELS)])
        )
    )
    keys: dict[str, list[str]] = {iid: [] for iid in instrument_ids}
    after = None
    while True:
        page_clauses, page_args = list(clauses), list(args)
        if after is not None:
            page_clauses.append(
                sql.SQL("(instrument_id, {}, storage_key) > (%s, %s, %s)").format(sql.Identifier(time_field))
            )
            page_args.extend(after)
        query = sql.SQL("SELECT {} FROM {} WHERE {} ORDER BY instrument_id, {}, storage_key LIMIT %s").format(
            selected,
            sql.Identifier(*collection.split(".")),
            sql.SQL(" AND ").join(page_clauses),
            sql.Identifier(time_field),
        )
        async with repo.transaction():
            cursor = _json_as_text(repo._conn().cursor())
            await cursor.execute(query, [*page_args, batch_size])
            rows = await cursor.fetchall()

        decoded = await asyncio.to_thread(_instrument_rows, collection, rows)

        for n, (iid, record, key) in enumerate(decoded, 1):
            out[iid].append(record)
            if level_rows is not None:
                keys[iid].append(key)
            if keys_out is not None:
                keys_out.setdefault(iid, []).append(key)
            if keep is not None:
                keep(iid, out[iid])
            if n % YIELD_ROWS == 0:
                await asyncio.sleep(0)
        if len(rows) < batch_size:
            break
        last = rows[-1]
        after = (_text(last, "instrument_id"), _timestamp(last[time_field]), _text(last, "storage_key"))
    if level_rows is not None:
        query = sql.SQL(
            "SELECT storage_key, bids, asks FROM (SELECT storage_key, bids, asks, integrity_ok,"
            " row_number() OVER (PARTITION BY instrument_id ORDER BY {f} DESC, storage_key DESC) AS rn,"
            " row_number() OVER (PARTITION BY instrument_id, integrity_ok IS TRUE ORDER BY {f} DESC,"
            " storage_key DESC) AS ok_rn FROM {t} WHERE {w}) AS ranked"
            " WHERE rn <= %s OR (integrity_ok IS TRUE AND ok_rn = 1)"
        ).format(
            f=sql.Identifier(time_field),
            t=sql.Identifier(*collection.split(".")),
            w=sql.SQL(" AND ").join(clauses),
        )
        async with repo.transaction():
            cursor = _json_as_text(repo._conn().cursor())
            await cursor.execute(query, [*args, level_rows])
            found = await cursor.fetchall()
        levels = await asyncio.to_thread(
            lambda: {
                _text(row, "storage_key"): {
                    name: decode_json(_text(row, name)) if row[name] is not None else None for name in LEVELS
                }
                for row in found
            }
        )
        for iid, records in out.items():
            for record, key in zip(records, keys[iid], strict=True):
                if key in levels:
                    record.update(bids=levels[key]["bids"], asks=levels[key]["asks"])
                else:
                    for name in LEVELS:
                        record.pop(name, None)
    return out
