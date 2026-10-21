"""Typed market encoding and batched SQL writes (§14)."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import timedelta
from decimal import Decimal

from tank.market_schema import MARKET_ALIASES, MARKET_COLUMNS, MARKET_IDENTITIES
from tank.util import iso, jsonable, parse_time, utcnow


def encode(collection, record):
    from tank.time_precision import propagate_timestamp

    record = jsonable(record)
    columns = MARKET_COLUMNS[collection]
    aliases = MARKET_ALIASES.get(collection, {})
    timestamps = {name for name, kind in columns.items() if kind == "timestamptz"}
    time_aliases = {name: target for name, target in aliases.items() if target in timestamps}
    precision_names = {field + "_precision": field for field in timestamps}
    precision_names.update({alias + "_precision": target for alias, target in time_aliases.items()})
    result = {}
    for key, value in record.items():
        if key in precision_names or key.removesuffix("_reason") in precision_names:
            continue  # validated with canonical declarations below, never silently chosen
        target = aliases.get(key, key)
        if target not in columns:
            raise ValueError(f"Unknown typed market field {collection}.{key}")
        if target in timestamps and value is not None:
            if isinstance(value, dict):
                value = value.get("value", value.get("ts"))
            value = parse_time(value) if value is not None else None
        if target in result and result[target] != value:
            if target.endswith("_prec"):
                continue  # duration-equivalent declarations are resolved by shared validation
            raise ValueError(f"Conflicting market aliases {collection}.{key}")
        result[target] = value
    for field in timestamps:
        associated = [alias for alias, target in time_aliases.items() if target == field]
        precision_aliases = [name for alias in associated for name in (alias + "_prec", alias + "_precision")]
        if collection == "mkt.oi_obs" and field == "ts":
            precision_aliases.append("timestamp_precision")
        declared = field in result or any(
            record.get(name) is not None
            for name in (field + "_prec", field + "_precision", *precision_aliases)
        )
        if declared:
            selected = dict(record)
            selected[field] = record.get(
                field, next((record[alias] for alias in associated if alias in record), result.get(field))
            )
            stamp = propagate_timestamp(selected, field, precision_aliases=precision_aliases)
            result.update(stamp.fields(field))
    for name, value in list(result.items()):
        if value is None:
            continue
        kind = columns[name]
        if kind == "timestamptz":
            result[name] = parse_time(value)
        elif kind == "numeric":
            result[name] = Decimal(str(value))
        elif kind == "text" and not isinstance(value, str):
            raise ValueError(f"{collection}.{name} requires text")
    return result


def decode(collection, row):
    result = {}
    for key, kind in MARKET_COLUMNS[collection].items():
        value = row.get(key)
        if value is None:
            result[key] = None
            continue
        if kind == "timestamptz":
            value = iso(value)
        elif kind == "numeric":
            value = str(value)
        result[key] = value
    for alias, target in MARKET_ALIASES.get(collection, {}).items():
        if target in result:
            result[alias] = result[target]
    return result


# Fields left out of a collection's fingerprint: book levels belong to their exchange_ts
# (the row's identity) and are the bulk of each summary.
FINGERPRINT_EXCLUDED = {"mkt.book_summaries": frozenset({"bids", "asks"})}


def semantic_fingerprint(record, collection=None):
    excluded = FINGERPRINT_EXCLUDED.get(collection, frozenset())
    if excluded:
        record = {k: v for k, v in record.items() if k not in excluded}

    def strip(value):
        if isinstance(value, dict):
            return {
                k: strip(v)
                for k, v in value.items()
                if v is not None
                and not k.startswith(("received_ts", "recorded_at", "updated_at", "observed_at"))
                and k not in ("code_version", "config_version", "semantic_fingerprint")
            }
        if isinstance(value, list):
            return [strip(v) for v in value]
        return value

    return hashlib.sha256(
        json.dumps(strip(jsonable(record)), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def merge_point(collection, old, incoming):
    """Memory-test parity for the named server-side point mergers."""
    import copy

    merged = copy.deepcopy(old)
    merged.update({k: v for k, v in incoming.items() if v is not None})
    if collection == "mkt.funding_obs":
        sources = copy.deepcopy(old.get("source_observations") or {})
        for key, value in (incoming.get("source_observations") or {}).items():
            if semantic_fingerprint(sources.get(key)) != semantic_fingerprint(value):
                sources[key] = copy.deepcopy(value)
        merged["source_observations"] = sources
        preferred = "PUBLIC_REST" if merged.get("kind") == "SETTLED" else "WEBSOCKET"
        preferred = preferred if preferred in sources else incoming.get("capture_source")
        if preferred in sources:
            chosen = sources[preferred]
            for field in (
                "rate",
                "interval_h",
                "rate_8h_eq",
                "as_of",
                "as_of_prec",
                "as_of_prec_reason",
                "funding_time",
                "funding_time_prec",
                "funding_time_prec_reason",
                "received_ts",
                "received_ts_prec",
                "received_ts_prec_reason",
                "recorded_at",
                "recorded_at_prec",
                "recorded_at_prec_reason",
                "source_path",
            ):
                if field in chosen:
                    merged[field] = chosen[field]
            merged["capture_source"] = preferred
            merged["primary_source"] = preferred
        merged["source_disagreement"] = {
            "present": len(
                {str(v["rate_8h_eq"]) for v in sources.values() if v.get("rate_8h_eq") is not None}
            )
            > 1,
            "values": {
                k: {f: v.get(f) for f in ("rate", "interval_h", "rate_8h_eq")} for k, v in sources.items()
            },
        }
    elif collection == "mkt.tickers_24h":
        sources = copy.deepcopy(old.get("field_sources") or {})
        for field, meta in (incoming.get("field_sources") or {}).items():
            prior = sources.get(field)
            if (
                prior is None
                or meta.get("as_of")
                and (not prior.get("as_of") or parse_time(meta["as_of"]) >= parse_time(prior["as_of"]))
            ):
                sources[field] = copy.deepcopy(meta)
            elif field in old:
                merged[field] = old[field]
        for field in old.get("field_sources") or {}:
            if field not in (incoming.get("field_sources") or {}) and field in old:
                merged[field] = old[field]
        merged["field_sources"] = sources
        merged["capture_sources"] = sorted(
            {m["capture_source"] for m in sources.values() if m.get("capture_source")}
        )
        for prefix in ("mark", "index"):
            meta = sources.get(prefix + "_price")
            if meta:
                for suffix in ("as_of", "as_of_prec", "as_of_prec_reason"):
                    merged[prefix + "_" + suffix] = meta.get(suffix)
        merged["unclocked_components"] = (old.get("unclocked_components") or {}) | (
            incoming.get("unclocked_components") or {}
        )
    merged["semantic_fingerprint"] = semantic_fingerprint(merged)
    return merged


async def put_market_batch(self, collection, records, *, keys=None):
    records = list(records)
    keys = None if keys is None else list(keys)
    if not hasattr(self, "_conn") or len(records) <= WRITE_CHUNK:
        return await _put_market_batch(self, collection, records, keys=keys)
    # psycopg dumps the array parameters of one statement on the event loop: bound them.
    # Later chunks overwrite or merge like later rows of one batch (same-batch tests).
    changed = []
    for at in range(0, len(records), WRITE_CHUNK):
        part = None if keys is None else keys[at : at + WRITE_CHUNK]
        changed += await _put_market_batch(self, collection, records[at : at + WRITE_CHUNK], keys=part)
    return changed


# A flow bar written incomplete only because its writer holds no proof for the minute is
# absence of evidence, not evidence of loss: it never replaces a stored complete bar (a
# restarted or second process would otherwise unprove minutes the live stream proved).
UNPROVEN = "STREAM_COVERAGE_UNPROVEN"


def _unproves(collection, old, new):
    return (
        collection == "mkt.flow_bars_1m"
        and old.get("complete") is True
        and new.get("complete") is not True
        and (new.get("gap_reason") or UNPROVEN) == UNPROVEN
    )


# Rows a batched range read decodes between two turns of the event loop.
YIELD_ROWS = 1000
# Rows per write statement (array parameters are serialized on the event loop).
WRITE_CHUNK = 500
# Book summary columns read only for the rows whose levels are walked.
LEVELS = ("bids", "asks")


def _note_writes(self, collection, rows):
    """When rows of a scope last changed: a dependency head that saw every row recorded by
    its assembly clock stays valid for a later clock until its scope is written again."""
    if not rows:
        return
    clock = self.__dict__.setdefault("_market_write_clock", {})
    now = utcnow()
    clock[collection, None, None] = now
    for row in rows:
        for scope in ("instrument_id", "token_id"):
            if row.get(scope) is not None:
                clock[collection, scope, row[scope]] = now


def _prepare_records(collection, records, keys, columns, migration_target):
    """Encoded, de-duplicated and fingerprinted batch records by storage key (no I/O)."""
    encoded = [encode(collection, r) for r in records]
    if migration_target:
        for row in encoded:
            unsupported = [name for name, value in row.items() if name not in columns and value is not None]
            if unsupported:
                raise ValueError(f"Non-null fields unavailable in migration target: {unsupported}")
    if keys is None:
        keys = [":".join(str(r.get(k, "")) for k in MARKET_IDENTITIES[collection]) for r in encoded]
    keys = list(keys)
    if len(keys) != len(records):
        raise ValueError("keys and records must have equal lengths")
    unique = {}
    identity_keys = {}
    for key, record in zip(keys, encoded, strict=True):
        identity = tuple(
            str(key) if name == "storage_key" else record.get(name) for name in MARKET_IDENTITIES[collection]
        )
        if any(value is None or value == "" for value in identity):
            raise ValueError(f"Missing typed market identity {collection}: {MARKET_IDENTITIES[collection]}")
        previous_key = identity_keys.get(identity)
        if previous_key is not None:
            previous_record = unique.pop(previous_key)
            if collection in ("mkt.funding_obs", "mkt.tickers_24h"):
                record = encode(collection, merge_point(collection, previous_record, record))
        identity_keys[identity] = str(key)
        record["semantic_fingerprint"] = semantic_fingerprint(record, collection)
        unique[str(key)] = record
    return unique


def _json_as_text(cursor):
    """A cursor whose json/jsonb columns arrive as text, to be parsed off the event loop."""
    from psycopg.types.string import TextLoader

    for name in ("json", "jsonb"):
        cursor.adapters.register_loader(name, TextLoader)
    return cursor


def _load_json(collection, row):
    """Worker thread: parse the jsonb columns of a row read through _json_as_text."""
    for name, kind in MARKET_COLUMNS[collection].items():
        if kind == "jsonb" and isinstance(row.get(name), str):
            row[name] = json.loads(row[name])
    return row


async def _put_market_batch(self, collection, records, *, keys=None, _columns=None):
    # Forward migration import can target a historical typed schema. Ordinary
    # production writes always use the entire current explicit column contract.
    columns = MARKET_COLUMNS[collection] if _columns is None else _columns
    from psycopg import sql

    records = list(records)
    if not records:
        return []
    if not hasattr(self, "_conn"):
        unique = _prepare_records(collection, records, keys, columns, _columns is not None)
        changed = []
        async with self.transaction():
            rows = self._data.setdefault(collection, {})
            for key, row in unique.items():
                value = decode(collection, row)
                previous = key if key in rows else None
                if key not in rows:
                    identity = tuple(value.get(n) for n in MARKET_IDENTITIES[collection])
                    previous = (
                        next(
                            (
                                k
                                for k, v in rows.items()
                                if tuple(v.get(n) for n in MARKET_IDENTITIES[collection]) == identity
                            ),
                            None,
                        )
                        if "storage_key" not in MARKET_IDENTITIES[collection]
                        else None
                    )
                old = rows.get(previous, {})
                if old and collection in ("mkt.funding_obs", "mkt.tickers_24h"):
                    value = merge_point(collection, old, value)
                if old.get("semantic_fingerprint") != value["semantic_fingerprint"] and not _unproves(
                    collection, old, value
                ):
                    if previous is not None and previous != key:
                        rows.pop(previous)
                    rows[key] = value
                    changed.append(value.copy())
        _note_writes(self, collection, changed)
        return changed
    names = ["storage_key", *columns]

    def prepare():
        # Worker thread: encoding, fingerprints and JSON text of a batch are CPU work, and
        # psycopg would otherwise serialize every jsonb value on the event loop.
        unique = _prepare_records(collection, records, keys, columns, _columns is not None)
        arrays = []
        for name in names:
            kind = "text" if name == "storage_key" else columns[name]
            values = []
            for key, row in unique.items():
                value = key if name == "storage_key" else row.get(name)
                values.append(
                    json.dumps(value) if kind in ("jsonb", "text[]") and value is not None else value
                )
            arrays.append(values)
        return arrays

    arrays = await asyncio.to_thread(prepare)
    selection = sql.SQL(",").join(
        sql.SQL(
            "CASE WHEN u.{} IS NULL THEN NULL ELSE ARRAY(SELECT jsonb_array_elements_text(u.{})) END"
        ).format(sql.Identifier(n), sql.Identifier(n))
        if columns.get(n) == "text[]"
        else sql.SQL("coalesce(u.id,nextval('mkt.liq_obs_id_seq'))")
        if collection == "mkt.liq_obs" and n == "id"
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
    if collection == "mkt.flow_bars_1m":
        query = sql.SQL(
            "INSERT INTO {} AS current ({}) {} ON CONFLICT ({}) DO UPDATE SET {} WHERE current.semantic_fingerprint IS DISTINCT FROM EXCLUDED.semantic_fingerprint"
            " AND NOT (current.complete IS TRUE AND EXCLUDED.complete IS NOT TRUE AND coalesce(EXCLUDED.gap_reason, {}) = {}) RETURNING *"
        ).format(
            table,
            sql.SQL(",").join(map(sql.Identifier, names)),
            source,
            sql.SQL(",").join(map(sql.Identifier, identity)),
            assignments,
            sql.Literal(UNPROVEN),
            sql.Literal(UNPROVEN),
        )
    if collection in ("mkt.funding_obs", "mkt.tickers_24h"):
        mode = "funding" if collection == "mkt.funding_obs" else "ticker"
        merged = sql.SQL("ops.merge_market_point({},to_jsonb(current),to_jsonb(EXCLUDED))").format(
            sql.Literal(mode)
        )
        query = sql.SQL(
            "INSERT INTO {} AS current ({}) {} ON CONFLICT ({}) DO UPDATE SET ({}) = (SELECT {} FROM jsonb_populate_record(NULL::{}, {}) AS point) WHERE ops.market_semantic({}) IS DISTINCT FROM ops.market_semantic(to_jsonb(current)) RETURNING *"
        ).format(
            table,
            sql.SQL(",").join(map(sql.Identifier, names)),
            source,
            sql.SQL(",").join(map(sql.Identifier, identity)),
            sql.SQL(",").join(map(sql.Identifier, names)),
            sql.SQL(",").join(sql.SQL("point.{}").format(sql.Identifier(n)) for n in names),
            table,
            merged,
            merged,
        )
    async with self.transaction():
        cursor = _json_as_text(self._conn().cursor())
        await cursor.execute(query, arrays)
        rows = await cursor.fetchall()
    changed = await asyncio.to_thread(lambda: [decode(collection, _load_json(collection, r)) for r in rows])
    _note_writes(self, collection, changed)
    return changed


async def latest_market_records(
    self,
    collection,
    *,
    group_by=("instrument_id",),
    time_field="ts",
    filters=None,
    cutoff=None,
    assembled_at=None,
    start=None,
    instrument_ids=None,
):
    """Latest point per typed group after applying semantic/availability bounds.

    Unknown legacy recorded_at remains visible for consumers to classify as unknown/stale;
    this read does not invent historical availability or precision.
    """
    from psycopg import sql

    from tank.storage import _exact_predicate

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
    clauses, args = _exact_predicate(collection, filters)
    for bound, operator in ((start, ">="), (cutoff, "<=")):
        if bound is not None:
            clauses.append(sql.SQL("{} " + operator + " %s").format(sql.Identifier(time_field)))
            args.append(bound)
    if assembled_at is not None:
        clauses.append(sql.SQL("(recorded_at IS NULL OR recorded_at<=%s)"))
        args.append(assembled_at)
    if not hasattr(self, "_conn"):
        grouped = {}
        async for row in self.iter_all(collection, filters=filters):
            if instrument_ids is not None and row.get("instrument_id") not in instrument_ids:
                continue
            at = parse_time(row[time_field]) if row.get(time_field) is not None else None
            arrival = parse_time(row["recorded_at"]) if row.get("recorded_at") is not None else None
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
            key = tuple(row.get(k) for k in group_by)
            previous = grouped.get(key)
            # iter_all is storage-key ascending; replacing ties matches SQL key DESC.
            if (
                previous is None
                or (
                    at is not None
                    and (previous.get(time_field) is None or at >= parse_time(previous[time_field]))
                )
                or at is None
                and previous.get(time_field) is None
            ):
                grouped[key] = row
        return list(grouped.values())
    groups = sql.SQL(",").join(map(sql.Identifier, group_by))
    if instrument_ids is not None and tuple(group_by) == ("instrument_id",):
        # Each scope seeks its last point through the existing instrument/time
        # index. DISTINCT ON over a whole history scans and sorts every point.
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


async def market_dependency_head(
    self,
    collection,
    semantic_field,
    cutoff,
    *,
    start=None,
    filters=None,
    instrument_ids=None,
    assembled_at=None,
    order="availability",
    orders=None,
):
    """Availability or semantic-boundary head within a scope, returning metadata only.
    With `orders`, every listed head comes from one round trip as {order: head}.

    A correction to an old point participates by its actual recorded_at, not by its
    position in semantic history. Unknown legacy availability is explicitly excluded.
    Bounds are inclusive; an empty instrument scope never broadens into all instruments.
    """
    from psycopg import sql

    from tank.storage import _exact_predicate

    if any(o not in ("availability", "semantic_latest", "semantic_earliest") for o in orders or (order,)):
        raise ValueError("Unsupported market dependency head order")
    if orders is not None and not hasattr(self, "_conn"):
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

    def metadata(row):
        return {
            "storage_key": row["storage_key"],
            "recorded_at": iso(row["recorded_at"]),
            "semantic_fingerprint": row.get("semantic_fingerprint"),
            semantic_field: iso(row[semantic_field]),
        }

    if not hasattr(self, "_conn"):
        # Validate filters with the same allowlist even when no rows match.
        _exact_predicate(collection, filters)
        selected = None
        async with self.transaction():
            for key, row in self._data.get(collection, {}).items():
                arrival, semantic = row.get("recorded_at"), row.get(semantic_field)
                if arrival is None or semantic is None:
                    continue
                arrival, semantic = parse_time(arrival), parse_time(semantic)
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
    clauses, args = _exact_predicate(collection, filters)
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

    def head(o):
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
    found = {row["head"]: metadata(row) for row in rows}
    return found.get(order) if orders is None else {o: found.get(o) for o in wanted}


async def iter_market_projection(
    self,
    collection,
    fields,
    time_field,
    *,
    start=None,
    end=None,
    filters=None,
    instrument_ids=None,
    assembled_at=None,
    batch_size=1000,
):
    """Stream explicitly selected typed columns, never full market records.

    Semantic bounds are [start,end). Unknown legacy availability is retained explicitly.
    Each keyset page releases its connection before yielding any projected row.
    """
    from psycopg import sql

    from tank.storage import _exact_predicate

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
    # Validate typed predicates even for Memory and empty matching populations.
    _exact_predicate(collection, filters)

    def project(row, key):
        result = {}
        for requested, field in selected.items():
            value = key if field == "storage_key" else row.get(field)
            kind = columns.get(field)
            if value is not None and kind == "timestamptz":
                value = iso(value)
            elif value is not None and kind == "numeric":
                value = str(value)
            result[requested] = value
        return result

    after = None
    while True:
        if hasattr(self, "_conn"):
            clauses, args = _exact_predicate(collection, filters)
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
            page = []
            for row in raw:
                result = {}
                for requested, field in selected.items():
                    value = row[requested]
                    kind = columns.get(field)
                    if value is not None and kind == "timestamptz":
                        value = iso(value)
                    elif value is not None and kind == "numeric":
                        value = str(value)
                    result[requested] = value
                page.append((row["_cursor_time"], row["_cursor_key"], result))
        else:
            page = []
            async with self.transaction():
                for key, row in self._data.get(collection, {}).items():
                    at = parse_time(row[time_field]) if row.get(time_field) is not None else None
                    arrival = parse_time(row["recorded_at"]) if row.get("recorded_at") is not None else None
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


async def market_series_counts(self, instrument_ids, start, cutoff, assembled_at=None):
    """Count complete finite positive closes of closed native 1m bars per instrument."""
    from psycopg import sql

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
    if not hasattr(self, "_conn"):
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
            close = Decimal(row["c"]) if row["c"] is not None else None
            if (
                close is not None
                and close.is_finite()
                and close > 0
                and parse_time(row["open_time"]) + timedelta(seconds=60) <= cutoff
            ):
                counts[row["instrument_id"]] += 1
        return counts
    args = [instrument_ids, start, cutoff, cutoff]
    availability = sql.SQL("")
    if assembled_at is not None:
        availability = sql.SQL(" AND (recorded_at IS NULL OR recorded_at<=%s)")
        args.append(assembled_at)
    query = sql.SQL(
        "SELECT instrument_id,count(*) AS n FROM mkt.klines_1m WHERE instrument_id=ANY(%s::text[]) AND open_time>=%s AND open_time<%s AND open_time+interval '60 seconds'<=%s AND interval_s=60 AND complete IS TRUE AND c>0 AND c<'Infinity'::numeric {} GROUP BY instrument_id"
    ).format(availability)
    async with self.transaction():
        rows = await (await self._conn().execute(query, args)).fetchall()
    counts.update({row["instrument_id"]: row["n"] for row in rows})
    return counts


async def latest_market_trades(repo, instrument_ids, *, start):
    """Newest flow bar holding a print, per instrument: its `last_trade` resumes capture."""
    instrument_ids = sorted(set(instrument_ids))
    start = parse_time(start)
    if not instrument_ids:
        return []
    if not hasattr(repo, "_conn"):
        latest = {}
        async for row in repo.iter_all("mkt.flow_bars_1m"):
            iid = row.get("instrument_id")
            if (
                iid not in instrument_ids
                or not row.get("last_trade")
                or parse_time(row["minute_start"]) < start
            ):
                continue
            if iid not in latest or parse_time(row["minute_start"]) > parse_time(latest[iid]["minute_start"]):
                latest[iid] = row
        return [
            {"instrument_id": iid, "last_trade": row["last_trade"]} for iid, row in sorted(latest.items())
        ]
    query = (
        "SELECT point.instrument_id, point.last_trade FROM unnest(%s::text[]) AS scope(instrument_id) "
        "CROSS JOIN LATERAL (SELECT instrument_id, last_trade FROM mkt.flow_bars_1m "
        "WHERE instrument_id = scope.instrument_id AND minute_start >= %s AND last_trade IS NOT NULL "
        "ORDER BY minute_start DESC LIMIT 1) AS point"
    )
    async with repo.transaction():
        rows = await (await repo._conn().execute(query, [instrument_ids, start])).fetchall()
    return [{"instrument_id": row["instrument_id"], "last_trade": row["last_trade"]} for row in rows]


async def market_series_day_counts(repo, instrument_ids, start, end, assembled_at=None):
    """Complete finite positive 1m closes per (instrument, UTC day) for whole days in [start, end)."""
    from psycopg import sql

    instrument_ids = sorted(set(instrument_ids))
    start, end = parse_time(start), parse_time(end)
    counts = {}
    if not instrument_ids or start >= end:
        return counts
    if not hasattr(repo, "_conn"):
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
            close = Decimal(row["c"]) if row["c"] is not None else None
            opened = parse_time(row["open_time"])
            if close is not None and close.is_finite() and close > 0 and opened < end:
                day = opened.replace(hour=0, minute=0, second=0, microsecond=0)
                counts[row["instrument_id"], day] = counts.get((row["instrument_id"], day), 0) + 1
        return counts
    args = [instrument_ids, start, end]
    availability = sql.SQL("")
    if assembled_at is not None:
        availability = sql.SQL(" AND (recorded_at IS NULL OR recorded_at<=%s)")
        args.append(parse_time(assembled_at))
    query = sql.SQL(
        "SELECT instrument_id, date_trunc('day', open_time AT TIME ZONE 'UTC') AS day, count(*) AS n "
        "FROM mkt.klines_1m WHERE instrument_id=ANY(%s::text[]) AND open_time>=%s AND open_time<%s "
        "AND interval_s=60 AND complete IS TRUE AND c>0 AND c<'Infinity'::numeric {} GROUP BY 1, 2"
    ).format(availability)
    async with repo.transaction():
        rows = await (await repo._conn().execute(query, args)).fetchall()
    from datetime import UTC

    for row in rows:
        counts[row["instrument_id"], row["day"].replace(tzinfo=UTC)] = row["n"]
    return counts


async def market_kline_days_recorded_since(repo, since, end):
    """(instrument, UTC day) of every 1m kline in [.., end) recorded at or after `since`."""
    since, end = parse_time(since), parse_time(end)
    days = set()
    if not hasattr(repo, "_conn"):
        async for row in repo.iter_late_arrivals(
            "mkt.klines_1m", "recorded_at", "open_time", since=since, market_end=end
        ):
            opened = parse_time(row["open_time"])
            if opened < end:
                days.add((row["instrument_id"], opened.replace(hour=0, minute=0, second=0, microsecond=0)))
        return days
    query = (
        "SELECT DISTINCT instrument_id, date_trunc('day', open_time AT TIME ZONE 'UTC') AS day "
        "FROM mkt.klines_1m WHERE recorded_at>=%s AND open_time<%s"
    )
    async with repo.transaction():
        rows = await (await repo._conn().execute(query, [since, end])).fetchall()
    from datetime import UTC

    return {(row["instrument_id"], row["day"].replace(tzinfo=UTC)) for row in rows}


async def sampled_market_history(
    repo, collection, time_field, group_fields, instrument_ids, start, end, bucket_s
):
    """The last point per (instrument, group, bucket_s bucket) in [start, end).

    Older history behind normal ranges is read at the base-tier cadence: denser hot
    points matter only near the event, which the caller reads in full.
    """
    from psycopg import sql

    instrument_ids = sorted(set(instrument_ids))
    start, end = parse_time(start), parse_time(end)
    if not instrument_ids or start >= end:
        return []
    if not hasattr(repo, "_conn"):
        latest = {}
        for iid in instrument_ids:
            async for row in repo.iter_range(
                collection, time_field, start=start, end=end, filters={"instrument_id": iid}
            ):
                at = parse_time(row[time_field])
                if not start <= at < end:
                    continue
                key = (iid, *(row.get(g) for g in group_fields), int(at.timestamp()) // bucket_s)
                if key not in latest or at >= parse_time(latest[key][time_field]):
                    latest[key] = row
        return sorted(latest.values(), key=lambda r: r[time_field])
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
        rows = await (await repo._conn().execute(query, args)).fetchall()
    return sorted((decode(collection, row) for row in rows), key=lambda r: r[time_field])


async def market_range_by_instrument(
    repo,
    collection,
    time_field,
    instrument_ids,
    start,
    end,
    filters=None,
    *,
    batch_size=1000,
    keep=None,
    level_rows=None,
):
    """Rows of several instruments in [start, end), paged on one key for all of them.

    Per instrument the rows come in the order iter_range returns them for that instrument
    alone ((time_field, storage_key)), so callers can replace a per-instrument loop of range
    reads (e.g. 9 collections x the instruments of a token) without changing any output.
    Pages are keyset-paged like iter_range; `keep(instrument_id, rows)` runs after each
    appended row so a caller can shrink older rows while they stream (book levels).
    1k-row pages (wide rows: 500) bound the transient of one page and the loop time of its fetch.
    """
    from psycopg import sql

    instrument_ids = sorted(set(instrument_ids))
    out = {iid: [] for iid in instrument_ids}
    if not instrument_ids:
        return out
    if not hasattr(repo, "_conn"):
        for iid in instrument_ids:
            async for row in repo.iter_range(
                collection,
                time_field,
                start=start,
                end=end,
                filters=dict(filters or {}, instrument_id=iid),
                batch_size=batch_size,
            ):
                out[iid].append(row)
                if keep is not None:
                    keep(iid, out[iid])
        return out
    from tank.storage import _exact_predicate, _read_record

    clauses = [
        sql.SQL("{} IS NOT NULL").format(sql.Identifier(time_field)),
        sql.SQL("instrument_id = ANY(%s::text[])"),
    ]
    args = [instrument_ids]
    for op, bound in ((">=", start), ("<", end)):
        if bound is not None:
            clauses.append(sql.SQL("{} " + op + " %s").format(sql.Identifier(time_field)))
            args.append(parse_time(bound))
    if filters:
        extra, values = _exact_predicate(collection, filters)
        clauses.extend(extra)
        args.extend(values)
    # Book levels are the bulk of a summary row and are decoded inside fetchall: pages carry
    # every column but bids/asks, and only the rows Market Truth walks get them after.
    selected = (
        sql.SQL("*")
        if level_rows is None
        else sql.SQL(",").join(
            map(sql.Identifier, ["storage_key", *(c for c in MARKET_COLUMNS[collection] if c not in LEVELS)])
        )
    )
    keys = {iid: [] for iid in instrument_ids}
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
        # JSON parsing and typed decode run in a worker thread (r111: json.loads inside
        # fetchall held the loop); with a short GIL switch interval the loop keeps its turns.
        decoded = await asyncio.to_thread(
            lambda rows=rows: [
                (
                    row["instrument_id"],
                    _read_record(collection, _load_json(collection, row)),
                    row["storage_key"],
                )
                for row in rows
            ]
        )
        for n, (iid, record, key) in enumerate(decoded, 1):
            out[iid].append(record)
            if level_rows is not None:
                keys[iid].append(key)
            if keep is not None:
                keep(iid, out[iid])
            if n % YIELD_ROWS == 0:
                await asyncio.sleep(0)
        if len(rows) < batch_size:
            break
        last = rows[-1]
        after = (last["instrument_id"], last[time_field], last["storage_key"])
    if level_rows is not None:
        # The newest `level_rows` rows per instrument, and its newest integrity-checked row.
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
                row["storage_key"]: {
                    name: json.loads(row[name]) if row[name] is not None else None for name in LEVELS
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
