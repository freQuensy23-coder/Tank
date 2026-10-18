"""Transactional PostgreSQL storage. MemoryRepository is an explicit test double only."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import timedelta
from pathlib import Path
from typing import Protocol

from tank.models import APPEND_ONLY, PARTITIONED, SERIAL_TABLES, TABLE_COLUMNS, Record
from tank.util import iso, jsonable, new_id, parse_time, utcnow


class ImmutableRecordError(ValueError):
    pass


class Repository(Protocol):
    async def open(self): ...
    async def close(self): ...
    async def migrate(self): ...
    async def put(self, collection: str, key: str, record: Record, *, immutable: bool = False) -> Record: ...
    async def get(self, collection: str, key: str) -> Record | None: ...
    async def list(
        self, collection: str, *, filters: dict | None = None, limit: int = 1000
    ) -> list[Record]: ...
    async def delete(self, collection: str, key: str): ...
    def transaction(self): ...
    def lock(self, key: str): ...
    async def enqueue(
        self, kind, payload, idem_key, *, priority=1, run_after=None, expires_at=None, max_attempts=3
    ): ...
    async def claim_job(self): ...
    async def finish_job(self, job_id): ...
    async def fail_job(self, job_id, error, *, retry_after=1): ...
    async def recover_jobs(self): ...
    async def prune_market(self, before): ...
    async def maintain_partitions(self): ...
    async def prune_jobs(self, before): ...
    async def preserved_market_windows(self): ...
    async def latest_before(self, collection, time_field, cutoff, *, filters=None): ...
    def iter_all(self, collection, *, filters=None, batch_size=1000): ...
    def iter_metadata(self, collection, fields, *, filters=None, batch_size=1000): ...
    def iter_late_arrivals(
        self,
        collection,
        arrival_field,
        market_field,
        *,
        since=None,
        end=None,
        market_end,
        filters=None,
        batch_size=1000,
    ): ...
    async def list_latest(self, collection, time_field, *, filters=None, limit=100, start=None, end=None): ...
    async def list_range(
        self, collection, time_field, *, start=None, end=None, filters=None, limit=1000, after=None
    ): ...
    def iter_range(self, collection, time_field, *, start=None, end=None, filters=None, batch_size=10000): ...


def _table(collection):
    if collection not in TABLE_COLUMNS:
        raise ValueError(f"Unknown collection: {collection}")
    return collection


def _job(kind, payload, idem_key, priority, run_after, expires_at, max_attempts):
    if not 0 <= priority <= 4 or max_attempts < 1:
        raise ValueError("priority must be 0..4 and max_attempts positive")
    default_expiry = {
        "FILTER_ITEM": 21600,
        "ROUTE_ITEM": 21600,
        "AGENT_MESSAGE": 86400,
        "RUN_MARKET_TRUTH": 1800,
        "DELIVER": 86400,
        "EVAL_CONDITIONS": 300,
        "REACTION_CAPTURE": 21600,
        "SCOUT_REVIEW": 604800,
    }
    if expires_at is None and kind in default_expiry:
        expiry_anchor = (
            parse_time(run_after) if kind == "REACTION_CAPTURE" and run_after is not None else utcnow()
        )
        expires_at = expiry_anchor + timedelta(seconds=default_expiry[kind])
    return dict(
        job_id=new_id("JOB"),
        type=kind,
        priority=priority,
        idem_key=idem_key,
        payload=jsonable(payload),
        status="QUEUED",
        attempts=0,
        max_attempts=max_attempts,
        run_after=iso(parse_time(run_after)) if run_after else iso(),
        expires_at=iso(parse_time(expires_at)) if expires_at else None,
        last_error=None,
        created_at=iso(),
        finished_at=None,
    )


class QueueMixin:
    async def enqueue(
        self, kind, payload, idem_key, *, priority=1, run_after=None, expires_at=None, max_attempts=3
    ):
        async with self.lock(f"queue:{kind}:{idem_key}"):
            old = await self.list("ops.jobs", filters={"type": kind, "idem_key": idem_key}, limit=1)
            if old:
                return old[0]
            job = _job(kind, payload, idem_key, priority, run_after, expires_at, max_attempts)
            return await self.put("ops.jobs", job["job_id"], job)

    async def finish_job(self, job_id):
        await self._transition_job(job_id, "DONE")

    async def fail_job(self, job_id, error, *, retry_after=1):
        async with self.lock(f"job:{job_id}"):
            job = await self.get("ops.jobs", job_id)
            if job is None:
                raise KeyError(job_id)
            if job["status"] != "RUNNING":
                return
            job["last_error"] = str(error)
            expired = job.get("expires_at") and parse_time(job["expires_at"]) <= utcnow()
            job["status"] = (
                "EXPIRED" if expired else ("FAILED" if job["attempts"] >= job["max_attempts"] else "QUEUED")
            )
            job["run_after"] = iso(utcnow() + timedelta(seconds=retry_after))
            await self.put("ops.jobs", job_id, job)

    async def _transition_job(self, job_id, status):
        async with self.lock(f"job:{job_id}"):
            job = await self.get("ops.jobs", job_id)
            if job is None:
                raise KeyError(job_id)
            if job["status"] == "RUNNING":
                job["status"] = status
                await self.put("ops.jobs", job_id, job)


class MemoryRepository(QueueMixin):
    """Deterministic test implementation with nested rollback and task-local ownership."""

    def __init__(self):
        self._data = {}
        self._immutable = set()
        self._mutex = asyncio.Lock()
        self._owner = None
        self._locks = {}
        self._held = ContextVar(f"memory_locks_{id(self)}", default=frozenset())

    async def open(self):
        return self

    async def close(self):
        pass

    async def migrate(self):
        pass

    @asynccontextmanager
    async def transaction(self):
        outer = self._owner is not asyncio.current_task()
        if outer:
            await self._mutex.acquire()
            self._owner = asyncio.current_task()
        # Stored domain documents are copy-on-write; only queue claims mutate rows in place.
        # Copy mappings rather than every historical market document for each test insert.
        saved = {collection: rows.copy() for collection, rows in self._data.items()}
        if "ops.jobs" in saved:
            saved["ops.jobs"] = copy.deepcopy(saved["ops.jobs"])
        snapshot = (saved, self._immutable.copy())
        try:
            yield self
        except BaseException:
            self._data, self._immutable = snapshot
            raise
        finally:
            if outer:
                self._owner = None
                self._mutex.release()

    @asynccontextmanager
    async def lock(self, key):
        # One in-memory transaction mutex models database atomicity and avoids lock-order inversions.
        async with self.transaction():
            yield self

    async def put(self, collection, key, record, *, immutable=False):
        _table(collection)
        key = str(key)
        record = jsonable(record)
        async with self.transaction():
            old = self._data.get(collection, {}).get(key)
            if old is not None and (
                (collection, key) in self._immutable or collection in APPEND_ONLY or immutable
            ):
                if old != record:
                    raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
                if immutable:
                    self._immutable.add((collection, key))
                return copy.deepcopy(old)
            if collection == "ops.jobs":
                if any(
                    r["type"] == record["type"] and r["idem_key"] == record["idem_key"] and k != key
                    for k, r in self._data.get(collection, {}).items()
                ):
                    raise ValueError("Duplicate job idempotency key")
            if collection == "ops.jobs":
                record.setdefault("created_at", iso())
                record["finished_at"] = (
                    (record.get("finished_at") or iso())
                    if record["status"] in ("DONE", "FAILED", "EXPIRED")
                    else None
                )
            self._data.setdefault(collection, {})[key] = copy.deepcopy(record)
            if immutable or collection in APPEND_ONLY:
                self._immutable.add((collection, key))
            return copy.deepcopy(record)

    async def get(self, collection, key):
        _table(collection)
        async with self.transaction():
            return copy.deepcopy(self._data.get(collection, {}).get(str(key)))

    async def list(self, collection, *, filters=None, limit=1000):
        _table(collection)
        if limit < 0:
            raise ValueError("limit must be nonnegative")
        async with self.transaction():
            return copy.deepcopy(
                [
                    r
                    for r in self._data.get(collection, {}).values()
                    if all(k in r and r[k] == v for k, v in (filters or {}).items())
                ][:limit]
            )

    async def delete(self, collection, key):
        _table(collection)
        async with self.transaction():
            if collection in APPEND_ONLY or (collection, str(key)) in self._immutable:
                raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
            self._data.get(collection, {}).pop(str(key), None)

    async def claim_job(self):
        async with self.transaction():
            now = utcnow()
            candidates = []
            for key, job in self._data.get("ops.jobs", {}).items():
                if job["status"] != "QUEUED":
                    continue
                if job.get("expires_at") and parse_time(job["expires_at"]) <= now:
                    job["status"] = "EXPIRED"
                    job["finished_at"] = iso()
                elif job["attempts"] >= job["max_attempts"] and not job.get("recovery_reclaim"):
                    job["status"] = "FAILED"
                    job["finished_at"] = iso()
                elif parse_time(job["run_after"]) <= now:
                    candidates.append(job)
            if not candidates:
                return None
            job = min(candidates, key=lambda r: (r["priority"], r["run_after"], r["job_id"]))
            job["status"] = "RUNNING"
            if not job.pop("recovery_reclaim", False):
                job["attempts"] += 1
            return copy.deepcopy(job)

    async def recover_jobs(self):
        async with self.transaction():
            count = 0
            for job in self._data.get("ops.jobs", {}).values():
                if job["status"] == "RUNNING":
                    job["status"] = (
                        "EXPIRED"
                        if job.get("expires_at") and parse_time(job["expires_at"]) <= utcnow()
                        else "QUEUED"
                    )
                    if job["status"] == "EXPIRED":
                        job["finished_at"] = iso()
                    else:
                        job.update(
                            recovery_reclaim=True,
                            recovery_at=iso(),
                            recovery_count=job.get("recovery_count", 0) + 1,
                            finished_at=None,
                        )
                    count += 1
            return count


def _projection(collection, record):
    """Map §2 nested names to §14 indexed columns without changing the document."""
    values = dict(record)
    aliases = {
        "body": "text",
        "raw_path": "raw_ref",
        "claim_type": "type",
        "gates": "structural_gates",
        "buy_usd": "buy_notional",
        "sell_usd": "sell_notional",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "funding_time": "settlement_time",
        "public_at_prec": "public_at_precision",
        "published_prec": "published_at_precision",
    }
    for column, alias in aliases.items():
        if column not in values and alias in values:
            values[column] = values[alias]
    if collection == "ingest.source_items":
        for obj, mappings in [
            ("relevance", {"decision": "relevance_decision", "p_relevant": "relevance_p"}),
            ("route", {"decision": "route_decision", "event_id": "route_event_id", "p_same": "route_p"}),
        ]:
            for source, target in mappings.items():
                if isinstance(record.get(obj), dict) and source in record[obj]:
                    values[target] = record[obj][source]
    if collection == "ops.llm_usage":
        for column, aliases in {
            "at": ("created_at",),
            "model": ("model_id",),
            "subject_id": ("event_id", "item_id", "session_id", "proposal_id"),
        }.items():
            if values.get(column) is None:
                values[column] = next((record[a] for a in aliases if record.get(a) is not None), None)
        usage = record.get("usage") or {}
        for column in (
            "input_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "output_tokens",
            "web_searches",
            "stop_reason",
        ):
            if values.get(column) is None and usage.get(column) is not None:
                values[column] = usage[column]
    if collection == "ana.calls":
        if isinstance(record.get("opened_by"), (list, tuple)):
            values["opened_by_alert"], values["opened_by_version"] = record["opened_by"]
        elif isinstance(record.get("opened_by"), dict):
            values["opened_by_alert"] = record["opened_by"].get("alert_id")
            values["opened_by_version"] = record["opened_by"].get("version")
    if collection == "evt.claims":
        measured = record.get("value")
        if isinstance(measured, dict):
            values.setdefault("value_num", measured.get("number"))
            values.setdefault("value_unit", measured.get("unit"))
        if not values.get("quote") and record.get("evidence"):
            first_evidence = record["evidence"][0]
            if isinstance(first_evidence, dict):
                values["quote"] = first_evidence.get("quote")
    if collection == "ana.decisions" and not values.get("mta_id"):
        values["mta_id"] = (record.get("provenance") or {}).get("mta_id")
    if collection == "ana.market_truth":
        detail = copy.deepcopy(values.get("detail") or {})
        for key in ("spot", "derivatives", "liquidity", "supply", "relative"):
            if key in record:
                detail[key] = record[key]
        values["detail"] = detail

    id_arrays = {
        ("ana.decisions", "conditions"): "cond_id",
        ("out.alerts", "decisions"): "dec_id",
        ("evt.state_changes", "evidence"): "claim_id",
    }
    for column, datatype in TABLE_COLUMNS[collection].items():
        if datatype == "text[]" and values.get(column) is not None:
            array = values[column]
            if not isinstance(array, (list, tuple)):
                raise ValueError(f"{collection}.{column} must be an array")
            id_field = id_arrays.get((collection, column))
            projected_array = []
            for element in array:
                if isinstance(element, dict) and id_field:
                    element = element.get(id_field)
                if not isinstance(element, str):
                    raise ValueError(
                        f"{collection}.{column} requires text values"
                        + (f" or objects carrying {id_field}" if id_field else "")
                    )
                projected_array.append(element)
            values[column] = projected_array
        if datatype == "timestamptz" and isinstance(values.get(column), dict):
            temporal = values[column]
            values[column] = temporal.get("value", temporal.get("ts"))
            precision_column = column + "_prec"
            if precision_column in TABLE_COLUMNS[collection]:
                values[precision_column] = temporal.get("precision", temporal.get("prec"))
    return {k: values.get(k) for k in TABLE_COLUMNS[collection]}


class PostgresRepository(QueueMixin):
    def __init__(self, database_url):
        self.database_url = database_url
        self._pool = None
        self._connection = ContextVar(f"pg_connection_{id(self)}", default=None)

    async def open(self):
        from psycopg.rows import dict_row
        from psycopg_pool import AsyncConnectionPool

        self._pool = AsyncConnectionPool(
            self.database_url, open=False, min_size=1, max_size=16, kwargs={"row_factory": dict_row}
        )
        await self._pool.open()
        await self._pool.wait()
        return self

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def transaction(self):
        existing = self._connection.get()
        if existing is not None:
            conn, owner = existing
            if owner is not asyncio.current_task():
                raise RuntimeError("Do not share a transaction between concurrent tasks")
            async with conn.transaction():
                yield self
            return
        if self._pool is None:
            raise RuntimeError("Repository.open() must be called first")
        async with self._pool.connection() as conn:
            token = self._connection.set((conn, asyncio.current_task()))
            try:
                async with conn.transaction():
                    yield self
            finally:
                self._connection.reset(token)

    def _conn(self):
        return self._connection.get()[0]

    @asynccontextmanager
    async def lock(self, key):
        async with self.transaction():
            digest = int.from_bytes(hashlib.sha256(str(key).encode()).digest()[:8], "big", signed=True)
            await self._conn().execute("SELECT pg_advisory_xact_lock(%s)", (digest,))
            yield self

    async def migrate(self, *, target=None):
        async with self.lock("tank:migrations"):
            conn = self._conn()
            await conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS ops.schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            for path in sorted((Path(__file__).parent / "migrations").glob("*.sql")):
                if target is not None and path.name > target:
                    break
                exists = await (
                    await conn.execute("SELECT 1 FROM ops.schema_migrations WHERE version=%s", (path.name,))
                ).fetchone()
                if exists is None:
                    await conn.execute(path.read_text(), prepare=False)
                    await conn.execute("INSERT INTO ops.schema_migrations(version) VALUES (%s)", (path.name,))

    async def put(self, collection, key, record, *, immutable=False):
        from psycopg import sql
        from psycopg.types.json import Jsonb

        _table(collection)
        key, record = str(key), jsonable(record)
        projected = _projection(collection, record)
        if collection in PARTITIONED and projected.get(PARTITIONED[collection]) is not None:
            day = parse_time(projected[PARTITIONED[collection]]).date()
            partition_name = collection + "_" + day.strftime("%Y%m%d")
            async with self.transaction():
                exists = await (
                    await self._conn().execute("SELECT to_regclass(%s) AS name", (partition_name,))
                ).fetchone()
            if exists["name"] is None:
                await self.maintain_partitions(extra_days={collection: {day}})
        async with self.lock(f"record:{collection}:{key}"):
            conn = self._conn()
            table = sql.Identifier(*collection.split("."))
            old = await (
                await conn.execute(
                    sql.SQL("SELECT document, immutable FROM {} WHERE storage_key=%s FOR UPDATE").format(
                        table
                    ),
                    (key,),
                )
            ).fetchone()
            if old and (old["immutable"] or immutable or collection in APPEND_ONLY):
                if old["document"] != record:
                    raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
                if immutable and not old["immutable"]:
                    await conn.execute(
                        sql.SQL("UPDATE {} SET immutable=true WHERE storage_key=%s").format(table), (key,)
                    )
                return old["document"]
            projected = _projection(collection, record)
            if collection in SERIAL_TABLES and projected.get("id") is None:
                projected.pop("id", None)
            values = [key, Jsonb(record), immutable or collection in APPEND_ONLY]
            for column, value in projected.items():
                datatype = TABLE_COLUMNS[collection][column]
                if value is not None:
                    if datatype == "jsonb":
                        value = Jsonb(value)
                    elif datatype == "timestamptz":
                        value = parse_time(value)
                    elif datatype == "text" and not isinstance(value, str):
                        value = json.dumps(value, sort_keys=True)
                values.append(value)
            names = ["storage_key", "document", "immutable", *projected]
            columns = sql.SQL(",").join(map(sql.Identifier, names))
            assignments = sql.SQL(",").join(
                sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(n), sql.Identifier(n)) for n in names[1:]
            )
            query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT(storage_key) DO UPDATE SET {}"
            ).format(table, columns, sql.SQL(",").join(sql.Placeholder() for _ in names), assignments)
            if collection in PARTITIONED:
                if old:
                    query = sql.SQL("UPDATE {} SET {} WHERE storage_key=%s").format(
                        table,
                        sql.SQL(",").join(sql.SQL("{}=%s").format(sql.Identifier(n)) for n in names[1:]),
                    )
                    await conn.execute(query, [*values[1:], key])
                else:
                    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        table, columns, sql.SQL(",").join(sql.Placeholder() for _ in names)
                    )
                    await conn.execute(query, values)
            else:
                await conn.execute(query, values)
            return copy.deepcopy(record)

    async def get(self, collection, key):
        from psycopg import sql

        _table(collection)
        async with self.transaction():
            row = await (
                await self._conn().execute(
                    sql.SQL("SELECT document FROM {} WHERE storage_key=%s").format(
                        sql.Identifier(*collection.split("."))
                    ),
                    (str(key),),
                )
            ).fetchone()
            return row["document"] if row else None

    async def list(self, collection, *, filters=None, limit=1000):
        from psycopg import sql
        from psycopg.types.json import Jsonb

        _table(collection)
        if limit < 0:
            raise ValueError("limit must be nonnegative")
        # JSONB containment preserves exact structured top-level values and nulls.
        clauses, args = [], []
        for k, v in (filters or {}).items():
            clauses.append(sql.SQL("document -> %s = %s"))
            args.extend([k, Jsonb(jsonable(v))])
            datatype = TABLE_COLUMNS[collection].get(k)
            if (
                datatype
                and v is not None
                and datatype not in ("jsonb", "text[]")
                and not isinstance(v, (dict, list))
            ):
                typed_value = v
                if datatype == "timestamptz":
                    try:
                        typed_value = parse_time(v)
                    except (ValueError, TypeError):
                        continue
                clauses.append(sql.SQL("{} = %s").format(sql.Identifier(k)))
                args.append(typed_value)
        where = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
        async with self.transaction():
            rows = await (
                await self._conn().execute(
                    sql.SQL("SELECT document FROM {} WHERE {} ORDER BY storage_key LIMIT %s").format(
                        sql.Identifier(*collection.split(".")), where
                    ),
                    [*args, limit],
                )
            ).fetchall()
            return [r["document"] for r in rows]

    async def delete(self, collection, key):
        from psycopg import sql

        _table(collection)
        async with self.lock(f"record:{collection}:{key}"):
            row = await (
                await self._conn().execute(
                    sql.SQL("SELECT immutable FROM {} WHERE storage_key=%s FOR UPDATE").format(
                        sql.Identifier(*collection.split("."))
                    ),
                    (str(key),),
                )
            ).fetchone()
            if collection in APPEND_ONLY or row and row["immutable"]:
                raise ImmutableRecordError(f"Immutable record: {collection}/{key}")
            await self._conn().execute(
                sql.SQL("DELETE FROM {} WHERE storage_key=%s").format(sql.Identifier(*collection.split("."))),
                (str(key),),
            )

    async def claim_job(self):
        async with self.transaction():
            conn = self._conn()
            await conn.execute(
                "UPDATE ops.jobs SET status='EXPIRED', document=jsonb_set(document,'{status}','\"EXPIRED\"') WHERE status='QUEUED' AND expires_at <= now()"
            )
            await conn.execute(
                "UPDATE ops.jobs SET status='FAILED', document=jsonb_set(document,'{status}','\"FAILED\"') WHERE status='QUEUED' AND attempts >= max_attempts AND coalesce((document->>'recovery_reclaim')::boolean,false)=false"
            )
            row = await (
                await conn.execute("""UPDATE ops.jobs SET status='RUNNING',
              attempts=attempts+CASE WHEN document->>'recovery_reclaim'='true' THEN 0 ELSE 1 END,
              document=document||jsonb_build_object('status','RUNNING','recovery_reclaim',false,
                'attempts',attempts+CASE WHEN document->>'recovery_reclaim'='true' THEN 0 ELSE 1 END)
              WHERE storage_key=(SELECT storage_key FROM ops.jobs WHERE status='QUEUED' AND run_after<=now()
                AND (expires_at IS NULL OR expires_at>now())
                AND (attempts<max_attempts OR document->>'recovery_reclaim'='true')
                ORDER BY priority,run_after,storage_key FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING document""")
            ).fetchone()
            return row["document"] if row else None

    async def recover_jobs(self):
        async with self.transaction():
            cursor = await self._conn().execute("""UPDATE ops.jobs
              SET status=CASE WHEN expires_at<=now() THEN 'EXPIRED' ELSE 'QUEUED' END,
              document=document||jsonb_build_object('status',CASE WHEN expires_at<=now() THEN 'EXPIRED' ELSE 'QUEUED' END,
                'recovery_reclaim',expires_at IS NULL OR expires_at>now(),'recovery_at',now(),
                'recovery_count',coalesce((document->>'recovery_count')::int,0)+1)
              WHERE status='RUNNING'""")
            return cursor.rowcount


async def _preserved_windows(repo):
    """Permanent MAJOR+ windows, including pre-truth beneficiaries and benchmarks."""
    major_ids = set()
    for grade in ("MAJOR", "STRUCTURAL", "STRUCTURAL_SHOCK"):
        async for row in repo.iter_all("evt.ems_assessments", filters={"class": grade}):
            major_ids.add(row["event_id"])
    benchmarks = {"bitcoin:BTC", "ethereum:ETH", "BTC", "ETH"}
    for symbol in ("BTC", "ETH"):
        async for row in repo.iter_all("reg.tokens", filters={"symbol": symbol}):
            benchmarks.add(row["token_id"])
    windows = []
    for eid in major_ids:
        event = await repo.get("evt.events", eid)
        if event is None:
            continue
        projected = _projection("evt.events", event)
        anchors = [
            parse_time(projected[k])
            for k in ("event_time", "public_at", "known_to_us_at")
            if projected.get(k)
        ]
        if not anchors:
            continue
        tokens = set(benchmarks)
        for item in event.get("tokens_mentioned", event.get("tokens", [])):
            token = item.get("token_id") if isinstance(item, dict) else item
            if token:
                tokens.add(token)
        for collection in ("ana.market_truth", "evt.beneficiary_edges", "evt.capture_chains"):
            async for row in repo.iter_all(collection, filters={"event_id": eid}):
                if row.get("token_id"):
                    tokens.add(row["token_id"])
        async for row in repo.iter_all("evt.beneficiary_maps", filters={"event_id": eid}):
            tokens.update(row.get("analysis_set") or [])
        windows.extend(
            (token, min(anchors) - timedelta(hours=24), max(anchors) + timedelta(hours=72))
            for token in tokens
        )
    return windows


async def _maintain_partitions(self, *, extra_days=None):
    """Create future UTC partitions and atomically split occupied historical DEFAULT days.

    Existing documents and typed values move verbatim. NOT VALID legacy completeness checks
    remain NOT VALID on the attached child, so moving history cannot invent missing data.
    """
    if not isinstance(self, PostgresRepository):
        return 0
    from psycopg import sql

    created = 0
    async with self.lock("tank:partition_maintenance"):
        for collection, ts in PARTITIONED.items():
            schema, name = collection.split(".")
            historical = sql.Identifier(schema, name + "_historical")
            days = {(utcnow() + timedelta(days=offset)).date() for offset in range(15)}
            days.update((extra_days or {}).get(collection, ()))
            rows = await (
                await self._conn().execute(
                    sql.SQL(
                        "SELECT DISTINCT date_trunc('day',{} AT TIME ZONE 'UTC')::date AS day FROM {} WHERE {} IS NOT NULL"
                    ).format(sql.Identifier(ts), historical, sql.Identifier(ts))
                )
            ).fetchall()
            days.update(row["day"] for row in rows)
            for day in sorted(days):
                partition_name = name + "_" + day.strftime("%Y%m%d")
                exists = await (
                    await self._conn().execute(
                        "SELECT to_regclass(%s) AS name", (schema + "." + partition_name,)
                    )
                ).fetchone()
                if exists["name"] is not None:
                    continue
                start = parse_time(day.isoformat())
                end = start + timedelta(days=1)
                await self._conn().execute(
                    sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(sql.Identifier(schema, name))
                )
                occupied = await (
                    await self._conn().execute(
                        sql.SQL("SELECT 1 FROM {} WHERE {} >= %s AND {} < %s LIMIT 1").format(
                            historical, sql.Identifier(ts), sql.Identifier(ts)
                        ),
                        (start, end),
                    )
                ).fetchone()
                child = sql.Identifier(schema, partition_name)
                parent = sql.Identifier(schema, name)
                if occupied:
                    # Standalone child accepts pre-existing legacy rows before matching inherited
                    # constraints are reattached with their original validation state.
                    await self._conn().execute(
                        sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING STORAGE)").format(
                            child, parent
                        )
                    )
                    copied = await self._conn().execute(
                        sql.SQL("INSERT INTO {} SELECT * FROM {} WHERE {} >= %s AND {} < %s").format(
                            child, historical, sql.Identifier(ts), sql.Identifier(ts)
                        ),
                        (start, end),
                    )
                    copied_count = copied.rowcount
                    checks = await (
                        await self._conn().execute(
                            "SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid=%s::regclass AND contype='c'",
                            (collection,),
                        )
                    ).fetchall()
                    for check in checks:
                        await self._conn().execute(
                            sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} {}").format(
                                child, sql.Identifier(check["conname"]), sql.SQL(check["definition"])
                            )
                        )
                    await self._conn().execute("SELECT set_config('tank.partition_relocation','on',true)")
                    removed = await self._conn().execute(
                        sql.SQL("DELETE FROM {} WHERE {} >= %s AND {} < %s").format(
                            historical, sql.Identifier(ts), sql.Identifier(ts)
                        ),
                        (start, end),
                    )
                    if removed.rowcount != copied_count:
                        raise RuntimeError("Partition relocation row count mismatch")
                    await self._conn().execute("SELECT set_config('tank.partition_relocation','off',true)")
                    await self._conn().execute(
                        sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES FROM ({}) TO ({})").format(
                            parent, child, sql.Literal(start), sql.Literal(end)
                        )
                    )
                else:
                    await self._conn().execute(
                        sql.SQL("CREATE TABLE {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})").format(
                            child, parent, sql.Literal(start), sql.Literal(end)
                        )
                    )
                created += 1
    return created


MemoryRepository.maintain_partitions = _maintain_partitions
PostgresRepository.maintain_partitions = _maintain_partitions


async def _prune_market(self, before):
    """Prune capture after backup, preserving whole UTC partitions overlapping MAJOR+ windows.

    Normalized series use the caller's cutoff (normally90days); raw trades use30days.
    Normal mutation/deletion of immutable raw evidence remains forbidden.
    """
    cutoff = parse_time(before)
    temporal = {
        "mkt.klines_1m": "open_time",
        "mkt.flow_bars_1m": "minute_start",
        "mkt.book_summaries": "exchange_ts",
        "mkt.oi_obs": "ts",
        "mkt.funding_obs": "as_of",
        "mkt.liq_obs": "ts",
        "mkt.tickers_24h": "ts",
        "mkt.trades": "ts",
        "mkt.supply_obs": "ts",
    }
    counts = {}
    async with self.lock("tank:market_retention"):
        # Retention is by whole UTC partition across all instruments, including benchmarks.
        days = set()
        for _, start, end in await _preserved_windows(self):
            day = start.replace(hour=0, minute=0, second=0, microsecond=0)
            last = end.replace(hour=0, minute=0, second=0, microsecond=0)
            while day <= last:
                days.add(day)
                day += timedelta(days=1)
        spans = []
        for day in sorted(days):
            if spans and spans[-1][1] == day:
                spans[-1] = (spans[-1][0], day + timedelta(days=1))
            else:
                spans.append((day, day + timedelta(days=1)))
        if isinstance(self, PostgresRepository):
            from psycopg import sql

            await self.maintain_partitions()
            await self._conn().execute("SELECT set_config('tank.retention_purge','on',true)")
            for collection, ts in temporal.items():
                age = utcnow() - timedelta(days=30) if collection == "mkt.trades" else cutoff
                args = [age]
                clauses = []
                for start, end in spans:
                    clauses.append(
                        sql.SQL("({} >= %s AND {} < %s)").format(sql.Identifier(ts), sql.Identifier(ts))
                    )
                    args.extend([start, end])
                protect = sql.SQL(" OR ").join(clauses) if clauses else sql.SQL("FALSE")
                mutable = sql.SQL("TRUE") if collection == "mkt.trades" else sql.SQL("NOT immutable")
                cursor = await self._conn().execute(
                    sql.SQL("DELETE FROM {} WHERE {} < %s AND {} AND NOT ({})").format(
                        sql.Identifier(*collection.split(".")), sql.Identifier(ts), mutable, protect
                    ),
                    args,
                )
                counts[collection] = cursor.rowcount
            await self._conn().execute("SELECT set_config('tank.retention_purge','off',true)")
        else:
            for collection, ts in temporal.items():
                age = utcnow() - timedelta(days=30) if collection == "mkt.trades" else cutoff
                deleted = 0
                for key, row in list(self._data.get(collection, {}).items()):
                    if not row.get(ts):
                        continue
                    at = parse_time(row[ts])
                    protected = any(start <= at < end for start, end in spans)
                    if (
                        at < age
                        and not protected
                        and (collection == "mkt.trades" or (collection, key) not in self._immutable)
                    ):
                        del self._data[collection][key]
                        self._immutable.discard((collection, key))
                        deleted += 1
                counts[collection] = deleted
    return counts


MemoryRepository.prune_market = _prune_market
PostgresRepository.prune_market = _prune_market


async def _range_page(
    self, collection, time_field, *, start=None, end=None, filters=None, limit=1000, after=None
):
    """Return (document, (timestamp, storage key)) pairs for deterministic keyset pagination."""
    _table(collection)
    if TABLE_COLUMNS[collection].get(time_field) != "timestamptz":
        raise ValueError(f"{collection}.{time_field} is not a typed timestamp column")
    if limit < 1:
        raise ValueError("limit must be positive")
    start_dt, end_dt = parse_time(start) if start else None, parse_time(end) if end else None
    if isinstance(self, PostgresRepository):
        from psycopg import sql
        from psycopg.types.json import Jsonb

        clauses, args = [sql.SQL("{} IS NOT NULL").format(sql.Identifier(time_field))], []
        for op, bound in [(">=", start_dt), ("<", end_dt)]:
            if bound is not None:
                clauses.append(sql.SQL("{} " + op + " %s").format(sql.Identifier(time_field)))
                args.append(bound)
        if after is not None:
            clauses.append(sql.SQL("({},storage_key) > (%s,%s)").format(sql.Identifier(time_field)))
            args.extend([parse_time(after[0]), str(after[1])])
        if filters:
            # Containment hits the GIN document index; equality preserves exact top-level objects.
            clauses.append(sql.SQL("document @> %s"))
            args.append(Jsonb(jsonable(filters)))
            for key, value in filters.items():
                clauses.append(sql.SQL("document -> %s = %s"))
                args.extend([key, Jsonb(jsonable(value))])
                if TABLE_COLUMNS[collection].get(key) == "text" and isinstance(value, str):
                    clauses.append(sql.SQL("{} = %s").format(sql.Identifier(key)))
                    args.append(value)
        async with self.transaction():
            query = sql.SQL(
                "SELECT document, {}, storage_key FROM {} WHERE {} ORDER BY {},storage_key LIMIT %s"
            ).format(
                sql.Identifier(time_field),
                sql.Identifier(*collection.split(".")),
                sql.SQL(" AND ").join(clauses),
                sql.Identifier(time_field),
            )
            rows = await (await self._conn().execute(query, [*args, limit])).fetchall()
            return [(r["document"], (iso(r[time_field]), r["storage_key"])) for r in rows]
    async with self.transaction():
        matches = []
        for key, document in self._data.get(collection, {}).items():
            timestamp = _projection(collection, document).get(time_field)
            if timestamp is None:
                continue
            timestamp = parse_time(timestamp)
            if start_dt and timestamp < start_dt or end_dt and timestamp >= end_dt:
                continue
            if after and (timestamp, key) <= (parse_time(after[0]), str(after[1])):
                continue
            if all(k in document and document[k] == v for k, v in (filters or {}).items()):
                matches.append((copy.deepcopy(document), (iso(timestamp), key)))
        matches.sort(key=lambda r: (parse_time(r[1][0]), r[1][1]))
        return matches[:limit]


async def _list_range(
    self, collection, time_field, *, start=None, end=None, filters=None, limit=1000, after=None
):
    return [
        r[0]
        for r in await _range_page(
            self, collection, time_field, start=start, end=end, filters=filters, limit=limit, after=after
        )
    ]


async def _iter_range(self, collection, time_field, *, start=None, end=None, filters=None, batch_size=10000):
    after = None
    while True:
        page = await _range_page(
            self, collection, time_field, start=start, end=end, filters=filters, limit=batch_size, after=after
        )
        for document, _ in page:
            yield document
        if len(page) < batch_size:
            return
        after = page[-1][1]


MemoryRepository.list_range = _list_range
PostgresRepository.list_range = _list_range
MemoryRepository.iter_range = _iter_range
PostgresRepository.iter_range = _iter_range


async def _prune_jobs(self, before):
    """Prune aged terminal jobs; retain references needed by jobs, sessions or tool recovery.

    Missing terminal timestamps are preserved. Queue payloads and persisted session/tool state
    are searched recursively for exact job IDs, including custom dependency field names.
    """
    cutoff = parse_time(before)

    def strings(value):
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for nested in value.values():
                yield from strings(nested)
        elif isinstance(value, list):
            for nested in value:
                yield from strings(nested)

    async with self.lock("tank:job_retention"):
        candidates_by_id = set()
        for status in ("DONE", "FAILED", "EXPIRED"):
            async for job in self.iter_range(
                "ops.jobs", "finished_at", end=cutoff, filters={"status": status}
            ):
                candidates_by_id.add(job["job_id"])
        if not candidates_by_id:
            return 0
        references = set()
        async for job in self.iter_all("ops.jobs"):
            references.update(value for value in strings(job.get("payload")) if value in candidates_by_id)
            for key in ("depends_on", "dependencies", "parent_job_id", "job_ids"):
                references.update(value for value in strings(job.get(key)) if value in candidates_by_id)
        for collection in ("ops.agent_sessions", "ops.tool_calls", "ops.routing_reservations"):
            async for record in self.iter_all(collection):
                references.update(value for value in strings(record) if value in candidates_by_id)
        candidates = list(candidates_by_id - references)
        if not candidates:
            return 0
        if isinstance(self, PostgresRepository):
            cursor = await self._conn().execute(
                """DELETE FROM ops.jobs WHERE job_id=ANY(%s)
                AND status IN ('DONE','FAILED','EXPIRED') AND finished_at<%s AND NOT immutable""",
                (candidates, cutoff),
            )
            return cursor.rowcount
        count = 0
        for job_id in candidates:
            if ("ops.jobs", job_id) not in self._immutable:
                self._data["ops.jobs"].pop(job_id, None)
                count += 1
        return count


MemoryRepository.prune_jobs = _prune_jobs
PostgresRepository.prune_jobs = _prune_jobs
MemoryRepository.preserved_market_windows = _preserved_windows
PostgresRepository.preserved_market_windows = _preserved_windows


async def _latest_before(self, collection, time_field, cutoff, *, filters=None):
    """Last actual observation strictly before cutoff, using typed time and stable key indexes."""
    _table(collection)
    if TABLE_COLUMNS[collection].get(time_field) != "timestamptz":
        raise ValueError(f"{collection}.{time_field} is not a typed timestamp column")
    cutoff = parse_time(cutoff)
    if isinstance(self, PostgresRepository):
        from psycopg import sql
        from psycopg.types.json import Jsonb

        clauses, args = [sql.SQL("{} < %s").format(sql.Identifier(time_field))], [cutoff]
        if filters:
            clauses.append(sql.SQL("document @> %s"))
            args.append(Jsonb(jsonable(filters)))
            for key, value in filters.items():
                clauses.append(sql.SQL("document -> %s = %s"))
                args.extend([key, Jsonb(jsonable(value))])
                if TABLE_COLUMNS[collection].get(key) == "text" and isinstance(value, str):
                    clauses.append(sql.SQL("{}=%s").format(sql.Identifier(key)))
                    args.append(value)
        async with self.transaction():
            row = await (
                await self._conn().execute(
                    sql.SQL(
                        "SELECT document FROM {} WHERE {} ORDER BY {} DESC,storage_key DESC LIMIT 1"
                    ).format(
                        sql.Identifier(*collection.split(".")),
                        sql.SQL(" AND ").join(clauses),
                        sql.Identifier(time_field),
                    ),
                    args,
                )
            ).fetchone()
            return row["document"] if row else None
    async with self.transaction():
        candidates = []
        for key, row in self._data.get(collection, {}).items():
            at = _projection(collection, row).get(time_field)
            if at is None:
                continue
            at = parse_time(at)
            if at < cutoff and all(k in row and row[k] == v for k, v in (filters or {}).items()):
                candidates.append((at, key, row))
        return copy.deepcopy(max(candidates, key=lambda item: (item[0], item[1]))[2]) if candidates else None


MemoryRepository.latest_before = _latest_before
PostgresRepository.latest_before = _latest_before


def _exact_predicate(collection, filters):
    from psycopg import sql
    from psycopg.types.json import Jsonb

    clauses, args = [], []
    if filters:
        clauses.append(sql.SQL("document @> %s"))
        args.append(Jsonb(jsonable(filters)))
        for key, value in filters.items():
            clauses.append(sql.SQL("document -> %s = %s"))
            args.extend([key, Jsonb(jsonable(value))])
            datatype = TABLE_COLUMNS[collection].get(key)
            if datatype == "text" and isinstance(value, str):
                clauses.append(sql.SQL("{}=%s").format(sql.Identifier(key)))
                args.append(value)
    return clauses, args


async def _iter_all(self, collection, *, filters=None, batch_size=1000):
    """Read all matching documents without a cap, using stable storage-key pages.

    Each page observes committed state. Call inside transaction when atomicity with writes is
    required; records inserted with keys before an already-read cursor are outside this scan.
    """
    _table(collection)
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    after = None
    while True:
        if isinstance(self, PostgresRepository):
            from psycopg import sql

            clauses, args = _exact_predicate(collection, filters)
            if after is not None:
                clauses.append(sql.SQL("storage_key>%s"))
                args.append(after)
            predicate = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
            async with self.transaction():
                query = sql.SQL(
                    "SELECT document,storage_key FROM {} WHERE {} ORDER BY storage_key LIMIT %s"
                ).format(sql.Identifier(*collection.split(".")), predicate)
                rows = await (await self._conn().execute(query, [*args, batch_size])).fetchall()
            page = [(r["storage_key"], r["document"]) for r in rows]
        else:
            async with self.transaction():
                page = []
                for key, row in sorted(self._data.get(collection, {}).items()):
                    if after is not None and key <= after:
                        continue
                    if all(k in row and row[k] == v for k, v in (filters or {}).items()):
                        page.append((key, copy.deepcopy(row)))
                        if len(page) == batch_size:
                            break
        for _, document in page:
            yield document
        if len(page) < batch_size:
            return
        after = page[-1][0]


async def _list_latest(self, collection, time_field, *, filters=None, limit=100, start=None, end=None):
    """Latest typed timestamp records, ordered by timestamp then stable key descending.

    Optional bounds are start-inclusive/end-exclusive. Null timestamps cannot represent a
    latest observation and are excluded. Filtering happens in SQL before the limit.
    """
    _table(collection)
    if TABLE_COLUMNS[collection].get(time_field) != "timestamptz":
        raise ValueError(f"{collection}.{time_field} is not a typed timestamp column")
    if limit < 0:
        raise ValueError("limit must be nonnegative")
    if limit == 0:
        return []
    if isinstance(self, PostgresRepository):
        from psycopg import sql

        clauses, args = _exact_predicate(collection, filters)
        clauses.append(sql.SQL("{} IS NOT NULL").format(sql.Identifier(time_field)))
        for bound, operator in ((start, ">="), (end, "<")):
            if bound is not None:
                clauses.append(sql.SQL("{} " + operator + " %s").format(sql.Identifier(time_field)))
                args.append(parse_time(bound))
        async with self.transaction():
            query = sql.SQL(
                "SELECT document FROM {} WHERE {} ORDER BY {} DESC,storage_key DESC LIMIT %s"
            ).format(
                sql.Identifier(*collection.split(".")),
                sql.SQL(" AND ").join(clauses),
                sql.Identifier(time_field),
            )
            rows = await (await self._conn().execute(query, [*args, limit])).fetchall()
            return [r["document"] for r in rows]
    async with self.transaction():
        candidates = []
        for key, row in self._data.get(collection, {}).items():
            timestamp = _projection(collection, row).get(time_field)
            if timestamp is None or not all(k in row and row[k] == v for k, v in (filters or {}).items()):
                continue
            timestamp = parse_time(timestamp)
            if (
                start is not None
                and timestamp < parse_time(start)
                or end is not None
                and timestamp >= parse_time(end)
            ):
                continue
            candidates.append((timestamp, key, row))
        return [
            copy.deepcopy(r[2]) for r in sorted(candidates, key=lambda r: (r[0], r[1]), reverse=True)[:limit]
        ]


MemoryRepository.iter_all = _iter_all
PostgresRepository.iter_all = _iter_all
MemoryRepository.list_latest = _list_latest
PostgresRepository.list_latest = _list_latest


async def _iter_late_arrivals(
    self,
    collection,
    arrival_field,
    market_field,
    *,
    since=None,
    end=None,
    market_end,
    filters=None,
    batch_size=1000,
):
    """Arrival pages bounded independently from the inclusive frozen semantic market cutoff.

    Unknown legacy availability is excluded rather than backdated. Consumers normally pass a
    durable availability checkpoint as since and the scheduler tick as end. Both timestamps
    are typed and indexed; no complete market history is loaded to discover late arrivals.
    """
    _table(collection)
    for field in (arrival_field, market_field):
        if TABLE_COLUMNS[collection].get(field) != "timestamptz":
            raise ValueError(f"{collection}.{field} is not a typed timestamp column")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    since = parse_time(since) if since is not None else None
    end = parse_time(end) if end is not None else None
    market_end = parse_time(market_end)
    after = None
    while True:
        if isinstance(self, PostgresRepository):
            from psycopg import sql

            clauses, args = _exact_predicate(collection, filters)
            clauses += [
                sql.SQL("{} IS NOT NULL").format(sql.Identifier(arrival_field)),
                sql.SQL("{} <= %s").format(sql.Identifier(market_field)),
            ]
            args.append(market_end)
            if since is not None:
                clauses.append(sql.SQL("{} >= %s").format(sql.Identifier(arrival_field)))
                args.append(since)
            if end is not None:
                clauses.append(sql.SQL("{} < %s").format(sql.Identifier(arrival_field)))
                args.append(end)
            if after is not None:
                clauses.append(sql.SQL("({},storage_key)>(%s,%s)").format(sql.Identifier(arrival_field)))
                args.extend(after)
            async with self.transaction():
                query = sql.SQL(
                    "SELECT document,{},storage_key FROM {} WHERE {} ORDER BY {},storage_key LIMIT %s"
                ).format(
                    sql.Identifier(arrival_field),
                    sql.Identifier(*collection.split(".")),
                    sql.SQL(" AND ").join(clauses),
                    sql.Identifier(arrival_field),
                )
                rows = await (await self._conn().execute(query, [*args, batch_size])).fetchall()
                page = [(row["document"], (row[arrival_field], row["storage_key"])) for row in rows]
        else:
            async with self.transaction():
                page = []
                for key, row in self._data.get(collection, {}).items():
                    projected = _projection(collection, row)
                    arrival, market = projected.get(arrival_field), projected.get(market_field)
                    if arrival is None or market is None:
                        continue
                    arrival, market = parse_time(arrival), parse_time(market)
                    if (
                        market > market_end
                        or since is not None
                        and arrival < since
                        or end is not None
                        and arrival >= end
                    ):
                        continue
                    if after is not None and (arrival, key) <= after:
                        continue
                    if all(k in row and row[k] == v for k, v in (filters or {}).items()):
                        page.append((copy.deepcopy(row), (arrival, key)))
                page.sort(key=lambda item: item[1])
                page = page[:batch_size]
        for document, _ in page:
            yield document
        if len(page) < batch_size:
            return
        after = page[-1][1]


MemoryRepository.iter_late_arrivals = _iter_late_arrivals
PostgresRepository.iter_late_arrivals = _iter_late_arrivals


async def _iter_metadata(self, collection, fields, *, filters=None, batch_size=1000):
    """Page selected top-level JSON fields without returning the full stored evidence payload.

    Keys absent from a record appear as None in the projection; exact filtering continues to
    operate on the original document, preserving missing-vs-null filter semantics.
    """
    _table(collection)
    fields = list(fields)
    if not fields or len(fields) > 50 or any(not isinstance(field, str) for field in fields):
        raise ValueError("fields must contain1..50 top-level string names")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if not isinstance(self, PostgresRepository):
        async for row in self.iter_all(collection, filters=filters, batch_size=batch_size):
            yield {field: row.get(field) for field in fields}
        return
    from psycopg import sql

    after = None
    expressions = sql.SQL(",").join(sql.SQL("%s::text,document -> %s::text") for _ in fields)
    projected_args = [key for field in fields for key in (field, field)]
    while True:
        clauses, args = _exact_predicate(collection, filters)
        if after is not None:
            clauses.append(sql.SQL("storage_key>%s"))
            args.append(after)
        predicate = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
        async with self.transaction():
            query = sql.SQL(
                "SELECT jsonb_build_object({}) AS metadata,storage_key FROM {} WHERE {} ORDER BY storage_key LIMIT %s"
            ).format(expressions, sql.Identifier(*collection.split(".")), predicate)
            page = await (await self._conn().execute(query, [*projected_args, *args, batch_size])).fetchall()
        for row in page:
            yield row["metadata"]
        if len(page) < batch_size:
            return
        after = page[-1]["storage_key"]


MemoryRepository.iter_metadata = _iter_metadata
PostgresRepository.iter_metadata = _iter_metadata
