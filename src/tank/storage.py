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
from typing import Any, Protocol

from tank.models import APPEND_ONLY, TABLE_COLUMNS, Record
from tank.util import iso, jsonable, new_id, parse_time, utcnow


class ImmutableRecordError(ValueError):
    pass


class Repository(Protocol):
    async def open(self): ...
    async def close(self): ...
    async def migrate(self): ...
    async def put(self, collection: str, key: str, record: Record, *, immutable: bool = False) -> Record: ...
    async def get(self, collection: str, key: str) -> Record | None: ...
    async def list(self, collection: str, *, filters: dict | None = None, limit: int = 1000) -> list[Record]: ...
    async def delete(self, collection: str, key: str): ...
    def transaction(self): ...
    def lock(self, key: str): ...
    async def enqueue(self, kind, payload, idem_key, *, priority=1, run_after=None, expires_at=None, max_attempts=3): ...
    async def claim_job(self): ...
    async def finish_job(self, job_id): ...
    async def fail_job(self, job_id, error, *, retry_after=1): ...
    async def recover_jobs(self): ...


def _table(collection):
    if collection not in TABLE_COLUMNS:
        raise ValueError(f"Unknown collection: {collection}")
    return collection


def _job(kind, payload, idem_key, priority, run_after, expires_at, max_attempts):
    if not 0 <= priority <= 4 or max_attempts < 1:
        raise ValueError('priority must be 0..4 and max_attempts positive')
    return dict(job_id=new_id('JOB'), type=kind, priority=priority, idem_key=idem_key,
                payload=jsonable(payload), status='QUEUED', attempts=0, max_attempts=max_attempts,
                run_after=iso(parse_time(run_after)) if run_after else iso(),
                expires_at=iso(parse_time(expires_at)) if expires_at else None, last_error=None)


class QueueMixin:
    async def enqueue(self, kind, payload, idem_key, *, priority=1, run_after=None, expires_at=None, max_attempts=3):
        async with self.lock(f'queue:{kind}:{idem_key}'):
            old = await self.list('ops.jobs', filters={'type': kind, 'idem_key': idem_key}, limit=1)
            if old:
                return old[0]
            job = _job(kind, payload, idem_key, priority, run_after, expires_at, max_attempts)
            return await self.put('ops.jobs', job['job_id'], job)

    async def finish_job(self, job_id):
        await self._transition_job(job_id, 'DONE')

    async def fail_job(self, job_id, error, *, retry_after=1):
        async with self.lock(f'job:{job_id}'):
            job = await self.get('ops.jobs', job_id)
            if job is None:
                raise KeyError(job_id)
            if job['status'] != 'RUNNING':
                return
            job['last_error'] = str(error)
            expired = job.get('expires_at') and parse_time(job['expires_at']) <= utcnow()
            job['status'] = 'EXPIRED' if expired else ('FAILED' if job['attempts'] >= job['max_attempts'] else 'QUEUED')
            job['run_after'] = iso(utcnow() + timedelta(seconds=retry_after))
            await self.put('ops.jobs', job_id, job)

    async def _transition_job(self, job_id, status):
        async with self.lock(f'job:{job_id}'):
            job = await self.get('ops.jobs', job_id)
            if job is None:
                raise KeyError(job_id)
            if job['status'] == 'RUNNING':
                job['status'] = status
                await self.put('ops.jobs', job_id, job)


class MemoryRepository(QueueMixin):
    """Deterministic test implementation with nested rollback and task-local ownership."""
    def __init__(self):
        self._data = {}
        self._immutable = set()
        self._mutex = asyncio.Lock()
        self._owner = None
        self._locks = {}
        self._held = ContextVar(f'memory_locks_{id(self)}', default=frozenset())

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
        snapshot = copy.deepcopy((self._data, self._immutable))
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
            if old is not None and ((collection, key) in self._immutable or collection in APPEND_ONLY or immutable):
                if old != record:
                    raise ImmutableRecordError(f'Immutable record: {collection}/{key}')
                if immutable:
                    self._immutable.add((collection, key))
                return copy.deepcopy(old)
            if collection == 'ops.jobs':
                if any(r['type'] == record['type'] and r['idem_key'] == record['idem_key'] and k != key
                       for k, r in self._data.get(collection, {}).items()):
                    raise ValueError('Duplicate job idempotency key')
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
            raise ValueError('limit must be nonnegative')
        async with self.transaction():
            return copy.deepcopy([r for r in self._data.get(collection, {}).values()
                                  if all(k in r and r[k] == v for k, v in (filters or {}).items())][:limit])

    async def delete(self, collection, key):
        _table(collection)
        async with self.transaction():
            if collection in APPEND_ONLY or (collection, str(key)) in self._immutable:
                raise ImmutableRecordError(f'Immutable record: {collection}/{key}')
            self._data.get(collection, {}).pop(str(key), None)

    async def claim_job(self):
        async with self.transaction():
            now = utcnow()
            candidates = []
            for key, job in self._data.get('ops.jobs', {}).items():
                if job['status'] != 'QUEUED':
                    continue
                if job.get('expires_at') and parse_time(job['expires_at']) <= now:
                    job['status'] = 'EXPIRED'
                elif job['attempts'] >= job['max_attempts']:
                    job['status'] = 'FAILED'
                elif parse_time(job['run_after']) <= now:
                    candidates.append(job)
            if not candidates:
                return None
            job = min(candidates, key=lambda r: (r['priority'], r['run_after'], r['job_id']))
            job['status'] = 'RUNNING'
            job['attempts'] += 1
            return copy.deepcopy(job)

    async def recover_jobs(self):
        async with self.transaction():
            count = 0
            for job in self._data.get('ops.jobs', {}).values():
                if job['status'] == 'RUNNING':
                    job['status'] = ('EXPIRED' if job.get('expires_at') and parse_time(job['expires_at']) <= utcnow()
                                     else 'FAILED' if job['attempts'] >= job['max_attempts'] else 'QUEUED')
                    count += 1
            return count


def _projection(collection, record):
    """Map §2 nested names to §14 indexed columns without changing the document."""
    values = dict(record)
    aliases = {'body': 'text', 'raw_path': 'raw_ref', 'claim_type': 'type', 'gates': 'structural_gates',
               'buy_usd': 'buy_notional', 'sell_usd': 'sell_notional', 'o': 'open', 'h': 'high',
               'l': 'low', 'c': 'close', 'funding_time': 'settlement_time'}
    for column, alias in aliases.items():
        if column not in values and alias in values:
            values[column] = values[alias]
    if collection == 'ingest.source_items':
        for obj, mappings in [('relevance', {'decision': 'relevance_decision', 'p_relevant': 'relevance_p'}),
                              ('route', {'decision': 'route_decision', 'event_id': 'route_event_id', 'p_same': 'route_p'})]:
            for source, target in mappings.items():
                if isinstance(record.get(obj), dict) and source in record[obj]:
                    values[target] = record[obj][source]
    if collection == 'ana.calls' and isinstance(record.get('opened_by'), (list, tuple)):
        values['opened_by_alert'], values['opened_by_version'] = record['opened_by']
    return {k: values.get(k) for k in TABLE_COLUMNS[collection]}


class PostgresRepository(QueueMixin):
    def __init__(self, database_url):
        self.database_url = database_url
        self._pool = None
        self._connection = ContextVar(f'pg_connection_{id(self)}', default=None)

    async def open(self):
        from psycopg_pool import AsyncConnectionPool
        from psycopg.rows import dict_row
        self._pool = AsyncConnectionPool(self.database_url, open=False, min_size=1, max_size=16,
                                         kwargs={'row_factory': dict_row})
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
                raise RuntimeError('Do not share a transaction between concurrent tasks')
            async with conn.transaction():
                yield self
            return
        if self._pool is None:
            raise RuntimeError('Repository.open() must be called first')
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
            digest = int.from_bytes(hashlib.sha256(str(key).encode()).digest()[:8], 'big', signed=True)
            await self._conn().execute('SELECT pg_advisory_xact_lock(%s)', (digest,))
            yield self

    async def migrate(self):
        async with self.lock('tank:migrations'):
            conn = self._conn()
            await conn.execute('CREATE SCHEMA IF NOT EXISTS ops')
            await conn.execute('CREATE TABLE IF NOT EXISTS ops.schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())')
            for path in sorted((Path(__file__).parent / 'migrations').glob('*.sql')):
                exists = await (await conn.execute('SELECT 1 FROM ops.schema_migrations WHERE version=%s', (path.name,))).fetchone()
                if exists is None:
                    await conn.execute(path.read_text(), prepare=False)
                    await conn.execute('INSERT INTO ops.schema_migrations(version) VALUES (%s)', (path.name,))

    async def put(self, collection, key, record, *, immutable=False):
        from psycopg import sql
        from psycopg.types.json import Jsonb
        _table(collection)
        key, record = str(key), jsonable(record)
        async with self.lock(f'record:{collection}:{key}'):
            conn = self._conn()
            table = sql.Identifier(*collection.split('.'))
            old = await (await conn.execute(sql.SQL('SELECT document, immutable FROM {} WHERE storage_key=%s FOR UPDATE').format(table), (key,))).fetchone()
            if old and (old['immutable'] or immutable or collection in APPEND_ONLY):
                if old['document'] != record:
                    raise ImmutableRecordError(f'Immutable record: {collection}/{key}')
                if immutable and not old['immutable']:
                    await conn.execute(sql.SQL('UPDATE {} SET immutable=true WHERE storage_key=%s').format(table), (key,))
                return old['document']
            projected = _projection(collection, record)
            values = [key, Jsonb(record), immutable or collection in APPEND_ONLY]
            for column, value in projected.items():
                datatype = TABLE_COLUMNS[collection][column]
                if value is not None:
                    if datatype == 'jsonb':
                        value = Jsonb(value)
                    elif datatype == 'timestamptz':
                        value = parse_time(value)
                    elif datatype == 'text' and not isinstance(value, str):
                        value = json.dumps(value, sort_keys=True)
                values.append(value)
            names = ['storage_key', 'document', 'immutable', *projected]
            columns = sql.SQL(',').join(map(sql.Identifier, names))
            assignments = sql.SQL(',').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(n), sql.Identifier(n)) for n in names[1:])
            query = sql.SQL('INSERT INTO {} ({}) VALUES ({}) ON CONFLICT(storage_key) DO UPDATE SET {}').format(
                table, columns, sql.SQL(',').join(sql.Placeholder() for _ in names), assignments)
            await conn.execute(query, values)
            return copy.deepcopy(record)

    async def get(self, collection, key):
        from psycopg import sql
        _table(collection)
        async with self.transaction():
            row = await (await self._conn().execute(sql.SQL('SELECT document FROM {} WHERE storage_key=%s').format(sql.Identifier(*collection.split('.'))), (str(key),))).fetchone()
            return row['document'] if row else None

    async def list(self, collection, *, filters=None, limit=1000):
        from psycopg import sql
        from psycopg.types.json import Jsonb
        _table(collection)
        if limit < 0:
            raise ValueError('limit must be nonnegative')
        # JSONB containment preserves exact structured top-level values and nulls.
        clauses, args = [], []
        for k, v in (filters or {}).items():
            clauses.append(sql.SQL('document -> %s = %s'))
            args.extend([k, Jsonb(jsonable(v))])
        where = sql.SQL(' AND ').join(clauses) if clauses else sql.SQL('TRUE')
        async with self.transaction():
            rows = await (await self._conn().execute(sql.SQL('SELECT document FROM {} WHERE {} ORDER BY storage_key LIMIT %s').format(sql.Identifier(*collection.split('.')), where), [*args, limit])).fetchall()
            return [r['document'] for r in rows]

    async def delete(self, collection, key):
        from psycopg import sql
        _table(collection)
        async with self.lock(f'record:{collection}:{key}'):
            row = await (await self._conn().execute(sql.SQL('SELECT immutable FROM {} WHERE storage_key=%s FOR UPDATE').format(sql.Identifier(*collection.split('.'))), (str(key),))).fetchone()
            if collection in APPEND_ONLY or row and row['immutable']:
                raise ImmutableRecordError(f'Immutable record: {collection}/{key}')
            await self._conn().execute(sql.SQL('DELETE FROM {} WHERE storage_key=%s').format(sql.Identifier(*collection.split('.'))), (str(key),))

    async def claim_job(self):
        async with self.transaction():
            conn = self._conn()
            await conn.execute("UPDATE ops.jobs SET status='EXPIRED', document=jsonb_set(document,'{status}','\"EXPIRED\"') WHERE status='QUEUED' AND expires_at <= now()")
            await conn.execute("UPDATE ops.jobs SET status='FAILED', document=jsonb_set(document,'{status}','\"FAILED\"') WHERE status='QUEUED' AND attempts >= max_attempts")
            row = await (await conn.execute("""UPDATE ops.jobs SET status='RUNNING', attempts=attempts+1,
                document=jsonb_set(jsonb_set(document,'{status}','"RUNNING"'),'{attempts}',to_jsonb(attempts+1))
                WHERE storage_key=(SELECT storage_key FROM ops.jobs WHERE status='QUEUED' AND run_after<=now()
                 AND (expires_at IS NULL OR expires_at>now()) AND attempts<max_attempts
                 ORDER BY priority,run_after,storage_key FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING document""")).fetchone()
            return row['document'] if row else None

    async def recover_jobs(self):
        async with self.transaction():
            cursor = await self._conn().execute("""UPDATE ops.jobs SET status=CASE WHEN expires_at<=now() THEN 'EXPIRED'
              WHEN attempts>=max_attempts THEN 'FAILED' ELSE 'QUEUED' END,
              document=jsonb_set(document,'{status}',to_jsonb((CASE WHEN expires_at<=now() THEN 'EXPIRED'
              WHEN attempts>=max_attempts THEN 'FAILED' ELSE 'QUEUED' END)::text)) WHERE status='RUNNING'""")
            return cursor.rowcount
