"""Disposable PostgreSQL test databases; no resources are opened for memory tests.

The caller supplies a guarded admin DSN whose tank_test* database outlives this
factory. Each worker owns its factory. Templates keep schema only, with no open
connections between tests; every case receives a separate mutable database.
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from tank.storage import PostgresRepository


def _connection_identity(admin_dsn):
    params = conninfo_to_dict(admin_dsn)
    if not params.get("dbname", "").startswith("tank_test"):
        raise ValueError("PostgreSQL test admin must select a dedicated tank_test* database")
    # Values can include credentials: this private key is never logged or exposed.
    # A child database on the same server/auth configuration uses the same template.
    identity = dict(params)
    identity.pop("dbname")
    identity.setdefault("port", "5432")
    return tuple(sorted(identity.items()))


async def _finish(operation):
    """Finish owned resource control/cleanup even if its caller is cancelled again."""
    task = asyncio.ensure_future(operation)
    cancellation = None
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError as error:
            if task.cancelled():
                raise
            cancellation = cancellation or error
    if cancellation is not None:
        raise cancellation
    return result


async def _drop_database(admin_dsn, name):
    async def drop():
        admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
        try:
            await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        finally:
            await admin.close()

    await _finish(drop())


async def _create_database(admin_dsn, template=None):
    _connection_identity(admin_dsn)
    name = "tank_test_fixture_" + uuid.uuid4().hex
    command = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
    if template is not None:
        command += sql.SQL(" TEMPLATE {}").format(sql.Identifier(template))
    created = False

    async def create():
        nonlocal created
        admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
        try:
            await admin.execute(command)
            created = True
        finally:
            await admin.close()

    try:
        await _finish(create())
    except BaseException:
        if created:
            await _drop_database(admin_dsn, name)
        raise
    return name


class PostgresTemplate:
    """Lazy per-worker template owner, safe to pass to a memory-backed fixture."""

    def __init__(self):
        self._templates = {}
        self._building = {}

    async def _template(self, admin_dsn):
        identity = _connection_identity(admin_dsn)
        if identity in self._templates:
            return self._templates[identity][1]
        # Only active callers share this loop-local lock. The last caller removes
        # it, leaving no task/lock/pool bound to a previous test's event loop.
        state = self._building.get(identity)
        if state is None:
            state = SimpleNamespace(lock=asyncio.Lock(), users=0)
            self._building[identity] = state
        state.users += 1
        try:
            async with state.lock:
                if identity not in self._templates:
                    name = await _create_database(admin_dsn)
                    repo = PostgresRepository(make_conninfo(admin_dsn, dbname=name))
                    try:
                        try:
                            await repo.open()
                            await repo.migrate()
                        finally:
                            await _finish(repo.close())
                    except BaseException:
                        await _drop_database(admin_dsn, name)
                        raise
                    # The template pool is closed before any case can clone it.
                    self._templates[identity] = (admin_dsn, name)
        finally:
            state.users -= 1
            if state.users == 0:
                self._building.pop(identity)
        return self._templates[identity][1]

    @asynccontextmanager
    async def repository(self, admin_dsn, *, migrated=True):
        template = await self._template(admin_dsn) if migrated else None
        name = await _create_database(admin_dsn, template)
        repo = PostgresRepository(make_conninfo(admin_dsn, dbname=name))
        try:
            await repo.open()
            yield repo
        finally:

            async def cleanup():
                try:
                    await repo.close()
                finally:
                    await _drop_database(admin_dsn, name)

            await _finish(cleanup())

    async def close(self):
        async def cleanup():
            for identity, (admin_dsn, name) in list(self._templates.items()):
                await _drop_database(admin_dsn, name)
                del self._templates[identity]

        await _finish(cleanup())
