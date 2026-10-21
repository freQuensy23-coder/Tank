"""Disposable PostgreSQL test databases; memory fixtures never open resources.

By default each worker lazily migrates its own schema-only template. An explicit
externally prepared template can be shared across workers; it must be sealed and
match the current migration inputs. Each case still gets a fresh mutable clone.
"""

import asyncio
import hashlib
import re
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from tank import storage
from tank.storage import PostgresRepository


def _connection_identity(admin_dsn):
    params = conninfo_to_dict(admin_dsn)
    if not params.get("dbname", "").startswith("tank_test"):
        raise ValueError("PostgreSQL test admin must select a dedicated tank_test* database")
    # Values can include credentials: this private key is never logged or exposed.
    identity = dict(params)
    identity.pop("dbname")
    identity.setdefault("port", "5432")
    return tuple(sorted(identity.items()))


def _template_name(name):
    if not isinstance(name, str) or not re.fullmatch(r"tank_test_[A-Za-z0-9_]+", name):
        raise ValueError("Schema template must name a dedicated tank_test_* database")
    if len(name) > 63:
        raise ValueError("Schema template database name exceeds PostgreSQL's 63-byte limit")
    return name


def _migration_paths():
    return sorted((Path(storage.__file__).parent / "migrations").glob("*.sql"))


def schema_template_marker():
    """Fingerprint actual SQL and migration code; no database or environment IO."""
    root = Path(storage.__file__).parent
    paths = [
        *_migration_paths(),
        *(root / name for name in ("storage.py", "market_migration.py", "market_schema.py")),
    ]
    digest = hashlib.sha256(b"tank-test-schema-template-v1\0")
    for path in paths:
        name = str(path.relative_to(root)).encode()
        data = path.read_bytes()
        digest.update(len(name).to_bytes(4, "big"))
        digest.update(name)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    # 004's dated legacy partitions are retired by027/032–035. The final typed
    # schema uses default partitions, so UTC midnight does not invalidate a seed.
    return "tank-test-schema-template:v1:" + digest.hexdigest()


async def _finish(operation):
    """Finish owned resource cleanup even if its caller is cancelled again."""
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


async def _create_database(admin_dsn, template=None, *, name=None):
    _connection_identity(admin_dsn)
    name = _template_name(name) if name is not None else "tank_test_fixture_" + uuid.uuid4().hex
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


async def _validate_schema_template(admin_dsn, name):
    _connection_identity(admin_dsn)
    _template_name(name)
    admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
    try:
        # PostgreSQL 16 CREATE DATABASE interlocks with autovacuum itself via
        # CountOtherDBBackends; client and unknown backend types still block us.
        row = await (
            await admin.execute(
                "SELECT d.datallowconn, shobj_description(d.oid,'pg_database'),"
                " (SELECT count(*) FROM pg_stat_activity WHERE datid=d.oid"
                " AND backend_type IS DISTINCT FROM 'autovacuum worker')"
                " FROM pg_database d WHERE d.datname=%s",
                (name,),
            )
        ).fetchone()
        if row is None:
            raise ValueError("Prepared schema template does not exist in the admin cluster")
        allowed, marker, connections = row
        if allowed:
            raise ValueError("Prepared schema template must have ALLOW_CONNECTIONS false")
        if connections:
            raise ValueError("Prepared schema template must have zero open connections")
        if marker != schema_template_marker():
            raise ValueError("Prepared schema template migration fingerprint does not match")
    finally:
        await _finish(admin.close())


async def prepare_schema_template(admin_dsn, name):
    """Create, migrate and seal a new CI-owned seed; never replace an existing DB."""
    _connection_identity(admin_dsn)
    _template_name(name)
    # CREATE refuses existing names. Only this successful creation is ours to clean.
    await _create_database(admin_dsn, name=name)
    repo = PostgresRepository(make_conninfo(admin_dsn, dbname=name))
    try:
        try:
            await repo.open()
            await repo.migrate()
            conn = await psycopg.AsyncConnection.connect(repo.database_url, autocommit=True)
            try:
                versions = await (
                    await conn.execute("SELECT version FROM ops.schema_migrations ORDER BY version")
                ).fetchall()
                if [row[0] for row in versions] != [path.name for path in _migration_paths()]:
                    raise ValueError("Prepared schema template did not apply every current migration")
            finally:
                await _finish(conn.close())
        finally:
            await _finish(repo.close())
        admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
        try:
            await admin.execute(
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(name), sql.Literal(schema_template_marker())
                )
            )
            await admin.execute(
                sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(name))
            )
        finally:
            await _finish(admin.close())
        await _validate_schema_template(admin_dsn, name)
    except BaseException:
        await _drop_database(admin_dsn, name)
        raise
    return name


class PostgresTemplate:
    """Lazy template owner; an explicit sealed external seed is never owned."""

    def __init__(self, *, schema_template=None):
        self._external_template = _template_name(schema_template) if schema_template is not None else None
        self._templates = {}
        self._building = {}

    async def _template(self, admin_dsn):
        if self._external_template is not None:
            # Use this admin connection's catalog: a separate seed DSN is forbidden.
            # Recheck immutability before every CREATE; retain no pooled resources.
            await _validate_schema_template(admin_dsn, self._external_template)
            return self._external_template
        identity = _connection_identity(admin_dsn)
        if identity in self._templates:
            return self._templates[identity][1]
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
