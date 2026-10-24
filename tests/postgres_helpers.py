"""Disposable PostgreSQL test databases; memory fixtures never open resources.

By default each worker lazily migrates its own schema-only template. An explicit
externally prepared template can be shared across workers; it must be sealed and
match the current migration inputs. Each case still gets a fresh mutable clone.
"""

import asyncio
import hashlib
import re
import uuid
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pydantic import BaseModel, ConfigDict, Field

from tank import storage
from tank.storage import PostgresRepository

type ConnectionIdentity = tuple[tuple[str, str | int | None], ...]


class OwnedTemplate(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    admin_dsn: str = Field(repr=False)
    name: str


class TemplateBuild(BaseModel):
    model_config = ConfigDict(
        strict=True, extra="forbid", arbitrary_types_allowed=True, validate_assignment=True
    )
    lock: asyncio.Lock = Field(default_factory=asyncio.Lock)
    users: int = Field(default=0, ge=0)


def _connection_identity(admin_dsn: str) -> ConnectionIdentity:
    params = conninfo_to_dict(admin_dsn)
    database = params.get("dbname")
    if not isinstance(database, str) or not database.startswith("tank_test"):
        raise ValueError("PostgreSQL test admin must select a dedicated tank_test* database")

    identity = dict(params)
    identity.pop("dbname")
    identity.setdefault("port", "5432")
    return tuple(sorted(identity.items()))


def _template_name(name: str) -> str:
    if not isinstance(name, str) or not re.fullmatch(r"tank_test_[A-Za-z0-9_]+", name):
        raise ValueError("Schema template must name a dedicated tank_test_* database")
    if len(name) > 63:
        raise ValueError("Schema template database name exceeds PostgreSQL's 63-byte limit")
    return name


def _migration_paths() -> list[Path]:
    return sorted((Path(storage.__file__).parent / "migrations").glob("*.sql"))


def schema_template_marker() -> str:
    """Fingerprint actual SQL and migration code; no database or environment IO."""
    root = Path(storage.__file__).parent
    paths = [
        *_migration_paths(),
        *(root / name for name in ("storage.py", "market_schema.py")),
    ]
    digest = hashlib.sha256(b"tank-test-schema-template-v1\0")
    for path in paths:
        name = str(path.relative_to(root)).encode()
        data = path.read_bytes()
        digest.update(len(name).to_bytes(4, "big"))
        digest.update(name)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)

    return "tank-test-schema-template:v1:" + digest.hexdigest()


async def _finish[T](operation: Awaitable[T]) -> T:
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


async def _attach_clickhouse(repo: PostgresRepository, name: str) -> None:
    """Each case's exchange series live in its own ClickHouse database (CLICKHOUSE_TEST_URL)."""
    import os

    from tank import clickhouse_market as chm

    url = os.environ.get("CLICKHOUSE_TEST_URL")
    if not url:
        return
    repo.clickhouse = chm.ClickHouse(url, database="mkt_" + re.sub(r"[^a-z0-9_]", "_", name.lower()))
    for statement in chm.ddl():
        await repo.clickhouse.execute(statement)


async def _detach_clickhouse(repo: PostgresRepository) -> None:
    ch = getattr(repo, "clickhouse", None)
    if ch is not None:
        try:
            await ch.execute("DROP DATABASE IF EXISTS mkt")
        finally:
            await ch.close()


async def _drop_database(admin_dsn: str, name: str) -> None:
    async def drop() -> None:
        admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
        try:
            await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        finally:
            await admin.close()

    await _finish(drop())


async def _create_database(admin_dsn: str, template: str | None = None, *, name: str | None = None) -> str:
    _connection_identity(admin_dsn)
    name = _template_name(name) if name is not None else "tank_test_fixture_" + uuid.uuid4().hex
    command = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
    if template is not None:
        command += sql.SQL(" TEMPLATE {}").format(sql.Identifier(template))
    created = False

    async def create() -> None:
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


async def _validate_schema_template(admin_dsn: str, name: str) -> None:
    _connection_identity(admin_dsn)
    _template_name(name)
    admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
    try:
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


async def prepare_schema_template(admin_dsn: str, name: str) -> str:
    """Create, migrate and seal a new CI-owned seed; never replace an existing DB."""
    _connection_identity(admin_dsn)
    _template_name(name)

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

    def __init__(self, *, schema_template: str | None = None) -> None:
        self._external_template = _template_name(schema_template) if schema_template is not None else None
        self._templates: dict[ConnectionIdentity, OwnedTemplate] = {}
        self._building: dict[ConnectionIdentity, TemplateBuild] = {}

    async def _template(self, admin_dsn: str) -> str:
        if self._external_template is not None:
            await _validate_schema_template(admin_dsn, self._external_template)
            return self._external_template
        identity = _connection_identity(admin_dsn)
        if identity in self._templates:
            return self._templates[identity].name
        state = self._building.get(identity)
        if state is None:
            state = TemplateBuild()
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
                    self._templates[identity] = OwnedTemplate(admin_dsn=admin_dsn, name=name)
        finally:
            state.users -= 1
            if state.users == 0:
                self._building.pop(identity)
        return self._templates[identity].name

    @asynccontextmanager
    async def repository(self, admin_dsn: str, *, migrated: bool = True) -> AsyncIterator[PostgresRepository]:
        template = await self._template(admin_dsn) if migrated else None
        name = await _create_database(admin_dsn, template)
        repo = PostgresRepository(make_conninfo(admin_dsn, dbname=name))
        try:
            await repo.open()
            await _attach_clickhouse(repo, name)
            yield repo
        finally:

            async def cleanup() -> None:
                try:
                    await repo.close()
                    await _detach_clickhouse(repo)
                finally:
                    await _drop_database(admin_dsn, name)

            await _finish(cleanup())

    async def close(self) -> None:
        async def cleanup() -> None:
            for identity, template in list(self._templates.items()):
                await _drop_database(template.admin_dsn, template.name)
                del self._templates[identity]

        await _finish(cleanup())
