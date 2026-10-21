"""Real PostgreSQL lifecycle contracts for the opt-in immutable schema seed."""

import os
import uuid

import psycopg
import pytest
import pytest_asyncio
from postgres_helpers import (
    PostgresTemplate,
    _connection_identity,
    _create_database,
    _drop_database,
    prepare_schema_template,
    schema_template_marker,
)
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def _admin_dsn():
    value = os.environ.get("TANK_TEST_DATABASE_URL")
    if not value:
        pytest.skip("requires disposable PostgreSQL TANK_TEST_DATABASE_URL")
    _connection_identity(value)
    return value


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def prepared_seed(request):
    admin_dsn = _admin_dsn()
    configured = request.config.getoption("postgres_schema_template")
    name = configured or "tank_test_schema_regression_" + uuid.uuid4().hex
    if configured is None:
        await prepare_schema_template(admin_dsn, name)
    try:
        yield admin_dsn, name
    finally:
        if configured is None:
            await _drop_database(admin_dsn, name)


async def test_external_schema_clones_isolate_writes_keep_blank_upgrades_and_preserve_seed(prepared_seed):
    admin_dsn, seed = prepared_seed
    factory = PostgresTemplate(schema_template=seed)
    children = []
    try:
        async with factory.repository(admin_dsn) as first, factory.repository(admin_dsn) as second:
            children.extend(conninfo_to_dict(repo.database_url)["dbname"] for repo in (first, second))
            assert children[0] != children[1]
            async with await psycopg.AsyncConnection.connect(first.database_url, autocommit=True) as conn:
                await conn.execute("CREATE TABLE public.template_isolation (value integer NOT NULL)")
                await conn.execute("INSERT INTO public.template_isolation VALUES (73)")
                assert (await (await conn.execute("SELECT value FROM public.template_isolation")).fetchone())[
                    0
                ] == 73
            async with await psycopg.AsyncConnection.connect(second.database_url, autocommit=True) as conn:
                assert (
                    await (await conn.execute("SELECT to_regclass('public.template_isolation')")).fetchone()
                )[0] is None
                assert (
                    await (
                        await conn.execute(
                            "SELECT to_regclass('mkt.funding_obs_instrument_kind_funding_time_key_idx')"
                        )
                    ).fetchone()
                )[0] is not None
            async with factory.repository(admin_dsn, migrated=False) as blank:
                children.append(conninfo_to_dict(blank.database_url)["dbname"])
                async with await psycopg.AsyncConnection.connect(blank.database_url, autocommit=True) as conn:
                    assert (
                        await (await conn.execute("SELECT to_regclass('ops.schema_migrations')")).fetchone()
                    )[0] is None
    finally:
        await factory.close()

    async with await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True) as admin:
        assert (
            await (
                await admin.execute("SELECT count(*) FROM pg_database WHERE datname=ANY(%s)", (children,))
            ).fetchone()
        )[0] == 0
        row = await (
            await admin.execute(
                "SELECT datallowconn, shobj_description(oid,'pg_database') FROM pg_database WHERE datname=%s",
                (seed,),
            )
        ).fetchone()
        assert row == (False, schema_template_marker())


@pytest.mark.parametrize("state", ["unsealed", "wrong_marker", "connected"])
async def test_invalid_external_seed_is_refused_before_any_clone_ddl(state):
    admin_dsn = _admin_dsn()
    seed = await _create_database(admin_dsn)
    role = "tank_test_no_create_" + uuid.uuid4().hex
    holder = None
    role_created = False
    factory = PostgresTemplate(schema_template=seed)
    try:
        async with await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True) as admin:
            await admin.execute(
                sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal("synthetic-test-only")
                )
            )
            role_created = True
            marker = "wrong" if state == "wrong_marker" else schema_template_marker()
            await admin.execute(
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(sql.Identifier(seed), sql.Literal(marker))
            )
            if state == "connected":
                holder = await psycopg.AsyncConnection.connect(
                    make_conninfo(admin_dsn, dbname=seed), autocommit=True
                )
            if state != "unsealed":
                await admin.execute(
                    sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(seed))
                )
        restricted = make_conninfo(admin_dsn, user=role, password="synthetic-test-only")
        reason = {
            "unsealed": "ALLOW_CONNECTIONS false",
            "wrong_marker": "fingerprint does not match",
            "connected": "zero open connections",
        }[state]
        with pytest.raises(ValueError, match=reason):
            async with factory.repository(restricted):
                pytest.fail("invalid external seed reached a mutable child")
        await factory.close()
        async with await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True) as admin:
            assert (
                await (
                    await admin.execute("SELECT count(*) FROM pg_database WHERE datname=%s", (seed,))
                ).fetchone()
            )[0] == 1
    finally:
        await factory.close()
        if holder is not None:
            await holder.close()
        await _drop_database(admin_dsn, seed)
        if role_created:
            async with await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True) as admin:
                await admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
