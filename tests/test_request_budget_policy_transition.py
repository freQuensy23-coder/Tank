"""Policy reloads preserve historical spending across legacy and structured clients."""

import asyncio
import os

import pytest
from postgres_helpers import PostgresTemplate
from psycopg import AsyncConnection, errors

from tank.request_budget import BudgetConstraint, RequestLedger


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["synthetic-shared-provider", "opaque|60.0|2.0"])
async def test_postgres_capacity_reload_preserves_legacy_history(
    postgres_template: PostgresTemplate, scope: str
) -> None:
    database_url = os.environ["TANK_SOURCE_TEST_DATABASE_URL"]
    async with postgres_template.repository(database_url) as repo:
        ledger = RequestLedger(repo.database_url, owner="new-client")
        try:
            async with await AsyncConnection.connect(repo.database_url) as connection:
                await connection.cursor().executemany(
                    "INSERT INTO ops.request_budget_ledger(bucket,at,weight,owner) "
                    "VALUES(%s,clock_timestamp()-interval '5 seconds',%s,'old-client')",
                    [(f"{scope}|60.0|2.0", 0.5), (f"{scope}|60.0|4.0", 1.5)],
                )
                before = await (
                    await connection.execute(
                        "SELECT at,weight,owner FROM ops.request_budget_ledger ORDER BY at"
                    )
                ).fetchall()
            reduced = BudgetConstraint(key=scope, capacity=1, window_s=60, weight=1)
            sibling = BudgetConstraint(key="untouched-sibling", capacity=3, window_s=60, weight=1)
            assert await ledger.wait_many([reduced, sibling]) > 0
            async with await AsyncConnection.connect(repo.database_url) as connection:
                after = await (
                    await connection.execute(
                        "SELECT at,weight,owner FROM ops.request_budget_ledger ORDER BY at"
                    )
                ).fetchall()
                assert after == before
                expanded = await (
                    await connection.execute(
                        "SELECT ops.request_budget_acquire_scope(%s,3,60,1,'new-client',NULL)",
                        (scope,),
                    )
                ).fetchone()
                assert expanded == (0.0,)
                old = await (
                    await connection.execute(
                        "SELECT ops.request_budget_acquire(%s,3,60,1,'old-client',NULL)",
                        (f"{scope}|60.0|3.0",),
                    )
                ).fetchone()
                assert old is not None and isinstance(old[0], float) and old[0] > 0
                independent = await (
                    await connection.execute(
                        "SELECT ops.request_budget_acquire_scope(%s,1,120,1,'new-client',NULL)",
                        (scope,),
                    )
                ).fetchone()
                assert independent == (0.0,)
                migrated = await (
                    await connection.execute(
                        "SELECT count(*) FROM ops.request_budget_ledger "
                        "WHERE bucket=ops.request_budget_scope_key(%s,60)",
                        (scope,),
                    )
                ).fetchone()
                assert migrated == (3,)
        finally:
            await ledger.shutdown()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_mixed_legacy_and_structured_clients_share_one_atomic_lock(
    postgres_template: PostgresTemplate,
) -> None:
    database_url = os.environ["TANK_SOURCE_TEST_DATABASE_URL"]
    async with postgres_template.repository(database_url) as repo:
        scope = "concurrent|60.0|1.0"

        async def acquire(index: int) -> float:
            async with await AsyncConnection.connect(repo.database_url, autocommit=True) as connection:
                if index % 2:
                    query = "SELECT ops.request_budget_acquire(%s,1,60,1,'legacy',NULL)"
                    identity = f"{scope}|60.0|1.0"
                else:
                    query = "SELECT ops.request_budget_acquire_scope(%s,1,60,1,'structured',NULL)"
                    identity = scope
                row = await (await connection.execute(query, (identity,))).fetchone()
                assert row is not None and isinstance(row[0], float)
                return row[0]

        delays = await asyncio.gather(*(acquire(index) for index in range(12)))
        assert delays.count(0.0) == 1
        assert all(delay > 0 for delay in delays if delay != 0)
        async with await AsyncConnection.connect(repo.database_url) as connection:
            row = await (
                await connection.execute(
                    "SELECT count(*),sum(weight) FROM ops.request_budget_ledger "
                    "WHERE bucket=ops.request_budget_scope_key(%s,60)",
                    (scope,),
                )
            ).fetchone()
        assert row == (1, 1)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity",
    ["scope|60.0|1.0", "scope|120.0|2.0", "scope|60.0|NaN", '["scope",60]', "scope|60"],
)
async def test_legacy_entrypoint_refuses_mismatched_or_new_identity(
    postgres_template: PostgresTemplate, identity: str
) -> None:
    database_url = os.environ["TANK_SOURCE_TEST_DATABASE_URL"]
    async with (
        postgres_template.repository(database_url) as repo,
        await AsyncConnection.connect(repo.database_url, autocommit=True) as connection,
    ):
        with pytest.raises(errors.RaiseException):
            await connection.execute(
                "SELECT ops.request_budget_acquire(%s,2,60,1,'legacy',NULL)", (identity,)
            )
        row = await (await connection.execute("SELECT count(*) FROM ops.request_budget_ledger")).fetchone()
        assert row == (0,)
