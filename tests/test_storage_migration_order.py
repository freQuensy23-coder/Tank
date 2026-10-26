"""Full migration filenames remain independent identities across parallel feature histories."""

import os
from pathlib import Path

import pytest
from postgres_helpers import PostgresTemplate
from psycopg.errors import CheckViolation
from test_storage import complete_fixture_record

from tank import storage
from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.models import Record
from tank.storage import PostgresRepository

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


class MigrationStamp(DomainModel):
    version: str
    applied_at: str


def migration_directory() -> Path:
    return Path(storage.__file__).parent / "migrations"


async def migration_stamps(repo: PostgresRepository) -> dict[str, str]:
    async with repo.transaction():
        rows = await (
            await repo._conn().execute(
                "SELECT version,applied_at::text AS applied_at FROM ops.schema_migrations ORDER BY version"
            )
        ).fetchall()
    values = [MigrationStamp.model_validate(row) for row in rows]
    return {value.version: value.applied_at for value in values}


async def scoped_schema_primitives(repo: PostgresRepository) -> tuple[str, str]:
    async with repo.transaction():
        row = await (
            await repo._conn().execute(
                "SELECT ops.request_budget_scope_key(%s,60) AS budget_key, "
                "ana.decision_state_head_key(%s,%s) AS decision_key",
                ("scope|60|1", 'event]"', 'token|"'),
            )
        ).fetchone()
    assert row is not None
    budget, decision = row["budget_key"], row["decision_key"]
    assert isinstance(budget, str) and isinstance(decision, str)
    assert decode_json(budget) == ["scope|60|1", 60]
    assert decode_json(decision) == ['event]"', 'token|"']
    return budget, decision


@pytest.mark.parametrize("preexisting", [False, True], ids=["fresh", "owned-006-and-007-already-applied"])
async def test_parallel_migration_filenames_apply_once_without_relabelling_existing_history(
    postgres_template: PostgresTemplate,
    preexisting: bool,
) -> None:
    dsn = os.environ["TANK_TEST_DATABASE_URL"]
    async with postgres_template.repository(dsn, migrated=False) as repo:
        before: dict[str, str] = {}
        if preexisting:
            await repo.migrate(target="005_ops_entry_errors.sql")
            async with repo.transaction():
                for name in ("006_request_budget_policy_identity.sql", "007_decision_state_heads.sql"):
                    await repo._conn().execute((migration_directory() / name).read_text(), prepare=False)
                    await repo._conn().execute(
                        "INSERT INTO ops.schema_migrations(version) VALUES (%s)", (name,)
                    )
            before = await migration_stamps(repo)
            assert "006_preliminary_cards.sql" not in before
            await scoped_schema_primitives(repo)
        else:
            await repo.migrate()
        event: Record = complete_fixture_record("evt.events", {"event_id": "event-migration-probe"})
        await repo.put("evt.events", "event-migration-probe", event)
        alert: Record = complete_fixture_record(
            "out.alerts",
            {
                "alert_id": "alert-migration-probe",
                "version": 1,
                "event_id": "event-migration-probe",
                "kind": "PRELIMINARY",
                "card_text": "Disposable preliminary-kind storage admission probe.",
                "card_struct": {"kind": "PRELIMINARY", "purpose": "MIGRATION_ADMISSION_ONLY"},
            },
        )
        if preexisting:
            with pytest.raises(CheckViolation):
                await repo.put("out.alerts", "alert-migration-probe:1", alert)
            assert await repo.get("out.alerts", "alert-migration-probe:1") is None
            await repo.migrate()
        accepted = await repo.put("out.alerts", "alert-migration-probe:1", alert)
        assert accepted["kind"] == "PRELIMINARY"
        applied = await migration_stamps(repo)
        assert tuple(applied) == tuple(path.name for path in sorted(migration_directory().glob("*.sql")))
        assert {
            "006_preliminary_cards.sql",
            "006_request_budget_policy_identity.sql",
            "007_decision_state_heads.sql",
            "007_db_refactor.sql",
        } <= applied.keys()
        assert all(applied[name] == stamp for name, stamp in before.items())
        primitives = await scoped_schema_primitives(repo)
        await repo.migrate()
        assert await migration_stamps(repo) == applied
        assert await scoped_schema_primitives(repo) == primitives
        assert await repo.get("out.alerts", "alert-migration-probe:1") == accepted
        assert await repo.get("evt.events", "event-migration-probe") == event
