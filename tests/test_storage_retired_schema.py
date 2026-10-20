"""Guarded retirement retains every legacy row before removing obsolete SQL."""

# ruff: noqa: F811

import asyncio
import hashlib
import io
import json
from decimal import Decimal
from pathlib import Path

import pytest
import zstandard
from test_storage_typed_market import typed_repo  # noqa: F401

from tank.market_migration import verify_legacy_table_bundle
from tank.storage import PostgresRepository


def bundle_rows(path):
    with Path(path).open("rb") as source, zstandard.ZstdDecompressor().stream_reader(source) as stream:
        payload = json.load(io.TextIOWrapper(stream))
    return [json.loads(row, parse_float=Decimal) for row in payload["rows"]]


async def test_fresh_default_postgres_drops_empty_archive_and_retired_linter(typed_repo):
    repo = typed_repo
    async with repo.transaction():
        value = (await (await repo._conn().execute("SHOW max_locks_per_transaction")).fetchone())[
            "max_locks_per_transaction"
        ]
    assert value == "64", "This proof must run against PostgreSQL's default lock budget"
    await repo.migrate()
    async with repo.transaction():
        found = await (
            await repo._conn().execute(
                "SELECT to_regnamespace('archive') AS retired,exists(SELECT 1 FROM information_schema.columns WHERE table_schema='out' AND table_name='alerts' AND column_name='linter') AS linter"
            )
        ).fetchone()
    assert found == {"retired": None, "linter": False}
    assert not await repo.list("ops.market_migration_exports")


async def seed_legacy(repo):
    from psycopg.types.json import Jsonb

    await repo.migrate(target="013_outcome_ledger.sql")
    budget = {"day": "2026-10-01", "spent_usd": "42.5", "historical_extra": {"preserve": True}}
    async with repo.transaction():
        await repo._conn().execute(
            "INSERT INTO ops.budget_state(storage_key,document,day,spent_usd) VALUES ('old-budget',%s,'2026-10-01',42.5)",
            (Jsonb(budget),),
        )
    await repo.migrate(target="026_timestamp_precision_metadata.sql")
    legacy = {
        "instrument_id": "legacy:OI",
        "token_id": "T",
        "metric_key": "test.oi",
        "ts": "2026-10-18T10:00:00Z",
        "ts_prec": "BUCKET",
        "ts_bucket_size_s": 300,
        "received_ts": "2026-10-18T10:00:01Z",
        "raw_value": "100.123456789012345678901",
        "raw_unit": "BASE",
        "sidedness": "SINGLE",
    }
    async with repo.transaction():
        await repo._conn().execute(
            "INSERT INTO mkt.oi_obs(storage_key,document,immutable,instrument_id,metric_key,ts,ts_prec,received_ts,raw_value,raw_unit,sidedness) VALUES ('legacy',%s,true,'legacy:OI','test.oi','2026-10-18T10:00:00Z','BUCKET','2026-10-18T10:00:01Z',100.123456789012345678901,'BASE','SINGLE')",
            [Jsonb(legacy)],
        )
    await repo.migrate(target="031_remove_retired_linter_column.sql")
    return budget, legacy


async def test_retirement_requires_durable_export_and_preserves_full_original_rows(typed_repo, tmp_path):
    repo = typed_repo
    budget, legacy = await seed_legacy(repo)
    before = await repo.get("mkt.oi_obs", "legacy")
    with pytest.raises(RuntimeError, match="legacy_bundle_dir"):
        await repo.migrate()
    async with repo.transaction():
        old = (
            await (
                await repo._conn().execute("SELECT document FROM archive.oi_obs WHERE storage_key='legacy'")
            ).fetchone()
        )["document"]
        version = await (
            await repo._conn().execute("SELECT 1 FROM ops.schema_migrations WHERE version LIKE '032%'")
        ).fetchone()
    assert old == legacy and version is None
    await repo.migrate(legacy_bundle_dir=tmp_path / "bundles")
    exports = await repo.list("ops.market_migration_exports")
    assert len(exports) == 2
    assert sum(receipt["row_count"] for receipt in exports) == 2
    for receipt in exports:
        assert (
            hashlib.sha256(
                zstandard.ZstdDecompressor().decompress(Path(receipt["manifest_path"]).read_bytes())
            ).hexdigest()
            == receipt["manifest_sha256"]
        )
    rows = {}
    for table in [entry for receipt in exports for entry in receipt["tables"]]:
        path = Path(table["path"])
        assert path.parent == tmp_path / "bundles"
        assert path.name == table["sha256"] + ".json.zst"
        verify_legacy_table_bundle(
            path, **{k: table[k] for k in ("sha256", "collection", "row_count", "row_chain_sha256")}
        )
        rows[table["collection"]] = bundle_rows(path)
    assert rows["archive.oi_obs"][0]["document"] == legacy
    assert rows["archive.oi_obs"][0]["immutable"] is True
    assert str(rows["archive.oi_obs"][0]["raw_value"]) == "100.123456789012345678901"
    assert rows["archive.ops_budget_state"][0]["document"] == budget
    assert await repo.get("mkt.oi_obs", "legacy") == before
    async with repo.transaction():
        assert (await (await repo._conn().execute("SELECT to_regnamespace('archive') AS name")).fetchone())[
            "name"
        ] is None
    await repo.migrate(legacy_bundle_dir=tmp_path / "bundles")
    assert len(await repo.list("ops.market_migration_exports")) == 2


async def test_corrupt_readback_aborts_sql_drop_and_can_restart(typed_repo, tmp_path, monkeypatch):
    import tank.market_migration as module

    repo = typed_repo
    _, legacy = await seed_legacy(repo)
    original = module.verify_legacy_table_bundle

    def fail(*a, **k):
        raise ValueError("synthetic checksum mismatch")

    monkeypatch.setattr(module, "verify_legacy_table_bundle", fail)
    with pytest.raises(ValueError, match="checksum mismatch"):
        await repo.migrate(legacy_bundle_dir=tmp_path / "bundles")
    async with repo.transaction():
        assert (
            await (
                await repo._conn().execute("SELECT document FROM archive.oi_obs WHERE storage_key='legacy'")
            ).fetchone()
        )["document"] == legacy
        assert not await (
            await repo._conn().execute("SELECT 1 FROM ops.schema_migrations WHERE version LIKE '032%'")
        ).fetchone()
    assert not await repo.list("ops.market_migration_exports")
    monkeypatch.setattr(module, "verify_legacy_table_bundle", original)
    await repo.migrate(legacy_bundle_dir=tmp_path / "bundles")


async def test_migration_lock_serializes_concurrent_runners_and_releases_after_cancellation(typed_repo):
    repo = typed_repo
    other = await PostgresRepository(repo.database_url).open()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold():
        async with repo._migration_connection():
            entered.set()
            await release.wait()

    task = asyncio.create_task(hold())
    await entered.wait()
    waiting = asyncio.create_task(other.migrate())
    try:
        await asyncio.sleep(0.05)
        assert not waiting.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(waiting, timeout=30)
        # The same pooled connection can reacquire without a leaked session lock.
        await asyncio.wait_for(repo.migrate(), timeout=10)
    finally:
        release.set()
        for operation in (task, waiting):
            if not operation.done():
                operation.cancel()
        await asyncio.gather(task, waiting, return_exceptions=True)
        await other.close()


@pytest.mark.parametrize("lock_kind", ["session", "legacy_transaction"])
async def test_cancelled_waiting_migration_does_not_keep_session_lock(typed_repo, lock_kind):
    repo = typed_repo
    other = await PostgresRepository(repo.database_url).open()
    try:
        async with repo._migration_connection() if lock_kind == "session" else repo.lock("tank:migrations"):
            waiting = asyncio.create_task(other.migrate())
            await asyncio.sleep(0.05)
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
        await asyncio.wait_for(other.migrate(), timeout=30)
    finally:
        await other.close()


async def test_linter_projection_removal_preserves_frozen_alert_document(typed_repo):
    from test_storage import complete_fixture_record

    repo = typed_repo
    await repo.migrate(target="030_kline_instrument_availability_index.sql")
    await repo.put("evt.events", "e", complete_fixture_record("evt.events", {"event_id": "e"}))
    alert = complete_fixture_record(
        "out.alerts", {"alert_id": "a", "version": 1, "event_id": "e", "linter": {}}
    )
    await repo.put("out.alerts", "a:1", alert, immutable=True)
    before = await repo.get("out.alerts", "a:1")
    await repo.migrate(target="031_remove_retired_linter_column.sql")
    assert await repo.get("out.alerts", "a:1") == before
    assert before["linter"] == {}
    async with repo.transaction():
        assert not await (
            await repo._conn().execute(
                "SELECT 1 FROM information_schema.columns WHERE table_schema='out' AND table_name='alerts' AND column_name='linter'"
            )
        ).fetchone()


async def test_later_group_failure_preserves_prior_receipt_and_restart_only_finishes_missing_groups(
    typed_repo, tmp_path, monkeypatch
):
    import tank.market_migration as module

    repo = typed_repo
    budget, legacy = await seed_legacy(repo)
    original = module.verify_legacy_table_bundle

    def fail_oi(*a, **k):
        if k.get("collection") == "archive.oi_obs":
            raise ValueError("synthetic later group readback failure")
        return original(*a, **k)

    monkeypatch.setattr(module, "verify_legacy_table_bundle", fail_oi)
    with pytest.raises(ValueError, match="later group"):
        await repo.migrate(legacy_bundle_dir=tmp_path / "bundles")
    exports = await repo.list("ops.market_migration_exports")
    assert len(exports) == 1
    assert bundle_rows(exports[0]["tables"][0]["path"])[0]["document"] == budget
    async with repo.transaction():
        assert (
            await (
                await repo._conn().execute("SELECT to_regclass('archive.ops_budget_state') AS name")
            ).fetchone()
        )["name"] is None
        assert (
            await (
                await repo._conn().execute("SELECT document FROM archive.oi_obs WHERE storage_key='legacy'")
            ).fetchone()
        )["document"] == legacy
        applied = [
            r["version"]
            for r in await (
                await repo._conn().execute(
                    "SELECT version FROM ops.schema_migrations WHERE version LIKE '03%' ORDER BY version"
                )
            ).fetchall()
        ]
    assert any(v.startswith("032_") for v in applied) and any(v.startswith("033_") for v in applied)
    assert not any(v.startswith("034_") for v in applied)
    monkeypatch.setattr(module, "verify_legacy_table_bundle", original)
    await repo.migrate(legacy_bundle_dir=tmp_path / "bundles")
    assert len(await repo.list("ops.market_migration_exports")) == 2
    receipts = await repo.list("ops.market_migration_exports")
    assert next(r for r in receipts if r["export_id"] == exports[0]["export_id"]) == exports[0]
