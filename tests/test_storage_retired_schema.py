"""Guarded retirement retains every legacy row before removing obsolete SQL."""

import asyncio
import io
import json
from decimal import Decimal
from pathlib import Path

import pytest
import zstandard

from tank.storage import PostgresRepository


def bundle_rows(path):
    with Path(path).open("rb") as source, zstandard.ZstdDecompressor().stream_reader(source) as stream:
        payload = json.load(io.TextIOWrapper(stream))
    return [json.loads(row, parse_float=Decimal) for row in payload["rows"]]


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
        await asyncio.wait_for(waiting, timeout=300)
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
        await asyncio.wait_for(other.migrate(), timeout=300)
    finally:
        await other.close()
