"""Actual source batches retain every deferred change across cursor advancement."""

import os
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from runtime_public_fixtures import public_runtime
from test_storage import repo as repo

from tank.contracts.reaction_demands import ReactionDemandLedger
from tank.models import Record
from tank.reaction_demands import CHECKPOINT_KEY, ReactionDemandCoordinator
from tank.storage import StorageBackend
from tank.time_precision import Timestamp, collection_clock
from tank.util import iso, utcnow


def ledger(row: Record | None) -> ReactionDemandLedger:
    assert row is not None
    return ReactionDemandLedger.model_validate(row["state"])


async def test_actual_source_batch_retains_skipped_change_and_empty_batch_resumes(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        if repo.backend == "postgres":
            endpoint = httpx.URL(os.environ["CLICKHOUSE_TEST_URL"])
            router.route(
                method="POST", scheme=endpoint.scheme, host=endpoint.host, port=endpoint.port, path="/"
            ).pass_through()
        now = utcnow().replace(microsecond=0)
        anchor = now - timedelta(hours=2)
        await repo.put(
            "out.reaction_checkpoints",
            "event:token:POST_1H:1",
            {
                "event_id": "event",
                "token_id": "token",
                "checkpoint": "POST_1H",
                "revision": 1,
                "anchor": {"type": "T0", **Timestamp(iso(anchor), "EXACT").fields("ts")},
                **collection_clock(now - timedelta(hours=1)).fields("captured_at"),
            },
        )
        for index in range(2):
            await repo.put(
                "mkt.backfill_runs",
                f"source-{index}",
                {
                    "backfill_id": f"source-{index}",
                    "event_id": "event",
                    "token_id": "token",
                    "status": "FAILED",
                    **Timestamp(iso(anchor), "EXACT").fields("anchor"),
                    **collection_clock(now - timedelta(seconds=2 - index)).fields("finished_at"),
                },
            )
        originals = await repo.list("mkt.backfill_runs")
        await runtime.scheduler.detect_late_data(now)
        first = ledger(await repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY))
        assert len(first.demands) == 2
        assert sorted(d.phase for d in first.demands.values()) == ["LINKED", "PENDING"]
        jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
        assert len(jobs) == 1
        job = jobs[0]
        job_id = job["job_id"]
        assert isinstance(job_id, str)
        await repo.put("ops.jobs", job_id, job | {"status": "FAILED"})
        coordinator = ReactionDemandCoordinator(repo, runtime.market)
        cursor = await coordinator.snapshot()
        assert cursor.cursor is not None and cursor.cursor.value is not None
        assert [
            row
            async for row in coordinator.reader.capture_changes(
                since=cursor.cursor.value, end=iso(now + timedelta(seconds=1))
            )
        ] == []
        await runtime.scheduler.detect_late_data(now + timedelta(seconds=1))
        resumed = ledger(await repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY))
        assert sorted(d.phase for d in resumed.demands.values()) == ["BLOCKED", "LINKED"]
        jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
        assert len(jobs) == 2
        for row in jobs:
            key = row["job_id"]
            assert isinstance(key, str)
            await repo.put("ops.jobs", key, row | {"status": "DONE"})
        await runtime.scheduler.detect_late_data(now + timedelta(seconds=2))
        final = ledger(await repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY))
        assert len(final.demands) == 2
        assert all(d.phase == "BLOCKED" for d in final.demands.values())
        assert len(await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})) == 2
        assert await repo.list("mkt.backfill_runs") == originals
