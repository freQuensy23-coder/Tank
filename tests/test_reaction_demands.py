"""Actual source batches retain every deferred change across cursor advancement."""

import asyncio
import os
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Literal

import httpx
import pytest
from pydantic import JsonValue
from runtime_public_fixtures import public_runtime
from test_storage import repo as repo

from tank.contracts.reaction_demands import ReactionDemandLedger
from tank.market.service import MarketService
from tank.models import Record
from tank.reaction_demands import CHECKPOINT_KEY, ReactionDemandCoordinator
from tank.storage import StorageBackend
from tank.time_precision import Timestamp, collection_clock
from tank.util import iso, utcnow


def ledger(row: Record | None) -> ReactionDemandLedger:
    assert row is not None
    return ReactionDemandLedger.model_validate(row["state"])


@pytest.mark.parametrize("interrupt_enqueue", ["NONE", "ERROR", "CANCEL"])
async def test_actual_source_batch_retains_skipped_change_and_empty_batch_resumes(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupt_enqueue: Literal["NONE", "ERROR", "CANCEL"],
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
        if interrupt_enqueue != "NONE":
            enqueue = repo.enqueue

            async def failed_enqueue(
                kind: str,
                payload: Mapping[str, JsonValue],
                idem_key: str,
                *,
                priority: int,
                max_attempts: int,
                run_after: Timestamp,
                expires_at: Timestamp,
            ) -> Record:
                await enqueue(
                    kind,
                    payload,
                    idem_key,
                    priority=priority,
                    max_attempts=max_attempts,
                    run_after=run_after,
                    expires_at=expires_at,
                )
                if interrupt_enqueue == "CANCEL":
                    raise asyncio.CancelledError
                raise OSError("Authored interruption after actual enqueue")

            with monkeypatch.context() as interruption:
                interruption.setattr(repo, "enqueue", failed_enqueue)
                expected = asyncio.CancelledError if interrupt_enqueue == "CANCEL" else ExceptionGroup
                with pytest.raises(expected):
                    await runtime.scheduler.detect_late_data(now)
            interrupted = ledger(await repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY))
            assert len(interrupted.demands) == 2
            assert all(d.phase == "PENDING" for d in interrupted.demands.values())
            assert await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"}) == []
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
        market = runtime.market
        assert isinstance(market, MarketService)
        coordinator = ReactionDemandCoordinator(repo, market)
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


async def test_cursor_revision_and_failed_commit_preserve_the_previous_batch(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, _):
        market = runtime.market
        assert isinstance(market, MarketService)
        coordinator = ReactionDemandCoordinator(repo, market)
        original = await coordinator.snapshot()
        first = collection_clock(utcnow())
        assert await coordinator._commit_batch(original, first, {}, {})
        committed = await repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY)
        assert committed is not None
        assert not await coordinator._commit_batch(
            original, collection_clock(utcnow() + timedelta(seconds=1)), {}, {}
        )
        assert await repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY) == committed
        current = await coordinator.snapshot()
        put = repo.put

        async def failed_put(
            collection: str, key: str, record: Mapping[str, JsonValue], *, immutable: bool = False
        ) -> Record:
            stored = await put(collection, key, record, immutable=immutable)
            if collection == "ops.scheduler_checkpoints" and key == CHECKPOINT_KEY:
                raise OSError("Authored commit failure after actual storage")
            return stored

        monkeypatch.setattr(repo, "put", failed_put)
        with pytest.raises(OSError, match="Authored commit failure"):
            await coordinator._commit_batch(
                current, collection_clock(utcnow() + timedelta(seconds=2)), {}, {}
            )
        assert await repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY) == committed


async def test_failed_target_drain_does_not_lose_independent_target_or_accept_unbound_marker(
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
        for event_id in ("blocked", "independent"):
            await repo.put(
                "out.reaction_checkpoints",
                event_id + ":token:POST_1H:1",
                {
                    "event_id": event_id,
                    "token_id": "token",
                    "checkpoint": "POST_1H",
                    "revision": 1,
                    "anchor": {"type": "T0", **Timestamp(iso(anchor), "EXACT").fields("ts")},
                    **collection_clock(now - timedelta(hours=1)).fields("captured_at"),
                },
            )
            await repo.put(
                "mkt.backfill_runs",
                event_id,
                {
                    "backfill_id": event_id,
                    "event_id": event_id,
                    "token_id": "token",
                    "status": "FAILED",
                    **Timestamp(iso(anchor), "EXACT").fields("anchor"),
                    **collection_clock(now - timedelta(seconds=1)).fields("finished_at"),
                },
            )
        enqueue = repo.enqueue

        async def failed_target_enqueue(
            kind: str,
            payload: Mapping[str, JsonValue],
            idem_key: str,
            *,
            priority: int,
            max_attempts: int,
            run_after: Timestamp,
            expires_at: Timestamp,
        ) -> Record:
            stored = await enqueue(
                kind,
                payload,
                idem_key,
                priority=priority,
                max_attempts=max_attempts,
                run_after=run_after,
                expires_at=expires_at,
            )
            if payload["event_id"] == "blocked":
                raise OSError("Authored target-specific failure")
            return stored

        with monkeypatch.context() as interruption:
            interruption.setattr(repo, "enqueue", failed_target_enqueue)
            with pytest.raises(ExceptionGroup, match="Reaction target demand drains failed"):
                await runtime.scheduler.detect_late_data(now)
        checkpoint = ledger(await repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY))
        assert len(checkpoint.demands) == 2
        by_event = {d.command.event_id: d for d in checkpoint.demands.values()}
        assert by_event["blocked"].phase == "PENDING"
        assert by_event["independent"].phase == "LINKED"
        jobs = await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"})
        assert len(jobs) == 1
        payload = jobs[0]["payload"]
        assert isinstance(payload, dict) and payload["event_id"] == "independent"
        blocked = by_event["blocked"]
        marker_key = "reaction-change:" + blocked.command.change_id
        marker = await repo.put(
            "ops.scheduler_checkpoints",
            marker_key,
            {"checkpoint_id": marker_key, **collection_clock(now).fields("at"), "state": {}},
            immutable=True,
        )
        await runtime.scheduler.detect_late_data(now + timedelta(seconds=1))
        refused = ledger(await repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY))
        retained = refused.demands[blocked.command.change_id]
        assert retained.phase == "BLOCKED"
        assert retained.reason == "APPLIED_MARKER_UNRESOLVED"
        assert len(refused.demands) == 2
        assert await repo.get("ops.scheduler_checkpoints", marker_key) == marker
        assert await repo.list("ops.jobs", filters={"type": "REACTION_CAPTURE"}) == jobs
