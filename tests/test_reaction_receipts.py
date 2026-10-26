"""Reaction completion receipts require actual stored effects and scope evidence."""

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from postgres_helpers import PostgresTemplate
from psycopg import AsyncConnection
from psycopg.errors import CheckViolation
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_versions

from tank.contracts.scheduler_results import (
    AppliedReactionChange,
    CapturedReaction,
    HistoricalCapturedReaction,
    ReactionChangeOwnership,
    UnchangedReaction,
)
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.time_precision import Timestamp, collection_clock
from tank.util import sha256


class ReceiptSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


async def test_reused_change_id_cannot_complete_a_foreign_reaction_target() -> None:
    repo = MemoryRepository()
    market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
    scheduler = Scheduler(repo, ReceiptSettings(), market, None, versions=fixture_versions())
    original = await scheduler.reaction_capture(
        {
            "event_id": "e",
            "token_id": "t",
            "checkpoint": "PRE_1H",
            "anchor": {
                "type": "T1",
                "ts": "2026-10-17T12:00:00Z",
                "ts_prec": "EXACT",
                "ts_prec_reason": None,
            },
        }
    )
    assert isinstance(original, CapturedReaction)
    unchanged = await scheduler.reaction_capture(
        {
            "event_id": "e",
            "token_id": "t",
            "checkpoint": "PRE_1H",
            "automatic_revision": True,
            "change_id": "shared",
            "reason": "Observed capture completion",
        }
    )
    assert isinstance(unchanged, UnchangedReaction)
    marker = await repo.get("ops.scheduler_checkpoints", "reaction-change:shared")
    before = await repo.list("out.reaction_checkpoints")
    with pytest.raises(ValueError, match="scope"):
        await scheduler.reaction_capture(
            {
                "event_id": "foreign",
                "token_id": "another-token",
                "checkpoint": "PRE_1H",
                "automatic_revision": True,
                "change_id": "shared",
                "reason": "Another target completion",
            }
        )
    assert await repo.list("out.reaction_checkpoints") == before
    assert await repo.get("ops.scheduler_checkpoints", "reaction-change:shared") == marker
    assert market.snapshot.await_count == 2
    await scheduler.close()


async def test_matching_change_replay_uses_stored_owner_without_new_measurement() -> None:
    repo = MemoryRepository()
    market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
    scheduler = Scheduler(repo, ReceiptSettings(), market, None, versions=fixture_versions())
    await scheduler.reaction_capture(
        {
            "event_id": "e",
            "token_id": "t",
            "checkpoint": "PRE_1H",
            "anchor": {
                "type": "T1",
                "ts": "2026-10-17T12:00:00Z",
                "ts_prec": "EXACT",
                "ts_prec_reason": None,
            },
        }
    )
    request = {
        "event_id": "e",
        "token_id": "t",
        "checkpoint": "PRE_1H",
        "automatic_revision": True,
        "change_id": "same",
        "reason": "Observed completion",
    }
    first = await scheduler.reaction_capture(request)
    assert isinstance(first, UnchangedReaction)
    marker = await repo.get("ops.scheduler_checkpoints", "reaction-change:same")
    owner = await repo.get("ops.scheduler_checkpoints", "reaction-change-owner:same")
    restarted = Scheduler(repo, ReceiptSettings(), market, None, versions=fixture_versions())
    replayed = await restarted.reaction_capture(request)
    assert isinstance(replayed, AppliedReactionChange)
    assert (replayed.event_id, replayed.token_id, replayed.checkpoint) == ("e", "t", "PRE_1H")
    assert replayed.revision == first.revision and replayed.data_fingerprint == first.data_fingerprint
    assert await repo.get("ops.scheduler_checkpoints", "reaction-change:same") == marker
    assert await repo.get("ops.scheduler_checkpoints", "reaction-change-owner:same") == owner
    assert market.snapshot.await_count == 2
    await restarted.close()
    await scheduler.close()


async def test_unbound_retained_change_marker_stays_unbound_and_refuses_replay() -> None:
    repo = MemoryRepository()
    stamp = collection_clock()
    marker: dict[str, JsonValue] = {
        "checkpoint_id": "reaction-change:old",
        **stamp.fields("at"),
        "state": {"changed": False, "revision": 1, "data_fingerprint": sha256({"observed": "historical"})},
    }
    await repo.put("ops.scheduler_checkpoints", "reaction-change:old", marker, immutable=True)
    scheduler = Scheduler(repo, ReceiptSettings(), None, None, versions=fixture_versions())
    with pytest.raises(ValueError, match="scope is unreported"):
        await scheduler.reaction_capture(
            {
                "event_id": "e",
                "token_id": "t",
                "checkpoint": "PRE_1H",
                "automatic_revision": True,
                "change_id": "old",
                "reason": "Retained job replay",
            }
        )
    assert await repo.get("ops.scheduler_checkpoints", "reaction-change:old") == marker
    assert await repo.get("ops.scheduler_checkpoints", "reaction-change-owner:old") is None
    assert await repo.list("out.reaction_checkpoints") == []
    await scheduler.close()


async def concurrent_change_scope(repo: MemoryRepository | PostgresRepository) -> None:
    release = asyncio.Event()
    at_measurements = asyncio.Event()
    gated = False
    attempts = 0

    async def snapshot(
        token_id: str, cutoff: Timestamp, *, anchor: dict[str, JsonValue], scope: str
    ) -> dict[str, JsonValue]:
        nonlocal attempts
        if gated:
            attempts += 1
            if attempts == 2:
                at_measurements.set()
            await release.wait()
        return {}

    market = SimpleNamespace(snapshot=AsyncMock(side_effect=snapshot))
    schedulers = [
        Scheduler(repo, ReceiptSettings(), market, None, versions=fixture_versions()) for _ in range(2)
    ]
    for index, scheduler in enumerate(schedulers):
        await scheduler.reaction_capture(
            {
                "event_id": f"e{index}",
                "token_id": f"t{index}",
                "checkpoint": "PRE_1H",
                "anchor": {
                    "type": "T1",
                    "ts": "2026-10-17T12:00:00Z",
                    "ts_prec": "EXACT",
                    "ts_prec_reason": None,
                },
            }
        )
    before = await repo.list("out.reaction_checkpoints")
    requests: list[dict[str, JsonValue]] = [
        {
            "event_id": f"e{index}",
            "token_id": f"t{index}",
            "checkpoint": "PRE_1H",
            "automatic_revision": True,
            "change_id": "contended",
            "reason": "Actual concurrent completion",
        }
        for index in range(2)
    ]
    gated = True
    tasks = [
        asyncio.create_task(scheduler.reaction_capture(request))
        for scheduler, request in zip(schedulers, requests, strict=True)
    ]
    try:
        await asyncio.wait_for(at_measurements.wait(), 1)
    finally:
        release.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    successful = [result for result in results if isinstance(result, UnchangedReaction)]
    refused = [result for result in results if isinstance(result, ValueError)]
    assert len(successful) == len(refused) == 1 and "scope" in str(refused[0])
    winner = successful[0]
    assert await repo.list("out.reaction_checkpoints") == before
    marker = await repo.get("ops.scheduler_checkpoints", "reaction-change:contended")
    owner_record = await repo.get("ops.scheduler_checkpoints", "reaction-change-owner:contended")
    assert marker is not None and owner_record is not None
    owner = ReactionChangeOwnership.model_validate(
        {
            field: owner_record[field]
            for field in ReactionChangeOwnership.model_fields
            if field in owner_record
        }
    )
    assert (owner.event_id, owner.token_id, owner.checkpoint) == (
        winner.event_id,
        winner.token_id,
        winner.checkpoint,
    )
    assert owner.marker_sha256 == sha256(marker)
    repeated = await schedulers[0 if winner.event_id == "e0" else 1].reaction_capture(
        requests[0 if winner.event_id == "e0" else 1]
    )
    assert isinstance(repeated, AppliedReactionChange) and repeated.event_id == winner.event_id
    assert market.snapshot.await_count == 4
    for scheduler in schedulers:
        await scheduler.close()


async def test_concurrent_targets_cannot_share_a_change_marker_owner() -> None:
    await concurrent_change_scope(MemoryRepository())


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_concurrent_targets_have_one_atomic_marker_owner(
    postgres_template: PostgresTemplate,
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        await concurrent_change_scope(repo)


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_failed_scope_write_rolls_back_marker_and_retry_is_bound(
    postgres_template: PostgresTemplate,
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
        scheduler = Scheduler(repo, ReceiptSettings(), market, None, versions=fixture_versions())
        captured = await scheduler.reaction_capture(
            {
                "event_id": "e",
                "token_id": "t",
                "checkpoint": "PRE_1H",
                "anchor": {
                    "type": "T1",
                    "ts": "2026-10-17T12:00:00Z",
                    "ts_prec": "EXACT",
                    "ts_prec_reason": None,
                },
            }
        )
        assert isinstance(captured, CapturedReaction)
        original = await repo.get("out.reaction_checkpoints", captured.checkpoint_key)
        request: dict[str, JsonValue] = {
            "event_id": "e",
            "token_id": "t",
            "checkpoint": "PRE_1H",
            "automatic_revision": True,
            "change_id": "rollback",
            "reason": "Actual retry after ownership storage failure",
        }
        async with await AsyncConnection.connect(repo.database_url, autocommit=True) as connection:
            await connection.execute("""
                CREATE FUNCTION public.reject_test_reaction_owner() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.storage_key = 'reaction-change-owner:rollback' THEN
                        RAISE EXCEPTION 'synthetic ownership persistence refusal' USING ERRCODE = 'check_violation';
                    END IF;
                    RETURN NEW;
                END
                $$
            """)
            await connection.execute("""
                CREATE TRIGGER reject_test_reaction_owner BEFORE INSERT ON ops.scheduler_checkpoints
                FOR EACH ROW EXECUTE FUNCTION public.reject_test_reaction_owner()
            """)
        with pytest.raises(CheckViolation, match="synthetic ownership persistence refusal"):
            await scheduler.reaction_capture(request)
        assert await repo.get("ops.scheduler_checkpoints", "reaction-change:rollback") is None
        assert await repo.get("ops.scheduler_checkpoints", "reaction-change-owner:rollback") is None
        assert await repo.list("out.reaction_checkpoints") == [original]
        async with await AsyncConnection.connect(repo.database_url, autocommit=True) as connection:
            await connection.execute("DROP TRIGGER reject_test_reaction_owner ON ops.scheduler_checkpoints")
            await connection.execute("DROP FUNCTION public.reject_test_reaction_owner()")
        result = await scheduler.reaction_capture(request)
        assert isinstance(result, UnchangedReaction) and result.event_id == "e"
        assert await repo.get("out.reaction_checkpoints", captured.checkpoint_key) == original
        replayed = await scheduler.reaction_capture(request)
        assert (
            isinstance(replayed, AppliedReactionChange)
            and replayed.data_fingerprint == result.data_fingerprint
        )
        assert market.snapshot.await_count == 3
        await scheduler.close()


async def test_retained_capture_without_fingerprint_is_explicit_and_never_rewritten() -> None:
    original_repo = MemoryRepository()
    market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
    scheduler = Scheduler(original_repo, ReceiptSettings(), market, None, versions=fixture_versions())
    receipt = await scheduler.reaction_capture(
        {
            "event_id": "e",
            "token_id": "t",
            "checkpoint": "PRE_1H",
            "anchor": {
                "type": "T1",
                "ts": "2026-10-17T12:00:00Z",
                "ts_prec": "EXACT",
                "ts_prec_reason": None,
            },
        }
    )
    assert isinstance(receipt, CapturedReaction)
    original = await original_repo.get("out.reaction_checkpoints", receipt.checkpoint_key)
    assert original is not None
    retained = {field: value for field, value in original.items() if field != "data_fingerprint"}
    historical_repo = MemoryRepository()
    await historical_repo.put("out.reaction_checkpoints", receipt.checkpoint_key, retained, immutable=True)
    historical = Scheduler(historical_repo, ReceiptSettings(), None, None, versions=fixture_versions())
    observed = await historical.reaction_capture({"event_id": "e", "token_id": "t", "checkpoint": "PRE_1H"})
    assert isinstance(observed, HistoricalCapturedReaction)
    assert observed.revision == 1 and observed.data_fingerprint is None
    assert observed.checkpoint_key == receipt.checkpoint_key
    assert await historical_repo.get("out.reaction_checkpoints", receipt.checkpoint_key) == retained
    assert await original_repo.get("out.reaction_checkpoints", receipt.checkpoint_key) == original
    assert market.snapshot.await_count == 1
    await historical.close()
    await scheduler.close()
