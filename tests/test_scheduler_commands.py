"""Typed reaction intents refuse missing facts and foreign stored ownership before effects."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import JsonValue, ValidationError
from pydantic_settings import SettingsConfigDict
from source_support import fixture_versions
from test_storage import repo as repo

from tank.contracts.scheduler_commands import (
    InitialReactionCommand,
    ManualReactionRevisionCommand,
    ReactionLookupCommand,
    reaction_command_from_wire,
)
from tank.contracts.scheduler_results import CapturedReaction
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository, StorageBackend


class CommandSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.parametrize("checkpoint", ["PRE_1H", "PRE_4H"])
async def test_opaque_id_delimiters_cannot_borrow_another_reaction_or_pack(
    repo: StorageBackend, checkpoint: str
) -> None:
    market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
    scheduler = Scheduler(repo, CommandSettings(), market, None, versions=fixture_versions())
    first = await scheduler.reaction_capture(
        reaction_command_from_wire(
            {
                "event_id": "event",
                "token_id": "token:part",
                "checkpoint": "PRE_1H",
                "anchor": {
                    "type": "T1",
                    "ts": "2026-10-17T12:00:00Z",
                    "ts_prec": "EXACT",
                    "ts_prec_reason": None,
                },
            }
        )
    )
    assert isinstance(first, CapturedReaction)
    before = await repo.list("out.reaction_checkpoints")
    with pytest.raises(ValueError, match="scope"):
        await scheduler.reaction_capture(
            reaction_command_from_wire(
                {
                    "event_id": "event:token",
                    "token_id": "part",
                    "checkpoint": checkpoint,
                    "anchor": {
                        "type": "T1",
                        "ts": "2026-10-17T12:00:00Z",
                        "ts_prec": "EXACT",
                        "ts_prec_reason": None,
                    },
                }
            )
        )
    assert await repo.list("out.reaction_checkpoints") == before
    assert market.snapshot.await_count == 1
    await scheduler.close()


def initial_wire() -> dict[str, JsonValue]:
    return {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "PRE_1H",
        "anchor": {"type": "T1", "ts": "2026-10-17T12:00:00Z"},
    }


@pytest.mark.parametrize("field", ["event_id", "token_id", "checkpoint", "anchor"])
async def test_required_command_fact_is_refused_before_measurement_or_storage(field: str) -> None:
    wire = initial_wire()
    del wire[field]
    repo = MemoryRepository()
    market = SimpleNamespace(snapshot=AsyncMock(side_effect=AssertionError("No admitted measurement")))
    scheduler = Scheduler(repo, CommandSettings(), market, None, versions=fixture_versions())
    with pytest.raises(ValidationError):
        await scheduler.reaction_capture(reaction_command_from_wire(wire))
    assert await repo.list("out.reaction_checkpoints") == []
    assert await repo.list("ops.scheduler_checkpoints") == []
    market.snapshot.assert_not_called()
    await scheduler.close()


@pytest.mark.parametrize("revision", [None, True, 1.0, 0, -1, "2"])
async def test_reported_invalid_revision_never_becomes_initial_revision(revision: JsonValue) -> None:
    repo = MemoryRepository()
    scheduler = Scheduler(repo, CommandSettings(), None, None, versions=fixture_versions())
    wire = initial_wire()
    wire["revision"] = revision
    with pytest.raises(ValidationError):
        await scheduler.reaction_capture(reaction_command_from_wire(wire))
    assert await repo.list("ops.scheduler_checkpoints") == []
    await scheduler.close()


def test_original_omission_and_explicit_nulls_roundtrip_without_added_scheduling_facts() -> None:
    omitted = initial_wire()
    command = reaction_command_from_wire(omitted)
    assert isinstance(command, InitialReactionCommand)
    assert command.storage_fields() == omitted
    assert command.anchor.timestamp.precision is None
    assert command.anchor.timestamp.precision_reason == "INCOMPLETE"
    explicit = initial_wire() | {
        "cutoff": None,
        "cutoff_prec": None,
        "cutoff_prec_reason": None,
        "cutoff_bucket_anchor": None,
        "retro": None,
        "reason": None,
    }
    reported = reaction_command_from_wire(explicit)
    assert isinstance(reported, InitialReactionCommand)
    assert reported.storage_fields() == explicit
    assert reported.storage_fields() != command.storage_fields()


async def test_manual_revision_uses_frozen_prior_anchor_without_guessing_unknown_retro() -> None:
    repo = MemoryRepository()
    market = SimpleNamespace(snapshot=AsyncMock(return_value={}))
    scheduler = Scheduler(repo, CommandSettings(), market, None, versions=fixture_versions())
    first = await scheduler.reaction_capture(reaction_command_from_wire(initial_wire()))
    assert isinstance(first, CapturedReaction)
    original = await repo.get("out.reaction_checkpoints", first.checkpoint_key)
    assert original is not None and "retro" not in original
    command = reaction_command_from_wire(
        {
            "event_id": "event",
            "token_id": "token",
            "checkpoint": "PRE_1H",
            "revision": 2,
            "reason": "Explicit new observation",
        }
    )
    assert isinstance(command, ManualReactionRevisionCommand) and command.anchor is None
    result = await scheduler.reaction_capture(command)
    assert isinstance(result, CapturedReaction) and result.revision == 2
    revision = await repo.get("out.reaction_checkpoints", result.checkpoint_key)
    assert revision is not None and "retro" not in revision
    assert revision["anchor"] == original["anchor"] and revision["reason"] == command.reason
    assert await repo.get("out.reaction_checkpoints", first.checkpoint_key) == original
    assert await scheduler.reaction_capture(command) == result
    assert market.snapshot.await_count == 2
    await scheduler.close()


async def test_new_capture_requires_market_port_but_lookup_requires_no_new_measurement() -> None:
    repo = MemoryRepository()
    scheduler = Scheduler(repo, CommandSettings(), None, None, versions=fixture_versions())
    with pytest.raises(ValueError, match="market read service"):
        await scheduler.reaction_capture(reaction_command_from_wire(initial_wire()))
    with pytest.raises(ValueError, match="no captured checkpoint"):
        await scheduler.reaction_capture(
            ReactionLookupCommand(event_id="event", token_id="token", checkpoint="PRE_1H")
        )
    assert await repo.list("out.reaction_checkpoints") == []
    assert await repo.list("ops.scheduler_checkpoints") == []
    await scheduler.close()
