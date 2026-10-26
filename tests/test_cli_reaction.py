"""The actual recapture producer retains its genuine manual-revision wire form."""

from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

import tank.cli as cli
from tank.contracts.scheduler_commands import ManualReactionRevisionCommand
from tank.runtime_reaction import reaction_job_command
from tank.settings import Settings
from tank.storage import MemoryRepository


class CliFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


async def test_click_recapture_enqueues_the_actual_manual_request_without_invented_schedule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands: list[Callable[[MemoryRepository, Settings], Awaitable[JsonValue]]] = []

    def capture(command: Callable[[MemoryRepository, Settings], Awaitable[JsonValue]]) -> None:
        commands.append(command)

    monkeypatch.setattr(cli, "run_db", capture)
    result = CliRunner().invoke(
        cli.main,
        [
            "reaction",
            "event",
            "--token-id",
            "token",
            "--recapture",
            "PRE_1H",
            "--reason",
            "Operator correction",
        ],
    )
    assert result.exit_code == 0 and len(commands) == 1
    repo = MemoryRepository()
    prior_inventory: dict[str, JsonValue] = {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "PRE_1H",
        "revision": 1,
    }
    await repo.put("out.reaction_checkpoints", "event:token:PRE_1H:1", prior_inventory, immutable=True)
    returned = await commands[0](repo, CliFixtureSettings(config_dir=tmp_path, data_dir=tmp_path))
    assert isinstance(returned, dict)
    payload = returned["payload"]
    assert isinstance(payload, dict)
    assert payload == {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "PRE_1H",
        "revision": 2,
        "reason": "Operator correction",
    }
    command = reaction_job_command(payload)
    assert isinstance(command, ManualReactionRevisionCommand)
    assert command.model_fields_set == set(payload)
    assert command.anchor is None and command.cutoff is None and command.retro is None
    assert command.storage_fields() == payload
    assert await repo.get("out.reaction_checkpoints", "event:token:PRE_1H:1") == prior_inventory
    assert await repo.list("ops.scheduler_checkpoints") == []


@pytest.mark.parametrize("fault", ["missing", "null", "boolean", "zero", "numeric-text"])
async def test_manual_cli_never_invents_a_revision_from_invalid_prior_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    commands: list[Callable[[MemoryRepository, Settings], Awaitable[JsonValue]]] = []

    def capture(command: Callable[[MemoryRepository, Settings], Awaitable[JsonValue]]) -> None:
        commands.append(command)

    monkeypatch.setattr(cli, "run_db", capture)
    result = CliRunner().invoke(
        cli.main,
        [
            "reaction",
            "event",
            "--token-id",
            "token",
            "--recapture",
            "PRE_1H",
            "--reason",
            "Operator correction",
        ],
    )
    assert result.exit_code == 0 and len(commands) == 1
    prior: dict[str, JsonValue] = {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "PRE_1H",
        "revision": 1,
    }
    if fault == "missing":
        prior.pop("revision")
    elif fault == "null":
        prior["revision"] = None
    elif fault == "boolean":
        prior["revision"] = True
    elif fault == "zero":
        prior["revision"] = 0
    else:
        prior["revision"] = "1"
    repo = MemoryRepository()
    await repo.put("out.reaction_checkpoints", "prior-inventory", prior, immutable=True)
    with pytest.raises(ValueError):
        await commands[0](repo, CliFixtureSettings(config_dir=tmp_path, data_dir=tmp_path))
    assert await repo.list("ops.jobs") == []
    assert await repo.get("out.reaction_checkpoints", "prior-inventory") == prior


@pytest.mark.parametrize("arguments", [["reaction", " "], ["reaction", "event", "--token-id", " "]])
def test_cli_reaction_does_not_treat_supplied_blank_identity_as_an_unreported_filter(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    called = False

    def refuse(command: Callable[[MemoryRepository, Settings], Awaitable[JsonValue]]) -> None:
        nonlocal called
        called = True
        raise AssertionError("Invalid identity cannot begin a database operation")

    monkeypatch.setattr(cli, "run_db", refuse)
    result = CliRunner().invoke(cli.main, arguments)
    assert result.exit_code != 0 and not called
    assert "nonblank" in result.output


async def test_manual_request_retains_unused_wire_overrides_but_plans_from_actual_prior_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from runtime_public_fixtures import public_runtime

    from tank.scheduler import PendingReactionPlan

    repo = MemoryRepository()
    prior_anchor: dict[str, JsonValue] = {"type": "T1", "ts": "2026-10-01T12:00:00Z", "ts_prec": "EXACT"}
    prior: dict[str, JsonValue] = {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "PRE_1H",
        "revision": 1,
        "anchor": prior_anchor,
        "retro": True,
        "captured_at": "2026-10-01T12:01:00Z",
        "captured_at_prec": "COLLECTION_ONLY",
    }
    await repo.put("out.reaction_checkpoints", "event:token:PRE_1H:1", prior, immutable=True)
    payload: dict[str, JsonValue] = {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "PRE_1H",
        "revision": 2,
        "reason": "Operator correction",
        "retro": False,
        "cutoff": "2000-01-01T00:00:00Z",
        "anchor": {"type": "T0", "ts": "2000-01-01T00:00:00Z", "ts_prec": "EXACT"},
    }
    command = reaction_job_command(payload)
    assert isinstance(command, ManualReactionRevisionCommand)
    assert command.storage_fields() == payload
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        plan = await runtime.scheduler._reaction_plan(command)
        assert isinstance(plan, PendingReactionPlan)
        assert plan.capture.anchor.source_fields() == prior_anchor
        assert plan.capture.retro is True and plan.capture.cutoff is None
        assert plan.capture.revision == 2 and plan.capture.reason == "Operator correction"
        assert plan.source.storage_fields() == payload
        assert not router.calls
    assert await repo.get("out.reaction_checkpoints", "event:token:PRE_1H:1") == prior
    assert len(await repo.list("out.reaction_checkpoints")) == 1
    assert await repo.list("ana.market_truth") == []
