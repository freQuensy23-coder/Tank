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
