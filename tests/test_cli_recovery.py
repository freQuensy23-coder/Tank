"""Explicit recovery commands use the real captured CLI and transactional writer."""

import asyncio
import os
from pathlib import Path

import pytest
from click.testing import CliRunner
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from test_reaction_recovery import seed_recovery_history

import tank.cli as cli
from tank.contracts.json import decode_unique_json
from tank.contracts.reaction_recovery import ReactionRecoveryLink
from tank.settings import Settings
from tank.time_precision import propagate_timestamp
from tank.util import parse_time


class RecoveryCliSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.parametrize(
    "arguments",
    [
        ["reaction-recover"],
        ["reaction-recover", "original"],
        ["reaction-recover", " ", "--source-backfill-id", "source"],
        ["reaction-recover", "original", "--source-backfill-id", " \t"],
    ],
)
def test_recovery_cli_rejects_missing_or_blank_required_subjects_before_resources(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    def no_settings() -> Settings:
        raise AssertionError("Invalid command cannot open resources")

    monkeypatch.setattr(cli, "Settings", no_settings)
    result = CliRunner().invoke(cli.main, arguments)
    assert result.exit_code == 2
    assert "Missing" in result.output or "nonblank" in result.output


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("replay", [False, True])
async def test_recovery_cli_uses_explicit_witnesses_and_actual_resource_composition(
    postgres_template: PostgresTemplate,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replay: bool,
) -> None:
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        request = await seed_recovery_history(repo)
        original = await repo.get("ops.jobs", request.original_job_id)
        assert original is not None
        before = await repo.list("ops.jobs")
        configured = RecoveryCliSettings(
            database_url=repo.database_url,
            clickhouse_url=None,
            data_dir=tmp_path,
            replay=replay,
        )
        created: list[Settings] = []

        def settings_factory() -> Settings:
            created.append(configured)
            return configured

        monkeypatch.setattr(cli, "Settings", settings_factory)
        arguments = [
            "reaction-recover",
            request.original_job_id,
            "--source-backfill-id",
            request.source_backfill_id,
        ]

        async def invoke() -> dict[str, JsonValue]:
            result = await asyncio.to_thread(CliRunner().invoke, cli.main, arguments)
            assert result.exit_code == 0, result.output
            wire = decode_unique_json(result.output)
            assert isinstance(wire, dict)
            return wire

        outcome = await invoke()
        assert len(created) == 1
        if replay:
            assert outcome == {"kind": "UNRESOLVED", "reason": "SOURCE_MODE_UNSUPPORTED"}
            assert await repo.list("ops.jobs") == before
            assert await repo.get("ops.scheduler_checkpoints", "reaction-recovery:original") is None
        else:
            assert outcome["kind"] == "QUEUED"
            job_id = outcome["job_id"]
            link_key = outcome["recovery_link_key"]
            assert isinstance(job_id, str) and isinstance(link_key, str)
            queued = await repo.get("ops.jobs", job_id)
            link_wire = await repo.get("ops.scheduler_checkpoints", link_key)
            assert queued is not None and link_wire is not None
            link = ReactionRecoveryLink.model_validate(link_wire)
            assert link.state.recovered_job_id == job_id
            assert queued["payload"] == original["payload"]
            created_at = propagate_timestamp(link_wire, "at")
            due = propagate_timestamp(queued, "run_after")
            expiry = propagate_timestamp(queued, "expires_at")
            assert created_at.value is not None and due.value is not None and expiry.value is not None
            assert (parse_time(due.value) - parse_time(created_at.value)).total_seconds() == 1800
            assert (parse_time(expiry.value) - parse_time(created_at.value)).total_seconds() == 21600
            again = await invoke()
            assert again["kind"] == "EXISTING_WORK"
            assert await repo.get("ops.jobs", job_id) == queued
            assert await repo.get("ops.scheduler_checkpoints", link_key) == link_wire
            assert len(await repo.list("ops.jobs")) == len(before) + 1
            assert len(created) == 2
        assert await repo.get("ops.jobs", request.original_job_id) == original
