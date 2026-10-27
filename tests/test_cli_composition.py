"""CLI composition keeps captured settings and actual repository ownership together."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import click
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue

import tank.cli as cli
from tank.contracts.json import decode_unique_json
from tank.settings import Settings
from tank.storage import PostgresRepository
from tank.util import JsonInput


def test_output_preserves_exact_native_decimal_explicit_absence_and_known_zero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    value: dict[str, JsonInput] = {
        "native": Decimal("1.00000000000000000001"),
        "at": datetime(2026, 10, 1, tzinfo=UTC),
        "missing": None,
        "known_zero": 0,
    }
    cli.output(value)
    wire = decode_unique_json(capsys.readouterr().out.encode())
    assert wire == {
        "native": "1.00000000000000000001",
        "at": "2026-10-01T00:00:00Z",
        "missing": None,
        "known_zero": 0,
    }


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("fail", [False, True])
async def test_run_db_captures_once_and_closes_actual_owned_repository(
    postgres_template: PostgresTemplate,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    fail: bool,
) -> None:
    import os

    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as fixture_repo:
        await fixture_repo.put("reg.tokens", "cli-control", {"token_id": "cli-control"})
        configured = Settings(
            _env_file=None,
            database_url=fixture_repo.database_url,
            clickhouse_url=None,
            data_dir=tmp_path,
        )
        created: list[Settings] = []
        resolved: list[Settings] = []
        events: list[str] = []
        real_resolve = cli.resolve_versions
        real_open = PostgresRepository.open
        real_close = PostgresRepository.close

        def settings_factory() -> Settings:
            created.append(configured)
            return configured

        def resolve(settings: Settings) -> Settings:
            actual = real_resolve(settings)
            resolved.append(actual)
            return actual

        async def opened(repository: PostgresRepository) -> PostgresRepository:
            actual = await real_open(repository)
            events.append("open")
            return actual

        async def closed(repository: PostgresRepository) -> None:
            await real_close(repository)
            events.append("close")

        monkeypatch.setattr(cli, "Settings", settings_factory)
        monkeypatch.setattr(cli, "resolve_versions", resolve)
        monkeypatch.setattr(PostgresRepository, "open", opened)
        monkeypatch.setattr(PostgresRepository, "close", closed)

        async def command(repository: PostgresRepository, settings: Settings) -> dict[str, JsonValue]:
            assert len(created) == len(resolved) == 1 and settings is resolved[0]
            versions = settings.require_versions()
            record = await repository.get("reg.tokens", "cli-control")
            assert record is not None and record["token_id"] == "cli-control"
            events.append("callback")
            if fail:
                raise ValueError("synthetic CLI failure")
            return {"token_id": record["token_id"], "config_version": versions.config_version}

        if fail:
            with pytest.raises(click.ClickException, match="synthetic CLI failure"):
                await asyncio.to_thread(cli.run_db, command)
            assert capsys.readouterr().out == ""
        else:
            await asyncio.to_thread(cli.run_db, command)
            wire = decode_unique_json(capsys.readouterr().out.encode())
            assert wire == {
                "token_id": "cli-control",
                "config_version": resolved[0].require_versions().config_version,
            }
        assert events == ["open", "callback", "close"]
        assert len(created) == len(resolved) == 1
