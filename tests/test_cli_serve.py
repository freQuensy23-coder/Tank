"""tank serve sets the GIL switch interval before the service starts."""

import json
import sys
from collections.abc import Awaitable, Callable

import pytest
from click.testing import CliRunner
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

import tank.cli as cli
from tank.settings import Settings
from tank.storage import Repository


def test_serve_sets_short_gil_switch_interval_before_running(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[float] = []

    def record_interval(command: Callable[[Repository, Settings], Awaitable[JsonValue]]) -> None:
        seen.append(sys.getswitchinterval())

    monkeypatch.setattr(cli, "run_db", record_interval)
    before = sys.getswitchinterval()
    try:
        sys.setswitchinterval(max(0.05, cli.GIL_SWITCH_INTERVAL_S * 10))
        result = CliRunner().invoke(cli.main, ["serve"])
        assert result.exit_code == 0, result.output
        assert seen == [cli.GIL_SWITCH_INTERVAL_S]
        assert sys.getswitchinterval() == cli.GIL_SWITCH_INTERVAL_S
    finally:
        sys.setswitchinterval(before)


def test_readiness_validates_missing_cutoff_without_leaking_input() -> None:
    class InvalidCalibration(Settings):
        model_config = SettingsConfigDict(env_file=None)

        def load_config(self, name: str) -> dict[str, JsonValue]:
            configuration = super().load_config(name)
            if name == "thresholds":
                configuration["filter_cutoff"] = None
            return configuration

    settings = InvalidCalibration(opencode_api_key="synthetic-private-readiness-key")
    checks = cli.readiness_checks(settings)
    by_name = {check.check: check for check in checks}
    assert not by_name["thresholds_syntax"].ok
    assert not by_name["calibration"].ok
    assert by_name["calibration"].reason == "ValidationError"
    assert by_name["opencode_api_key"].ok
    wire = json.dumps([check.model_dump(mode="json", exclude_none=True) for check in checks])
    assert "synthetic-private-readiness-key" not in wire
    assert "filter_cutoff" not in wire


def test_readiness_requires_declared_openma_identity_keys() -> None:
    class MissingAgentIdentity(Settings):
        model_config = SettingsConfigDict(env_file=None)

        def load_config(self, name: str) -> dict[str, JsonValue]:
            configuration = super().load_config(name)
            if name == "openma":
                del configuration["agent_id"]
            return configuration

    checks = {check.check: check for check in cli.readiness_checks(MissingAgentIdentity())}
    assert not checks["openma_syntax"].ok
    assert checks["openma_syntax"].reason == "ValidationError"
