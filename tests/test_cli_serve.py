"""tank serve sets the GIL switch interval before the service starts."""

import json
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

import tank.cli as cli
from tank.contracts.configuration import Thresholds
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

        def thresholds(self) -> Thresholds:
            configuration = super().thresholds().model_dump(mode="json")
            configuration["filter_cutoff"] = None
            return Thresholds.model_validate(configuration)

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


@pytest.mark.parametrize(
    "configuration_name,required_field",
    [("registry", "tokens"), ("semantics", "metrics"), ("dependency_map", "edges"), ("api_limits", "limits")],
)
def test_readiness_validates_structural_configuration_not_only_yaml(
    tmp_path: Path, configuration_name: str, required_field: str
) -> None:
    marker = "synthetic-private-configuration-value"
    payload = Settings().load_config(configuration_name)
    del payload[required_field]
    payload["private_fixture_marker"] = marker
    (tmp_path / (configuration_name + ".yaml")).write_text(json.dumps(payload))
    settings = Settings(config_dir=tmp_path, codex_auth_path=tmp_path / "unused-auth.json")
    checks = cli.readiness_checks(settings)
    by_name = {check.check: check for check in checks}
    result = by_name[configuration_name + "_syntax"]
    assert not result.ok and result.reason == "ValidationError"
    assert marker not in json.dumps([check.model_dump(mode="json") for check in checks])
