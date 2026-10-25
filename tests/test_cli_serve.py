"""tank serve sets the GIL switch interval before the service starts."""

import asyncio
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

    settings = InvalidCalibration(foundry_api_key="synthetic-private-readiness-key")
    checks = cli.readiness_checks(settings)
    by_name = {check.check: check for check in checks}
    assert not by_name["thresholds_syntax"].ok
    assert not by_name["calibration"].ok
    assert by_name["calibration"].reason == "ValidationError"
    assert by_name["foundry_api_key"].ok
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


class UncapturedSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.asyncio
async def test_sync_preflight_failure_stops_and_drains_runtime_before_propagating() -> None:
    stop, started, drained = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def runtime() -> None:
        started.set()
        await stop.wait()
        await asyncio.sleep(0)
        drained.set()

    async def synchronize() -> None:
        await started.wait()
        UncapturedSettings().require_versions()

    with pytest.raises(ValueError, match="captured"):
        await cli.run_with_synchronization(runtime(), synchronize(), stop)
    assert stop.is_set() and drained.is_set()


@pytest.mark.asyncio
async def test_successful_one_shot_sync_keeps_runtime_alive_until_signal_stop() -> None:
    stop, started, synchronized, drained = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )

    async def runtime() -> None:
        started.set()
        await stop.wait()
        drained.set()

    async def synchronize() -> None:
        await started.wait()
        synchronized.set()

    running = asyncio.create_task(cli.run_with_synchronization(runtime(), synchronize(), stop))
    try:
        await asyncio.wait_for(synchronized.wait(), timeout=1)
        assert not running.done()
        stop.set()
        await asyncio.wait_for(running, timeout=1)
        assert drained.is_set()
    finally:
        stop.set()
        await running


@pytest.mark.asyncio
async def test_shutdown_cancellation_awaits_sync_cleanup_and_runtime_drain() -> None:
    stop, started, sync_started, drained, sync_closed = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )

    async def runtime() -> None:
        started.set()
        await stop.wait()
        await asyncio.sleep(0)
        drained.set()

    async def synchronize() -> None:
        await started.wait()
        sync_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            sync_closed.set()

    running = asyncio.create_task(cli.run_with_synchronization(runtime(), synchronize(), stop))
    await asyncio.wait_for(sync_started.wait(), timeout=1)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert stop.is_set() and drained.is_set() and sync_closed.is_set()


@pytest.mark.asyncio
async def test_sync_failure_does_not_hide_runtime_drain_failure() -> None:
    stop, started = asyncio.Event(), asyncio.Event()

    async def runtime() -> None:
        started.set()
        await stop.wait()
        raise RuntimeError("owned runtime drain failed")

    async def synchronize() -> None:
        await started.wait()
        UncapturedSettings().require_versions()

    with pytest.raises(ExceptionGroup) as failed:
        await cli.run_with_synchronization(runtime(), synchronize(), stop)
    assert {type(error) for error in failed.value.exceptions} == {ValueError, RuntimeError}
