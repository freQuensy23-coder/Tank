import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tank.capture import CaptureService


def settings(**overrides):
    return SimpleNamespace(capture_only=True, replay=False, **overrides)


@pytest.mark.asyncio
async def test_capture_starts_after_registry_and_partitions_without_model_jobs(monkeypatch):
    order = []
    stop = asyncio.Event()
    repo = SimpleNamespace(maintain_partitions=AsyncMock(side_effect=lambda: order.append("partitions")))

    async def sync(actual_repo, actual_settings):
        assert actual_repo is repo
        assert actual_settings.capture_only
        order.append("registry")

    monkeypatch.setattr("tank.capture.sync_registry", sync)

    async def source_run(flag):
        order.append("sources")
        assert order[:2] == ["registry", "partitions"]
        await flag.wait()

    async def market_run(flag):
        order.append("market")
        stop.set()
        await flag.wait()

    sources = SimpleNamespace(run=source_run, close=AsyncMock())
    market = SimpleNamespace(run=market_run, close=AsyncMock())
    service = CaptureService(repo, settings(), sources=sources, market=market)
    await service.run(stop)
    assert {"sources", "market"} <= set(order)
    sources.close.assert_awaited_once()
    market.close.assert_awaited_once()
    assert not hasattr(service, "gateway")
    assert not hasattr(service, "handlers")


@pytest.mark.asyncio
async def test_startup_failure_closes_capture_resources(monkeypatch):
    monkeypatch.setattr("tank.capture.sync_registry", AsyncMock(side_effect=ValueError("bad registry")))
    source = SimpleNamespace(run=AsyncMock(), close=AsyncMock())
    market = SimpleNamespace(run=AsyncMock(), close=AsyncMock())
    service = CaptureService(SimpleNamespace(), settings(), sources=source, market=market)
    with pytest.raises(ValueError, match="bad registry"):
        await service.run(asyncio.Event())
    source.run.assert_not_awaited()
    market.run.assert_not_awaited()
    source.close.assert_awaited_once()
    market.close.assert_awaited_once()


@pytest.mark.parametrize(
    "config", [SimpleNamespace(capture_only=False), SimpleNamespace(capture_only=True, replay=True)]
)
def test_capture_requires_explicit_live_phase(config):
    with pytest.raises(ValueError):
        CaptureService(SimpleNamespace(), config, sources=object(), market=object())


def test_capture_cli_sets_explicit_phase_without_full_runtime(monkeypatch):
    from click.testing import CliRunner

    from tank.cli import main

    calls = []

    class FakeSettings:
        def model_copy(self, *, update):
            assert update == {"capture_only": True}
            return SimpleNamespace(capture_only=True)

    class FakeCapture:
        def __init__(self, repo, config):
            assert config.capture_only
            calls.append(repo)

        async def run(self, stop):
            assert isinstance(stop, asyncio.Event)
            stop.set()

    def run_db(fn):
        result = asyncio.run(fn("fake-repo", FakeSettings()))
        assert result == {"status": "stopped", "phase": "capture"}

    monkeypatch.setattr("tank.cli.run_db", run_db)
    monkeypatch.setattr("tank.capture.CaptureService", FakeCapture)
    result = CliRunner().invoke(main, ["capture"])
    assert result.exit_code == 0, result.output
    assert calls == ["fake-repo"]
