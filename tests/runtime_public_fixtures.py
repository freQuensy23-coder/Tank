"""Actual required runtime services with captured settings and refused external I/O."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import respx
from pydantic_settings import SettingsConfigDict

from tank import runtime_composition
from tank.runtime import Runtime
from tank.settings import Settings, resolve_versions
from tank.storage import StorageBackend


class PublicRepairSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@asynccontextmanager
async def public_runtime(
    repo: StorageBackend,
    directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[Runtime, respx.MockRouter]]:
    def preserve_test_logging(settings: Settings) -> None:
        return None

    monkeypatch.setattr(runtime_composition, "configure_logging", preserve_test_logging)
    settings = resolve_versions(
        PublicRepairSettings(
            data_dir=directory / "data",
            backup_dir=directory / "backup",
            codex_auth_path=directory / "unread-auth.json",
            telegram_token="test",
            telegram_alerts_chat="alerts",
            telegram_ops_chat="ops",
            openma_api_key="synthetic-openma-key",
            codex_gateway_key="synthetic-gateway-key",
            ops_progress_enabled=False,
        )
    )
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as router:
        runtime = runtime_composition.build_runtime(repo, settings)
        try:
            yield runtime, router
        finally:
            await runtime.close()
