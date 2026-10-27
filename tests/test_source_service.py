import asyncio
import ssl
from pathlib import Path
from typing import Never

import httpx
import pytest
from httpx import AsyncClient
from pydantic import JsonValue, ValidationError
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions
from test_storage import repo as repo

from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import StorageBackend


class ServiceSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


class BrokenServiceSettings(ServiceSettings):
    def load_config(self, name: str) -> dict[str, JsonValue]:
        result = super().load_config(name)
        if name == "sources":
            sources = result["sources"]
            if not isinstance(sources, list) or not sources or not isinstance(sources[0], dict):
                raise AssertionError("Source fixture requires the actual configuration shape")
            sources[0]["name"] = False
        return result


def refuse_request(request: httpx.Request) -> Never:
    raise AssertionError("A stopped source runner cannot perform an external request")


@pytest.fixture
def created_clients(monkeypatch: pytest.MonkeyPatch) -> list[AsyncClient]:
    clients: list[AsyncClient] = []

    def create(
        *, timeout: float, follow_redirects: bool, headers: dict[str, str], verify: ssl.SSLContext
    ) -> AsyncClient:
        client = AsyncClient(
            timeout=timeout,
            follow_redirects=follow_redirects,
            headers=headers,
            verify=verify,
            transport=httpx.MockTransport(refuse_request),
        )
        clients.append(client)
        return client

    monkeypatch.setattr(httpx, "AsyncClient", create)
    return clients


@pytest.mark.parametrize("borrowed", [False, True])
async def test_source_constructor_and_stopped_runner_preserve_actual_client_ownership(
    repo: StorageBackend, tmp_path: Path, created_clients: list[AsyncClient], borrowed: bool
) -> None:
    http = AsyncClient(transport=httpx.MockTransport(refuse_request)) if borrowed else None
    budgets = fixture_request_budgets()
    try:
        service = SourceService(
            repo,
            ServiceSettings(data_dir=tmp_path / "data", ops_progress_enabled=False),
            http=http,
            budgets=budgets,
            versions=fixture_versions(),
        )
        try:
            assert service.owns_http is (not borrowed)
            stop = asyncio.Event()
            stop.set()
            await service.run(stop)
            assert service.poller.active_acquisitions == {} and service.poller.active_details == {}
            assert await repo.list("ops.source_checkpoints") == []
            assert await repo.list("ingest.source_items") == [] and await repo.list("ops.jobs") == []
        finally:
            await service.close()
        assert service.http.is_closed is (not borrowed)
        await budgets.acquire("source-shared-after-close", 1, 60)
        if borrowed:
            assert created_clients == [] and service.http is http
        else:
            assert created_clients == [service.http]
    finally:
        if http is not None:
            await http.aclose()
        await budgets.shutdown()


async def test_invalid_source_configuration_refuses_before_allocating_owned_http(
    repo: StorageBackend, tmp_path: Path, created_clients: list[AsyncClient]
) -> None:
    budgets = fixture_request_budgets()
    try:
        with pytest.raises(ValidationError):
            SourceService(
                repo,
                BrokenServiceSettings(data_dir=tmp_path / "data", ops_progress_enabled=False),
                budgets=budgets,
                versions=fixture_versions(),
            )
        assert created_clients == []
        assert await repo.list("ops.source_checkpoints") == [] and await repo.list("ops.jobs") == []
    finally:
        await budgets.shutdown()
