from pathlib import Path
from typing import Literal

import httpx
import pytest
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_storage import repo as repo

from tank.contracts.source_checks import SavedSourceHealth, SourceProbeSuccess, source_check_record
from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import StorageBackend
from tank.util import sha256


class CheckSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.parametrize("owner", ["MATCHING", "FOREIGN", "UNREPORTED"])
async def test_saved_health_binds_only_reported_checkpoint_owner_without_rewriting_history(
    repo: StorageBackend, tmp_path: Path, owner: Literal["MATCHING", "FOREIGN", "UNREPORTED"]
) -> None:
    stored: dict[str, JsonValue] = {"status": "HEALTHY", "opaque_extension": {"retained": [1, "actual"]}}
    if owner != "UNREPORTED":
        stored["source_id"] = "S30" if owner == "MATCHING" else "S31"
    await repo.put("ops.source_checkpoints", "S30:0", stored)
    before = await repo.get("ops.source_checkpoints", "S30:0")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Saved checkpoint health cannot perform an HTTP probe")

    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
            service = SourceService(
                repo,
                CheckSettings(data_dir=tmp_path, ops_progress_enabled=False),
                http=http,
                budgets=budgets,
                versions=fixture_versions(),
            )
            source = source_definition(
                {
                    "source_id": "S30",
                    "name": "Requested source",
                    "feeds": [{"url": "https://source.example/feed"}],
                }
            )
            service.sources = {source.source_id: source}
            try:
                [health] = await service.check()
            finally:
                await service.close()
        assert isinstance(health, SavedSourceHealth)
        assert health.source_id == "S30"
        assert health.status == ("HEALTHY" if owner == "MATCHING" else "UNVERIFIED")
        assert health.checkpoints[0].document == before
        record = source_check_record(health)
        assert record["states"] == [before]
        assert await repo.get("ops.source_checkpoints", "S30:0") == before
        assert await repo.list("ingest.source_items") == [] and await repo.list("ops.jobs") == []
    finally:
        await budgets.shutdown()


async def test_protocol_feed_kind_uses_the_same_admitted_rss_boundary_without_ingestion(
    repo: StorageBackend, tmp_path: Path
) -> None:
    payload = (
        b'<rss version="2.0"><channel><title>Authored feed</title><item><guid>one</guid>'
        b"<title>Actual release</title><link>https://source.example/one</link>"
        b"<description>Actual source body</description></item></channel></rss>"
    )
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        assert str(request.url) == "https://source.example/feed"
        return httpx.Response(200, content=payload, headers={"content-type": "application/rss+xml"})

    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            service = SourceService(
                repo,
                CheckSettings(data_dir=tmp_path, ops_progress_enabled=False),
                http=http,
                budgets=budgets,
                versions=fixture_versions(),
            )
            source = source_definition(
                {
                    "source_id": "S30",
                    "name": "Authored RSS feed",
                    "feeds": [{"url": "https://source.example/feed", "kind": "feed"}],
                }
            )
            service.sources = {source.source_id: source}
            try:
                [probe] = await service.check(probe=True)
            finally:
                await service.close()
        assert isinstance(probe, SourceProbeSuccess)
        assert probe.status == "PASS" and probe.item_count == 1
        assert probe.context.kind == "feed" and probe.context.method == "GET"
        assert probe.response.http_status == 200 and probe.response.payload_sha256 == sha256(payload)
        assert probe.response.response_content_type == "application/rss+xml"
        assert calls == ["https://source.example/feed"]
        assert await repo.list("ingest.source_items") == [] and await repo.list("ops.jobs") == []
        assert await repo.list("ops.source_checkpoints") == []
        assert list(tmp_path.rglob("*.zst")) == []
    finally:
        await budgets.shutdown()
