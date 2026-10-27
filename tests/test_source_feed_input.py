"""HTTP feed bytes never authorize SDK filename or URL interpretation."""

from pathlib import Path
from typing import Literal, Never

import feedparser.api
import feedparser.http
import httpx
import pytest
import zstandard
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_storage import repo as repo

from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import StorageBackend
from tank.util import sha256


class LiteralFeedSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


type BodyMode = Literal["PATH", "URL", "XML"]


@pytest.mark.parametrize("mode", ["PATH", "URL", "XML"])
async def test_source_ingests_only_literal_http_feed_bytes(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: BodyMode
) -> None:
    local = tmp_path / "authored-local-only.xml"
    xml = (
        '<rss version="2.0"><channel><title>Authored feed</title><item><guid>literal-item</guid>'
        "<title>Bitcoin authored protocol notice</title><link>https://source.example/item</link>"
        "<description>"
        + "Bitcoin protocol authored source observation. " * 30
        + "</description><pubDate>Fri, 16 Oct 2026 12:00:00 GMT</pubDate></item></channel></rss>"
    ).encode()
    local.write_bytes(xml)
    if mode == "PATH":
        wire = str(local).encode()
    elif mode == "URL":
        wire = b"https://unused.invalid/feed"
    else:
        wire = xml
    file_reads: list[object] = []
    network_reads: list[tuple[object, ...]] = []

    def refuse_file(path: object, mode: object = "r") -> Never:
        file_reads.append(path)
        raise AssertionError("Feed SDK cannot interpret HTTP data as a filename")

    def refuse_network(*args: object, **kwargs: object) -> Never:
        network_reads.append(args)
        raise AssertionError("Feed SDK cannot interpret HTTP data as a URL")

    monkeypatch.setattr(feedparser.api, "open", refuse_file, raising=False)
    monkeypatch.setattr(feedparser.http, "get", refuse_network)
    requests: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://source.example/feed"
        requests.append(str(request.url))
        return httpx.Response(200, content=wire, headers={"content-type": "application/rss+xml"})

    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            service = SourceService(
                repo,
                LiteralFeedSettings(data_dir=tmp_path / "data", ops_progress_enabled=False),
                http=http,
                budgets=budgets,
                versions=fixture_versions(),
            )
            source = source_definition(
                {
                    "source_id": "S30",
                    "name": "Authored literal feed",
                    "feeds": [{"url": "https://source.example/feed", "kind": "rss"}],
                }
            )
            service.sources = {source.source_id: source}
            try:
                outcome = await service.poll_once(source.source_id)
            finally:
                await service.close()
        assert requests == ["https://source.example/feed"]
        assert file_reads == [] and network_reads == []
        assert local.read_bytes() == xml
        checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
        assert checkpoint is not None
        assert checkpoint["http_status"] == 200 and checkpoint["payload_sha256"] == sha256(wire)
        rows = await repo.list("ingest.source_items")
        if mode == "XML":
            assert outcome.status == "HEALTHY" and outcome.items == 1
            assert len(rows) == 1
            [record] = rows
            assert record["title"] == "Bitcoin authored protocol notice"
            assert record["raw_sha256"] == sha256(wire)
            raw_ref = record["raw_ref"]
            assert isinstance(raw_ref, str)
            assert zstandard.ZstdDecompressor().decompress(Path(raw_ref).read_bytes()) == wire
            jobs = await repo.list("ops.jobs")
            assert len(jobs) == 1 and jobs[0]["type"] == "FILTER_ITEM"
            payload = jobs[0]["payload"]
            assert isinstance(payload, dict) and payload["item_id"] == record["item_id"]
        else:
            assert outcome.status == "DEGRADED" and outcome.items == 0
            assert rows == [] and await repo.list("ops.jobs") == []
            assert list((tmp_path / "data").rglob("*.zst")) == []
    finally:
        await budgets.shutdown()
