"""Source ingestion boundaries from the 2026-10-20 requirements update."""

import asyncio
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml

from tank.sources import SourceService, parsers
from tank.storage import MemoryRepository


def config(tmp_path: Path, sources):
    return SimpleNamespace(
        data_dir=tmp_path,
        user_agent="Tank source regression",
        config_version="test",
        code_version="test",
        load_config=lambda name: {"sources": sources} if name == "sources" else {},
    )


@pytest.mark.asyncio
async def test_dtcc_official_html_only_and_zero_links_stays_degraded(tmp_path: Path) -> None:
    source = next(
        s
        for s in yaml.safe_load(Path("config/sources.yaml").read_text())["sources"]
        if s["source_id"] == "S08"
    )
    index = source["feeds"][0]["url"]
    assert index == "https://www.dtcc.com/press-releases"
    assert source["feeds"][0]["kind"] == "html"
    release = index + "/2026/DTCC-Official-release"
    mode = ["release"]
    calls = []

    def transport(request: pytest.FixtureRequest):
        calls.append((request.method, str(request.url)))
        assert request.url.host == "www.dtcc.com"
        assert not request.headers.get("authorization")
        if str(request.url) == index:
            body = '<a href="' + release + '">Release</a>' if mode[0] == "release" else '<a href="/">Home</a>'
            return httpx.Response(200, text=body)
        assert str(request.url) == release
        return httpx.Response(
            200,
            text='<main><h1>DTCC release</h1><div class="content-item--richtextblock"><div class="rtf">An authored official release.</div></div></main>',
        )

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        service = SourceService(repo, config(tmp_path, [source]), http=http)
        try:
            first = await service.poll_once("S08")
            assert first["status"] == "HEALTHY" and first["items"] == 1
            mode[0] = "empty"
            for _ in range(2):
                outcome = await service.poll_once("S08")
                state = await repo.get("ops.source_checkpoints", "S08:0")
                assert outcome["status"] == state["status"] == "DEGRADED"
                assert "No article links matched" in state["acquisition_error"]
            assert len(await repo.list("ingest.source_items")) == 1
            assert all(method == "GET" for method, _ in calls)
            assert not any("coveo" in url or "/_next/" in url for _, url in calls)
        finally:
            await service.close()


@pytest.mark.asyncio
async def test_duplicate_preserves_capture_and_counts_one_observation(tmp_path: Path) -> None:
    repo = MemoryRepository()
    source = {"source_id": "S30", "name": "Original publisher", "feeds": []}
    service = SourceService(repo, config(tmp_path, [source]))
    first_time, later = "2026-10-20T10:00:00Z", "2026-10-20T10:05:00Z"

    def captured(item_id, payload, item):
        ref, digest = service.raw(payload, source_id="S30", item_id=item_id, observed_at=item["observed_at"])
        return dict(item, item_id=item_id, raw_ref=ref, raw_sha256=digest)

    incoming = {
        "external_id": "one",
        "title": "Original title",
        "text": "Original text",
        "published_at": first_time,
        "published_at_precision": "EXACT",
        "observed_at": first_time,
    }
    first = deepcopy(await service.ingest(source, captured("first", b"Original publication bytes", incoming)))
    changed = captured(
        "second",
        b"Observed edited publication bytes",
        {
            **incoming,
            "feed": "https://example.org/second-feed",
            "title": "Edited title",
            "text": "Edited text",
            "published_at": later,
            "observed_at": later,
        },
    )
    duplicate = await service.ingest({**source, "name": "New config name"}, changed)
    replay = await service.ingest(source, captured("first", b"Original publication bytes", incoming))
    try:
        assert {k: v for k, v in duplicate.items() if k not in {"observation_count", "observed_feeds"}} == {
            k: v for k, v in first.items() if k not in {"observation_count", "observed_feeds"}
        }
        assert duplicate["item_id"] == replay["item_id"] == "first"
        assert replay["observation_count"] == 2
        assert not Path(changed["raw_ref"]).exists() and Path(first["raw_ref"]).exists()
        assert len(await repo.list("ingest.source_items")) == 1
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_poll_and_loop_never_reconstruct_legacy_binance_body_or_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = MemoryRepository()
    source = {"source_id": "S09", "feeds": []}
    service = SourceService(repo, config(tmp_path, [source]))
    ref, digest = service.raw(
        json.dumps(
            {
                "data": {
                    "code": "legacy",
                    "publishDate": 1791948608368,
                    "body": {"child": [{"text": "A later parser could read this"}]},
                }
            }
        ).encode(),
        source_id="S09",
        item_id="legacy",
        observed_at="2026-10-14T03:30:09Z",
    )
    original = {
        "item_id": "legacy",
        "source_id": "S09",
        "external_id": "legacy",
        "title": "First capture",
        "text": "",
        "published_at": "2026-10-14T03:30:08.368000Z",
        "published_at_precision": "SECOND",
        "published_prec": "EXACT",
        "detail_raw_ref": ref,
        "detail_raw_sha256": digest,
        "raw_ref": ref,
        "raw_sha256": digest,
    }
    await repo.put("ingest.source_items", "legacy", deepcopy(original))

    def forbidden(*args, **kwargs) -> None:
        raise AssertionError("Captured news must not be reparsed")

    monkeypatch.setattr(parsers, "binance_body", forbidden)
    try:
        for _ in range(2):
            assert (await service.poll_once("S09"))["status"] == "HEALTHY"
        stop = asyncio.Event()
        stop.set()
        await service.poller.loop(source, stop)
        assert await repo.get("ingest.source_items", "legacy") == original
        assert not await repo.list("ops.source_checkpoints")
        assert not await repo.list("ops.jobs")
    finally:
        await service.close()
