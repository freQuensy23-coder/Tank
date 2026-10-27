"""Announcement wire admission precedes item/raw/cache authority."""

from pathlib import Path
from typing import Literal, Protocol

import httpx
import pytest
import zstandard
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_sources import collector_http as collector_http

from tank.contracts.json import encode_json
from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import MemoryRepository
from tank.util import parse_time

URL = "https://api.bybit.com/v5/announcements/index?locale=en-US&limit=20"
type InvalidAnnouncement = Literal["MISSING", "FALSE", "FLOAT", "DUPLICATE", "ESCAPED", "NESTED"]


class AnnouncementSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


class CollectorBinder(Protocol):
    def __call__(self, http: httpx.AsyncClient) -> None: ...


def item() -> dict[str, JsonValue]:
    return {
        "title": "Synthetic protocol announcement",
        "description": "A synthetic announcement used to qualify source intake",
        "url": "https://announcements.bybit.com/en-US/article/synthetic-intake/",
        "publishTime": int(parse_time("2026-10-20T12:00:00Z").timestamp() * 1000),
    }


def malformed(kind: InvalidAnnouncement) -> bytes:
    result: dict[str, JsonValue] = {"list": [item()]}
    if kind == "MISSING":
        return encode_json({"result": result})
    if kind in {"FALSE", "FLOAT"}:
        return encode_json({"retCode": False if kind == "FALSE" else 0.0, "result": result})
    suffix = b',"result":' + encode_json(result) + b"}"
    if kind == "DUPLICATE":
        return b'{"retCode":0,"retCode":0' + suffix
    if kind == "ESCAPED":
        return b'{"r\\u0065tCode":0,"retCode":0' + suffix
    return b'{"retCode":0,"telemetry":{"ignored":1,"ignored":2}' + suffix


@pytest.mark.parametrize("invalid", ["MISSING", "FALSE", "FLOAT", "DUPLICATE", "ESCAPED", "NESTED"])
async def test_invalid_announcement_envelope_cannot_author_item_raw_or_etag(
    tmp_path: Path, collector_http: CollectorBinder, invalid: InvalidAnnouncement
) -> None:
    body = malformed(invalid)
    etag = "invalid"
    observed: list[bytes] = []

    def respond(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == URL
        observed.append(body)
        return httpx.Response(200, content=body, headers={"etag": etag})

    repo = MemoryRepository()
    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            collector_http(http)
            service = SourceService(
                repo,
                AnnouncementSettings(data_dir=tmp_path, ops_progress_enabled=False),
                http=http,
                versions=fixture_versions(),
                budgets=budgets,
            )
            service.sources = {
                "S11": source_definition({"source_id": "S11", "feeds": [{"url": URL, "kind": "json"}]})
            }
            refused = await service.poll_once("S11")
            assert refused.status == "DEGRADED" and refused.items == 0
            assert await repo.list("ingest.source_items") == []
            assert not list((tmp_path / "raw").rglob("*.zst"))
            checkpoint = await repo.get("ops.source_checkpoints", "S11:0")
            assert checkpoint is not None and "etag" not in checkpoint
            assert not checkpoint.get("pending_batches")
            body = encode_json({"retCode": 0, "result": {"list": [item()]}})
            etag = "valid"
            assert (await service.poll_once("S11")).items == 1
            [stored] = await repo.list("ingest.source_items")
            raw_ref = stored["raw_ref"]
            assert isinstance(raw_ref, str)
            assert zstandard.ZstdDecompressor().decompress(Path(raw_ref).read_bytes()) == body
            body = encode_json({"retCode": 0, "result": {"list": []}})
            etag = "empty"
            empty = await service.poll_once("S11")
            assert empty.status == "HEALTHY" and empty.items == 0
            assert await repo.list("ingest.source_items") == [stored]
            checkpoint = await repo.get("ops.source_checkpoints", "S11:0")
            assert checkpoint is not None and checkpoint["etag"] == "empty"
            await service.close()
        assert observed == [
            malformed(invalid),
            encode_json({"retCode": 0, "result": {"list": [item()]}}),
            body,
        ]
    finally:
        await budgets.shutdown()
        await repo.close()
