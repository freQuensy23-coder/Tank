from pathlib import Path
from typing import Literal

import httpx
import pytest
import zstandard
from pydantic import JsonValue
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_source_announcement_envelopes import AnnouncementSettings, CollectorBinder
from test_sources import collector_http as collector_http
from test_storage import repo as repo

from tank.contracts.json import decode_unique_json, encode_json
from tank.contracts.source_items import ExchangeAnnouncementDiscovery
from tank.sources import SourceService, parsers
from tank.storage import StorageBackend
from tank.util import parse_time, sha256, utcnow

type Publication = Literal["OMITTED", "NULL", "REPORTED", "ZERO"]
URL = "https://api.bybit.com/v5/announcements/index?locale=en-US&limit=20"
AUTHOR_TIME = 1679045608000
PUBLISHED_TIME = 1792497600000


def announcement(publication: Publication) -> bytes:
    item: dict[str, JsonValue] = {
        "url": "https://announcements.bybit.com/en-US/article/authored-release/",
        "title": "Authored release",
        "description": "Actual authored announcement body.",
        "dateTimestamp": AUTHOR_TIME,
    }
    if publication != "OMITTED":
        item["publishTime"] = {"NULL": None, "REPORTED": PUBLISHED_TIME, "ZERO": 0}[publication]
    return encode_json({"retCode": 0, "result": {"list": [item]}})


@pytest.mark.parametrize("publication", ["OMITTED", "NULL", "REPORTED", "ZERO"])
def test_only_reported_publication_timestamp_can_author_publication(publication: Publication) -> None:
    parsed = parsers.json_items(announcement(publication), "S11")
    assert parsed.errors == []
    [entry] = parsed.discoveries
    item = entry.discovery
    assert isinstance(item, ExchangeAnnouncementDiscovery)
    expected = {
        "OMITTED": None,
        "NULL": None,
        "REPORTED": "2026-10-20T12:00:00Z",
        "ZERO": "1970-01-01T00:00:00Z",
    }[publication]
    assert item.published_at == expected
    assert item.publication.precision == ("EXACT" if expected is not None else "COLLECTION_ONLY")
    assert item.publication.precision_reason == (None if expected is not None else "INCOMPLETE")
    assert item.text == "Actual authored announcement body."


@pytest.mark.parametrize("publication", ["OMITTED", "NULL", "REPORTED", "ZERO"])
async def test_actual_source_storage_preserves_unknown_publication_and_author_wire_evidence(
    repo: StorageBackend, tmp_path: Path, collector_http: CollectorBinder, publication: Publication
) -> None:
    payload = announcement(publication)
    requests: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        assert str(request.url) == URL
        return httpx.Response(200, content=payload)

    budgets = fixture_request_budgets()
    source = source_definition({"source_id": "S11", "feeds": [{"url": URL, "kind": "json"}]})
    settings = AnnouncementSettings(data_dir=tmp_path, ops_progress_enabled=False)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            collector_http(http)
            first = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            first.sources = {"S11": source}
            try:
                before = utcnow()
                outcome = await first.poll_once("S11")
                after = utcnow()
                assert outcome.status == "HEALTHY" and outcome.items == 1
            finally:
                await first.close()
            [stored] = await repo.list("ingest.source_items")
            expected = {
                "OMITTED": None,
                "NULL": None,
                "REPORTED": "2026-10-20T12:00:00Z",
                "ZERO": "1970-01-01T00:00:00Z",
            }[publication]
            assert stored["published_at"] == expected
            assert stored["published_prec"] == ("EXACT" if expected is not None else "COLLECTION_ONLY")
            if expected is None:
                assert stored["published_at_reason"] == "INCOMPLETE"
                assert stored["published_prec_reason"] == "INCOMPLETE"
            observed = stored["observed_at"]
            assert isinstance(observed, str) and before <= parse_time(observed) <= after
            assert stored["observed_at_prec"] == "COLLECTION_ONLY"
            reference = stored["raw_ref"]
            assert isinstance(reference, str)
            raw = zstandard.ZstdDecompressor().decompress(Path(reference).read_bytes())
            assert raw == payload and stored["raw_sha256"] == sha256(payload)
            wire = decode_unique_json(raw)
            assert isinstance(wire, dict)
            result = wire["result"]
            assert isinstance(result, dict)
            entries = result["list"]
            assert isinstance(entries, list)
            entry = entries[0]
            assert isinstance(entry, dict) and entry["dateTimestamp"] == AUTHOR_TIME
            assert ("publishTime" in entry) == (publication != "OMITTED")
            restarted = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            restarted.sources = {"S11": source}
            try:
                repeated = await restarted.poll_once("S11")
                assert repeated.status == "HEALTHY" and repeated.items == 0
            finally:
                await restarted.close()
            assert await repo.list("ingest.source_items") == [stored]
            assert len(requests) == 2
    finally:
        await budgets.shutdown()
