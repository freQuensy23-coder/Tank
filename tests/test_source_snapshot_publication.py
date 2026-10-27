from pathlib import Path
from typing import Literal

import httpx
import pytest
import zstandard
from pydantic import JsonValue
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_source_announcement_envelopes import AnnouncementSettings
from test_storage import repo as repo

from tank.contracts.json import decode_unique_json, encode_json
from tank.contracts.source_items import GovernanceDiscovery
from tank.sources import SourceService, parsers
from tank.storage import StorageBackend
from tank.util import parse_time, sha256, utcnow

type Creation = Literal["OMITTED", "NULL", "REPORTED", "ZERO"]
URL = "https://hub.snapshot.org/graphql"
START = 1792497600
END = 1792584000
CREATED = 1792494000


def proposal(creation: Creation) -> bytes:
    record: dict[str, JsonValue] = {
        "id": "authored-proposal",
        "title": "Actual authored vote",
        "body": "Actual governance body.",
        "state": "closed",
        "start": START,
        "end": END,
        "scores": [1, 2],
        "scores_total": 3,
        "space": {"id": "authored.eth"},
    }
    if creation != "OMITTED":
        record["created"] = {"NULL": None, "REPORTED": CREATED, "ZERO": 0}[creation]
    return encode_json({"data": {"proposals": [record]}})


def expected_creation(creation: Creation) -> str | None:
    return {
        "OMITTED": None,
        "NULL": None,
        "REPORTED": "2026-10-20T11:00:00Z",
        "ZERO": "1970-01-01T00:00:00Z",
    }[creation]


@pytest.mark.parametrize("creation", ["OMITTED", "NULL", "REPORTED", "ZERO"])
def test_vote_start_cannot_substitute_for_unreported_proposal_creation(creation: Creation) -> None:
    parsed = parsers.json_items(proposal(creation), "S24")
    assert parsed.errors == []
    [entry] = parsed.discoveries
    item = entry.discovery
    assert isinstance(item, GovernanceDiscovery)
    assert item.published_at == expected_creation(creation)
    assert item.end == "2026-10-21T12:00:00Z" and item.state == "closed"
    assert item.external_id == "authored-proposal|closed" and item.proposal_id == "authored-proposal"
    assert item.scores == [1, 2] and item.scores_total == 3
    assert item.publication.precision_reason == ("INCOMPLETE" if creation in {"OMITTED", "NULL"} else None)


@pytest.mark.parametrize("creation", ["OMITTED", "NULL", "REPORTED", "ZERO"])
async def test_actual_snapshot_query_storage_keeps_creation_unknown_and_vote_clocks_original(
    repo: StorageBackend, tmp_path: Path, creation: Creation
) -> None:
    payload = proposal(creation)
    fields = "id title body state start end scores scores_total space { id }"
    if creation != "OMITTED":
        fields += " created"
    query = (
        'query { proposals(first: 20, where: {space_in: ["authored.eth"]}, orderBy: "created", orderDirection: desc) { '
        + fields
        + " } }"
    )
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        assert str(request.url) == URL and request.method == "POST"
        assert decode_unique_json(request.content) == {"query": query}
        return httpx.Response(200, content=payload)

    source = source_definition(
        {
            "source_id": "S24",
            "feeds": [{"url": URL, "kind": "json", "method": "POST", "body": {"query": query}}],
        }
    )
    settings = AnnouncementSettings(data_dir=tmp_path, ops_progress_enabled=False)
    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            first = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            first.sources = {"S24": source}
            try:
                before = utcnow()
                result = await first.poll_once("S24")
                after = utcnow()
                assert result.status == "HEALTHY" and result.items == 1
            finally:
                await first.close()
            [stored] = await repo.list("ingest.source_items")
            assert stored["published_at"] == expected_creation(creation)
            assert stored["published_prec"] == (
                "COLLECTION_ONLY" if creation in {"OMITTED", "NULL"} else "EXACT"
            )
            if creation in {"OMITTED", "NULL"}:
                assert (
                    stored["published_at_reason"] == "INCOMPLETE"
                    and stored["published_prec_reason"] == "INCOMPLETE"
                )
            observed = stored["observed_at"]
            assert isinstance(observed, str) and before <= parse_time(observed) <= after
            assert stored["observed_at_prec"] == "COLLECTION_ONLY"
            assert stored["end"] == "2026-10-21T12:00:00Z" and stored["state"] == "closed"
            assert stored["external_id"] == "authored-proposal|closed" and stored["scores_total"] == 3
            reference = stored["raw_ref"]
            assert isinstance(reference, str)
            raw = zstandard.ZstdDecompressor().decompress(Path(reference).read_bytes())
            assert raw == payload and stored["raw_sha256"] == sha256(payload)
            wire = decode_unique_json(raw)
            assert isinstance(wire, dict)
            data = wire["data"]
            assert isinstance(data, dict)
            proposals = data["proposals"]
            assert isinstance(proposals, list)
            recorded = proposals[0]
            assert isinstance(recorded, dict) and recorded["start"] == START and recorded["end"] == END
            assert ("created" in recorded) == (creation != "OMITTED")
            restarted = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            restarted.sources = {"S24": source}
            try:
                repeated = await restarted.poll_once("S24")
                assert repeated.status == "HEALTHY" and repeated.items == 0
            finally:
                await restarted.close()
            assert await repo.list("ingest.source_items") == [stored] and len(calls) == 2
    finally:
        await budgets.shutdown()
