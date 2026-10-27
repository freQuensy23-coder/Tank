from pathlib import Path
from typing import Literal

import httpx
import pytest
import zstandard
from pydantic import JsonValue
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_source_announcement_envelopes import AnnouncementSettings
from test_storage import repo as repo

from tank.contracts.json import encode_json
from tank.sources import SourceService, parsers
from tank.storage import StorageBackend
from tank.util import sha256

type Fault = Literal["FALSE", "ZERO", "EMPTY", "ARRAY", "OBJECT"]
URL = "https://source.example/list?per_page=2"
NEXT_URL = "https://source.example/list?per_page=2&page=2"


def record(identity: str) -> dict[str, JsonValue]:
    return {
        "document_number": identity,
        "title": "Actual " + identity,
        "abstract": "Actual body " + identity,
        "html_url": "https://source.example/doc/" + identity,
        "publication_date": "2026-10-20",
    }


def page(ids: list[str], cursor: JsonValue, *, reported: bool = True) -> bytes:
    wire: dict[str, JsonValue] = {"results": [record(identity) for identity in ids]}
    if reported:
        wire["next_page_url"] = cursor
    return encode_json(wire)


@pytest.mark.parametrize("reported", [False, True])
def test_absent_or_null_terminal_cursor_is_preserved(reported: bool) -> None:
    parsed = parsers.json_items(page([], None, reported=reported), "S03")
    assert parsed.discoveries == [] and parsed.errors == [] and parsed.wire_count == 0


@pytest.mark.parametrize("fault", ["FALSE", "ZERO", "EMPTY", "ARRAY", "OBJECT"])
async def test_invalid_reported_cursor_cannot_claim_exhaustion_or_commit_new_page_items(
    repo: StorageBackend, tmp_path: Path, fault: Fault
) -> None:
    invalid: dict[Fault, JsonValue] = {"FALSE": False, "ZERO": 0, "EMPTY": "", "ARRAY": [], "OBJECT": {}}
    phase: Literal["BASELINE", "INVALID", "RECOVER"] = "BASELINE"
    baseline_payload = page(["old-boundary"], None)
    invalid_payload = page(["new-2", "new-1"], invalid[fault])
    recovered_payload = page(["new-2", "new-1"], NEXT_URL)
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if str(request.url) == NEXT_URL:
            assert phase == "RECOVER"
            return httpx.Response(200, content=page(["old-boundary"], None))
        assert str(request.url) == URL
        payload = {"BASELINE": baseline_payload, "INVALID": invalid_payload, "RECOVER": recovered_payload}[
            phase
        ]
        return httpx.Response(200, content=payload, headers={"etag": phase})

    source = source_definition({"source_id": "S03", "feeds": [{"url": URL, "kind": "json"}]})
    budgets = fixture_request_budgets()
    settings = AnnouncementSettings(data_dir=tmp_path, ops_progress_enabled=False)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            first = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            first.sources = {"S03": source}
            try:
                baseline = await first.poll_once("S03")
                assert baseline.status == "HEALTHY" and baseline.items == 1
                before = await repo.list("ingest.source_items")
                checkpoint_before = await repo.get("ops.source_checkpoints", "S03:0")
                assert (
                    checkpoint_before is not None
                    and checkpoint_before["discovery_checkpoint"] == "old-boundary"
                )
                phase = "INVALID"
                refused = await first.poll_once("S03")
                assert refused.status == "DEGRADED" and refused.items == 0 and refused.errors
                assert await repo.list("ingest.source_items") == before
                checkpoint = await repo.get("ops.source_checkpoints", "S03:0")
                assert checkpoint is not None and checkpoint["discovery_checkpoint"] == "old-boundary"
                assert checkpoint["pending_batches"] == checkpoint_before["pending_batches"]
                assert checkpoint.get("history_gap") == checkpoint_before.get("history_gap")
                assert checkpoint["etag"] == "BASELINE"
                assert checkpoint["http_status"] == 200 and checkpoint["payload_sha256"] == sha256(
                    invalid_payload
                )
                assert len(list(tmp_path.rglob("*.zst"))) == 1
            finally:
                await first.close()
            phase = "RECOVER"
            restarted = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            restarted.sources = {"S03": source}
            try:
                recovered = await restarted.poll_once("S03")
                assert recovered.status == "HEALTHY" and recovered.items == 2
                repeated = await restarted.poll_once("S03")
                assert repeated.status == "HEALTHY" and repeated.items == 0
            finally:
                await restarted.close()
        stored = await repo.list("ingest.source_items")
        assert {item["external_id"] for item in stored} == {"old-boundary", "new-2", "new-1"}
        for item in stored:
            reference = item["raw_ref"]
            assert isinstance(reference, str)
            raw = zstandard.ZstdDecompressor().decompress(Path(reference).read_bytes())
            expected = baseline_payload if item["external_id"] == "old-boundary" else recovered_payload
            assert raw == expected and item["raw_sha256"] == sha256(expected)
        final = await repo.get("ops.source_checkpoints", "S03:0")
        assert final is not None and final["discovery_checkpoint"] == "new-2"
        assert final["history_gap"] is None and final["pending_batches"] == [] and final["etag"] == "RECOVER"
        assert calls == [URL, URL, URL, NEXT_URL, URL]
    finally:
        await budgets.shutdown()
