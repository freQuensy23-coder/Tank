import json
from pathlib import Path
from typing import Literal

import httpx
import pytest
import zstandard
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_storage import repo as repo

from tank.settings import Settings
from tank.sources import SourceService, parsers
from tank.storage import StorageBackend
from tank.util import sha256

type Fault = Literal[
    "MISSING_CONTENT",
    "NULL_CONTENT",
    "BOOLEAN_CONTENT",
    "MISSING_TYPE",
    "MISSING_VALUE",
    "BOOLEAN_VALUE",
    "EMPTY",
]


class ContentfulSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def document(second: JsonValue, *, empty: bool = False) -> bytes:
    children: list[JsonValue] = (
        []
        if empty
        else [
            {"nodeType": "paragraph", "content": [{"nodeType": "text", "value": "Actual first block."}]},
            second,
        ]
    )
    data = {
        "props": {
            "pageProps": {
                "article": {
                    "fields": {
                        "title": "Actual release",
                        "content": {"nodeType": "document", "content": children},
                    }
                }
            }
        }
    }
    return (
        '<main><h1>Actual release</h1></main><script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(data)
        + "</script>"
    ).encode()


def test_reported_empty_contentful_branch_and_opaque_text_are_not_missing_fields() -> None:
    parsed = parsers.article(document({"nodeType": "paragraph", "content": []}), source_id="S13")
    assert parsed.text == "Actual first block."
    opaque = '{"nodeType":"paragraph","content":null}'
    raw = document({"nodeType": "paragraph", "content": [{"nodeType": "text", "value": opaque}]})
    assert parsers.article(raw, source_id="S13").text == "Actual first block. " + opaque
    assert parsers.article(document(None, empty=True), source_id="S13").text == ""


@pytest.mark.parametrize(
    "fault",
    [
        "MISSING_CONTENT",
        "NULL_CONTENT",
        "BOOLEAN_CONTENT",
        "MISSING_TYPE",
        "MISSING_VALUE",
        "BOOLEAN_VALUE",
        "EMPTY",
    ],
)
async def test_malformed_contentful_block_cannot_commit_a_partial_successful_article(
    repo: StorageBackend, tmp_path: Path, fault: Fault
) -> None:
    faults: dict[Fault, JsonValue] = {
        "MISSING_CONTENT": {"nodeType": "paragraph"},
        "NULL_CONTENT": {"nodeType": "paragraph", "content": None},
        "BOOLEAN_CONTENT": {"nodeType": "paragraph", "content": False},
        "MISSING_TYPE": {"content": []},
        "MISSING_VALUE": {"nodeType": "paragraph", "content": [{"nodeType": "text"}]},
        "BOOLEAN_VALUE": {"nodeType": "paragraph", "content": [{"nodeType": "text", "value": False}]},
        "EMPTY": None,
    }
    payload = document(faults[fault], empty=fault == "EMPTY")
    failed_payload = payload
    index = b'<a href="/release">Actual release</a>'
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if str(request.url) == "https://source.example/index":
            return httpx.Response(200, content=index, headers={"etag": '"actual-index"'})
        assert str(request.url) == "https://source.example/release"
        return httpx.Response(200, content=payload)

    source = source_definition(
        {
            "source_id": "S13",
            "name": "Authored rich text",
            "feeds": [
                {"url": "https://source.example/index", "kind": "html", "article_pattern": "^/release$"},
            ],
        }
    )
    settings = ContentfulSettings(data_dir=tmp_path, ops_progress_enabled=False)
    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            first = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            first.sources = {source.source_id: source}
            try:
                await first.poller.acquire(source, 0)
                before = await repo.get("ops.source_checkpoints", "S13:0")
                assert before is not None
                pending_before = before["pending_batches"]
                failed = await first.poller.drain(source, 0, force=True)
                assert failed.items == 0 and failed.errors
            finally:
                await first.close()
            stored_failure = await repo.get("ops.source_checkpoints", "S13:0")
            assert stored_failure is not None and stored_failure["pending_batches"] == pending_before
            errors = stored_failure["article_errors"]
            assert isinstance(errors, dict)
            receipt = errors["https://source.example/release"]
            assert isinstance(receipt, dict)
            assert receipt["http_status"] == 200 and receipt["payload_sha256"] == sha256(failed_payload)
            assert receipt["reason"] == (
                "EMPTY_ARTICLE_BODY" if fault == "EMPTY" else "INVALID_CONTENTFUL_STRUCTURE"
            )
            assert "etag" not in stored_failure
            assert await repo.list("ingest.source_items") == [] and await repo.list("ops.jobs") == []
            assert list(tmp_path.rglob("*.zst")) == []
            payload = document(
                {"nodeType": "paragraph", "content": [{"nodeType": "text", "value": "Actual second block."}]}
            )
            restarted = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            restarted.sources = {source.source_id: source}
            try:
                completed = await restarted.poller.drain(source, 0, force=True)
                assert completed.items == 1 and completed.errors == []
                assert (await restarted.poller.drain(source, 0, force=True)).items == 0
            finally:
                await restarted.close()
        assert calls == [
            "https://source.example/index",
            "https://source.example/release",
            "https://source.example/release",
        ]
        [item] = await repo.list("ingest.source_items")
        assert item["text"] == "Actual first block. Actual second block."
        assert item["raw_sha256"] == sha256(payload)
        reference = item["raw_ref"]
        assert isinstance(reference, str)
        assert zstandard.ZstdDecompressor().decompress(Path(reference).read_bytes()) == payload
        final = await repo.get("ops.source_checkpoints", "S13:0")
        assert final is not None and final["pending_batches"] == [] and final["article_errors"] == {}
        assert final["etag"] == '"actual-index"'
    finally:
        await budgets.shutdown()
