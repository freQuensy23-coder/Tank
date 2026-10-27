import json
from pathlib import Path

import httpx
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_storage import repo as repo

from tank.settings import Settings
from tank.sources import SourceService, parsers
from tank.storage import StorageBackend
from tank.util import sha256


class ParserSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def test_press_release_without_reported_reference_date_keeps_publication_unknown() -> None:
    payload = json.dumps(
        {
            "results": {
                "response": {
                    "hits": {
                        "hits": [
                            {
                                "_source": {
                                    "url": "https://publisher.example/release",
                                    "title": "Actual release",
                                    "contentString": "Actual source release body",
                                }
                            }
                        ]
                    }
                }
            }
        }
    ).encode()
    parsed = parsers.json_items(payload, "S19")
    assert parsed.errors == [] and len(parsed.discoveries) == 1
    [entry] = parsed.discoveries
    assert entry.discovery.external_id == "https://publisher.example/release"
    assert entry.discovery.text == "Actual source release body"
    assert entry.discovery.published_at is None
    assert entry.discovery.published_at_precision == "COLLECTION_ONLY"
    assert entry.discovery.publication.precision_reason == "INCOMPLETE"


async def test_ambiguous_cms_index_cannot_create_work_or_advance_coverage(
    repo: StorageBackend, tmp_path: Path
) -> None:
    entry = {
        "sys": {"id": "actual-entry"},
        "fields": {
            "slug": "release",
            "title": "Actual release",
            "canonicalUrl": "https://publisher.example/news/release",
            "content": {"nodeType": "document", "content": [{"nodeType": "text", "value": "Actual body"}]},
            "opaque_text": '{"id":"first","id":"last"}',
        },
    }
    encoded = json.dumps(entry)
    ambiguous = encoded.replace('"id": "actual-entry"', '"id": "synthetic-first", "id": "actual-entry"')

    def html(document: str) -> bytes:
        return (
            '<a href="/news/release">Actual release</a><script id="__NEXT_DATA__" type="application/json">'
            + document
            + "</script>"
        ).encode()

    payload = html(ambiguous)
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        assert str(request.url) == "https://publisher.example/news"
        return httpx.Response(200, content=payload, headers={"etag": '"actual-index"'})

    source = source_definition(
        {
            "source_id": "S13",
            "name": "Authored CMS source",
            "feeds": [
                {
                    "url": "https://publisher.example/news",
                    "kind": "html",
                    "article_pattern": "^/news/release$",
                    "embedded_json_fields": ["canonicalUrl"],
                }
            ],
        }
    )
    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            service = SourceService(
                repo,
                ParserSettings(data_dir=tmp_path / "data", ops_progress_enabled=False),
                http=http,
                budgets=budgets,
                versions=fixture_versions(),
            )
            service.sources = {source.source_id: source}
            try:
                refused = await service.poller.acquire(source, 0)
                assert refused.items == 0 and refused.errors
                checkpoint = await repo.get("ops.source_checkpoints", "S13:0")
                assert checkpoint is not None and checkpoint["payload_sha256"] == sha256(payload)
                assert not checkpoint.get("pending_batches") and "etag" not in checkpoint
                assert "synthetic-first" not in json.dumps(checkpoint)
                assert await repo.list("ingest.source_items") == [] and await repo.list("ops.jobs") == []
                assert list((tmp_path / "data").rglob("*.zst")) == []
                payload = html(encoded)
                accepted = await service.poller.acquire(source, 0)
                assert accepted.errors == []
                valid = await repo.get("ops.source_checkpoints", "S13:0")
                assert valid is not None and valid["payload_sha256"] == sha256(payload)
                pending = await service.poller.pending(source, "S13:0", valid)
                assert pending.pending_external_ids() == {"https://publisher.example/news/release"}
                assert "etag" not in valid
            finally:
                await service.close()
        assert calls == ["https://publisher.example/news", "https://publisher.example/news"]
    finally:
        await budgets.shutdown()
