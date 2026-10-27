import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import httpx
import pytest
import zstandard
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_storage import repo as repo

from tank.contracts.source_items import PendingSourceItem, discovered_item, pending_source_item
from tank.settings import Settings
from tank.sources import SourceService, parsers
from tank.storage import StorageBackend
from tank.time_precision import interval_bounds
from tank.util import sha256

PAGE = "https://publisher.example/releases/current?edition=one"


class ArticleSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def page(structured: str) -> bytes:
    return (
        "<html><body><main><h1>Actual main release</h1><p>Actual authored body.</p></main>"
        '<script type="application/ld+json">' + structured + "</script></body></html>"
    ).encode()


@pytest.mark.parametrize(
    "identity",
    [
        {},
        {"url": "https://publisher.example/releases/foreign"},
        {"mainEntityOfPage": {"@id": "https://publisher.example/releases/foreign"}},
        {"@id": "https://publisher.example/releases/foreign#article"},
        {"@id": PAGE + "#article"},
        {"url": "https://publisher.example/releases/current?edition=two"},
    ],
)
def test_unowned_structured_article_cannot_supply_publication(identity: dict[str, JsonValue]) -> None:
    payload = page(
        json.dumps(
            {
                "@type": "NewsArticle",
                "headline": "Unrelated release",
                "datePublished": "2026-10-01T12:00:00Z",
                **identity,
            }
        )
    )
    result = parsers.article(payload, page_url=PAGE)
    assert result.publication.value is None
    assert result.title == "Actual main release"
    assert "Actual authored body." in result.text


@pytest.mark.parametrize("field", ["url", "mainEntityOfPage"])
def test_actual_page_identity_allows_reported_headline_variation(field: str) -> None:
    result = parsers.article(
        page(
            json.dumps(
                {
                    "@type": "NewsArticle",
                    "headline": "Actual main release — publisher display variant",
                    "datePublished": "2026-10-01T12:00:00Z",
                    field: PAGE + "#article",
                }
            )
        ),
        page_url=PAGE,
    )
    assert result.publication.value == "2026-10-01T12:00:00Z"
    assert result.publication_basis == "ARTICLE_JSON_LD_DATE_PUBLISHED"


def test_matching_title_does_not_override_known_foreign_page() -> None:
    result = parsers.article(
        page(
            json.dumps(
                {
                    "@type": "NewsArticle",
                    "headline": "Actual main release",
                    "url": "https://publisher.example/releases/foreign",
                    "datePublished": "2026-10-01T12:00:00Z",
                }
            )
        ),
        page_url=PAGE,
    )
    assert result.publication.value is None


def test_ambiguous_json_ld_cannot_author_clock_but_unique_headline_can() -> None:
    unique = '{"@type":"NewsArticle","headline":"Actual main release","datePublished":"2026-10-01T12:00:00Z"}'
    assert parsers.article(page(unique), page_url=PAGE).publication.value == "2026-10-01T12:00:00Z"
    ambiguous = unique.replace(
        '"Actual main release"', '"Unrelated release","headline":"Actual main release"'
    )
    assert parsers.article(page(ambiguous), page_url=PAGE).publication.value is None


def test_discovery_publication_projection_preserves_absent_and_explicitly_unknown_bucket_edge() -> None:
    wire: dict[str, JsonValue] = {
        "external_id": "authored-day",
        "url": "https://publisher.example/authored-day",
        "title": "Authored calendar-day observation",
        "text": "Actual authored source content",
        **parsers.timestamp_fields("2026-10-01"),
    }
    material = discovered_item("S03", wire).discovery
    expected = datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 2, tzinfo=UTC)
    assert interval_bounds(material.publication) == expected
    assert material.published_at_precision == "BUCKET(1d)"
    pending = PendingSourceItem(
        discovery=material,
        item_id="authored-day-item",
        feed="https://publisher.example/feed",
        observed_at="2026-10-02T12:00:00Z",
        discovery_config_version="authored-config",
        discovery_code_version="authored-code",
        raw=None,
    )
    stored = pending.storage_record()
    assert "published_at_bucket_anchor" not in stored
    restored = pending_source_item("S03", stored)
    assert isinstance(restored, PendingSourceItem)
    assert restored.storage_record() == stored
    assert interval_bounds(restored.discovery.publication) == expected
    explicitly_unknown = discovered_item("S03", {**wire, "published_at_bucket_anchor": None}).discovery
    assert explicitly_unknown.publication.bucket_anchor == "UNSPECIFIED"
    assert interval_bounds(explicitly_unknown.publication) == (None, None)


@pytest.mark.parametrize("ownership", ["SAME_PAGE", "FOREIGN", "DUPLICATE"])
async def test_pending_article_restart_retains_scope_and_admits_only_its_own_publication(
    repo: StorageBackend,
    tmp_path: Path,
    ownership: Literal["SAME_PAGE", "FOREIGN", "DUPLICATE"],
) -> None:
    structured = json.dumps(
        {
            "@type": "NewsArticle",
            "headline": "Actual main release — alternate publisher display title",
            "url": PAGE if ownership == "SAME_PAGE" else "https://publisher.example/foreign",
            "datePublished": "2026-10-01T12:00:00Z",
        }
    )
    if ownership == "DUPLICATE":
        structured = structured.replace(
            '"url": "https://publisher.example/foreign"',
            '"url": "https://publisher.example/foreign", "url": ' + json.dumps(PAGE),
        )
    body = page(structured).replace(b"Actual authored body.", b"Bitcoin authored main-page facts. " * 30)
    index = b'<a href="/releases/current?edition=one">Actual main release</a>'
    calls: list[str] = []
    detail_calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal detail_calls
        calls.append(str(request.url))
        if str(request.url) == "https://publisher.example/index":
            return httpx.Response(200, content=index, headers={"etag": '"actual-index"'})
        assert str(request.url) == PAGE
        detail_calls += 1
        return (
            httpx.Response(404, content=b"actual initially unavailable article")
            if detail_calls == 1
            else httpx.Response(200, content=body)
        )

    source = source_definition(
        {
            "source_id": "S30",
            "name": "Authored article source",
            "feeds": [
                {
                    "url": "https://publisher.example/index",
                    "kind": "html",
                    "article_pattern": "^/releases/current$",
                }
            ],
        }
    )
    budgets = fixture_request_budgets()
    settings = ArticleSettings(data_dir=tmp_path / "data", ops_progress_enabled=False)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            first = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            first.sources = {source.source_id: source}
            try:
                await first.poller.acquire(source, 0)
                acquired = await repo.get("ops.source_checkpoints", "S30:0")
                assert acquired is not None
                batches = acquired["pending_batches"]
                assert isinstance(batches, list) and len(batches) == 1
                batch = batches[0]
                assert isinstance(batch, dict)
                pending_rows = batch["rows"]
                assert isinstance(pending_rows, list) and len(pending_rows) == 1
                pending = pending_rows[0]
                assert isinstance(pending, dict)
                assert pending["pending_phase"] == "ARTICLE_LINK"
                assert pending["url"] == PAGE
                assert "text" not in pending and "published_at" not in pending and "raw_ref" not in pending
                original = json.dumps(pending, sort_keys=True)
                failed = await first.poller.drain(source, 0, force=True)
                assert failed.items == 0 and failed.errors
                assert await repo.list("ingest.source_items") == []
            finally:
                await first.close()
            restarted = SourceService(repo, settings, http=http, budgets=budgets, versions=fixture_versions())
            restarted.sources = {source.source_id: source}
            try:
                checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
                assert checkpoint is not None
                assert checkpoint.get("etag") != '"actual-index"'
                decoded = await restarted.poller.pending(source, "S30:0", checkpoint)
                assert len(decoded.batches) == 1
                restored = decoded.batches[0].storage_record()["rows"]
                assert isinstance(restored, list) and json.dumps(restored[0], sort_keys=True) == original
                completed = await restarted.poller.drain(source, 0, force=True)
                assert completed.items == 1 and completed.errors == []
                assert (await restarted.poller.drain(source, 0, force=True)).items == 0
            finally:
                await restarted.close()
        assert calls == ["https://publisher.example/index", PAGE, PAGE]
        records = await repo.list("ingest.source_items")
        assert len(records) == 1
        record = records[0]
        assert record["url"] == PAGE and record["observed_at"] == pending["observed_at"]
        assert record["published_at"] == ("2026-10-01T12:00:00Z" if ownership == "SAME_PAGE" else None)
        assert record["raw_sha256"] == sha256(body)
        raw_ref = record["raw_ref"]
        assert isinstance(raw_ref, str)
        assert zstandard.ZstdDecompressor().decompress(Path(raw_ref).read_bytes()) == body
        final = await repo.get("ops.source_checkpoints", "S30:0")
        assert final is not None and final["pending_batches"] == [] and final["etag"] == '"actual-index"'
    finally:
        await budgets.shutdown()


async def test_link_only_filing_cannot_bypass_the_required_reported_text_filter(
    repo: StorageBackend, tmp_path: Path
) -> None:
    index = b'<a href="/filing">Actual filing link without reported filing text</a>'
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        assert str(request.url) == "https://source.example/index"
        return httpx.Response(200, content=index, headers={"etag": '"unapplied-index"'})

    source = source_definition(
        {
            "source_id": "S02",
            "name": "Authored filing source",
            "feeds": [
                {"url": "https://source.example/index", "kind": "html", "article_pattern": "^/filing$"}
            ],
        }
    )
    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            service = SourceService(
                repo,
                ArticleSettings(data_dir=tmp_path / "data", ops_progress_enabled=False),
                http=http,
                budgets=budgets,
                versions=fixture_versions(),
            )
            service.sources = {source.source_id: source}
            try:
                outcome = await service.poll_once(source.source_id)
            finally:
                await service.close()
        assert outcome.status == "DEGRADED" and outcome.items == 0
        assert any("requires the reported filing text" in reason for reason in outcome.errors)
        assert calls == ["https://source.example/index"]
        assert await repo.list("ingest.source_items") == [] and await repo.list("ops.jobs") == []
        checkpoint = await repo.get("ops.source_checkpoints", "S02:0")
        assert checkpoint is not None and checkpoint["payload_sha256"] == sha256(index)
        assert "etag" not in checkpoint and not checkpoint.get("pending_batches")
        assert list((tmp_path / "data").rglob("*.zst")) == []
    finally:
        await budgets.shutdown()
