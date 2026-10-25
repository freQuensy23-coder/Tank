import json
from pathlib import Path

import httpx
import pytest
import zstandard
from source_support import fixture_request_budgets, fixture_versions, source_definition, source_feed
from test_sources import Repo, settings

from tank.sources import SourceService, parsers
from tank.util import sha256

FIXTURES = Path(__file__).parent / "fixtures/s13-cms-recovery"
FEED = {
    "url": "https://robinhood.com/newsroom/",
    "article_pattern": r"^/(?:us/en/)?newsroom/(?!category/)[A-Za-z0-9-]+/?$",
    "embedded_json_fields": ["canonicalUrl"],
}
OLD = "https://robinhood.com/us/en/newsroom/introducingukcryptotrading"
NEW = "https://robinhood.com/newsroom/robinhood-launches-crypto-trading-for-uk-investors/"


def retained(name):
    return zstandard.ZstdDecompressor().decompress((FIXTURES / (name + ".html.zst")).read_bytes())


def original():
    row = next(r for r in parsers.article_links(retained("index"), FEED) if r["url"] == OLD)
    return dict(row, item_id="item-s13-recovery")


def test_retained_cms_entry_declares_slug_without_replacing_canonical_identity() -> None:
    row = original()
    assert row["publisher_cms_identity"]["entry_id"] == "59JVkSSAyZoVoQP8zH3Wkn"
    assert row["publisher_cms_slug_url"] == NEW
    assert row["url"] == row["external_id"] == OLD
    assert row["publisher_cms_identity"] in parsers.publisher_cms_entries(retained("detail"))


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", [None, "entry_id", "title", "content"])
async def test_retained_canonical_404_recovers_only_exact_same_cms_entry(tmp_path: Path, mismatch) -> None:
    detail = retained("detail")
    if mismatch:
        from selectolax.parser import HTMLParser

        tree = HTMLParser(detail)
        script = tree.css_first("script#__NEXT_DATA__")
        data = json.loads(script.text())

        def change(x) -> None:
            if isinstance(x, dict):
                if x.get("sys", {}).get("id") == "59JVkSSAyZoVoQP8zH3Wkn":
                    if mismatch == "entry_id":
                        x["sys"]["id"] = "different-entry"
                    elif mismatch == "title":
                        x["fields"]["title"] = "Different article"
                    else:
                        x["fields"]["content"] = {"nodeType": "document", "content": []}
                for v in x.values():
                    change(v)
            elif isinstance(x, list):
                for v in x:
                    change(v)

        change(data)
        script.replace_with(
            '<script id="__NEXT_DATA__" type="application/json">' + json.dumps(data) + "</script>"
        )
        detail = tree.html.encode()
    calls = []

    def handle(request: httpx.Request):
        calls.append(str(request.url))
        if str(request.url) == OLD:
            return httpx.Response(404, content=retained("not-found"))
        assert str(request.url) == NEW
        return httpx.Response(200, content=detail)

    source = {"source_id": "S13", "name": "Robinhood", "author_organization": "Robinhood", "feeds": [FEED]}
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(
            repo,
            settings(tmp_path, [source]),
            http=http,
            versions=fixture_versions(),
            budgets=fixture_request_budgets(),
        )
        row = original()
        row["feed"] = FEED["url"]
        row["discovery_config_version"] = "test"
        row["discovery_code_version"] = "test"
        row["observed_at"] = "2026-10-19T07:59:02.218121Z"
        batch = {"kind": "html"}
        if mismatch:
            with pytest.raises(ValueError, match="identity/content mismatch"):
                await svc.poller._item(source_definition(source), source_feed(FEED), "S13:0", batch, row)
            assert not any(c == "ingest.source_items" for c, _ in repo.rows)
        else:
            await svc.poller._item(source_definition(source), source_feed(FEED), "S13:0", batch, row)
            item = next(v for (c, _), v in repo.rows.items() if c == "ingest.source_items")
            assert item["external_id"] == OLD
            assert item["url"] == NEW
            assert item["publisher_url_aliases"] == [OLD, NEW]
            assert item["observed_at"] == row["observed_at"]
            recovery = item["publisher_cms_recovery"]
            assert recovery["status"] == "VERIFIED_SAME_CMS_ENTRY"
            assert recovery["original_payload_sha256"] == sha256(retained("not-found"))
            assert recovery["original_http_status"] == 404 and recovery["canonical_url"] == OLD
            assert item["raw_sha256"] == sha256(detail) and Path(item["raw_ref"]).exists()
    assert calls == [OLD, NEW]


@pytest.mark.parametrize("slug", ["../escape", "https://evil.org/article", "/absolute", "with?query"])
def test_untrusted_slug_never_becomes_fallback(slug) -> None:
    payload = {
        "sys": {"id": "one"},
        "fields": {"slug": slug, "title": "Article", "content": {"nodeType": "document"}},
    }
    html = '<script id="__NEXT_DATA__" type="application/json">' + json.dumps(payload) + "</script>"
    assert not parsers.publisher_cms_entries(html)


@pytest.mark.asyncio
async def test_cms_fallback_is_not_used_for_non404_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    import tank.sources.fetch as source_module

    calls, retry_delays = [], []

    async def record_retry(delay: float) -> None:
        retry_delays.append(delay)

    monkeypatch.setattr(source_module, "asyncio", SimpleNamespace(sleep=record_retry))

    def handle(request: httpx.Request):
        calls.append(str(request.url))
        return httpx.Response(403, text="Forbidden")

    source = {"source_id": "S13", "name": "Robinhood", "feeds": [FEED]}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(
            Repo(),
            settings(tmp_path, [source]),
            http=http,
            versions=fixture_versions(),
            budgets=fixture_request_budgets(),
        )
        row = original()
        row["feed"] = FEED["url"]
        row["discovery_config_version"] = "test"
        row["discovery_code_version"] = "test"
        row["observed_at"] = "2026-10-19T07:59:02.218121Z"
        with pytest.raises(httpx.HTTPStatusError) as error:
            await svc.poller._item(
                source_definition(source), source_feed(FEED), "S13:0", {"kind": "html"}, row
            )
        assert error.value.response.status_code == 403
    assert calls == [OLD] * 4
    assert retry_delays == [2, 4, 8]
    assert NEW not in calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fallback",
    ["https://evil.org/newsroom/article/", NEW + "?other=1", "https://robinhood.com/newsroom/other/"],
)
async def test_checkpoint_fallback_must_match_same_host_and_declared_slug(tmp_path: Path, fallback) -> None:
    calls = []

    def handle(request: httpx.Request):
        calls.append(str(request.url))
        return httpx.Response(404, content=retained("not-found"))

    source = {"source_id": "S13", "name": "Robinhood", "feeds": [FEED]}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(
            Repo(),
            settings(tmp_path, [source]),
            http=http,
            versions=fixture_versions(),
            budgets=fixture_request_budgets(),
        )
        row = original()
        row["feed"] = FEED["url"]
        row["discovery_config_version"] = "test"
        row["discovery_code_version"] = "test"
        row.update(observed_at="2026-10-19T07:59:02.218121Z", publisher_cms_slug_url=fallback)
        with pytest.raises(httpx.HTTPStatusError):
            await svc.poller._item(
                source_definition(source), source_feed(FEED), "S13:0", {"kind": "html"}, row
            )
    assert calls == [OLD]
