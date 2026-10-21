"""Actual retained DTCC authored blocks, excluding contact and related-news cards."""

import hashlib
import json
from pathlib import Path

import httpx
import pytest
import zstandard
from selectolax.parser import HTMLParser

from tank.settings import Settings
from tank.sources import SourceService, parsers
from tank.storage import MemoryRepository

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/calibration/supplementary-sweep-20261019-run1/raw-items.new.jsonl"


def retained_raw(ref):
    """The sweep recorded absolute paths of the machine it ran on; files live under RAW's directory."""
    return RAW.parent / ref.split("supplementary-sweep-20261019-run1/", 1)[1]


def retained():
    if not RAW.exists():
        pytest.skip("Owned actual supplementary capture absent")
    assert (
        hashlib.sha256(RAW.read_bytes()).hexdigest()
        == "9ca7e6b7a606b587eada5b2ad8faa9c861a114afa78ee0e4671d912bb59cffca"
    )
    result = []
    for index, line in enumerate(RAW.read_text().splitlines()):
        row = json.loads(line)
        if row["source_id"] != "S08":
            continue
        payload = zstandard.ZstdDecompressor().decompress(retained_raw(row["raw_ref"]).read_bytes())
        assert hashlib.sha256(payload).hexdigest() == row["raw_sha256"]
        result.append((index, row, payload))
    assert len(result) == 142
    return result


@pytest.mark.historical
@pytest.mark.integration
def test_all_actual_dtcc_pages_use_ordered_authored_blocks_without_rewriting_capture():
    for index, row, payload in retained():
        original = json.dumps(row, sort_keys=True)
        parsed = parsers.article(payload, source_id="S08")
        tree = HTMLParser(payload)
        blocks = tree.css("main .content-item--richtextblock .rtf")
        expected = parsers.text("<div>" + "\n".join(node.html for node in blocks) + "</div>")
        assert parsed["text"] == expected and len(expected) >= 1600
        assert parsed["title"] == row["title"]
        assert parsed["published_at_precision"] == "BUCKET(1d)"
        assert parsed["published_at_basis"] == "DTCC_CALENDAR_MASTHEAD"
        assert (
            parsed["published_at"][:10]
            == __import__("datetime")
            .datetime.strptime(
                tree.css_first("main .page-detail-masthead__meta .paragraph-small").text(strip=True),
                "%b %d, %Y",
            )
            .date()
            .isoformat()
        )
        assert json.dumps(row, sort_keys=True) == original
        if index == 932:
            assert "First tokenization company" in expected and "Tonie Wells" not in expected
        if index == 1067:
            assert "confirmation of four" in expected and "Samir Pandiri" not in expected


@pytest.mark.parametrize(
    "body",
    [
        "",
        '<div class="content-item--richtextblock"><div class="rtf"></div></div>',
        '<div class="content-item--richtextblock"><div class="rtf">outer<div class="rtf">inner</div></div></div>',
    ],
)
def test_dtcc_missing_empty_nested_authored_nodes_refuse_without_nav_fallback(body):
    html = "<main><article>Unrelated sidebar</article>" + body + "</main>"
    with pytest.raises(ValueError, match="DTCC release"):
        parsers.article(html, source_id="S08")
    assert "Unrelated sidebar" in parsers.article(html, source_id="S27")["text"]


@pytest.mark.historical
@pytest.mark.integration
async def test_normal_dtcc_poll_uses_actual_retained_release_bodies(tmp_path):
    fixtures = [entry for entry in retained() if entry[0] in {932, 1067}]
    pages = {row["url"]: payload for _, row, payload in fixtures}
    index_url = "https://www.dtcc.com/press-releases"
    index_html = "".join('<a href="' + url + '">Official release</a>' for url in pages)
    source = {
        "source_id": "S08",
        "name": "DTCC",
        "author_organization": "DTCC",
        "feeds": [
            {
                "url": index_url,
                "kind": "html",
                "article_pattern": r"^/press-releases/\d{4}/[A-Za-z0-9][A-Za-z0-9._~-]*$",
            }
        ],
    }

    def transport(request):
        if str(request.url) == index_url:
            return httpx.Response(200, text=index_html)
        assert request.method == "GET" and not request.headers.get("Authorization")
        return httpx.Response(200, content=pages[str(request.url)])

    settings = Settings(data_dir=tmp_path, x_enabled=False, user_agent="Tank offline regression")
    object.__setattr__(
        settings,
        "load_config",
        lambda name: {"sources": [source]} if name == "sources" else Settings.load_config(settings, name),
    )
    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        service = SourceService(repo, settings, http=http)
        try:
            outcome = await service.poll_once("S08")
            assert outcome["status"] == "HEALTHY" and outcome["items"] == 2, outcome
            rows = await repo.list("ingest.source_items")
            for _, original, payload in fixtures:
                row = next(x for x in rows if x["external_id"] == original["external_id"])
                assert row["text"] == parsers.article(payload, source_id="S08")["text"]
                assert row["raw_sha256"] == original["raw_sha256"]
                assert row["published_at"] == parsers.article(payload, source_id="S08")["published_at"]
                assert row["published_at_precision"] == "BUCKET(1d)"
                assert row["observed_at"] != original["observed_at"]
            jobs, items = await repo.list("ops.jobs"), await repo.list("ingest.source_items")
            # Issuer and market-structure items go straight to routing (03 §3.1).
            assert {j["idem_key"] for j in jobs} == {x["item_id"] for x in items}
            assert {j["type"] for j in jobs} == {"ROUTE_ITEM"}
        finally:
            await service.close()
