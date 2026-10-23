"""Retained Uniswap authored bodies must exclude current related-post cards."""

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
        pytest.fail(
            "Required supplementary captures missing; run python tests/fixtures/supplementary-sources/stage.py"
        )
    assert hashlib.sha256(RAW.read_bytes()).hexdigest() == (
        "9ca7e6b7a606b587eada5b2ad8faa9c861a114afa78ee0e4671d912bb59cffca"
    )
    result = []
    for index, line in enumerate(RAW.read_text().splitlines()):
        row = json.loads(line)
        if row["source_id"] != "S15":
            continue
        payload = zstandard.ZstdDecompressor().decompress(retained_raw(row["raw_ref"]).read_bytes())
        assert hashlib.sha256(payload).hexdigest() == row["raw_sha256"]
        result.append((index, row, payload))
    assert len(result) == 18
    return result


@pytest.mark.historical
@pytest.mark.integration
def test_all_seventeen_retained_uniswap_articles_preserve_authored_facts_and_clocks():
    count = 0
    canary = "SYNTHETIC_FUTURE_RELATED_CARD_NOT_AN_AUTHORED_CLAIM"
    for index, row, payload in retained():
        original = json.dumps(row, sort_keys=True)
        tree = HTMLParser(payload)
        if index == 207:
            assert not tree.css("main .ArticleContent")
            with pytest.raises(ValueError, match="Uniswap article"):
                parsers.article(payload, source_id="S15")
            continue
        count += 1
        nodes = tree.css("main .ArticleContent")
        assert len(nodes) == 1
        expected = parsers.text(nodes[0].html)
        parsed = parsers.article(payload, source_id="S15")
        assert parsed["text"] == expected
        assert parsed["title"] == row["title"]
        ordinary = parsers.article(payload, source_id="S27")
        for key in ("published_at", "published_at_precision", "published_at_basis"):
            assert parsed.get(key) == ordinary.get(key)
        changed = payload.decode().replace("</main>", f"<aside><p>{canary}</p></aside></main>", 1)
        assert parsers.article(changed, source_id="S15")["text"] == expected
        assert canary in parsers.article(changed, source_id="S27")["text"]
        assert json.dumps(row, sort_keys=True) == original
        if index == 206:
            assert "Governance holds upgrade and configuration rights after launch." in expected
            assert "USDC/USDG and USDC/USDT" in expected
            assert "VolatilePair Hook: Pricing Both Sides of the Trade" not in expected
            assert "SMBC Nikko and Nethermind" not in expected
            assert "Uniswap is Live on Arc" not in expected
        if index == 216:
            assert "BUIDL" in expected and "Securitize" in expected
            assert "BlackRock has also made a strategic investment" in expected
            assert "Guidestar Joins Uniswap Labs" not in expected
            assert "A Win for DeFi" not in expected
            assert "Introducing: Uniswap Labs X KidSuper" not in expected
    assert count == 17


@pytest.mark.parametrize(
    "authored",
    [
        "",
        '<div class="ArticleContent"></div>',
        '<div class="ArticleContent"><script>not authored text</script></div>',
        '<div class="ArticleContent">one</div><div class="ArticleContent">two</div>',
        '<div class="ArticleContent">outer<div class="ArticleContent">inner</div></div>',
    ],
)
def test_uniswap_missing_empty_or_ambiguous_body_refuses_without_fallback(authored):
    payload = "<h1>Article</h1><main><article>Unrelated current card</article>" + authored + "</main>"
    with pytest.raises(ValueError, match="Uniswap article"):
        parsers.article(payload, source_id="S15")
    assert "Unrelated current card" in parsers.article(payload, source_id="S27")["text"]


def test_uniswap_discovery_excludes_navigation_but_preserves_real_article_links(tmp_path):
    settings = Settings(data_dir=tmp_path)
    source = next(x for x in settings.load_config("sources")["sources"] if x["source_id"] == "S15")
    feed = source["feeds"][0]
    payload = """<a href="/latest-updates">Latest Updates</a>
    <a href="/category/protocols">Category</a>
    <a href="/keeping-value-in-the-pool-how-stablepair-hook-works">Real article</a>"""
    assert [x["url"] for x in parsers.article_links(payload, feed)] == [
        "https://blog.uniswap.org/keeping-value-in-the-pool-how-stablepair-hook-works"
    ]


@pytest.mark.historical
@pytest.mark.integration
@pytest.mark.parametrize("malformed_sibling", [False, True])
async def test_normal_uniswap_poll_uses_retained_authored_body_and_never_fetches_index(
    tmp_path, malformed_sibling
):
    fixtures = [entry for entry in retained() if entry[0] in {206, 216}]
    pages = {row["url"]: payload for _, row, payload in fixtures}
    settings = Settings(data_dir=tmp_path, x_enabled=False)
    source = next(x for x in settings.load_config("sources")["sources"] if x["source_id"] == "S15")
    feed_url = source["feeds"][0]["url"]
    links = '<a href="/latest-updates">Latest Updates</a>' + "".join(
        f'<a href="{url}">Article</a>' for url in pages
    )
    malformed_url = "https://blog.uniswap.org/missing-authored-node"
    if malformed_sibling:
        links += f'<a href="{malformed_url}">Malformed sibling</a>'
    seen = []

    def transport(request):
        url = str(request.url)
        seen.append(url)
        if url == feed_url:
            return httpx.Response(200, text=links)
        if url == malformed_url:
            return httpx.Response(200, text="<main>Unrelated current sidebar</main>")
        assert url in pages, url
        return httpx.Response(200, content=pages[url])

    object.__setattr__(
        settings,
        "load_config",
        lambda name: {"sources": [source]} if name == "sources" else Settings.load_config(settings, name),
    )
    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        service = SourceService(repo, settings, http=http)
        try:
            result = await service.poll_once("S15")
            assert result["status"] == ("DEGRADED" if malformed_sibling else "HEALTHY"), result
            assert result["items"] == 2, result
            stored = await repo.list("ingest.source_items")
            assert len(stored) == 2
            for _, original, payload in fixtures:
                item = next(x for x in stored if x["url"] == original["url"])
                assert item["text"] == parsers.article(payload, source_id="S15")["text"]
                assert item["raw_sha256"] == original["raw_sha256"]
                assert item["observed_at"] != original["observed_at"]
            assert all("latest-updates" not in url for url in seen)
            jobs, items = await repo.list("ops.jobs"), await repo.list("ingest.source_items")

            assert {j["idem_key"] for j in jobs if j["type"] in {"FILTER_ITEM", "ROUTE_ITEM"}} == {
                x["item_id"] for x in items
            }
            progress = await repo.list("out.ops_notices", filters={"kind": "ITEM_PROGRESS"})
            assert {row["subject_id"] for row in progress} == {x["item_id"] for x in items}
            assert {(row["subject_id"], row["stage"]) for row in progress} == {
                (item["item_id"], stage) for item in items for stage in ("RECEIVED", "JEV_BYPASSED")
            }
            assert len(progress) == 2 * len(items)
            assert {j["type"] for j in jobs} == {"ROUTE_ITEM", "DELIVER"}
        finally:
            await service.close()
