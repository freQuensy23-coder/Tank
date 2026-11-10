import hashlib
import json
from pathlib import Path

import httpx
import pytest
import zstandard
from selectolax.parser import HTMLParser
from test_sources import Repo, settings

from tank.settings import Settings
from tank.sources import SourceService, parsers

ROOT = Path(__file__).parent / "fixtures/s22-paxos-newsroom"
RECEIPT = json.loads((ROOT / "receipt.json").read_text())
SOURCE = next(s for s in Settings().load_config("sources")["sources"] if s["source_id"] == "S22")
FEED = SOURCE["feeds"][0]
RELEASES = {
    "sec-clearing-agency-registration": (
        "Paxos Securities Settlement Company Receives Clearing Agency Registration"
        " from the U.S. Securities and Exchange Commission",
        "2026-05-28T00:00:00Z",
        "Paxos becomes the first & only blockchain-native firm registered",
        "Contact: press@paxos.com",
    ),
    "schwab-spot-crypto-trading": (
        "Charles Schwab Announces Details of Spot Crypto Trading Launch",
        "2026-04-16T00:00:00Z",
        "This press release was originally issued by Charles Schwab",
        "and is available here.",
    ),
    "solana-developer-platform": (
        "Paxos Joins Solana Developer Platform",
        "2026-03-24T00:00:00Z",
        "Enterprises and financial institutions are moving fast",
        "contact Paxos to learn more.",
    ),
    "occ-trust-conversion": (
        "OCC Approves Paxos Application to Convert to OCC Trust; Paxos to Complete Conversion Imminently"
        " to Become a Federally Regulated Blockchain Infrastructure Provider",
        "2025-12-12T00:00:00Z",
        "Update: Paxos officially converted its New York Department of Financial Services",
        "Founders Fund and PayPal Ventures.",
    ),
}


def retained(name):
    payload = zstandard.ZstdDecompressor().decompress((ROOT / (name + ".html.zst")).read_bytes())
    assert hashlib.sha256(payload).hexdigest() == RECEIPT[name]["sha256"]
    return payload


def test_newsroom_listing_yields_each_release_once_with_its_title():
    assert FEED["url"] == RECEIPT["newsroom"]["url"] == RECEIPT["newsroom"]["response_url"]
    rows = parsers.article_links(retained("newsroom"), FEED)
    assert [(row["url"], row["title"]) for row in rows] == [
        (RECEIPT[name]["url"], title) for name, (title, *_) in RELEASES.items()
    ]
    assert all(row["external_id"] == row["url"] for row in rows)
    assert all(row["url"].startswith("https://www.paxos.com/blog/") for row in rows)


def test_newsroom_listing_no_longer_links_the_previous_article_path():
    with pytest.raises(ValueError, match="No article links matched"):
        parsers.article_links(retained("newsroom"), dict(FEED, article_pattern=r"^/newsroom/[a-z0-9-]+$"))


@pytest.mark.parametrize("name", RELEASES)
def test_release_page_gives_title_publication_time_and_only_the_authored_body(name):
    title, published_at, opening, closing = RELEASES[name]
    row = parsers.article(retained(name), source_id="S22")
    assert row["title"] == title
    assert row["published_at"] == published_at
    assert row["published_prec"] == "EXACT"
    assert row["published_at_basis"] == "DESIGNATED_PUBLICATION_TIME_ELEMENT"
    assert row["text"].startswith(opening) and row["text"].endswith(closing)
    assert "Related articles" not in row["text"] and "All articles" not in row["text"]
    for other, (other_title, *_) in RELEASES.items():
        assert other == name or other_title not in row["text"]


@pytest.mark.parametrize("change", ["missing", "ambiguous"])
def test_release_page_without_one_authored_body_never_falls_back_to_the_whole_page(change):
    tree = HTMLParser(retained("solana-developer-platform"))
    node = tree.css_first('[data-framer-name="Article body"]')
    if change == "missing":
        node.decompose()
    else:
        tree = HTMLParser(tree.html.replace(node.html, node.html + node.html))
    with pytest.raises(ValueError, match="Paxos article"):
        parsers.article(tree.html, source_id="S22")


@pytest.mark.asyncio
async def test_poll_reads_every_listed_release_from_the_captured_site(tmp_path):
    pages = {entry["url"]: name for name, entry in RECEIPT.items()}
    calls = []

    def handle(request):
        calls.append(str(request.url))
        return httpx.Response(200, content=retained(pages[str(request.url)]))

    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, [SOURCE]), http=http)
        result = await svc.poll_once("S22")
        assert result["status"] == "HEALTHY" and result["items"] == len(RELEASES)
        items = {r["url"]: r for (table, _), r in repo.rows.items() if table == "ingest.source_items"}
        assert set(items) == {RECEIPT[name]["url"] for name in RELEASES}
        for name, (title, published_at, opening, _) in RELEASES.items():
            item = items[RECEIPT[name]["url"]]
            assert item["source_id"] == "S22" and item["external_id"] == item["url"]
            assert item["title"] == title and item["published_at"] == published_at
            assert item["text"].startswith(opening)
        state = await repo.get("ops.source_checkpoints", "S22:0")
        assert state["status"] == "HEALTHY" and not state["pending_batches"]
        assert len(repo.jobs) == len(RELEASES)
        assert (await svc.poll_once("S22"))["items"] == 0
        await svc.close()
    assert sorted(set(calls)) == sorted(pages)
