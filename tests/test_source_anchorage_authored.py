"""Anchorage authored-body boundary, including the actual retained twelve pages."""

import hashlib
import io
import json
from html import escape
from pathlib import Path

import httpx
import pytest
import zstandard

from tank.settings import Settings
from tank.sources import SourceService, parsers
from tank.storage import MemoryRepository

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/calibration/supplementary-sweep-20261019-run1/raw-items.new.jsonl"
QUOTES = {
    181: "When selecting crypto assets to invest in",
    182: "Cryptocurrencies have seen amazing growth over the past decade",
    183: "Ethereum’s transition to proof-of-stake",
    184: "For thirty years the answer to the above question was a subscription",
    185: "it’s important to figure out the API design",
    186: "RIAs require a partner that can provide fully integrated products",
    187: "In the world of crypto investing",
    188: "What does an AI-generated answer cost to produce",
    189: "Money is moving onto new rails, built in code",
    190: "Nearly 23 million ETH",
    191: "Crypto markets can be volatile",
    192: "Custody is the most important decision an institution must make",
}


def retained_raw(ref):
    """The sweep recorded absolute paths of the machine it ran on; files live under RAW's directory."""
    return RAW.parent / ref.split("supplementary-sweep-20261019-run1/", 1)[1]


def retained():
    if not RAW.exists():
        pytest.skip("Actual owned supplementary sweep fixtures not available")
    assert (
        hashlib.sha256(RAW.read_bytes()).hexdigest()
        == "9ca7e6b7a606b587eada5b2ad8faa9c861a114afa78ee0e4671d912bb59cffca"
    )
    records = [json.loads(line) for line in RAW.read_text().splitlines()]
    result = []
    for index, quote in QUOTES.items():
        item = records[index]
        assert item["source_id"] == "S28" and item["text"] == "Institution Type"
        with zstandard.ZstdDecompressor().stream_reader(
            io.BytesIO(retained_raw(item["raw_ref"]).read_bytes())
        ) as stream:
            payload = stream.read()
        assert hashlib.sha256(payload).hexdigest() == item["raw_sha256"]
        result.append((item, payload, quote))
    return result


@pytest.mark.parametrize("index", list(QUOTES))
def test_actual_anchorage_payload_has_authored_body_not_form_label(index):
    item, payload, quote = retained()[list(QUOTES).index(index)]
    parsed = parsers.article(payload, source_id="S28")
    assert quote in parsed["text"]
    assert len(parsed["text"]) > 2000
    assert "Institution Type" not in parsed["text"]
    assert parsed["title"] == item["title"]
    assert parsed["published_at"] == item["published_at"]
    assert item["text"] == "Institution Type"  # Original capture remains unchanged.


@pytest.mark.parametrize(
    "authored",
    [
        "",
        '<div class="insight-rich-text w-richtext"></div>',
        '<div class="insight-rich-text w-richtext">one</div><div class="insight-rich-text w-richtext">two</div>',
    ],
)
def test_missing_empty_or_ambiguous_authored_node_refuses_nav_fallback(authored):
    html = "<h1>Release</h1><article>Institution Type</article><main>Navigation</main>" + authored
    with pytest.raises(ValueError, match="one nonempty authored body"):
        parsers.article(html, source_id="S28")
    # Other source contracts keep their existing generic article fallback.
    assert parsers.article(html, source_id="S27")["text"] == "Institution Type"


async def test_normal_anchorage_poll_uses_actual_retained_bodies_and_source_ingest(tmp_path):
    fixtures = retained()
    index_url = "https://www.anchorage.com/insights"
    index_html = (
        "<main>"
        + "".join(
            '<a href="' + escape(item["url"]) + '">' + escape(item["title"]) + "</a>"
            for item, _, _ in fixtures
        )
        + "</main>"
    )
    pages = {item["url"]: payload for item, payload, _ in fixtures}
    requested = []

    def transport(request):
        requested.append(str(request.url))
        return httpx.Response(
            200, content=index_html.encode() if str(request.url) == index_url else pages[str(request.url)]
        )

    settings = Settings(data_dir=tmp_path, x_enabled=False, user_agent="Tank offline source regression")
    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        service = SourceService(repo, settings, http=http)
        try:
            outcome = await service.poll_once("S28")
            assert outcome["status"] == "HEALTHY" and outcome["items"] == 12, outcome
            items = await repo.list("ingest.source_items")
            assert len(items) == 12
            for original, payload, quote in fixtures:
                item = next(row for row in items if row["external_id"] == original["external_id"])
                assert quote in item["text"] and "Institution Type" not in item["text"]
                assert item["raw_sha256"] == hashlib.sha256(payload).hexdigest()
                assert item["published_at"] == original["published_at"]
                assert (
                    item["observed_at"] != original["observed_at"]
                )  # New controlled observation, no backdating.
            jobs, items = await repo.list("ops.jobs"), await repo.list("ingest.source_items")
            # Issuer and market-structure items go straight to routing (03 §3.1).
            assert {j["idem_key"] for j in jobs} == {x["item_id"] for x in items}
            assert {j["type"] for j in jobs} == {"ROUTE_ITEM"}
            assert len(requested) == 13
        finally:
            await service.close()
