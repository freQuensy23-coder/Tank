"""Anchorage authored-body boundary, including the actual retained twelve pages."""

import hashlib
import io
import json
from html import escape
from pathlib import Path

import httpx
import pytest
import zstandard
from source_support import fixture_request_budgets, fixture_versions

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


@pytest.fixture(scope="session")
def retained_captures():
    if not RAW.exists():
        pytest.fail(
            "Required supplementary captures missing; run python tests/fixtures/supplementary-sources/stage.py"
        )
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
    return tuple(result)


@pytest.mark.historical
@pytest.mark.integration
@pytest.mark.parametrize("index", list(QUOTES))
def test_actual_anchorage_payload_has_authored_body_not_form_label(index, retained_captures) -> None:
    item, payload, quote = retained_captures[list(QUOTES).index(index)]
    parsed = parsers.article(payload, source_id="S28")
    assert quote in parsed["text"]
    assert len(parsed["text"]) > 2000
    assert "Institution Type" not in parsed["text"]
    assert parsed["title"] == item["title"]
    assert parsed["published_at"] == item["published_at"]
    assert item["text"] == "Institution Type"


@pytest.mark.parametrize(
    "authored",
    [
        "",
        '<div class="insight-rich-text w-richtext"></div>',
        '<div class="insight-rich-text w-richtext">one</div><div class="insight-rich-text w-richtext">two</div>',
    ],
)
def test_missing_empty_or_ambiguous_authored_node_refuses_nav_fallback(authored) -> None:
    html = "<h1>Release</h1><article>Institution Type</article><main>Navigation</main>" + authored
    with pytest.raises(ValueError, match="one nonempty authored body"):
        parsers.article(html, source_id="S28")

    assert parsers.article(html, source_id="S27")["text"] == "Institution Type"


@pytest.mark.historical
@pytest.mark.integration
async def test_normal_anchorage_poll_uses_actual_retained_bodies_and_source_ingest(
    tmp_path: Path, retained_captures
) -> None:
    fixtures = retained_captures
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

    def transport(request: httpx.Request):
        requested.append(str(request.url))
        return httpx.Response(
            200, content=index_html.encode() if str(request.url) == index_url else pages[str(request.url)]
        )

    settings = Settings(data_dir=tmp_path, user_agent="Tank offline source regression")
    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        service = SourceService(
            repo, settings, http=http, versions=fixture_versions(), budgets=fixture_request_budgets()
        )
        try:
            outcome = await service.poll_once("S28")
            assert outcome.status == "HEALTHY" and outcome.items == 12, outcome
            items = await repo.list("ingest.source_items")
            assert len(items) == 12
            for original, payload, quote in fixtures:
                item = next(row for row in items if row["external_id"] == original["external_id"])
                assert quote in item["text"] and "Institution Type" not in item["text"]
                assert item["raw_sha256"] == hashlib.sha256(payload).hexdigest()
                assert item["published_at"] == original["published_at"]
                assert item["observed_at"] != original["observed_at"]
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
            assert len(requested) == 13
        finally:
            await service.close()
