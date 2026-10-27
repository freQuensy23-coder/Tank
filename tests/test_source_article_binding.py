import json
from typing import Literal

import pytest
from pydantic import JsonValue

from tank.contracts.source_parsing import ParsedArticle
from tank.sources import parsers

CANONICAL = "https://www.anchorage.com/press-room/authored-release"
INSIGHTS = "https://www.anchorage.com/insights/authored-release"


def s28_page(nodes: list[dict[str, JsonValue]], canonicals: list[str], headings: list[str]) -> bytes:
    return (
        "<html><head>"
        + "".join('<link rel="canonical" href="' + value + '">' for value in canonicals)
        + '</head><body><main><div class="insight-rich-text w-richtext"><p>Actual authored body.</p></div>'
        + "".join("<h1>" + value + "</h1>" for value in headings)
        + '</main><script type="application/ld+json">'
        + json.dumps({"@graph": nodes})
        + "</script></body></html>"
    ).encode()


def posting() -> dict[str, JsonValue]:
    return {
        "@type": "BlogPosting",
        "headline": "Actual release",
        "url": INSIGHTS,
        "mainEntityOfPage": {"@id": INSIGHTS + "#webpage"},
        "datePublished": "2026-10-01",
    }


@pytest.mark.parametrize("response_url", [None, CANONICAL])
def test_captured_anchorage_route_binding_preserves_unreported_response_identity(
    response_url: str | None,
) -> None:
    payload = s28_page([posting()], [CANONICAL], ["Actual release"])
    parsed = parsers.article(payload, source_id="S28", page_url=CANONICAL, response_url=response_url)
    assert parsed.publication.value == "2026-10-01T00:00:00Z"
    assert parsed.publication.precision == "BUCKET(1d)"
    assert parsed.anchorage_binding is not None
    assert parsed.anchorage_binding.canonical_url == CANONICAL
    assert parsed.anchorage_binding.response_url == response_url
    assert ParsedArticle.model_validate_json(parsed.model_dump_json()) == parsed
    wire = parsed.storage_record()
    assert wire["published_at_basis"] == "ARTICLE_JSON_LD_DATE_PUBLISHED"
    assert "response_url" not in wire and "anchorage_binding" not in wire


type BindingFailure = Literal[
    "HOST",
    "SLUG",
    "HEADLINE",
    "NO_CANONICAL",
    "TWO_CANONICALS",
    "TWO_HEADINGS",
    "TWO_URIS",
    "CONFLICTING_MAIN_ENTITY",
    "TWO_POSTINGS",
    "RESPONSE",
    "RESPONSE_ONLY",
    "OTHER_SOURCE",
]


@pytest.mark.parametrize(
    "failure",
    [
        "HOST",
        "SLUG",
        "HEADLINE",
        "NO_CANONICAL",
        "TWO_CANONICALS",
        "TWO_HEADINGS",
        "TWO_URIS",
        "CONFLICTING_MAIN_ENTITY",
        "TWO_POSTINGS",
        "RESPONSE",
        "RESPONSE_ONLY",
        "OTHER_SOURCE",
    ],
)
def test_anchorage_route_variant_never_overrides_missing_or_conflicting_subject_facts(
    failure: BindingFailure,
) -> None:
    node = posting()
    nodes = [node]
    canonicals = [CANONICAL]
    headings = ["Actual release"]
    page_url: str | None = CANONICAL
    response_url: str | None = None
    source_id = "S28"
    if failure == "HOST":
        canonicals = [CANONICAL.replace("www.anchorage.com", "foreign.example")]
    elif failure == "SLUG":
        node["url"] = "https://www.anchorage.com/insights/other-release"
    elif failure == "HEADLINE":
        node["headline"] = "Known other release"
    elif failure == "NO_CANONICAL":
        canonicals = []
    elif failure == "TWO_CANONICALS":
        canonicals = [CANONICAL, CANONICAL]
    elif failure == "TWO_HEADINGS":
        headings.append("Known other release")
    elif failure == "TWO_URIS":
        node["url"] = [INSIGHTS, INSIGHTS]
    elif failure == "CONFLICTING_MAIN_ENTITY":
        node["mainEntityOfPage"] = {"@id": "https://www.anchorage.com/insights/other-release#webpage"}
    elif failure == "TWO_POSTINGS":
        nodes.append(posting())
    elif failure == "RESPONSE":
        response_url = INSIGHTS
    elif failure == "RESPONSE_ONLY":
        page_url = None
        response_url = INSIGHTS
    elif failure == "OTHER_SOURCE":
        source_id = "S30"
    parsed = parsers.article(
        s28_page(nodes, canonicals, headings),
        source_id=source_id,
        page_url=page_url,
        response_url=response_url,
    )
    assert parsed.publication.value is None
    assert parsed.anchorage_binding is None
    assert parsed.text == (
        "Actual authored body. Actual release" if failure == "OTHER_SOURCE" else "Actual authored body."
    )
