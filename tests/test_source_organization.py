import copy
import json
from types import SimpleNamespace

import httpx
import pytest

from tank.agents.pipeline import Pipeline
from tank.settings import Settings
from tank.sources import SourceService, parsers
from tank.sources.organization import normalize_author_organization, resolve_author_organization
from tank.storage import MemoryRepository
from tank.util import sha256

FR_URL = "https://www.federalregister.gov/api/v1/documents.json?per_page=20"
FR_ITEM = "https://www.federalregister.gov/documents/2026/10/01/2026-10000/example"
REPO = "https://github.com/LayerZero-Labs/LayerZero-v2"
FEED = REPO + "/commits/main.atom"
ATOM = (
    f'<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>commit-one</id><title>Fix base oapp</title><updated>2026-10-18T12:00:00Z</updated><link href="{REPO}/commit/abc"/><content type="html">Fix base oapp</content></entry></feed>'
).encode()


def source(sid):
    feed = (
        {"url": FR_URL, "kind": "json"}
        if sid == "S03"
        else {"url": FEED, "kind": "atom", "subtype": "commits"}
    )
    return {"source_id": sid, "name": "Federal Register" if sid == "S03" else "GitHub", "feeds": [feed]}


def retained(sid):
    src = source(sid)
    if sid == "S03":
        payload = json.dumps(
            {
                "results": [
                    {
                        "document_number": "2026-10000",
                        "title": "Official action",
                        "abstract": "Specific change.",
                        "html_url": FR_ITEM,
                        "publication_date": "2026-10-01",
                        "agencies": [
                            {"name": "Securities and Exchange Commission"},
                            {"raw_name": "Treasury Department"},
                        ],
                    }
                ]
            }
        ).encode()
        item = parsers.json_items(payload, sid)[0]
    else:
        payload = ATOM
        item = parsers.feeds(payload, sid, src["feeds"][0])[0]
    item.update(
        source_id=sid,
        feed=src["feeds"][0]["url"],
        author=src["name"],
        author_organization=None,
        raw_ref="retained",
        raw_sha256=sha256(payload),
    )
    item["content_hash"] = sha256(" ".join((item["title"] + " " + item["text"] + " ").split()))
    return src, item, payload


@pytest.mark.parametrize(
    "sid,expected",
    [("S03", "Securities and Exchange Commission; Treasury Department"), ("S25", "LayerZero-Labs")],
)
def test_retained_issuer_and_repository_publisher_are_factual_metadata_only(sid, expected):
    src, item, payload = retained(sid)
    original = copy.deepcopy(item)
    result, receipt = normalize_author_organization(item, sources=[src], raw_payloads={"retained": payload})
    assert receipt["outcome"] == "VERIFIED" and receipt["changed_fields"] == ["author_organization"]
    assert result == {**original, "author_organization": expected}
    assert result["author"] == original["author"] and item == original
    assert receipt["organization_basis"] == (
        "OFFICIAL_RAW_ISSUING_AGENCIES"
        if sid == "S03"
        else "CONFIGURED_REPOSITORY_PUBLISHING_NAMESPACE_NOT_EMPLOYER"
    )


def test_no_false_publisher_from_title_foreign_repo_forum_or_forged_agencies():
    src, item, payload = retained("S03")
    forged = {**item, "agencies": [{"name": "Foreign authority"}]}
    unchanged, receipt = normalize_author_organization(
        forged, sources=[src], raw_payloads={"retained": payload}
    )
    assert unchanged == forged and receipt["outcome"] == "UNVERIFIABLE"
    assert receipt["reason"] == "ISSUING_AGENCY_RAW_METADATA_MISMATCH"
    assert resolve_author_organization(src, {**item, "agencies": None, "title": "SEC announces"}) is None
    assert resolve_author_organization(src, {**item, "feed": "https://foreign.example/feed"}) is None
    assert resolve_author_organization(src, {**item, "url": "https://foreign.example/article"}) is None
    github, commit, _ = retained("S25")
    for changed in [
        {"url": "https://github.com/foreign/repo/commit/abc"},
        {"url": REPO + "-other/commit/abc"},
        {"feed": "https://github.com/foreign/repo/commits/main.atom"},
    ]:
        assert resolve_author_organization(github, {**commit, **changed}) is None
    forum = {"source_id": "S23", "feeds": [{"url": "https://gov.near.org/latest.json", "kind": "json"}]}
    assert (
        resolve_author_organization(
            forum,
            {
                "feed": forum["feeds"][0]["url"],
                "author": "Unverified contributor",
                "title": "NEAR Foundation",
            },
        )
        is None
    )
    # Actual explicit metadata/config precedence is retained, never rewritten to convenient identities.
    assert (
        resolve_author_organization(github, {**commit, "author_organization": "Explicit author organization"})
        == "Explicit author organization"
    )


@pytest.mark.parametrize("sid", ["S03", "S25"])
async def test_normal_poll_preserves_actual_author_and_exposes_exact_pruner_organization(tmp_path, sid):
    src, _, payload = retained(sid)
    registry = Settings().load_config("registry")
    config = SimpleNamespace(
        data_dir=tmp_path,
        user_agent="Tank offline source test",
        config_version="test",
        code_version="test",
        x_list_id=None,
        capture_only=True,
        load_config=lambda name: (
            {"sources": [src]} if name == "sources" else registry if name == "registry" else {}
        ),
    )
    requests = []

    def response(request):
        requests.append(str(request.url))
        assert str(request.url) == src["feeds"][0]["url"]
        return httpx.Response(200, content=payload)

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        service = SourceService(repo, config, http=http)
        try:
            await service.poll_once(sid)
            rows = await repo.list("ingest.source_items")
            assert len(rows) == 1 and len(requests) == 1
            record = rows[0]
            assert record["author"] == src["name"]
            identities = Pipeline(repo, config, http=object()).identities(record)
            assert "sec" in identities if sid == "S03" else "layerzero" in identities
            assert record["author_organization"] == (
                "Securities and Exchange Commission; Treasury Department"
                if sid == "S03"
                else "LayerZero-Labs"
            )
        finally:
            await service.close()


@pytest.mark.parametrize("sid", ["S03", "S25"])
def test_publisher_raw_item_url_forgery_is_not_normalized(sid):
    src, item, payload = retained(sid)
    wrong = {**item, "url": FR_ITEM + "-other" if sid == "S03" else REPO + "/commit/other"}
    result, receipt = normalize_author_organization(wrong, sources=[src], raw_payloads={"retained": payload})
    assert result == wrong and receipt["outcome"] == "UNVERIFIABLE"
    assert receipt["reason"] == "PUBLISHER_ITEM_URL_IDENTITY_MISMATCH"


def test_issuing_agency_can_be_restored_from_verified_raw_without_changing_source_fields():
    src, item, payload = retained("S03")
    item.pop("agencies")
    original = copy.deepcopy(item)
    result, receipt = normalize_author_organization(item, sources=[src], raw_payloads={"retained": payload})
    assert result == {
        **original,
        "author_organization": "Securities and Exchange Commission; Treasury Department",
    }
    assert "agencies" not in result and receipt["outcome"] == "VERIFIED"
    assert resolve_author_organization(src, {**item, "agencies": [{"name": True}]}) is None
    assert resolve_author_organization(src, {**item, "agencies": [{"name": ""}]}) is None
