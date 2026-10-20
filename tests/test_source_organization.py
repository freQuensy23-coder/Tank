import json
from types import SimpleNamespace

import httpx
import pytest

from tank.agents.pipeline import Pipeline
from tank.settings import Settings
from tank.sources import SourceService, parsers
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
