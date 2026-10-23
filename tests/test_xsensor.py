"""S29 runs the Apify X actor; responses follow the shape of a real run-sync call (23 Oct 2026)."""

import json
from pathlib import Path

import httpx
import pydantic
import pytest
import zstandard
from test_sources import Repo, settings

from tank.sources import SourceService
from tank.sources.xsensor import AccountFailure, XSensor

URL = "https://api.apify.com/v2/actors/actor/run-sync?build=direct&memory=256&timeout=110"


def post(status_id, author="ethereum", text="Text"):
    return {
        "id": status_id,
        "author": author,
        "text": text,
        "publishedTs": 1792598834.0,
        "publishedAt": "2026-10-21T16:07:14+00:00",
        "url": f"https://x.com/{author}/status/{status_id}",
        "source": "x-public-profile",
    }


def account(handle, *, gap=False):
    return {"handle": handle, "status": "ok", "scanTs": 1.0, "posts": 5, "wireBytes": 1, "historyGap": gap}


def failed(handle):
    return {"handle": handle, "status": "failed", "scanTs": 1.0, "error": "PUBLIC_PROFILE_HTTP_404"}


def run(posts, accounts, cursors, *, health="healthy", error=None):
    return {
        "schemaVersion": 1,
        "mode": "fetch",
        "requestId": None,
        "source": "x-public-profile",
        "checkedAt": "2026-10-23T13:49:11.467537+00:00",
        "ok": health == "healthy",
        "health": health,
        "posts": posts,
        "accounts": accounts,
        "nextCursors": cursors,
        "failedAccounts": [a for a in accounts if a["status"] == "failed"],
        "coverageGaps": [],
        "error": error,
        "stats": {"durationSeconds": 4.1},
    }


class Actor:
    """The run-sync endpoint: records requests and answers with the queued responses."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []
        self.sent = []

    def __call__(self, request):
        self.requests.append(request)
        status, body = self.responses.pop(0)
        self.sent.append(json.dumps(body).encode())
        return httpx.Response(status, content=self.sent[-1])

    def body(self, index):
        return json.loads(self.requests[index].content)


@pytest.mark.asyncio
async def test_first_run_is_a_baseline_then_only_posts_above_the_cursor_are_new():
    actor = Actor(
        (
            201,
            run(
                [post("100"), post("90", "AyaMiyagotchi")],
                [account("ethereum"), account("AyaMiyagotchi")],
                {"ethereum": "100", "ayamiyagotchi": "90"},
            ),
        ),
        (
            201,
            run(
                [post("105", text="New"), post("100"), post("90", "AyaMiyagotchi")],
                [account("ethereum"), account("AyaMiyagotchi", gap=True), failed("gone")],
                {"ethereum": "105", "ayamiyagotchi": "90"},
                health="degraded",
            ),
        ),
    )
    sensor = XSensor(httpx.AsyncClient(transport=httpx.MockTransport(actor)), "FAKE-token")

    first = await sensor.posts(URL, ["ethereum", "AyaMiyagotchi"], {})
    assert first.rows == [] and first.cursors == {"ethereum": "100", "ayamiyagotchi": "90"}
    assert actor.body(0) == {"handles": ["ethereum", "AyaMiyagotchi"]}
    assert actor.requests[0].headers["authorization"] == "Bearer FAKE-token"

    second = await sensor.posts(URL, ["ethereum", "AyaMiyagotchi", "gone"], {**first.cursors, "removed": "1"})
    assert actor.body(1)["cursors"] == {"ethereum": "100", "ayamiyagotchi": "90"}
    assert [row["external_id"] for row in second.rows] == ["105"]
    assert second.rows[0]["text"] == "New" and second.rows[0]["published_at"] == "2026-10-21T16:07:14Z"
    assert second.rows[0]["_payload"] == actor.sent[1]
    assert second.cursors == {"ethereum": "105", "ayamiyagotchi": "90", "removed": "1"}
    assert second.failures == [AccountFailure("gone", "PUBLIC_PROFILE_HTTP_404")]
    assert second.gaps == ["AyaMiyagotchi"]


@pytest.mark.asyncio
async def test_a_failed_or_unreadable_run_raises():
    actor = Actor(
        (201, run([], [failed("ethereum")], {}, health="failed", error="PUBLIC_SOURCE_UNAVAILABLE")),
        (400, {"error": {"type": "run-failed", "message": "Actor run did not succeed"}}),
        (201, {"health": "healthy", "posts": [], "accounts": []}),
    )
    sensor = XSensor(httpx.AsyncClient(transport=httpx.MockTransport(actor)), "FAKE-token")
    with pytest.raises(RuntimeError, match="PUBLIC_SOURCE_UNAVAILABLE"):
        await sensor.posts(URL, ["ethereum"], {"ethereum": "1"})
    with pytest.raises(httpx.HTTPStatusError):
        await sensor.posts(URL, ["ethereum"], {"ethereum": "1"})
    with pytest.raises(pydantic.ValidationError):
        await sensor.posts(URL, ["ethereum"], {"ethereum": "1"})
    degraded = Actor(
        (201, run([], [account("ethereum")], {"ethereum": "1"}, health="degraded", error="COVERAGE"))
    )
    scan = await XSensor(httpx.AsyncClient(transport=httpx.MockTransport(degraded)), "FAKE-token").posts(
        URL, ["ethereum"], {"ethereum": "1"}
    )
    assert scan.failures == [AccountFailure(None, "COVERAGE")]
    with pytest.raises(ValueError, match="APIFY_API_TOKEN"):
        await XSensor(httpx.AsyncClient(), None).posts(URL, ["ethereum"], {})


@pytest.mark.asyncio
async def test_s29_poll_stores_new_posts_and_moves_cursors_only_after_them(tmp_path):
    source = {"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": URL}]}
    config = settings(tmp_path, [source])
    config.apify_api_token = "FAKE-token"
    accounts = [account("Official_Org"), account("other_account")]
    actor = Actor(
        (
            201,
            run(
                [post("100", "Official_Org"), post("99", "other_account")],
                accounts,
                {"official_org": "100", "other_account": "99"},
            ),
        ),
        (
            201,
            run(
                [
                    post("105", "Official_Org", "Upgrade vote"),
                    post("100", "Official_Org"),
                    post("102", "other_account", "Market view"),
                ],
                accounts,
                {"official_org": "105", "other_account": "102"},
            ),
        ),
        (
            201,
            run(
                [],
                [failed("Official_Org"), failed("other_account")],
                {},
                health="failed",
                error="PUBLIC_SOURCE_UNAVAILABLE",
            ),
        ),
    )
    repo = Repo()
    svc = SourceService(repo, config, http=httpx.AsyncClient(transport=httpx.MockTransport(actor)))
    svc.accounts = {
        "official_org": {"handle": "Official_Org", "role": "ORGANIZATION", "entity_id": "uniswap"},
        "other_account": {"handle": "@other_account", "role": "OTHER", "entity_id": "other"},
    }

    def items():
        return {v["external_id"]: v for (c, _), v in repo.rows.items() if c == "ingest.source_items"}

    async def checkpoint():
        return await repo.get("ops.source_checkpoints", "S29:0")

    result = await svc.poll_once("S29")
    assert result["status"] == "HEALTHY" and result["items"] == 0 and not items()
    assert (await checkpoint())["account_cursors"] == {"official_org": "100", "other_account": "99"}
    assert actor.body(0) == {"handles": ["Official_Org", "other_account"]}

    result = await svc.poll_once("S29")
    assert result["status"] == "HEALTHY" and result["items"] == 2 and sorted(items()) == ["102", "105"]
    official, other = items()["105"], items()["102"]
    assert official["author_role"] == "ORGANIZATION" and official["monitored_accounts"] == ["Official_Org"]
    assert other["author_role"] == "OTHER" and other["deferrable"]
    raw = zstandard.ZstdDecompressor().decompress(Path(official["raw_ref"]).read_bytes())
    assert json.loads(raw)["nextCursors"] == {"official_org": "105", "other_account": "102"}
    assert [(args[0], kw["priority"]) for args, kw in repo.jobs] == [("ROUTE_ITEM", 1), ("FILTER_ITEM", 2)]
    assert (await checkpoint())["account_cursors"] == {"official_org": "105", "other_account": "102"}

    result = await svc.poll_once("S29")
    state = await checkpoint()
    assert result["status"] == "DEGRADED" and "PUBLIC_SOURCE_UNAVAILABLE" in state["acquisition_error"]
    assert state["account_cursors"] == {"official_org": "105", "other_account": "102"}
