"""S29 reads signed-out X profile pages; synthetic pages follow the embedded GraphQL layout of x.com."""

import json
import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import zstandard
from test_sources import Repo, settings

from tank.sources import SourceService
from tank.sources.xsensor import AccountFailure, ProfileError, XSensor, js_string, profile_posts, scan

CREATED_MS = 1792598834000
CREATED_AT = "2026-10-21T16:07:14Z"


def tweet(status_id, text="", *, author="tracked", **extra):
    return {
        "__typename": "Tweet",
        "rest_id": status_id,
        "core": {"user_results": {"result": {"core": {"screen_name": author}}}},
        "details": {"full_text": text, "created_at_ms": CREATED_MS},
        **extra,
    }


def entry(result, entry_id=None):
    return {
        "entry_id": entry_id or "tweet-" + result["rest_id"],
        "content": {"content": {"tweet_results": {"result": result}}},
    }


def profile_page(account, posts, *, pinned=None, entries=()):
    """A signed-out profile document: profile and timeline results as streamed scripts."""
    instructions = [{"__typename": "TimelineClearCache"}]
    if pinned:
        instructions.append({"__typename": "TimelinePinEntry", "entry": entry(pinned)})
    instructions.append(
        {"__typename": "TimelineAddEntries", "entries": [*(entry(post) for post in posts), *entries]}
    )
    profile = {"privacy": {"protected": False}, "core": {"screen_name": account}}
    timeline = {"profile_user_originals_timeline": {"timeline": {"instructions": instructions}}}
    scripts = "".join(
        f'<script>$R[{index}]={{kind:"GraphQLRequestStream.Completed",requestId:{index},key:"op",'
        f'result:$R[{index + 1}]={{kind:"Result.Ok",value:{json.dumps(json.dumps(stream))}}}}}</script>'
        for index, stream in enumerate(
            {"data": {"user_result_by_screen_name": {"result": result}}} for result in (profile, timeline)
        )
    )
    return f"<!DOCTYPE html><html><body>{scripts}</body></html>"


def ok(page):
    return 200, page.encode()


def test_profile_posts_read_text_quote_media_and_pin():
    long_post = tweet(
        "100",
        "Short https://t.co/abc…",
        note_tweet={
            "note_tweet_results": {
                "result": {
                    "text": "Full long post https://t.co/note &amp; more",
                    "entity_set": {
                        "urls": [{"url": "https://t.co/note", "expanded_url": "https://example.org/a"}]
                    },
                }
            }
        },
        media_entities2=[{"type": "photo", "media_url_https": "https://pbs.twimg.com/media/a.jpg"}],
        quoted_tweet_results={
            "rest_id": "50",
            "result": {
                "__typename": "TweetWithVisibilityResults",
                "tweet": tweet("50", "Quoted", author="q"),
            },
        },
    )
    linked = tweet(
        "99",
        "See https://t.co/x",
        url_entities=[{"url": "https://t.co/x", "expanded_url": "https://example.org/x"}],
        quoted_tweet_results={"rest_id": "40", "result": {"__typename": "TweetTombstone"}},
    )
    cursor = {"entry_id": "cursor-bottom-1", "content": {"cursorType": "Bottom"}}
    page = profile_page("Tracked", [long_post, linked], pinned=tweet("10", "Pinned"), entries=[cursor])

    pinned, first, second = profile_posts(page, "Tracked")

    assert (pinned.status_id, pinned.pinned) == (10, True)
    assert first.text == "Full long post https://example.org/a & more" and first.quoted == "Quoted"
    assert first.media == ("https://pbs.twimg.com/media/a.jpg",) and first.published_at == CREATED_AT
    assert (first.account, first.author, first.pinned) == ("Tracked", "tracked", False)
    assert second.text == "See https://example.org/x" and second.quoted is None
    assert first.row(b"page")["url"] == "https://x.com/tracked/status/100"
    repinned = tweet("8")
    assert [
        post.status_id for post in profile_posts(profile_page("t", [repinned], pinned=repinned), "t")
    ] == [8]


def test_real_page_variants_still_read():
    assert js_string(r"\x3Cb\x3E \"q\" \\x41 \n \u00e9") == '<b> "q" \\x41 \n é'
    stub_note = tweet("7", "Short text", note_tweet={"note_tweet_results": {"result": {"rest_id": "1"}}})
    page = profile_page("t", [stub_note]).replace("Short text", "Short \\x3C text")
    [post] = profile_posts(page, "t")
    assert post.text == "Short < text"


def test_unexpected_page_format_fails_the_account():
    with pytest.raises(ProfileError, match="TIMELINE_MISSING"):
        profile_posts("<html>Something went wrong</html>", "t")
    broken = '<script>$R[1]={kind:"Result.Ok",value:"{not json"}</script>'
    with pytest.raises(ProfileError, match="PROFILE_DATA_INVALID"):
        profile_posts(broken, "t")
    moved = {"entry_id": "tweet-9", "content": {"itemContent": {"tweet_results": {"result": tweet("9")}}}}
    with pytest.raises(ProfileError, match="PROFILE_DATA_INVALID"):
        profile_posts(profile_page("t", [], entries=[moved]), "t")
    renamed = {key: value for key, value in tweet("9").items() if key != "details"}
    with pytest.raises(ProfileError, match="PROFILE_DATA_INVALID"):
        profile_posts(profile_page("t", [renamed]), "t")


def test_scan_baseline_new_posts_gaps_and_failures():
    old_pin = tweet("10")
    first = scan(
        {
            "Tracked": ok(profile_page("Tracked", [tweet("101"), tweet("100")], pinned=old_pin)),
            "Empty": ok(profile_page("Empty", [])),
        },
        {},
    )
    assert first.rows == [] and first.cursors == {"tracked": "101"} and not first.failures and not first.gaps

    tracked = ok(profile_page("Tracked", [tweet("103"), tweet("102"), tweet("101")], pinned=old_pin))
    second = scan(
        {
            "Tracked": tracked,
            "Burst": ok(profile_page("Burst", [tweet(str(i)) for i in range(210, 205, -1)])),
            "Missing": (404, b""),
            "Blocked": RuntimeError("Page.goto: net::ERR_HTTP_RESPONSE_CODE_FAILURE\nCall log: ..."),
        },
        {"tracked": "101", "burst": "200", "missing": "77"},
    )
    assert [row["external_id"] for row in second.rows] == ["103", "102", "210", "209", "208", "207", "206"]
    assert second.rows[0]["_payload"] == tracked[1]
    assert second.gaps == ["Burst"]
    assert second.cursors == {"tracked": "103", "burst": "210", "missing": "77"}
    assert second.failures == [
        AccountFailure("Missing", "HTTP_404"),
        AccountFailure("Blocked", "NAVIGATION_FAILED: Page.goto: net::ERR_HTTP_RESPONSE_CODE_FAILURE"),
    ]
    with pytest.raises(RuntimeError, match="every account failed: HTTP_403 x2"):
        scan({"a": (403, b""), "b": (403, b"")}, {"a": "1"})


class FakeContext:
    async def route(self, pattern, handler):
        self.pattern = pattern

    async def close(self):
        self.closed = True


class FakeChromium:
    async def new_context(self, **options):
        self.context = FakeContext()
        return self.context


@pytest.mark.asyncio
async def test_s29_poll_baseline_then_stores_new_posts(tmp_path):
    source = {"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": "https://x.com/{handle}"}]}
    repo = Repo()
    svc = SourceService(repo, settings(tmp_path, [source]))
    svc.accounts = {
        "official_org": {"handle": "Official_Org", "role": "ORGANIZATION", "entity_id": "uniswap"},
        "other_account": {"handle": "@other_account", "role": "OTHER", "entity_id": "other"},
    }
    chromium = FakeChromium()
    svc.xsensor.browser = AsyncMock(return_value=chromium)
    pages = {
        "Official_Org": ok(profile_page("Official_Org", [tweet("100", "Old", author="Official_Org")])),
        "other_account": ok(profile_page("other_account", [tweet("99", "Old too", author="other_account")])),
    }

    async def read(context, account):
        return pages[account]

    def items():
        return {v["external_id"]: v for (c, _), v in repo.rows.items() if c == "ingest.source_items"}

    async def checkpoint():
        return await repo.get("ops.source_checkpoints", "S29:0")

    svc.xsensor.read = read
    try:
        result = await svc.poll_once("S29")
        assert result["status"] == "HEALTHY" and result["items"] == 0 and not items()
        assert chromium.context.pattern == "**/*" and chromium.context.closed
        assert (await checkpoint())["account_cursors"] == {"official_org": "100", "other_account": "99"}

        pages["Official_Org"] = ok(
            profile_page("Official_Org", [tweet("105", "Upgrade vote", author="Official_Org"), tweet("100")])
        )
        pages["other_account"] = ok(
            profile_page("other_account", [tweet("102", "Market view", author="other_account"), tweet("99")])
        )
        result = await svc.poll_once("S29")
        assert result["status"] == "HEALTHY" and result["items"] == 2
        official, other = items()["105"], items()["102"]
        assert official["author_role"] == "ORGANIZATION" and official["monitored_accounts"] == [
            "Official_Org"
        ]
        assert (
            official["published_at"] == CREATED_AT and other["author_role"] == "OTHER" and other["deferrable"]
        )
        raw = zstandard.ZstdDecompressor().decompress(Path(official["raw_ref"]).read_bytes())
        assert raw == pages["Official_Org"][1]
        assert [(args[0], kw["priority"]) for args, kw in repo.jobs] == [
            ("ROUTE_ITEM", 1),
            ("FILTER_ITEM", 2),
        ]
        assert (await checkpoint())["account_cursors"] == {"official_org": "105", "other_account": "102"}

        pages["other_account"] = (503, b"")
        result = await svc.poll_once("S29")
        state = await checkpoint()
        assert result["status"] == "DEGRADED" and state["account_cursors"]["other_account"] == "102"
        assert state["entry_errors"] == [{"account": "other_account", "reason": "HTTP_503"}]

        pages["Official_Org"] = (403, b"")
        result = await svc.poll_once("S29")
        state = await checkpoint()
        assert result["status"] == "DEGRADED" and "every account failed" in state["acquisition_error"]
        assert state["account_cursors"] == {"official_org": "105", "other_account": "102"}
    finally:
        await svc.close()


@pytest.mark.browser
@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_chromium_reads_only_the_profile_document():
    if os.getenv("TANK_BROWSER_TESTS") != "1":
        pytest.skip("Requires TANK_BROWSER_TESTS=1")
    from types import SimpleNamespace

    from tank.sources.browser import BrowserReader

    sensor = XSensor()
    renderer = BrowserReader(SimpleNamespace(), shared_sensor=sensor)
    page = profile_page("tracked", [tweet("100", "Synthetic post")]).replace(
        "</body>",
        '<img src="https://pbs.twimg.com/a.jpg"><script src="https://abs.twimg.com/a.js"></script></body>',
    )
    requests = []

    async def serve(route):
        requests.append((route.request.resource_type, route.request.url))
        await route.fulfill(status=200, content_type="text/html", body=page)

    browser = await sensor.browser()
    original = browser.new_context

    async def routed_context(**options):
        context = await original(**options)
        await context.route("https://x.com/**", serve)
        return context

    try:
        assert "HeadlessChrome" not in sensor.user_agent
        browser.new_context = routed_context
        result = await sensor.posts(["tracked"], {"tracked": "99"})
        assert [row["external_id"] for row in result.rows] == ["100"]
        assert requests == [("document", "https://x.com/tracked")]
        browser.new_context = original
        await renderer.open()
        assert renderer.browser is browser and await renderer.context.cookies() == []
    finally:
        await renderer.close()
        await sensor.close()
