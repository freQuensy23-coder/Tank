import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
import zstandard

from tank.sources import SourceService, parsers

RSS = b"""<rss version="2.0"><channel><title>Official</title><item><guid>release-42</guid><title>New protocol</title><link>https://example.org/a</link><pubDate>Fri, 16 Oct 2026 12:00:00 GMT</pubDate><description>&lt;p&gt;Protocol deployment &amp;amp; vote&lt;/p&gt;</description></item></channel></rss>"""


class Repo:
    def __init__(self):
        self.rows = {}
        self.jobs = []

    async def get(self, c, k):
        return self.rows.get((c, k))

    async def put(self, c, k, v):
        self.rows[c, k] = dict(v)
        return v

    async def list(self, *a, **k):
        return []

    async def enqueue(self, *a, **k):
        self.jobs.append((a, k))

    @asynccontextmanager
    async def lock(self, key):
        yield

    @asynccontextmanager
    async def transaction(self):
        yield


def settings(tmp_path, sources):
    return SimpleNamespace(
        data_dir=tmp_path,
        user_agent="Tank test",
        config_version="test",
        code_version="test",
        x_list_id=None,
        load_config=lambda name: (
            {"sources": sources} if name == "sources" else {"x_accounts": [], "tokens": [], "entities": []}
        ),
    )


def test_rss_atom():
    row = parsers.feeds(RSS, "S30", {"url": "https://example.org/rss"})[0]
    assert row["external_id"] == "release-42"
    assert row["published_at"] == "2026-10-16T12:00:00Z"
    assert row["text"] == "Protocol deployment & vote"
    atom = b'<feed xmlns="http://www.w3.org/2005/Atom"><title>Commits</title><entry><id>tag:github.com,2008:Grit::Commit/abc</id><title>Fix swap</title><updated>2026-10-16T12:00:00Z</updated><link href="https://github.com/org/repo/commit/abc"/><content type="html">&lt;p&gt;Fix routing&lt;/p&gt;</content></entry></feed>'
    row = parsers.feeds(atom, "S25", {"url": "https://github.com/org/repo/commits/main.atom"})[0]
    assert row["subtype"] == "commits" and row["text"] == "Fix routing"


def test_html_patterns_and_article():
    feed = {"url": "https://example.org/news", "article_pattern": r"^/news/[a-z]+$"}
    rows = parsers.article_links(
        '<a href="/news/test">Test</a><a href="https://evil.org/news/test">Bad</a>', feed
    )
    assert len(rows) == 1 and rows[0]["url"] == "https://example.org/news/test"
    with pytest.raises(ValueError, match="layout"):
        parsers.article_links('<a href="/">Home</a>', feed)
    row = parsers.article(
        '<nav>Ignore</nav><article><h1>Title</h1><time datetime="2026-10-16T12:00:00Z"></time><p>Evidence</p><script>bad()</script></article>'
    )
    assert row["text"] == "Title Evidence"


def test_json_contracts():
    row = parsers.json_items(
        {
            "result": {
                "list": [
                    {
                        "url": "https://bybit.com/a",
                        "title": "Listing",
                        "description": "<p>ARB</p>",
                        "type": {"key": "new_crypto"},
                        "dateTimestamp": 1679045608000,
                    }
                ]
            }
        },
        "S11",
    )[0]
    assert row["text"] == "ARB" and row["announcement_type"]["key"] == "new_crypto"
    row = parsers.json_items(
        {"data": [{"url": "https://okx.com/a", "title": "New", "pTime": "1679045608000"}]}, "S10"
    )[0]
    assert row["external_id"].endswith("|1679045608000")
    row = parsers.json_items(
        {
            "data": {
                "catalogs": [
                    {"articles": [{"code": "abc", "title": "Release", "releaseDate": 1679045608000}]}
                ]
            }
        },
        "S09",
    )[0]
    assert row["external_id"] == "abc"
    row = parsers.json_items(
        {
            "data": {
                "proposals": [
                    {
                        "id": "0xabc",
                        "title": "Vote",
                        "body": "Proposal",
                        "state": "closed",
                        "start": 1679045608,
                        "end": 1679046608,
                        "scores": [1, 2],
                        "scores_total": 3,
                        "space": {"id": "cow.eth"},
                    }
                ]
            }
        },
        "S24",
    )[0]
    assert row["external_id"] == "0xabc|closed" and row["scores_total"] == 3


@pytest.mark.parametrize(
    "venue,payload,symbol",
    [
        (
            "binance",
            {
                "symbols": [
                    {"symbol": "ABCUSDT", "baseAsset": "ABC", "quoteAsset": "USDT", "status": "TRADING"}
                ]
            },
            "ABCUSDT",
        ),
        (
            "coinbase",
            [{"id": "ABC-USD", "base_currency": "ABC", "quote_currency": "USD", "status": "online"}],
            "ABC-USD",
        ),
        (
            "kraken",
            {"error": [], "result": {"ABCUSD": {"base": "ABC", "quote": "USD", "status": "online"}}},
            "ABCUSD",
        ),
        (
            "okx",
            {"code": "0", "data": [{"instId": "ABC-USDT-SWAP", "state": "live", "ctType": "linear"}]},
            "ABC-USDT-SWAP",
        ),
        (
            "bybit",
            {
                "retCode": 0,
                "result": {
                    "list": [
                        {"symbol": "ABCUSDT", "baseCoin": "ABC", "quoteCoin": "USDT", "status": "Trading"}
                    ]
                },
            },
            "ABCUSDT",
        ),
        ("hyperliquid", {"universe": [{"name": "ABC", "isDelisted": False}]}, "ABC"),
    ],
)
def test_instrument_adapters(venue, payload, symbol):
    assert parsers.instruments(payload, {"venue": venue})[symbol]["status"]


@pytest.mark.asyncio
async def test_raw_first_dedup_and_conditional_get(tmp_path):
    calls = []

    def handle(request):
        calls.append(request)
        return (
            httpx.Response(304)
            if request.headers.get("If-None-Match")
            else httpx.Response(200, content=RSS, headers={"ETag": '"v1"'})
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    repo = Repo()
    svc = SourceService(
        repo,
        settings(
            tmp_path, [{"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}]
        ),
        http=http,
    )
    assert (await svc.poll_once("S30"))["items"] == 1
    assert (await svc.poll_once("S30"))["items"] == 0
    record = next(v for (c, k), v in repo.rows.items() if c == "ingest.source_items")
    assert (
        zstandard.ZstdDecompressor().decompress(__import__("pathlib").Path(record["raw_ref"]).read_bytes())
        == RSS
    )
    assert repo.jobs[0][0][0] == "FILTER_ITEM"
    await svc.ingest(
        svc.sources["S30"],
        parsers.feeds(RSS, "S30", {"url": "https://example.org/rss"})[0],
        record["raw_ref"],
        record["raw_sha256"],
    )
    assert len(repo.jobs) == 1
    assert (await repo.get("ingest.source_items", record["item_id"]))["observation_count"] == 2
    await http.aclose()


def test_all_34_source_configurations():
    from tank.settings import Settings

    cfg = Settings().load_config("sources")["sources"]
    assert {s["source_id"] for s in cfg} == {f"S{i:02d}" for i in range(1, 35)}
    for source in cfg:
        assert source["feeds"]
        for feed in source["feeds"]:
            assert feed["kind"] in {
                "rss",
                "atom",
                "html",
                "json",
                "graphql",
                "dtcc_search",
                "instruments",
                "x",
                "rendered_html",
                "rendered",
            }
            assert feed["url"].startswith("https://")
            if feed["kind"] in {"html", "rendered", "rendered_html"}:
                assert feed["article_pattern"]
                __import__("re").compile(feed["article_pattern"])
            if feed["kind"] == "instruments":
                assert feed["venue"] in {"binance", "coinbase", "kraken", "okx", "bybit", "hyperliquid"}
            if feed.get("method") == "POST":
                assert feed["body"]


def test_binance_rich_document():
    payload = json.dumps(
        {
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [{"text": "We list ABC"}, {"text": "on Monday."}]}
            ],
        }
    )
    assert parsers.binance_body(payload) == "We list ABC on Monday."


@pytest.mark.asyncio
async def test_instrument_baseline_diff_and_bybit_cursor(tmp_path):
    phase = [0]
    calls = []

    def handle(request):
        calls.append(str(request.url))
        cursor = request.url.params.get("cursor")
        if cursor:
            rows = [{"symbol": "DEFUSDT", "baseCoin": "DEF", "quoteCoin": "USDT", "status": "Trading"}]
            next_cursor = ""
        else:
            rows = [
                {
                    "symbol": "ABCUSDT",
                    "baseCoin": "ABC",
                    "quoteCoin": "USDT",
                    "status": "Trading" if phase[0] == 0 else "Settling",
                }
            ]
            next_cursor = "next"
        return httpx.Response(
            200, json={"retCode": 0, "result": {"list": rows, "nextPageCursor": next_cursor}}
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    repo = Repo()
    source = {
        "source_id": "S14",
        "feeds": [
            {
                "kind": "instruments",
                "url": "https://api.bybit.com/v5/market/instruments-info?category=linear",
                "venue": "bybit",
                "market_type": "PERP_LINEAR",
            }
        ],
    }
    svc = SourceService(repo, settings(tmp_path, [source]), http=http)
    assert (await svc.poll_once("S14"))["items"] == 0
    phase[0] = 1
    assert (await svc.poll_once("S14"))["items"] == 1
    row = next(v for (c, k), v in repo.rows.items() if c == "ingest.source_items")
    assert row["change"] == "STATE_UPDATE" and "Settling" in row["text"]
    assert len(calls) == 4
    await http.aclose()


@pytest.mark.asyncio
async def test_snapshot_closed_proposal_checked_after_leaving_latest(tmp_path):
    phase = [0]

    def proposal(state):
        return {
            "id": "0xold",
            "title": "Vote",
            "body": "Proposal",
            "state": state,
            "start": 1679045608,
            "end": 1679046608,
            "scores": [1, 2],
            "scores_total": 3,
            "space": {"id": "cow.eth"},
        }

    def handle(request):
        query = json.loads(request.content)["query"]
        rows = [proposal("active")] if phase[0] == 0 else [proposal("closed")] if "id_in:" in query else []
        return httpx.Response(200, json={"data": {"proposals": rows}})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    repo = Repo()
    source = {
        "source_id": "S24",
        "feeds": [
            {
                "kind": "graphql",
                "url": "https://hub.snapshot.org/graphql",
                "method": "POST",
                "body": {"query": "query { proposals(first:20) {id} }"},
            }
        ],
    }
    svc = SourceService(repo, settings(tmp_path, [source]), http=http)
    assert (await svc.poll_once("S24"))["items"] == 1
    phase[0] = 1
    assert (await svc.poll_once("S24"))["items"] == 1
    rows = [v for (c, k), v in repo.rows.items() if c == "ingest.source_items"]
    # State must remain part of normalized content to avoid global content dedup suppressing update.
    assert any(r["external_id"] == "0xold|closed" for r in rows)
    await http.aclose()


@pytest.mark.asyncio
async def test_source_ingest_postgres_projection_and_durable_queue(tmp_path):
    """Exercise the actual schema, not a permissive in-memory record store."""
    import os

    from tank.storage import PostgresRepository
    from tank.util import new_id

    database_url = os.getenv("TANK_SOURCE_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Requires isolated PostgreSQL source test database")
    repo = PostgresRepository(database_url)
    await repo.open()
    await repo.migrate()
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=RSS)))
    try:
        official = {
            "source_id": "S01",
            "name": "SEC",
            "feeds": [{"kind": "rss", "url": "https://example.org/rss"}],
        }
        filtered = {
            "source_id": "S30",
            "name": "BBC",
            "feeds": [{"kind": "rss", "url": "https://example.org/rss"}],
        }
        svc = SourceService(repo, settings(tmp_path, [official, filtered]), http=http)
        for source, expected, job_kind in [
            (official, "ALWAYS", "ROUTE_ITEM"),
            (filtered, None, "FILTER_ITEM"),
        ]:
            key = new_id("test-source")
            raw_ref, digest = svc.raw(key.encode())
            item = {
                "external_id": key,
                "title": key,
                "text": "Actual source content",
                "url": "https://example.org/item",
                "published_at": "2026-10-16T12:00:00Z",
            }
            record = await svc.ingest(source, item, raw_ref, digest)
            assert record["relevance"]["decision"] == expected
            await svc.ingest(source, item, raw_ref, digest)
            async with repo.transaction():
                row = await (
                    await repo._conn().execute(
                        "SELECT relevance_decision,published_prec,body,raw_path FROM ingest.source_items WHERE item_id=%s",
                        (record["item_id"],),
                    )
                ).fetchone()
                jobs = await (
                    await repo._conn().execute(
                        "SELECT count(*) AS n FROM ops.jobs WHERE payload->>'item_id'=%s AND type=%s",
                        (record["item_id"], job_kind),
                    )
                ).fetchone()
            assert row["relevance_decision"] == expected
            assert row["published_prec"] == "EXACT" and row["body"] == item["text"]
            assert row["raw_path"] == raw_ref and jobs["n"] == 1
            assert (await repo.get("ingest.source_items", record["item_id"]))["observation_count"] == 2
    finally:
        await http.aclose()
        await repo.close()


@pytest.mark.asyncio
async def test_probe_reads_current_payload_without_ingestion_jobs(tmp_path):
    calls = []

    def handle(request):
        calls.append(request)
        if request.url.host == "blocked.example.org":
            return httpx.Response(403, content=b"Forbidden")
        return httpx.Response(200, content=RSS, headers={"content-type": "application/rss+xml"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    repo = Repo()
    svc = SourceService(
        repo,
        settings(
            tmp_path,
            [
                {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]},
                {"source_id": "S31", "feeds": [{"url": "https://blocked.example.org/rss", "kind": "rss"}]},
                {
                    "source_id": "S19",
                    "feeds": [
                        {
                            "url": "https://rendered.example.org/press",
                            "kind": "html",
                            "article_pattern": "/press/[a-z]+",
                        }
                    ],
                },
                {"source_id": "S29", "feeds": [{"url": "https://x.com/i/lists/PRIVATE", "kind": "x"}]},
            ],
        ),
        http=http,
    )
    results = await svc.check(probe=True)
    assert [r["status"] for r in results] == ["PASS", "HTTP_ERROR", "UNAVAILABLE", "UNAVAILABLE"]
    assert results[0]["item_count"] == 1 and results[1]["http_status"] == 403
    assert len(calls) == 2 and not repo.jobs and not repo.rows
    assert "PRIVATE" not in json.dumps(results)
    assert (
        zstandard.ZstdDecompressor().decompress(
            __import__("pathlib").Path(results[0]["raw_ref"]).read_bytes()
        )
        == RSS
    )
    # Default check is now purely persisted health and makes no hidden remote requests.
    await svc.check()
    assert len(calls) == 2
    await http.aclose()


def test_okx_live_observed_nested_details_schema():
    payload = {
        "code": "0",
        "data": [
            {
                "details": [
                    {
                        "annType": "announcements-new-listings",
                        "title": "OKX to list DNAUSD X-Perp",
                        "url": "https://www.okx.com/help/okx-to-list-dnausd-x-perp",
                        "pTime": "1792152012457",
                        "businessPTime": "1792152000000",
                    }
                ]
            }
        ],
    }
    row = parsers.json_items(payload, "S10")[0]
    assert row["external_id"] == "https://www.okx.com/help/okx-to-list-dnausd-x-perp|1792152012457"


def test_robinhood_canonical_article_metadata_same_origin_only():
    feed = {
        "url": "https://robinhood.com/newsroom/",
        "article_pattern": r"^/(?:us/en/)?newsroom/(?!category/)[A-Za-z0-9-]+/?$",
        "embedded_json_fields": ["canonicalUrl"],
    }
    data = {
        "props": {
            "pageProps": {
                "entries": [
                    {
                        "fields": {
                            "title": "Protocol release",
                            "canonicalUrl": "https://robinhood.com/us/en/newsroom/protocol-release",
                        }
                    },
                    {"fields": {"canonicalUrl": "https://evil.org/us/en/newsroom/injected"}},
                    {"fields": {"canonicalUrl": "https://robinhood.com/newsroom/category/crypto"}},
                ]
            }
        }
    }
    html = '<script id="__NEXT_DATA__" type="application/json">' + json.dumps(data) + "</script>"
    rows = parsers.article_links(html, feed)
    assert len(rows) == 1 and rows[0]["title"] == "Protocol release"
    assert rows[0]["url"] == "https://robinhood.com/us/en/newsroom/protocol-release"


@pytest.mark.asyncio
async def test_dtcc_public_search_pagination_checkpoint_and_ordering(tmp_path):
    calls = []

    def release(slug, stamp):
        url = "https://www.dtcc.com/press-releases/2026/" + slug
        return {
            "clickUri": url,
            "title": slug,
            "excerpt": "Official release",
            "raw": {"news_full_name": slug, "news_date": stamp},
        }

    ordered = [
        release("third-release", 1790654400000),
        release("second-release", 1790568000000),
        release("first-release", 1790481600000),
    ]

    def handle(request):
        if request.url.host == "www.dtcc.com":
            return httpx.Response(200, text='publicSettings={apiKey:"anonymous-public-search-only"}')
        body = json.loads(request.content)
        calls.append(body["firstResult"])
        assert request.headers["Authorization"] == "Bearer anonymous-public-search-only"
        return httpx.Response(
            200, json={"totalCount": 3, "results": ordered[body["firstResult"] : body["firstResult"] + 2]}
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    svc = SourceService(Repo(), settings(tmp_path, []), http=http)
    feed = {
        "url": "https://search.example.org/rest/search/v2",
        "kind": "dtcc_search",
        "bootstrap_script_url": "https://www.dtcc.com/_next/static/chunks/public.js",
        "body": {"numberOfResults": 2},
    }
    payload, _state = await svc.dtcc_payload(feed, "test", {})
    rows = parsers.json_items(payload, "S08")
    assert [r["title"] for r in rows] == ["third-release", "second-release", "first-release"]
    assert calls == [0, 2] and len(json.loads(payload)["raw_pages"]) == 2
    calls.clear()
    payload, _state = await svc.dtcc_payload(feed, "test", {"dtcc_checkpoint": ordered[1]["clickUri"]})
    assert [r["title"] for r in parsers.json_items(payload, "S08")] == ["third-release", "second-release"]
    assert calls == [0]
    await http.aclose()


@pytest.mark.asyncio
async def test_x_real_browser_local_dom_full_quote_repost_and_checkpoint(tmp_path):
    """Synthetic X DOM executed in Chromium; no real X request/profile is used."""
    import os

    if os.getenv("TANK_BROWSER_TESTS") != "1":
        pytest.skip("Opt-in real Chromium test requires TANK_BROWSER_TESTS=1")
    from tank.sources.xsensor import XSensor as BrowserReader

    reader = BrowserReader(
        SimpleNamespace(
            x_profile_dir=tmp_path / "synthetic-x-profile",
            x_storage_state_path=tmp_path / "absent-synthetic-secret",
        )
    )
    list_html = """<html><body>
      <article><a href="/actual_author/status/100"><time datetime="2026-10-17T12:00:00Z"></time></a>
      <div data-testid="tweetText" id="original">Short</div>
      <button data-testid="tweet-text-show-more-link" onclick="document.getElementById('original').textContent='Full original text beyond truncation';this.remove()">Show more</button>
      <a href="/quoted_author/status/50">Quote</a><div data-testid="tweetText">Truncated quote</div></article>
      <article><div data-testid="socialContext"><a href="/tracked_reposter">Tracked reposter reposted</a></div>
      <a href="/original_poster/status/99"><time datetime="2026-10-17T11:00:00Z"></time></a><div data-testid="tweetText">Original reposted text</div></article>
      <article><a href="/previous/status/98"><time datetime="2026-10-17T10:00:00Z"></time></a><div data-testid="tweetText">Previously seen</div></article>
    </body></html>"""
    quote_html = """<article><a href="/quoted_author/status/50"><time datetime="2026-10-16T10:00:00Z"></time></a><div data-testid="tweetText">Complete quoted post text from its status page</div></article>"""
    try:
        await reader.open()

        async def route(request_route):
            url = request_route.request.url
            if "/i/lists/local-test" in url:
                await request_route.fulfill(status=200, content_type="text/html", body=list_html)
            elif "/quoted_author/status/50" in url:
                await request_route.fulfill(status=200, content_type="text/html", body=quote_html)
            else:
                await request_route.abort()

        await reader.context.route("**/*", route)
        rows, reached = await reader.posts("local-test", "98")
        assert reached and [r["external_id"] for r in rows] == ["100", "99"]
        assert (
            rows[0]["author"] == "actual_author" and rows[0]["text"] == "Full original text beyond truncation"
        )
        assert rows[0]["quoted"] == "Complete quoted post text from its status page"
        assert rows[0]["related_authors"] == [{"author": "quoted_author", "status_id": "50"}]
        assert (
            rows[1]["author"] == "original_poster"
            and rows[1]["repost_context"] == "Tracked reposter reposted"
        )
        assert rows[1]["url"] == "https://x.com/original_poster/status/99"
    finally:
        await reader.close()


@pytest.mark.asyncio
async def test_milestone_producer_drives_source_cadence_and_cancellation(tmp_path, monkeypatch):
    from datetime import timedelta

    import tank.sources as sources_module
    from tank.event_tools import EventToolService
    from tank.settings import Settings
    from tank.storage import MemoryRepository
    from tank.util import iso, utcnow

    repo = MemoryRepository()
    await repo.open()
    clock = [utcnow()]
    monkeypatch.setattr(sources_module, "utcnow", lambda: clock[0])
    monkeypatch.setattr(sources_module, "iso", lambda value=None: iso(clock[0] if value is None else value))
    source = {
        "source_id": "S15",
        "name": "Official source",
        "interval_s": 600,
        "feeds": [{"url": "https://example.org/rss", "kind": "rss", "interval_s": 600}],
    }
    settings_obj = Settings(data_dir=tmp_path)
    original_load = settings_obj.load_config

    # Keep the real event-tool configuration while supplying a tiny read-only HTTP source.
    class SourceSettings:
        data_dir = tmp_path
        user_agent = "Tank test"
        config_version = "test"
        code_version = "test"
        x_list_id = None

        def load_config(self, name):
            return {"sources": [source]} if name == "sources" else original_load(name)

    calls = []

    def response(request):
        calls.append(clock[0])
        return httpx.Response(200, content=RSS)

    http = httpx.AsyncClient(transport=httpx.MockTransport(response))
    service = SourceService(repo, SourceSettings(), http=http)
    tools = EventToolService(repo, settings_obj, None, None)
    await repo.put("reg.sources", "S15", source)
    await repo.put("evt.events", "cadence-event", {"event_id": "cadence-event"})
    due = clock[0] + timedelta(hours=1)
    await tools.schedule_milestone(
        {"event_id": "cadence-event", "due_at": iso(due), "source": "S15", "milestone": "LAUNCH"}, {}
    )
    assert not await service.is_escalated("S15")
    await service.control_once("S15")
    assert len(calls) == 1
    clock[0] += timedelta(seconds=15)
    await service.control_once("S15")
    assert len(calls) == 1  # normal600s cadence despite controltick
    clock[0] = due - timedelta(minutes=5)
    await service.control_once("S15")
    assert len(calls) == 2 and await service.is_escalated("S15")
    clock[0] += timedelta(seconds=14)
    await service.control_once("S15")
    assert len(calls) == 2
    clock[0] += timedelta(seconds=1)
    await service.control_once("S15")
    assert len(calls) == 3
    rows = await repo.list("ops.source_escalations", filters={"source_id": "S15"})
    assert rows  # actual EventToolService producer, not a hand-crafted consumer-only row
    for row in rows:
        row["status"] = "CONFIRMED"
        row["confirmed_at"] = iso(clock[0])
        await repo.put(
            "ops.source_escalations",
            row.get("escalation_id")
            or row.get("key")
            or row["source_id"] + ":" + row["event_id"] + ":" + row["trigger"],
            row,
        )
    assert not await service.is_escalated("S15")
    clock[0] += timedelta(seconds=15)
    await service.control_once("S15")
    assert len(calls) == 3
    clock[0] = due + timedelta(hours=2)
    assert not await service.is_escalated("S15")
    await http.aclose()
    await repo.close()


@pytest.mark.asyncio
async def test_run_controls_wake_within_15s_for_600s_source_without_extra_reads(tmp_path, monkeypatch):
    import asyncio

    import tank.sources as sources_module
    from tank.util import iso

    repo = Repo()
    source = {
        "source_id": "S15",
        "interval_s": 600,
        "feeds": [{"url": "https://example.org/rss", "kind": "rss", "interval_s": 600}],
    }
    await repo.put("ops.source_checkpoints", "S15:0", {"last_success_at": iso(), "status": "HEALTHY"})
    calls = []
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: calls.append(request) or httpx.Response(200, content=RSS)
        )
    )
    service = SourceService(repo, settings(tmp_path, [source]), http=http)
    stop = asyncio.Event()
    timeouts = []

    async def wait(awaitable, *, timeout):
        timeouts.append(timeout)
        stop.set()
        return await awaitable

    monkeypatch.setattr(sources_module.asyncio, "wait_for", wait)
    await service.run(stop)
    assert len(timeouts) == 1 and 0 < timeouts[0] <= 15
    assert not calls
    await http.aclose()


@pytest.mark.asyncio
async def test_major_assessment_producer_activates_15s_source_reads(tmp_path, monkeypatch):
    from datetime import timedelta

    import tank.event_tools as event_tools_module
    import tank.sources as sources_module
    from tank.event_tools import EventToolService
    from tank.settings import Settings
    from tank.storage import MemoryRepository
    from tank.util import iso, utcnow

    clock = [utcnow()]
    monkeypatch.setattr(sources_module, "utcnow", lambda: clock[0])
    monkeypatch.setattr(event_tools_module, "utcnow", lambda: clock[0])
    monkeypatch.setattr(sources_module, "iso", lambda value=None: iso(clock[0] if value is None else value))
    repo = MemoryRepository()
    await repo.open()
    config = Settings(data_dir=tmp_path)
    registry = config.load_config("registry")
    token = next(t for t in registry["tokens"] if t["symbol"] == "UNI")
    calls = []
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: calls.append(clock[0]) or httpx.Response(200, content=RSS)
        )
    )
    service = SourceService(repo, config, http=http)
    service.sources["S15"] = {
        "source_id": "S15",
        "interval_s": 600,
        "feeds": [{"kind": "rss", "url": "https://example.org/feed", "interval_s": 600}],
    }
    await service.control_once("S15")
    assert len(calls) == 1
    event = {
        "event_id": "major-cadence",
        "certainty": "HIGH",
        "state_verified": True,
        "event_state": "LIVE",
        "tokens_mentioned": [token["token_id"]],
        "entities": [{"entity_id": token["entity_id"]}],
        "lifecycle": "TRACKED",
        "event_time": {"value": iso(clock[0]), "precision": "EXACT"},
        "public_at": iso(clock[0]),
        "known_to_us_at": iso(clock[0]),
    }
    await repo.put("evt.events", event["event_id"], event)
    quote = "Binance listed the protocol token"
    await repo.put(
        "evt.claims",
        "major-proof",
        {
            "claim_id": "major-proof",
            "event_id": event["event_id"],
            "accepted": True,
            "state_claim": "LIVE",
            "evidence": [{"quote": quote}],
        },
    )
    dimensions = {
        name: {
            "value": None,
            "rationale": "Not estimated from this evidence",
            "evidence": [],
            "reason": "NOT_ESTIMABLE",
        }
        for name in [
            "economic_directness",
            "magnitude",
            "permanence",
            "distribution_expansion",
            "mechanical_flows",
            "surprise",
            "implementation",
        ]
    }
    tools = EventToolService(repo, config, None, None)
    result = await tools.set_assessment(
        {
            "event_id": event["event_id"],
            "assessment": {"class": "MAJOR", "criterion": "M1", "criterion_quote": quote, "dims": dimensions},
        },
        {},
    )
    assert result["class"] == "MAJOR" and await service.is_escalated("S15")
    clock[0] += timedelta(seconds=14)
    await service.control_once("S15")
    assert len(calls) == 1
    clock[0] += timedelta(seconds=1)
    await service.control_once("S15")
    assert len(calls) == 2
    clock[0] += timedelta(hours=2)
    assert not await service.is_escalated("S15")
    await http.aclose()
    await repo.close()


@pytest.mark.asyncio
async def test_control_ticks_do_not_overpoll_failed_normal_cadence(tmp_path, monkeypatch):
    from datetime import timedelta

    import tank.sources as sources_module
    from tank.util import iso, utcnow

    clock = [utcnow()]
    monkeypatch.setattr(sources_module, "utcnow", lambda: clock[0])
    monkeypatch.setattr(sources_module, "iso", lambda value=None: iso(clock[0] if value is None else value))
    source = {
        "source_id": "S15",
        "interval_s": 600,
        "feeds": [{"url": "https://example.org/broken", "kind": "rss", "interval_s": 600}],
    }
    repo = Repo()
    calls = []
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: calls.append(request) or httpx.Response(200, text="<html>Broken RSS</html>")
        )
    )
    service = SourceService(repo, settings(tmp_path, [source]), http=http)
    assert (await service.control_once("S15"))["status"] == "DEGRADED"
    assert len(calls) == 1
    clock[0] += timedelta(seconds=15)
    await service.control_once("S15")
    assert len(calls) == 1
    clock[0] += timedelta(seconds=585)
    await service.control_once("S15")
    assert len(calls) == 2
    await http.aclose()


@pytest.mark.asyncio
async def test_x_storage_state_seeds_persistent_profile_once_without_remote_reads(tmp_path):
    import os
    import time

    if os.getenv("TANK_BROWSER_TESTS") != "1":
        pytest.skip("Requires TANK_BROWSER_TESTS=1")
    from tank.sources.xsensor import XSensor as BrowserReader

    secret = tmp_path / "synthetic-x-secret.json"
    profile = tmp_path / "profile"
    secret.write_text(
        json.dumps(
            {
                "cookies": [
                    {
                        "name": "auth_token",
                        "value": "synthetic-auth-only",
                        "domain": ".x.com",
                        "path": "/",
                        "expires": time.time() + 86400,
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    },
                    {
                        "name": "unrelated_private_cookie",
                        "value": "must-not-import",
                        "domain": ".unrelated.example",
                        "path": "/",
                        "expires": time.time() + 86400,
                        "secure": True,
                        "sameSite": "Lax",
                    },
                ],
                "origins": [
                    {
                        "origin": "https://x.com",
                        "localStorage": [{"name": "synthetic-key", "value": "synthetic-local-only"}],
                    },
                    {
                        "origin": "https://unrelated.example",
                        "localStorage": [{"name": "do-not-import", "value": "other-origin"}],
                    },
                ],
            }
        )
    )
    config = SimpleNamespace(x_profile_dir=profile, x_storage_state_path=secret)
    reader = BrowserReader(config)
    try:
        await reader.open()
        cookies = await reader.context.cookies()
        assert [(c["name"], c["value"]) for c in cookies] == [("auth_token", "synthetic-auth-only")]
        page = await reader.context.new_page()
        await page.route(
            "**/*", lambda route: route.fulfill(status=200, content_type="text/html", body="<html></html>")
        )
        await page.goto("https://x.com")
        assert await page.evaluate("localStorage.getItem('synthetic-key')") == "synthetic-local-only"
        await page.close()
        marker = profile / ".tank-x-session-seeded.json"
        assert set(json.loads(marker.read_text())) == {"version", "seeded_at"}
        assert marker.stat().st_mode & 0o777 == 0o600
        assert profile.stat().st_mode & 0o777 == 0o700
    finally:
        await reader.close()
    # A persistent seeded profile must not read/reapply the secret on restart.
    secret.write_text("INVALID SECRET MUST NOT BE READ AFTER INITIALIZATION")
    reader = BrowserReader(config)
    try:
        await reader.open()
        assert any(
            c["name"] == "auth_token" and c["value"] == "synthetic-auth-only"
            for c in await reader.context.cookies()
        )
        page = await reader.context.new_page()
        await page.route(
            "**/*", lambda route: route.fulfill(status=200, content_type="text/html", body="<html></html>")
        )
        await page.goto("https://x.com")
        assert await page.evaluate("localStorage.getItem('synthetic-key')") == "synthetic-local-only"
        await page.close()
    finally:
        await reader.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state",
    [
        {
            "cookies": [
                {
                    "name": "auth_token",
                    "value": "NEVER_PRINT_TEST_SECRET",
                    "domain": ".x.com",
                    "path": "/",
                    "expires": 1,
                }
            ]
        },
        {
            "cookies": [
                {
                    "name": "auth_token",
                    "value": "NEVER_PRINT_TEST_SECRET",
                    "domain": ".unrelated.example",
                    "path": "/",
                    "expires": -1,
                }
            ]
        },
        {"cookies": "NEVER_PRINT_TEST_SECRET"},
    ],
)
async def test_invalid_expired_x_bootstrap_fails_without_secret_leak(tmp_path, state):
    import os

    if os.getenv("TANK_BROWSER_TESTS") != "1":
        pytest.skip("Requires TANK_BROWSER_TESTS=1")
    from tank.sources.xsensor import XSensor as BrowserReader

    secret = tmp_path / "secret"
    secret.write_text(json.dumps(state))
    profile = tmp_path / "profile"
    reader = BrowserReader(SimpleNamespace(x_profile_dir=profile, x_storage_state_path=secret))
    with pytest.raises(ValueError, match="invalid, expired, or unreadable") as error:
        await reader.open()
    assert "NEVER_PRINT_TEST_SECRET" not in str(error.value)
    assert not (profile / ".tank-x-session-seeded.json").exists()
    assert reader.context is None and reader.driver is None


@pytest.mark.asyncio
async def test_x_media_empty_quotes_repost_priority_raw_html_and_deleted_checkpoint(tmp_path):
    import os

    if os.getenv("TANK_BROWSER_TESTS") != "1":
        pytest.skip("Requires TANK_BROWSER_TESTS=1")
    from pathlib import Path

    from tank.sources.xsensor import XSensor as BrowserReader

    repo = Repo()
    config = settings(
        tmp_path,
        [
            {
                "source_id": "S29",
                "name": "X",
                "feeds": [{"kind": "x", "url": "https://x.com/i/lists/synthetic"}],
                "list_id": "synthetic",
            }
        ],
    )
    config.x_profile_dir = tmp_path / "synthetic-profile"
    config.x_storage_state_path = tmp_path / "absent-secret"
    svc = SourceService(repo, config)
    svc.accounts = {
        "official_org": {"role": "ORGANIZATION", "entity_id": "uniswap"},
        "other_account": {"role": "OTHER", "entity_id": "other"},
    }
    await repo.put(
        "ops.source_checkpoints", "S29:0", {"status_id": "777-deleted", "recent_status_ids": ["98"]}
    )
    html = """<html><body><article><div data-testid="socialContext"><a href="/official_org">Official reposted</a></div><a href="/media_original/status/100"><time datetime="2026-10-17T12:00:00Z"></time></a><div data-testid="tweetPhoto"><img alt="Protocol diagram" src="/synthetic.png"></div><div role="link"><a href="/quoted_q/status/51">Quoted author</a><div data-testid="tweetText">Shown quote text</div></div></article><article><div data-testid="socialContext"><a href="/other_account">Other reposted</a></div><a href="/original_poster/status/99"><time datetime="2026-10-17T11:00:00Z"></time></a><div data-testid="tweetText">Actual original text</div><a href="/empty_quote/status/50">Quoted media</a></article><article><a href="/old/status/98"><time datetime="2026-10-17T10:00:00Z"></time></a><div data-testid="tweetText">Old known post</div></article></body></html>"""
    quote = '<article><a href="/empty_quote/status/50"><time datetime="2026-10-16T12:00:00Z"></time></a><div data-testid="tweetPhoto"><img alt="Quote diagram"></div></article>'
    svc.xsensor = BrowserReader(config, raw_capture=svc.raw)
    try:
        await svc.xsensor.open()

        async def route(r):
            if "/i/lists/synthetic" in r.request.url:
                await r.fulfill(status=200, content_type="text/html", body=html)
            elif "/empty_quote/status/50" in r.request.url:
                await r.fulfill(status=200, content_type="text/html", body=quote)
            elif "/quoted_q/status/51" in r.request.url:
                await r.fulfill(
                    status=200,
                    content_type="text/html",
                    body='<article><a href="/quoted_q/status/51"><time datetime="2026-10-16T12:00:00Z"></time></a><div data-testid="tweetText">Full quoted text with its original author</div></article>',
                )
            else:
                await r.abort()

        await svc.xsensor.context.route("**/*", route)
        result = await svc.poll_once("S29")
        assert result["status"] == "HEALTHY" and result["items"] == 2
        records = [v for (c, _), v in repo.rows.items() if c == "ingest.source_items"]
        media = next(r for r in records if r["external_id"] == "100")
        other = next(r for r in records if r["external_id"] == "99")
        assert (
            media["text"] == ""
            and media["author"] == "media_original"
            and media["monitored_author"] == "official_org"
        )
        assert media["relevance"]["decision"] == "ALWAYS" and media["author_role"] == "ORGANIZATION"
        assert media["quoted_posts"] == [
            {"status_id": "51", "author": "quoted_q", "text": "Full quoted text with its original author"}
        ]
        assert other["author_role"] == "OTHER" and other["deferrable"]
        assert other["quoted_unavailable"] == [{"status_id": "50", "reason": "MEDIA_ONLY_OR_EMPTY"}]
        raw = zstandard.ZstdDecompressor().decompress(Path(media["raw_ref"]).read_bytes())
        assert raw.startswith(b"<html>") and b"Protocol diagram" in raw
        assert (await repo.get("ops.source_checkpoints", "S29:0"))["status_id"] == "100"
        assert [(args[0], kw["priority"]) for args, kw in repo.jobs] == [
            ("ROUTE_ITEM", 1),
            ("FILTER_ITEM", 2),
        ]
    finally:
        await svc.close()


def test_sec_filing_identity_boundaries_and_date_only_precision(tmp_path):
    from tank.settings import Settings

    svc = SourceService(Repo(), Settings(data_dir=tmp_path))
    assert not svc.filing_matches({"title": "Linear Manufacturing S-1", "text": "Filed with SEC"})
    assert not svc.filing_matches(
        {"title": "Quantitative Research company", "text": "Near the registered office"}
    )
    assert svc.filing_matches({"title": "Uniswap protocol filing", "text": ""})
    assert svc.filing_matches({"title": "Bitcoin Trust ETF", "text": ""})
    assert svc.filing_matches({"title": "Digital Asset Trust", "text": ""})
    row = parsers.json_items(
        {
            "results": [
                {
                    "document_number": "2026-1",
                    "title": "Rule",
                    "abstract": "Digital assets",
                    "html_url": "https://www.federalregister.gov/rule",
                    "publication_date": "2026-10-17",
                }
            ]
        },
        "S03",
    )[0]
    assert row["published_at_precision"] == "DAY"


@pytest.mark.asyncio
async def test_source_config_forums_metadata_and_registry_source_mappings():
    from tank.settings import Settings

    config = Settings()
    sources = {s["source_id"]: s for s in config.load_config("sources")["sources"]}
    forums = sources["S23"]
    assert [f["interval_s"] for f in forums["feeds"]] == [300, 300, 300, 300, 300, 600, 600, 600]
    assert all(
        all(k in s for k in ("bucket", "source_class", "cost_class", "auth", "adapter_status"))
        for s in sources.values()
    )
    assert all(
        s["cost_class"] in {"FREE_OPEN", "FREE_ACCOUNT", "FREE_TIER", "PAID"}
        and s["auth"] in {"NONE", "API_KEY", "OAUTH", "BROWSER_SESSION"}
        and s["adapter_status"] in {"NOT_TESTED", "WORKS", "PARTIAL", "BROKEN", "DEPRECATED"}
        for s in sources.values()
    )
    entities = {e["entity_id"]: e for e in config.load_config("registry")["entities"]}
    for eid, sid in [
        ("binance", "S09"),
        ("coinbase", "S14"),
        ("okx", "S10"),
        ("bybit", "S11"),
        ("kraken", "S12"),
        ("robinhood", "S13"),
        ("sec", "S01"),
        ("cftc", "S04"),
        ("federal-reserve", "S05"),
        ("occ", "S06"),
        ("ofac", "S07"),
        ("fincen", "S03"),
        ("fdic", "S03"),
    ]:
        assert sid in entities[eid]["official_sources"]


@pytest.mark.asyncio
async def test_s09_seen_articles_do_not_refetch_detail(tmp_path):
    calls = []

    def handle(request):
        calls.append(str(request.url))
        if "detail/query" in str(request.url):
            return httpx.Response(
                200, json={"data": {"body": json.dumps({"children": [{"text": "Official release body"}]})}}
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "catalogs": [
                        {"articles": [{"code": "article1", "title": "Release", "releaseDate": 1792238400000}]}
                    ]
                }
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    repo = Repo()
    svc = SourceService(
        repo,
        settings(
            tmp_path,
            [{"source_id": "S09", "feeds": [{"kind": "json", "url": "https://www.binance.com/list"}]}],
        ),
        http=http,
    )
    await svc.poll_once("S09")
    await svc.poll_once("S09")
    assert len([url for url in calls if "detail/query" in url]) == 1
    assert len(repo.jobs) == 1
    await http.aclose()


@pytest.mark.asyncio
async def test_sec_provider_and_source_configured_budgets_are_shared(tmp_path, monkeypatch):
    import tank.sources as module
    from tank.settings import Settings

    calls = []

    async def acquire(key, capacity, window_s, *, weight=1):
        calls.append((key, capacity, window_s, weight))

    monkeypatch.setattr(module.shared_request_budgets, "acquire", acquire)
    service = SourceService(Repo(), Settings(data_dir=tmp_path))
    try:
        await service.acquire_budget({"url": "https://www.sec.gov/news/pressreleases.rss"})
        await service.acquire_budget(
            {"url": "https://www.sec.gov/cgi-bin/browse-edgar", "source_limits": {"rate_per_min": 25}}
        )
        assert [row for row in calls if row[0] == "sec.gov"] == [("sec.gov", 5, 1, 1), ("sec.gov", 5, 1, 1)]
        assert ("www.sec.gov", 25, 60, 1) in calls
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_dtcc_resolves_current_deployment_manifest_without_stale_hash(tmp_path):
    calls = []

    def handle(request):
        calls.append(str(request.url))
        path = request.url.path
        if path == "/press-releases":
            return httpx.Response(
                200,
                text='<script src="/_next/static/chunks/webpack-new.js"></script><script src="/_next/static/chunks/registry-new.js"></script>',
            )
        if "webpack-new" in path:
            return httpx.Response(200, text='"static/chunks/"+e+"."+({900:"abcdef123"})[e]+".js"')
        if "registry-new" in path:
            return httpx.Response(
                200, text="PressReleaseSearchModuleBlock:()=>Promise.all([i.e(900)]).then(i.bind(i,123))"
            )
        if "900.abcdef123" in path:
            return httpx.Response(200, text='settings={apiKey:"public-synthetic-query-token"}')
        return httpx.Response(404)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    svc = SourceService(Repo(), settings(tmp_path, []), http=http)
    feed = {
        "official_index_url": "https://www.dtcc.com/press-releases",
        "bootstrap_script_url": "https://www.dtcc.com/old-hash.js",
        "bootstrap_discovery": True,
    }
    assert await svc.resolve_dtcc_bootstrap(feed, "dynamic") == "public-synthetic-query-token"
    assert len(calls) == 4 and not any("old-hash" in url for url in calls)
    await http.aclose()


@pytest.mark.asyncio
async def test_capture_only_never_enqueues_analysis_and_org_repost_promotes_existing_item(tmp_path):
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    await repo.open()
    config = settings(tmp_path, [])
    config.capture_only = True
    svc = SourceService(repo, config)
    try:
        source = {"source_id": "S29", "name": "X"}
        svc.accounts = {"other": {"role": "OTHER"}, "official": {"role": "ORGANIZATION"}}
        raw_ref, digest = svc.raw(b"<html>source evidence</html>")
        item = {
            "external_id": "same-status",
            "author": "original",
            "monitored_author": "other",
            "title": "",
            "text": "Actual original content",
            "published_at": None,
        }
        first = await svc.ingest(source, item, raw_ref, digest)
        assert first["deferrable"] and not await repo.list("ops.jobs")
        config.capture_only = False
        promoted = await svc.ingest(
            source, dict(item, monitored_author="official", reposted_by="official"), raw_ref, digest
        )
        assert promoted["item_id"] == first["item_id"] and promoted["author"] == "original"
        assert promoted["author_role"] == "ORGANIZATION" and promoted["relevance"]["decision"] == "ALWAYS"
        assert set(promoted["monitored_accounts"]) == {"other", "official"}
        jobs = await repo.list("ops.jobs")
        assert len(jobs) == 1 and jobs[0]["type"] == "ROUTE_ITEM" and jobs[0]["max_attempts"] == 2
    finally:
        await svc.close()
        await repo.close()


@pytest.mark.asyncio
async def test_dtcc_rotated_public_query_key_refreshes_manifest_once(tmp_path, monkeypatch):
    from tank.util import utcnow

    svc = SourceService(Repo(), settings(tmp_path, []))
    feed = {
        "url": "https://search.example.org/rest/search/v2",
        "bootstrap_script_url": "https://www.dtcc.com/historical.js",
        "bootstrap_discovery": True,
        "body": {"numberOfResults": 20},
    }
    svc.dtcc_public_keys[feed["bootstrap_script_url"]] = ("old-public-query-key", utcnow())
    seen = []
    resolved = []

    async def fetch(request, key):
        auth = request["headers"]["Authorization"]
        seen.append(auth)
        if auth.endswith("old-public-query-key"):
            response = httpx.Response(401, request=httpx.Request("POST", feed["url"]))
            raise httpx.HTTPStatusError(
                "Expired anonymous token", request=response.request, response=response
            )
        payload = json.dumps({"totalCount": 0, "results": []}).encode()
        ref, digest = svc.raw(payload)
        return payload, {"raw_ref": ref, "raw_sha256": digest}

    async def resolve(request, key):
        resolved.append(key)
        return "new-public-query-key"

    monkeypatch.setattr(svc, "fetch", fetch)
    monkeypatch.setattr(svc, "resolve_dtcc_bootstrap", resolve)
    try:
        payload, _ = await svc.dtcc_payload(feed, "rotated", {})
        assert json.loads(payload)["results"] == [] and len(resolved) == 1
        assert seen == ["Bearer old-public-query-key", "Bearer new-public-query-key"]
    finally:
        await svc.close()


@pytest.mark.asyncio
async def test_public_renderer_is_isolated_from_x_profile_and_secret(tmp_path):
    import os

    if os.getenv("TANK_BROWSER_TESTS") != "1":
        pytest.skip("Requires TANK_BROWSER_TESTS=1")
    from tank.sources.browser import BrowserReader

    class ForbiddenXSettings:
        def __getattr__(self, name):
            raise AssertionError("Public renderer must never access X settings: " + name)

    renderer = BrowserReader(ForbiddenXSettings())
    try:
        await renderer.open()
        await renderer.context.route(
            "**/*",
            lambda route: route.fulfill(
                status=200,
                content_type="text/html",
                body="<html><article><h1>Public issuer release</h1><p>Actual public text</p></article></html>",
            ),
        )
        payload = await renderer.html("https://public.synthetic.example/release")
        assert parsers.article(payload)["title"] == "Public issuer release"
        assert not await renderer.context.cookies()
    finally:
        await renderer.close()


@pytest.mark.asyncio
async def test_xsensor_has_own_feed_loop_and_service_delegates_s29(tmp_path, monkeypatch):
    import asyncio

    from tank.sources.xsensor import XSensor

    sensor = XSensor(
        SimpleNamespace(
            x_profile_dir=tmp_path / "unused-x-profile", x_storage_state_path=tmp_path / "absent-secret"
        )
    )
    stop = asyncio.Event()
    polls = []

    async def poll():
        polls.append("X")
        stop.set()

    await sensor.run(stop, poll)
    assert polls == ["X"] and sensor.context is None
    source = {"source_id": "S29", "feeds": [{"kind": "x", "url": "https://x.com/i/lists/synthetic"}]}
    service = SourceService(Repo(), settings(tmp_path, [source]))
    stop = asyncio.Event()
    delegated = []

    async def run(event, poll_callback):
        delegated.append((event, callable(poll_callback)))
        event.set()

    monkeypatch.setattr(service.xsensor, "run", run)
    await service.run(stop)
    assert delegated == [(stop, True)]


@pytest.mark.asyncio
async def test_disabled_x_never_reads_network_profile_or_account_list(tmp_path, monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    source = {"source_id": "S29", "feeds": [{"kind": "x", "url": "https://x.com/i/lists/{list_id}"}]}
    config = settings(tmp_path, [source])
    config.x_enabled = False
    service = SourceService(Repo(), config)
    sensor_run = AsyncMock(side_effect=AssertionError("Disabled X must not start"))
    sensor_posts = AsyncMock(side_effect=AssertionError("Disabled X must not poll"))
    monkeypatch.setattr(service.xsensor, "run", sensor_run)
    monkeypatch.setattr(service.xsensor, "posts", sensor_posts)
    result = await service.poll_once("S29")
    assert result["status"] == "DISABLED" and result["reason"] == "X_DISABLED_BY_OPERATOR"
    assert (await service.check())[0]["status"] == "DISABLED"
    assert all(row["status"] == "DISABLED" for row in await service.check(probe=True, check_accounts=True))
    stop = asyncio.Event()
    stop.set()
    await service.run(stop)
    sensor_run.assert_not_called()
    sensor_posts.assert_not_called()
    assert service.repo.rows == {}


@pytest.mark.asyncio
async def test_binance_source_discovery_charges_verified_shared_market_weights(tmp_path, monkeypatch):
    import tank.sources as module
    from tank.settings import Settings

    acquired = []

    async def acquire(key, capacity, window_s, *, weight=1):
        acquired.append((key, capacity, window_s, weight))

    monkeypatch.setattr(module.shared_request_budgets, "acquire", acquire)
    service = SourceService(Repo(), Settings(data_dir=tmp_path))
    try:
        await service.acquire_budget({"url": "https://api.binance.com/api/v3/exchangeInfo"})
        await service.acquire_budget({"url": "https://fapi.binance.com/fapi/v1/exchangeInfo"})
        assert ("binance.spot", 3000, 60, 20) in acquired
        assert ("binance.usdm", 1200, 60, 1) in acquired
        with pytest.raises(ValueError, match="Unconfigured Binance request weight"):
            await service.acquire_budget({"url": "https://api.binance.com/unconfigured-api-path"})
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_shared_weight_budget_large_waiter_does_not_block_fitting_baseline(monkeypatch):
    import asyncio

    import tank.sources.rate_limits as module

    budget = module.SharedRequestBudgets()
    sleep_started = asyncio.Event()
    release_sleep = asyncio.Event()

    async def sleep(delay):
        assert delay > 0
        sleep_started.set()
        await release_sleep.wait()

    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    await budget.acquire("binance.spot", 100, 60, weight=98)
    optional = asyncio.create_task(budget.acquire("binance.spot", 100, 60, weight=50))
    try:
        await asyncio.wait_for(sleep_started.wait(), timeout=1)
        await asyncio.wait_for(budget.acquire("binance.spot", 100, 60, weight=2), timeout=1)
        assert not optional.done()
        assert sum(weight for _, weight in budget.windows[("binance.spot", 60.0, 100.0)]) == 100
        assert not budget.locks[("binance.spot", 60.0, 100.0)].locked()
    finally:
        optional.cancel()
        with pytest.raises(asyncio.CancelledError):
            await optional


@pytest.mark.asyncio
async def test_missing_publisher_article_keeps_raw_error_and_continues_valid_entries(tmp_path):
    from pathlib import Path

    calls = []

    def handle(request):
        calls.append(request)
        if request.url.path == "/newsroom/":
            if request.headers.get("If-None-Match"):
                return httpx.Response(304)
            return httpx.Response(
                200,
                text='<a href="/newsroom/missing">Missing release</a><a href="/newsroom/valid">Valid release</a>',
                headers={"ETag": '"index-v1"'},
            )
        if request.url.path.endswith("/missing"):
            return httpx.Response(404, text="<html>Publisher removed this article</html>")
        return httpx.Response(
            200, text="<article><h1>Valid publisher release</h1><p>Actual visible evidence.</p></article>"
        )

    repo = Repo()
    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    service = SourceService(
        repo,
        settings(
            tmp_path,
            [
                {
                    "source_id": "S13",
                    "feeds": [
                        {
                            "kind": "html",
                            "url": "https://publisher.example/newsroom/",
                            "article_pattern": "^/newsroom/[a-z]+$",
                        }
                    ],
                }
            ],
        ),
        http=http,
    )
    try:
        result = await service.poll_once("S13")
        assert result["status"] == "DEGRADED" and result["items"] == 1
        rows = [r for (table, _), r in repo.rows.items() if table == "ingest.source_items"]
        assert len(rows) == 1 and rows[0]["title"] == "Valid publisher release"
        state = await repo.get("ops.source_checkpoints", "S13:0")
        failure = state["article_errors"]["https://publisher.example/newsroom/missing"]
        assert state["status"] == "DEGRADED" and failure["http_status"] == 404
        assert (
            zstandard.ZstdDecompressor().decompress(Path(failure["raw_ref"]).read_bytes())
            == b"<html>Publisher removed this article</html>"
        )
        assert not await repo.get(
            "ops.source_dedup",
            __import__("tank.util", fromlist=["sha256"]).sha256(
                "S13|https://publisher.example/newsroom/missing"
            ),
        )
        assert len([r for r in calls if r.url.path.endswith("/missing")]) == 1
        await service.poll_once("S13")
        assert len([r for r in calls if r.url.path.endswith("/missing")]) == 2
        assert all(not r.headers.get("If-None-Match") for r in calls if r.url.path == "/newsroom/")
        assert len([r for r in calls if r.url.path.endswith("/valid")]) == 1
    finally:
        await http.aclose()
        await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("empty_body", [b"", b"<html><script>hydration()</script></html>"])
async def test_empty_200_article_isolated_without_suppressing_valid_sibling(tmp_path, empty_body):
    from pathlib import Path

    def handle(request):
        if request.url.path == "/newsroom/":
            return httpx.Response(
                200, text='<a href="/newsroom/empty">Empty entry</a><a href="/newsroom/valid">Valid entry</a>'
            )
        if request.url.path.endswith("/empty"):
            return httpx.Response(200, content=empty_body)
        return httpx.Response(
            200, text="<article><h1>Actual release</h1><p>Actual visible evidence.</p></article>"
        )

    repo = Repo()
    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    service = SourceService(
        repo,
        settings(
            tmp_path,
            [
                {
                    "source_id": "S13",
                    "feeds": [
                        {
                            "kind": "html",
                            "url": "https://publisher.example/newsroom/",
                            "article_pattern": "^/newsroom/[a-z]+$",
                        }
                    ],
                }
            ],
        ),
        http=http,
    )
    try:
        result = await service.poll_once("S13")
        assert result["status"] == "DEGRADED" and result["items"] == 1
        state = await repo.get("ops.source_checkpoints", "S13:0")
        error = state["article_errors"]["https://publisher.example/newsroom/empty"]
        assert error["reason"] == "EMPTY_ARTICLE_BODY" and error["http_status"] == 200
        assert zstandard.ZstdDecompressor().decompress(Path(error["raw_ref"]).read_bytes()) == empty_body
        assert not await repo.get(
            "ops.source_dedup",
            __import__("tank.util", fromlist=["sha256"]).sha256(
                "S13|https://publisher.example/newsroom/empty"
            ),
        )
        assert len([r for (table, _), r in repo.rows.items() if table == "ingest.source_items"]) == 1
    finally:
        await http.aclose()
        await service.close()


@pytest.mark.asyncio
async def test_seen_full_article_empty_index_deduplicates_before_new_content_guard(tmp_path):
    phase = [0]
    calls = []

    def handle(request):
        calls.append(request.url.path)
        if request.url.path == "/newsroom/":
            sibling = "" if phase[0] == 0 else '<a href="/newsroom/later">Later entry</a>'
            return httpx.Response(200, text='<a href="/newsroom/known"></a>' + sibling)
        if request.url.path.endswith("/known"):
            return httpx.Response(
                200,
                text="<article><h1>Original full article</h1><p>Original durable full evidence.</p></article>",
            )
        return httpx.Response(
            200, text="<article><h1>Later full article</h1><p>Later actual evidence.</p></article>"
        )

    repo = Repo()
    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    source = {
        "source_id": "S13",
        "feeds": [
            {
                "kind": "html",
                "url": "https://publisher.example/newsroom/",
                "article_pattern": "^/newsroom/[a-z]+$",
            }
        ],
    }
    service = SourceService(repo, settings(tmp_path, [source]), http=http)
    try:
        assert (await service.poll_once("S13"))["status"] == "HEALTHY"
        original = next(r for (table, _), r in repo.rows.items() if table == "ingest.source_items")
        original_hash = original["content_hash"]
        original_text = original["text"]
        phase[0] = 1
        result = await service.poll_once("S13")
        assert result["status"] == "HEALTHY" and result["items"] == 2
        known = await repo.get("ingest.source_items", original["item_id"])
        assert (
            known["text"] == original_text
            and known["content_hash"] == original_hash
            and known["observation_count"] == 2
        )
        assert calls.count("/newsroom/known") == 1 and calls.count("/newsroom/later") == 1
        assert len([r for (table, _), r in repo.rows.items() if table == "ingest.source_items"]) == 2
        with pytest.raises(ValueError, match="no normalized content"):
            await service.ingest(
                source,
                {"external_id": "new-empty", "title": "", "text": ""},
                original["raw_ref"],
                original["raw_sha256"],
            )
        changed = await service.ingest(
            source,
            {
                "external_id": known["external_id"],
                "title": "Changed index title",
                "text": "Changed index summary",
            },
            original["raw_ref"],
            original["raw_sha256"],
        )
        assert changed["content_hash"] == original_hash and changed["text"] == original_text
    finally:
        await http.aclose()
        await service.close()
