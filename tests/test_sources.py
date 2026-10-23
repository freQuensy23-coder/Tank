import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
import zstandard

from tank.sources import SourceService, parsers
from tank.util import new_id, sha256


@pytest.fixture
def collector_http(monkeypatch):
    """Use each test's HTTP fixture as the external collector response boundary.

    SourceService still chooses capture_fetch, parses responses and persists its
    checkpoints/items/raw evidence. Durable capture RPC is tested separately in
    test_external_capture.py and test_fetch_page_deadline.py.
    """
    from tank.sources.capture_fetch import CONDITIONAL_HEADERS, is_exchange_url

    def bind(http):
        async def fetch(repo, method, url, *, headers=None, body=None, source_id=None, budget_s=None):
            assert is_exchange_url(url)
            headers = {
                key: value for key, value in (headers or {}).items() if key.lower() not in CONDITIONAL_HEADERS
            }
            return await http.request(method, url, headers=headers, json=body)

        monkeypatch.setattr("tank.sources.capture_fetch", fetch)

    return bind


def captured(svc, sid, item, payload=b"<html>source evidence</html>", observed_at="2026-10-16T12:00:00Z"):
    """An item as the poller hands it to ingest: raw payload already stored under its item_id."""
    item_id = new_id("item")
    raw_ref, digest = svc.raw(payload, source_id=sid, item_id=item_id, observed_at=observed_at)
    return dict(item, item_id=item_id, raw_ref=raw_ref, raw_sha256=digest, observed_at=observed_at)


RSS = b"""<rss version="2.0"><channel><title>Official</title><item><guid>release-42</guid><title>New protocol</title><link>https://example.org/a</link><pubDate>Fri, 16 Oct 2026 12:00:00 GMT</pubDate><description>&lt;p&gt;Protocol deployment &amp;amp; vote&lt;/p&gt;</description></item></channel></rss>"""


class Repo:
    def __init__(self):
        self.rows = {}
        self.jobs = []

    async def get(self, c, k):
        from copy import deepcopy

        return deepcopy(self.rows.get((c, k)))

    async def put(self, c, k, v, *, immutable=False):
        self.rows[c, k] = dict(v)
        return v

    async def list(self, collection, *, filters=None, limit=1000):
        rows = [
            dict(row)
            for (table, _), row in self.rows.items()
            if table == collection and all(row.get(k) == v for k, v in (filters or {}).items())
        ]
        return rows[:limit]

    async def iter_all(self, collection, *, filters=None, batch_size=1000):
        from copy import deepcopy

        for (table, _), row in sorted(self.rows.items()):
            if table == collection and all(row.get(k) == v for k, v in (filters or {}).items()):
                yield deepcopy(row)

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

    assert (await svc.poll_once("S30"))["items"] == 0
    assert len(repo.jobs) == 1
    assert (await repo.get("ingest.source_items", record["item_id"]))["observation_count"] == 1
    assert [p.name for p in tmp_path.rglob("*.zst")] == [record["item_id"] + ".json.zst"]
    await http.aclose()


@pytest.mark.asyncio
async def test_raw_stored_only_for_captured_items_across_repeated_polls(tmp_path, collector_http):
    """02 §7.2: an unchanged feed or instrument snapshot leaves no raw file behind."""
    phase = [0]

    def handle(request):
        if request.url.host == "api.binance.com":
            status = "TRADING" if phase[0] < 2 else "BREAK"
            return httpx.Response(
                200,
                json={
                    "symbols": [
                        {"symbol": "ABCUSDT", "baseAsset": "ABC", "quoteAsset": "USDT", "status": status}
                    ]
                },
            )
        return httpx.Response(200, content=RSS)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    collector_http(http)
    repo = Repo()
    sources = [
        {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]},
        {
            "source_id": "S14",
            "feeds": [
                {
                    "kind": "instruments",
                    "url": "https://api.binance.com/api/v3/exchangeInfo",
                    "venue": "binance",
                    "market_type": "SPOT",
                }
            ],
        },
    ]
    svc = SourceService(repo, settings(tmp_path, sources), http=http)
    for _ in range(3):
        await svc.poll_once("S30")
        await svc.poll_once("S14")
        phase[0] += 1
    items = [v for (c, k), v in repo.rows.items() if c == "ingest.source_items"]
    assert sorted(i["source_id"] for i in items) == ["S14", "S30"]
    assert sorted(p.name for p in tmp_path.rglob("*.zst")) == sorted(
        i["item_id"] + ".json.zst" for i in items
    )
    change = next(i for i in items if i["source_id"] == "S14")
    assert change["change"] == "STATE_UPDATE" and "BREAK" in change["text"]
    assert all(i["observation_count"] == 1 for i in items)
    await http.aclose()


@pytest.mark.asyncio
async def test_sighting_in_another_feed_counts_one_observation(tmp_path):
    """03 §3.1: the same external_id from a second feed increments the count once."""
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=RSS)))
    repo = Repo()
    source = {
        "source_id": "S30",
        "feeds": [
            {"url": "https://example.org/rss", "kind": "rss"},
            {"url": "https://example.org/rss2", "kind": "rss"},
        ],
    }
    svc = SourceService(repo, settings(tmp_path, [source]), http=http)
    await svc.poll_once("S30")
    await svc.poll_once("S30")
    items = [v for (c, k), v in repo.rows.items() if c == "ingest.source_items"]
    assert len(items) == 1 and items[0]["observation_count"] == 2
    assert len(list(tmp_path.rglob("*.zst"))) == 1
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
async def test_instrument_baseline_diff_and_bybit_cursor(tmp_path, collector_http):
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
    collector_http(http)
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

    assert any(r["external_id"] == "0xold|closed" for r in rows)
    await http.aclose()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_source_ingest_postgres_projection_and_durable_queue(tmp_path, postgres_template):
    """Exercise the actual schema, not a permissive in-memory record store."""
    import os
    from pathlib import Path

    from tank.util import new_id, sha256

    database_url = os.getenv("TANK_SOURCE_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Requires isolated PostgreSQL source test database")
    async with postgres_template.repository(database_url) as repo:
        http = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=RSS))
        )
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
                item_id = new_id("item")
                raw_ref, digest = svc.raw(
                    key.encode(),
                    source_id=source["source_id"],
                    item_id=item_id,
                    observed_at="2026-10-16T12:00:01Z",
                )
                item = {
                    "item_id": item_id,
                    "raw_ref": raw_ref,
                    "raw_sha256": digest,
                    "observed_at": "2026-10-16T12:00:01Z",
                    "external_id": key,
                    "title": key,
                    "text": "Actual source content",
                    "url": "https://example.org/item",
                    "published_at": "2026-10-16T12:00:00Z",
                    "published_at_precision": "EXACT",
                }
                record = await svc.ingest(source, item)
                assert record["relevance"]["decision"] == expected
                await svc.ingest(source, item)
                async with repo.transaction():
                    row = await (
                        await repo._conn().execute(
                            "SELECT relevance_decision,published_prec,body,raw_path,raw_sha256 FROM ingest.source_items WHERE item_id=%s",
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
                expected_raw_path = (
                    tmp_path.resolve()
                    / "raw"
                    / "items"
                    / source["source_id"]
                    / record["observed_at"][:10].replace("-", "/")
                    / (record["item_id"] + ".json.zst")
                )
                assert Path(row["raw_path"]) == expected_raw_path
                assert row["raw_path"] == record["raw_ref"] == raw_ref
                assert zstandard.ZstdDecompressor().decompress(expected_raw_path.read_bytes()) == key.encode()
                assert row["raw_sha256"] == record["raw_sha256"] == digest == sha256(key.encode())
                assert jobs["n"] == 1
                assert (await repo.get("ingest.source_items", record["item_id"]))["observation_count"] == 1
        finally:
            await http.aclose()


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
                {"source_id": "S29", "feeds": [{"url": "https://api.apify.example/run-sync", "kind": "x"}]},
            ],
        ),
        http=http,
    )
    results = await svc.check(probe=True)
    assert [r["status"] for r in results] == ["PASS", "HTTP_ERROR", "UNAVAILABLE", "UNAVAILABLE"]
    assert results[0]["item_count"] == 1 and results[1]["http_status"] == 403
    assert len(calls) == 2 and not repo.jobs and not repo.rows
    assert results[3]["reason"] == "X runs through a paid Apify actor run; excluded from the HTTP smoke"
    assert results[0]["payload_sha256"] == sha256(RSS) and not list(tmp_path.rglob("*.zst"))

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


@pytest.mark.integration
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
    assert len(calls) == 1
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
    assert rows
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


@pytest.mark.integration
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
            "assessment": {
                "class": "MAJOR",
                "criterion": "M1",
                "criterion_venue": "binance",
                "criterion_quote": quote,
                "dims": dimensions,
            },
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
    assert row["published_at_precision"] == "BUCKET(1d)"


@pytest.mark.asyncio
async def test_s09_seen_articles_do_not_refetch_detail(tmp_path, collector_http):
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
    collector_http(http)
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

    async def acquire(key, capacity, window_s, *, weight=1, host=None):
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


@pytest.mark.browser
@pytest.mark.integration
@pytest.mark.asyncio
async def test_public_renderer_is_isolated_from_x_settings(tmp_path):
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
async def test_binance_source_discovery_leaves_shared_market_weights_to_capture(tmp_path, monkeypatch):
    import tank.sources as module
    from tank.settings import Settings

    acquired = []

    async def acquire(key, capacity, window_s, *, weight=1, host=None):
        acquired.append((key, capacity, window_s, weight))

    monkeypatch.setattr(module.shared_request_budgets, "acquire", acquire)
    service = SourceService(Repo(), Settings(data_dir=tmp_path))
    try:
        for url in (
            "https://api.binance.com/api/v3/exchangeInfo",
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
            "https://api.binance.com/unconfigured-api-path",
        ):
            await service.acquire_budget({"url": url})
        assert acquired == []
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
                    "source_id": "S20",
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
        result = await service.poll_once("S20")
        assert result["status"] == "DEGRADED" and result["items"] == 1
        rows = [r for (table, _), r in repo.rows.items() if table == "ingest.source_items"]
        assert len(rows) == 1 and rows[0]["title"] == "Valid publisher release"
        state = await repo.get("ops.source_checkpoints", "S20:0")
        failure = state["article_errors"]["https://publisher.example/newsroom/missing"]
        assert state["status"] == "DEGRADED" and failure["http_status"] == 404
        assert failure["payload_sha256"] == sha256(b"<html>Publisher removed this article</html>")
        assert len(list(Path(tmp_path).rglob("*.zst"))) == 1
        assert not await service.captured(
            source_id="S20", external_id="https://publisher.example/newsroom/missing"
        )
        assert len([r for r in calls if r.url.path.endswith("/missing")]) == 1
        await service.poll_once("S20")
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
                    "source_id": "S20",
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
        result = await service.poll_once("S20")
        assert result["status"] == "DEGRADED" and result["items"] == 1
        state = await repo.get("ops.source_checkpoints", "S20:0")
        error = state["article_errors"]["https://publisher.example/newsroom/empty"]
        assert error["reason"] == "EMPTY_ARTICLE_BODY" and error["http_status"] == 200
        assert error["payload_sha256"] == sha256(empty_body)
        assert len(list(Path(tmp_path).rglob("*.zst"))) == 1
        assert not await service.captured(
            source_id="S20", external_id="https://publisher.example/newsroom/empty"
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
        "source_id": "S20",
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
        assert (await service.poll_once("S20"))["status"] == "HEALTHY"
        original = next(r for (table, _), r in repo.rows.items() if table == "ingest.source_items")
        original_hash = original["content_hash"]
        original_text = original["text"]
        phase[0] = 1
        result = await service.poll_once("S20")
        assert result["status"] == "HEALTHY" and result["items"] == 1
        known = await repo.get("ingest.source_items", original["item_id"])
        assert known == original
        assert calls.count("/newsroom/known") == 1 and calls.count("/newsroom/later") == 1
        assert len([r for (table, _), r in repo.rows.items() if table == "ingest.source_items"]) == 2
        with pytest.raises(ValueError, match="no normalized content"):
            await service.ingest(
                source, captured(service, "S20", {"external_id": "new-empty", "title": "", "text": ""})
            )
        changed = await service.ingest(
            source,
            captured(
                service,
                "S20",
                {
                    "external_id": known["external_id"],
                    "title": "Changed index title",
                    "text": "Changed index summary",
                },
            ),
        )
        assert changed["content_hash"] == original_hash and changed["text"] == original_text
    finally:
        await http.aclose()
        await service.close()


@pytest.mark.asyncio
async def test_declared_author_organization_precedence_and_identity_without_body_name(tmp_path):
    from tank.agents.pipeline import Pipeline
    from tank.settings import Settings

    repo = Repo()
    config = Settings(data_dir=tmp_path)
    service = SourceService(repo, config)
    source = {
        "source_id": "S01",
        "name": "SEC",
        "author_organization": "SEC",
        "feeds": [{"url": "https://www.sec.gov/synthetic", "author_organization": "CFTC"}],
    }
    payload = b"<article>Actual synthetic test evidence</article>"
    try:
        for external, item_org, feed, expected in [
            ("item", "Circle", "https://www.sec.gov/synthetic", "Circle"),
            ("feed", None, "https://www.sec.gov/synthetic", "CFTC"),
            ("source", None, "https://www.sec.gov/other", "SEC"),
        ]:
            item = {
                "external_id": external,
                "feed": feed,
                "title": external + " custody guidance",
                "text": "Custody standards updated",
                "author_organization": item_org,
            }
            record = await service.ingest(source, captured(service, "S01", item, payload))
            assert record["author_organization"] == expected
            if external == "source":
                assert "sec" in Pipeline(repo, config, http=object()).identities(record)
        news = await service.ingest(
            {"source_id": "S30", "name": "BBC News"},
            captured(
                service,
                "S30",
                {"external_id": "news", "title": "Separate custody guidance", "text": "Standards changed"},
                payload,
            ),
        )
        assert news["author"] == "BBC News" and news["author_organization"] is None
        assert "sec" not in Pipeline(repo, config, http=object()).identities(news)
    finally:
        await service.close()


def test_wire_timestamp_precision_survives_normalization():
    row = parsers.article(
        b'<article><h1>Release</h1><time datetime="2026-10-18"></time><p>Body</p></article>'
    )
    assert row["published_at_precision"] == "BUCKET(1d)"
    dated = RSS.replace(b"Fri, 16 Oct 2026 12:00:00 GMT", b"2026-10-16")
    assert (
        parsers.feeds(dated, "S01", {"url": "https://example.org"})[0]["published_at_precision"]
        == "BUCKET(1d)"
    )
    assert parsers.feeds(RSS, "S01", {"url": "https://example.org"})[0]["published_at_precision"] == "EXACT"
    assert parsers.timestamp_fields(None)["published_at_precision"] == "COLLECTION_ONLY"


@pytest.mark.asyncio
async def test_failed_instrument_write_retries_unadvanced_snapshot(tmp_path, collector_http):
    phase = ["Trading"]
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(
            200,
            headers={"ETag": phase[0]},
            json={
                "retCode": 0,
                "result": {
                    "list": [
                        {"symbol": "ABCUSDT", "baseCoin": "ABC", "quoteCoin": "USDT", "status": phase[0]}
                    ],
                    "nextPageCursor": "",
                },
            },
        )

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
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        collector_http(http)
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        await svc.poll_once("S14")
        phase[0] = "Settling"
        original = svc.ingest

        async def fail(*args):
            raise RuntimeError("storage unavailable")

        svc.ingest = fail
        assert (await svc.poll_once("S14"))["status"] == "DEGRADED"
        checkpoint = repo.rows["ops.source_checkpoints", "S14:0"]

        pending = [row for batch in checkpoint["pending_batches"] for row in batch["rows"]]
        assert [row["change"] for row in pending] == ["STATE_UPDATE"] and "Settling" in pending[0]["text"]
        assert "etag" not in checkpoint and "instruments" not in checkpoint
        snapshot = repo.rows["ops.source_checkpoints", "S14:0:instruments"]
        assert snapshot["instruments"]["ABCUSDT"]["status"] == "Settling"
        svc.ingest = original
        assert (await svc.poll_once("S14"))["items"] == 1
        assert "if-none-match" not in requests[-1].headers


@pytest.mark.asyncio
async def test_content_duplicate_counts_once_and_is_remembered_by_the_feed(tmp_path):
    repo = Repo()
    svc = SourceService(repo, settings(tmp_path, []))
    first = await svc.ingest(
        {"source_id": "S01"}, captured(svc, "S01", {"external_id": "a", "title": "Same", "text": "Release"})
    )
    duplicate = await svc.ingest(
        {"source_id": "S04"}, captured(svc, "S04", {"external_id": "b", "title": "Same", "text": "Release"})
    )
    assert first["item_id"] == duplicate["item_id"] and duplicate["observation_count"] == 2
    assert len([key for key in repo.rows if key[0] == "ingest.source_items"]) == 1

    assert [p.name for p in tmp_path.rglob("*.zst")] == [first["item_id"] + ".json.zst"]
    await svc.close()

    republished = RSS.replace(
        b"</channel>",
        b"<item><guid>release-42-copy</guid><title>New protocol</title><link>https://example.org/a</link>"
        b"<pubDate>Fri, 16 Oct 2026 12:00:00 GMT</pubDate><description>Protocol deployment &amp; vote</description></item></channel>",
    )
    repo = Repo()
    source = {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=republished))
    ) as http:
        svc = SourceService(repo, settings(tmp_path / "feed", [source]), http=http)
        await svc.poll_once("S30")
        state = await repo.get("ops.source_checkpoints", "S30:0")
        assert len(state["duplicate_ids"]) == 1
        await svc.poll_once("S30")
        items = [v for (c, _), v in repo.rows.items() if c == "ingest.source_items"]
        assert len(items) == 1 and items[0]["observation_count"] == 1
        assert len(list((tmp_path / "feed").rglob("*.zst"))) == 1


@pytest.mark.asyncio
async def test_slow_feed_does_not_block_fast_sibling_ingestion(tmp_path):
    import asyncio

    blocked = asyncio.Event()
    release = asyncio.Event()

    async def handle(request):
        if request.url.path == "/slow":
            blocked.set()
            await release.wait()
        return httpx.Response(200, content=RSS)

    source = {
        "source_id": "S30",
        "feeds": [
            {"kind": "rss", "url": "https://example.org/slow"},
            {"kind": "rss", "url": "https://example.org/fast"},
        ],
    }
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        poll = asyncio.create_task(svc.poll_once("S30"))
        await asyncio.wait_for(blocked.wait(), 1)

        async def fast_ingested():
            while not any(c == "ingest.source_items" for c, _ in repo.rows):
                await asyncio.sleep(0)

        await asyncio.wait_for(fast_ingested(), 1)
        assert not poll.done()
        row = next(v for (c, _), v in repo.rows.items() if c == "ingest.source_items")
        assert row["feed"].endswith("/fast")
        release.set()
        assert (await poll)["status"] == "HEALTHY"


@pytest.mark.asyncio
async def test_slow_detail_does_not_block_index_cadence_and_restart_preserves_first_seen(tmp_path):
    import asyncio

    from tank.util import iso, parse_time

    calls = []
    detail_started = asyncio.Event()
    release = asyncio.Event()

    async def handle(request):
        if request.url.path == "/index":
            calls.append(iso())
            return httpx.Response(200, text='<a href="/article/one">Release</a>')
        detail_started.set()
        await release.wait()
        return httpx.Response(200, text="<article><h1>Release</h1><p>Full release evidence</p></article>")

    source = {
        "source_id": "S20",
        "feeds": [
            {
                "kind": "html",
                "url": "https://example.org/index",
                "article_pattern": "^/article/",
                "interval_s": 0.02,
            }
        ],
    }
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        stop = asyncio.Event()
        run = asyncio.create_task(svc.run(stop))
        await asyncio.wait_for(detail_started.wait(), 1)

        async def three_indexes():
            while len(calls) < 3:
                await asyncio.sleep(0.005)

        await asyncio.wait_for(three_indexes(), 1)
        state = await repo.get("ops.source_checkpoints", "S20:0")
        first_seen = state["pending_batches"][0]["rows"][0]["observed_at"]
        assert not any(c == "ingest.source_items" for c, _ in repo.rows)
        assert parse_time(first_seen) < parse_time(calls[-1])
        stop.set()
        await asyncio.wait_for(run, 1)
        release.set()
        restarted = SourceService(repo, settings(tmp_path, [source]), http=http)
        await restarted.poll_once("S20")
        row = next(v for (c, _), v in repo.rows.items() if c == "ingest.source_items")
        assert row["observed_at"] == first_seen

        assert row["observation_count"] == 1
        assert not (await repo.get("ops.source_checkpoints", "S20:0"))["pending_batches"]


@pytest.mark.asyncio
@pytest.mark.parametrize("source_id", ["S03", "S09", "S24"])
async def test_json_discovery_burst_reads_every_page_until_durable_boundary(
    tmp_path, source_id, collector_http
):
    import re

    phase = [0]
    pages = []

    def handle(request):
        if "detail/query" in request.url.path:
            code = request.url.params["articleCode"]
            return httpx.Response(
                200, json={"data": {"body": json.dumps({"children": [{"text": "Body for " + code}]})}}
            )
        body = json.loads(request.content) if request.content else {}
        if source_id == "S24":
            match = re.search(r"\bskip\s*:\s*(\d+)", body["query"])
            offset = int(match.group(1)) if match else 0
        elif source_id == "S09":
            offset = (int(request.url.params.get("pageNo", 1)) - 1) * 2
        else:
            offset = (int(request.url.params.get("page", 1)) - 1) * 2
        pages.append((phase[0], offset))
        all_ids = list(range(1 if phase[0] == 0 else 8, -1, -1))
        ids = all_ids[offset : offset + 2]
        if source_id == "S03":
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "document_number": str(i),
                            "title": "Release " + str(i),
                            "abstract": "Body " + str(i),
                            "html_url": "https://example.org/doc/" + str(i),
                            "publication_date": "2026-10-18",
                        }
                        for i in ids
                    ],
                    "next_page_url": "https://example.org/list?per_page=2&page=" + str(offset // 2 + 2)
                    if offset + 2 < len(all_ids)
                    else None,
                },
            )
        if source_id == "S09":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "total": len(all_ids),
                        "articles": [
                            {"code": str(i), "title": "Release " + str(i), "releaseDate": 1792238400000 + i}
                            for i in ids
                        ],
                    }
                },
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "proposals": [
                        {
                            "id": str(i),
                            "title": "Proposal " + str(i),
                            "body": "Body " + str(i),
                            "state": "closed",
                            "start": 1792238400,
                            "end": 1792248400,
                            "created": 1792238300 + i,
                            "scores": [i],
                            "scores_total": i,
                            "space": {"id": "configured.eth"},
                        }
                        for i in ids
                    ]
                }
            },
        )

    feed = {"url": "https://example.org/list?per_page=2", "kind": "json"}
    if source_id == "S09":
        feed["url"] = "https://www.binance.com/list?pageNo=1&pageSize=2"
    elif source_id == "S24":
        feed.update(
            method="POST",
            body={
                "query": 'query { proposals(first: 2, where: {space_in: ["configured.eth"]}, orderBy: "created", orderDirection: desc) { id title body state start end created scores scores_total space { id } } }'
            },
        )
    source = {"source_id": source_id, "feeds": [feed]}
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        collector_http(http)
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        assert (await svc.poll_once(source_id))["status"] == "HEALTHY"
        assert (await repo.get("ops.source_checkpoints", source_id + ":0"))["discovery_checkpoint"] == "1"
        phase[0] = 1
        assert (await svc.poll_once(source_id))["status"] == "HEALTHY"
        rows = [row for (collection, _), row in repo.rows.items() if collection == "ingest.source_items"]
        assert len(rows) == 9
        assert {row.get("proposal_id", row["external_id"]) for row in rows} == {str(i) for i in range(9)}
        assert [offset for phase, offset in pages if phase == 1] == [0, 2, 4, 6]
        state = await repo.get("ops.source_checkpoints", source_id + ":0")
        assert state["discovery_checkpoint"] == "8"
        assert not state["pending_batches"]


@pytest.mark.asyncio
async def test_failed_later_history_page_retains_earlier_items_without_advancing_boundary(tmp_path):
    phase = [0]

    def handle(request):
        if phase[0] == 1 and request.url.params.get("page") == "2":
            return httpx.Response(404, text="Publisher page unavailable")
        ids = [1] if phase[0] == 0 else [3, 2] if request.url.params.get("page") != "2" else [1]
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "document_number": str(i),
                        "title": "Release " + str(i),
                        "abstract": "Body " + str(i),
                        "html_url": "https://example.org/doc/" + str(i),
                        "publication_date": "2026-10-18",
                    }
                    for i in ids
                ],
                "next_page_url": "https://example.org/list?page=2"
                if phase[0] and request.url.params.get("page") != "2"
                else None,
            },
        )

    source = {"source_id": "S03", "feeds": [{"url": "https://example.org/list", "kind": "json"}]}
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        await svc.poll_once("S03")
        phase[0] = 1
        assert (await svc.poll_once("S03"))["status"] == "DEGRADED"
        rows = [row for (collection, _), row in repo.rows.items() if collection == "ingest.source_items"]
        assert {row["external_id"] for row in rows} == {"1", "2", "3"}
        assert (await repo.get("ops.source_checkpoints", "S03:0"))["discovery_checkpoint"] == "1"
        phase[0] = 2
        assert (await svc.poll_once("S03"))["status"] == "HEALTHY"
        assert (await repo.get("ops.source_checkpoints", "S03:0"))["discovery_checkpoint"] == "3"


@pytest.mark.parametrize("schema_type", ["Article", "BlogPosting", "NewsArticle"])
def test_publication_metadata_extracts_actual_publication_not_modification(schema_type):
    payload = (
        '<article><h1>Actual Release</h1><p>Evidence</p></article><script type="application/ld+json">'
        + json.dumps(
            {
                "@context": "https://schema.org",
                "@graph": [
                    {"@type": "Organization", "foundingDate": "2020-01-01"},
                    {
                        "@type": schema_type,
                        "headline": "Actual Release",
                        "datePublished": "2026-10-17",
                        "dateModified": "2026-10-18T23:00:00Z",
                    },
                ],
            }
        )
        + "</script>"
    )
    row = parsers.article(payload)
    assert row["published_at"] == "2026-10-17T00:00:00Z"
    assert row["published_at_precision"] == "BUCKET(1d)"
    assert row["published_at_basis"] == "ARTICLE_JSON_LD_DATE_PUBLISHED"
    conflicting = payload.replace("</script>", ", invalid json</script>")
    assert parsers.article(conflicting)["published_at"] is None


def test_publication_meta_and_ambiguous_articles_preserve_precision():
    row = parsers.article(
        '<article>Release</article><meta property="article:published_time" content="2026-10-17">'
    )
    assert row["published_at_precision"] == "BUCKET(1d)"
    blackrock = '<article>Release</article><meta name="publicationDate" content="1791936000000">'
    assert parsers.article(blackrock, source_id="S18")["published_at_precision"] == "BUCKET(1d)"
    assert parsers.article(blackrock, source_id="S20")["published_at"] is None
    competing = (
        '<article><h1>Release</h1></article><script type="application/ld+json">'
        + json.dumps(
            [
                {"@type": "Article", "headline": "Other", "datePublished": "2026-10-17"},
                {"@type": "Article", "headline": "Unrelated", "datePublished": "2026-10-18"},
            ]
        )
        + "</script>"
    )
    row = parsers.article(competing)
    assert row["published_at"] is None and row["published_at_missing_reason"] == "UNVERIFIED_SEMANTICS"


@pytest.mark.asyncio
async def test_completed_later_batch_cannot_erase_failed_earlier_coverage(tmp_path):
    phase = [0]
    fail = [False]

    def handle(request):
        ids = [1] if phase[0] == 0 else [2, 1] if phase[0] == 1 else [3, 2, 1]
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "document_number": str(i),
                        "title": "Release " + str(i),
                        "abstract": "Body " + str(i),
                        "html_url": "https://example.org/doc/" + str(i),
                        "publication_date": "2026-10-18",
                    }
                    for i in ids
                ],
                "next_page_url": None,
            },
        )

    source = {"source_id": "S03", "feeds": [{"url": "https://example.org/list", "kind": "json"}]}
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        original = svc.ingest

        async def ingest(*args):
            if fail[0] and args[1]["external_id"] == "2":
                raise RuntimeError("storage failed for one original")
            return await original(*args)

        svc.ingest = ingest
        await svc.poll_once("S03")
        phase[0], fail[0] = 1, True
        assert (await svc.poll_once("S03"))["status"] == "DEGRADED"
        phase[0] = 2
        assert (await svc.poll_once("S03"))["status"] == "DEGRADED"
        rows = [row for (collection, _), row in repo.rows.items() if collection == "ingest.source_items"]
        assert {row["external_id"] for row in rows} == {"1", "3"}
        assert (await repo.get("ops.source_checkpoints", "S03:0"))["discovery_checkpoint"] == "1"
        fail[0] = False
        await svc.poller.drain(source, 0, force=True)
        state = await repo.get("ops.source_checkpoints", "S03:0")
        assert state["discovery_checkpoint"] == "3" and not state["pending_batches"]
        assert len(repo.jobs) == 3


@pytest.mark.asyncio
async def test_conditional_304_does_not_starve_durable_pending_detail(tmp_path):
    from tank.util import iso

    source = {
        "source_id": "S20",
        "feeds": [{"url": "https://example.org/index", "kind": "html", "article_pattern": "^/article/"}],
    }
    repo = Repo()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: (
                httpx.Response(304)
                if request.url.path == "/index"
                else httpx.Response(200, text="<article><h1>Release</h1><p>Body</p></article>")
            )
        )
    ) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        state = {"etag": "older-index", "fetched_at": iso()}
        await svc.poller.queue_rows(
            state,
            "S20:0",
            source,
            source["feeds"][0],
            [
                {
                    "external_id": "https://example.org/article/one",
                    "url": "https://example.org/article/one",
                    "title": "Release",
                    "text": "",
                }
            ],
            "html",
            {},
            "captured-before-restart",
            state["fetched_at"],
        )
        await repo.put("ops.source_checkpoints", "S20:0", state)
        result = await svc.poll_once("S20")
        assert result["items"] == 1 and result["status"] == "HEALTHY"
        assert not (await repo.get("ops.source_checkpoints", "S20:0"))["pending_batches"]
        assert len(repo.jobs) == 1


def test_source_timestamp_canonical_bucket_and_timezone_uncertainty():
    day = parsers.timestamp_fields("2026-10-18")
    assert day["published_prec"] == "BUCKET"
    assert day["published_at_precision"] == "BUCKET(1d)"
    assert day["published_at_bucket_size_s"] == 86400
    unknown_zone = parsers.timestamp_fields("2026-10-18T12:00:00")
    assert unknown_zone["published_at"] is None
    assert unknown_zone["published_prec_reason"] == "UNVERIFIED_SEMANTICS"
    assert parsers.timestamp_fields(None)["published_at_reason"] == "INCOMPLETE"
    known_instant = parsers.timestamp_fields("2026-10-18T12:00:00+02:00")
    assert known_instant["published_at"] == "2026-10-18T10:00:00Z"
    assert known_instant["published_prec"] == "EXACT"
    end = parsers.timestamp_fields(1792238400, "end")
    assert end["end_prec"] == end["end_precision"] == "EXACT"
    assert (
        parsers.timestamp_fields(None, "business_published_at")["business_published_at_prec_reason"]
        == "INCOMPLETE"
    )


@pytest.mark.asyncio
async def test_due_time_uses_actual_request_start_after_budget_admission(tmp_path, monkeypatch):
    from datetime import timedelta

    import tank.sources as source_module
    from tank.util import iso, utcnow

    clock = [utcnow()]
    monkeypatch.setattr(source_module, "utcnow", lambda: clock[0])
    monkeypatch.setattr(source_module, "iso", lambda value=None: iso(clock[0] if value is None else value))
    source = {
        "source_id": "S30",
        "feeds": [{"url": "https://example.org/rss", "kind": "rss", "interval_s": 60}],
    }
    repo = Repo()
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: calls.append(clock[0]) or httpx.Response(200, content=RSS)
        )
    ) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        first = [True]

        async def budget(feed):
            if first[0]:
                clock[0] += timedelta(seconds=30)
                first[0] = False

        svc.acquire_budget = budget
        await svc.control_once("S30")
        state = await repo.get("ops.source_checkpoints", "S30:0")
        assert state["last_attempt_at"] == iso(calls[0])
        assert state["last_attempt_at_prec"] == "COLLECTION_ONLY"
        row = next(v for (c, _), v in repo.rows.items() if c == "ingest.source_items")
        assert row["observed_at"] == iso(calls[0])
        assert row["observed_at_prec"] == "COLLECTION_ONLY"
        clock[0] += timedelta(seconds=59)
        await svc.control_once("S30")
        assert len(calls) == 1
        clock[0] += timedelta(seconds=1)
        await svc.control_once("S30")
        assert len(calls) == 2


@pytest.mark.asyncio
async def test_invalid_index_retains_source_associated_raw_before_parse_failure(tmp_path):
    from pathlib import Path

    source = {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}
    repo = Repo()
    payload = b"<html>Publisher layout changed</html>"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=payload, headers={"ETag": "unprocessed"})
        )
    ) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        assert (await svc.poll_once("S30"))["status"] == "DEGRADED"
        state = await repo.get("ops.source_checkpoints", "S30:0")
        assert state["response_url"] == "https://example.org/rss"
        assert state["fetched_at_prec"] == "COLLECTION_ONLY"
        assert state["payload_sha256"] == sha256(payload) and not list(Path(tmp_path).rglob("*.zst"))
        assert "etag" not in state
        assert not repo.jobs


def test_atom_update_time_is_not_fabricated_original_publication():
    updated = "<updated>2026-10-18T12:00:00Z</updated>"
    prefix = '<feed xmlns="http://www.w3.org/2005/Atom"><title>Activity</title><entry><id>entry1</id><title>Release</title><link href="https://example.org/entry1"/>'
    suffix = "<content>Actual body</content></entry></feed>"
    row = parsers.feeds(
        (prefix + updated + suffix).encode(), "S25", {"url": "https://example.org/commits/main.atom"}
    )[0]
    assert row["published_at"] is None
    assert row["published_prec"] == "COLLECTION_ONLY"
    assert row["published_at_reason"] == "INCOMPLETE"
    assert row["source_updated_at"] == "2026-10-18T12:00:00Z"
    assert row["source_updated_at_prec"] == "EXACT"
    both = parsers.feeds(
        (prefix + "<published>2026-10-17T10:00:00Z</published>" + updated + suffix).encode(),
        "S25",
        {"url": "https://example.org/releases.atom"},
    )[0]
    assert both["published_at"] == "2026-10-17T10:00:00Z"
    assert both["source_updated_at"] == "2026-10-18T12:00:00Z"


@pytest.mark.asyncio
@pytest.mark.parametrize("source_id", ["S03", "S30"])
async def test_malformed_index_entry_does_not_starve_valid_sibling_or_advance_validators(tmp_path, source_id):
    calls = []
    rss = RSS.replace(b"<item><guid>", b"<item><title>Bad entry without identity</title></item><item><guid>")
    data = {
        "results": [
            {"title": "Malformed original without identifier"},
            {
                "document_number": "good",
                "title": "Valid release",
                "abstract": "Full visible body",
                "html_url": "https://example.org/good",
                "publication_date": "2026-10-18",
            },
        ],
        "next_page_url": None,
    }

    def handle(request):
        calls.append(request)
        return (
            httpx.Response(200, headers={"ETag": "partial-response"}, json=data)
            if source_id == "S03"
            else httpx.Response(200, headers={"ETag": "partial-response"}, content=rss)
        )

    source = {
        "source_id": source_id,
        "feeds": [{"url": "https://example.org/index", "kind": "json" if source_id == "S03" else "rss"}],
    }
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        result = await svc.poll_once(source_id)
        assert result["items"] == 1 and result["status"] == "DEGRADED"
        state = await repo.get("ops.source_checkpoints", source_id + ":0")
        assert state["entry_errors"] and "etag" not in state
        assert not state.get("discovery_checkpoint")
        assert len(repo.jobs) == 1
        await svc.poll_once(source_id)
        assert "if-none-match" not in calls[-1].headers
        assert len(repo.jobs) == 1


@pytest.mark.asyncio
async def test_paginated_instrument_change_uses_actual_page_time_and_raw_evidence(
    tmp_path, monkeypatch, collector_http
):
    from datetime import timedelta
    from pathlib import Path

    import tank.sources as source_module
    from tank.util import iso, utcnow

    clock = [utcnow()]
    monkeypatch.setattr(source_module, "utcnow", lambda: clock[0])
    monkeypatch.setattr(source_module, "iso", lambda value=None: iso(clock[0] if value is None else value))
    phase = [0]

    def handle(request):
        clock[0] += timedelta(seconds=5)
        cursor = request.url.params.get("cursor")
        symbol = "DEFUSDT" if cursor else "ABCUSDT"
        rows = (
            []
            if cursor and phase[0] == 2
            else [
                {
                    "symbol": symbol,
                    "baseCoin": "DEF" if cursor else "ABC",
                    "quoteCoin": "USDT",
                    "status": "Settling" if cursor and phase[0] == 1 else "Trading",
                }
            ]
        )
        return httpx.Response(
            200, json={"retCode": 0, "result": {"list": rows, "nextPageCursor": "" if cursor else "next"}}
        )

    source = {
        "source_id": "S14",
        "feeds": [
            {
                "url": "https://api.bybit.com/v5/market/instruments-info?category=linear",
                "kind": "instruments",
                "venue": "bybit",
                "market_type": "PERP_LINEAR",
            }
        ],
    }
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        collector_http(http)
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        await svc.poll_once("S14")
        phase[0] = 1
        await svc.poll_once("S14")
        row = next(row for (collection, _), row in repo.rows.items() if collection == "ingest.source_items")
        assert row["observed_at"] == iso(clock[0])
        payload = json.loads(zstandard.ZstdDecompressor().decompress(Path(row["raw_ref"]).read_bytes()))
        assert payload["result"]["list"][0]["symbol"] == "DEFUSDT"
        phase[0] = 2
        await svc.poll_once("S14")
        removed = next(
            row
            for (collection, _), row in repo.rows.items()
            if collection == "ingest.source_items" and row["change"] == "REMOVED"
        )
        assert removed["observed_at"] == iso(clock[0])
        pages = zstandard.ZstdDecompressor().decompress(Path(removed["raw_ref"]).read_bytes()).split(b"\n")
        assert [len(json.loads(page)["result"]["list"]) for page in pages] == [1, 0]


@pytest.mark.asyncio
async def test_long_history_catchup_refreshes_current_head_at_feed_cadence(tmp_path, monkeypatch):
    from datetime import timedelta

    import tank.sources as source_module
    from tank.util import iso, utcnow

    clock = [utcnow()]
    monkeypatch.setattr(source_module, "utcnow", lambda: clock[0])
    monkeypatch.setattr(source_module, "iso", lambda value=None: iso(clock[0] if value is None else value))
    phase = [0]
    calls = []

    def handle(request):
        page = int(request.url.params.get("page", 1))
        calls.append((phase[0], page, clock[0]))
        clock[0] += timedelta(seconds=2)
        ids = [1, 0] if phase[0] == 0 else [8, 7, 6, 5, 4, 3, 2, 1, 0]
        if phase[0] == 1 and page == 1 and sum(phase == 1 and p == 1 for phase, p, _ in calls) > 1:
            ids = [9] + ids
        selected = ids[(page - 1) * 2 : page * 2]
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "document_number": str(i),
                        "title": "Release " + str(i),
                        "abstract": "Body " + str(i),
                        "html_url": "https://example.org/doc/" + str(i),
                        "publication_date": "2026-10-18",
                    }
                    for i in selected
                ],
                "next_page_url": "https://example.org/list?page=" + str(page + 1)
                if page * 2 < len(ids)
                else None,
            },
        )

    source = {
        "source_id": "S03",
        "feeds": [{"url": "https://example.org/list", "kind": "json", "interval_s": 2}],
    }
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        await svc.poll_once("S03")
        phase[0] = 1
        assert (await svc.poll_once("S03"))["status"] == "HEALTHY"
        rows = [row for (collection, _), row in repo.rows.items() if collection == "ingest.source_items"]
        assert {row["external_id"] for row in rows} == {str(i) for i in range(10)}
        assert sum(phase == 1 and page == 1 for phase, page, _ in calls) > 1
        assert (await repo.get("ops.source_checkpoints", "S03:0"))["discovery_checkpoint"] == "8"


@pytest.mark.asyncio
async def test_coinbase_trading_restriction_change_emits_item_without_status_enum_change(
    tmp_path, collector_http
):
    restricted = [False]
    source = {
        "source_id": "S14",
        "feeds": [
            {
                "url": "https://api.exchange.coinbase.com/products",
                "kind": "instruments",
                "venue": "coinbase",
                "market_type": "SPOT",
            }
        ],
    }
    repo = Repo()

    def handle(request):
        return httpx.Response(
            200,
            json=[
                {
                    "id": "ETH-USD",
                    "base_currency": "ETH",
                    "quote_currency": "USD",
                    "status": "online",
                    "trading_disabled": False,
                    "cancel_only": restricted[0],
                    "limit_only": False,
                    "post_only": False,
                    "auction_mode": False,
                }
            ],
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        collector_http(http)
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        assert (await svc.poll_once("S14"))["items"] == 0
        restricted[0] = True
        assert (await svc.poll_once("S14"))["items"] == 1
        row = next(row for (collection, _), row in repo.rows.items() if collection == "ingest.source_items")
        change = json.loads(row["text"])
        assert change["before"]["status"] == change["after"]["status"] == "online"
        assert change["before"]["status_flags"]["cancel_only"] is False
        assert change["after"]["status_flags"]["cancel_only"] is True


@pytest.mark.asyncio
async def test_new_coinbase_flag_observation_does_not_fabricate_historical_status_change(
    tmp_path, collector_http
):
    flag = [True]
    source = {
        "source_id": "S14",
        "feeds": [
            {
                "url": "https://api.exchange.coinbase.com/products",
                "kind": "instruments",
                "venue": "coinbase",
                "market_type": "SPOT",
            }
        ],
    }
    repo = Repo()
    original = {
        "id": "ETH-USD",
        "base_currency": "ETH",
        "quote_currency": "USD",
        "status": "online",
        "trading_disabled": False,
    }
    previous = parsers.instruments([original], source["feeds"][0])
    previous["ETH-USD"].pop("status_flags")
    await repo.put("ops.source_checkpoints", "S14:0", {"instruments": previous})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=[dict(original, cancel_only=flag[0])])
        )
    ) as http:
        collector_http(http)
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        assert (await svc.poll_once("S14"))["items"] == 0
        assert (await repo.get("ops.source_checkpoints", "S14:0"))["instrument_snapshot_schema_version"] == 2
        flag[0] = False
        assert (await svc.poll_once("S14"))["items"] == 1


def test_treasury_duplicate_designated_publication_elements_are_one_source_timestamp():
    payload = '<main><h1>Official release</h1><div class="field--name-field-news-publication-date"><time datetime="2026-10-16T20:00:00Z">October 16, 2026</time></div><div class="field--name-field-news-publication-date"><time datetime="2026-10-16T20:00:00Z">October 16, 2026</time></div></main>'
    row = parsers.article(payload, source_id="S07")
    assert row["published_at"] == "2026-10-16T20:00:00Z"
    assert row["published_prec"] == "EXACT"
    assert row["published_at_basis"] == "DESIGNATED_PUBLICATION_TIME_ELEMENT"
    conflicting = payload.replace("2026-10-16T20:00:00Z", "2026-10-15T20:00:00Z", 1)
    assert parsers.article(conflicting, source_id="S07")["published_at"] is None


@pytest.mark.asyncio
async def test_unavailable_known_snapshot_proposal_remains_pending_and_explicitly_degraded(tmp_path):
    source = {
        "source_id": "S24",
        "feeds": [
            {
                "url": "https://hub.snapshot.org/graphql",
                "kind": "graphql",
                "method": "POST",
                "body": {
                    "query": 'query { proposals(first: 20, orderBy: "created", orderDirection: desc) { id title body state start end created scores scores_total space { id } } }'
                },
            }
        ],
    }
    repo = Repo()
    await repo.put("ops.source_checkpoints", "S24:0", {"pending_proposals": ["known-now-unavailable"]})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"data": {"proposals": []}}))
    ) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        result = await svc.poll_once("S24")
        assert result["items"] == 0 and result["status"] == "DEGRADED"
        state = await repo.get("ops.source_checkpoints", "S24:0")
        assert state["pending_proposals"] == ["known-now-unavailable"]
        assert state["entry_errors"][0]["reason"] == "PENDING_PROPOSAL_UNAVAILABLE"
        assert not repo.jobs


def test_null_publisher_identity_is_not_stringified_into_fabricated_item():
    rows = parsers.json_items(
        {
            "results": [
                {
                    "document_number": None,
                    "title": "Missing identity",
                    "abstract": "Body",
                    "html_url": "https://example.org/unknown",
                    "publication_date": "2026-10-18",
                },
                {
                    "document_number": "real-id",
                    "title": "Valid",
                    "abstract": "Actual",
                    "html_url": "https://example.org/real",
                    "publication_date": "2026-10-18",
                },
            ]
        },
        "S03",
    )
    assert len(rows) == 1 and rows[0]["external_id"] == "real-id"
    assert len(rows.errors) == 1


def test_raw_item_writer_exact_hld_path_utc_bytes_and_immutable_conflict(tmp_path):
    from pathlib import Path

    from tank.sources.raw_store import write_raw_item
    from tank.util import sha256

    payload = b"<rss>Exact original wire bytes</rss>"
    path, digest = write_raw_item(tmp_path, "S01", "ITM_example", "2026-10-18T00:30:00+03:00", payload)
    assert Path(path) == tmp_path.resolve() / "raw/items/S01/2026/10/17/ITM_example.json.zst"
    assert digest == sha256(payload)
    assert zstandard.ZstdDecompressor().decompress(Path(path).read_bytes()) == payload
    assert write_raw_item(tmp_path, "S01", "ITM_example", "2026-10-17T21:30:00Z", payload) == (path, digest)
    with pytest.raises(ValueError, match="immutable"):
        write_raw_item(tmp_path, "S01", "ITM_example", "2026-10-17T21:30:00Z", b"different body")
    assert zstandard.ZstdDecompressor().decompress(Path(path).read_bytes()) == payload
    with pytest.raises(ValueError, match="source_id"):
        write_raw_item(tmp_path, "../escape", "ITM_example", "2026-10-17T21:30:00Z", payload)


@pytest.mark.asyncio
async def test_new_source_item_raw_path_uses_final_item_id_without_rewriting_old_reference(tmp_path):
    from pathlib import Path

    repo = Repo()
    source = {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=RSS))
    ) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        await svc.poll_once("S30")
        row = next(v for (c, _), v in repo.rows.items() if c == "ingest.source_items")
        day = row["observed_at"][:10].replace("-", "/")
        assert Path(row["raw_ref"]) == tmp_path.resolve() / "raw/items/S30" / day / (
            row["item_id"] + ".json.zst"
        )
        assert zstandard.ZstdDecompressor().decompress(Path(row["raw_ref"]).read_bytes()) == RSS
        original_path = row["raw_ref"]
        await svc.poll_once("S30")
        assert (await repo.get("ingest.source_items", row["item_id"]))["raw_ref"] == original_path


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["file", "directory"])
async def test_raw_durability_failure_prevents_source_item_and_job_persistence(
    tmp_path, monkeypatch, failure_stage
):
    import os
    import stat
    from pathlib import Path

    from tank.sources import raw_store

    source = {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}
    repo = Repo()
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=RSS)))
    svc = SourceService(repo, settings(tmp_path, [source]), http=http)
    original_fsync = os.fsync

    def fail_fsync(descriptor):
        is_directory = stat.S_ISDIR(os.fstat(descriptor).st_mode)
        if is_directory == (failure_stage == "directory"):
            raise OSError("Injected durable storage failure")
        return original_fsync(descriptor)

    monkeypatch.setattr(raw_store.os, "fsync", fail_fsync)
    result = await svc.poll_once("S30")
    assert result["status"] == "DEGRADED" and "durable storage failure" in result["errors"][0]
    assert not any(collection in {"ingest.source_items"} for collection, _ in repo.rows)
    assert not repo.jobs
    assert not list(Path(tmp_path).rglob("*.tmp"))
    monkeypatch.setattr(raw_store.os, "fsync", original_fsync)
    assert (await svc.poll_once("S30"))["items"] == 1
    recovered = next(v for (c, _), v in repo.rows.items() if c == "ingest.source_items")
    assert recovered["raw_ref"].endswith(recovered["item_id"] + ".json.zst")
    assert len(repo.jobs) == 1
    await svc.close()


def test_dtcc_retained_public_release_slugs_preserve_case_dots_and_document_boundaries():
    urls = [
        "https://www.dtcc.com/press-releases/2026/US-Treasury-Central-Clearing-Survey-Industry-Ready-for-Cash-Clearing-Repo-Work-Remains",
        "https://www.dtcc.com/press-releases/2026/DTCC-FundSERV-Adds-Ondo-Finance-as-First-Tokenization-Member",
        "https://www.dtcc.com/press-releases/2025/arianne-m.-collette-joins-dtcc-as-managing-director-and-head-of-u.s.-equities",
        "https://www.dtcc.com/press-releases/2024/dtccs-ficc-launches-cclf-public-calculator-to-support-expansion-of-u.s.-treasury-clearing",
        "https://www.dtcc.com/press-releases/2024/ici-sifma-and-dtcc-comment-on-u.s.-move-to-t1",
    ]
    excluded = [
        "https://www.dtcc.com.evil.example/press-releases/2026/release",
        "https://www.dtcc.com/press-releases/2026/release/extra",
        "https://www.dtcc.com/press-releases/2026/release.pdf",
        "https://www.dtcc.com/press-releases/2026/release.PDF",
        "https://www.dtcc.com/~/media/Files/Downloads/release.pdf",
        "https://www.dtcc.com/press-releases/2026/../release",
        "https://www.dtcc.com/press-releases/2026/release%2Fextra",
    ]
    page = "".join(f'<a href="{url}">Release</a>' for url in urls + excluded).encode()
    feed = {
        "url": "https://www.dtcc.com/press-releases",
        "article_pattern": r"^/press-releases/\d{4}/[A-Za-z0-9][A-Za-z0-9._~-]*$",
    }
    parsed = parsers.article_links(page, feed)
    assert [row["url"] for row in parsed] == urls
    assert all(row["external_id"] == row["url"] for row in parsed)


def test_binance_observed_child_tree_preserves_text_not_metadata():
    body = {
        "node": "root",
        "child": [
            {
                "node": "element",
                "tag": "p",
                "attr": {"title": "NOT BODY"},
                "child": [
                    {
                        "node": "element",
                        "tag": "em",
                        "child": [{"node": "text", "text": "Collateral &amp; margin"}],
                    }
                ],
            },
            {"node": "element", "tag": "p", "child": [{"node": "text", "text": "2026-10-14 08:00 (UTC)."}]},
            {"node": "element", "text": {"metadata": "NOT BODY"}},
        ],
        "layout": "NOT BODY",
    }
    assert parsers.binance_body(json.dumps(body)) == "Collateral & margin 2026-10-14 08:00 (UTC)."


@pytest.mark.asyncio
async def test_binance_empty_body_pending_recovers_after_restart(tmp_path, monkeypatch, collector_http):
    from copy import deepcopy
    from pathlib import Path

    body = {"node": "root", "child": [{"node": "text", "text": "Actual authored release."}]}
    detail = json.dumps({"data": {"publishDate": 1791948608368, "body": json.dumps(body)}}).encode()
    calls = []

    def handle(request):
        calls.append(str(request.url))
        if request.url.path.endswith("/query"):
            return httpx.Response(200, content=detail)
        return httpx.Response(
            200,
            json={
                "data": {
                    "total": 1,
                    "articles": [
                        {"code": "actual-code", "title": "Actual release", "releaseDate": 1791948608368}
                    ],
                }
            },
        )

    repo = Repo()
    cfg = settings(
        tmp_path,
        [
            {
                "source_id": "S09",
                "feeds": [{"kind": "json", "url": "https://www.binance.com/list?pageNo=1&pageSize=20"}],
            }
        ],
    )
    parser = parsers.binance_body
    monkeypatch.setattr(parsers, "binance_body", lambda _: "")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        collector_http(http)
        first = SourceService(repo, cfg, http=http)
        assert (await first.poll_once("S09"))["status"] == "DEGRADED"
        before = deepcopy(await repo.get("ops.source_checkpoints", "S09:0"))
        assert before["pending_batches"] and not before.get("discovery_checkpoint")
        assert not any(table == "ingest.source_items" for table, _ in repo.rows)
        await first.close()
        monkeypatch.setattr(parsers, "binance_body", parser)
        resumed = SourceService(repo, cfg, http=http)
        result = await resumed.poller.drain(cfg.load_config("sources")["sources"][0], 0, force=True)
        assert result["items"] == 1 and not result["errors"]
        after = await repo.get("ops.source_checkpoints", "S09:0")
        assert not after["pending_batches"] and after["discovery_checkpoint"] == "actual-code"
        item = next(row for (table, _), row in repo.rows.items() if table == "ingest.source_items")
        assert item["observed_at"] == before["pending_batches"][0]["rows"][0]["observed_at"]
        assert item["text"] == "Actual authored release."
        assert item["published_at"] == "2026-10-14T03:30:08.368000Z"
        assert item["published_prec"] == "EXACT"
        assert zstandard.ZstdDecompressor().decompress(Path(item["raw_ref"]).read_bytes()) == detail
        assert len(calls) == 3
        await resumed.close()


@pytest.mark.asyncio
async def test_read_counters_and_failed_read_rate_report(tmp_path):
    from tank.sources.read_report import rates, snapshot

    fail = [False]

    def handle(request):
        if request.url.path == "/bad" or fail[0]:
            return httpx.Response(500, content=b"down")
        return httpx.Response(200, content=RSS)

    sources = [
        {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]},
        {"source_id": "S31", "feeds": [{"url": "https://example.org/bad", "kind": "rss"}]},
    ]
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, sources), http=http)

        async def no_wait(delay):
            return None

        import tank.sources as module

        original_sleep = module.asyncio.sleep
        module.asyncio.sleep = no_wait
        try:
            start = await snapshot(repo, sources)
            for _ in range(3):
                await svc.poll_once("S30")
                await svc.poll_once("S31")
            end = await snapshot(repo, sources)
        finally:
            module.asyncio.sleep = original_sleep
    report = rates(end, start, ["S30", "S31"])
    assert report["sources"]["S30"] == {
        "ok": 4,
        "failed": 0,
        "feeds": 1,
        "reads": 4,
        "failed_rate": 0.0,
        "pass": True,
    }
    s31 = report["sources"]["S31"]
    assert (s31["ok"], s31["failed"], s31["pass"]) == (0, 3, False)
    assert s31["degraded_feeds"][0]["feed"] == "S31:0"
    assert not report["all_pass"]


def test_source_http_client_uses_standard_tls_hello(tmp_path):
    from tank.sources import tls_context

    assert tls_context().post_handshake_auth is True
    svc = SourceService(Repo(), settings(tmp_path, []))
    assert svc.http._transport._pool._ssl_context.post_handshake_auth is True


def test_article_text_without_site_boilerplate():
    sec = (
        b'<main><div class="main-content__sidenav">More in this Section Press Release Other release</div>'
        b'<div class="main-content__main"><h1>SEC charges firm</h1>'
        b'<div class="field--name-body"><p>The Commission today charged a firm.</p></div>'
        b"<p>Last Reviewed or Updated: Oct. 20, 2026</p></div></main>"
    )
    assert parsers.article(sec, source_id="S01")["text"] == "The Commission today charged a firm."
    entry = {
        "fields": {
            "title": "Robinhood launches X",
            "content": {
                "nodeType": "document",
                "content": [
                    {"nodeType": "paragraph", "content": [{"nodeType": "text", "value": "First paragraph."}]},
                    {
                        "nodeType": "paragraph",
                        "content": [{"nodeType": "text", "value": "Second paragraph."}],
                    },
                ],
            },
        }
    }
    robinhood = (
        "<div>All News Ideas Education Design Crypto</div><h1>Robinhood launches X</h1>"
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps({"props": {"pageProps": {"article": entry}}})
        + "</script>"
    ).encode()
    row = parsers.article(robinhood, source_id="S13")
    assert row["text"] == "First paragraph. Second paragraph." and row["title"] == "Robinhood launches X"
    bybit = {
        "retCode": 0,
        "result": {
            "list": [
                {
                    "title": "Delisting of ABC",
                    "url": "https://announcements.bybit.com/a",
                    "description": "Bybit is committed to maintaining a secure and reliable trading environment for our community. ",
                    "publishTime": 1792238400000,
                },
                {
                    "title": "New listing DEF",
                    "url": "https://announcements.bybit.com/b",
                    "description": "",
                    "publishTime": 1792238400000,
                },
            ]
        },
    }
    assert [row["text"] for row in parsers.json_items(bybit, "S11")] == [
        "Delisting of ABC",
        "New listing DEF",
    ]


@pytest.mark.asyncio
async def test_pending_rows_without_item_id_are_dropped_and_rediscovered(tmp_path):
    source = {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}
    repo = Repo()
    old_row = {"external_id": "release-42", "url": "https://example.org/a", "title": "Old", "text": "Old"}
    await repo.put(
        "ops.source_checkpoints",
        "S30:0",
        {
            "source_id": "S30",
            "pending_batches": [{"batch_id": "old", "kind": "rss", "rows": [old_row], "coverage": {}}],
        },
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=RSS))
    ) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        await svc.poller.drain(source, 0, force=True)
        assert not (await repo.get("ops.source_checkpoints", "S30:0"))["pending_batches"]
        assert (await svc.poll_once("S30"))["items"] == 1


@pytest.mark.asyncio
async def test_stale_article_errors_clear_when_nothing_is_pending(tmp_path):
    source = {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}
    repo = Repo()
    stale = {"https://example.org/old": {"reason": "METADATA_UNVERIFIABLE"}}
    await repo.put(
        "ops.source_checkpoints",
        "S30:0",
        {"source_id": "S30", "status": "DEGRADED", "article_errors": stale, "pending_batches": []},
    )
    svc = SourceService(repo, settings(tmp_path, [source]))
    await svc.poller.drain(source, 0, force=True)
    state = await repo.get("ops.source_checkpoints", "S30:0")
    assert state["article_errors"] == {} and state["status"] == "HEALTHY"


@pytest.mark.asyncio
async def test_poll_clears_stale_article_errors(tmp_path):
    source = {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}
    repo = Repo()
    await repo.put(
        "ops.source_checkpoints",
        "S30:0",
        {
            "source_id": "S30",
            "status": "DEGRADED",
            "article_errors": {"https://example.org/old": {}},
            "pending_batches": [],
        },
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(304))) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        await svc.poller.acquire(source, 0)
        assert (await repo.get("ops.source_checkpoints", "S30:0"))["article_errors"] == {}


@pytest.mark.asyncio
async def test_instrument_snapshot_written_only_when_it_changes(tmp_path, collector_http):
    status = ["TRADING"]

    def handle(request):
        rows = [{"symbol": "ABCUSDT", "baseAsset": "ABC", "quoteAsset": "USDT", "status": status[0]}]
        return httpx.Response(200, json={"symbols": rows})

    class Counting(Repo):
        def __init__(self):
            super().__init__()
            self.snapshot_writes = 0

        async def put(self, c, k, v, *, immutable=False):
            self.snapshot_writes += k.endswith(":instruments")
            return await super().put(c, k, v)

    repo = Counting()
    source = {
        "source_id": "S14",
        "feeds": [
            {
                "kind": "instruments",
                "url": "https://api.binance.com/api/v3/exchangeInfo",
                "venue": "binance",
                "market_type": "SPOT",
            }
        ],
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        collector_http(http)
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        for _ in range(3):
            await svc.poll_once("S14")
        assert repo.snapshot_writes == 1
        assert "instruments" not in repo.rows["ops.source_checkpoints", "S14:0"]
        status[0] = "BREAK"
        assert (await svc.poll_once("S14"))["items"] == 1
        assert repo.snapshot_writes == 2


def test_one_poll_of_a_venue_list_groups_symbols_that_made_the_same_move():
    from tank.sources.polling import instrument_changes

    feed = {"url": "https://api.kraken.com/0/public/AssetPairs", "venue": "kraken", "market_type": "SPOT"}
    pages = [(b"page", "2026-10-23T14:00:00Z")]
    online = {"status": "online"}
    cancel = {"status": "cancel_only"}
    previous = {"ABCUSD": online, "ABCEUR": online, "DEFUSD": online, "GHIUSD": online}
    snapshot = {
        "ABCUSD": cancel,
        "ABCEUR": cancel,
        "DEFUSD": cancel,
        "GHIUSD": online,
        "NEWUSD": {"base": "NEW", "status": "online"},
        "NEWEUR": {"base": "NEW", "status": "online"},
        "OTHUSD": {"base": "OTH", "status": "online"},
    }
    rows, _ = instrument_changes(
        snapshot, previous, feed, pages, dict.fromkeys(snapshot, 0), "2026-10-23T14:00:05Z"
    )
    by_title = {row["title"]: row for row in rows}
    assert sorted(by_title) == [
        "OTHUSD instrument change",
        "kraken SPOT: 2 NEW instruments ADDED",
        "kraken SPOT: 3 instruments STATE_UPDATE online → cancel_only",
    ]
    delisting = by_title["kraken SPOT: 3 instruments STATE_UPDATE online → cancel_only"]
    assert delisting["change"] == "STATE_UPDATE"
    assert delisting["observed_at"] == "2026-10-23T14:00:00Z" and delisting["_payload"] == b"page"
    assert [c["symbol"] for c in json.loads(delisting["text"])["changes"]] == ["ABCEUR", "ABCUSD", "DEFUSD"]
    assert by_title["kraken SPOT: 2 NEW instruments ADDED"]["change"] == "ADDED"
    assert json.loads(by_title["OTHUSD instrument change"]["text"])["before"] is None


def test_a_grouped_change_keeps_its_own_identity_when_the_list_returns_to_an_earlier_snapshot():
    from tank.sources.polling import instrument_changes

    feed = {"url": "https://api.kraken.com/0/public/AssetPairs", "venue": "kraken", "market_type": "SPOT"}
    pages = [(b"page", "2026-10-23T14:00:00Z")]
    online, cancel = {"status": "online"}, {"status": "cancel_only"}
    first = {"A": online, "B": online, "C": online, "D": online}
    second = {"A": cancel, "B": cancel, "C": online, "D": online}
    third = {"A": cancel, "B": cancel, "C": cancel, "D": cancel}

    def ids(snapshot, previous):
        rows, digest = instrument_changes(snapshot, previous, feed, pages, dict.fromkeys(snapshot, 0), "x")
        return [row["external_id"] for row in rows], digest

    forward, at_second = ids(second, first)
    other, _ = ids(third, second)
    back, back_digest = ids(second, third)
    assert back_digest == at_second
    assert len({*forward, *other, *back}) == 3
