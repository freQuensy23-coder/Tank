"""fetch_page reaches the agent's key sources: redirects, per-host headers and error classes."""

import asyncio
import time

import httpx
import pytest

import tank.event_tools as CANDIDATE
from tank.agents.sessions import SessionService
from tank.settings import Settings
from tank.storage import MemoryRepository

CONTACT = "TankV0/1.0 (ops@example.org)"


async def service_with(tmp_path, handler):
    repo = MemoryRepository()
    await repo.open()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=None)
    settings = Settings(data_dir=tmp_path, user_agent=CONTACT)
    return CANDIDATE.EventToolService(repo, settings, None, None, http=client), client


def page(text):
    return httpx.Response(200, headers={"content-type": "text/html"}, content=f"<p>{text}</p>".encode())


async def test_follows_redirect_chain_and_stores_final_page(tmp_path):
    seen = []

    def handler(request):
        seen.append(str(request.url))
        if request.url.host == "news.google.com":
            return httpx.Response(302, headers={"location": "https://www.theblock.co/post/1"})
        if request.url.path == "/post/1":
            return httpx.Response(301, headers={"location": "/post/1/story"})
        return page("Final story")

    service, client = await service_with(tmp_path, handler)
    async with client:
        item = await service.fetch_page({"url": "https://news.google.com/rss/articles/abc"}, {})
    assert seen == [
        "https://news.google.com/rss/articles/abc",
        "https://www.theblock.co/post/1",
        "https://www.theblock.co/post/1/story",
    ]
    assert item["text"] == "Final story"
    assert item["url"] == "https://www.theblock.co/post/1/story"
    assert len(await service.repo.list("ingest.source_items")) == 1


@pytest.mark.parametrize("redirects,accepted", [(5, True), (6, False)])
async def test_redirect_limit(tmp_path, redirects, accepted):
    def handler(request):
        hop = int(request.url.path.strip("/") or 0)
        if hop < redirects:
            return httpx.Response(307, headers={"location": f"/{hop + 1}"})
        return page("done")

    service, client = await service_with(tmp_path, handler)
    async with client:
        if accepted:
            item = await service.fetch_page({"url": "https://example.com/0"}, {})
            assert item["url"] == f"https://example.com/{redirects}"
        else:
            with pytest.raises(httpx.TooManyRedirects):
                await service.fetch_page({"url": "https://example.com/0"}, {})
            assert not await service.repo.list("ingest.source_items")


@pytest.mark.parametrize(
    "location,message",
    [
        ("http://example.com/plain", "Only HTTPS"),
        ("https://example.com/p?api_key=abc", "Credential-bearing"),
        ("https://user:pw@example.com/p", "Credential-bearing"),
    ],
)
async def test_each_redirect_hop_passes_the_url_checks(tmp_path, location, message):
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(302, headers={"location": location})

    service, client = await service_with(tmp_path, handler)
    async with client:
        with pytest.raises(ValueError, match=message):
            await service.fetch_page({"url": "https://example.com/start"}, {})
    assert seen == ["https://example.com/start"]
    assert not await service.repo.list("ingest.source_items")


async def answer_capture(service, results):
    """Play tank-capture: answer each queued HTTP_FETCH with the next result; return the requests."""
    requests = []
    for result in results:
        while not (
            rows := [r for r in await service.repo.list("mkt.capture_requests") if r["status"] == "PENDING"]
        ):
            await asyncio.sleep(0.01)
        row = rows[0]
        requests.append(row["request"])
        row.update(status="COMPLETE", result=result)
        await service.repo.put("mkt.capture_requests", row["request_id"], row)
    return requests


async def test_exchange_hops_go_through_tank_capture_one_hop_at_a_time(tmp_path):
    exchange = "https://www.binance.com/en/support/announcement/abc"
    final = "https://www.okx.com/help/final"

    def handler(request):
        if request.url.host in {"www.binance.com", "www.okx.com"}:
            raise AssertionError("core reached an exchange host")
        return httpx.Response(302, headers={"location": exchange})

    service, client = await service_with(tmp_path, handler)
    results = [
        {"status_code": 302, "location": final},
        {"status_code": 200, "content_type": "text/html", "body": "<p>x</p>"},
    ]
    async with client, asyncio.timeout(5):
        worker = asyncio.create_task(answer_capture(service, results))
        item = await service.fetch_page({"url": "https://example.com/link"}, {})
        requests = await worker
    assert [r["url"] for r in requests] == [exchange, final]
    browser = service.settings.load_config("sources")["evidence_user_agent"]
    for request in requests:
        assert request["redirects"] == "manual"
        assert request["headers"]["User-Agent"] == browser
    assert item["url"] == final


@pytest.mark.parametrize(
    "location,message",
    [("http://www.okx.com/plain", "Only HTTPS"), ("https://example.com/p?apikey=1", "Credential-bearing")],
)
async def test_redirect_returned_by_tank_capture_passes_the_url_checks(tmp_path, location, message):
    def handler(request):
        raise AssertionError("core must not fetch " + str(request.url))

    service, client = await service_with(tmp_path, handler)
    async with client, asyncio.timeout(5):
        worker = asyncio.create_task(answer_capture(service, [{"status_code": 301, "location": location}]))
        with pytest.raises(ValueError, match=message):
            await service.fetch_page({"url": "https://www.okx.com/help/x"}, {})
        await worker
    assert not await service.repo.list("ingest.source_items")


async def test_redirect_hops_share_one_deadline(tmp_path):
    async def handler(request):
        await asyncio.sleep(0.6)
        return httpx.Response(302, headers={"location": "/again"})

    service, client = await service_with(tmp_path, handler)
    start = time.monotonic()
    async with client:
        with pytest.raises(httpx.TimeoutException, match="within 1 s"):
            await service.fetch_page({"url": "https://example.com/start", "timeout_s": 1}, {})
    assert time.monotonic() - start < 1.5


async def test_browser_headers_for_news_and_contact_agent_for_declared_hosts(tmp_path):
    agents = {}

    def handler(request):
        agents[request.url.host] = dict(request.headers)
        return page("ok")

    service, client = await service_with(tmp_path, handler)
    async with client:
        for url in [
            "https://www.theblock.co/post/1",
            "https://announcements.bybit.com/en/",
            "https://www.sec.gov/x",
        ]:
            await service.fetch_page({"url": url}, {})
    browser = service.settings.load_config("sources")["evidence_user_agent"]
    assert browser.startswith("Mozilla/5.0 ") and "Tank" not in browser
    for host in ["www.theblock.co", "announcements.bybit.com"]:
        assert agents[host]["user-agent"] == browser
        assert agents[host]["accept"].startswith("text/html")
        assert agents[host]["accept-language"]
    assert agents["www.sec.gov"]["user-agent"] == CONTACT


@pytest.mark.parametrize(
    "error,category,reason",
    [
        (
            httpx.HTTPStatusError(
                "x",
                request=httpx.Request("GET", "https://www.coindesk.com/"),
                response=httpx.Response(429, headers={"retry-after": "30"}),
            ),
            "SOURCE_RATE_LIMITED",
            "Source rate-limited the request (HTTP 429); Retry-After 30 s",
        ),
        (
            httpx.HTTPStatusError(
                "x",
                request=httpx.Request("GET", "https://www.reuters.com/"),
                response=httpx.Response(401),
            ),
            "SOURCE_ACCESS_DENIED",
            "Source refused access to the page (HTTP 401)",
        ),
        (
            httpx.HTTPStatusError(
                "x",
                request=httpx.Request("GET", "https://example.com/"),
                response=httpx.Response(404),
            ),
            "SOURCE_HTTP",
            "Source request failed (HTTP 404)",
        ),
        (
            httpx.TimeoutException(
                "Page not received within 8 s", request=httpx.Request("GET", "https://x.org")
            ),
            "SOURCE_TIMEOUT",
            "Source did not answer within the fetch timeout",
        ),
        (
            httpx.TooManyRedirects("loop", request=httpx.Request("GET", "https://x.org")),
            "SOURCE_REDIRECT",
            "Source redirected too many times",
        ),
    ],
)
def test_source_failures_have_distinct_categories(error, category, reason):
    service = object.__new__(SessionService)
    result, status = service.tool_failure(error)
    assert status == "ERROR"
    assert result["category"] == category
    assert result["reason"] == reason
