"""Offline search contract, including the actual pinned DDGS engine/parser."""

import asyncio
import threading
from importlib.metadata import version
from itertools import pairwise

import httpx
import pytest
from ddgs.ddgs import ENGINES
from ddgs.exceptions import DDGSException, RatelimitException, TimeoutException

from tank import search
from tank.search import DuckDuckGoSearchService, SearchError, SearchLimiter

USER_AGENT = "TankV0/search-test (offline fixture)"
RESULT = {"title": "Official announcement", "href": "https://example.test/official", "body": "A fact."}
DDG_HTML = """<!doctype html><html><body>
<div class="result__body"><h2><a href="https://example.test/official">Official announcement</a></h2>
<a class="result__snippet" href="https://example.test/official">A fact.</a></div>
<div class="result__body"><h2>Advertisement</h2>
<a href="https://duckduckgo.com/y.js?ad=1">Ad text</a></div>
</body></html>"""


class Time:
    def __init__(self):
        self.now = 0.0
        self.delays = []

    def clock(self):
        return self.now

    async def sleep(self, delay):
        self.delays.append(delay)
        self.now += delay
        await asyncio.sleep(0)


def service(attempt):
    timer = Time()
    limiter = SearchLimiter(clock=timer.clock, sleep=timer.sleep)
    return DuckDuckGoSearchService(
        user_agent=USER_AGENT, limiter=limiter, sleep=timer.sleep, attempt=attempt
    ), timer


def install_http(monkeypatch, respond):
    original = httpx.Client

    def make_client(**kwargs):
        return original(transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr(search.httpx, "Client", make_client)


async def test_pinned_engine_payload_parser_and_unmodified_result(monkeypatch):
    assert version("ddgs") == "9.16.0"
    requests = []
    calls = []
    original_text = search.DDGS.text

    def text(instance, query, **kwargs):
        calls.append((query, kwargs))
        return original_text(instance, query, **kwargs)

    monkeypatch.setattr(search.DDGS, "text", text)
    monkeypatch.setenv("DDGS_PROXY", "http://forbidden.test:1")
    monkeypatch.setenv("HTTPS_PROXY", "http://forbidden.test:1")

    def respond(request):
        requests.append(request)
        assert request.url == search.SEARCH_URL
        assert request.method == "POST"
        assert request.headers["User-Agent"] == USER_AGENT
        assert b"q=official+announcement" in request.content
        return httpx.Response(200, text=DDG_HTML)

    install_http(monkeypatch, respond)

    class Forbidden:
        def __init__(self, *args, **kwargs):
            pytest.fail("An alternate engine was instantiated")

    monkeypatch.setitem(ENGINES, "text", {"forbidden": Forbidden})
    adapter, _ = service(search._search_sync)
    result = await adapter.search("official announcement")
    assert result.results == [RESULT]
    assert result.attempts == 1
    assert len(requests) == 1
    assert calls == [("official announcement", {"backend": "duckduckgo", "max_results": 10})]


@pytest.mark.parametrize(
    "status,html,code",
    [
        (429, "Too many requests", "RATE_LIMIT"),
        (403, "Forbidden", "BLOCKED"),
        (202, "Challenge", "BLOCKED"),
        (200, '<form id="challenge-form">Challenge</form>', "BLOCKED"),
        (302, "Redirect to another host", "BLOCKED"),
    ],
)
async def test_real_ddgs_preserves_block_and_retry_contract(monkeypatch, status, html, code):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(status, text=html, headers={"Location": "https://forbidden.test"})

    install_http(monkeypatch, respond)
    adapter, timer = service(search._search_sync)
    accounted = []

    async def account(number):
        accounted.append(number)

    with pytest.raises(SearchError) as failure:
        await adapter.search("official", on_attempt=account)
    assert failure.value.code == code
    assert failure.value.attempts == 4
    assert failure.value.as_dict()["certainty"] == "MEDIUM"
    assert failure.value.as_dict()["verification"] == "UNCONFIRMED"
    assert accounted == [1, 2, 3, 4]
    assert timer.delays == [5, 15, 45]
    assert len(requests) == 4
    assert all(str(request.url) == search.SEARCH_URL for request in requests)


@pytest.mark.parametrize("exception", [TimeoutException("timed out"), RatelimitException("limit")])
async def test_typed_library_timeout_and_ratelimit_retry_exactly(exception):
    starts = []

    def attempt(query, user_agent, mark):
        mark()
        starts.append(timer.now)
        raise exception

    adapter, timer = service(attempt)
    with pytest.raises(SearchError) as failure:
        await adapter.search("a fact")
    assert starts == [0, 5, 20, 65]
    assert timer.delays == [5, 15, 45]
    assert failure.value.attempts == 4


async def test_actual_http_timeout_preserved_without_hidden_retry(monkeypatch):
    starts = []

    def respond(request):
        starts.append(timer.now)
        raise httpx.ReadTimeout("timeout", request=request)

    install_http(monkeypatch, respond)
    adapter, timer = service(search._search_sync)
    with pytest.raises(SearchError) as failure:
        await adapter.search("official")
    assert failure.value.code == "TIMEOUT"
    assert starts == [0, 5, 20, 65]


@pytest.mark.parametrize(
    "exception,code",
    [
        (DDGSException("No results found."), "NO_RESULTS"),
        (RuntimeError("do not expose internals"), "SEARCH_FAILED"),
        (SearchError("POLICY", "Unexpected request"), "POLICY"),
    ],
)
async def test_nonretryable_errors_return_once(exception, code):
    def attempt(query, user_agent, mark):
        mark()
        raise exception

    adapter, timer = service(attempt)
    with pytest.raises(SearchError) as failure:
        await adapter.search("a fact")
    assert failure.value.code == code
    assert failure.value.attempts == 1
    assert timer.delays == []
    assert "do not expose internals" not in str(failure.value)


async def test_default_limiter_is_shared_across_generations_and_concurrent_calls(monkeypatch):
    timer = Time()
    limiter = SearchLimiter(clock=timer.clock, sleep=timer.sleep)
    monkeypatch.setattr(search, "_PROCESS_LIMITER", limiter)
    starts = []

    def attempt(query, user_agent, mark):
        mark()
        starts.append(timer.now)
        return [dict(RESULT)]

    generations = [DuckDuckGoSearchService(user_agent=USER_AGENT, attempt=attempt) for _ in range(3)]
    assert all(generation.limiter is limiter for generation in generations)
    await asyncio.gather(*(generation.search("official") for generation in generations))
    assert starts == [0, 2, 4]
    assert timer.delays == [2, 2]


async def test_retries_and_other_queries_share_the_same_limiter():
    starts = []
    tries = {}

    def attempt(query, user_agent, mark):
        mark()
        starts.append((query, timer.now))
        tries[query] = tries.get(query, 0) + 1
        if query == "retry" and tries[query] == 1:
            raise TimeoutException("timeout")
        return [dict(RESULT)]

    adapter, timer = service(attempt)
    await asyncio.gather(adapter.search("retry"), adapter.search("other"))
    assert len(starts) == 3
    assert all(b[1] - a[1] >= 2 for a, b in pairwise(starts))


async def test_blocking_search_does_not_block_ingestion_and_cancel_drains_worker():
    entered, release = threading.Event(), threading.Event()
    accounted = []

    def attempt(query, user_agent, mark):
        mark()
        entered.set()
        assert release.wait(5)
        return [dict(RESULT)]

    async def account(number):
        accounted.append(number)

    adapter, _ = service(attempt)
    task = asyncio.create_task(adapter.search("official", on_attempt=account))
    try:
        await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), timeout=3)
        ticks = []
        await asyncio.wait_for(asyncio.sleep(0), timeout=0.2)
        ticks.append("ingestion ran")
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert ticks == ["ingestion ran"]
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert accounted == [1]


async def test_cancel_before_request_does_not_charge_attempt():
    adapter, _ = service(lambda *args: pytest.fail("Cancelled request executed"))
    accounted = []

    async def account(number):
        accounted.append(number)

    async with adapter.limiter._lock:
        task = asyncio.create_task(adapter.search("official", on_attempt=account))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert accounted == []


def test_transport_rejects_other_engines_redirect_target_and_second_request():
    requests = []
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(200, text=DDG_HTML)
        )
    ) as client:
        for url in ("https://www.google.com/search", "https://html.duckduckgo.com.evil.test/html/"):
            transport = search._DuckDuckGoHTTP(client, lambda: None)
            with pytest.raises(SearchError, match="Unexpected"):
                transport.request("POST", url)
        transport = search._DuckDuckGoHTTP(client, lambda: None)
        transport.request("POST", search.SEARCH_URL)
        with pytest.raises(SearchError, match="Unexpected"):
            transport.request("POST", search.SEARCH_URL)
    assert len(requests) == 1


def test_version_drift_fails_closed(monkeypatch):
    monkeypatch.setattr(search, "version", lambda name: "9.17.0")
    with pytest.raises(SearchError) as failure:
        search._search_sync("official", USER_AGENT, lambda: None)
    assert failure.value.code == "VERSION"


async def test_no_key_needed_and_invalid_query_does_not_make_request():
    adapter, _ = service(lambda *args: pytest.fail("Invalid query executed"))
    with pytest.raises(ValueError):
        await adapter.search(" ")
    with pytest.raises(ValueError):
        DuckDuckGoSearchService(user_agent="")
