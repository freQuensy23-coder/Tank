"""Credential-free DuckDuckGo search, isolated from DDGS's metasearch fallback.

The DDGS private hooks below are deliberately tied to 9.16.0. See docs/SEARCH.md
before upgrading: accepting ``backend='duckduckgo'`` alone is not a safety boundary.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from importlib.metadata import version
from typing import Any

import httpx
from ddgs.ddgs import DDGS
from ddgs.engines.duckduckgo import Duckduckgo
from ddgs.exceptions import DDGSException, RatelimitException, TimeoutException

DDGS_VERSION = "9.16.0"
SEARCH_URL = "https://html.duckduckgo.com/html/"
RETRY_DELAYS = (5, 15, 45)


class SearchError(Exception):
    """A search failure that must never be mistaken for corroborating evidence."""

    def __init__(self, code: str, message: str, *, retryable: bool = False, attempts: int = 0):
        super().__init__(message)
        self.code, self.retryable, self.attempts = code, retryable, attempts

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": "duckduckgo",
            "error": self.code,
            "message": str(self),
            "attempts": self.attempts,
            "retryable": self.retryable,
            "verification": "UNCONFIRMED",
            "certainty": "MEDIUM",
        }


@dataclass(frozen=True)
class SearchResponse:
    results: list[dict[str, Any]]
    attempts: int


class SearchLimiter:
    """One in-flight request and >=2 seconds between actual request starts.

    This object belongs to the process, not a config generation. The lock stays
    held until a cancelled thread finishes, so cancellation cannot orphan requests.
    """

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self.clock, self.sleep = clock, sleep
        self._lock = asyncio.Lock()
        self._last_started: float | None = None

    def mark_started(self) -> None:
        # Called on the worker thread immediately before the sole HTTP request.
        self._last_started = self.clock()

    async def run(self, attempt: Callable[[], list[dict[str, Any]]]) -> list[dict[str, Any]]:
        async with self._lock:
            if self._last_started is not None:
                while (remaining := 2 - (self.clock() - self._last_started)) > 0:
                    await self.sleep(remaining)
            task = asyncio.create_task(asyncio.to_thread(attempt))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                # to_thread cannot kill a blocking request. Drain it before releasing
                # the shared lock, including when callers cancel more than once.
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                    except Exception:  # noqa: BLE001 - retrieve any drained vendor exception below
                        break
                if task.done() and not task.cancelled():
                    task.exception()
                raise


_PROCESS_LIMITER = SearchLimiter()


class _DuckDuckGoHTTP:
    """Only this exact DDG endpoint; no redirects, ambient proxies or hidden retries."""

    def __init__(self, client: httpx.Client, mark_started: Callable[[], None]):
        self.client, self.mark_started = client, mark_started
        self.requests = 0
        self.error: SearchError | None = None

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        try:
            if method != "POST" or url != SEARCH_URL or self.requests:
                raise SearchError("POLICY", "Unexpected DuckDuckGo request; search stopped")
            self.requests += 1
            self.mark_started()
            response = self.client.request(method, url, **kwargs)
            if response.status_code == 429:
                raise SearchError("RATE_LIMIT", "DuckDuckGo rate limited the search", retryable=True)
            if response.status_code in {202, 401, 403, 418, 503} or 300 <= response.status_code < 400:
                raise SearchError("BLOCKED", "DuckDuckGo blocked the search", retryable=True)
            if response.status_code != 200:
                raise SearchError("HTTP_ERROR", f"DuckDuckGo returned HTTP {response.status_code}")
            lowered = response.text.lower()
            if any(
                marker in lowered
                for marker in (
                    "anomaly-modal",
                    "challenge-form",
                    "bots use duckduckgo too",
                    "unfortunately, bots",
                    "verify you are human",
                    'id="captcha"',
                )
            ):
                raise SearchError("BLOCKED", "DuckDuckGo returned a challenge", retryable=True)
            return response
        except httpx.TimeoutException as exc:
            self.error = SearchError("TIMEOUT", "DuckDuckGo search timed out", retryable=True)
            raise self.error from exc
        except httpx.RequestError as exc:
            self.error = SearchError("REQUEST_FAILED", "DuckDuckGo search request failed")
            raise self.error from exc
        except SearchError as exc:
            self.error = exc
            raise


class _DuckDuckGoEngine(Duckduckgo):
    def __init__(self, transport: _DuckDuckGoHTTP):
        # Do not construct DDGS's primp client: it selects random browser identity.
        # Inherit the actual pinned payload builder, parser and result normalizers.
        self.http_client = transport
        self.results = []


class _DuckDuckGoOnly(DDGS):
    def __init__(self, engine: _DuckDuckGoEngine):
        # The upstream constructor also reads DDGS_PROXY from the environment.
        # This transport has no proxy configuration or credential lookup.
        self._timeout = 20
        self.engine = engine

    def _get_engines(self, category: str, backend: str) -> list[_DuckDuckGoEngine]:
        if category != "text" or backend != "duckduckgo":
            raise SearchError("POLICY", "Only DuckDuckGo text search is allowed")
        # Never call super: its empty/unknown registry path falls back to auto.
        return [self.engine]


def _search_sync(query: str, user_agent: str, mark_started: Callable[[], None]) -> list[dict[str, Any]]:
    if version("ddgs") != DDGS_VERSION:
        raise SearchError("VERSION", "DuckDuckGo adapter requires ddgs 9.16.0")
    with httpx.Client(
        timeout=20,
        follow_redirects=False,
        trust_env=False,
        headers={"User-Agent": user_agent},
    ) as client:
        transport = _DuckDuckGoHTTP(client, mark_started)
        search = _DuckDuckGoOnly(_DuckDuckGoEngine(transport))
        try:
            results = search.text(query, backend="duckduckgo", max_results=10)
            if transport.error is not None:
                raise transport.error
            return results
        except DDGSException as exc:
            # DDGS catches engine exceptions in its executor; preserve the typed
            # transport failure rather than downgrading a block to "no results".
            if transport.error is not None:
                raise transport.error from exc
            raise


def _classify(exc: Exception) -> SearchError:
    if isinstance(exc, SearchError):
        return exc
    if isinstance(exc, (TimeoutException, httpx.TimeoutException, TimeoutError)):
        return SearchError("TIMEOUT", "DuckDuckGo search timed out", retryable=True)
    if isinstance(exc, RatelimitException):
        return SearchError("RATE_LIMIT", "DuckDuckGo rate limited the search", retryable=True)
    if isinstance(exc, DDGSException) and str(exc) == "No results found.":
        return SearchError("NO_RESULTS", "DuckDuckGo returned no search results")
    return SearchError("SEARCH_FAILED", "DuckDuckGo search failed")


class DuckDuckGoSearchService:
    def __init__(
        self,
        *,
        user_agent: str,
        limiter: SearchLimiter | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        attempt: Callable[[str, str, Callable[[], None]], list[dict[str, Any]]] | None = None,
    ):
        if not user_agent.strip():
            raise ValueError("DuckDuckGo search requires a declared User-Agent")
        self.user_agent, self.limiter = user_agent, limiter or _PROCESS_LIMITER
        self.sleep, self._attempt = sleep, attempt or _search_sync

    async def search(
        self, query: str, *, on_attempt: Callable[[int], Awaitable[None]] | None = None
    ) -> SearchResponse:
        if not isinstance(query, str) or not query.strip():
            raise ValueError("Search query must not be empty")
        for attempt_number in range(1, 5):
            error = None
            launched = False

            def run_attempt():
                nonlocal launched
                launched = True
                return self._attempt(query, self.user_agent, self.limiter.mark_started)

            try:
                results = await self.limiter.run(run_attempt)
            except asyncio.CancelledError:
                # The limiter drains a submitted request before cancellation returns.
                # Log its attempt even though the caller no longer awaits a result.
                if launched and on_attempt is not None:
                    await on_attempt(attempt_number)
                raise
            except Exception as exc:  # noqa: BLE001 - remote/vendor failures are safe tool errors
                error = _classify(exc)
                error.attempts = attempt_number
            if on_attempt is not None:
                await on_attempt(attempt_number)
            if error is None:
                return SearchResponse(results=results, attempts=attempt_number)
            if not error.retryable or attempt_number == 4:
                raise error
            await self.sleep(RETRY_DELAYS[attempt_number - 1])
        raise AssertionError("unreachable")
