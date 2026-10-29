"""Exchange URLs are fetched by tank-capture, never by core.

A source feed on an exchange host becomes an HTTP_FETCH row in mkt.capture_requests;
tank-capture claims it, fetches within the shared venue budget and stores the response
in the row's result. The caller gets an httpx.Response built from that stored result.
"""

import asyncio
import base64
import time
from datetime import timedelta
from urllib.parse import urlsplit

import httpx

from tank.time_precision import Timestamp, collection_clock
from tank.util import iso, sha256, utcnow

EXCHANGE_FETCH_HOSTS = frozenset(
    {
        "api.binance.com",
        "fapi.binance.com",
        "www.binance.com",
        "data.binance.vision",
        "api.exchange.coinbase.com",
        "api.kraken.com",
        "www.okx.com",
        "openapi.okx.com",
        "api.bybit.com",
        "api.hyperliquid.xyz",
    }
)

FETCH_BUDGET_S = 45.0
FETCH_GRACE_S = 3.0
TERMINAL = ("COMPLETE", "FAILED", "EXPIRED")
CONDITIONAL_HEADERS = ("if-none-match", "if-modified-since")


CAPTURE_TIMEOUTS = frozenset({"REQUEST_BUDGET_EXHAUSTED", "EXPIRED", "NO_ANSWER"})


class CaptureRateLimited(httpx.TransportError):
    """The exchange kept answering HTTP 429 until tank-capture's retries or budget ran out."""

    def __init__(self, message, *, request, retry_after=None):
        super().__init__(message, request=request)
        self.retry_after = retry_after or ""


def is_exchange_url(url):
    return (urlsplit(url).hostname or "") in EXCHANGE_FETCH_HOSTS


async def _capture_request(repo, kind, request, budget_s):
    """Queue a request for tank-capture and wait for its terminal row."""
    requested = utcnow()
    request_id = ("CF-" if kind == "HTTP_FETCH" else "CA-") + sha256(
        {"url": request["url"], "method": request.get("method"), "requested_at": iso(requested)}
    )
    pending = Timestamp(None, None, "PENDING")
    await repo.put(
        "mkt.capture_requests",
        request_id,
        {
            "request_id": request_id,
            "kind": kind,
            "request": request,
            "budget_s": budget_s,
            **collection_clock(requested).fields("requested_at"),
            **collection_clock(requested + timedelta(seconds=budget_s)).fields("deadline"),
            **pending.fields("started_at"),
            **pending.fields("finished_at"),
            "status": "PENDING",
            "reason": None,
            "result": None,
        },
    )
    until = time.monotonic() + budget_s + FETCH_GRACE_S
    row = {}
    while time.monotonic() < until:
        row = await repo.get("mkt.capture_requests", request_id) or {}
        if row.get("status") in TERMINAL:
            break
        await asyncio.sleep(0.2)
    return request_id, row


async def capture_fetch(
    repo,
    method,
    url,
    *,
    headers=None,
    body=None,
    source_id=None,
    budget_s=FETCH_BUDGET_S,
    follow_redirects=True,
):
    """Ask tank-capture to fetch url; return its response, or raise httpx.TransportError.

    With follow_redirects=False tank-capture returns a redirect as is, with its Location header.
    """
    request = {"method": (method or "GET").upper(), "url": url, "source_id": source_id}
    if not follow_redirects:
        request["redirects"] = "manual"
    if headers:
        request["headers"] = {k: v for k, v in headers.items() if k.lower() not in CONDITIONAL_HEADERS}
    if body is not None:
        request["body"] = body
    request_id, row = await _capture_request(repo, "HTTP_FETCH", request, budget_s)
    result = row.get("result") or {}
    if row.get("status") in TERMINAL:
        await repo.delete("mkt.capture_requests", request_id)
    http_request = httpx.Request(request["method"], url)
    if row.get("status") != "COMPLETE":
        reason = row.get("reason") or row.get("status") or "NO_ANSWER"
        message = f"tank-capture fetch {request_id}: {reason}"
        if reason == "RATE_LIMITED":
            raise CaptureRateLimited(message, request=http_request, retry_after=result.get("retry_after"))
        if reason in CAPTURE_TIMEOUTS:
            raise httpx.TimeoutException(message, request=http_request)
        raise httpx.TransportError(message, request=http_request)
    if result.get("body_b64") is not None:
        content = base64.b64decode(result["body_b64"])
    else:
        content = (result.get("body") or "").encode()
    response_headers = {"content-type": result["content_type"]} if result.get("content_type") else {}
    if result.get("location"):
        response_headers["location"] = result["location"]
    return httpx.Response(
        int(result["status_code"]), content=content, headers=response_headers, request=http_request
    )
