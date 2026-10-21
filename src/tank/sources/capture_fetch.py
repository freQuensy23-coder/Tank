"""Exchange URLs are fetched by tank-capture, never by core.

A source feed on an exchange host becomes an HTTP_FETCH row in mkt.capture_requests;
tank-capture claims it, fetches within the shared venue budget and stores the response
in the row's result. The caller gets an httpx.Response built from that stored result.
"""

from __future__ import annotations

import asyncio
import base64
import time
from datetime import timedelta
from urllib.parse import urlsplit

import httpx

from tank.time_precision import Timestamp, collection_clock
from tank.util import iso, sha256, utcnow

# Hosts only tank-capture may reach (capture/internal/capture/fetch.go FetchHosts).
EXCHANGE_FETCH_HOSTS = frozenset(
    {
        "api.binance.com",
        "fapi.binance.com",
        "www.binance.com",
        "data.binance.vision",
        "api.exchange.coinbase.com",
        "api.kraken.com",
        "www.okx.com",
        "api.bybit.com",
        "api.hyperliquid.xyz",
    }
)
FETCH_BUDGET_S = 25.0
FETCH_GRACE_S = 3.0
TERMINAL = ("COMPLETE", "FAILED", "EXPIRED")
CONDITIONAL_HEADERS = ("if-none-match", "if-modified-since")


def is_exchange_url(url):
    return (urlsplit(url).hostname or "") in EXCHANGE_FETCH_HOSTS


async def capture_fetch(repo, method, url, *, headers=None, body=None, source_id=None, budget_s=FETCH_BUDGET_S):
    """Ask tank-capture to fetch url; return its response, or raise httpx.TransportError."""
    requested = utcnow()
    request = {"method": (method or "GET").upper(), "url": url, "source_id": source_id}
    if headers:
        request["headers"] = {k: v for k, v in headers.items() if k.lower() not in CONDITIONAL_HEADERS}
    if body is not None:
        request["body"] = body
    request_id = "CF-" + sha256({"url": url, "method": request["method"], "requested_at": iso(requested)})
    pending = Timestamp(None, None, "PENDING")
    await repo.put(
        "mkt.capture_requests",
        request_id,
        {
            "request_id": request_id,
            "kind": "HTTP_FETCH",
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
    result = row.get("result") or {}
    http_request = httpx.Request(request["method"], url)
    if row.get("status") != "COMPLETE":
        reason = row.get("reason") or row.get("status") or "NO_ANSWER"
        raise httpx.TransportError(f"tank-capture fetch {request_id}: {reason}", request=http_request)
    if result.get("body_b64") is not None:
        content = base64.b64decode(result["body_b64"])
    else:
        content = (result.get("body") or "").encode()
    response_headers = {"content-type": result["content_type"]} if result.get("content_type") else {}
    return httpx.Response(int(result["status_code"]), content=content, headers=response_headers, request=http_request)
