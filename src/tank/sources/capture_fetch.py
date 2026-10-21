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
        "openapi.okx.com",
        "api.bybit.com",
        "api.hyperliquid.xyz",
    }
)
# A restart's capture burst holds a venue budget (Hyperliquid) for ~25 s; polls are 60 s apart.
FETCH_BUDGET_S = 45.0
ARCHIVE_BUDGET_S = 600.0
FETCH_GRACE_S = 3.0
TERMINAL = ("COMPLETE", "FAILED", "EXPIRED")
CONDITIONAL_HEADERS = ("if-none-match", "if-modified-since")


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
    repo, method, url, *, headers=None, body=None, source_id=None, budget_s=FETCH_BUDGET_S
):
    """Ask tank-capture to fetch url; return its response, or raise httpx.TransportError."""
    request = {"method": (method or "GET").upper(), "url": url, "source_id": source_id}
    if headers:
        request["headers"] = {k: v for k, v in headers.items() if k.lower() not in CONDITIONAL_HEADERS}
    if body is not None:
        request["body"] = body
    request_id, row = await _capture_request(repo, "HTTP_FETCH", request, budget_s)
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
    return httpx.Response(
        int(result["status_code"]), content=content, headers=response_headers, request=http_request
    )


async def capture_archive(repo, url, *, budget_s=ARCHIVE_BUDGET_S):
    """Ask tank-capture to store one Binance public-data archive file (data.binance.vision zip).

    tank-capture checks it against the published .CHECKSUM and stores it on the shared data
    volume; this returns {path, sha256, bytes, checksum_text}. Core reads the file, never the site.
    """
    request_id, row = await _capture_request(repo, "ARCHIVE_FETCH", {"url": url}, budget_s)
    if row.get("status") != "COMPLETE":
        reason = row.get("reason") or row.get("status") or "NO_ANSWER"
        raise RuntimeError(f"tank-capture archive {request_id}: {reason}")
    return row["result"]
