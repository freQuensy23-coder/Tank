"""Read the public HTML X sends to logged-out visitors. Never accepts cookies."""

import asyncio
import base64
import gzip
import hashlib
import io
import json
import os
import re
import time
import urllib.parse
import zlib
from contextlib import suppress
from datetime import UTC, datetime

import httpx
from bs4 import BeautifulSoup

LINK = re.compile(r"^/(?:@)?([A-Za-z0-9_]{1,15})/status/(\d+)(?:[/?#]|$)")
BODY_LIMIT = 2_000_000
RUN_EVIDENCE_LIMIT = 12_000_000
PROFILE_ORIGIN = "https://x.com/"
DIAGNOSTIC_HEADERS = {
    "content-type",
    "content-encoding",
    "content-length",
    "retry-after",
    "server",
    "date",
    "x-request-id",
    "x-transaction-id",
    "cf-ray",
    "x-rate-limit-limit",
    "x-rate-limit-remaining",
    "x-rate-limit-reset",
}


class ProfileFailure(RuntimeError):
    def __init__(self, reason, diagnostics):
        super().__init__(reason)
        self.diagnostics = diagnostics


def redact(value):
    for name in ("APIFY_TOKEN", "APIFY_API_TOKEN", "ACTOR_TOKEN"):
        if secret := os.environ.get(name):
            value = value.replace(secret, "[REDACTED]")
    value = re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/-]+", r"\1[REDACTED]", value)
    return re.sub(
        r"(?i)((?<![A-Za-z0-9_])(?:authorization|set-cookie|cookie|auth_token|ct0)[\"']?[ \t]*[:=][ \t]*)"
        r"(\"(?:\\[^\r\n]|[^\"\\\r\n])*\"?|'(?:\\[^\r\n]|[^'\\\r\n])*'?|"
        r"(?:(?:Basic|Bearer)[ \t]+)?[^\s,;&<>{}\[\]\"']+)",
        lambda match: (
            match[1]
            + (match[2][0] if match[2][0] in "\"'" else "")
            + "[REDACTED]"
            + (match[2][0] if match[2][0] in "\"'" and match[2][-1] == match[2][0] else "")
        ),
        value,
    )


async def read_body(response, parts, limit=BODY_LIMIT):
    size = 0
    async for part in response.aiter_raw():
        captured = part[: limit + 1 - size]
        parts.append(captured)
        size += len(captured)
        if size > limit:
            break


def response_evidence(response, raw, read_error=None, limit=BODY_LIMIT, serialized_limit=None):
    headers = {
        name.lower(): redact(value)
        for name, value in response.headers.items()
        if name.lower() in DIAGNOSTIC_HEADERS
    }
    truncated = len(raw) > limit
    captured = raw[:limit]
    encoding_error = None
    if headers.get("content-encoding", "").lower() == "gzip":
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(captured)) as decoded:
                captured = decoded.read(limit + 1)
            truncated = truncated or len(captured) > limit
            captured = captured[:limit]
        except (OSError, EOFError, zlib.error):
            encoding_error = "GZIP_DECODE_FAILED_WIRE_PREFIX"
    original = captured
    try:
        text = captured.decode("utf-8")
        textual = not any(ord(c) < 32 and c not in "\r\n\t" for c in text)
    except UnicodeError:
        textual = False
    safe = redact(captured.decode("latin-1")).encode("latin-1")
    if textual:
        body = redact(original.decode("utf-8"))
        safe = body.encode("utf-8")
    else:
        body = base64.b64encode(safe).decode("ascii")
    redacted = original != safe
    if serialized_limit is not None and len(json.dumps(body, ensure_ascii=True)) > serialized_limit:
        safe = safe[: serialized_limit // 6]
        if textual:
            body = safe.decode("utf-8", errors="ignore")
            safe = body.encode("utf-8")
        else:
            body = base64.b64encode(safe).decode("ascii")
        truncated = True
    length = headers.get("content-length", "")
    total = int(length) if length.isdecimal() and len(length) <= 20 else None
    truncated = truncated or (total is not None and total > len(raw))
    return {
        "status": response.status_code,
        "headers": headers,
        "body": body,
        "bodyEncoding": "utf-8" if textual else "base64",
        "bodyBytes": len(safe),
        "bodySha256": hashlib.sha256(safe).hexdigest(),
        "wireBytes": min(len(raw), limit),
        "captureLimitBytes": limit,
        "serializedBodyLimitBytes": serialized_limit,
        "bodyTruncated": truncated or read_error is not None,
        "omittedWireBytes": max(0, total - min(len(raw), limit)) if total is not None else None,
        "readError": read_error,
        "decodeError": encoding_error,
        "redacted": redacted,
    }


def run_deadline():
    remaining = 100.0
    if value := os.environ.get("ACTOR_TIMEOUT_AT"):
        with suppress(ValueError):
            remaining = min(
                remaining, datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() - time.time() - 2
            )
    return time.monotonic() + max(0, remaining)


def parse_profile(raw, handle):
    soup = BeautifulSoup(raw, "html.parser")
    title = soup.title.get_text() if soup.title else ""
    if " / X" not in title or ("@" + handle.lower()) not in title.lower():
        raise RuntimeError("PUBLIC_PROFILE_NOT_READABLE")
    posts = {}
    for article in soup.find_all("article"):
        if article.find_parent("article"):
            continue
        link = next(
            (
                m
                for a in article.select("a[href]")
                if isinstance(href := a.get("href"), str) and (m := LINK.match(href))
            ),
            None,
        )
        if not link or link.group(1).lower() != handle.lower():
            continue
        post_id, author = link.group(2), link.group(1)
        content = article.select_one(".whitespace-pre-wrap") or article.select_one(
            '[data-testid="tweetText"]'
        )
        if content is None and not article.select(
            'video, img[src*="/media/"], a[href*="/photo/"], a[href*="/video/"]'
        ):
            raise RuntimeError("PUBLIC_POST_BODY_MISSING")
        published = ((int(post_id) >> 22) + 1288834974657) / 1000
        posts[post_id] = {
            "id": post_id,
            "author": author,
            "text": content.get_text("", strip=False).strip() if content else "",
            "publishedTs": published,
            "publishedAt": datetime.fromtimestamp(published, UTC).isoformat(),
            "url": f"https://x.com/{author}/status/{post_id}",
            "source": "x-public-profile",
        }
    if not posts:
        raise RuntimeError("NO_PUBLIC_POSTS_VISIBLE")
    return sorted(posts.values(), key=lambda r: int(r["id"]))


async def fetch_profile(handle, deadline=None, evidence_limit=BODY_LIMIT):
    deadline = run_deadline() if deadline is None else deadline
    url = PROFILE_ORIGIN + urllib.parse.quote(handle)
    diagnostics = {
        "request": {"method": "GET", "url": url, "body": None},
        "attempts": [],
        "attemptCount": 0,
        "outcome": "exhausted",
    }
    reason = "PUBLIC_PROFILE_DEADLINE_EXHAUSTED"
    capture_limit = min(BODY_LIMIT, evidence_limit)
    async with httpx.AsyncClient(follow_redirects=True, trust_env=False) as client:
        for index, delay in enumerate((0, 1, 5), 1):
            if deadline - time.monotonic() <= delay:
                diagnostics["outcome"] = "deadline_exhausted"
                break
            if delay:
                await asyncio.sleep(delay)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                diagnostics["outcome"] = "deadline_exhausted"
                break
            attempt = {"attempt": index, "at": datetime.now(UTC).isoformat()}
            retryable = False
            response = None
            parts = []
            read_error = None
            try:
                async with asyncio.timeout(remaining):
                    async with client.stream(
                        "GET",
                        url,
                        headers={
                            "User-Agent": "Mozilla/5.0",
                            "Accept": "text/html",
                            "Accept-Encoding": "gzip",
                            "Cache-Control": "no-cache",
                        },
                        timeout=min(12, remaining),
                    ) as response:
                        if response.status_code < 400 and response.url.host not in (
                            "x.com",
                            "www.x.com",
                            "twitter.com",
                            "www.twitter.com",
                        ):
                            raise RuntimeError("UNEXPECTED_PUBLIC_PROFILE_REDIRECT")
                        await read_body(
                            response, parts, capture_limit if response.status_code >= 400 else BODY_LIMIT
                        )
                if response.status_code < 400:
                    compressed = b"".join(parts)
                    if len(compressed) > BODY_LIMIT:
                        raise RuntimeError("PUBLIC_PROFILE_TOO_LARGE")
                    if response.headers.get("Content-Encoding", "").lower() == "gzip":
                        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as decoded:
                            raw = decoded.read(BODY_LIMIT + 1)
                    else:
                        raw = compressed
                    if len(raw) > BODY_LIMIT:
                        raise RuntimeError("PUBLIC_PROFILE_TOO_LARGE")
                    rows = parse_profile(raw.decode(), handle)
                    if time.monotonic() >= deadline:
                        raise RuntimeError("PUBLIC_PROFILE_DEADLINE_EXHAUSTED")
                    return rows, len(compressed)
            except TimeoutError:
                reason = "PUBLIC_PROFILE_DEADLINE_EXHAUSTED"
                read_error = "RESPONSE_BODY_DEADLINE_EXHAUSTED"
            except RuntimeError as exc:
                reason = str(exc)
            except (httpx.HTTPError, OSError, EOFError):
                reason = "PUBLIC_PROFILE_CONNECTION_FAILED"
                retryable = True
                read_error = "RESPONSE_BODY_READ_FAILED"
            except (UnicodeError, ValueError, zlib.error):
                reason = "PUBLIC_PROFILE_NOT_READABLE"
            if response is not None and response.status_code >= 400:
                reason = "PUBLIC_PROFILE_HTTP_" + str(response.status_code)
                retryable = response.status_code in (408, 429) or 500 <= response.status_code <= 599
            if response is not None and (response.status_code >= 400 or read_error):
                attempt["response"] = response_evidence(
                    response, b"".join(parts), read_error, capture_limit, evidence_limit
                )
                attempt["status"] = response.status_code
            attempt.update(error=reason, retryable=retryable)
            diagnostics["attempts"].append(attempt)
            diagnostics["attemptCount"] = len(diagnostics["attempts"])
            if time.monotonic() >= deadline:
                diagnostics["outcome"] = "deadline_exhausted"
                break
            if not retryable:
                diagnostics["outcome"] = "permanent_failure"
                break
    raise ProfileFailure(reason, diagnostics) from None


async def collect(config, known_ids, cutoff):
    started = time.monotonic()
    deadline = run_deadline()
    names = list(dict.fromkeys(h.lstrip("@").lower() for h in config["handles"]))
    if not names or any(not re.fullmatch(r"[a-z0-9_]{1,15}", h) for h in names):
        raise RuntimeError("INVALID_MONITORED_HANDLE")
    semaphore = asyncio.Semaphore(max(1, min(8, int(config.get("concurrency", 6)))))
    account_state = config.get("_accountState") or {}
    evidence_limit = RUN_EVIDENCE_LIMIT // (3 * len(names))

    async def one(handle):
        async with semaphore:
            stamp = time.time()
            try:
                rows, size = await fetch_profile(handle, deadline, evidence_limit)
                previous = account_state.get(handle) or {}
                previous_max = previous.get("lastMaxId")
                current_max = max(int(r["id"]) for r in rows)
                if previous_max and current_max < int(previous_max):
                    raise RuntimeError("PUBLIC_PROFILE_STALE_SAMPLE")
                gap = bool(
                    previous_max
                    and previous_max not in {r["id"] for r in rows}
                    and current_max > int(previous_max)
                )
                return rows, {
                    "handle": handle,
                    "status": "ok",
                    "scanTs": stamp,
                    "posts": len(rows),
                    "wireBytes": size,
                    "lastMaxId": max(rows, key=lambda r: int(r["id"]))["id"],
                    "historyGap": gap,
                }
            except RuntimeError as exc:
                return [], {
                    "handle": handle,
                    "status": "failed",
                    "scanTs": stamp,
                    "error": str(exc),
                    "diagnostics": getattr(exc, "diagnostics", None),
                }

    pairs = await asyncio.gather(*[one(h) for h in names])
    rows = {r["id"]: r for records, _ in pairs for r in records}
    accounts = [report for _, report in pairs]
    failed = [x for x in accounts if x["status"] != "ok"]
    gaps = [x["handle"] for x in accounts if x.get("historyGap")]
    report = {
        "source": "x-public-profile",
        "seconds": round(time.monotonic() - started, 3),
        "watchedAccounts": len(names),
        "accountsRead": len(names) - len(failed),
        "failedAccounts": failed,
        "coverageGaps": gaps,
        "accountResults": accounts,
        "wireBytes": sum(x.get("wireBytes", 0) for x in accounts),
        "postCount": len(rows),
        "authorsSeen": len({r["author"].lower() for r in rows.values()}),
        "reachedHistory": not failed and not gaps,
        "error": "PUBLIC_SOURCE_UNAVAILABLE" if len(failed) == len(names) else None,
    }
    return sorted(rows.values(), key=lambda r: (r["publishedTs"], int(r["id"]))), report
