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
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from typing import Literal, Protocol, runtime_checkable

import httpx
from actor_contracts import (
    AccountFailure,
    AccountResult,
    AccountSuccess,
    ActorRequest,
    CollectionReport,
    DiagnosticAttempt,
    DiagnosticRequest,
    DiagnosticResponse,
    ProfileDiagnostics,
    PublicPost,
)
from bs4 import BeautifulSoup
from pydantic import JsonValue

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
    def __init__(self, reason: str, evidence: ProfileDiagnostics) -> None:
        super().__init__(reason)
        self.evidence = evidence

    @property
    def diagnostics(self) -> dict[str, JsonValue]:
        return self.evidence.model_dump(mode="json", exclude_unset=True)


def redact(value: str) -> str:
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


async def read_body(response: httpx.Response, parts: list[bytes], limit: int = BODY_LIMIT) -> None:
    size = 0
    async for part in response.aiter_raw():
        captured = part[: limit + 1 - size]
        parts.append(captured)
        size += len(captured)
        if size > limit:
            break


def response_evidence(
    response: httpx.Response,
    raw: bytes,
    read_error: str | None = None,
    limit: int = BODY_LIMIT,
    serialized_limit: int | None = None,
) -> DiagnosticResponse:
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
    return DiagnosticResponse(
        status=response.status_code,
        headers=headers,
        body=body,
        bodyEncoding="utf-8" if textual else "base64",
        bodyBytes=len(safe),
        bodySha256=hashlib.sha256(safe).hexdigest(),
        wireBytes=min(len(raw), limit),
        captureLimitBytes=limit,
        serializedBodyLimitBytes=serialized_limit,
        bodyTruncated=truncated or read_error is not None,
        omittedWireBytes=max(0, total - min(len(raw), limit)) if total is not None else None,
        readError=read_error,
        decodeError=encoding_error,
        redacted=redacted,
    )


def run_deadline() -> float:
    remaining = 100.0
    value = os.environ.get("ACTOR_TIMEOUT_AT")
    if value is not None:
        try:
            declared = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("INVALID_ACTOR_TIMEOUT_AT") from None
        if declared.tzinfo is None or declared.utcoffset() is None:
            raise ValueError("ACTOR_TIMEOUT_AT_REQUIRES_TIMEZONE")
        remaining = min(remaining, declared.timestamp() - time.time() - 2)
    return time.monotonic() + max(0, remaining)


def parse_profile(raw: str, handle: str) -> list[PublicPost]:
    soup = BeautifulSoup(raw, "html.parser")
    title = soup.title.get_text() if soup.title else ""
    if " / X" not in title or ("@" + handle.lower()) not in title.lower():
        raise RuntimeError("PUBLIC_PROFILE_NOT_READABLE")
    posts: dict[str, PublicPost] = {}
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
        posts[post_id] = PublicPost(
            id=post_id,
            author=author,
            text=content.get_text("", strip=False).strip() if content else "",
            publishedTs=published,
            publishedAt=datetime.fromtimestamp(published, UTC).isoformat(),
            url=f"https://x.com/{author}/status/{post_id}",
            source="x-public-profile",
        )
    if not posts:
        raise RuntimeError("NO_PUBLIC_POSTS_VISIBLE")
    return sorted(posts.values(), key=lambda r: int(r.id))


@runtime_checkable
class AsyncConnection(Protocol):
    async def aclose(self) -> None: ...


async def fetch_profile(
    handle: str,
    deadline: float | None = None,
    evidence_limit: int = BODY_LIMIT,
) -> tuple[list[PublicPost], int]:
    deadline = run_deadline() if deadline is None else deadline
    url = PROFILE_ORIGIN + urllib.parse.quote(handle)
    attempts: list[DiagnosticAttempt] = []
    outcome: Literal["exhausted", "deadline_exhausted", "permanent_failure"] = "exhausted"
    reason = "PUBLIC_PROFILE_DEADLINE_EXHAUSTED"
    capture_limit = min(BODY_LIMIT, evidence_limit)

    async def trace(name: str, info: dict[str, object]) -> None:
        if name == "connection.connect_tcp.complete":
            connection = info["return_value"]
            if not isinstance(connection, AsyncConnection):
                raise RuntimeError("INVALID_HTTP_CONNECTION_TRACE")
            cleanup.push_async_callback(connection.aclose)

    async with (
        AsyncExitStack() as cleanup,
        httpx.AsyncClient(follow_redirects=True, trust_env=False) as client,
    ):
        for index, delay in enumerate((0, 1, 5), 1):
            if deadline - time.monotonic() <= delay:
                outcome = "deadline_exhausted"
                break
            if delay:
                await asyncio.sleep(delay)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                outcome = "deadline_exhausted"
                break
            attempt_at = datetime.now(UTC).isoformat()
            response_capture: DiagnosticResponse | None = None
            retryable = False
            response = None
            parts: list[bytes] = []
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
                        extensions={"trace": trace},
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
                response_capture = response_evidence(
                    response, b"".join(parts), read_error, capture_limit, evidence_limit
                )
            if response_capture is None:
                attempt = DiagnosticAttempt(
                    attempt=index,
                    at=attempt_at,
                    error=reason,
                    retryable=retryable,
                )
            else:
                attempt = DiagnosticAttempt(
                    attempt=index,
                    at=attempt_at,
                    error=reason,
                    retryable=retryable,
                    response=response_capture,
                    status=response_capture.status,
                )
            attempts.append(attempt)
            if time.monotonic() >= deadline:
                outcome = "deadline_exhausted"
                break
            if not retryable:
                outcome = "permanent_failure"
                break
    raise ProfileFailure(
        reason,
        ProfileDiagnostics(
            request=DiagnosticRequest(method="GET", url=url, body=None),
            attempts=attempts,
            attemptCount=len(attempts),
            outcome=outcome,
        ),
    ) from None


async def collect(config: ActorRequest) -> tuple[list[PublicPost], CollectionReport]:
    started = time.monotonic()
    deadline = run_deadline()
    names = config.handles
    semaphore = asyncio.Semaphore(config.concurrency)
    evidence_limit = RUN_EVIDENCE_LIMIT // (3 * len(names))

    async def one(handle: str) -> tuple[list[PublicPost], AccountResult]:
        async with semaphore:
            stamp = time.time()
            try:
                rows, size = await fetch_profile(handle, deadline, evidence_limit)
                previous_max = config.cursors.get(handle)
                current_max = max(int(row.id) for row in rows)
                if previous_max is not None and current_max < int(previous_max):
                    raise RuntimeError("PUBLIC_PROFILE_STALE_SAMPLE")
                gap = (
                    previous_max is not None
                    and previous_max not in {row.id for row in rows}
                    and current_max > int(previous_max)
                )
                return rows, AccountSuccess(
                    handle=handle,
                    status="ok",
                    scanTs=stamp,
                    posts=len(rows),
                    wireBytes=size,
                    lastMaxId=max(rows, key=lambda row: int(row.id)).id,
                    historyGap=gap,
                )
            except RuntimeError as exc:
                return [], AccountFailure(
                    handle=handle,
                    status="failed",
                    scanTs=stamp,
                    error=str(exc),
                    diagnostics=exc.evidence if isinstance(exc, ProfileFailure) else None,
                )

    pairs = await asyncio.gather(*(one(handle) for handle in names))
    rows = {row.id: row for records, _ in pairs for row in records}
    accounts = [report for _, report in pairs]
    failed = [account for account in accounts if isinstance(account, AccountFailure)]
    successful = [account for account in accounts if isinstance(account, AccountSuccess)]
    gaps = [account.handle for account in successful if account.historyGap]
    report = CollectionReport(
        source="x-public-profile",
        seconds=round(time.monotonic() - started, 3),
        watchedAccounts=len(names),
        accountsRead=len(successful),
        failedAccounts=failed,
        coverageGaps=gaps,
        accountResults=accounts,
        wireBytes=sum(account.wireBytes for account in successful),
        postCount=len(rows),
        authorsSeen=len({row.author.lower() for row in rows.values()}),
        reachedHistory=not failed and not gaps,
        error="PUBLIC_SOURCE_UNAVAILABLE" if len(failed) == len(names) else None,
    )
    return sorted(rows.values(), key=lambda row: (row.publishedTs, int(row.id))), report
