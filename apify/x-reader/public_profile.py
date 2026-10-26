import asyncio
import base64
import gzip
import hashlib
import html
import io
import json
import os
import re
import time
import urllib.parse
import zlib
from collections.abc import Callable
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
    EmbeddedJsonDocument,
    EmbeddedTweet,
    ProfileDiagnostics,
    PublicPost,
    ReferencedPost,
    ReferencedPostContent,
    ReferenceReadFailure,
    ReferenceReadSuccess,
    ReferenceRelation,
    ReferenceResolution,
)
from bs4 import BeautifulSoup, Tag
from pydantic import JsonValue

LINK = re.compile(r"^/(?:@)?([A-Za-z0-9_]{1,15})/status/(\d+)(?:[/?#]|$)")
STATUS_URL = re.compile(
    r"^https?://(?:www\.|mobile\.)?(?:x|twitter)\.com/(?:@)?([A-Za-z0-9_]{1,15})/status/(\d{1,20})(?:[/?#]|$)",
    re.IGNORECASE,
)
POST_ID = re.compile(r"[1-9][0-9]{0,19}")
SHORT_LINK = re.compile(r"[ \t]*https://t\.co/[A-Za-z0-9]+")
RELAY_RESULT = re.compile(r'kind:"Result\.Ok",value:(?=")')
JS_TOKEN = re.compile(r'[^"\\]+|\\(x[0-9A-Fa-f]{2}|u[0-9A-Fa-f]{4}|.)', re.DOTALL)
JS_ESCAPES = {"b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v", "0": "\0"}
MEDIA = 'video, img[src*="/media/"], a[href*="/photo/"], a[href*="/video/"]'
TEXT = '.whitespace-pre-wrap, [data-testid="tweetText"]'
SNOWFLAKE_EPOCH_MS = 1288834974657
FIRST_DATED_ID = 10**11
BODY_LIMIT = 2_000_000
RUN_EVIDENCE_LIMIT = 12_000_000
REFERENCE_FETCH_LIMIT = 24
REFERENCE_EVIDENCE_LIMIT = 4096
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


def js_string(source: str, start: int) -> tuple[str, int]:
    """Decode the public page's double-quoted JavaScript literal without evaluation."""
    if source[start : start + 1] != '"':
        raise ValueError("JS_STRING_EXPECTED")
    parts: list[str] = []
    index = start + 1
    while index < len(source):
        if source[index] == '"':
            return "".join(parts).encode("utf-16", "surrogatepass").decode("utf-16"), index + 1
        match = JS_TOKEN.match(source, index)
        if match is None:
            break
        escape = match.group(1)
        if escape is None:
            parts.append(match.group(0))
        elif len(escape) > 1:
            parts.append(chr(int(escape[1:], 16)))
        else:
            parts.append(JS_ESCAPES.get(escape, escape))
        index = match.end()
    raise ValueError("UNTERMINATED_JS_STRING")


def field(node: JsonValue, *keys: str) -> JsonValue:
    """Traverse optional untrusted GraphQL enrichment, never an admitted domain record."""
    for key in keys:
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node


def post_identity(value: JsonValue) -> str | None:
    return value if isinstance(value, str) and POST_ID.fullmatch(value) is not None else None


def observed_author(value: JsonValue) -> str | None:
    return value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_]{1,15}", value) else None


def project_tweet(wire: dict[str, JsonValue]) -> EmbeddedTweet:
    """Project usable optional enrichment; malformed extension nodes remain unavailable."""
    identity = post_identity(wire["rest_id"])
    if identity is None:
        raise ValueError("EMBEDDED_POST_ID_REQUIRED")
    note = field(wire, "note_tweet", "note_tweet_results", "result")
    note_text = field(note, "text")
    full_text = field(wire, "details", "full_text")
    if isinstance(note_text, str):
        text: str | None = note_text
        links_wire = field(note, "entity_set", "urls")
    else:
        text = html.unescape(full_text) if isinstance(full_text, str) else None
        links_wire = field(wire, "url_entities")
    links: list[str] = []
    for link in links_wire if isinstance(links_wire, list) else []:
        if not isinstance(link, dict):
            continue
        expanded = link.get("expanded_url")
        short = link.get("url")
        if isinstance(expanded, str):
            links.append(expanded)
        if text is not None and isinstance(short, str):
            text = text.replace(short, expanded if isinstance(expanded, str) else "")
    if text is not None:
        text = SHORT_LINK.sub("", text).strip()
    edited = field(wire, "edit_control", "edit_tweet_ids")
    if not isinstance(edited, list) or not edited:
        edited = field(wire, "edit_control", "edit_control_initial", "edit_tweet_ids")
    edits = (
        [edit_id for value in edited if (edit_id := post_identity(value)) is not None]
        if isinstance(edited, list)
        else []
    )
    quote = next(
        (
            value
            for candidate in (
                field(wire, "quoted_tweet_results", "rest_id"),
                field(wire, "quoted_tweet_results", "result", "rest_id"),
                field(wire, "quoted_tweet_results", "result", "tweet", "rest_id"),
            )
            if (value := post_identity(candidate)) is not None
        ),
        None,
    )
    created = field(wire, "details", "created_at_ms")
    return EmbeddedTweet(
        id=identity,
        author=observed_author(field(wire, "core", "user_results", "result", "core", "screen_name")),
        text=text,
        links=links,
        created_at_ms=created
        if isinstance(created, int) and not isinstance(created, bool) and created > 0
        else None,
        edit_ids=edits,
        quoted_id=quote,
        quoted_unavailable=field(wire, "quoted_tweet_results", "result", "__typename") == "TweetTombstone",
        replied_to_id=post_identity(field(wire, "reply_to_results", "rest_id")),
        reply_author=observed_author(field(wire, "reply_to_user_results", "result", "core", "screen_name")),
    )


def unique_embedded_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("AMBIGUOUS_EMBEDDED_JSON_OBJECT")
        result[name] = value
    return result


def decode_embedded_json(source: str) -> JsonValue:
    """Check every decoded key before the authoritative typed JSON tree decoder."""
    json.loads(source, object_pairs_hook=unique_embedded_object)
    return EmbeddedJsonDocument.model_validate_json(source).root


def embedded_tweets(soup: BeautifulSoup) -> dict[str, EmbeddedTweet]:
    """Index named enrichment observations from the optional external JSON tree."""
    tweets: dict[str, EmbeddedTweet] = {}
    for script in soup.find_all("script"):
        source = script.get_text()
        for match in RELAY_RESULT.finditer(source):
            try:
                decoded, _ = js_string(source, match.end())
                pending: list[JsonValue] = [decode_embedded_json(decoded)]
            except (ValueError, UnicodeError):
                continue
            while pending:
                node = pending.pop()
                if isinstance(node, list):
                    pending.extend(node)
                elif isinstance(node, dict):
                    pending.extend(node.values())
                    kind = node.get("__typename")
                    wire = (
                        node
                        if kind == "Tweet"
                        else node.get("tweet")
                        if kind == "TweetWithVisibilityResults"
                        else None
                    )
                    if isinstance(wire, dict) and post_identity(wire.get("rest_id")) is not None:
                        tweet = project_tweet(wire)
                        for identity in [tweet.id, *tweet.edit_ids]:
                            known = tweets.get(identity)
                            if known is None or (known.text is None and tweet.text is not None):
                                tweets[identity] = tweet
    return tweets


def published_at(post_id: str, tweet: EmbeddedTweet | None = None) -> str | None:
    if tweet is not None and tweet.created_at_ms is not None:
        return datetime.fromtimestamp(tweet.created_at_ms / 1000, UTC).isoformat()
    if int(post_id) < FIRST_DATED_ID:
        return None
    return datetime.fromtimestamp(((int(post_id) >> 22) + SNOWFLAKE_EPOCH_MS) / 1000, UTC).isoformat()


def status_link(article: Tag) -> re.Match[str] | None:
    for anchor in article.select("a[href]"):
        href = anchor.get("href")
        if isinstance(href, str) and (match := LINK.match(href)) is not None:
            return match
    return None


def own_text(article: Tag) -> Tag | None:
    return next((node for node in article.select(TEXT) if node.find_parent("article") is article), None)


def reference(
    relation: ReferenceRelation, post_id: str, author: str | None, tweets: dict[str, EmbeddedTweet]
) -> ReferencedPost:
    tweet = tweets.get(post_id)
    observed = tweet.author if tweet is not None and tweet.author is not None else author
    return ReferencedPost(
        relation=relation,
        id=post_id,
        author=observed,
        url=f"https://x.com/{observed if observed is not None else 'i'}/status/{post_id}",
        publishedAt=published_at(post_id, tweet),
        text=tweet.text if tweet is not None else None,
    )


def post_references(
    post_id: str, tweet: EmbeddedTweet | None, article: Tag, tweets: dict[str, EmbeddedTweet]
) -> list[ReferencedPost]:
    found: dict[str, ReferencedPost] = {}
    if tweet is not None:
        if tweet.quoted_id is not None:
            quoted = reference("quoted", tweet.quoted_id, None, tweets)
            if tweet.quoted_unavailable and quoted.text is None:
                quoted = ReferencedPost.model_validate(
                    quoted.model_copy(update={"error": "QUOTED_POST_UNAVAILABLE"})
                )
            found[quoted.id] = quoted
        if tweet.replied_to_id is not None and tweet.replied_to_id not in found:
            replied = reference("replied_to", tweet.replied_to_id, tweet.reply_author, tweets)
            found[replied.id] = replied
        links = tweet.links
    else:
        nested = article.find("article")
        link = status_link(nested) if isinstance(nested, Tag) else None
        if link is not None and link.group(2) != post_id and isinstance(nested, Tag):
            quoted = reference("quoted", link.group(2), link.group(1), tweets)
            content = own_text(nested)
            quoted = ReferencedPost.model_validate(
                quoted.model_copy(
                    update={
                        "text": content.get_text("", strip=False).strip() if content is not None else None
                    }
                )
            )
            found[quoted.id] = quoted
        links = []
        for anchor in article.select("a[href]"):
            href = anchor.get("href")
            if isinstance(href, str) and anchor.find_parent("article") is article:
                links.append(href)
    for url in links:
        match = STATUS_URL.match(url)
        if match is not None and match.group(2) != post_id and match.group(2) not in found:
            linked = reference(
                "linked", match.group(2), None if match.group(1).lower() == "i" else match.group(1), tweets
            )
            found[linked.id] = linked
    return list(found.values())


def parse_profile(raw: str, handle: str) -> list[PublicPost]:
    soup = BeautifulSoup(raw, "html.parser")
    title = soup.title.get_text() if soup.title is not None else ""
    if " / X" not in title or ("@" + handle.lower()) not in title.lower():
        raise RuntimeError("PUBLIC_PROFILE_NOT_READABLE")
    tweets = embedded_tweets(soup)
    posts: dict[str, PublicPost] = {}
    for article in soup.find_all("article"):
        if not isinstance(article, Tag) or article.find_parent("article"):
            continue
        link = status_link(article)
        if link is None or link.group(1).lower() != handle.lower():
            continue
        post_id, author = link.group(2), link.group(1)
        content = own_text(article)
        if content is None and article.find("article") is None and not article.select(MEDIA):
            raise RuntimeError("PUBLIC_POST_BODY_MISSING")
        tweet = tweets.get(post_id)
        if tweet is not None and tweet.author is not None and tweet.author.lower() != author.lower():
            raise RuntimeError("PUBLIC_POST_AUTHOR_CONFLICT")
        text = tweet.text if tweet is not None else None
        if text is None:
            text = content.get_text("", strip=False).strip() if content is not None else ""
        published = ((int(post_id) >> 22) + SNOWFLAKE_EPOCH_MS) / 1000
        posts[post_id] = PublicPost(
            id=post_id,
            author=author,
            text=text,
            publishedTs=published,
            publishedAt=datetime.fromtimestamp(published, UTC).isoformat(),
            url=f"https://x.com/{author}/status/{post_id}",
            source="x-public-profile",
            referencedPosts=post_references(post_id, tweet, article, tweets),
        )
    if not posts:
        raise RuntimeError("NO_PUBLIC_POSTS_VISIBLE")
    return sorted(posts.values(), key=lambda post: int(post.id))


def parse_post(raw: str, post_id: str) -> ReferencedPostContent:
    soup = BeautifulSoup(raw, "html.parser")
    tweet = embedded_tweets(soup).get(post_id)
    if tweet is not None and tweet.text is not None:
        return ReferencedPostContent(
            author=tweet.author, text=tweet.text, publishedAt=published_at(post_id, tweet)
        )
    for article in soup.find_all("article"):
        if not isinstance(article, Tag):
            continue
        link = status_link(article)
        content = own_text(article)
        if link is not None and link.group(2) == post_id and content is not None:
            return ReferencedPostContent(
                author=link.group(1), text=content.get_text("", strip=False).strip(), publishedAt=None
            )
    raise RuntimeError("PUBLIC_POST_NOT_READABLE")


async def fetch_profile(
    handle: str,
    deadline: float | None = None,
    evidence_limit: int = BODY_LIMIT,
) -> tuple[list[PublicPost], int]:
    return await fetch_public(
        PROFILE_ORIGIN + urllib.parse.quote(handle),
        lambda raw: parse_profile(raw, handle),
        "PROFILE",
        deadline,
        evidence_limit,
    )


async def fetch_post(author: str | None, post_id: str, deadline: float) -> ReferencedPostContent:
    found, _ = await fetch_public(
        PROFILE_ORIGIN + urllib.parse.quote(author if author is not None else "i") + "/status/" + post_id,
        lambda raw: parse_post(raw, post_id),
        "POST",
        deadline,
        REFERENCE_EVIDENCE_LIMIT,
    )
    return found


@runtime_checkable
class AsyncConnection(Protocol):
    async def aclose(self) -> None: ...


async def fetch_public[T](
    url: str,
    parse: Callable[[str], T],
    subject: Literal["PROFILE", "POST"],
    deadline: float | None = None,
    evidence_limit: int = BODY_LIMIT,
) -> tuple[T, int]:
    deadline = run_deadline() if deadline is None else deadline
    attempts: list[DiagnosticAttempt] = []
    outcome: Literal["exhausted", "deadline_exhausted", "permanent_failure"] = "exhausted"
    reason = f"PUBLIC_{subject}_DEADLINE_EXHAUSTED"
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
                            raise RuntimeError(f"UNEXPECTED_PUBLIC_{subject}_REDIRECT")
                        await read_body(
                            response, parts, capture_limit if response.status_code >= 400 else BODY_LIMIT
                        )
                if response.status_code < 400:
                    compressed = b"".join(parts)
                    if len(compressed) > BODY_LIMIT:
                        raise RuntimeError(f"PUBLIC_{subject}_TOO_LARGE")
                    if response.headers.get("Content-Encoding", "").lower() == "gzip":
                        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as decoded:
                            raw = decoded.read(BODY_LIMIT + 1)
                    else:
                        raw = compressed
                    if len(raw) > BODY_LIMIT:
                        raise RuntimeError(f"PUBLIC_{subject}_TOO_LARGE")
                    result = parse(raw.decode())
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"PUBLIC_{subject}_DEADLINE_EXHAUSTED")
                    return result, len(compressed)
            except TimeoutError:
                reason = f"PUBLIC_{subject}_DEADLINE_EXHAUSTED"
                read_error = "RESPONSE_BODY_DEADLINE_EXHAUSTED"
            except RuntimeError as exc:
                reason = str(exc)
            except (httpx.HTTPError, OSError, EOFError):
                reason = f"PUBLIC_{subject}_CONNECTION_FAILED"
                retryable = True
                read_error = "RESPONSE_BODY_READ_FAILED"
            except (UnicodeError, ValueError, zlib.error):
                reason = f"PUBLIC_{subject}_NOT_READABLE"
            if response is not None and response.status_code >= 400:
                reason = f"PUBLIC_{subject}_HTTP_" + str(response.status_code)
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


async def resolve_references(
    pairs: list[tuple[list[PublicPost], AccountResult]],
    cursors: dict[str, str],
    semaphore: asyncio.Semaphore,
    deadline: float,
) -> list[tuple[list[PublicPost], AccountResult]]:
    pending: dict[str, list[ReferencedPost]] = {}
    for records, report in pairs:
        cursor = cursors.get(report.handle)
        if not isinstance(report, AccountSuccess) or cursor is None:
            continue
        for row in records:
            if int(row.id) <= int(cursor):
                continue
            for ref in row.referencedPosts:
                if ref.text is None and ref.error is None:
                    pending.setdefault(ref.id, []).append(ref)

    async def one(post_id: str, refs: list[ReferencedPost]) -> ReferenceResolution:
        async with semaphore:
            try:
                found = await fetch_post(refs[0].author, post_id, deadline)
            except RuntimeError as error:
                return ReferenceReadFailure(post_id=post_id, error=str(error))
        return ReferenceReadSuccess(post_id=post_id, content=found)

    selected = list(pending.items())
    resolved = {
        item.post_id: item
        for item in await asyncio.gather(
            *(one(post_id, refs) for post_id, refs in selected[:REFERENCE_FETCH_LIMIT])
        )
    }
    for post_id, _ in selected[REFERENCE_FETCH_LIMIT:]:
        resolved[post_id] = ReferenceReadFailure(post_id=post_id, error="REFERENCE_FETCH_LIMIT")
    result: list[tuple[list[PublicPost], AccountResult]] = []
    for records, report in pairs:
        updated: list[PublicPost] = []
        for row in records:
            references: list[ReferencedPost] = []
            for ref in row.referencedPosts:
                outcome = resolved.get(ref.id)
                if ref.text is not None or ref.error is not None or outcome is None:
                    references.append(ref)
                elif isinstance(outcome, ReferenceReadFailure):
                    references.append(
                        ReferencedPost.model_validate(ref.model_copy(update={"error": outcome.error}))
                    )
                else:
                    content = outcome.content
                    author = content.author if content.author is not None else ref.author
                    references.append(
                        ReferencedPost.model_validate(
                            ref.model_copy(
                                update={
                                    "author": author,
                                    "text": content.text,
                                    "publishedAt": content.publishedAt
                                    if content.publishedAt is not None
                                    else ref.publishedAt,
                                    "url": f"https://x.com/{author if author is not None else 'i'}/status/{ref.id}",
                                }
                            )
                        )
                    )
            updated.append(PublicPost.model_validate(row.model_copy(update={"referencedPosts": references})))
        result.append((updated, report))
    return result


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
    pairs = await resolve_references(pairs, config.cursors, semaphore, deadline)
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
