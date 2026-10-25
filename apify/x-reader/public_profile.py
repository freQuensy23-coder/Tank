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
from contextlib import AsyncExitStack, suppress
from datetime import UTC, datetime

import httpx
from bs4 import BeautifulSoup

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


def js_string(source, start):
    """Decode the double-quoted JavaScript string literal that starts at source[start]."""
    if source[start : start + 1] != '"':
        raise ValueError("JS_STRING_EXPECTED")
    parts, index = [], start + 1
    while index < len(source):
        if source[index] == '"':
            text = "".join(parts)
            return text.encode("utf-16", "surrogatepass").decode("utf-16"), index + 1
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


def valid_post_id(value):
    return isinstance(value, str) and POST_ID.fullmatch(value) is not None


def field(node, *keys):
    """The value at keys inside nested GraphQL objects, or None when any level is not an object."""
    for key in keys:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def embedded_tweets(soup):
    """Posts of the GraphQL results a signed-out x.com page embeds, by status ID; empty when absent."""
    tweets = {}
    for script in soup.find_all("script"):
        source = script.get_text()
        for match in RELAY_RESULT.finditer(source):
            try:
                pending = [json.loads(js_string(source, match.end())[0])]
            except (ValueError, UnicodeError):
                continue
            while pending:
                node = pending.pop()
                if isinstance(node, list):
                    pending.extend(node)
                elif isinstance(node, dict):
                    pending.extend(node.values())
                    kind = node.get("__typename")
                    tweet = (
                        node
                        if kind == "Tweet"
                        else node.get("tweet")
                        if kind == "TweetWithVisibilityResults"
                        else None
                    )
                    if isinstance(tweet, dict) and valid_post_id(tweet.get("rest_id")):
                        for post_id in [tweet["rest_id"], *edit_ids(tweet)]:
                            known = tweets.get(post_id)
                            if known is None or (tweet_text(known) is None and tweet_text(tweet) is not None):
                                tweets[post_id] = tweet
    return tweets


def edit_ids(tweet):
    """IDs of every version of an edited post; the page links the newest, the result names the first."""
    ids = field(tweet, "edit_control", "edit_tweet_ids") or field(
        tweet, "edit_control", "edit_control_initial", "edit_tweet_ids"
    )
    return [value for value in ids if valid_post_id(value)] if isinstance(ids, list) else []


def tweet_author(tweet):
    name = field(tweet, "core", "user_results", "result", "core", "screen_name")
    return name if isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_]{1,15}", name) else None


def tweet_text(tweet):
    """The complete post text with its links expanded, or None when the page carries no text."""
    note = field(tweet, "note_tweet", "note_tweet_results", "result")
    if isinstance(field(note, "text"), str):
        text, links = note["text"], field(note, "entity_set", "urls")
    else:
        full_text = field(tweet, "details", "full_text")
        if not isinstance(full_text, str):
            return None
        text, links = html.unescape(full_text), field(tweet, "url_entities")
    for link in links if isinstance(links, list) else []:
        if isinstance(link, dict) and isinstance(link.get("url"), str):
            expanded = link.get("expanded_url")
            text = text.replace(link["url"], expanded if isinstance(expanded, str) else "")
    return SHORT_LINK.sub("", text).strip()


def tweet_links(tweet):
    note = field(tweet, "note_tweet", "note_tweet_results", "result")
    links = field(note, "entity_set", "urls") if isinstance(note, dict) else field(tweet, "url_entities")
    return [
        link["expanded_url"]
        for link in (links if isinstance(links, list) else [])
        if isinstance(link, dict) and isinstance(link.get("expanded_url"), str)
    ]


def published_at(post_id, tweet=None):
    created = field(tweet, "details", "created_at_ms")
    if isinstance(created, int) and not isinstance(created, bool) and created > 0:
        return datetime.fromtimestamp(created / 1000, UTC).isoformat()
    if int(post_id) < FIRST_DATED_ID:
        return None
    return datetime.fromtimestamp(((int(post_id) >> 22) + SNOWFLAKE_EPOCH_MS) / 1000, UTC).isoformat()


def status_link(article):
    return next(
        (
            m
            for a in article.select("a[href]")
            if isinstance(href := a.get("href"), str) and (m := LINK.match(href))
        ),
        None,
    )


def own_text(article):
    """The text element of this post, not of a post it quotes."""
    return next((node for node in article.select(TEXT) if node.find_parent("article") is article), None)


def reference(relation, post_id, author, tweets):
    tweet = tweets.get(post_id)
    author = (tweet_author(tweet) if tweet else None) or author
    return {
        "relation": relation,
        "id": post_id,
        "author": author,
        "url": f"https://x.com/{author or 'i'}/status/{post_id}",
        "publishedAt": published_at(post_id, tweet),
        "text": tweet_text(tweet) if tweet else None,
    }


def post_references(post_id, tweet, article, tweets):
    """Posts this post quotes, replies to or links, with their text when the page carries it."""
    found = {}
    if tweet is not None:
        quoted_id = next(
            (
                value
                for value in (
                    field(tweet, "quoted_tweet_results", "rest_id"),
                    field(tweet, "quoted_tweet_results", "result", "rest_id"),
                    field(tweet, "quoted_tweet_results", "result", "tweet", "rest_id"),
                )
                if valid_post_id(value)
            ),
            None,
        )
        if quoted_id is not None:
            found[quoted_id] = reference("quoted", quoted_id, None, tweets)
            unavailable = field(tweet, "quoted_tweet_results", "result", "__typename") == "TweetTombstone"
            if unavailable and found[quoted_id]["text"] is None:
                found[quoted_id]["error"] = "QUOTED_POST_UNAVAILABLE"
        parent = field(tweet, "reply_to_results", "rest_id")
        if valid_post_id(parent) and parent not in found:
            name = field(tweet, "reply_to_user_results", "result", "core", "screen_name")
            found[parent] = reference("replied_to", parent, name if isinstance(name, str) else None, tweets)
        links = tweet_links(tweet)
    else:
        nested = article.find("article")
        link = status_link(nested) if nested else None
        if link and link.group(2) != post_id:
            found[link.group(2)] = reference("quoted", link.group(2), link.group(1), tweets)
            content = own_text(nested)
            found[link.group(2)]["text"] = content.get_text("", strip=False).strip() if content else None
        links = [
            href
            for a in article.select("a[href]")
            if isinstance(href := a.get("href"), str) and a.find_parent("article") is article
        ]
    for url in links:
        match = STATUS_URL.match(url)
        if match and match.group(2) != post_id and match.group(2) not in found:
            author = None if match.group(1).lower() == "i" else match.group(1)
            found[match.group(2)] = reference("linked", match.group(2), author, tweets)
    return list(found.values())


def parse_profile(raw, handle):
    soup = BeautifulSoup(raw, "html.parser")
    title = soup.title.get_text() if soup.title else ""
    if " / X" not in title or ("@" + handle.lower()) not in title.lower():
        raise RuntimeError("PUBLIC_PROFILE_NOT_READABLE")
    tweets = embedded_tweets(soup)
    posts = {}
    for article in soup.find_all("article"):
        if article.find_parent("article"):
            continue
        link = status_link(article)
        if not link or link.group(1).lower() != handle.lower():
            continue
        post_id, author = link.group(2), link.group(1)
        content = own_text(article)
        if content is None and article.find("article") is None and not article.select(MEDIA):
            raise RuntimeError("PUBLIC_POST_BODY_MISSING")
        tweet = tweets.get(post_id)
        text = tweet_text(tweet) if tweet else None
        if text is None:
            text = content.get_text("", strip=False).strip() if content else ""
        published = ((int(post_id) >> 22) + SNOWFLAKE_EPOCH_MS) / 1000
        posts[post_id] = {
            "id": post_id,
            "author": author,
            "text": text,
            "publishedTs": published,
            "publishedAt": datetime.fromtimestamp(published, UTC).isoformat(),
            "url": f"https://x.com/{author}/status/{post_id}",
            "source": "x-public-profile",
            "referencedPosts": post_references(post_id, tweet, article, tweets),
        }
    if not posts:
        raise RuntimeError("NO_PUBLIC_POSTS_VISIBLE")
    return sorted(posts.values(), key=lambda r: int(r["id"]))


def parse_post(raw, post_id):
    """The author, text and time of one post from its signed-out status page."""
    soup = BeautifulSoup(raw, "html.parser")
    tweet = embedded_tweets(soup).get(post_id)
    if tweet is not None and (text := tweet_text(tweet)) is not None:
        return {"author": tweet_author(tweet), "text": text, "publishedAt": published_at(post_id, tweet)}
    for article in soup.find_all("article"):
        link = status_link(article)
        if link and link.group(2) == post_id and (content := own_text(article)) is not None:
            return {"author": link.group(1), "text": content.get_text("", strip=False).strip()}
    raise RuntimeError("PUBLIC_POST_NOT_READABLE")


async def fetch_profile(handle, deadline=None, evidence_limit=BODY_LIMIT):
    return await fetch_public(
        PROFILE_ORIGIN + urllib.parse.quote(handle),
        lambda raw: parse_profile(raw, handle),
        "PROFILE",
        deadline,
        evidence_limit,
    )


async def fetch_post(author, post_id, deadline):
    url = PROFILE_ORIGIN + urllib.parse.quote(author or "i") + "/status/" + post_id
    found, _ = await fetch_public(
        url, lambda raw: parse_post(raw, post_id), "POST", deadline, REFERENCE_EVIDENCE_LIMIT
    )
    return found


async def fetch_public(url, parse, subject, deadline=None, evidence_limit=BODY_LIMIT):
    """GET one signed-out x.com page with bounded retries and parse it; failures carry diagnostics."""
    deadline = run_deadline() if deadline is None else deadline
    diagnostics = {
        "request": {"method": "GET", "url": url, "body": None},
        "attempts": [],
        "attemptCount": 0,
        "outcome": "exhausted",
    }
    reason = f"PUBLIC_{subject}_DEADLINE_EXHAUSTED"
    capture_limit = min(BODY_LIMIT, evidence_limit)

    async def trace(name, info):
        if name == "connection.connect_tcp.complete":
            cleanup.push_async_callback(info["return_value"].aclose)

    async with (
        AsyncExitStack() as cleanup,
        httpx.AsyncClient(follow_redirects=True, trust_env=False) as client,
    ):
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
                reason = f"PUBLIC_{subject}_HTTP_{response.status_code}"
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


async def resolve_references(pairs, account_state, semaphore, deadline):
    """Read the text of posts that new posts reply to or link but whose page did not carry it.

    Only posts above an account's cursor are new to Tank, so baseline reads fetch nothing.
    A post that cannot be read keeps its reference with the reason in "error".
    """
    pending = {}
    for records, report in pairs:
        cursor = (account_state.get(report["handle"]) or {}).get("lastMaxId")
        if report["status"] != "ok" or not cursor:
            continue
        for row in records:
            if int(row["id"]) <= int(cursor):
                continue
            for ref in row["referencedPosts"]:
                if ref["text"] is None and "error" not in ref:
                    pending.setdefault(ref["id"], []).append(ref)

    async def one(post_id, refs):
        async with semaphore:
            try:
                found = await fetch_post(refs[0]["author"], post_id, deadline)
            except RuntimeError as exc:
                for ref in refs:
                    ref["error"] = str(exc)
                return
        for ref in refs:
            ref.update({key: value for key, value in found.items() if value is not None})
            ref["url"] = f"https://x.com/{ref['author'] or 'i'}/status/{post_id}"

    selected = list(pending.items())
    for _, refs in selected[REFERENCE_FETCH_LIMIT:]:
        for ref in refs:
            ref["error"] = "REFERENCE_FETCH_LIMIT"
    await asyncio.gather(*(one(post_id, refs) for post_id, refs in selected[:REFERENCE_FETCH_LIMIT]))


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
    await resolve_references(pairs, account_state, semaphore, deadline)
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
