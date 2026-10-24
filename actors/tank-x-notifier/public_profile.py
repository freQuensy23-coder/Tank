"""Read the public HTML X sends to logged-out visitors. Never accepts cookies."""

import asyncio
import gzip
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from http.client import HTTPException

from bs4 import BeautifulSoup

LINK = re.compile(r"^/(?:@)?([A-Za-z0-9_]{1,15})/status/(\d+)(?:[/?#]|$)")


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


def fetch_profile(handle):
    request = urllib.request.Request(
        "https://x.com/" + urllib.parse.quote(handle),
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html",
            "Accept-Encoding": "gzip",
            "Cache-Control": "no-cache",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            if urllib.parse.urlsplit(response.url).hostname not in (
                "x.com",
                "www.x.com",
                "twitter.com",
                "www.twitter.com",
            ):
                raise RuntimeError("UNEXPECTED_PUBLIC_PROFILE_REDIRECT")
            compressed = response.read(2_000_001)
            if len(compressed) > 2_000_000:
                raise RuntimeError("PUBLIC_PROFILE_TOO_LARGE")
            raw = (
                gzip.decompress(compressed)
                if response.headers.get("Content-Encoding") == "gzip"
                else compressed
            )
            rows = parse_profile(raw.decode(), handle)
            return rows, len(compressed)
    except urllib.error.HTTPError as exc:
        raise RuntimeError("PUBLIC_PROFILE_HTTP_" + str(exc.code)) from None
    except RuntimeError:
        raise
    except (OSError, EOFError, UnicodeError, HTTPException, ValueError):
        raise RuntimeError("PUBLIC_PROFILE_CONNECTION_FAILED") from None


async def collect(config, known_ids, cutoff):
    started = time.monotonic()
    names = list(dict.fromkeys(h.lstrip("@").lower() for h in config["handles"]))
    if not names or any(not re.fullmatch(r"[a-z0-9_]{1,15}", h) for h in names):
        raise RuntimeError("INVALID_MONITORED_HANDLE")
    semaphore = asyncio.Semaphore(max(1, min(8, int(config.get("concurrency", 6)))))
    account_state = config.get("_accountState") or {}

    async def one(handle):
        async with semaphore:
            stamp = time.time()
            try:
                rows, size = await asyncio.to_thread(fetch_profile, handle)
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
