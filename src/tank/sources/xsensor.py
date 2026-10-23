"""Read-only X sensor: the registry accounts' public profile pages in Chromium (02 §3.7).

No X account, session or API key. A signed-out profile page embeds the account's
latest five own posts (a pinned post counts among them) as GraphQL JSON in the page
document. The sensor reads them from there and keeps the document as each new
post's raw evidence. Reposts and protected accounts are not visible signed out.

X answers Chromium's headless shell, and any user agent naming HeadlessChrome,
with HTTP 403, so the sensor runs the full Chromium build under the browser's own
user agent without "Headless".
"""

import asyncio
import html
import json
import re
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Literal, Self

from playwright.async_api import Browser, BrowserContext, Playwright, Route, async_playwright
from pydantic import BaseModel, ConfigDict, Field, model_validator

from tank.util import iso

PROFILE_URL = "https://x.com/{account}"
EMBEDDED_RESULT = re.compile(r'kind:"Result\.Ok",value:"((?:[^"\\]|\\.)*)"')
JS_HEX_ESCAPE = re.compile(r"\\\\|\\x([0-9a-fA-F]{2})")

type ProfileRead = tuple[int, bytes] | Exception


class ProfileError(Exception):
    """An account's profile page could not be read; the message is the reason."""


class XModel(BaseModel):
    """The part of X's GraphQL JSON the sensor reads; a missing field fails validation."""

    model_config = ConfigDict(frozen=True)


class ScreenName(XModel):
    screen_name: str


class User(XModel):
    core: ScreenName


class UserResults(XModel):
    result: User


class Author(XModel):
    user_results: UserResults


class Details(XModel):
    full_text: str
    created_at_ms: int


class Link(XModel):
    url: str
    expanded_url: str


class Media(XModel):
    media_url_https: str


class NoteLinks(XModel):
    urls: list[Link] = []


class Note(XModel):
    text: str | None = None
    entity_set: NoteLinks = NoteLinks()


class NoteResults(XModel):
    result: Note


class NoteTweet(XModel):
    note_tweet_results: NoteResults


class PostText(XModel):
    """The text of a post. A quote inside a quote arrives without it, so every field is optional."""

    details: Details | None = None
    note_tweet: NoteTweet | None = None
    url_entities: list[Link] = []

    @model_validator(mode="before")
    @classmethod
    def unwrap_visibility(cls, data: object) -> object:
        """X wraps a post with visibility limits as TweetWithVisibilityResults."""
        if isinstance(data, dict) and data.get("__typename") == "TweetWithVisibilityResults":
            return data.get("tweet")
        return data

    @property
    def text(self) -> str | None:
        """Full text, from the note of a long post, with t.co links expanded."""
        note = self.note_tweet.note_tweet_results.result if self.note_tweet else None
        if note and note.text:
            text, links = note.text, [*note.entity_set.urls, *self.url_entities]
        elif self.details:
            text, links = self.details.full_text, self.url_entities
        else:
            return None
        for link in links:
            text = text.replace(link.url, link.expanded_url)
        return html.unescape(text).strip()


class QuotedResults(XModel):
    result: PostText | None = None


class Tweet(PostText):
    rest_id: str
    core: Author
    details: Details
    media_entities2: list[Media] = []
    quoted_tweet_results: QuotedResults | None = None


class Unavailable(XModel):
    typename: Literal["TweetTombstone", "TweetUnavailable"] = Field(alias="__typename")


class TweetResults(XModel):
    result: Tweet | Unavailable | None = None


class TimelineTweet(XModel):
    tweet_results: TweetResults


class EntryContent(XModel):
    content: TimelineTweet | None = None


class Entry(XModel):
    entry_id: str
    content: EntryContent


class Instruction(XModel):
    typename: str = Field(alias="__typename")
    entries: list[Entry] = []
    entry: Entry | None = None


class Timeline(XModel):
    instructions: list[Instruction]


class OriginalsTimeline(XModel):
    timeline: Timeline


class ProfileUser(XModel):
    profile_user_originals_timeline: OriginalsTimeline | None = None


class UserByScreenName(XModel):
    result: ProfileUser | None = None


class EmbeddedData(XModel):
    user_result_by_screen_name: UserByScreenName | None = None


class Embedded(XModel):
    data: EmbeddedData | None = None


@dataclass(frozen=True)
class XPost:
    """One post on a monitored account's profile."""

    status_id: int
    account: str
    author: str
    text: str
    quoted: str | None
    media: tuple[str, ...]
    published_at: str
    pinned: bool

    @classmethod
    def from_tweet(cls, tweet: Tweet, account: str, *, pinned: bool) -> Self:
        quoted = tweet.quoted_tweet_results.result if tweet.quoted_tweet_results else None
        return cls(
            status_id=int(tweet.rest_id),
            account=account,
            author=tweet.core.user_results.result.core.screen_name,
            text=tweet.text or "",
            quoted=quoted.text if quoted else None,
            media=tuple(media.media_url_https for media in tweet.media_entities2),
            published_at=iso(tweet.details.created_at_ms),
            pinned=pinned,
        )

    def row(self, raw: bytes) -> dict[str, object]:
        """The post as the source poller ingests it; raw is the page it was read from."""
        return {
            "external_id": str(self.status_id),
            "author": self.author,
            "monitored_author": self.account,
            "title": "",
            "text": self.text,
            "quoted": self.quoted,
            "media": list(self.media),
            "url": f"https://x.com/{self.author}/status/{self.status_id}",
            "published_at": self.published_at,
            "published_at_precision": "EXACT",
            "pinned": self.pinned,
            "_payload": raw,
        }


@dataclass(frozen=True)
class AccountFailure:
    account: str
    reason: str


@dataclass(frozen=True)
class Scan:
    """One pass over the profiles: new post rows, next cursors and account health."""

    rows: list[dict[str, object]]
    cursors: dict[str, str]
    failures: list[AccountFailure]
    gaps: list[str]


def js_string(literal: str) -> str:
    """Decode a double-quoted JavaScript string literal; X also uses \\xHH escapes, which JSON lacks."""
    escaped = JS_HEX_ESCAPE.sub(lambda match: "\\u00" + match[1] if match[1] else match[0], literal)
    decoded: str = json.loads(f'"{escaped}"', strict=False)
    return decoded


def embedded_timeline(document: str) -> Timeline:
    for match in EMBEDDED_RESULT.finditer(document):
        try:
            embedded = Embedded.model_validate_json(js_string(match.group(1)))
        except ValueError:
            raise ProfileError("PROFILE_DATA_INVALID") from None
        user = embedded.data.user_result_by_screen_name if embedded.data else None
        profile = user.result.profile_user_originals_timeline if user and user.result else None
        if profile:
            return profile.timeline
    raise ProfileError("TIMELINE_MISSING")


def profile_posts(document: str, account: str) -> list[XPost]:
    """The posts on a signed-out profile page.

    A change in X's page format fails validation or leaves a tweet entry without
    its tweet; both raise ProfileError instead of reading as an empty profile.
    """
    posts: dict[int, XPost] = {}
    for instruction in embedded_timeline(document).instructions:
        pinned = instruction.typename == "TimelinePinEntry"
        for entry in [*instruction.entries, *([instruction.entry] if instruction.entry else [])]:
            if not entry.entry_id.startswith("tweet-"):
                continue
            if entry.content.content is None:
                raise ProfileError("PROFILE_DATA_INVALID")
            tweet = entry.content.content.tweet_results.result
            if isinstance(tweet, Tweet):
                post = XPost.from_tweet(tweet, account, pinned=pinned)
                posts.setdefault(post.status_id, post)
    return list(posts.values())


def page_body(read: ProfileRead) -> bytes:
    if isinstance(read, Exception):
        message = str(read).splitlines()[0] if str(read) else type(read).__name__
        raise ProfileError(f"NAVIGATION_FAILED: {message}")
    status, body = read
    if status != 200:
        raise ProfileError(f"HTTP_{status}")
    return body


def scan(reads: Mapping[str, ProfileRead], cursors: Mapping[str, str]) -> Scan:
    """New posts, next cursors, failed accounts and history gaps of one pass.

    The first read of an account only records its newest post ID, so posts made
    before Tank watched it are never ingested; after that a post is new when its ID
    is above the cursor. When every visible post besides the pin is newer than the
    cursor, the account posted more than its profile shows since the last pass: a
    history gap. A failed account keeps its cursor; the pass fails when all failed.
    """
    rows: list[dict[str, object]] = []
    failures: list[AccountFailure] = []
    gaps: list[str] = []
    next_cursors = dict(cursors)
    for account, read in reads.items():
        try:
            body = page_body(read)
            posts = profile_posts(body.decode(errors="replace"), account)
        except ProfileError as exc:
            failures.append(AccountFailure(account, str(exc)))
            continue
        if not posts:
            continue
        newest = max(post.status_id for post in posts)
        key = account.lower()
        cursor = int(cursors[key]) if key in cursors else None
        next_cursors[key] = str(newest if cursor is None else max(newest, cursor))
        if cursor is None:
            continue
        regular = [post.status_id for post in posts if not post.pinned]
        if regular and min(regular) > cursor:
            gaps.append(account)
        rows.extend(post.row(body) for post in posts if post.status_id > cursor)
    if reads and len(failures) == len(reads):
        reasons = Counter(failure.reason for failure in failures)
        raise RuntimeError(
            "X public profiles unavailable, every account failed: "
            + ", ".join(f"{reason} x{count}" for reason, count in reasons.most_common())
        )
    return Scan(rows=rows, cursors=next_cursors, failures=failures, gaps=gaps)


async def document_only(route: Route) -> None:
    """Only the profile document is read; its scripts, media and API calls are never fetched."""
    if route.request.resource_type == "document":
        await route.fallback()
    else:
        await route.abort()


class XSensor:
    """The service Chromium process and the signed-out pass over the X profiles."""

    def __init__(self, *, headless: bool = True) -> None:
        self.headless = headless
        self.open_lock = asyncio.Lock()
        self.scan_lock = asyncio.Lock()
        self.driver: Playwright | None = None
        self.chromium: Browser | None = None
        self.user_agent = ""

    async def browser(self) -> Browser:
        """The service Chromium; the public S19 renderer opens its own context in it."""
        async with self.open_lock:
            if self.chromium is None:
                self.driver = await async_playwright().start()
                try:
                    self.chromium = await self.driver.chromium.launch(
                        channel="chromium", headless=self.headless
                    )
                    page = await self.chromium.new_page()
                    agent = await page.evaluate("navigator.userAgent")
                    await page.close()
                    self.user_agent = agent.replace("HeadlessChrome/", "Chrome/")
                except Exception:
                    await self.close()
                    raise
            return self.chromium

    async def close(self) -> None:
        if self.chromium:
            await self.chromium.close()
        if self.driver:
            await self.driver.stop()
        self.chromium = None
        self.driver = None

    @staticmethod
    async def read(context: BrowserContext, account: str) -> tuple[int, bytes]:
        """HTTP status and document bytes of one profile page."""
        page = await context.new_page()
        try:
            response = await page.goto(
                PROFILE_URL.format(account=account), wait_until="commit", timeout=20000
            )
            if response is None:
                raise RuntimeError("Profile navigation returned no response")
            return response.status, await response.body()
        finally:
            await page.close()

    async def posts(self, accounts: list[str], cursors: Mapping[str, str], *, concurrency: int = 6) -> Scan:
        """Read every account's profile once in a fresh signed-out context; see `scan`."""
        async with self.scan_lock:
            browser = await self.browser()
            context = await browser.new_context(
                user_agent=self.user_agent, locale="en-US", service_workers="block"
            )
            semaphore = asyncio.Semaphore(concurrency)

            async def visit(account: str) -> ProfileRead:
                async with semaphore:
                    try:
                        return await self.read(context, account)
                    except Exception as exc:
                        return exc

            try:
                await context.route("**/*", document_only)
                reads = await asyncio.gather(*(visit(account) for account in accounts))
            finally:
                await context.close()
        return scan(dict(zip(accounts, reads, strict=True)), cursors)

    async def run(self, stop: asyncio.Event, poll: Callable[[], Awaitable[object]]) -> None:
        """Own the X feed control loop; polling deadlines remain durable in the service."""
        while not stop.is_set():
            started = asyncio.get_running_loop().time()
            await poll()
            remaining = max(0, 15 - (asyncio.get_running_loop().time() - started))
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=remaining)
