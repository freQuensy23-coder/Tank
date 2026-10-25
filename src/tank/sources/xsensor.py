"""X (S29) through the Apify actor that reads public X profiles; no X account or session.

Each pass is one synchronous actor run: the registry handles and the per-account
cursors go in, and every visible own post (usually the latest five per account),
the next cursors and per-account health come back. The token is APIFY_API_TOKEN.
A post that quotes, replies to or links another post carries that post's author,
URL and text; Tank stores them in the item's "quoted" text with their origin named.
Signed-out profiles show no reposts and no replies to other accounts.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from tank.logging import redact_text
from tank.sources.x_diagnostics import sanitize
from tank.util import iso

RUN_TIMEOUT_S = 130


class ApifyModel(BaseModel):
    """The part of the actor's response Tank reads; a missing field fails validation."""

    model_config = ConfigDict(frozen=True)


class ApifyReferencedPost(ApifyModel):
    """A post that a monitored post quotes, replies to or links; text is None when unreadable."""

    relation: Literal["quoted", "replied_to", "linked"]
    id: str
    author: str | None = None
    url: str
    published_at: str | None = Field(default=None, alias="publishedAt")
    text: str | None = None
    error: str | None = None

    @field_validator("published_at")
    @classmethod
    def utc_time(cls, value: str | None) -> str | None:
        """The publish time in Tank's UTC form; an unreadable time is left out, not fatal to the run."""
        if value is None:
            return None
        try:
            return iso(value)
        except (ValueError, OverflowError, OSError):
            return None

    def context(self, monitored: str) -> str:
        """The referenced post with its origin stated, as the agent and the classifier read it."""
        verb = {"quoted": "quoted", "replied_to": "replied to", "linked": "linked"}[self.relation]
        origin = f"@{self.author}" if self.author else "an unknown account"
        when = f", published {self.published_at}" if self.published_at else ""
        if self.text is None:
            body = f"[text unavailable: {self.error or 'NOT_RETURNED'}]"
        else:
            body = self.text or "[the post has no text]"
        return f"@{monitored} {verb} a post by {origin} ({self.url}{when}):\n{body}"


class ApifyPost(ApifyModel):
    id: str
    author: str
    text: str
    url: str
    published_at: str = Field(alias="publishedAt")
    referenced_posts: list[ApifyReferencedPost] = Field(default_factory=list, alias="referencedPosts")

    def row(self, raw: bytes) -> dict[str, object]:
        """The post as the source poller ingests it; raw is the actor response it came in."""
        row: dict[str, object] = {
            "external_id": self.id,
            "author": self.author,
            "monitored_author": self.author,
            "title": "",
            "text": self.text,
            "url": self.url,
            "published_at": iso(self.published_at),
            "published_at_precision": "EXACT",
            "_payload": raw,
        }
        if self.referenced_posts:
            row["quoted"] = "\n\n".join(post.context(self.author) for post in self.referenced_posts)
        return row


class ApifyAccount(ApifyModel):
    handle: str
    status: Literal["ok", "failed"]
    posts: int = 0
    history_gap: bool = Field(default=False, alias="historyGap")
    error: str | None = None
    diagnostics: dict | None = None


class ApifyRun(ApifyModel):
    health: Literal["healthy", "degraded", "failed"]
    posts: list[ApifyPost]
    accounts: list[ApifyAccount]
    next_cursors: dict[str, str] = Field(alias="nextCursors")
    error: str | None = None


@dataclass(frozen=True)
class AccountFailure:
    """A failed account, or the run as a whole when account is None."""

    account: str | None
    reason: str
    diagnostics: dict | None = None


@dataclass(frozen=True)
class Scan:
    """One pass: new post rows, next cursors, failed accounts and accounts with a history gap."""

    rows: list[dict[str, object]]
    cursors: dict[str, str]
    failures: list[AccountFailure]
    gaps: list[str]


def scan(run: ApifyRun, accounts: list[str], cursors: Mapping[str, str], raw: bytes) -> Scan:
    """New posts and next cursors of one actor run over the requested accounts.

    The actor returns every visible post each time. An account without a cursor is a
    baseline: its posts are not ingested, so there is no backfill. After that a post
    is new when its ID is above the account's cursor. A failed run raises, so the
    cursors stay where they were. Every requested account must come back: one
    missing from the run, or read with posts but without a next cursor, is a failure.
    A degraded run that names no failure or gap is recorded as a run-level failure.
    """
    if run.health == "failed" and not any(a.status == "failed" and a.diagnostics for a in run.accounts):
        raise RuntimeError(f"Apify X run failed: {run.error}")
    rows = [
        post.row(raw)
        for post in run.posts
        if post.author.lower() in cursors and int(post.id) > int(cursors[post.author.lower()])
    ]
    failures = [
        AccountFailure(a.handle, a.error or "FAILED", a.diagnostics)
        for a in run.accounts
        if a.status == "failed"
    ]
    reported = {account.handle.lower(): account for account in run.accounts}
    for handle in accounts:
        account = reported.get(handle.lower())
        if account is None:
            failures.append(AccountFailure(handle, "MISSING_FROM_RUN"))
        elif account.status == "ok" and account.posts and handle.lower() not in run.next_cursors:
            failures.append(AccountFailure(handle, "NO_CURSOR"))
    gaps = [account.handle for account in run.accounts if account.history_gap]
    if run.health == "degraded" and not failures and not gaps:
        failures.append(AccountFailure(None, run.error or "RUN_DEGRADED"))
    failed_handles = {failure.account.lower() for failure in failures if failure.account}
    next_cursors = dict(cursors)
    next_cursors.update(
        {
            handle: cursor
            for handle, cursor in run.next_cursors.items()
            if handle.lower() not in failed_handles and run.health != "failed"
        }
    )
    if run.health == "failed":
        rows = []
    else:
        rows = [row for row in rows if str(row["author"]).lower() not in failed_handles]
    return Scan(rows=rows, cursors=next_cursors, failures=failures, gaps=gaps)


class XSensor:
    def __init__(self, http: httpx.AsyncClient, token: str | None) -> None:
        self.http = http
        self.token = token

    async def posts(self, url: str, accounts: list[str], cursors: Mapping[str, str]) -> Scan:
        """Run the actor once over the accounts; see `scan`."""
        if not self.token:
            raise ValueError("APIFY_API_TOKEN is not set")
        known = {
            account.lower(): cursors[account.lower()] for account in accounts if account.lower() in cursors
        }
        try:
            response = await self.http.post(
                url,
                json={"handles": accounts, "cursors": known} if known else {"handles": accounts},
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=RUN_TIMEOUT_S,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            message = redact_text(str(exc), (self.token, repr(self.token)[1:-1]))
            raise RuntimeError(f"Apify X run request failed: {message}") from None
        run = ApifyRun.model_validate_json(response.content)
        run = run.model_copy(
            update={
                "accounts": [
                    account.model_copy(update={"diagnostics": sanitize(account.diagnostics, self.token)})
                    for account in run.accounts
                ]
            }
        )
        return scan(run, accounts, cursors, response.content)
