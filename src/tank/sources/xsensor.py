"""Validated public-profile X collection through the configured Apify actor.

Quoted, replied-to and linked post context retains its reported origin while the
original actor response remains the raw evidence and cursor admission boundary.
"""

import base64
import binascii
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Annotated, Literal, Self
from urllib.parse import unquote_plus, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.http_policy import HttpAdmission, HttpRefused
from tank.contracts.json import JsonDocument, decode_unique_json
from tank.contracts.source_errors import XDiagnostics
from tank.contracts.source_items import DiscoveryEnvelope, DiscoveryObservation, SocialDiscovery
from tank.contracts.sources import HttpFetchPlan
from tank.logging import redact_text
from tank.sources.http_client import RequestStarted, SourceHttpTransport
from tank.sources.x_diagnostics import sanitize
from tank.util import iso

RUN_TIMEOUT_S = 130
HANDLE = re.compile(r"@?[A-Za-z0-9_]{1,15}")
CURSOR = re.compile(r"[0-9]+")


def account_key(handle: str) -> str:
    if HANDLE.fullmatch(handle) is None:
        raise ValueError("X account requires a declared profile handle")
    return handle.lstrip("@").lower()


def sensitive_text(value: str, secrets: Sequence[str]) -> bool:
    if any(secret in value or secret in unquote_plus(value) for secret in secrets):
        return True
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error):
        return False
    return any(secret.encode() in decoded for secret in secrets)


def contains_credential(value: JsonValue, secrets: Sequence[str]) -> bool:
    if isinstance(value, str):
        return sensitive_text(value, secrets)
    if isinstance(value, list):
        return any(contains_credential(child, secrets) for child in value)
    if isinstance(value, dict):
        return any(
            sensitive_text(name, secrets) or contains_credential(child, secrets)
            for name, child in value.items()
        )
    return False


def redacted_projection(value: JsonValue, secrets: Sequence[str]) -> JsonValue:
    if isinstance(value, str):
        return "[REDACTED]" if sensitive_text(value, secrets) else value
    if isinstance(value, list):
        return [redacted_projection(child, secrets) for child in value]
    if isinstance(value, dict):
        return {
            "[REDACTED]" if sensitive_text(name, secrets) else name: redacted_projection(child, secrets)
            for name, child in value.items()
        }
    return value


def diagnostic_projection(value: JsonValue, *, token: str, secrets: Sequence[str]) -> JsonValue:
    projected = redacted_projection(value, secrets)
    if not isinstance(value, dict) or not isinstance(projected, dict):
        return projected
    if "accounts" not in value or "accounts" not in projected:
        return projected
    originals = value["accounts"]
    replacements = projected["accounts"]
    if not isinstance(originals, list) or not isinstance(replacements, list):
        return projected
    for original, replacement in zip(originals, replacements, strict=True):
        if not isinstance(original, dict) or not isinstance(replacement, dict):
            continue
        if "diagnostics" not in original or original["diagnostics"] is None:
            continue
        raw_diagnostics = original["diagnostics"]
        if not isinstance(raw_diagnostics, dict):
            raise ValueError("X response contains credential material")
        approved = sanitize(raw_diagnostics, token)
        wire = JsonDocument.validate_value(approved.model_dump(mode="json"))
        if contains_credential(wire, secrets):
            raise ValueError("X response contains credential material")
        replacement["diagnostics"] = wire
    return projected


class XPostObservation(DiscoveryEnvelope):
    discovery: SocialDiscovery
    payload: bytes


class ApifyModel(BaseModel):
    """Concrete wire projection; extra actor telemetry remains in retained response bytes."""

    model_config = ConfigDict(strict=True, frozen=True, extra="ignore", hide_input_in_errors=True)


class ApifyReferencedPost(ApifyModel):
    """A post that a monitored post quotes, replies to or links; text is None when unreadable."""

    relation: Literal["quoted", "replied_to", "linked"]
    id: str = Field(pattern=r"^[0-9]{1,20}$")
    author: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_]{1,15}$")
    url: str
    published_at: str | None = Field(default=None, alias="publishedAt")
    text: str | None = None
    error: str | None = Field(default=None, min_length=1, pattern=r"\S")

    @model_validator(mode="after")
    def referenced_identity(self) -> Self:
        if self.error is not None and self.text is not None:
            raise ValueError("Referenced X body conflicts with its recorded failure")
        parts = urlsplit(self.url)
        match = re.fullmatch(r"/([A-Za-z0-9_]{1,15})/status/([0-9]{1,20})/?", parts.path)
        if (
            parts.scheme != "https"
            or parts.hostname not in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}
            or parts.username is not None
            or parts.password is not None
            or match is None
            or match[2] != self.id
            or (self.author is not None and account_key(match[1]) != account_key(self.author))
        ):
            raise ValueError("Referenced X URL conflicts with its reported identity")
        return self

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
            body = f"[text unavailable: {self.error if self.error is not None else 'NOT_RETURNED'}]"
        else:
            body = self.text or "[the post has no text]"
        return f"@{monitored} {verb} a post by {origin} ({self.url}{when}):\n{body}"


class ApifyPost(ApifyModel):
    id: str = Field(pattern=r"^[0-9]+$")
    author: str
    text: str
    url: str
    published_at: str = Field(alias="publishedAt")
    referenced_posts: list[ApifyReferencedPost] = Field(default_factory=list, alias="referencedPosts")

    @field_validator("author")
    @classmethod
    def valid_author(cls, value: str) -> str:
        account_key(value)
        return value

    @field_validator("published_at")
    @classmethod
    def declared_instant(cls, value: str) -> str:
        if datetime.fromisoformat(value).tzinfo is None:
            raise ValueError("X publication requires an explicit timezone")
        return value

    @model_validator(mode="after")
    def own_post_identity(self) -> Self:
        parts = urlsplit(self.url)
        match = re.fullmatch(r"/([A-Za-z0-9_]+)/status/([0-9]+)/?", parts.path)
        if (
            parts.scheme != "https"
            or parts.hostname not in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}
            or match is None
            or account_key(match[1]) != account_key(self.author)
            or match[2] != self.id
        ):
            raise ValueError("X post URL conflicts with its declared author or status identity")
        return self

    def row(self, raw: bytes) -> XPostObservation:
        discovery = SocialDiscovery(
            source_id="S29",
            external_id=self.id,
            author=self.author,
            monitored_author=self.author,
            title="",
            text=self.text,
            url=self.url,
            published_at=iso(self.published_at),
            published_at_precision="EXACT",
            published_prec="EXACT",
        )
        if self.referenced_posts:
            discovery = SocialDiscovery.model_validate(
                discovery.model_copy(
                    update={
                        "quoted": "\n\n".join(post.context(self.author) for post in self.referenced_posts)
                    }
                )
            )
        return XPostObservation(discovery=discovery, observation=DiscoveryObservation(), payload=raw)


class ApifyOkAccount(ApifyModel):
    handle: str
    status: Literal["ok"]
    posts: int = Field(ge=0)
    history_gap: bool = Field(alias="historyGap")
    error: str | None = None

    @field_validator("handle")
    @classmethod
    def valid_handle(cls, value: str) -> str:
        account_key(value)
        return value


class ApifyFailedAccount(ApifyModel):
    handle: str
    status: Literal["failed"]
    diagnostics: dict[str, JsonValue] | None = None
    error: str = Field(min_length=1)
    history_gap: bool = Field(default=False, alias="historyGap")

    @field_validator("error")
    @classmethod
    def declared_failure(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Failed X account requires error evidence")
        return value

    @field_validator("handle")
    @classmethod
    def valid_handle(cls, value: str) -> str:
        account_key(value)
        return value


type ApifyAccount = Annotated[ApifyOkAccount | ApifyFailedAccount, Field(discriminator="status")]


class ApifyRun(ApifyModel):
    health: Literal["healthy", "degraded", "failed"]
    posts: list[ApifyPost]
    accounts: list[ApifyAccount]
    next_cursors: dict[str, str] = Field(alias="nextCursors")
    error: str | None = None

    @field_validator("next_cursors")
    @classmethod
    def valid_cursors(cls, value: dict[str, str]) -> dict[str, str]:
        result: dict[str, str] = {}
        for handle, cursor in value.items():
            key = account_key(handle)
            if key in result or CURSOR.fullmatch(cursor) is None:
                raise ValueError("X actor cursor identities are malformed or duplicated")
            result[key] = cursor
        return result

    @model_validator(mode="after")
    def unique_reports(self) -> Self:
        keys = [account_key(account.handle) for account in self.accounts]
        if len(set(keys)) != len(keys):
            raise ValueError("X actor returned duplicate account reports")
        if (
            self.health == "failed"
            and not (self.error is not None and self.error.strip())
            and not any(isinstance(account, ApifyFailedAccount) for account in self.accounts)
        ):
            raise ValueError("Failed X actor run requires its error evidence")
        return self


class AccountFailure(DomainModel):
    account: str | None
    reason: str = Field(min_length=1)
    diagnostics: XDiagnostics | None = None


class Scan(DomainModel):
    rows: list[XPostObservation]
    cursors: dict[str, str]
    failures: list[AccountFailure]
    gaps: list[str]


def cursor_state(cursors: Mapping[str, str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for handle, cursor in cursors.items():
        key = account_key(handle)
        if key in result or CURSOR.fullmatch(cursor) is None:
            raise ValueError("Stored X account cursor is malformed or duplicated")
        result[key] = cursor
    return result


def scan(
    run: ApifyRun,
    accounts: list[str],
    cursors: Mapping[str, str],
    raw: bytes,
    *,
    token: str | None = None,
    retain_posts: bool = True,
) -> Scan:
    """Advance an existing boundary only after the requested account reports a complete range.

    A first observed head establishes the explicit monitoring baseline; no prior
    cursor exists whose historical range could be acknowledged by that read.
    """
    requested = {account_key(account) for account in accounts}
    if len(requested) != len(accounts):
        raise ValueError("X scan requested duplicate account identities")
    previous = cursor_state(cursors)
    diagnostic_failure = any(
        isinstance(account, ApifyFailedAccount)
        and account.diagnostics is not None
        and account_key(account.handle) in requested
        for account in run.accounts
    )
    if run.health == "failed" and not diagnostic_failure:
        reason = (
            run.error
            if run.error is not None and run.error.strip()
            else "; ".join(
                account.error for account in run.accounts if isinstance(account, ApifyFailedAccount)
            )
        )
        raise RuntimeError(f"Apify X run failed: {reason}")
    rows = [
        post.row(raw)
        for post in run.posts
        if retain_posts
        and account_key(post.author) in requested
        and account_key(post.author) in previous
        and int(post.id) > int(previous[account_key(post.author)])
    ]
    reported = {account_key(account.handle): account for account in run.accounts}
    failures = [
        AccountFailure(
            account=account.handle,
            reason=account.error,
            diagnostics=None if account.diagnostics is None else sanitize(account.diagnostics, token),
        )
        for account in run.accounts
        if isinstance(account, ApifyFailedAccount) and account_key(account.handle) in requested
    ]
    updated = dict(previous)
    for handle in accounts:
        key = account_key(handle)
        account = reported.get(key)
        if account is None:
            failures.append(AccountFailure(account=handle, reason="MISSING_FROM_RUN"))
            continue
        if isinstance(account, ApifyFailedAccount) or run.health == "failed" or not retain_posts:
            continue
        next_cursor = run.next_cursors.get(key)
        if next_cursor is None:
            if account.posts:
                failures.append(AccountFailure(account=handle, reason="NO_CURSOR"))
            continue
        if key in previous and int(next_cursor) < int(previous[key]):
            failures.append(AccountFailure(account=handle, reason="CURSOR_REGRESSION"))
            continue
        if account.history_gap and key in previous:
            continue
        updated[key] = next_cursor
    gaps = [
        account.handle
        for account in run.accounts
        if account.history_gap and account_key(account.handle) in requested
    ]
    if any(key not in requested for key in reported) or any(key not in requested for key in run.next_cursors):
        failures.append(AccountFailure(account=None, reason="UNREQUESTED_ACCOUNT_RESULT"))
    if run.health == "degraded" and not failures and not gaps:
        failures.append(
            AccountFailure(
                account=None,
                reason=run.error if run.error is not None and run.error.strip() else "RUN_DEGRADED",
            )
        )
    failed_handles = {account_key(failure.account) for failure in failures if failure.account is not None}
    if run.health == "failed":
        rows = []
    else:
        rows = [row for row in rows if account_key(row.discovery.author) not in failed_handles]
    return Scan(rows=rows, cursors=updated, failures=failures, gaps=gaps)


class XSensor:
    def __init__(
        self,
        http: httpx.AsyncClient,
        token: str | None,
        *,
        admission: HttpAdmission,
    ) -> None:
        self.http = http
        self.token = token
        self.admission = admission

    async def _admit(self, feed: HttpFetchPlan, request: httpx.Request) -> None:
        if (
            request.url.scheme != "https"
            or request.url.host != "api.apify.com"
            or request.url.port not in (None, 443)
            or request.url.userinfo
        ):
            raise ValueError("X actor requests require the declared HTTPS Apify API destination")
        decision = await self.admission.admit(request)
        if isinstance(decision, HttpRefused):
            raise ValueError("X actor HTTP admission refused: " + decision.reason)

    async def posts(
        self,
        url: str,
        accounts: list[str],
        cursors: Mapping[str, str],
        *,
        on_start: RequestStarted | None = None,
    ) -> Scan:
        if not self.token:
            raise ValueError("APIFY_API_TOKEN is not set")
        previous = cursor_state(cursors)
        known = {
            account_key(account): previous[account_key(account)]
            for account in accounts
            if account_key(account) in previous
        }
        secrets = (self.token, repr(self.token)[1:-1])
        try:
            request = self.http.build_request(
                "POST",
                url,
                json={"handles": accounts, "cursors": known} if known else {"handles": accounts},
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=RUN_TIMEOUT_S,
            )
            response = await SourceHttpTransport(self.http, self._admit).send(
                HttpFetchPlan(url=url, method="POST"), request, on_start
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Apify X run request failed: {redact_text(str(exc), secrets)}") from None
        try:
            decoded = decode_unique_json(response.content)
        except ValueError:
            raise ValueError("Apify X response JSON is invalid or ambiguous") from None
        contains_token = any(
            secret.encode() in response.content for secret in secrets
        ) or contains_credential(decoded, secrets)
        if contains_token:
            try:
                run = ApifyRun.model_validate(
                    diagnostic_projection(decoded, token=self.token, secrets=secrets)
                )
            except ValueError:
                raise ValueError("Apify X response contains credential material") from None
        else:
            run = ApifyRun.model_validate(decoded)
        requested = {account_key(account) for account in accounts}
        has_diagnostics = any(
            isinstance(account, ApifyFailedAccount)
            and account.diagnostics is not None
            and account_key(account.handle) in requested
            for account in run.accounts
        )
        if contains_token and not has_diagnostics:
            raise ValueError("Apify X response contains credential material")
        try:
            result = scan(
                run,
                accounts,
                previous,
                response.content,
                token=self.token,
                retain_posts=not contains_token,
            )
        except RuntimeError as exc:
            raise RuntimeError(redact_text(str(exc), secrets)) from None
        failures = [
            AccountFailure(
                account=failure.account,
                reason=redact_text(failure.reason, secrets),
                diagnostics=failure.diagnostics,
            )
            for failure in result.failures
        ]
        if contains_token:
            failures.append(AccountFailure(account=None, reason="CREDENTIAL_MATERIAL_IN_ACTOR_RESPONSE"))
        return Scan(
            rows=[] if contains_token else result.rows,
            cursors=previous if contains_token else result.cursors,
            failures=failures,
            gaps=result.gaps,
        )
