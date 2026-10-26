"""Standalone actor contracts included in the actor image closure."""

import base64
import hashlib
import re
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    RootModel,
    field_validator,
    model_validator,
)

HANDLE = re.compile(r"[a-z0-9_]{1,15}")
POST_ID = re.compile(r"[0-9]{1,20}")
CREDENTIAL_FIELDS = {"xSession", "cookies", "auth_token", "ct0"}


class ActorModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
        revalidate_instances="always",
    )


def canonical_handle(value: JsonValue) -> str:
    if not isinstance(value, str):
        raise ValueError("INVALID_MONITORED_HANDLE")
    result = value.strip().removeprefix("@").lower()
    if not HANDLE.fullmatch(result):
        raise ValueError("INVALID_MONITORED_HANDLE")
    return result


def fixed_integer(value: JsonValue) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("INTEGER_REQUIRED")
    return value


type SchemaVersion = Annotated[Literal[1], BeforeValidator(fixed_integer)]


class ActorRequest(ActorModel):
    handles: list[str] = Field(min_length=1, max_length=200)
    mode: Literal["fetch"] = "fetch"
    concurrency: int = Field(default=6, ge=1, le=8)
    cursors: dict[str, str] = Field(default_factory=dict, max_length=200)
    requestId: str | None = Field(default=None, max_length=128)

    @field_validator("handles", mode="before")
    @classmethod
    def monitored_handles(cls, value: JsonValue) -> list[str]:
        if not isinstance(value, list) or not 1 <= len(value) <= 200:
            raise ValueError("HANDLES_REQUIRED_MAX_200")
        return list(dict.fromkeys(canonical_handle(item) for item in value))

    @field_validator("cursors", mode="before")
    @classmethod
    def declared_cursors(cls, value: JsonValue) -> dict[str, str]:
        if not isinstance(value, dict) or len(value) > 200:
            raise ValueError("CURSORS_OBJECT_REQUIRED")
        result: dict[str, str] = {}
        for handle, post_id in value.items():
            name = canonical_handle(handle)
            if not isinstance(post_id, str) or not POST_ID.fullmatch(post_id) or int(post_id) <= 0:
                raise ValueError("CURSOR_POST_ID_STRING_REQUIRED")
            if name in result and result[name] != post_id:
                raise ValueError("CONFLICTING_CURSOR_VALUES")
            result[name] = post_id
        return result

    @model_validator(mode="after")
    def watched_cursor_owners(self) -> Self:
        if set(self.cursors) - set(self.handles):
            raise ValueError("CURSOR_FOR_UNWATCHED_ACCOUNT")
        return self


class EmbeddedJsonDocument(RootModel[JsonValue]):
    """External heterogeneous GraphQL tree, decoded only before named fact projection."""

    model_config = ConfigDict(strict=True, allow_inf_nan=False, hide_input_in_errors=True)


type ReferenceRelation = Literal["quoted", "replied_to", "linked"]


class ReferencedPost(ActorModel):
    relation: ReferenceRelation
    id: str = Field(pattern=r"^[0-9]{1,20}$")
    author: str | None = Field(pattern=r"^[A-Za-z0-9_]{1,15}$")
    url: str
    publishedAt: str | None
    text: str | None
    error: str | None = Field(default=None, min_length=1, pattern=r"\S")

    @model_validator(mode="after")
    def reported_identity(self) -> Self:
        if self.error is not None and self.text is not None:
            raise ValueError("REFERENCE_BODY_FAILURE_CONFLICT")
        parts = urlsplit(self.url)
        match = re.fullmatch(r"/([A-Za-z0-9_]{1,15})/status/([0-9]{1,20})/?", parts.path)
        if (
            parts.scheme != "https"
            or parts.hostname not in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}
            or parts.username is not None
            or parts.password is not None
            or match is None
            or match[2] != self.id
            or (self.author is not None and match[1].lower() != self.author.lower())
        ):
            raise ValueError("REFERENCE_IDENTITY_MISMATCH")
        return self


class ReferencedPostContent(ActorModel):
    author: str | None = Field(pattern=r"^[A-Za-z0-9_]{1,15}$")
    text: str
    publishedAt: str | None


class EmbeddedTweet(ActorModel):
    """Only observed, usable enrichment facts from an optional external GraphQL node."""

    id: str = Field(pattern=r"^[0-9]{1,20}$")
    author: str | None = Field(pattern=r"^[A-Za-z0-9_]{1,15}$")
    text: str | None
    links: list[str]
    created_at_ms: int | None = Field(gt=0)
    edit_ids: list[str]
    quoted_id: str | None = Field(pattern=r"^[0-9]{1,20}$")
    quoted_unavailable: bool
    replied_to_id: str | None = Field(pattern=r"^[0-9]{1,20}$")
    reply_author: str | None = Field(pattern=r"^[A-Za-z0-9_]{1,15}$")


class ReferenceReadSuccess(ActorModel):
    post_id: str = Field(pattern=r"^[0-9]{1,20}$")
    content: ReferencedPostContent


class ReferenceReadFailure(ActorModel):
    post_id: str = Field(pattern=r"^[0-9]{1,20}$")
    error: str = Field(min_length=1, pattern=r"\S")


type ReferenceResolution = ReferenceReadSuccess | ReferenceReadFailure


class PublicPost(ActorModel):
    id: str = Field(pattern=r"^[0-9]{1,20}$")
    author: str = Field(min_length=1)
    text: str
    publishedTs: float
    publishedAt: str
    url: str
    source: Literal["x-public-profile"] = "x-public-profile"
    referencedPosts: list[ReferencedPost] = Field(default_factory=list)


class DiagnosticRequest(ActorModel):
    method: Literal["GET"]
    url: str
    body: None


class DiagnosticResponse(ActorModel):
    status: int
    headers: dict[str, str]
    body: str
    bodyEncoding: Literal["utf-8", "base64"]
    bodyBytes: int = Field(ge=0)
    bodySha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    wireBytes: int = Field(ge=0)
    captureLimitBytes: int = Field(ge=0)
    serializedBodyLimitBytes: int | None = Field(ge=0)
    bodyTruncated: bool
    omittedWireBytes: int | None = Field(ge=0)
    readError: str | None
    decodeError: str | None
    redacted: bool

    @model_validator(mode="after")
    def captured_body_integrity(self) -> Self:
        payload = (
            base64.b64decode(self.body, validate=True)
            if self.bodyEncoding == "base64"
            else self.body.encode("utf-8")
        )
        if len(payload) != self.bodyBytes or hashlib.sha256(payload).hexdigest() != self.bodySha256:
            raise ValueError("DIAGNOSTIC_BODY_INTEGRITY_MISMATCH")
        if self.wireBytes > self.captureLimitBytes:
            raise ValueError("DIAGNOSTIC_WIRE_CAPTURE_LIMIT_EXCEEDED")
        return self


class DiagnosticAttempt(ActorModel):
    attempt: int = Field(ge=1, le=3)
    at: str
    error: str
    retryable: bool
    response: DiagnosticResponse | None = None
    status: int | None = None

    @model_validator(mode="after")
    def response_status(self) -> Self:
        if self.response is not None and self.status != self.response.status:
            raise ValueError("DIAGNOSTIC_STATUS_MISMATCH")
        if self.response is None and self.status is not None:
            raise ValueError("DIAGNOSTIC_RESPONSE_REQUIRED")
        return self


class ProfileDiagnostics(ActorModel):
    request: DiagnosticRequest
    attempts: list[DiagnosticAttempt] = Field(max_length=3)
    attemptCount: int = Field(ge=0, le=3)
    outcome: Literal["exhausted", "deadline_exhausted", "permanent_failure"]

    @model_validator(mode="after")
    def actual_attempt_count(self) -> Self:
        if self.attemptCount != len(self.attempts):
            raise ValueError("DIAGNOSTIC_ATTEMPT_COUNT_MISMATCH")
        return self


class AccountSuccess(ActorModel):
    handle: str
    status: Literal["ok"] = "ok"
    scanTs: float
    posts: int = Field(ge=1)
    wireBytes: int = Field(ge=0)
    lastMaxId: str = Field(pattern=r"^[0-9]{1,20}$")
    historyGap: bool


class AccountFailure(ActorModel):
    handle: str
    status: Literal["failed"] = "failed"
    scanTs: float
    error: str
    diagnostics: ProfileDiagnostics | None


type AccountResult = Annotated[AccountSuccess | AccountFailure, Field(discriminator="status")]


class CollectionReport(ActorModel):
    source: Literal["x-public-profile"] = "x-public-profile"
    seconds: float = Field(ge=0)
    watchedAccounts: int = Field(ge=1, le=200)
    accountsRead: int = Field(ge=0, le=200)
    failedAccounts: list[AccountFailure]
    coverageGaps: list[str]
    accountResults: list[AccountResult]
    wireBytes: int = Field(ge=0)
    postCount: int = Field(ge=0)
    authorsSeen: int = Field(ge=0)
    reachedHistory: bool
    error: Literal["PUBLIC_SOURCE_UNAVAILABLE"] | None


class SnapshotStats(ActorModel):
    durationSeconds: float = Field(ge=0)
    watchedAccounts: int = Field(ge=1, le=200)
    accountsRead: int = Field(ge=0, le=200)
    postCount: int = Field(ge=0)
    wireBytes: int = Field(ge=0)


class ActorSnapshot(ActorModel):
    schemaVersion: SchemaVersion
    mode: Literal["fetch"]
    requestId: str | None
    source: Literal["x-public-profile"]
    checkedAt: str
    ok: bool
    health: Literal["failed", "degraded", "healthy"]
    posts: list[PublicPost]
    accounts: list[AccountResult]
    nextCursors: dict[str, str]
    failedAccounts: list[AccountFailure]
    coverageGaps: list[str]
    error: Literal["PUBLIC_SOURCE_UNAVAILABLE"] | None
    stats: SnapshotStats
