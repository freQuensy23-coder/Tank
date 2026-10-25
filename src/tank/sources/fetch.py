"""Typed source HTTP reads, conditional cache admission and immutable response receipts."""

import asyncio
import math
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from typing import Literal

import httpx
from pydantic import Field, JsonValue, field_validator

from tank.contracts import DomainModel
from tank.contracts.sources import HttpFetchPlan
from tank.sources.http_client import RequestStarted
from tank.sources.parsers import published
from tank.util import iso, parse_iso_time, sha256


class ConditionalCheckpointWire(DomainModel):
    etag: str | None = None
    last_modified: str | None = None
    article_errors: dict[str, JsonValue] = Field(default_factory=dict)
    entry_errors: list[JsonValue] = Field(default_factory=list)


class HttpCheckpointProjection(DomainModel):
    etag: str | None = None
    last_modified: str | None = None
    blocked_by_processing_errors: bool = False

    @classmethod
    def from_document(cls, value: Mapping[str, JsonValue] | None) -> "HttpCheckpointProjection":
        if value is None:
            return cls()
        wire = ConditionalCheckpointWire.model_validate(
            {key: value[key] for key in ConditionalCheckpointWire.model_fields if key in value}
        )
        return cls(
            etag=wire.etag,
            last_modified=wire.last_modified,
            blocked_by_processing_errors=bool(wire.article_errors or wire.entry_errors),
        )

    def request_headers(self, declared: Mapping[str, str]) -> dict[str, str]:
        headers = dict(declared)
        if not self.blocked_by_processing_errors:
            if self.etag is not None:
                headers["If-None-Match"] = self.etag
            if self.last_modified is not None:
                headers["If-Modified-Since"] = self.last_modified
        return headers


class FetchEvidence(DomainModel):
    payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    fetched_at: str
    fetched_at_prec: Literal["COLLECTION_ONLY"] = "COLLECTION_ONLY"
    fetched_at_prec_reason: None = None
    http_status: int = Field(ge=100, le=599)
    response_url: str = Field(min_length=1)

    @field_validator("fetched_at")
    @classmethod
    def aware_clock(cls, value: str) -> str:
        return iso(parse_iso_time(value))

    def storage_record(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class FetchReceipt(FetchEvidence):
    etag: str | None
    last_modified: str | None

    def evidence(self) -> FetchEvidence:
        return FetchEvidence(
            payload_sha256=self.payload_sha256,
            fetched_at=self.fetched_at,
            fetched_at_prec=self.fetched_at_prec,
            fetched_at_prec_reason=self.fetched_at_prec_reason,
            http_status=self.http_status,
            response_url=self.response_url,
        )


class FetchedContent(DomainModel):
    payload: bytes
    receipt: FetchReceipt


class NotModified(DomainModel):
    payload: None = None
    receipt: None = None


type FetchResult = FetchedContent | NotModified
type FetchSender = Callable[
    [HttpFetchPlan, Mapping[str, str], str, RequestStarted | None], Awaitable[httpx.Response]
]
type ReceiptRecorder = Callable[[str, FetchEvidence], Awaitable[None]]


class SourceHttpReader:
    def __init__(
        self,
        send: FetchSender,
        record: ReceiptRecorder | None,
        now: Callable[[], datetime],
    ) -> None:
        self.send, self.record, self.now = send, record, now

    async def read(
        self,
        feed: HttpFetchPlan,
        key: str,
        cache: HttpCheckpointProjection,
        on_start: RequestStarted | None,
    ) -> FetchResult:
        headers = cache.request_headers(feed.headers)
        error: httpx.HTTPError | TimeoutError | None = None
        for attempt in range(4):
            try:
                response = await self.send(feed, headers, key, on_start)
                if response.status_code == 304:
                    return NotModified()
                receipt = FetchReceipt(
                    etag=response.headers.get("etag"),
                    last_modified=response.headers.get("last-modified"),
                    payload_sha256=sha256(response.content),
                    fetched_at=iso(self.now()),
                    http_status=response.status_code,
                    response_url=str(response.url),
                )
                if self.record is not None:
                    await self.record(key, receipt.evidence())
                response.raise_for_status()
                return FetchedContent(payload=response.content, receipt=receipt)
            except (httpx.HTTPError, TimeoutError) as exc:
                if isinstance(exc, httpx.HTTPStatusError):
                    error = httpx.HTTPStatusError(
                        f"Source HTTP status {exc.response.status_code}",
                        request=exc.request,
                        response=exc.response,
                    )
                elif isinstance(exc, httpx.RequestError):
                    error = httpx.RequestError(
                        "Source HTTP transport failed: " + type(exc).__name__, request=exc.request
                    )
                elif isinstance(exc, TimeoutError):
                    error = TimeoutError("Source HTTP transport timed out")
                else:
                    error = httpx.HTTPError("Source HTTP request failed")
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404:
                    raise error from None
                if attempt < 3:
                    delay = float(2 ** (attempt + 1))
                    if isinstance(exc, httpx.HTTPStatusError):
                        retry = exc.response.headers.get("retry-after")
                        if retry is not None:
                            try:
                                seconds = float(retry)
                                if not math.isfinite(seconds) or seconds < 0:
                                    raise ValueError("Invalid retry delay")
                                delay = max(delay, seconds)
                            except ValueError:
                                retry_at = published(retry)
                                if retry_at is not None:
                                    delay = max(
                                        delay, (parse_iso_time(retry_at) - self.now()).total_seconds()
                                    )
                    await asyncio.sleep(delay)
        if error is None:
            raise RuntimeError("Source fetch exhausted retries without a recorded failure")
        raise error
