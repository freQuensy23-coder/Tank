"""Explicit live HTTP and verified recorded-evidence page composition."""

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Never, Protocol

import httpx

from tank.contracts import DomainModel
from tank.contracts.http_policy import HttpAdmission
from tank.contracts.page_evidence import LivePageProvenance, PageDigest, PageRead
from tank.http_transport import RequestGuard, send_admitted
from tank.time_precision import collection_clock
from tank.util import sha256


class VerifiedRecordedPageReader(Protocol):
    async def read(self, url: str) -> PageRead: ...


class RecordedResultsOnlyPageReader(DomainModel):
    input_sha256: PageDigest
    captured_result_catalog_sha256: PageDigest

    async def read(self, url: str) -> Never:
        raise ValueError("Verified packet retains tool results but no original page response bytes")


@dataclass(frozen=True, slots=True)
class LivePageAccess:
    admission: HttpAdmission


@dataclass(frozen=True, slots=True)
class RecordedPageAccess:
    reader: VerifiedRecordedPageReader


type PageAccess = LivePageAccess | RecordedPageAccess


async def read_live_page(
    access: LivePageAccess,
    url: str,
    *,
    client: httpx.AsyncClient | None,
    timeout_s: float,
    user_agent: str,
    guard: RequestGuard,
) -> PageRead:
    async with asyncio.timeout(timeout_s), AsyncExitStack() as resources:
        actual = client
        if actual is None:
            actual = await resources.enter_async_context(
                httpx.AsyncClient(timeout=timeout_s, follow_redirects=False)
            )
        request = actual.build_request("GET", url, headers={"User-Agent": user_agent})
        try:
            response = await send_admitted(actual, access.admission, request, guard, stream=True)
        except httpx.RequestError as exc:
            raise type(exc)(f"{request.url.host}: {type(exc).__name__}", request=request) from None
        resources.push_async_callback(response.aclose)
        if response.is_error:
            raise httpx.HTTPStatusError(
                f"{response.request.url.host}: HTTP {response.status_code}",
                request=response.request,
                response=response,
            )
        body = bytearray()
        try:
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > 2 * 1024 * 1024:
                    raise ValueError("Page exceeds 2 MB")
        except httpx.RequestError as exc:
            raise type(exc)(
                f"{response.request.url.host}: {type(exc).__name__}", request=response.request
            ) from None
        content = bytes(body)
        return PageRead(
            body=content,
            body_sha256=sha256(content),
            media_type=response.headers.get("content-type"),
            provenance=LivePageProvenance(
                mode="LIVE_RESPONSE",
                received_at=collection_clock(),
                content_type_header=response.headers.get("content-type"),
            ),
        )
