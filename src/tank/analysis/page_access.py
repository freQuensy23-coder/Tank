"""Explicit live HTTP and verified recorded-evidence page composition."""

from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Protocol

import httpx

from tank.contracts.http_policy import HttpAdmission
from tank.contracts.page_evidence import LivePageProvenance, PageRead
from tank.http_transport import RequestGuard, send_admitted
from tank.time_precision import collection_clock
from tank.util import sha256


class VerifiedRecordedPageReader(Protocol):
    async def read(self, url: str) -> PageRead: ...


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
    async with AsyncExitStack() as resources:
        actual = client
        if actual is None:
            actual = await resources.enter_async_context(
                httpx.AsyncClient(timeout=timeout_s, follow_redirects=False)
            )
        request = actual.build_request("GET", url, headers={"User-Agent": user_agent})
        response = await send_admitted(actual, access.admission, request, guard, stream=True)
        resources.push_async_callback(response.aclose)
        response.raise_for_status()
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > 2 * 1024 * 1024:
                raise ValueError("Page exceeds 2 MB")
        content = bytes(body)
        return PageRead(
            body=content,
            body_sha256=sha256(content),
            media_type=response.headers.get("content-type"),
            provenance=LivePageProvenance(mode="LIVE_RESPONSE", received_at=collection_clock()),
        )
