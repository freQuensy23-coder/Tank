"""Explicit current-read smoke probes; never create source items or queue jobs."""

import asyncio
from collections.abc import Mapping
from typing import Literal, Protocol
from urllib.parse import urlsplit

import httpx
from pydantic import JsonValue

import tank.sources.parsers as parsers
from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.source_checks import (
    SourceHttpProbeContext,
    SourceProbeContext,
    SourceProbeFailure,
    SourceProbeResponse,
    SourceProbeResult,
    SourceProbeSuccess,
    SourceProbeUnavailable,
)
from tank.contracts.source_errors import SourceEntryError
from tank.contracts.sources import HttpFetchPlan, SourceDefinition, SourceFeed
from tank.logging import redact_text
from tank.sources.capture_fetch import CaptureRepository, capture_fetch, is_exchange_url
from tank.sources.http_client import SourceHttpTransport
from tank.time_precision import collection_clock
from tank.util import sha256


class ProbeSecretSettings(Protocol):
    def configured_secrets(self) -> frozenset[str]: ...


class ProbeService(Protocol):
    @property
    def sources(self) -> Mapping[str, SourceDefinition]: ...
    @property
    def versions(self) -> ResolvedVersionIdentity: ...
    @property
    def repo(self) -> CaptureRepository: ...
    @property
    def transport(self) -> SourceHttpTransport: ...
    @property
    def settings(self) -> ProbeSecretSettings: ...
    async def acquire_budget(self, feed: HttpFetchPlan, *, request: httpx.Request | None = None) -> None: ...
    def admit_entry_errors(self, errors: JsonValue) -> list[SourceEntryError]: ...


class ProbeFailureCause(DomainModel):
    status: Literal["HTTP_ERROR", "NETWORK_ERROR", "PARSE_ERROR"]
    reason: str


async def probe_feeds(service: ProbeService) -> list[SourceProbeResult]:
    host_locks: dict[str, asyncio.Lock] = {}
    semaphore = asyncio.Semaphore(4)
    secrets = tuple(service.settings.configured_secrets())

    async def probe(source: SourceDefinition, index: int, feed: SourceFeed) -> SourceProbeResult:
        started_at = collection_clock()
        if source.source_id == "S29":
            return SourceProbeUnavailable(
                context=SourceProbeContext(
                    source_id=source.source_id,
                    feed_index=index,
                    url=feed.url,
                    kind=feed.kind,
                    started_at=started_at,
                    versions=service.versions,
                ),
                finished_at=collection_clock(),
                status="UNAVAILABLE",
                item_count=None,
                reason="X runs through a paid Apify actor run; excluded from the HTTP smoke",
            )
        context = SourceHttpProbeContext(
            source_id=source.source_id,
            feed_index=index,
            url=feed.url,
            kind=feed.kind,
            method=feed.method,
            started_at=started_at,
            versions=service.versions,
        )
        host = urlsplit(feed.url).netloc
        lock = host_locks.setdefault(host, asyncio.Lock())
        response_observation: SourceProbeResponse | None = None
        item_count: int | None = None
        entry_errors: list[SourceEntryError] | None = None
        failure: ProbeFailureCause | None = None
        async with semaphore, lock:
            try:
                if is_exchange_url(feed.url):
                    await service.acquire_budget(feed.request_plan(source.limits))
                    response = await capture_fetch(
                        service.repo,
                        feed.method,
                        feed.url,
                        body=feed.body if feed.method == "POST" else None,
                        source_id=source.source_id,
                    )
                else:
                    response = await service.transport.request(
                        feed.request_plan(source.limits), feed.headers, None
                    )
                payload = response.content
                response_observation = SourceProbeResponse(
                    http_status=response.status_code,
                    response_content_type=response.headers.get("content-type"),
                    payload_sha256=sha256(payload),
                    response_bytes=len(payload),
                )
                response.raise_for_status()
                if feed.kind in {"rss", "atom", "feed"}:
                    batch = parsers.feeds(payload, source.source_id, feed)
                    item_count = len(batch.discoveries)
                    if batch.errors:
                        entry_errors = list(batch.errors)
                        raise ValueError("Publisher entries contain parser errors; valid siblings retained")
                elif feed.kind in {"html", "rendered_html", "rendered"}:
                    listings = parsers.article_links(payload, feed)
                    item_count = len(listings)
                elif feed.kind == "instruments":
                    snapshot = parsers.instruments(payload, feed)
                    if not snapshot:
                        raise ValueError("Empty instrument list: cannot validate listing coverage")
                    item_count = len(snapshot)
                else:
                    parsed = parsers.json_items(payload, source.source_id)
                    item_count = len(parsed.discoveries)
                    if parsed.errors:
                        entry_errors = list(parsed.errors)
                        raise ValueError("Publisher entries contain parser errors; valid siblings retained")
            except httpx.HTTPStatusError as exc:
                failure = ProbeFailureCause(status="HTTP_ERROR", reason=f"HTTP {exc.response.status_code}")
            except httpx.RequestError as exc:
                failure = ProbeFailureCause(
                    status="NETWORK_ERROR", reason=redact_text(f"{type(exc).__name__}: {exc}", secrets)
                )
            except Exception as exc:
                failure = ProbeFailureCause(
                    status="PARSE_ERROR", reason=redact_text(f"{type(exc).__name__}: {exc}", secrets)
                )
            finally:
                finished_at = collection_clock()
                await asyncio.sleep(0.5)
        if failure is not None:
            return SourceProbeFailure(
                context=context,
                response=response_observation,
                finished_at=finished_at,
                status=failure.status,
                item_count=item_count,
                parser_entry_errors=entry_errors,
                reason=failure.reason,
            )
        if response_observation is None or item_count is None:
            raise RuntimeError("Successful source probe lacks its actual response or parsed count")
        return SourceProbeSuccess(
            context=context,
            response=response_observation,
            finished_at=finished_at,
            status="PASS" if item_count else "PASS_EMPTY",
            item_count=item_count,
            validation="Current configured endpoint and source-specific parser; no alternate discovery",
        )

    return list(
        await asyncio.gather(
            *(
                probe(source, index, feed)
                for source in service.sources.values()
                for index, feed in enumerate(source.feeds)
            )
        )
    )
