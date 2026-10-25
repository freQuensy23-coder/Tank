"""Source HTTP receipts preserve evidence independently of conditional-cache commits."""

from collections.abc import Mapping
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

import tank.sources.fetch as fetch_module
from tank.contracts.sources import HttpFetchPlan
from tank.sources.fetch import (
    FetchedContent,
    FetchEvidence,
    HttpCheckpointProjection,
    NotModified,
    SourceHttpReader,
)
from tank.sources.http_client import RequestStarted


@pytest.mark.asyncio
async def test_receipt_cache_commits_only_after_caller_accepts_the_content() -> None:
    received: list[httpx.Headers] = []
    evidence: list[FetchEvidence] = []

    def respond(request: httpx.Request) -> httpx.Response:
        received.append(request.headers)
        if "if-none-match" in request.headers:
            return httpx.Response(304)
        return httpx.Response(200, content=b"source bytes", headers={"ETag": '"version-1"'})

    async def record(key: str, receipt: FetchEvidence) -> None:
        assert key == "S30:0"
        evidence.append(receipt)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:

        async def send(
            feed: HttpFetchPlan,
            headers: Mapping[str, str],
            key: str,
            start: RequestStarted | None,
        ) -> httpx.Response:
            return await client.request(feed.method, feed.url, headers=headers)

        reader = SourceHttpReader(send, record, lambda: datetime(2026, 10, 25, tzinfo=UTC))
        plan = HttpFetchPlan(url="https://publisher.invalid/feed")
        first = await reader.read(plan, "S30:0", HttpCheckpointProjection(), None)
        assert isinstance(first, FetchedContent)
        assert first.payload == b"source bytes"
        assert first.receipt.etag == '"version-1"'
        assert "etag" not in evidence[0].model_dump()
        cache = HttpCheckpointProjection.from_document(first.receipt.storage_record())
        second = await reader.read(plan, "S30:0", cache, None)
        assert isinstance(second, NotModified)
        assert len(evidence) == 1
        rejected = HttpCheckpointProjection.from_document(
            {
                **first.receipt.storage_record(),
                "entry_errors": [{"reason": "MISSING_EXTERNAL_ID"}],
            }
        )
        third = await reader.read(plan, "S30:0", rejected, None)
        assert isinstance(third, FetchedContent)
    assert "if-none-match" not in received[0]
    assert received[1]["if-none-match"] == '"version-1"'
    assert "if-none-match" not in received[2]


@pytest.mark.asyncio
async def test_each_retry_retains_response_evidence_without_exposing_query_in_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[httpx.Request] = []
    delays: list[float] = []
    evidence: list[FetchEvidence] = []

    async def pause(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(fetch_module, "asyncio", SimpleNamespace(sleep=pause))

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(403, content=b"provider refusal", headers={"Retry-After": "unparseable"})

    async def record(key: str, receipt: FetchEvidence) -> None:
        evidence.append(receipt)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:

        async def send(
            feed: HttpFetchPlan,
            headers: Mapping[str, str],
            key: str,
            start: RequestStarted | None,
        ) -> httpx.Response:
            return await client.request(feed.method, feed.url, headers=headers)

        reader = SourceHttpReader(send, record, lambda: datetime(2026, 10, 25, tzinfo=UTC))
        with pytest.raises(httpx.HTTPStatusError) as failed:
            await reader.read(
                HttpFetchPlan(url="https://publisher.invalid/feed?apikey=do-not-log"),
                "S30:0",
                HttpCheckpointProjection(),
                None,
            )
    assert len(calls) == len(evidence) == 4
    assert delays == [2, 4, 8]
    assert failed.value.response.status_code == 403
    assert "do-not-log" not in str(failed.value)
    assert "publisher.invalid" not in str(failed.value)


def test_conditional_cache_projection_rejects_malformed_processing_evidence() -> None:
    with pytest.raises(ValidationError):
        HttpCheckpointProjection.from_document({"entry_errors": "malformed"})
