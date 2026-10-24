"""Actual local async HTTPX streams; production 20-second deadline is unmodified."""

import asyncio
import time
from pathlib import Path

import httpx
import pytest
import zstandard

import tank.event_tools as CANDIDATE
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, sha256

CLIENT = httpx.AsyncClient


class Body(httpx.AsyncByteStream):
    def __init__(self, chunks, delay=0) -> None:
        self.chunks, self.delay, self.closed, self.yielded = chunks, delay, False, 0

    async def __aiter__(self):
        for chunk in self.chunks:
            await asyncio.sleep(self.delay)
            self.yielded += 1
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


async def setup(tmp_path: Path, handler, owned, monkeypatch: pytest.MonkeyPatch):
    repo = MemoryRepository()
    await repo.open()
    client = CLIENT(transport=httpx.MockTransport(handler), timeout=None)
    service = CANDIDATE.EventToolService(
        repo, Settings(data_dir=tmp_path), None, None, http=None if owned else client
    )
    if owned:

        def factory(**kwargs):
            assert kwargs == {"timeout": 20, "follow_redirects": False}
            return client

        monkeypatch.setattr(CANDIDATE.httpx, "AsyncClient", factory)
    return service, client


async def no_evidence(service) -> None:
    assert not await service.repo.list("ingest.source_items")
    assert not list(Path(service.settings.data_dir).rglob("*.json.zst"))


@pytest.mark.integration(reason="Real default 20-second deadline across HTTP headers or streamed body")
@pytest.mark.parametrize("owned", [False, True])
@pytest.mark.parametrize("kind", ["slow_drip", "delayed_headers"])
async def test_real_twenty_second_total_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, owned, kind
) -> None:
    body = Body([b"x"] * 40, delay=1)
    cancelled = asyncio.Event()

    async def handler(request: pytest.FixtureRequest):
        if kind == "delayed_headers":
            try:
                await asyncio.sleep(40)
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return httpx.Response(200, stream=body)

    service, client = await setup(tmp_path, handler, owned, monkeypatch)
    start = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            await service.fetch_page({"url": "https://example.com/page"}, {})
        elapsed = time.monotonic() - start
        assert 19.5 <= elapsed < 23, elapsed
        if kind == "slow_drip":
            assert 10 < body.yielded < 22
            assert body.closed
        else:
            assert cancelled.is_set()
        await no_evidence(service)
        assert client.is_closed is owned
    finally:
        await client.aclose()


@pytest.mark.parametrize("owned", [False, True])
async def test_complete_exact_raw_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, owned) -> None:
    original = b"<html><script>ignored</script><body>Actual evidence</body></html>"
    body = Body([original[:20], original[20:]], delay=0.01)
    service, client = await setup(
        tmp_path, lambda request: httpx.Response(200, stream=body), owned, monkeypatch
    )
    try:
        item = await service.fetch_page({"url": "https://example.com/page"}, {})
        assert item["text"] == "Actual evidence"
        assert zstandard.ZstdDecompressor().decompress(Path(item["raw_ref"]).read_bytes()) == original
        assert len(await service.repo.list("ingest.source_items")) == 1
        assert body.closed
    finally:
        await client.aclose()


@pytest.mark.parametrize("status", [404, 503])
async def test_http_failure_has_no_partial_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status
) -> None:
    service, client = await setup(
        tmp_path, lambda request: httpx.Response(status, content=b"error"), False, monkeypatch
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await service.fetch_page({"url": "https://example.com/page"}, {})
        await no_evidence(service)
    finally:
        await client.aclose()


@pytest.mark.parametrize("owned", [False, True])
async def test_caller_cancellation_propagates_and_closes_body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, owned
) -> None:
    body = Body([b"first", b"second"], delay=0.1)
    service, client = await setup(
        tmp_path, lambda request: httpx.Response(200, stream=body), owned, monkeypatch
    )
    try:
        task = asyncio.create_task(service.fetch_page({"url": "https://example.com/page"}, {}))
        await asyncio.sleep(0.15)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert body.yielded == 1 and body.closed
        await no_evidence(service)
    finally:
        await client.aclose()


@pytest.mark.parametrize("extra,accepted", [(0, True), (1, False)])
async def test_exact_two_mib_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra, accepted
) -> None:
    body = Body([b"x" * (2 * 1024 * 1024), b"y" * extra])
    service, client = await setup(
        tmp_path, lambda request: httpx.Response(200, stream=body), False, monkeypatch
    )
    try:
        if accepted:
            item = await service.fetch_page({"url": "https://example.com/page"}, {})
            assert item["raw_size_bytes"] == 2 * 1024 * 1024
        else:
            with pytest.raises(ValueError, match="Page exceeds 2 MB"):
                await service.fetch_page({"url": "https://example.com/page"}, {})
            await no_evidence(service)
        assert body.closed
    finally:
        await client.aclose()


def test_existing_session_error_protocol_accepts_wall_timeout() -> None:
    from tank.agents.sessions import SessionService

    service = object.__new__(SessionService)
    error, status = service.tool_failure(TimeoutError())
    assert status == "ERROR"
    assert error["type"] == "TimeoutError"
    assert error["category"] == "TOOL_EXECUTION"
    assert isinstance(asyncio.CancelledError(), BaseException)
    assert not isinstance(asyncio.CancelledError(), Exception)


@pytest.mark.asyncio
async def test_exchange_page_is_fetched_by_tank_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def handler(request: pytest.FixtureRequest) -> None:
        raise AssertionError("core reached an exchange host: " + request.url.host)

    service, client = await setup(tmp_path, handler, False, monkeypatch)
    url = "https://www.binance.com/en/support/announcement/abc"

    request_enqueued = asyncio.Event()
    put = service.repo.put

    async def observed_put(collection, key, value, *args, **kwargs):
        result = await put(collection, key, value, *args, **kwargs)
        if collection == "mkt.capture_requests" and value.get("status") == "PENDING":
            request_enqueued.set()
        return result

    monkeypatch.setattr(service.repo, "put", observed_put)

    async def tank_capture() -> None:
        await request_enqueued.wait()
        rows = await service.repo.list("mkt.capture_requests")
        row = rows[0]
        assert row["kind"] == "HTTP_FETCH" and row["request"]["url"] == url
        row.update(
            status="COMPLETE",
            result={
                "status_code": 200,
                "content_type": "text/html",
                "body": "<p>listed</p>",
                "sha256": sha256(b"<p>listed</p>"),
                "fetched_at": iso(),
                "bytes": len(b"<p>listed</p>"),
                "source_id": row["request"]["source_id"],
            },
        )
        await service.repo.put("mkt.capture_requests", row["request_id"], row)

    async with client:
        worker = asyncio.create_task(tank_capture())
        try:
            async with asyncio.timeout(2):
                item = await service.fetch_page({"url": url}, {})
                await worker
        finally:
            if not worker.done():
                worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
    assert "listed" in item["text"]
    assert item["raw_sha256"] == sha256(b"<p>listed</p>")
    assert zstandard.ZstdDecompressor().decompress(Path(item["raw_ref"]).read_bytes()) == b"<p>listed</p>"
