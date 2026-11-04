import asyncio
import base64
import gzip
import hashlib
import importlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from test_sources import settings as source_settings

from tank.delivery import DeliveryService
from tank.sources import SourceService
from tank.storage import MemoryRepository

pytestmark = pytest.mark.integration


@pytest.fixture
def wire_server(monkeypatch):
    root = Path(__file__).resolve().parents[1] / "apify" / "x-reader"
    monkeypatch.syspath_prepend(str(root))
    reader = importlib.import_module("public_profile")
    worker = importlib.import_module("direct_fetch")
    state = {
        "profile": [],
        "telegram": [],
        "outputs": [],
        "body": b"upstream diagnostic full response " * 1000,
    }

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, status, payload, headers=None):
            self.send_response(status)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            state["profile"].append((time.monotonic(), self.path, dict(self.headers)))
            wire = gzip.compress(state["body"])
            self.respond(
                503,
                wire,
                {
                    "Content-Type": "text/plain",
                    "Content-Encoding": "gzip",
                    "X-Request-ID": "safe-wire-id",
                    "Set-Cookie": "forbidden-cookie",
                },
            )

        def do_POST(self):
            payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if self.path == "/actor":
                output = asyncio.run(worker.fetch_snapshot(json.loads(payload)))
                state["outputs"].append(output)
                self.respond(201, json.dumps(output).encode(), {"Content-Type": "application/json"})
            else:
                state["telegram"].append((self.path, dict(self.headers), payload))
                self.respond(
                    200,
                    json.dumps(
                        {"ok": True, "result": {"message_id": len(state["telegram"]), "date": 1}}
                    ).encode(),
                    {"Content-Type": "application/json"},
                )

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setattr(reader, "PROFILE_ORIGIN", origin + "/")
    try:
        yield origin, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


class LoopbackTelegramTransport(httpx.AsyncBaseTransport):
    def __init__(self, origin):
        self.origin = origin
        self.transport = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request):
        target = httpx.Request(
            request.method,
            self.origin + request.url.path,
            headers=request.headers,
            stream=request.stream,
            extensions=request.extensions,
        )
        return await self.transport.handle_async_request(target)

    async def aclose(self):
        await self.transport.aclose()


async def test_real_worker_http_errors_keep_cursors_and_send_response_documents(tmp_path, wire_server):
    origin, state = wire_server
    expected = state["body"] + b"\r\nCookie: [REDACTED]\r\nbody"
    state["body"] += b"\r\nCookie: sid=cookie-primary; refresh=cookie-secondary, tail=cookie-tail\r\nbody"
    source = {"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": origin + "/actor"}]}
    config = source_settings(tmp_path, [source])
    config.apify_api_token = "isolated-apify-token"
    config.telegram_token = "isolated-telegram-token"
    config.telegram_ops_chat = "isolated-ops"
    config.ops_progress_enabled = True
    repo = MemoryRepository()
    await repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"alice": "90"}})
    async with httpx.AsyncClient() as http:
        service = SourceService(repo, config, http=http)
        service.accounts = {"alice": {"handle": "alice", "entity_id": "alice", "role": "OTHER"}}
        result = await service.poll_once("S29")
    checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
    assert result["status"] == "DEGRADED" and result["items"] == 0
    assert checkpoint["account_cursors"] == {"alice": "90"}
    assert state["outputs"][0]["accounts"][0]["diagnostics"]["attemptCount"] == 3
    assert [
        attempt["response"]["body"].encode()
        for attempt in state["outputs"][0]["accounts"][0]["diagnostics"]["attempts"]
    ] == [expected] * 3
    assert len(state["profile"]) == 3
    starts = [attempt[0] for attempt in state["profile"]]
    assert 0.9 <= starts[1] - starts[0] < 3
    assert 4.9 <= starts[2] - starts[1] < 7
    [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
    async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
        delivery = DeliveryService(repo, config, http=http)
        await delivery.deliver(job["payload"])
        await delivery.deliver(job["payload"])
    assert len(state["telegram"]) == 5
    assert state["telegram"][0][0].endswith("sendMessage")
    from test_x_diagnostics import multipart

    for path, headers, body in state["telegram"][1:4]:
        assert path.endswith("sendDocument")
        request = httpx.Request("POST", origin, headers=headers, content=body)
        assert multipart(request)["document"][1] == expected
        assert b"forbidden-cookie" not in body
    path, headers, body = state["telegram"][4]
    metadata_request = httpx.Request("POST", origin, headers=headers, content=body)
    metadata = json.loads(multipart(metadata_request)["document"][1])
    digest = hashlib.sha256(expected).hexdigest()
    assert [attempt["response"]["bodySha256"] for attempt in metadata["attempts"]] == [digest] * 3
    assert all(
        secret not in body
        for path, headers, body in state["telegram"]
        for secret in (b"cookie-primary", b"cookie-secondary", b"cookie-tail")
    )
    assert checkpoint["entry_errors"][0]["diagnostics"]["attemptCount"] == 3
    receipts = await repo.list("out.deliveries", filters={"status": "SENT"})
    assert len(receipts) == 5


@pytest.mark.parametrize("credential", ["auth_token", "ct0"])
@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
async def test_real_actor_http_redacts_query_credential_suffixes_before_consumer(
    wire_server, credential, encoding
):
    origin, state = wire_server
    raw = (
        f"safe body prefix https://x.com/alice?{credential}="
        "query-distinctprefix,query-distinctsuffix;query-distincttail&detail=safe\r\n"
        f'https://x.com/alice?{credential}="'
        'query-doubleprefix,query-doublesuffix;query-doubletail"&detail=double-safe\r\n'
        f"https://x.com/alice?before=single-safe&{credential}='"
        "query-singleprefix,query-singlesuffix;query-singletail'&detail=single-safe\r\n"
        f"https://x.com/alice?{credential}=&detail=empty-safe\r\n"
        f'https://x.com/alice?{credential}=""&detail=double-empty-safe\r\n'
        f"https://x.com/alice?{credential}=''&detail=single-empty-safe\r\n"
        "safe body suffix"
    ).encode()
    expected = (
        f"safe body prefix https://x.com/alice?{credential}=[REDACTED]&detail=safe\r\n"
        f'https://x.com/alice?{credential}="[REDACTED]"&detail=double-safe\r\n'
        f"https://x.com/alice?before=single-safe&{credential}='[REDACTED]'&detail=single-safe\r\n"
        f"https://x.com/alice?{credential}=[REDACTED]&detail=empty-safe\r\n"
        f'https://x.com/alice?{credential}="[REDACTED]"&detail=double-empty-safe\r\n'
        f"https://x.com/alice?{credential}='[REDACTED]'&detail=single-empty-safe\r\n"
        "safe body suffix"
    ).encode()
    secret_pieces = (
        b"query-distinctprefix",
        b"query-distinctsuffix",
        b"query-distincttail",
        b"query-doubleprefix",
        b"query-doublesuffix",
        b"query-doubletail",
        b"query-singleprefix",
        b"query-singlesuffix",
        b"query-singletail",
    )
    if encoding == "base64":
        raw += b"\x00\xff"
        expected += b"\x00\xff"
    state["body"] = raw
    async with httpx.AsyncClient(timeout=20) as http:
        response = await http.post(origin + "/actor", json={"handles": ["alice"], "cursors": {"alice": "90"}})
    assert response.status_code == 201
    output = response.json()
    assert output["health"] == "failed" and output["posts"] == []
    assert output["nextCursors"] == {"alice": "90"}
    evidence = output["accounts"][0]["diagnostics"]
    assert evidence["outcome"] == "exhausted" and evidence["attemptCount"] == 3
    assert len(state["profile"]) == 3
    for attempt in evidence["attempts"]:
        captured = attempt["response"]
        body = (
            base64.b64decode(captured["body"], validate=True)
            if encoding == "base64"
            else captured["body"].encode()
        )
        assert body == expected
        assert captured["bodyEncoding"] == encoding
        assert captured["bodyBytes"] == len(expected)
        assert captured["bodySha256"] == hashlib.sha256(expected).hexdigest()
        assert captured["redacted"] and not captured["bodyTruncated"]
        assert captured["status"] == 503
        assert all(piece not in body and piece not in response.content for piece in secret_pieces)
    assert state["telegram"] == []


@pytest.mark.timeout(10)
@pytest.mark.parametrize("phase", ["status", "headers", "chunk_extension", "chunk_trailer"])
async def test_absolute_deadline_closes_slow_http_transport_and_retains_completed_evidence(
    monkeypatch, phase
):
    root = Path(__file__).resolve().parents[1] / "apify" / "x-reader"
    monkeypatch.syspath_prepend(str(root))
    reader = importlib.import_module("public_profile")
    worker = importlib.import_module("direct_fetch")
    disconnected = asyncio.Event()
    tasks = set()
    requests = []
    failures = []

    async def serve(stream, writer):
        task = asyncio.current_task()
        tasks.add(task)
        slow = False
        try:
            requests.append(await stream.readuntil(b"\r\n\r\n"))
            if len(requests) == 1:
                writer.write(b"HTTP/1.1 503 Busy\r\nContent-Length: 5\r\nConnection: close\r\n\r\nfirst")
                await writer.drain()
                return
            slow = True
            if phase == "status":
                initial = b"HTTP/1.1 503 "
            elif phase == "headers":
                initial = b"HTTP/1.1 503 Busy\r\nX-Slow: "
            else:
                initial = b"HTTP/1.1 503 Busy\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nprefix\r\n"
                initial += b"1;slow=" if phase == "chunk_extension" else b"0\r\nX-Slow: "
            writer.write(initial)
            await writer.drain()
            eof = asyncio.create_task(stream.read())
            try:
                for _ in range(200):
                    done, _ = await asyncio.wait({eof}, timeout=0.02)
                    if done:
                        assert eof.result() == b""
                        disconnected.set()
                        break
                    writer.write(b"x")
                    await writer.drain()
            finally:
                eof.cancel()
                await asyncio.gather(eof, return_exceptions=True)
        except Exception as exc:
            failures.append(exc)
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)
            if slow:
                assert disconnected.is_set()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setattr(reader, "PROFILE_ORIGIN", f"http://127.0.0.1:{port}/")
    monkeypatch.setattr(reader, "run_deadline", lambda: time.monotonic() + 1.8)
    started = time.monotonic()
    try:
        output = await asyncio.wait_for(
            worker.fetch_snapshot({"handles": ["alice"], "cursors": {"alice": "90"}}), 4
        )
        assert time.monotonic() - started < 2.8
        await asyncio.wait_for(disconnected.wait(), 1)
        assert output["health"] == "failed" and output["posts"] == []
        assert output["nextCursors"] == {"alice": "90"}
        diagnostics = output["accounts"][0]["diagnostics"]
        assert diagnostics["outcome"] == "deadline_exhausted" and diagnostics["attemptCount"] == 2
        assert diagnostics["attempts"][0]["response"]["body"] == "first"
        second = diagnostics["attempts"][1]
        if phase.startswith("chunk"):
            assert second["response"]["body"] == "prefix"
            assert second["response"]["bodyTruncated"]
            assert second["response"]["readError"] == "RESPONSE_BODY_DEADLINE_EXHAUSTED"
        else:
            assert second["error"] == "PUBLIC_PROFILE_DEADLINE_EXHAUSTED"
        assert len(requests) == 2 and not failures
    finally:
        server.close()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await server.wait_closed()


@pytest.mark.timeout(10)
async def test_absolute_deadline_closes_stalled_tls_handshake(monkeypatch):
    root = Path(__file__).resolve().parents[1] / "apify" / "x-reader"
    monkeypatch.syspath_prepend(str(root))
    reader = importlib.import_module("public_profile")
    disconnected = asyncio.Event()
    tasks = set()
    received = []

    async def serve(stream, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            received.append(await stream.read(65536))
            assert await stream.read() == b""
            disconnected.set()
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setattr(reader, "PROFILE_ORIGIN", f"https://127.0.0.1:{port}/")
    started = time.monotonic()
    try:
        with pytest.raises(reader.ProfileFailure) as failure:
            await asyncio.wait_for(reader.fetch_profile("alice", started + 0.5), 2)
        await asyncio.wait_for(disconnected.wait(), 1)
        assert time.monotonic() - started < 1.5
        assert received[0].startswith(b"\x16\x03")
        assert failure.value.diagnostics["outcome"] == "deadline_exhausted"
        assert failure.value.diagnostics["attemptCount"] == 1
    finally:
        server.close()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await server.wait_closed()
