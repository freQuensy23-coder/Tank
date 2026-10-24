import asyncio
import gzip
import importlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from test_sources import settings as source_settings
from test_x_diagnostics import multipart

from tank.delivery import DeliveryService
from tank.sources import SourceService
from tank.storage import MemoryRepository
from tank.util import sha256

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
            headers={key: value for key, value in request.headers.items() if key.lower() != "host"},
            stream=request.stream,
            extensions=request.extensions,
        )
        return await self.transport.handle_async_request(target)

    async def aclose(self):
        await self.transport.aclose()


async def test_real_worker_http_errors_flow_through_source_checkpoint_to_multipart_ops_documents(
    tmp_path, wire_server
):
    origin, state = wire_server
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
    uploads = [
        multipart(httpx.Request("POST", origin + path, headers=headers, content=payload))
        for path, headers, payload in state["telegram"][1:]
    ]
    assert [upload["document"][1] for upload in uploads[:3]] == [state["body"]] * 3
    assert all(upload["chat_id"][1] == b"isolated-ops" for upload in uploads)
    metadata = json.loads(uploads[3]["document"][1])
    assert all(attempt["response"]["bodySha256"] == sha256(state["body"]) for attempt in metadata["attempts"])
    assert all("set-cookie" not in attempt["response"]["headers"] for attempt in metadata["attempts"])
    assert metadata["request"]["body"] is None
    receipts = await repo.list("out.deliveries", filters={"status": "SENT"})
    assert len(receipts) == 5
