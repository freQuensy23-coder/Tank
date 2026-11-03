import base64
import gzip
import importlib
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from test_x_diagnostics import multipart, telegram_settings

from tank.delivery import DeliveryService
from tank.ops_entry_errors import report_entry_errors
from tank.sources.x_diagnostics import documents
from tank.sources.xsensor import XSensor
from tank.storage import MemoryRepository
from tank.util import sha256

HTTP_CLIENT = httpx.AsyncClient
SECRET_TEXT = (
    b" configured-secret Cookie: cookie-secret\n"
    b"1.https://reader:db-password@host/path?api_token=query-secret"
)
SAFE_TEXT = (
    b" [REDACTED] Cookie: [REDACTED]\n"
    b"1.https://reader:[REDACTED]@host/path?api_token=[REDACTED]"
)
CASES = [
    (200, b"<title>This page is down / X</title>", "PUBLIC_PROFILE_NOT_READABLE", False, 0),
    (200, b"", "PUBLIC_PROFILE_NOT_READABLE", False, 0),
    (204, b"", "PUBLIC_PROFILE_NOT_READABLE", False, 0),
    (
        200,
        b"<title>Sign in / X</title><form>Blocked</form>",
        "PUBLIC_PROFILE_NOT_READABLE",
        False,
        0,
    ),
    (
        202,
        b"<title>Alice (@alice) / X</title><script>page</script>",
        "NO_PUBLIC_POSTS_VISIBLE",
        True,
        0,
    ),
    (
        200,
        b'<title>Alice (@alice) / X</title><article><a href="/alice/status/100">post</a></article>',
        "PUBLIC_POST_BODY_MISSING",
        False,
        1,
    ),
    (200, b"\xff\x00original", "PUBLIC_PROFILE_NOT_READABLE", False, None),
]


class ProfileTransport(httpx.AsyncBaseTransport):
    def __init__(self, origin):
        self.origin = origin
        self.transport = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request):
        target = httpx.Request(
            request.method,
            self.origin + request.url.raw_path.decode(),
            headers=request.headers,
            stream=request.stream,
            extensions=request.extensions,
        )
        return await self.transport.handle_async_request(target)

    async def aclose(self):
        await self.transport.aclose()


@pytest.fixture
def parse_wire(monkeypatch):
    root = Path(__file__).resolve().parents[1] / "apify" / "x-reader"
    monkeypatch.syspath_prepend(str(root))
    reader = importlib.import_module("public_profile")
    worker = importlib.import_module("direct_fetch")
    state = {"requests": [], "uploads": [], "body": b"", "status": 200, "gzip": False}

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
            state["requests"].append(self.path)
            if "?" not in self.path:
                self.respond(302, b"", {"Location": "/alice?api_token=query-secret"})
                return
            body = gzip.compress(state["body"]) if state["gzip"] else state["body"]
            headers = {"Content-Type": "text/html", "Set-Cookie": "header-secret"}
            if state["gzip"]:
                headers["Content-Encoding"] = "gzip"
            self.respond(state["status"], body, headers)

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            state["uploads"].append(
                httpx.Request(
                    "POST",
                    "https://api.telegram.org" + self.path,
                    headers=dict(self.headers),
                    content=body,
                )
            )
            self.respond(
                200,
                json.dumps({"ok": True, "result": {"message_id": len(state["uploads"])}}).encode(),
                {"Content-Type": "application/json"},
            )

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setenv("APIFY_TOKEN", "configured-secret")
    monkeypatch.setattr(
        reader.httpx,
        "AsyncClient",
        lambda **kwargs: HTTP_CLIENT(transport=ProfileTransport(origin), **kwargs),
    )
    try:
        yield worker, origin, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.integration
@pytest.mark.parametrize("status,raw,reason,compressed,articles", CASES)
async def test_real_actor_2xx_parse_failure_bytes_reach_ops_without_cursor_advance(
    parse_wire, status, raw, reason, compressed, articles
):
    worker, origin, state = parse_wire
    body = raw + SECRET_TEXT if raw else raw
    expected = raw + SAFE_TEXT if raw else raw
    state.update(status=status, body=body, gzip=compressed)
    output = await worker.fetch_snapshot({"handles": ["alice"], "cursors": {"alice": "90"}})
    assert output["health"] == "failed" and output["ok"] is False and output["posts"] == []
    assert output["nextCursors"] == {"alice": "90"}
    assert output["failedAccounts"] == output["accounts"]
    account = output["accounts"][0]
    assert account["error"] == reason
    diagnostic = account["diagnostics"]
    assert diagnostic["attemptCount"] == 1 and diagnostic["outcome"] == "permanent_failure"
    [attempt] = diagnostic["attempts"]
    assert attempt["error"] == reason and attempt["retryable"] is False and attempt["status"] == status
    response = attempt["response"]
    retained = (
        base64.b64decode(response["body"])
        if response["bodyEncoding"] == "base64"
        else response["body"].encode()
    )
    assert retained == expected and response["bodySha256"] == sha256(expected)
    assert response["status"] == status and response["bodyBytes"] == len(expected)
    assert response["bodyTruncated"] is False
    assert response["finalUrl"] == "https://x.com/alice?api_token=[REDACTED]"
    assert response["readError"] is None
    if articles is not None:
        assert response["articleCount"] == response["statusLinkCount"] == articles
        assert response["textCount"] == 0 and response["scriptCount"] == int(compressed)
        expected_title = raw.decode().split("<title>")[1].split("</title>")[0] if raw else ""
        assert response["title"] == expected_title
    assert state["requests"] == ["/alice", "/alice?api_token=query-secret"]
    assert all(
        secret not in json.dumps(output)
        for secret in (
            "configured-secret",
            "cookie-secret",
            "db-password",
            "query-secret",
            "header-secret",
        )
    )
    async with HTTP_CLIENT(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, json=output))
    ) as http:
        scan = await XSensor(http, "configured-secret").posts(
            "https://actor.invalid", ["alice"], {"alice": "90"}
        )
    assert scan.failed and scan.cursors == {"alice": "90"} and scan.rows == []
    [failure] = scan.failures
    assert failure.reason == reason
    assert failure.diagnostics["attempts"][0]["response"]["finalUrl"] == response["finalUrl"]
    repo = MemoryRepository()
    notice_id = await report_entry_errors(
        repo,
        telegram_settings(),
        {"source_id": "S29", "name": "X"},
        {"url": "https://actor.invalid"},
        [{"account": "alice", "reason": reason, "diagnostics": failure.diagnostics}],
        "2026-11-03T20:00:00Z",
    )
    async with HTTP_CLIENT(transport=ProfileTransport(origin)) as http:
        await DeliveryService(repo, telegram_settings(), http=http).deliver({"notice_id": notice_id})
    uploads = [multipart(request)["document"][1] for request in state["uploads"][1:]]
    assert len(uploads) == (2 if expected else 1)
    if expected:
        assert uploads[0] == expected
    metadata = json.loads(uploads[-1])["attempts"][0]["response"]
    assert metadata["status"] == status and metadata["bodySha256"] == sha256(expected)
    assert metadata["finalUrl"] == response["finalUrl"]
    assert all(
        secret not in b"".join(uploads)
        for secret in (
            b"configured-secret",
            b"cookie-secret",
            b"db-password",
            b"query-secret",
            b"header-secret",
        )
    )


@pytest.mark.integration
@pytest.mark.postgres
async def test_parse_metadata_survives_jsonb_and_real_multipart_upload(postgres_template, parse_wire):
    worker, origin, state = parse_wire
    state["body"] = b"<title>Alice (@alice) / X</title>" + SECRET_TEXT
    expected = b"<title>Alice (@alice) / X</title>" + SAFE_TEXT
    output = await worker.fetch_snapshot({"handles": ["alice"], "cursors": {"alice": "90"}})
    async with HTTP_CLIENT(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, json=output))
    ) as http:
        scan = await XSensor(http, "configured-secret").posts(
            "https://actor.invalid", ["alice"], {"alice": "90"}
        )
    assert scan.failed and scan.cursors == {"alice": "90"} and scan.rows == []
    [failure] = scan.failures
    assert failure.reason == "NO_PUBLIC_POSTS_VISIBLE"
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice_id = await report_entry_errors(
            repo,
            telegram_settings(),
            {"source_id": "S29", "name": "X"},
            {"url": "https://actor.invalid"},
            [{"account": "alice", "reason": failure.reason, "diagnostics": failure.diagnostics}],
            "2026-11-03T20:00:00Z",
        )
        saved = await repo.get("out.ops_notices", notice_id)
        retained_documents = documents(saved["entry_errors"])
        assert retained_documents[0]["content"] == expected
        async with HTTP_CLIENT(transport=ProfileTransport(origin)) as http:
            await DeliveryService(repo, telegram_settings(), http=http).deliver({"notice_id": notice_id})
        uploads = [multipart(request)["document"][1] for request in state["uploads"][1:]]
        assert uploads == [document["content"] for document in retained_documents]
        metadata = json.loads(uploads[1])["attempts"][0]["response"]
        assert metadata["bodySha256"] == sha256(expected) and metadata["status"] == 200
        assert metadata["title"] == "Alice (@alice) / X" and metadata["articleCount"] == 0
        assert metadata["finalUrl"] == "https://x.com/alice?api_token=[REDACTED]"
        assert all(
            secret not in b"".join(uploads)
            for secret in (
                b"configured-secret",
                b"cookie-secret",
                b"db-password",
                b"query-secret",
                b"header-secret",
            )
        )
        receipts = await repo.list(
            "out.deliveries", filters={"status": "SENT", "method": "sendDocument"}
        )
        assert {row["document_sha256"] for row in receipts} == {sha256(expected), sha256(uploads[1])}
