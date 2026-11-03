import base64
import gzip
import hashlib
import importlib
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

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
QUERY = (
    "api_token=query-secret,token-comma;token-tail"
    "&auth=redirect-auth-credential,auth-comma;auth-tail"
    "&AuTh=mixed-prefix,mixed-comma;mixed-tail"
    "&api-token=dash-prefix,dash-comma;dash-tail"
    "&apiToken=camel-prefix,camel-comma;camel-tail&view=public"
)
SAFE_QUERY = (
    "api_token=[REDACTED]&auth=[REDACTED]&AuTh=[REDACTED]"
    "&api-token=[REDACTED]&apiToken=[REDACTED]&view=public"
)
QUERY_SECRET_PARTS = (
    "query-secret",
    "token-comma",
    "token-tail",
    "redirect-auth-credential",
    "auth-comma",
    "auth-tail",
    "mixed-prefix",
    "mixed-comma",
    "mixed-tail",
    "dash-prefix",
    "dash-comma",
    "dash-tail",
    "camel-prefix",
    "camel-comma",
    "camel-tail",
)
SECRET_TEXT = (
    b" configured-secret Cookie: cookie-secret\n1.https://reader:db-password@host/path?" + QUERY.encode()
)
SAFE_TEXT = b" [REDACTED] Cookie: [REDACTED]\n1.https://reader:[REDACTED]@host/path?" + SAFE_QUERY.encode()
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
                self.respond(302, b"", {"Location": "/alice?" + QUERY})
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
        reader,
        "httpx",
        SimpleNamespace(
            AsyncClient=lambda **kwargs: HTTP_CLIENT(transport=ProfileTransport(origin), **kwargs),
            HTTPError=httpx.HTTPError,
        ),
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
    assert response["finalUrl"].encode() == ("https://x.com/alice?" + SAFE_QUERY).encode()
    assert response["readError"] is None
    if articles is not None:
        assert response["articleCount"] == response["statusLinkCount"] == articles
        assert response["textCount"] == 0 and response["scriptCount"] == int(compressed)
        expected_title = raw.decode().split("<title>")[1].split("</title>")[0] if raw else ""
        assert response["title"] == expected_title
    assert state["requests"] == ["/alice", "/alice?" + QUERY]
    assert all(part not in json.dumps(output) for part in QUERY_SECRET_PARTS)
    assert all(
        secret not in json.dumps(output)
        for secret in (
            "configured-secret",
            "cookie-secret",
            "db-password",
            "query-secret",
            "redirect-auth-credential",
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
    assert metadata["bodyBytes"] == len(expected)
    assert metadata["bodySha256"] == hashlib.sha256(expected).hexdigest()
    assert all(part.encode() not in b"".join(uploads) for part in QUERY_SECRET_PARTS)
    assert all(
        secret not in b"".join(uploads)
        for secret in (
            b"configured-secret",
            b"cookie-secret",
            b"db-password",
            b"query-secret",
            b"redirect-auth-credential",
            b"header-secret",
        )
    )


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("consumer_input", ["actor", "unredacted"])
async def test_parse_metadata_survives_jsonb_and_real_multipart_upload(
    postgres_template, parse_wire, consumer_input
):
    worker, origin, state = parse_wire
    state["body"] = ("<title>Alice (@alice) / X https://host/?" + QUERY + "</title>").encode() + SECRET_TEXT
    expected_title = "Alice (@alice) / X https://host/?" + SAFE_QUERY
    expected = ("<title>" + expected_title + "</title>").encode() + SAFE_TEXT
    output = await worker.fetch_snapshot({"handles": ["alice"], "cursors": {"alice": "90"}})
    response = output["accounts"][0]["diagnostics"]["attempts"][0]["response"]
    assert response["finalUrl"].encode() == ("https://x.com/alice?" + SAFE_QUERY).encode()
    assert response["body"].encode() == expected and response["bodySha256"] == sha256(expected)
    assert response["title"].encode() == expected_title.encode()
    assert state["requests"] == ["/alice", "/alice?" + QUERY]
    assert all(part not in json.dumps(output) for part in QUERY_SECRET_PARTS)
    if consumer_input == "unredacted":
        response.update(
            finalUrl="https://x.com/alice?" + QUERY,
            title="Alice (@alice) / X https://host/?" + QUERY,
            body=state["body"].decode(),
            bodyBytes=len(state["body"]),
            bodySha256=sha256(state["body"]),
            redacted=False,
        )
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
        saved_response = saved["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]
        assert saved_response["finalUrl"].encode() == ("https://x.com/alice?" + SAFE_QUERY).encode()
        assert saved_response["title"].encode() == expected_title.encode()
        assert saved_response["body"].encode() == expected
        assert saved_response["bodyBytes"] == len(expected)
        assert saved_response["bodySha256"] == hashlib.sha256(expected).hexdigest()
        assert all(part not in json.dumps(saved) for part in QUERY_SECRET_PARTS)
        assert all(
            secret not in json.dumps(saved)
            for secret in (
                "configured-secret",
                "cookie-secret",
                "db-password",
                "query-secret",
                "redirect-auth-credential",
                "header-secret",
            )
        )
        retained_documents = documents(saved["entry_errors"])
        assert retained_documents[0]["content"] == expected
        assert retained_documents[0]["sha256"] == hashlib.sha256(expected).hexdigest()
        async with HTTP_CLIENT(transport=ProfileTransport(origin)) as http:
            await DeliveryService(repo, telegram_settings(), http=http).deliver({"notice_id": notice_id})
        uploads = [multipart(request)["document"][1] for request in state["uploads"][1:]]
        assert uploads == [document["content"] for document in retained_documents]
        metadata = json.loads(uploads[1])["attempts"][0]["response"]
        assert metadata["bodySha256"] == sha256(expected) and metadata["status"] == 200
        assert metadata["title"] == expected_title and metadata["articleCount"] == 0
        assert metadata["finalUrl"].encode() == ("https://x.com/alice?" + SAFE_QUERY).encode()
        assert metadata["bodyBytes"] == len(expected)
        assert metadata["bodySha256"] == hashlib.sha256(expected).hexdigest()
        assert all(part.encode() not in b"".join(uploads) for part in QUERY_SECRET_PARTS)
        assert all(
            secret not in b"".join(uploads)
            for secret in (
                b"configured-secret",
                b"cookie-secret",
                b"db-password",
                b"query-secret",
                b"redirect-auth-credential",
                b"header-secret",
            )
        )
        receipts = await repo.list("out.deliveries", filters={"status": "SENT", "method": "sendDocument"})
        assert {row["document_sha256"] for row in receipts} == {sha256(expected), sha256(uploads[1])}
