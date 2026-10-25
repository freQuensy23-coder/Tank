import base64
import gzip
import importlib
import json
import time
from pathlib import Path

import httpx
import pytest

HTTP_CLIENT = httpx.AsyncClient
ACTOR_ROOT = Path(__file__).resolve().parents[1] / "apify" / "x-reader"
HTML = b'<title>Alice (@alice) / X</title><article><a href="/alice/status/100">post</a><div data-testid="tweetText">Hello</div></article>'


@pytest.fixture
def reader(monkeypatch):
    monkeypatch.syspath_prepend(str(ACTOR_ROOT))
    return importlib.import_module("public_profile")


@pytest.fixture
def clock(reader, monkeypatch):
    class Clock:
        now = 0.0

        def __init__(self):
            self.waits = []

        def monotonic(self):
            return self.now

        async def sleep(self, value):
            self.waits.append(value)
            self.now += value

    clock = Clock()
    monkeypatch.setattr(
        reader,
        "time",
        type("Time", (), {"monotonic": clock.monotonic, "time": time.time}),
    )
    monkeypatch.setattr(reader.asyncio, "sleep", clock.sleep)
    return clock


class BodyStream(httpx.AsyncByteStream):
    def __init__(self, body):
        self.body = body
        self.closed = False

    async def __aiter__(self):
        yield self.body

    async def aclose(self):
        self.closed = True


def http_error(status=503, body=b"upstream unavailable", headers=None):
    return httpx.Response(status, headers=headers or {"Content-Type": "text/plain"}, stream=BodyStream(body))


def sequence(reader, monkeypatch, values, clock):
    attempts = []

    def open_profile(request):
        attempts.append((clock.now, request, request.extensions["timeout"]["connect"]))
        value = values.pop(0)
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, httpx.Response) else httpx.Response(200, stream=BodyStream(value))

    monkeypatch.setattr(
        reader.httpx,
        "AsyncClient",
        lambda **kwargs: HTTP_CLIENT(transport=httpx.MockTransport(open_profile), **kwargs),
    )
    return attempts


@pytest.mark.parametrize(
    "success_on,expected_times,expected_waits", [(1, [0], []), (2, [0, 1], [1]), (3, [0, 1, 6], [1, 5])]
)
async def test_transient_http_retry_stops_on_first_success(
    reader, monkeypatch, clock, success_on, expected_times, expected_waits
):
    attempts = sequence(reader, monkeypatch, [http_error() for _ in range(success_on - 1)] + [HTML], clock)
    rows, size = await reader.fetch_profile("alice", 100)
    assert [row.id for row in rows] == ["100"]
    assert rows[0].text == "Hello" and size == len(HTML)
    assert [attempt[0] for attempt in attempts] == expected_times
    assert clock.waits == expected_waits
    assert all(request.method == "GET" and request.content == b"" for _, request, _ in attempts)


@pytest.mark.parametrize("status", [408, 429, 500, 503, 599])
async def test_exhaustion_retains_all_three_response_bodies(reader, monkeypatch, clock, status):
    attempts = sequence(
        reader, monkeypatch, [http_error(status, f"response-{n}".encode()) for n in range(1, 4)], clock
    )
    with pytest.raises(reader.ProfileFailure) as failure:
        await reader.fetch_profile("alice", 100)
    diagnostics = failure.value.diagnostics
    assert [a[0] for a in attempts] == [0, 1, 6] and clock.waits == [1, 5]
    assert diagnostics["attemptCount"] == 3 and diagnostics["outcome"] == "exhausted"
    assert [a["response"]["body"] for a in diagnostics["attempts"]] == [
        "response-1",
        "response-2",
        "response-3",
    ]
    assert diagnostics["request"] == {"method": "GET", "url": "https://x.com/alice", "body": None}
    assert all(a["status"] == status and a["retryable"] for a in diagnostics["attempts"])


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410])
async def test_permanent_http_failure_is_not_retried(reader, monkeypatch, clock, status):
    attempts = sequence(reader, monkeypatch, [http_error(status)], clock)
    with pytest.raises(reader.ProfileFailure) as failure:
        await reader.fetch_profile("alice", 100)
    assert len(attempts) == 1 and clock.waits == []
    assert failure.value.diagnostics["outcome"] == "permanent_failure"
    assert failure.value.diagnostics["attempts"][0]["response"]["body"] == "upstream unavailable"


async def test_connection_errors_retry_and_parser_errors_do_not(reader, monkeypatch, clock):
    attempts = sequence(
        reader, monkeypatch, [httpx.ConnectError("reset"), httpx.ReadTimeout("timeout"), HTML], clock
    )
    assert (await reader.fetch_profile("alice", 100))[0][0].id == "100"
    assert [a[0] for a in attempts] == [0, 1, 6]
    attempts = sequence(reader, monkeypatch, [b"<title>login</title>"], clock)
    with pytest.raises(reader.ProfileFailure) as failure:
        await reader.fetch_profile("alice", 100)
    assert len(attempts) == 1
    assert str(failure.value) == "PUBLIC_PROFILE_NOT_READABLE"


@pytest.mark.parametrize("deadline,expected", [(0, []), (1, [0]), (6, [0, 1])])
async def test_run_deadline_prevents_retry_and_preserves_completed_attempts(
    reader, monkeypatch, clock, deadline, expected
):
    attempts = sequence(reader, monkeypatch, [http_error() for _ in range(3)], clock)
    with pytest.raises(reader.ProfileFailure) as failure:
        await reader.fetch_profile("alice", deadline)
    assert [a[0] for a in attempts] == expected
    assert failure.value.diagnostics["attemptCount"] == len(expected)
    assert failure.value.diagnostics["outcome"] == "deadline_exhausted"
    assert all(timeout <= deadline - start for start, _, timeout in attempts)


@pytest.mark.parametrize("kind", ["gzip", "binary", "oversized", "gzip_oversized", "broken_gzip"])
def test_response_capture_is_bounded_and_preserves_encoding(reader, kind):
    content = (
        b"full response body"
        if kind == "gzip"
        else b"\x00\xff\x80binary"
        if kind == "binary"
        else b"a" * (reader.BODY_LIMIT + 50)
    )
    wire = (
        gzip.compress(content)
        if kind.startswith("gzip")
        else b"not gzip"
        if kind == "broken_gzip"
        else content
    )
    headers = {"Content-Length": str(len(wire)), "Content-Type": "application/octet-stream"}
    if "gzip" in kind:
        headers["Content-Encoding"] = "gzip"
    evidence = reader.response_evidence(http_error(headers=headers), wire)
    evidence = evidence.model_dump(mode="json", exclude_unset=True)
    captured = (
        base64.b64decode(evidence["body"])
        if evidence["bodyEncoding"] == "base64"
        else evidence["body"].encode()
    )
    assert captured == (wire if kind == "broken_gzip" else content)[: reader.BODY_LIMIT]
    assert evidence["bodyBytes"] == len(captured)
    assert evidence["bodyTruncated"] == (kind in {"oversized", "gzip_oversized"})
    if kind == "oversized":
        assert evidence["omittedWireBytes"] == 50
    if kind == "broken_gzip":
        assert evidence["decodeError"] == "GZIP_DECODE_FAILED_WIRE_PREFIX"


async def test_partial_error_stream_preserves_prefix_and_closes_transport(reader, monkeypatch, clock):
    class BrokenBody(BodyStream):
        async def __aiter__(self):
            yield b"prefix"
            raise httpx.RemoteProtocolError("truncated body")

    stream = BrokenBody(b"")
    sequence(
        reader,
        monkeypatch,
        [httpx.Response(403, headers={"Content-Length": "20"}, stream=stream)],
        clock,
    )
    with pytest.raises(reader.ProfileFailure) as failure:
        await reader.fetch_profile("alice", 100)
    evidence = failure.value.diagnostics["attempts"][0]["response"]
    assert evidence["body"] == "prefix"
    assert evidence["bodyTruncated"] and evidence["omittedWireBytes"] == 14
    assert evidence["readError"] == "RESPONSE_BODY_READ_FAILED"
    assert stream.closed


def test_partial_error_response_reports_known_missing_bytes(reader):
    evidence = reader.response_evidence(http_error(headers={"Content-Length": "20"}), b"prefix")
    evidence = evidence.model_dump(mode="json", exclude_unset=True)
    assert evidence["body"] == "prefix" and evidence["bodyTruncated"]
    assert evidence["omittedWireBytes"] == 14


def test_diagnostics_exclude_credentials_even_if_echoed_by_upstream(reader, monkeypatch):
    monkeypatch.setenv("APIFY_TOKEN", "secret-apify-value")
    evidence = reader.response_evidence(
        http_error(
            headers={
                "Content-Type": "text/plain",
                "Set-Cookie": "session-secret",
                "Authorization": "auth-secret",
                "X-Request-ID": "safe-id",
            },
        ),
        b"secret-apify-value\nCookie: session-secret\nAuthorization: Bearer auth-secret",
    )
    evidence = evidence.model_dump(mode="json", exclude_unset=True)
    serialized = json.dumps(evidence)
    assert all(secret not in serialized for secret in ("secret-apify-value", "session-secret", "auth-secret"))
    assert evidence["headers"] == {"content-type": "text/plain", "x-request-id": "safe-id"}
    assert evidence["redacted"]


async def test_failed_actor_output_keeps_cursor_and_transports_diagnostics(reader, monkeypatch, clock):
    sequence(reader, monkeypatch, [http_error() for _ in range(3)], clock)
    fetch_snapshot = importlib.import_module("direct_fetch").fetch_snapshot
    result = await fetch_snapshot({"handles": ["alice"], "cursors": {"alice": "90"}})
    assert result["health"] == "failed" and result["posts"] == []
    assert result["nextCursors"] == {"alice": "90"}
    assert result["accounts"][0]["diagnostics"]["attemptCount"] == 3
    assert result["failedAccounts"] == result["accounts"]


async def test_recovered_actor_has_no_terminal_failure(reader, monkeypatch, clock):
    sequence(reader, monkeypatch, [http_error(), HTML], clock)
    fetch_snapshot = importlib.import_module("direct_fetch").fetch_snapshot
    result = await fetch_snapshot({"handles": ["alice"], "cursors": {"alice": "90"}})
    assert result["health"] == "degraded" and result["failedAccounts"] == []
    assert result["nextCursors"] == {"alice": "100"}
    assert "diagnostics" not in result["accounts"][0]


@pytest.mark.parametrize(
    "raw,expected",
    [
        (b'{"cookie":"session","detail":"upstream A"}', b'{"cookie":"[REDACTED]","detail":"upstream A"}'),
        (b'{"ct0":"escaped\\"secret","detail":1}', b'{"ct0":"[REDACTED]","detail":1}'),
        (b"cookie=session; detail=upstream A", b"cookie=[REDACTED]; detail=upstream A"),
        (b"?auth_token=session&detail=upstream+A", b"?auth_token=[REDACTED]&detail=upstream+A"),
        (b"authorization: Basic abc123, detail=upstream A", b"authorization: [REDACTED], detail=upstream A"),
        (
            b'\x00\xff{"cookie":"session","detail":"upstream A"}',
            b'\x00\xff{"cookie":"[REDACTED]","detail":"upstream A"}',
        ),
    ],
)
def test_credential_value_redaction_preserves_adjacent_response_evidence(reader, raw, expected):
    evidence = reader.response_evidence(http_error(), raw)
    evidence = evidence.model_dump(mode="json", exclude_unset=True)
    safe = (
        base64.b64decode(evidence["body"])
        if evidence["bodyEncoding"] == "base64"
        else evidence["body"].encode()
    )
    assert safe == expected
    assert evidence["bodySha256"] == reader.hashlib.sha256(expected).hexdigest()
    assert evidence["redacted"]


@pytest.mark.integration
@pytest.mark.parametrize("count", [86, 200])
@pytest.mark.parametrize("kind", ["json_escaped", "binary"])
async def test_actor_outage_bounds_aggregate_evidence_and_uploads_retained_prefixes(
    reader, monkeypatch, clock, count, kind, tmp_path: Path
):
    import tracemalloc

    from test_x_diagnostics import multipart, notice_for, telegram_settings, telegram_transport

    from tank.delivery import DeliveryService
    from tank.storage import MemoryRepository

    raw = (b"\x00\xff\x80" if kind == "binary" else b'"\\\t') * (reader.BODY_LIMIT // 3 + 1)
    raw = raw[: reader.BODY_LIMIT]
    names = [f"account{index}" for index in range(count)]
    streams = []

    def open_profile(request):
        response = http_error(body=raw, headers={"Content-Length": str(len(raw))})
        streams.append(response.stream)
        return response

    monkeypatch.setattr(
        reader.httpx,
        "AsyncClient",
        lambda **kwargs: HTTP_CLIENT(transport=httpx.MockTransport(open_profile), **kwargs),
    )
    monkeypatch.setattr(reader, "run_deadline", lambda: 10000)
    fetch_snapshot = importlib.import_module("direct_fetch").fetch_snapshot
    tracemalloc.start()
    try:
        output = await fetch_snapshot({"handles": names, "cursors": dict.fromkeys(names, "90")})
        serialized = json.dumps(output, ensure_ascii=True).encode()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 128_000_000
    assert len(serialized) < 2 * reader.RUN_EVIDENCE_LIMIT + 2_000_000
    assert len(output["accounts"]) == len(output["failedAccounts"]) == count
    assert output["posts"] == [] and output["nextCursors"] == dict.fromkeys(names, "90")
    assert len(streams) == 3 * count and all(stream.closed for stream in streams)
    total = 0
    for account in output["accounts"]:
        diagnostics = account["diagnostics"]
        assert diagnostics["attemptCount"] == 3
        for attempt in diagnostics["attempts"]:
            response = attempt["response"]
            body = response["body"]
            captured = base64.b64decode(body) if response["bodyEncoding"] == "base64" else body.encode()
            assert captured and raw.startswith(captured)
            assert response["bodyTruncated"] and response["omittedWireBytes"] > 0
            assert response["serializedBodyLimitBytes"] == reader.RUN_EVIDENCE_LIMIT // (3 * count)
            total += len(json.dumps(body, ensure_ascii=True))
    assert total <= reader.RUN_EVIDENCE_LIMIT
    evidence = output["accounts"][0]["diagnostics"]
    repo = MemoryRepository()
    notice = await notice_for(repo, evidence, tmp_path)
    sent = []

    def send(request):
        if request.url.path.endswith("sendDocument"):
            sent.append(multipart(request)["document"][1])
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent) + 1}})

    async with HTTP_CLIENT(transport=telegram_transport(send)) as http:
        await DeliveryService(repo, telegram_settings(tmp_path), http=http).deliver(
            {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        )
    expected = evidence["attempts"][0]["response"]
    prefix = (
        base64.b64decode(expected["body"])
        if expected["bodyEncoding"] == "base64"
        else expected["body"].encode()
    )
    assert sent[:3] == [prefix] * 3
    metadata = json.loads(sent[3])
    assert all(attempt["response"]["bodyTruncated"] for attempt in metadata["attempts"])
    assert all(
        attempt["response"]["captureLimitBytes"] == expected["captureLimitBytes"]
        for attempt in metadata["attempts"]
    )
