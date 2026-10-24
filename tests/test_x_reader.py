import base64
import gzip
import importlib
import io
import json
import time
import urllib.error
from email.message import Message
from pathlib import Path

import pytest

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

        def sleep(self, value):
            self.waits.append(value)
            self.now += value

    clock = Clock()
    monkeypatch.setattr(
        reader,
        "time",
        type("Time", (), {"monotonic": clock.monotonic, "sleep": clock.sleep, "time": time.time}),
    )
    return clock


def http_error(status=503, body=b"upstream unavailable", headers=None):
    message = Message()
    for name, value in (headers or {"Content-Type": "text/plain"}).items():
        message[name] = value
    return urllib.error.HTTPError("https://x.com/alice", status, "failure", message, io.BytesIO(body))


class ProfileResponse(io.BytesIO):
    url = "https://x.com/alice"
    headers = {}


def sequence(reader, monkeypatch, values, clock):
    attempts = []

    def open_profile(request, timeout):
        attempts.append((clock.now, request, timeout))
        value = values.pop(0)
        if isinstance(value, Exception):
            raise value
        return ProfileResponse(value)

    monkeypatch.setattr(reader.urllib.request, "urlopen", open_profile)
    return attempts


@pytest.mark.parametrize(
    "success_on,expected_times,expected_waits", [(1, [0], []), (2, [0, 1], [1]), (3, [0, 1, 6], [1, 5])]
)
def test_transient_http_retry_stops_on_first_success(
    reader, monkeypatch, clock, success_on, expected_times, expected_waits
):
    attempts = sequence(reader, monkeypatch, [http_error() for _ in range(success_on - 1)] + [HTML], clock)
    rows, size = reader.fetch_profile("alice", 100)
    assert [row["id"] for row in rows] == ["100"]
    assert rows[0]["text"] == "Hello" and size == len(HTML)
    assert [attempt[0] for attempt in attempts] == expected_times
    assert clock.waits == expected_waits
    assert all(request.get_method() == "GET" and request.data is None for _, request, _ in attempts)


@pytest.mark.parametrize("status", [408, 429, 500, 503, 599])
def test_exhaustion_retains_all_three_response_bodies(reader, monkeypatch, clock, status):
    attempts = sequence(
        reader, monkeypatch, [http_error(status, f"response-{n}".encode()) for n in range(1, 4)], clock
    )
    with pytest.raises(reader.ProfileFailure) as failure:
        reader.fetch_profile("alice", 100)
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
def test_permanent_http_failure_is_not_retried(reader, monkeypatch, clock, status):
    attempts = sequence(reader, monkeypatch, [http_error(status)], clock)
    with pytest.raises(reader.ProfileFailure) as failure:
        reader.fetch_profile("alice", 100)
    assert len(attempts) == 1 and clock.waits == []
    assert failure.value.diagnostics["outcome"] == "permanent_failure"
    assert failure.value.diagnostics["attempts"][0]["response"]["body"] == "upstream unavailable"


def test_connection_errors_retry_and_parser_errors_do_not(reader, monkeypatch, clock):
    attempts = sequence(reader, monkeypatch, [urllib.error.URLError("reset"), TimeoutError(), HTML], clock)
    assert reader.fetch_profile("alice", 100)[0][0]["id"] == "100"
    assert [a[0] for a in attempts] == [0, 1, 6]
    attempts = sequence(reader, monkeypatch, [b"<title>login</title>"], clock)
    with pytest.raises(reader.ProfileFailure) as failure:
        reader.fetch_profile("alice", 100)
    assert len(attempts) == 1
    assert str(failure.value) == "PUBLIC_PROFILE_NOT_READABLE"


@pytest.mark.parametrize("deadline,expected", [(0, []), (1, [0]), (6, [0, 1])])
def test_run_deadline_prevents_retry_and_preserves_completed_attempts(
    reader, monkeypatch, clock, deadline, expected
):
    attempts = sequence(reader, monkeypatch, [http_error() for _ in range(3)], clock)
    with pytest.raises(reader.ProfileFailure) as failure:
        reader.fetch_profile("alice", deadline)
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
    evidence = reader.response_evidence(http_error(body=wire, headers=headers))
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


def test_error_body_read_honors_deadline_and_reports_prefix(reader, clock):
    class SlowBody(io.BytesIO):
        def read1(self, size=-1):
            clock.now += 2
            return super().read(3)

    error = http_error(body=b"abcdefghij", headers={"Content-Length": "10"})
    error.fp = SlowBody(b"abcdefghij")
    evidence = reader.response_evidence(error, deadline=3)
    assert evidence["body"] == "abcdef"
    assert evidence["bodyTruncated"] and evidence["omittedWireBytes"] == 4
    assert evidence["readError"] == "RESPONSE_BODY_DEADLINE_EXHAUSTED"


def test_partial_error_response_reports_known_missing_bytes(reader):
    evidence = reader.response_evidence(http_error(body=b"prefix", headers={"Content-Length": "20"}))
    assert evidence["body"] == "prefix" and evidence["bodyTruncated"]
    assert evidence["omittedWireBytes"] == 14


def test_diagnostics_exclude_credentials_even_if_echoed_by_upstream(reader, monkeypatch):
    monkeypatch.setenv("APIFY_TOKEN", "secret-apify-value")
    evidence = reader.response_evidence(
        http_error(
            body=b"secret-apify-value\nCookie: session-secret\nAuthorization: Bearer auth-secret",
            headers={
                "Content-Type": "text/plain",
                "Set-Cookie": "session-secret",
                "Authorization": "auth-secret",
                "X-Request-ID": "safe-id",
            },
        )
    )
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
