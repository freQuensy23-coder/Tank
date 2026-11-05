import base64
import copy
import gzip
import hashlib
import importlib
import json
import os

import httpx
import pytest
from test_sources import settings as source_settings
from test_x_body_capture_boundary_privacy import evidence_for
from test_x_diagnostics import failed_run, multipart
from test_x_reader import reader as reader
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server

from tank.delivery import DeliveryService
from tank.sources import SourceService
from tank.sources.x_diagnostics import documents, sanitize

TOKEN = "unique-wire-boundary-credential-secret-tail"


def body_bytes(response):
    return (
        base64.b64decode(response["body"], validate=True)
        if response["bodyEncoding"] == "base64"
        else response["body"].encode()
    )


@pytest.mark.parametrize("limit", [64, 2_000_000], ids=["small", "full"])
@pytest.mark.parametrize("binary", [False, True], ids=["text", "binary"])
async def test_real_producer_capture_removes_known_boundary_prefix(reader, monkeypatch, limit, binary):
    monkeypatch.setenv("APIFY_TOKEN", TOKEN)
    prefix = (b"\x00\xff" if binary else b"ok") + b"x" * (limit - 6)
    raw = prefix + TOKEN.encode() + b"unretained"

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield raw
            raise AssertionError("capture continued beyond owned limit")

    response = httpx.Response(503, stream=Stream(), headers={"Content-Length": str(len(raw))})
    parts = []
    await reader.read_body(response, parts, limit)
    assert len(b"".join(parts)) == limit + 1
    produced = reader.response_evidence(response, b"".join(parts), limit=limit)
    assert body_bytes(produced) == prefix
    assert produced["wireBytes"] == limit and produced["bodyTruncated"] and produced["redacted"]
    assert produced["bodySha256"] == hashlib.sha256(prefix).hexdigest()
    evidence = evidence_for(b"", "utf-8")
    evidence["attempts"][0]["response"] = produced
    clean = sanitize(evidence, TOKEN)
    assert body_bytes(clean["attempts"][0]["response"]) == prefix
    assert documents([{"diagnostics": clean}])[0]["content"] == prefix


@pytest.mark.parametrize("mode", ["declared", "read_error", "gzip", "serialized"])
def test_producer_incomplete_paths_trim_known_suffix(reader, monkeypatch, mode):
    monkeypatch.setenv("APIFY_API_TOKEN", TOKEN)
    raw = b"safe uniq"
    headers = {"Content-Length": "100"} if mode == "declared" else {}
    error = "READ_FAILED" if mode == "read_error" else None
    limit = 64
    if mode == "gzip":
        raw = gzip.compress(b"x" * 500 + TOKEN.encode() + b"tail")
        headers = {"Content-Encoding": "gzip"}
        limit = 504
    if mode == "serialized":
        raw = b"safe uniqX " + b"x" * 80
        limit = 200
    response = httpx.Response(503, headers=headers)
    produced = reader.response_evidence(
        response, raw, error, limit=limit, serialized_limit=54 if mode == "serialized" else None
    )
    expected = {"gzip": b"x" * 500, "serialized": b"safe uniq"}.get(mode, b"safe ")
    assert body_bytes(produced) == expected
    assert produced["bodyTruncated"]
    assert produced["redacted"] is (mode != "serialized")


@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
@pytest.mark.parametrize(
    "raw,token,limit,expected,truncated",
    [
        (b"safe uniq", TOKEN, 100, b"safe ", True),
        (b"safe unique-wire", TOKEN, 9, b"safe ", True),
        (b"safe uniqX", TOKEN, 9, b"safe uniq", True),
        (b"safe uniq", TOKEN, 100, b"safe uniq", False),
        (b"safe ababa", "ababab", 100, b"safe ", True),
        ("safe π🙂".encode(), "π🙂tail", 100, b"safe ", True),
        (b"safe \x00uniq", TOKEN, 100, b"safe \x00", True),
        (b"", TOKEN, 0, b"", True),
    ],
    ids=["upstream", "own_before_upstream", "disproved", "complete", "overlap", "unicode", "nul", "empty"],
)
def test_consumer_sanitizes_upstream_boundary_without_backfill(
    encoding, raw, token, limit, expected, truncated
):
    evidence = evidence_for(raw, encoding)
    evidence["attempts"][0]["response"]["bodyTruncated"] = truncated
    budget = [limit]
    clean = sanitize(evidence, token, budget)
    response = clean["attempts"][0]["response"]
    assert body_bytes(response) == expected
    assert response["consumerCapturedBytes"] == min(len(raw), limit)
    assert response["bodySha256"] == hashlib.sha256(expected).hexdigest()
    assert response["redacted"] is (expected != raw[:limit])
    assert budget == [limit - max(min(len(raw), limit), len(expected))]


@pytest.mark.parametrize("binary", [False, True], ids=["text", "binary"])
@pytest.mark.integration
@pytest.mark.postgres
async def test_producer_returned_prefix_jsonb_checkpoint_immutable_ops_and_multipart(
    reader, monkeypatch, tmp_path, postgres_template, wire_server, binary
):
    monkeypatch.setenv("ACTOR_TOKEN", TOKEN)
    limit = 2_000_000
    prefix = (b"\x00\xff" if binary else b"ok") + b"x" * (limit - 6)
    produced = reader.response_evidence(
        httpx.Response(503, headers={"Content-Length": str(limit + 100)}),
        prefix + TOKEN.encode()[:5],
        limit=limit,
    )
    assert body_bytes(produced) == prefix
    evidence = evidence_for(b"", "utf-8")
    evidence["attempts"][0]["response"] = produced
    broken = failed_run(evidence)

    async def upstream(payload):
        assert payload == {"handles": ["alice"], "cursors": {"alice": "90"}}
        return copy.deepcopy(broken)

    monkeypatch.setattr(importlib.import_module("direct_fetch"), "fetch_snapshot", upstream)
    origin, state = wire_server
    config = source_settings(
        tmp_path,
        [{"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": origin + "/actor"}]}],
    )
    config.apify_api_token = TOKEN
    config.telegram_token = "isolated-telegram-token"
    config.telegram_ops_chat = "ops"
    config.ops_progress_enabled = True
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"alice": "90"}})
        async with httpx.AsyncClient() as http:
            service = SourceService(repo, config, http=http)
            service.accounts = {"alice": {"handle": "alice", "role": "OTHER", "entity_id": "alice"}}
            result = await service.poll_once("S29")
        checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
        assert result["status"] == "DEGRADED" and result["items"] == 0
        assert checkpoint["account_cursors"] == {"alice": "90"}
        stored = checkpoint["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]
        assert body_bytes(stored) == prefix
        assert stored["consumerCapturedBytes"] == len(prefix)
        assert stored["bodyTruncated"] and not stored["consumerCaptureTruncated"]
        [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
        [marker] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERROR"})
        assert notice["entry_errors"] == checkpoint["entry_errors"]
        assert marker["entry_error"] == checkpoint["entry_errors"][0]
        [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
        async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
            await DeliveryService(repo, config, http=http).deliver(job["payload"])
        uploads = [
            multipart(httpx.Request("POST", origin, headers=headers, content=body))["document"]
            for route, headers, body in state["telegram"]
            if route.endswith("sendDocument")
        ]
        assert len(uploads) == 2 and uploads[0][1] == prefix
        assert notice["document_manifest"] == [
            {"filename": name, "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}
            for name, content in uploads
        ]
        receipts = await repo.list("out.deliveries", filters={"status": "SENT", "method": "sendDocument"})
        assert {row["document_sha256"] for row in receipts} == {
            hashlib.sha256(content).hexdigest() for _, content in uploads
        }
        assert await repo.get("out.ops_notices", notice["notice_id"]) == notice
        for payload in [json.dumps(checkpoint).encode(), json.dumps(notice).encode(), *(v for _, v in uploads)]:
            assert b"uniq" not in payload and b"secret-tail" not in payload
