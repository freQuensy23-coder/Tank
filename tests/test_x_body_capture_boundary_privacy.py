import base64
import copy
import hashlib
import importlib
import json
import os

import httpx
import pytest
from test_sources import settings as source_settings
from test_x_diagnostics import diagnostics, failed_run, multipart
from test_x_metadata_boundary_privacy import expected_metadata
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server

from tank.delivery import DeliveryService
from tank.sources import SourceService
from tank.sources.x_diagnostics import documents, sanitize


TOKEN = "unique-boundary-credential-π-secret-tail"


def evidence_for(raw, encoding):
    evidence = diagnostics(
        base64.b64encode(raw).decode() if encoding == "base64" else raw.decode(), encoding=encoding
    )
    evidence["attempts"] = evidence["attempts"][:1]
    return evidence


def assert_response(
    clean, expected, captured, limit, capture_clipped, transform_clipped, redacted, encoding
):
    response = clean["attempts"][0]["response"]
    actual = (
        base64.b64decode(response["body"], validate=True)
        if response["bodyEncoding"] == "base64"
        else response["body"].encode()
    )
    assert actual == expected
    assert response["bodyEncoding"] == encoding
    assert response["bodyBytes"] == len(expected)
    assert response["bodySha256"] == hashlib.sha256(expected).hexdigest()
    assert response["consumerCapturedBytes"] == captured
    assert response["consumerBodyLimitBytes"] == limit
    assert response["consumerCaptureTruncated"] is capture_clipped
    assert response["consumerTransformTruncated"] is transform_clipped
    assert response["bodyTruncated"] is (capture_clipped or transform_clipped)
    assert response["redacted"] is redacted
    attachments = documents([{"account": "alice", "diagnostics": clean}])
    if expected:
        assert attachments[0]["content"] == expected
        assert attachments[0]["sha256"] == hashlib.sha256(expected).hexdigest()
    return attachments


@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
@pytest.mark.parametrize("limit", [8192, 2_000_000])
@pytest.mark.parametrize("prefix_kind", ["ascii", "multibyte", "controls"])
def test_known_token_crossing_owned_capture_boundary(encoding, limit, prefix_kind):
    lead = {
        "ascii": b"safe ",
        "multibyte": "🙂é ".encode(),
        "controls": b"\x00\x01\t",
    }[prefix_kind]
    prefix = lead + b"x" * (limit - len(lead) - 4)
    raw = prefix + TOKEN.encode() + b"UNCAPTURED-SAFE-TAIL"
    budget = [limit]
    clean = sanitize(evidence_for(raw, encoding), TOKEN, budget)
    output_encoding = "base64" if encoding == "base64" or prefix_kind == "controls" else "utf-8"
    attachments = assert_response(
        clean, prefix + b"[RED", limit, limit, True, True, True, output_encoding
    )
    assert budget == [0]
    for payload in [json.dumps(clean).encode(), *(item["content"] for item in attachments)]:
        assert b"uniq" not in payload
        assert b"secret-tail" not in payload
        assert b"UNCAPTURED-SAFE-TAIL" not in payload


@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
def test_earlier_redaction_does_not_backfill_boundary_lookahead(encoding):
    token = TOKEN.encode()
    prefix = b"safe " + token + b" companion "
    limit = len(prefix) + 4
    raw = prefix + token + b"UNRETAINED"
    budget = [limit + 3]
    first = sanitize(evidence_for(raw, encoding), TOKEN, [limit])
    assert_response(
        first, b"safe [REDACTED] companion [REDACTED]", limit, limit, True, False, True, encoding
    )
    shared = sanitize(evidence_for(raw, encoding), TOKEN, budget)
    expected = b"safe [REDACTED] companion [REDACTED]"
    assert_response(shared, expected, limit + 3, limit + 3, True, False, True, encoding)
    assert budget == [0]
    empty = sanitize(evidence_for(b"later", encoding), TOKEN, budget)
    assert_response(empty, b"", 0, 0, True, False, False, encoding)


@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
@pytest.mark.parametrize(
    "case,limit,expected,captured,capture_clipped,transform_clipped,redacted",
    [
        ("safe", 5, b"safe ", 5, True, False, False),
        ("full", 100, b"safe [REDACTED] tail", 51, False, False, True),
        ("empty", 0, b"", 0, False, False, False),
        ("zero", 0, b"", 0, True, False, False),
        ("unknown", 9, b"safe uniq", 9, False, False, False),
        ("safe_suffix", 6, b"safe u", 6, True, False, False),
        ("multibyte_token", 7, b"safe [R", 7, True, True, True),
        ("overlap", 8, b"safe [RE", 8, True, True, True),
        ("overlap_full", 12, b"safe [REDACT", 12, True, True, True),
        ("utf8_split", 3, b"\xf0\x9f\x99", 3, True, False, False),
    ],
)
def test_capture_boundary_preserves_non_crossing_and_edge_contracts(
    encoding, case, limit, expected, captured, capture_clipped, transform_clipped, redacted
):
    raw, token = {
        "safe": (b"safe ordinary companion", TOKEN),
        "full": (b"safe " + TOKEN.encode() + b" tail", TOKEN),
        "empty": (b"", TOKEN),
        "zero": (TOKEN.encode(), TOKEN),
        "unknown": (b"safe uniq", TOKEN),
        "safe_suffix": (b"safe uZ", TOKEN),
        "multibyte_token": ("safe π🙂tail".encode(), "π🙂tail"),
        "overlap": (b"safe abababab tail", "ababab"),
        "overlap_full": (b"safe abababab tail", "ababab"),
        "utf8_split": ("🙂tail".encode(), TOKEN),
    }[case]
    budget = [limit]
    clean = sanitize(evidence_for(raw, encoding), token, budget)
    output_encoding = "base64" if encoding == "base64" or case == "utf8_split" else "utf-8"
    assert_response(
        clean,
        expected,
        captured,
        limit,
        capture_clipped,
        transform_clipped,
        redacted,
        output_encoding,
    )
    assert budget == [limit - max(captured, len(expected))]


@pytest.mark.parametrize("bad_position", ["capture", "lookahead"])
def test_invalid_base64_lookahead_refuses_without_retaining_new_bytes(bad_position):
    raw = b"safe " + TOKEN.encode() + b"tail"
    body = base64.b64encode(raw).decode()
    position = 0 if bad_position == "capture" else 12
    body = body[:position] + "!" + body[position + 1 :]
    evidence = diagnostics(body, encoding="base64")
    evidence["attempts"] = evidence["attempts"][:1]
    budget = [9]
    clean = sanitize(evidence, TOKEN, budget)
    assert "bodyEncoding[1]" in clean["invalidEvidence"]
    captured = 0 if bad_position == "capture" else 9
    assert_response(clean, b"", captured, 9, True, False, bool(captured), "base64")
    assert budget == [9 - captured]


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
async def test_full_capture_boundary_jsonb_ops_and_multipart(
    tmp_path, postgres_template, wire_server, monkeypatch, encoding
):
    origin, state = wire_server
    prefix = b"safe " + b"x" * (2_000_000 - 9)
    wire_token = "unique-wire-boundary-credential-secret-tail"
    raw = prefix + wire_token.encode() + b"UNCAPTURED-SAFE-TAIL"
    broken = failed_run(evidence_for(raw, encoding))

    async def upstream(payload):
        assert payload == {"handles": ["alice"], "cursors": {"alice": "90"}}
        return copy.deepcopy(broken)

    monkeypatch.setattr(importlib.import_module("direct_fetch"), "fetch_snapshot", upstream)
    config = source_settings(
        tmp_path,
        [{"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": origin + "/actor"}]}],
    )
    config.apify_api_token = wire_token
    config.telegram_token = "isolated-telegram-token"
    config.telegram_ops_chat = "ops"
    config.ops_progress_enabled = True
    expected_body = prefix + b"[RED"
    digest = hashlib.sha256(expected_body).hexdigest()
    suffix = "bin" if encoding == "base64" else "txt"
    expected = expected_metadata(("request", "url"), "https://x.com/alice")
    response = expected["attempts"][0]["response"]
    response.update(
        bodyEncoding=encoding,
        bodyBytes=2_000_000,
        bodySha256=digest,
        bodyTruncated=True,
        consumerCapturedBytes=2_000_000,
        consumerCaptureTruncated=True,
        consumerTransformTruncated=True,
        redacted=True,
        bodyDocument=f"x-alice-1-attempt-1-{digest[:12]}.{suffix}",
    )
    expected_metadata_bytes = json.dumps(
        expected, ensure_ascii=False, indent=2, sort_keys=True
    ).encode()
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"alice": "90"}})
        async with httpx.AsyncClient() as http:
            service = SourceService(repo, config, http=http)
            service.accounts = {"alice": {"handle": "alice", "role": "OTHER", "entity_id": "alice"}}
            result = await service.poll_once("S29")
        checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
        assert result["status"] == "DEGRADED" and result["items"] == 0
        assert checkpoint["account_cursors"] == {"alice": "90"}
        clean = checkpoint["entry_errors"][0]["diagnostics"]
        assert_response(clean, expected_body, 2_000_000, 2_000_000, True, True, True, encoding)
        [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
        assert notice["entry_errors"] == checkpoint["entry_errors"]
        [marker] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERROR"})
        assert marker["entry_error"] == checkpoint["entry_errors"][0]
        assert notice["document_manifest"] == [
            {"filename": response["bodyDocument"], "bytes": len(expected_body), "sha256": digest},
            {
                "filename": "x-alice-1-diagnostics.json",
                "bytes": len(expected_metadata_bytes),
                "sha256": hashlib.sha256(expected_metadata_bytes).hexdigest(),
            },
        ]
        [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
        async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
            await DeliveryService(repo, config, http=http).deliver(job["payload"])
        uploads = [
            multipart(httpx.Request("POST", origin, headers=headers, content=body))["document"][1]
            for route, headers, body in state["telegram"]
            if route.endswith("sendDocument")
        ]
        assert uploads == [expected_body, expected_metadata_bytes]
        for payload in [json.dumps(checkpoint).encode(), json.dumps(notice).encode(), *uploads]:
            assert b"uniq" not in payload and b"secret-tail" not in payload
            assert b"UNCAPTURED-SAFE-TAIL" not in payload
        receipts = await repo.list(
            "out.deliveries", filters={"status": "SENT", "method": "sendDocument"}
        )
        assert {row["document_sha256"] for row in receipts} == {
            hashlib.sha256(content).hexdigest() for content in uploads
        }
        assert await repo.get("out.ops_notices", notice["notice_id"]) == notice
