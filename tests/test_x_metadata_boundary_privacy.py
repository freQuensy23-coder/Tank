import copy
import hashlib
import importlib
import json
import os

import httpx
import pytest
from test_sources import settings as source_settings
from test_x_diagnostics import diagnostics, failed_run, multipart
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server

from tank.delivery import DeliveryService
from tank.sources import SourceService
from tank.sources.x_diagnostics import documents, sanitize


METADATA_FIELDS = [
    (("request", "url"), "request.url"),
    (("attempts", 0, "error"), "error[1]"),
    (("attempts", 0, "response", "headers", "x-request-id"), "headers[1].x-request-id"),
    (("attempts", 0, "response", "readError"), "readError[1]"),
    (("attempts", 0, "response", "decodeError"), "decodeError[1]"),
]


def metadata_evidence(path, value):
    evidence = diagnostics("safe upstream body")
    evidence["attempts"] = evidence["attempts"][:1]
    target = evidence
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return evidence


def expected_metadata(path, value, invalid=None):
    raw = b"safe upstream body"
    digest = hashlib.sha256(raw).hexdigest()
    response = {
        "status": 503,
        "headers": {"content-type": "text/plain", "x-request-id": "one"},
        "bodyEncoding": "utf-8",
        "bodyBytes": len(raw),
        "bodySha256": digest,
        "bodyTruncated": False,
        "consumerCapturedBytes": len(raw),
        "consumerCaptureTruncated": False,
        "consumerTransformTruncated": False,
        "consumerBodyLimitBytes": 2_000_000,
        "redacted": False,
        "omittedWireBytes": None,
        "wireBytes": None,
        "captureLimitBytes": None,
        "serializedBodyLimitBytes": None,
        "readError": None,
        "decodeError": None,
        "bodyDocument": f"x-alice-1-attempt-1-{digest[:12]}.txt",
    }
    expected = {
        "request": {"method": "GET", "url": "https://x.com/alice", "body": None},
        "outcome": "exhausted",
        "attemptCount": 1,
        "attempts": [
            {
                "attempt": 1,
                "at": "2026-10-24T12:00:00+00:00",
                "error": "PUBLIC_PROFILE_HTTP_503",
                "status": 503,
                "retryable": True,
                "response": response,
            }
        ],
    }
    target = expected
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    if invalid:
        expected["invalidEvidence"] = invalid
    return expected


def assert_documents(clean, expected):
    raw = b"safe upstream body"
    metadata = json.dumps(expected, ensure_ascii=False, indent=2, sort_keys=True).encode()
    attachments = documents([{"account": "alice", "diagnostics": clean}])
    assert attachments == [
        {
            "filename": expected["attempts"][0]["response"]["bodyDocument"],
            "content": raw,
            "sha256": hashlib.sha256(raw).hexdigest(),
        },
        {
            "filename": "x-alice-1-diagnostics.json",
            "content": metadata,
            "sha256": hashlib.sha256(metadata).hexdigest(),
        },
    ]
    serialized = json.dumps(clean, ensure_ascii=False).encode()
    assert b"\\u0000" not in serialized and b"\x00" not in serialized
    return [serialized, *(item["content"] for item in attachments)]


@pytest.mark.parametrize("path,invalid_path", METADATA_FIELDS)
def test_metadata_rejects_token_straddling_input_bound_without_retaining_fragments(path, invalid_path):
    token = "unique-credential-prefix-unique-credential-suffix"
    evidence = metadata_evidence(path, "x" * 8188 + token)
    evidence["invalidEvidence"] = ["untrusted-inherited-error"]
    clean = sanitize(evidence, token)
    expected = expected_metadata(path, "INVALID_DIAGNOSTIC_TEXT", sorted([invalid_path, "invalidEvidence"]))
    payloads = assert_documents(clean, expected)
    assert all(
        fragment not in payload
        for payload in payloads
        for fragment in (b"uniq", b"unique-credential-suffix", b"untrusted-inherited-error")
    )


@pytest.mark.parametrize("path,invalid_path", METADATA_FIELDS)
@pytest.mark.parametrize(
    "candidate,token,expected",
    [
        ("ordinary safe π text", "apify-secret", "ordinary safe π text"),
        ("x" * 8192, "apify-secret", "x" * 8192),
        ("safe apify-secret tail", "apify-secret", "safe [REDACTED] tail"),
        ("safe\x00text", "safe[NUL]text", "INVALID_DIAGNOSTIC_TEXT"),
        ("safe\ud800text", "safe?text", "INVALID_DIAGNOSTIC_TEXT"),
        ("safe\udffftext", "safe?text", "INVALID_DIAGNOSTIC_TEXT"),
        ("safe REDACTED tail", "REDACTED", "INVALID_DIAGNOSTIC_TEXT"),
        ("x" * 8180 + "?token=a", "unrelated-secret", "INVALID_DIAGNOSTIC_TEXT"),
        ("x" * 8193, "INVALID_DIAGNOSTIC_TEXT", ""),
        ({"raw": "apify-secret"}, "INVALID_DIAGNOSTIC_TEXT", ""),
    ],
)
def test_metadata_preserves_bounded_text_or_refuses_unsafe_transformations(
    path, invalid_path, candidate, token, expected
):
    clean = sanitize(metadata_evidence(path, candidate), token)
    invalid = [invalid_path] if expected in {"INVALID_DIAGNOSTIC_TEXT", ""} else None
    payloads = assert_documents(clean, expected_metadata(path, expected, invalid))
    assert all(token.encode() not in payload for payload in payloads)


@pytest.mark.integration
@pytest.mark.postgres
async def test_metadata_boundary_survives_jsonb_checkpoint_ops_and_http_documents(
    tmp_path, postgres_template, wire_server, monkeypatch
):
    origin, state = wire_server
    token = "unique-credential-prefix-unique-credential-suffix"
    path = ("attempts", 0, "response", "headers", "x-request-id")
    evidence = metadata_evidence(path, "x" * 8188 + token)
    broken = failed_run(evidence)

    async def upstream(payload):
        assert payload == {"handles": ["alice"], "cursors": {"alice": "90"}}
        return copy.deepcopy(broken)

    monkeypatch.setattr(importlib.import_module("direct_fetch"), "fetch_snapshot", upstream)
    config = source_settings(
        tmp_path,
        [{"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": origin + "/actor"}]}],
    )
    config.apify_api_token = token
    config.telegram_token = "isolated-telegram-token"
    config.telegram_ops_chat = "ops"
    config.ops_progress_enabled = True
    expected = expected_metadata(path, "INVALID_DIAGNOSTIC_TEXT", ["headers[1].x-request-id"])
    expected_bytes = json.dumps(expected, ensure_ascii=False, indent=2, sort_keys=True).encode()
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
        assert_documents(clean, expected)
        [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
        assert notice["entry_errors"] == checkpoint["entry_errors"]
        [marker] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERROR"})
        assert marker["entry_error"] == checkpoint["entry_errors"][0]
        assert notice["document_manifest"] == [
            {
                "filename": expected["attempts"][0]["response"]["bodyDocument"],
                "bytes": len(b"safe upstream body"),
                "sha256": hashlib.sha256(b"safe upstream body").hexdigest(),
            },
            {
                "filename": "x-alice-1-diagnostics.json",
                "bytes": len(expected_bytes),
                "sha256": hashlib.sha256(expected_bytes).hexdigest(),
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
        assert uploads == [b"safe upstream body", expected_bytes]
        for payload in [json.dumps(checkpoint).encode(), json.dumps(notice).encode(), *uploads]:
            assert b"uniq" not in payload and b"unique-credential-suffix" not in payload
        assert await repo.get("out.ops_notices", notice["notice_id"]) == notice
