import base64
import copy
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from test_x_diagnostics import multipart, telegram_settings

from tank.delivery import DeliveryError, DeliveryService
from tank.settings import Settings
from tank.sources import x_diagnostic_manifest
from tank.sources.x_diagnostics import BODY_LIMIT, documents, sanitize
from tank.storage import MemoryRepository

BODY = b"Public fixture body.\n"
BODY_HASH = "254f62811e6241faff2c9a940b9dcbfe2bedd218db7810c05bbe6c94e8d5c932"
METADATA_HASH = "2a213e067f9359d52fce7365f427a818fe6cbf32ae8f6335cd5fb1a2046b0414"
METADATA = (Path(__file__).parent / "fixtures/x-legacy/metadata.json").read_bytes()


def legacy_notice():
    evidence = json.loads(METADATA)
    response = evidence["attempts"][0]["response"]
    filename = response.pop("bodyDocument")
    response["body"] = BODY.decode()
    return {
        "notice_id": "legacy-public-fixture",
        "kind": "ENTRY_ERRORS",
        "text": "Public diagnostic fixture",
        "created_at": "2026-10-24T12:00:00Z",
        "created_at_prec": "COLLECTION_ONLY",
        "created_by": "source-poller",
        "entry_errors": [{"account": "example", "diagnostics": evidence}],
        "document_manifest": [
            {"filename": filename, "sha256": BODY_HASH, "bytes": 21},
            {
                "filename": "x-example-1-diagnostics.json",
                "sha256": METADATA_HASH,
                "bytes": 1122,
            },
        ],
    }


def retained_manifest(notice):
    manifest = []
    for index, error in enumerate(notice["entry_errors"], 1):
        metadata = copy.deepcopy(error["diagnostics"])
        for attempt in metadata["attempts"]:
            response = attempt.get("response")
            if response is None:
                continue
            encoded = response.pop("body")
            body = base64.b64decode(encoded) if response["bodyEncoding"] == "base64" else encoded.encode()
            if not body:
                response["body"] = ""
                response["bodyDocument"] = None
                continue
            digest = hashlib.sha256(body).hexdigest()
            suffix = "bin" if response["bodyEncoding"] == "base64" else "txt"
            filename = f"x-example-{index}-attempt-{attempt['attempt']}-{digest[:12]}.{suffix}"
            response["bodyDocument"] = filename
            manifest.append({"filename": filename, "sha256": digest, "bytes": len(body)})
        content = json.dumps(metadata, ensure_ascii=False, indent=2).encode()
        manifest.append(
            {
                "filename": f"x-example-{index}-diagnostics.json",
                "sha256": hashlib.sha256(content).hexdigest(),
                "bytes": len(content),
            }
        )
    notice["document_manifest"] = manifest


def legacy_byte_oracle(kind):
    if kind == "utf8":
        notice = legacy_notice()
        return notice, [
            (notice["document_manifest"][0]["filename"], BODY),
            ("x-example-1-diagnostics.json", METADATA),
        ]
    content = (Path(__file__).parent / f"fixtures/x-legacy/{kind}.json").read_bytes()
    length, digest = {
        "binary": (1123, "9c60d8bcf24c45b181026e9d6cce6dc9addb05bd0fda7afd940150d9c5ab6e10"),
        "empty": (1104, "19263fbf8036a03bedad2f12defd08d0dfbe0dae02060963059f4c0b6ea68a66"),
    }[kind]
    assert len(content) == length and hashlib.sha256(content).hexdigest() == digest
    evidence = json.loads(content)
    response = evidence["attempts"][0]["response"]
    filename = response.pop("bodyDocument")
    body = b"\xff\x00public binary fixture\n" if kind == "binary" else b""
    response["body"] = base64.b64encode(body).decode() if kind == "binary" else ""
    notice = legacy_notice()
    notice["entry_errors"][0]["diagnostics"] = evidence
    notice["document_manifest"] = []
    expected = []
    if body:
        notice["document_manifest"].append(
            {
                "filename": filename,
                "sha256": "940a32abb538dc43c8d014d89315466931aa59f9ef94263047cdc4e996785672",
                "bytes": 24,
            }
        )
        expected.append((filename, body))
    notice["document_manifest"].append(
        {"filename": "x-example-1-diagnostics.json", "sha256": digest, "bytes": length}
    )
    expected.append(("x-example-1-diagnostics.json", content))
    return notice, expected


def reordered(notice):
    return json.loads(json.dumps(notice, sort_keys=True))


@pytest.mark.parametrize("kind", ["utf8", "timeout", "binary", "empty"])
async def test_legacy_metadata_and_body_match_independent_original_bytes_after_reordering(kind):
    repo = MemoryRepository()
    notice, expected = legacy_byte_oracle("utf8" if kind == "timeout" else kind)
    if kind == "timeout":
        content = (Path(__file__).parent / "fixtures/x-legacy/timeout.json").read_bytes()
        notice["entry_errors"][0]["diagnostics"] = json.loads(content)
        notice["document_manifest"] = [
            {
                "filename": "x-example-1-diagnostics.json",
                "sha256": "9dbf8606d0f7406a2de387137b2acb131604971198b80dd4c5f7d2f23c855f05",
                "bytes": 297,
            }
        ]
        expected = [("x-example-1-diagnostics.json", content)]
    notice = reordered(notice)
    await repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
    requests = []

    def send(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(requests)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        await DeliveryService(repo, telegram_settings(), http=http).deliver(
            {"notice_id": notice["notice_id"]}
        )
    assert [multipart(request)["document"] for request in requests[1:]] == expected
    assert await repo.get("out.ops_notices", notice["notice_id"]) == notice


@pytest.mark.parametrize("part", ["body", "hash", "length", "filename", "extra_manifest_field"])
async def test_corrupt_legacy_manifest_refuses_before_any_document_upload(part):
    notice = legacy_notice()
    if part == "body":
        notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]["body"] += "corrupt"
    elif part == "hash":
        notice["document_manifest"][-1]["sha256"] = "0" * 64
    elif part == "length":
        notice["document_manifest"][-1]["bytes"] += 1
    elif part == "filename":
        notice["document_manifest"][-1]["filename"] = "unknown.json"
    else:
        notice["document_manifest"][-1]["unknown"] = "refuse"
    await assert_refused(notice)


async def assert_refused(notice, secrets=(), settings=None):
    repo = MemoryRepository()
    notice = reordered(notice)
    await repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
    requests = []
    if settings is None:
        settings = telegram_settings()
        settings.configured_secrets = lambda: secrets

    def send(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(requests)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        with pytest.raises(DeliveryError):
            await DeliveryService(repo, settings, http=http).deliver({"notice_id": notice["notice_id"]})
    assert [request.url.path.rsplit("/", 1)[-1] for request in requests] == ["sendMessage"]
    assert not await repo.list("out.deliveries", filters={"method": "sendDocument"})
    parents = await repo.list("out.deliveries", filters={"status": "SENT"})
    assert len(parents) == 1 and parents[0]["message_ref"] == "1"
    assert not await repo.list("out.deliveries", filters={"method": "sendDocument", "status": "GENERATED"})
    assert await repo.get("out.ops_notices", notice["notice_id"]) == notice


@pytest.mark.parametrize(
    "part",
    [
        "unknown_diagnostic",
        "unknown_response",
        "unknown_request",
        "unknown_attempt",
        "outcome",
        "attempt_number",
        "attempt_count",
        "timestamp",
        "status",
        "retryable",
        "response_status",
        "wire_bytes",
        "body_bytes",
        "redacted",
        "unsafe_header",
        "body_secret",
        "metadata_secret",
        "credentials",
        "oversized_body",
    ],
)
async def test_hash_matching_legacy_evidence_still_requires_typed_safe_schema(part):
    notice = legacy_notice()
    evidence = notice["entry_errors"][0]["diagnostics"]
    attempt = evidence["attempts"][0]
    response = attempt["response"]
    changes = {
        "unknown_diagnostic": (evidence, "unknown", "untrusted"),
        "unknown_response": (response, "unknown", "untrusted"),
        "unknown_request": (evidence["request"], "unknown", "untrusted"),
        "unknown_attempt": (attempt, "unknown", "untrusted"),
        "outcome": (evidence, "outcome", "untrusted"),
        "attempt_number": (attempt, "attempt", True),
        "attempt_count": (evidence, "attemptCount", True),
        "timestamp": (attempt, "at", "not-a-date"),
        "status": (attempt, "status", 700),
        "retryable": (attempt, "retryable", 1),
        "response_status": (response, "status", 99),
        "wire_bytes": (response, "wireBytes", -1),
        "body_bytes": (response, "bodyBytes", True),
        "redacted": (response, "redacted", 0),
        "unsafe_header": (response["headers"], "authorization", "untrusted"),
        "body_secret": (response, "body", "fixture-private-value"),
        "metadata_secret": (response["headers"], "server", "fixture-private-value"),
        "credentials": (response, "body", "1.https://reader:private-password@host/path"),
        "oversized_body": (response, "body", "x" * 2_000_001),
    }
    target, key, value = changes[part]
    target[key] = value
    if part in {"body_secret", "credentials", "oversized_body"}:
        response["bodyBytes"] = len(value.encode())
        response["bodySha256"] = hashlib.sha256(value.encode()).hexdigest()
    retained_manifest(notice)
    await assert_refused(notice, ("fixture-private-value",))


@pytest.mark.parametrize("scope", ["document", "notice"])
async def test_legacy_header_candidate_budget_refuses_before_any_upload(scope):
    notice = legacy_notice()
    if scope == "document":
        notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]["headers"]["retry-after"] = "1"
    else:
        notice["entry_errors"] = [copy.deepcopy(notice["entry_errors"][0]) for _ in range(6)]
    retained_manifest(notice)
    await assert_refused(notice)


@pytest.mark.parametrize("version", [0, True, 3, "1"])
async def test_unknown_legacy_serialization_version_refuses_before_upload(version):
    notice = legacy_notice()
    notice["document_serialization_version"] = version
    await assert_refused(notice)


def oversized_legacy_notice(case):
    notice = legacy_notice()
    evidence = notice["entry_errors"][0]["diagnostics"]
    attempt = evidence["attempts"][0]
    response = attempt["response"]
    if case == "attempts":
        evidence["attempts"] = [attempt] * 10_000
        evidence["attemptCount"] = 10_000
    elif case in {"utf8", "base64", "multibyte", "aggregate"}:
        response["body"] = {
            "utf8": "x" * (BODY_LIMIT + 1),
            "base64": "eHh4" * (BODY_LIMIT // 3 + 1),
            "multibyte": "\U0001f600" * (BODY_LIMIT // 4 + 1),
            "aggregate": "x" * BODY_LIMIT,
        }[case]
        if case == "base64":
            response["bodyEncoding"] = "base64"
        raw = base64.b64decode(response["body"]) if case == "base64" else response["body"].encode()
        response["bodyBytes"] = len(raw)
        response["wireBytes"] = len(raw)
        response["bodySha256"] = hashlib.sha256(raw).hexdigest()
        if case == "aggregate":
            notice["entry_errors"] = [copy.deepcopy(notice["entry_errors"][0]) for _ in range(7)]
    elif case == "unknown_nested":
        evidence["unknown"] = {"nested": ["x" * 100_000] * 10}
    elif case == "known_container":
        attempt["error"] = {"nested": ["x" * 100_000] * 10}
    elif case == "metadata_text":
        attempt["error"] = "x" * 100_000
    elif case == "entries":
        notice["entry_errors"] = [notice["entry_errors"][0]] * 4097
    return notice


@pytest.mark.parametrize(
    "case",
    [
        "attempts",
        "utf8",
        "base64",
        "multibyte",
        "aggregate",
        "unknown_nested",
        "known_container",
        "metadata_text",
        "entries",
    ],
)
async def test_legacy_bounds_refuse_before_materialization_and_preserve_delivery_state(case):
    notice = oversized_legacy_notice(case)
    with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
        await assert_refused(notice)
    materialize.assert_not_called()


async def test_hash_matching_unknown_legacy_container_cannot_use_canonical_fast_path():
    notice = oversized_legacy_notice("unknown_nested")
    notice["document_manifest"] = [
        {"filename": row["filename"], "sha256": row["sha256"], "bytes": len(row["content"])}
        for row in documents(notice["entry_errors"])
    ]
    with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
        await assert_refused(notice)
    materialize.assert_not_called()


@pytest.mark.parametrize("credential", ["tank_db_password", "oma_db_password", "clickhouse_url"])
@pytest.mark.parametrize("location", ["body", "header"])
async def test_real_settings_legacy_delivery_refuses_independent_configured_secrets(
    monkeypatch, credential, location
):
    monkeypatch.setattr(os, "environ", {})
    sentinel = "legacy-independent-private-value"
    value = (
        f"http://reader:{sentinel}@clickhouse.invalid:8123" if credential == "clickhouse_url" else sentinel
    )
    settings = Settings(
        _env_file=None,
        database_url="postgresql://tank:unrelated-tank-url-password@postgres.invalid/tank",
        openma_database_url="postgresql://oma:unrelated-oma-url-password@postgres.invalid/oma",
        telegram_token="telegram-fixture-token",
        telegram_ops_chat="ops",
        **{credential: value},
    )
    assert settings.database_url.endswith("@postgres.invalid/tank")
    assert settings.openma_database_url.endswith("@postgres.invalid/oma")
    notice = legacy_notice()
    response = notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]
    if location == "body":
        response["body"] = sentinel
        response["bodyBytes"] = len(sentinel)
        response["wireBytes"] = len(sentinel)
        response["bodySha256"] = hashlib.sha256(sentinel.encode()).hexdigest()
    else:
        response["headers"]["server"] = sentinel
    retained_manifest(notice)
    await assert_refused(notice, settings=settings)


@pytest.mark.parametrize("version", [1, 2])
def test_valid_canonical_documents_keep_fast_path_with_large_header_permutation_space(version):
    notice = legacy_notice()
    evidence = notice["entry_errors"][0]["diagnostics"]
    evidence["attempts"][0]["response"]["headers"]["retry-after"] = "1"
    notice["entry_errors"][0]["diagnostics"] = sanitize(evidence)
    expected = documents(notice["entry_errors"])
    notice["document_serialization_version"] = version
    notice["document_manifest"] = [
        {"filename": row["filename"], "sha256": row["sha256"], "bytes": len(row["content"])}
        for row in expected
    ]
    assert x_diagnostic_manifest.verified_documents(reordered(notice)) == expected


def test_legacy_body_and_notice_budget_accept_exact_existing_limits():
    notice = oversized_legacy_notice("aggregate")
    notice["entry_errors"] = notice["entry_errors"][:6]
    for error in notice["entry_errors"]:
        error["diagnostics"]["attempts"][0]["response"]["headers"] = {}
    retained_manifest(notice)
    attachments = x_diagnostic_manifest.verified_documents(reordered(notice))
    assert sum(len(row["content"]) for row in attachments if row["filename"].endswith(".txt")) == 12_000_000
    assert [
        {"filename": row["filename"], "sha256": row["sha256"], "bytes": len(row["content"])}
        for row in attachments
    ] == notice["document_manifest"]
