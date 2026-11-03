import copy
import hashlib
import json
from pathlib import Path

import httpx
import pytest
from test_x_diagnostics import multipart, telegram_settings

from tank.delivery import DeliveryError, DeliveryService
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
            body = response.pop("body").encode()
            digest = hashlib.sha256(body).hexdigest()
            filename = f"x-example-{index}-attempt-{attempt['attempt']}-{digest[:12]}.txt"
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


def reordered(notice):
    return json.loads(json.dumps(notice, sort_keys=True))


async def test_legacy_metadata_and_body_match_independent_original_bytes_after_reordering():
    repo = MemoryRepository()
    notice = reordered(legacy_notice())
    await repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
    requests = []

    def send(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(requests)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        await DeliveryService(repo, telegram_settings(), http=http).deliver(
            {"notice_id": notice["notice_id"]}
        )
    assert [multipart(request)["document"] for request in requests[1:]] == [
        (notice["document_manifest"][0]["filename"], BODY),
        ("x-example-1-diagnostics.json", METADATA),
    ]
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


async def assert_refused(notice, secrets=()):
    repo = MemoryRepository()
    notice = reordered(notice)
    await repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
    requests = []
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
    assert await repo.get("out.ops_notices", notice["notice_id"]) == notice


@pytest.mark.parametrize(
    "part",
    [
        "unknown_diagnostic", "unknown_response", "unknown_request", "unknown_attempt",
        "outcome", "attempt_number", "attempt_count", "timestamp", "status", "retryable",
        "response_status", "wire_bytes", "body_bytes", "redacted", "unsafe_header",
        "body_secret", "metadata_secret", "credentials", "oversized_body",
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
        notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]["headers"][
            "retry-after"
        ] = "1"
    else:
        notice["entry_errors"] = [copy.deepcopy(notice["entry_errors"][0]) for _ in range(6)]
    retained_manifest(notice)
    await assert_refused(notice)
