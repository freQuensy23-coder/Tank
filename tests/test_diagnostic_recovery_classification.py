import asyncio
import base64
import copy
import json
import re
from datetime import UTC, datetime, timedelta

import pytest
from test_x_diagnostic_manifests import digest, legacy_notice, manifest, reopen, reverse_objects
from test_x_diagnostic_manifests import diagnostic_repo as diagnostic_repo
from test_x_diagnostics import telegram_settings

from tank.diagnostic_recovery import recover_legacy_diagnostic_job
from tank.sources.x_diagnostics import verified_documents
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock, offset_schedule
from tank.util import sha256


@pytest.fixture
def recovery_clock(monkeypatch):
    now = datetime(2026, 10, 24, 12, tzinfo=UTC)
    monkeypatch.setattr("tank.diagnostic_recovery.utcnow", lambda: now)
    return collection_clock(now)


def historical_documents(entry_errors):
    result = []
    for index, error in enumerate(entry_errors, 1):
        if error.get("diagnostics") is None:
            continue
        account = re.sub(r"[^a-z0-9_]", "_", str(error.get("account") or "run").lower())[:30]
        metadata = copy.deepcopy(error["diagnostics"])
        for attempt in metadata["attempts"]:
            response = attempt.get("response")
            if response is None:
                continue
            body = response.pop("body")
            content = (
                base64.b64decode(body, validate=True)
                if response["bodyEncoding"] == "base64"
                else body.encode("utf-8")
            )
            if not content:
                response["body"] = ""
                response["bodyDocument"] = None
                continue
            suffix = "bin" if response["bodyEncoding"] == "base64" else "txt"
            filename = f"x-{account}-{index}-attempt-{attempt['attempt']}-{digest(content)[:12]}.{suffix}"
            response["bodyDocument"] = filename
            result.append({"filename": filename, "content": content, "sha256": digest(content)})
        content = json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8")
        result.append(
            {
                "filename": f"x-{account}-{index}-diagnostics.json",
                "content": content,
                "sha256": digest(content),
            }
        )
    return result


def parent_receipt(notice_id):
    return {
        "notice_id": notice_id,
        "channel": "telegram:ops",
        "method": "sendMessage",
        "status": "SENT",
        "attempt": 1,
        "message_ref": "77",
        "chat_id": "ops",
        "at": "2026-10-24T12:00:00Z",
        "at_prec": "COLLECTION_ONLY",
        "at_prec_reason": None,
    }


def document_receipt(notice_id, document, *, status="GENERATED", attempt=4, terminal=False):
    return {
        **parent_receipt(notice_id),
        "method": "sendDocument",
        "document_id": sha256({"filename": document["filename"], "sha256": document["sha256"]}),
        "filename": document["filename"],
        "document_sha256": document["sha256"],
        "document_bytes": len(document["content"]),
        "status": status,
        "attempt": attempt,
        "terminal": terminal,
        "message_ref": "88" if status == "SENT" else None,
        "chat_id": "ops" if status == "SENT" else None,
        "request_reply_to_message_ref": "77",
    }


async def seed_failed_delivery(repo, notice, clock):
    notice_id = notice["notice_id"]
    await repo.put("out.ops_notices", notice_id, notice, immutable=True)
    original = await repo.enqueue(
        "DELIVER",
        {"notice_id": notice_id, "channel": "telegram:ops"},
        notice_id,
        priority=2,
        run_after=offset_schedule(clock, -timedelta(hours=1)),
        expires_at=offset_schedule(clock, timedelta(days=1)),
        max_attempts=5,
    )
    original.update(
        status="FAILED",
        attempts=5,
        last_error="DeliveryError",
        **clock.fields("finished_at"),
        **clock.fields("delivery_failure_recorded_at"),
    )
    original = await repo.put("ops.jobs", original["job_id"], original)
    await repo.put("out.deliveries", "original_parent", parent_receipt(notice_id), immutable=True)
    return original


@pytest.mark.parametrize("kind", ["missing", "empty", "text", "binary"])
@pytest.mark.parametrize("explicit_version", [False, True])
async def test_same_delivery_error_with_matching_raw_legacy_bytes_gets_no_recovery(
    recovery_clock, kind, explicit_version
):
    repo = MemoryRepository()
    notice, expected = legacy_notice(kind)
    if explicit_version:
        notice["document_serialization_version"] = 1
    original = await seed_failed_delivery(repo, notice, recovery_clock)
    await repo.put(
        "out.deliveries",
        "unfinished_document",
        document_receipt(notice["notice_id"], expected[-1]),
        immutable=True,
    )
    saved = await repo.get("out.ops_notices", notice["notice_id"])
    assert saved is not None
    assert historical_documents(saved["entry_errors"]) == expected
    assert manifest(historical_documents(saved["entry_errors"])) == saved["document_manifest"]
    assert verified_documents(saved) == expected
    ledger = await repo.list("out.deliveries")

    for _ in range(2):
        results = await asyncio.gather(
            recover_legacy_diagnostic_job(repo, telegram_settings(), original),
            recover_legacy_diagnostic_job(repo, telegram_settings(), original),
        )
        assert results == [None, None]
        assert await repo.list("ops.jobs", filters={"type": "DELIVER"}) == [original]
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved
        assert await repo.list("out.deliveries") == ledger


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("kind", ["missing", "empty", "text", "binary"])
async def test_jsonb_mismatch_recovery_persists_classification_without_refunding_transport_budget(
    diagnostic_repo, recovery_clock, kind
):
    repo = diagnostic_repo
    notice, expected = legacy_notice(kind)
    original = await seed_failed_delivery(repo, notice, recovery_clock)
    notice_id = notice["notice_id"]
    for index, document in enumerate(expected):
        for attempt in range(1, 5) if index == len(expected) - 1 else (1,):
            await repo.put(
                "out.deliveries",
                f"historical_document_{index}_{attempt}",
                document_receipt(
                    notice_id,
                    document,
                    status="GENERATED" if index == len(expected) - 1 else "SENT",
                    attempt=attempt,
                ),
                immutable=True,
            )
    ledger = await repo.list("out.deliveries")

    async with reopen(repo) as first, reopen(repo) as second:
        saved = await first.get("out.ops_notices", notice_id)
        assert saved is not None
        raw_documents = historical_documents(saved["entry_errors"])
        assert manifest(raw_documents) != saved["document_manifest"]
        assert raw_documents[-1]["sha256"] != saved["document_manifest"][-1]["sha256"]
        assert verified_documents(saved) == expected
        results = await asyncio.gather(
            recover_legacy_diagnostic_job(first, telegram_settings(), original),
            recover_legacy_diagnostic_job(second, telegram_settings(), original),
        )
        assert results[0] is not None
        assert results[1] == results[0]
        recovery_id = results[0]["job_id"]

    async with reopen(repo) as reloaded:
        recovery = await reloaded.get("ops.jobs", recovery_id)
        assert recovery is not None
        proof = recovery["payload"]["_diagnostic_recovery"]
        assert proof["classification"] == "legacy_serialization_mismatch"
        assert proof["notice_sha256"] == sha256(saved)
        assert proof["manifest_sha256"] == sha256(saved["document_manifest"])
        assert proof["source_job_id"] == original["job_id"]
        assert proof["parent_message_ref"] == "77"
        assert proof["chat_id"] == "ops"
        assert recovery["status"] == "QUEUED"
        assert recovery["attempts"] == 0
        assert recovery["max_attempts"] == original["max_attempts"]
        assert recovery["priority"] == original["priority"]
        for field in ("expires_at", "expires_at_prec", "expires_at_prec_reason"):
            assert recovery[field] == original[field]
        recovery.update(status="FAILED", attempts=recovery["max_attempts"], last_error="DeliveryError")
        recovery.update(recovery_clock.fields("finished_at"))
        exhausted = await reloaded.put("ops.jobs", recovery_id, recovery)

    for _ in range(2):
        async with reopen(repo) as first, reopen(repo) as second:
            results = await asyncio.gather(
                recover_legacy_diagnostic_job(first, telegram_settings(), original),
                recover_legacy_diagnostic_job(second, telegram_settings(), original),
            )
            assert results == [exhausted, exhausted]
            assert await first.get("ops.jobs", recovery_id) == exhausted
            assert await first.get("ops.jobs", original["job_id"]) == original
            assert len(await first.list("ops.jobs", filters={"type": "DELIVER"})) == 2
            assert await first.get("out.ops_notices", notice_id) == saved
            assert await first.list("out.deliveries") == ledger
            assert await recover_legacy_diagnostic_job(first, telegram_settings(), exhausted) is None


@pytest.mark.parametrize(
    "corruption", ["manifest_hash", "manifest_size", "body", "unknown_metadata", "unknown_version"]
)
async def test_raw_mismatch_without_verified_compatibility_never_creates_recovery(recovery_clock, corruption):
    repo = MemoryRepository()
    notice, _ = legacy_notice("text")
    notice = reverse_objects(notice)
    assert isinstance(notice, dict)
    if corruption == "manifest_hash":
        notice["document_manifest"][-1]["sha256"] = "0" * 64
    elif corruption == "manifest_size":
        notice["document_manifest"][-1]["bytes"] += 1
    elif corruption == "body":
        notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]["body"] += "changed"
    elif corruption == "unknown_metadata":
        notice["entry_errors"][0]["diagnostics"]["unknown"] = "unproven"
    else:
        notice["document_serialization_version"] = 99
    original = await seed_failed_delivery(repo, notice, recovery_clock)
    saved = await repo.get("out.ops_notices", notice["notice_id"])
    assert saved is not None
    assert manifest(historical_documents(saved["entry_errors"])) != saved["document_manifest"]
    with pytest.raises(ValueError):
        verified_documents(saved)
    ledger = await repo.list("out.deliveries")

    for _ in range(2):
        results = await asyncio.gather(
            recover_legacy_diagnostic_job(repo, telegram_settings(), original),
            recover_legacy_diagnostic_job(repo, telegram_settings(), original),
        )
        assert results == [None, None]
        assert await repo.list("ops.jobs", filters={"type": "DELIVER"}) == [original]
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved
        assert await repo.list("out.deliveries") == ledger


@pytest.mark.parametrize("document_state", ["sent", "generated_exhausted", "terminal"])
async def test_proven_legacy_mismatch_does_not_override_closed_document_budget(recovery_clock, document_state):
    repo = MemoryRepository()
    notice, expected = legacy_notice("missing")
    original = await seed_failed_delivery(repo, reverse_objects(notice), recovery_clock)
    await repo.put(
        "out.deliveries",
        "closed_document",
        document_receipt(
            notice["notice_id"],
            expected[-1],
            status="SENT" if document_state == "sent" else "GENERATED",
            attempt=5 if document_state == "generated_exhausted" else 1,
            terminal=document_state == "terminal",
        ),
        immutable=True,
    )
    saved = await repo.get("out.ops_notices", notice["notice_id"])
    assert saved is not None
    assert manifest(historical_documents(saved["entry_errors"])) != saved["document_manifest"]
    assert verified_documents(saved) == expected
    ledger = await repo.list("out.deliveries")

    for _ in range(2):
        results = await asyncio.gather(
            recover_legacy_diagnostic_job(repo, telegram_settings(), original),
            recover_legacy_diagnostic_job(repo, telegram_settings(), original),
        )
        assert results == [None, None]
        assert await repo.list("ops.jobs", filters={"type": "DELIVER"}) == [original]
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved
        assert await repo.list("out.deliveries") == ledger
