import base64
import copy
import hashlib
import json
import os
from contextlib import asynccontextmanager

import httpx
import pytest
from test_x_diagnostics import diagnostics, multipart, notice_for, telegram_settings

from tank.delivery import DeliveryError, DeliveryService
from tank.sources.x_diagnostics import documents, sanitize
from tank.storage import MemoryRepository, PostgresRepository


def digest(content):
    return hashlib.sha256(content).hexdigest()


def manifest(attachments):
    return [
        {"filename": row["filename"], "sha256": digest(row["content"]), "bytes": len(row["content"])}
        for row in attachments
    ]


def reverse_objects(value):
    if isinstance(value, dict):
        return {key: reverse_objects(value[key]) for key in reversed(value)}
    if isinstance(value, list):
        return [reverse_objects(item) for item in value]
    return value


def evidence(kind):
    raw = b"\x00\xff\x80diagnostic" if kind == "binary" else "upstream отказ".encode()
    value = diagnostics(
        base64.b64encode(raw).decode() if kind == "binary" else raw.decode(),
        encoding="base64" if kind == "binary" else "utf-8",
    )
    value["attempts"] = value["attempts"][:1]
    if kind == "missing":
        value["attempts"][0].pop("response")
        value["attempts"][0].pop("status")
        value["attempts"][0]["retryable"] = False
    if kind == "empty":
        value["attempts"][0]["response"]["body"] = ""
    return value


def legacy_notice(kind):
    attempt = {
        "attempt": 1,
        "at": "2026-10-24T12:00:00Z",
        "status": 503,
        "retryable": True,
        "error": "PUBLIC_PROFILE_HTTP_503",
    }
    if kind == "missing":
        attempt.pop("status")
        attempt["retryable"] = False
    value = {
        "request": {"method": "GET", "url": "https://x.com/alice", "body": None},
        "outcome": "exhausted",
        "attempts": [attempt],
        "attemptCount": 1,
    }
    raw = b"\x00\xff\x80diagnostic" if kind == "binary" else "upstream отказ".encode()
    if kind == "empty":
        raw = b""
    if kind != "missing":
        attempt["response"] = {
            "status": 503,
            "headers": {"content-type": "text/plain", "x-request-id": "one"},
            "body": base64.b64encode(raw).decode() if kind == "binary" else raw.decode(),
            "bodyEncoding": "base64" if kind == "binary" else "utf-8",
            "bodyBytes": len(raw),
            "bodySha256": digest(raw),
            "bodyTruncated": False,
            "omittedWireBytes": None,
            "wireBytes": None,
            "captureLimitBytes": None,
            "serializedBodyLimitBytes": None,
            "readError": None,
            "decodeError": None,
            "redacted": False,
        }
    metadata = copy.deepcopy(value)
    attachments = []
    if kind != "missing":
        response = metadata["attempts"][0]["response"]
        response.pop("body")
        if raw:
            suffix = "bin" if kind == "binary" else "txt"
            filename = f"x-alice-1-attempt-1-{digest(raw)[:12]}.{suffix}"
            attachments.append({"filename": filename, "content": raw, "sha256": digest(raw)})
            response["bodyDocument"] = filename
        else:
            response["body"] = ""
            response["bodyDocument"] = None
    content = json.dumps(metadata, ensure_ascii=False, indent=2).encode()
    attachments.append(
        {"filename": "x-alice-1-diagnostics.json", "content": content, "sha256": digest(content)}
    )
    notice = {
        "notice_id": "legacy_diagnostics_" + kind,
        "kind": "ENTRY_ERRORS",
        "subject_id": "S29",
        "text": "Synthetic historical X diagnostic",
        "created_at": "2026-10-24T12:00:00Z",
        "created_at_prec": "COLLECTION_ONLY",
        "created_at_prec_reason": None,
        "created_by": "source-poller",
        "entry_errors": [{"account": "alice", "reason": "PUBLIC_PROFILE_HTTP_503", "diagnostics": value}],
        "document_manifest": manifest(attachments),
    }
    return notice, attachments


class TelegramRecorder:
    def __init__(self):
        self.requests = []

    def send(self, request):
        self.requests.append(request)
        return httpx.Response(
            200, json={"ok": True, "result": {"message_id": 100 + len(self.requests), "date": 1}}
        )

    @property
    def uploads(self):
        return [
            multipart(request)
            for request in self.requests
            if request.url.path.endswith("/sendDocument")
        ]


@pytest.fixture
async def diagnostic_repo(postgres_template):
    url = os.environ.get("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.fail("Disposable TANK_TEST_DATABASE_URL is required for diagnostic manifest integration")
    async with postgres_template.repository(url) as repo:
        yield repo


@asynccontextmanager
async def reopen(repo):
    reloaded = PostgresRepository(repo.database_url)
    await reloaded.open()
    try:
        yield reloaded
    finally:
        await reloaded.close()


async def assert_receipts(repo, notice_id, expected):
    receipts = await repo.list(
        "out.deliveries",
        filters={"notice_id": notice_id, "method": "sendDocument", "status": "SENT"},
    )
    assert len(receipts) == len(expected)
    assert {row["filename"] for row in receipts} == set(expected)
    for row in receipts:
        content = expected[row["filename"]]
        assert row["document_sha256"] == digest(content)
        assert row["document_bytes"] == len(content)
        assert row["message_ref"]
        assert row["request_reply_to_message_ref"]
        assert row["attempt"] == 1


@pytest.mark.parametrize("kind", ["missing", "empty", "text", "binary"])
def test_diagnostic_document_bytes_are_canonical_under_nested_object_reordering(kind):
    errors = [{"account": "alice", "diagnostics": sanitize(evidence(kind))}]
    original = copy.deepcopy(errors)
    first = documents(errors)
    reordered = documents(reverse_objects(errors))
    assert first == reordered
    assert errors == original
    for row in first:
        assert row["sha256"] == digest(row["content"])
        if row["filename"].endswith(".json"):
            assert row["content"] == json.dumps(
                json.loads(row["content"]), ensure_ascii=False, indent=2, sort_keys=True
            ).encode()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("kind", ["missing", "empty", "text", "binary"])
async def test_producer_manifest_survives_jsonb_reload_and_multipart_delivery(diagnostic_repo, kind):
    repo = diagnostic_repo
    notice = await notice_for(repo, evidence(kind))
    notice_id = notice["notice_id"]
    assert manifest(documents(notice["entry_errors"])) == notice["document_manifest"]
    [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
    assert job["payload"] == {"notice_id": notice_id, "channel": "telegram:ops"}
    recorder = TelegramRecorder()
    async with httpx.AsyncClient(transport=httpx.MockTransport(recorder.send)) as http:
        for _ in range(2):
            async with reopen(repo) as reloaded:
                await DeliveryService(reloaded, telegram_settings(), http=http).deliver(job["payload"])
    assert len(recorder.requests) == len(notice["document_manifest"]) + 1
    parent = json.loads(recorder.requests[0].content)
    assert parent["chat_id"] == "ops" and "Ошибка HTTP-сбора X" in parent["text"]
    expected = {row["filename"]: row["content"] for row in documents(notice["entry_errors"])}
    assert len(recorder.uploads) == len(expected)
    raw = b"\x00\xff\x80diagnostic" if kind == "binary" else "upstream отказ".encode()
    body_uploads = [upload for upload in recorder.uploads if not upload["document"][0].endswith(".json")]
    assert [upload["document"][1] for upload in body_uploads] == ([raw] if kind in {"text", "binary"} else [])
    [metadata_upload] = [upload for upload in recorder.uploads if upload["document"][0].endswith(".json")]
    metadata = json.loads(metadata_upload["document"][1])
    assert metadata["request"] == {"method": "GET", "url": "https://x.com/alice", "body": None}
    if kind in {"text", "binary"}:
        assert metadata["attempts"][0]["response"]["bodySha256"] == digest(raw)
        assert metadata["attempts"][0]["response"]["bodyDocument"] == body_uploads[0]["document"][0]
    elif kind == "empty":
        assert metadata["attempts"][0]["response"]["body"] == ""
        assert metadata["attempts"][0]["response"]["bodyDocument"] is None
    else:
        assert "response" not in metadata["attempts"][0]
    for upload in recorder.uploads:
        filename, content = upload["document"]
        assert content == expected[filename]
        assert upload["chat_id"][1] == b"ops"
        assert json.loads(upload["reply_parameters"][1]) == {"message_id": 101}
        assert f"sha256={digest(content)}".encode() in upload["caption"][1]
    async with reopen(repo) as reloaded:
        await assert_receipts(reloaded, notice_id, expected)
        assert await reloaded.get("out.ops_notices", notice_id) == notice
        assert await notice_for(reloaded, evidence(kind)) is None
        assert len(await reloaded.list("ops.jobs", filters={"type": "DELIVER"})) == 1


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("kind", ["missing", "empty", "text", "binary"])
@pytest.mark.parametrize("metadata_already_sent", [False, True])
async def test_existing_manifest_is_verified_before_legacy_delivery_or_upgrade(
    diagnostic_repo, kind, metadata_already_sent
):
    repo = diagnostic_repo
    notice, attachments = legacy_notice(kind)
    notice_id = notice["notice_id"]
    await repo.put("out.ops_notices", notice_id, notice, immutable=True)
    expected = {row["filename"]: row["content"] for row in attachments}
    if metadata_already_sent:
        await repo.put(
            "out.deliveries",
            "legacy_parent_receipt",
            {
                "notice_id": notice_id,
                "channel": "telegram:ops",
                "status": "SENT",
                "attempt": 1,
                "message_ref": "77",
                "chat_id": "ops",
                "at": "2026-10-24T12:00:00Z",
                "at_prec": "COLLECTION_ONLY",
                "at_prec_reason": None,
            },
            immutable=True,
        )
        metadata = attachments[-1]
        document_id = hashlib.sha256(
            json.dumps(
                {"filename": metadata["filename"], "sha256": metadata["sha256"]},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode()
        ).hexdigest()
        await repo.put(
            "out.deliveries",
            "legacy_document_receipt",
            {
                "notice_id": notice_id,
                "channel": "telegram:ops",
                "method": "sendDocument",
                "status": "SENT",
                "attempt": 1,
                "document_id": document_id,
                "filename": metadata["filename"],
                "document_sha256": metadata["sha256"],
                "document_bytes": len(metadata["content"]),
                "request_reply_to_message_ref": "77",
                "message_ref": "78",
                "chat_id": "ops",
                "at": "2026-10-24T12:00:00Z",
                "at_prec": "COLLECTION_ONLY",
                "at_prec_reason": None,
            },
            immutable=True,
        )
    recorder = TelegramRecorder()
    async with httpx.AsyncClient(transport=httpx.MockTransport(recorder.send)) as http:
        for _ in range(2):
            async with reopen(repo) as reloaded:
                await DeliveryService(reloaded, telegram_settings(), http=http).deliver(
                    {"notice_id": notice_id, "channel": "telegram:ops"}
                )
    filenames = [upload["document"][0] for upload in recorder.uploads]
    assert filenames == [
        row["filename"]
        for row in attachments
        if not metadata_already_sent or not row["filename"].endswith(".json")
    ]
    assert len(recorder.requests) == len(filenames) + int(not metadata_already_sent)
    for upload in recorder.uploads:
        filename, content = upload["document"]
        if filename.endswith(".json"):
            canonical = json.dumps(
                json.loads(expected[filename]), ensure_ascii=False, indent=2, sort_keys=True
            ).encode()
            assert content in (expected[filename], canonical)
        else:
            assert content == expected[filename]
        expected[filename] = content
        assert upload["chat_id"][1] == b"ops"
        assert json.loads(upload["reply_parameters"][1]) == {
            "message_id": 77 if metadata_already_sent else 101
        }
    async with reopen(repo) as reloaded:
        await assert_receipts(reloaded, notice_id, expected)
        assert await reloaded.get("out.ops_notices", notice_id) == notice


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize(
    "tamper",
    [
        "digest",
        "bytes",
        "filename",
        "metadata",
        "request_method",
        "unknown_metadata",
        "body",
        "missing_document",
        "extra_document",
    ],
)
async def test_jsonb_manifest_compatibility_rejects_tampered_evidence(diagnostic_repo, legacy, tamper):
    if legacy:
        notice, _ = legacy_notice("text")
    else:
        notice = await notice_for(MemoryRepository(), evidence("text"))
    if tamper == "digest":
        notice["document_manifest"][-1]["sha256"] = "0" * 64
    elif tamper == "bytes":
        notice["document_manifest"][-1]["bytes"] += 1
    elif tamper == "filename":
        notice["document_manifest"][-1]["filename"] = "renamed-diagnostics.json"
    elif tamper == "metadata":
        notice["entry_errors"][0]["diagnostics"]["outcome"] = "altered"
    elif tamper == "request_method":
        notice["entry_errors"][0]["diagnostics"]["request"]["method"] = "POST"
    elif tamper == "unknown_metadata":
        notice["entry_errors"][0]["diagnostics"]["unproven"] = "must not be silently discarded"
    elif tamper == "body":
        response = notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]
        response["body"] = response["body"].replace("upstream", "tampered")
    elif tamper == "missing_document":
        notice["document_manifest"].pop(0)
    else:
        notice["document_manifest"].append(
            {"filename": "unproven.txt", "sha256": digest(b"extra"), "bytes": 5}
        )
    repo = diagnostic_repo
    notice_id = notice["notice_id"]
    await repo.put("out.ops_notices", notice_id, notice, immutable=True)
    recorder = TelegramRecorder()
    async with (
        reopen(repo) as reloaded,
        httpx.AsyncClient(transport=httpx.MockTransport(recorder.send)) as http,
    ):
        with pytest.raises(DeliveryError, match="manifest"):
            await DeliveryService(reloaded, telegram_settings(), http=http).deliver(
                {"notice_id": notice_id, "channel": "telegram:ops"}
            )
        assert recorder.uploads == []
        assert not await reloaded.list(
            "out.deliveries", filters={"notice_id": notice_id, "method": "sendDocument"}
        )
        assert await reloaded.get("out.ops_notices", notice_id) == notice
