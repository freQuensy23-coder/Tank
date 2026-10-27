import asyncio
import copy
import json
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from test_x_diagnostic_manifests import (
    assert_receipts,
    digest,
    evidence,
    legacy_notice,
    manifest,
    reopen,
)
from test_x_diagnostic_manifests import diagnostic_repo as diagnostic_repo
from test_x_diagnostics import multipart, notice_for, telegram_settings
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server

from tank.delivery import DeliveryDeferred, DeliveryError, DeliveryService
from tank.runtime import Runtime
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock, offset_schedule
from tank.util import sha256

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture
async def diagnostic_clock(diagnostic_repo, monkeypatch):
    async with diagnostic_repo.transaction():
        row = await (
            await diagnostic_repo._conn().execute("SELECT clock_timestamp() AS clock")
        ).fetchone()
    clock = row["clock"]
    for module in ("tank.runtime", "tank.delivery", "tank.storage", "tank.util"):
        monkeypatch.setattr(module + ".utcnow", lambda: clock)
    return collection_clock(clock)


def runtime_for(repo, http):
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.settings = telegram_settings()
    runtime.delivery = DeliveryService(repo, runtime.settings, http=http)
    runtime.handlers = {"DELIVER": runtime.delivery.deliver}
    runtime.active_jobs = set()
    return runtime


async def drain_worker(runtime):
    stop = asyncio.Event()

    async def stop_when_idle(flag, seconds):
        flag.set()

    runtime.pause = stop_when_idle
    await asyncio.wait_for(runtime.worker(stop), timeout=5)
    assert not runtime.active_jobs


async def reconcile_concurrently(repo, http):
    async with reopen(repo) as first, reopen(repo) as second:
        await asyncio.gather(
            runtime_for(first, http).reconcile_delivery_failures(),
            runtime_for(second, http).reconcile_delivery_failures(),
        )


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
        "historical_evidence": {"transport": "accepted", "unrecognized": ["retain", 17]},
    }


async def seed_failed_job(
    repo,
    notice,
    clock,
    *,
    expired=False,
    status="FAILED",
    channel="telegram:ops",
    failure_recorded=True,
):
    notice_id = notice["notice_id"]
    await repo.put("out.ops_notices", notice_id, notice, immutable=True)
    original = await repo.enqueue(
        "DELIVER",
        {"notice_id": notice_id, "channel": channel},
        notice_id,
        priority=2,
        run_after=offset_schedule(clock, -timedelta(hours=1)),
        expires_at=offset_schedule(clock, -timedelta(seconds=1) if expired else timedelta(days=1)),
        max_attempts=5,
    )
    original = {
        **original,
        "status": status,
        "attempts": 5,
        "last_error": "DeliveryError",
        **offset_schedule(clock, -timedelta(minutes=1)).fields("finished_at"),
        "historical_evidence": {"failure": "serialization guard", "unknown": {"keep": [4, 9]}},
    }
    if failure_recorded:
        original.update(clock.fields("delivery_failure_recorded_at"))
    await repo.put("ops.jobs", original["job_id"], original)
    return original


async def assert_original_preserved(repo, original, notice, previous_receipts):
    current = await repo.get("ops.jobs", original["job_id"])
    for key, value in original.items():
        assert current[key] == value
    assert await repo.get("out.ops_notices", notice["notice_id"]) == notice
    for key, receipt in previous_receipts.items():
        assert await repo.get("out.deliveries", key) == receipt
    assert await repo.list("out.ops_notices") == [notice]


async def recovery_job(repo, original):
    jobs = await repo.list("ops.jobs", filters={"type": "DELIVER"})
    assert len(jobs) == 2
    [recovery] = [job for job in jobs if job["job_id"] != original["job_id"]]
    assert recovery["idem_key"] != original["idem_key"]
    assert recovery["payload"]["notice_id"] == original["payload"]["notice_id"]
    assert recovery["payload"]["channel"] == "telegram:ops"
    for field in ("expires_at", "expires_at_prec", "expires_at_prec_reason"):
        assert recovery[field] == original[field]
    assert original["job_id"] in json.dumps(recovery)
    return recovery


def uploads_from(origin, state):
    return [
        multipart(httpx.Request("POST", origin + path, headers=headers, content=payload))
        for path, headers, payload in state["telegram"]
        if path.endswith("/sendDocument")
    ]


def assert_uploads(uploads, attachments, parent):
    assert [upload["document"] for upload in uploads] == [
        (row["filename"], row["content"]) for row in attachments
    ]
    for upload in uploads:
        filename, content = upload["document"]
        assert upload["chat_id"][1] == b"ops"
        assert json.loads(upload["reply_parameters"][1]) == {"message_id": parent}
        assert filename.encode() in upload["caption"][1]
        assert f"sha256={digest(content)}".encode() in upload["caption"][1]
        assert f"{len(content)} bytes".encode() in upload["caption"][1]


@pytest.mark.parametrize("kind", ["missing", "empty", "text", "binary"])
async def test_canonical_producer_jsonb_reload_real_http_delivery_is_repeatable(
    diagnostic_repo, diagnostic_clock, wire_server, kind
):
    repo = diagnostic_repo
    origin, state = wire_server
    notice = await notice_for(repo, evidence(kind))
    notice_id = notice["notice_id"]
    _, expected = legacy_notice(kind)
    expected = copy.deepcopy(expected)
    for document in expected:
        if document["filename"].endswith(".json"):
            document["content"] = json.dumps(
                json.loads(document["content"]), ensure_ascii=False, indent=2, sort_keys=True
            ).encode()
            document["sha256"] = digest(document["content"])
    assert notice["document_manifest"] == manifest(expected)
    [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
    async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
        for _ in range(3):
            async with reopen(repo) as reloaded:
                assert await reloaded.get("out.ops_notices", notice_id) == notice
                await DeliveryService(reloaded, telegram_settings(), http=http).deliver(job["payload"])
    [parent] = [row for row in state["telegram"] if row[0].endswith("/sendMessage")]
    parent_payload = json.loads(parent[2])
    assert parent_payload["chat_id"] == "ops"
    assert "Ошибка HTTP-сбора X" in parent_payload["text"]
    assert len(state["telegram"]) == len(expected) + 1
    assert_uploads(uploads_from(origin, state), expected, 1)
    async with reopen(repo) as reloaded:
        await assert_receipts(
            reloaded, notice_id, {row["filename"]: row["content"] for row in expected}
        )
        for receipt in await reloaded.list(
            "out.deliveries", filters={"notice_id": notice_id, "method": "sendDocument", "status": "SENT"}
        ):
            assert receipt["request_reply_to_message_ref"] == "1"
        assert await reloaded.get("out.ops_notices", notice_id) == notice
        assert await notice_for(reloaded, evidence(kind)) is None
        assert len(await reloaded.list("ops.jobs", filters={"type": "DELIVER"})) == 1


@pytest.mark.parametrize("failure_recorded", [False, True])
async def test_failed_legacy_job_reconciles_once_then_worker_attaches_exact_original_bytes(
    diagnostic_repo, diagnostic_clock, wire_server, failure_recorded
):
    repo = diagnostic_repo
    origin, state = wire_server
    notice, expected = legacy_notice("missing")
    original = await seed_failed_job(
        repo, notice, diagnostic_clock, failure_recorded=failure_recorded
    )
    previous = {"original_parent": parent_receipt(notice["notice_id"])}
    await repo.put("out.deliveries", "original_parent", previous["original_parent"], immutable=True)
    async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
        await reconcile_concurrently(repo, http)
        async with reopen(repo) as reloaded:
            recovery = await recovery_job(reloaded, original)
            assert recovery["status"] == "QUEUED"
            assert recovery["attempts"] == 0
            assert state["telegram"] == []
            await drain_worker(runtime_for(reloaded, http))
            completed = await recovery_job(reloaded, original)
            assert completed["status"] == "DONE"
            assert completed["attempts"] == 1
        assert_uploads(uploads_from(origin, state), expected, 77)
        assert len(state["telegram"]) == len(expected)
        for _ in range(2):
            async with reopen(repo) as reloaded:
                await reloaded.recover_jobs()
                runtime = runtime_for(reloaded, http)
                await runtime.reconcile_delivery_failures()
                await drain_worker(runtime)
                assert await recovery_job(reloaded, original) == completed
                await assert_original_preserved(reloaded, original, notice, previous)
                await assert_receipts(
                    reloaded,
                    notice["notice_id"],
                    {row["filename"]: row["content"] for row in expected},
                )
    assert len(state["telegram"]) == len(expected)
    assert all(path.endswith("/sendDocument") for path, _, _ in state["telegram"])


@pytest.fixture
def document_failure_server():
    state = {"telegram": [], "remaining_failures": 2}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_POST(self):
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            state["telegram"].append((self.path, dict(self.headers), payload))
            failed = False
            if self.path.endswith("/sendDocument"):
                upload = multipart(
                    httpx.Request(
                        "POST", "http://127.0.0.1" + self.path, headers=dict(self.headers), content=payload
                    )
                )
                filename = upload["document"][0]
                assert isinstance(filename, str)
                failed = filename.endswith(".json") and state["remaining_failures"] > 0
            if failed:
                state["remaining_failures"] -= 1
            result = json.dumps(
                {"ok": False, "description": "Synthetic upstream failure"}
                if failed
                else {"ok": True, "result": {"message_id": 100 + len(state["telegram"]), "date": 1}}
            ).encode()
            self.send_response(500 if failed else 200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(result)))
            self.end_headers()
            self.wfile.write(result)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


async def historical_network_attempts(notice, http, count):
    legacy_repo = MemoryRepository()
    await legacy_repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
    await legacy_repo.put(
        "out.deliveries", "original_parent", parent_receipt(notice["notice_id"]), immutable=True
    )
    delivery = DeliveryService(legacy_repo, telegram_settings(), http=http)
    for attempt in range(1, count + 1):
        expected_error = DeliveryError if attempt == 5 else DeliveryDeferred
        with pytest.raises(expected_error):
            await delivery.deliver({"notice_id": notice["notice_id"], "channel": "telegram:ops"})
    return {
        f"historical_receipt_{index}": receipt
        for index, receipt in enumerate(await legacy_repo.list("out.deliveries"))
    }


async def persist_receipts(repo, receipts):
    for key, receipt in receipts.items():
        await repo.put("out.deliveries", key, receipt, immutable=True)


async def test_recovery_preserves_successful_document_and_real_failed_network_attempts(
    diagnostic_repo, diagnostic_clock, document_failure_server
):
    repo = diagnostic_repo
    origin, state = document_failure_server
    notice, attachments = legacy_notice("text")
    async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
        previous = await historical_network_attempts(notice, http, 2)
        assert state["remaining_failures"] == 0
        assert len(state["telegram"]) == 3
        await persist_receipts(repo, previous)
        original = await seed_failed_job(repo, notice, diagnostic_clock)
        await reconcile_concurrently(repo, http)
        async with reopen(repo) as reloaded:
            await recovery_job(reloaded, original)
            await drain_worker(runtime_for(reloaded, http))
            completed = await recovery_job(reloaded, original)
            assert completed["status"] == "DONE"
            await assert_original_preserved(reloaded, original, notice, previous)
            receipts = await reloaded.list(
                "out.deliveries", filters={"notice_id": notice["notice_id"], "method": "sendDocument"}
            )
            metadata_id = sha256(
                {"filename": attachments[-1]["filename"], "sha256": attachments[-1]["sha256"]}
            )
            metadata = [row for row in receipts if row["document_id"] == metadata_id]
            assert sorted(row["attempt"] for row in metadata if row["status"] == "GENERATED") == [1, 2, 3]
            assert sorted(row["attempt"] for row in metadata if row["status"] == "FAILED") == [1, 2]
            [sent] = [row for row in metadata if row["status"] == "SENT"]
            assert sent["attempt"] == 3
            assert sent["document_sha256"] == digest(attachments[-1]["content"])
            assert sent["document_bytes"] == len(attachments[-1]["content"])
            assert sent["request_reply_to_message_ref"] == "77"
            body = [row for row in receipts if row["filename"] == attachments[0]["filename"]]
            assert len(body) == 2
            assert {row["status"] for row in body} == {"GENERATED", "SENT"}
        await reconcile_concurrently(repo, http)
        async with reopen(repo) as reloaded:
            await drain_worker(runtime_for(reloaded, http))
            assert await recovery_job(reloaded, original) == completed
            await assert_original_preserved(reloaded, original, notice, previous)
    uploads = uploads_from(origin, state)
    assert_uploads(uploads[:1], attachments[:1], 77)
    assert_uploads(uploads[1:], attachments[-1:] * 3, 77)
    assert len(state["telegram"]) == 4
    assert all(path.endswith("/sendDocument") for path, _, _ in state["telegram"])


async def test_terminal_document_with_five_real_http_failures_is_not_given_a_recovery_budget(
    diagnostic_repo, diagnostic_clock, document_failure_server
):
    repo = diagnostic_repo
    origin, state = document_failure_server
    state["remaining_failures"] = 5
    notice, attachments = legacy_notice("missing")
    async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
        previous = await historical_network_attempts(notice, http, 5)
        await persist_receipts(repo, previous)
        original = await seed_failed_job(repo, notice, diagnostic_clock)
        for _ in range(2):
            async with reopen(repo) as reloaded:
                await reloaded.recover_jobs()
                runtime = runtime_for(reloaded, http)
                await runtime.reconcile_delivery_failures()
                await drain_worker(runtime)
                assert await reloaded.list("ops.jobs", filters={"type": "DELIVER"}) == [original]
                await assert_original_preserved(reloaded, original, notice, previous)
                documents = await reloaded.list(
                    "out.deliveries", filters={"notice_id": notice["notice_id"], "method": "sendDocument"}
                )
                assert len(documents) == 10
                assert not any(row["status"] == "SENT" for row in documents)
                [terminal] = [row for row in documents if row.get("terminal")]
                assert terminal["status"] == "FAILED" and terminal["attempt"] == 5
    assert_uploads(uploads_from(origin, state), attachments * 5, 77)
    assert len(state["telegram"]) == 5


@pytest.mark.parametrize(
    "exclusion",
    [
        "expired_failed",
        "expired_status",
        "unrelated_kind",
        "wrong_channel",
        "no_parent",
        "terminal_parent",
        "corrupt_digest",
        "corrupt_size",
        "corrupt_filename",
        "corrupt_metadata",
        "corrupt_body",
        "unknown_evidence",
        "unknown_response_evidence",
    ],
)
async def test_reconciliation_does_not_revive_ineligible_or_unproven_diagnostic_jobs(
    diagnostic_repo, diagnostic_clock, wire_server, exclusion
):
    repo = diagnostic_repo
    origin, state = wire_server
    notice, _ = legacy_notice(
        "text" if exclusion in {"corrupt_body", "unknown_response_evidence"} else "missing"
    )
    if exclusion == "unrelated_kind":
        notice["kind"] = "SOURCE_FAILED"
    elif exclusion == "corrupt_digest":
        notice["document_manifest"][0]["sha256"] = "0" * 64
    elif exclusion == "corrupt_size":
        notice["document_manifest"][0]["bytes"] += 1
    elif exclusion == "corrupt_filename":
        notice["document_manifest"][0]["filename"] = "unproven.json"
    elif exclusion == "corrupt_metadata":
        notice["entry_errors"][0]["diagnostics"]["outcome"] = "altered"
    elif exclusion == "corrupt_body":
        notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]["body"] += "altered"
    elif exclusion == "unknown_evidence":
        notice["entry_errors"][0]["diagnostics"]["unknown"] = {"version": 99, "keep": ["evidence"]}
    elif exclusion == "unknown_response_evidence":
        notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]["unknown"] = "retain"
    original = await seed_failed_job(
        repo,
        notice,
        diagnostic_clock,
        expired=exclusion in {"expired_failed", "expired_status"},
        status="EXPIRED" if exclusion == "expired_status" else "FAILED",
        channel="telegram:alerts" if exclusion == "wrong_channel" else "telegram:ops",
    )
    previous = {}
    if exclusion != "no_parent":
        parent = parent_receipt(notice["notice_id"])
        if exclusion == "terminal_parent":
            parent.update(status="FAILED", terminal=True, attempt=5, message_ref=None, error="DeliveryError")
        previous["original_parent"] = parent
        await persist_receipts(repo, previous)
    async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
        for _ in range(2):
            async with reopen(repo) as reloaded:
                await reloaded.recover_jobs()
                runtime = runtime_for(reloaded, http)
                await runtime.reconcile_delivery_failures()
                await drain_worker(runtime)
                assert await reloaded.list("ops.jobs", filters={"type": "DELIVER"}) == [original]
                await assert_original_preserved(reloaded, original, notice, previous)
                assert not await reloaded.list(
                    "out.deliveries", filters={"notice_id": notice["notice_id"], "method": "sendDocument"}
                )
    assert state["telegram"] == []
