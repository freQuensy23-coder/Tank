import base64
import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from email import policy
from email.parser import BytesParser
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic_settings import SettingsConfigDict
from source_support import (
    fixture_http_admission,
    fixture_request_budgets,
    fixture_versions,
    source_definition,
)
from test_sources import settings as source_settings
from test_xsensor import URL, run

from tank import runtime_composition
from tank.contracts.sources import SourceAccount
from tank.delivery import DeliveryError, DeliveryService
from tank.ops_entry_errors import report_entry_errors
from tank.runtime import Runtime
from tank.settings import Settings, resolve_versions
from tank.sources import SourceService
from tank.sources.x_diagnostics import documents, sanitize
from tank.sources.xsensor import XSensor
from tank.storage import MemoryRepository
from tank.util import sha256


def diagnostics(
    body="upstream body", *, encoding="utf-8", status=503, at="2026-10-24T12:00:00Z", request_id="one"
):
    return {
        "request": {"method": "GET", "url": "https://x.com/alice", "body": None},
        "outcome": "exhausted",
        "attemptCount": 3,
        "attempts": [
            {
                "attempt": index,
                "at": at,
                "error": f"PUBLIC_PROFILE_HTTP_{status}",
                "status": status,
                "retryable": True,
                "response": {
                    "status": status,
                    "headers": {"content-type": "text/plain", "x-request-id": request_id},
                    "body": body,
                    "bodyEncoding": encoding,
                    "bodyTruncated": False,
                    "bodyBytes": len(base64.b64decode(body) if encoding == "base64" else body.encode()),
                    "bodySha256": sha256(base64.b64decode(body) if encoding == "base64" else body.encode()),
                    "redacted": False,
                },
            }
            for index in range(1, 4)
        ],
    }


def failed_run(evidence):
    return run(
        [],
        [
            {
                "handle": "alice",
                "status": "failed",
                "error": "PUBLIC_PROFILE_HTTP_503",
                "diagnostics": evidence,
            }
        ],
        {"alice": "999"},
        health="failed",
        error="PUBLIC_SOURCE_UNAVAILABLE",
    )


class DiagnosticFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def telegram_settings(tmp_path: Path) -> Settings:
    return DiagnosticFixtureSettings(
        data_dir=tmp_path / "data",
        backup_dir=tmp_path / "backup",
        telegram_token="test",
        telegram_ops_chat="ops",
        telegram_alerts_chat="alerts",
        ops_progress_enabled=True,
        apify_api_token="apify-secret",
        openma_api_key="synthetic-openma-key",
        codex_gateway_key="synthetic-gateway-key",
    )


def diagnostic_source():
    return source_definition({"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": URL}]})


def telegram_transport(
    handler: Callable[[httpx.Request], httpx.Response],
) -> httpx.MockTransport:
    def guarded(request: httpx.Request) -> httpx.Response:
        if (
            request.method != "POST"
            or request.url.scheme != "https"
            or request.url.host != "api.telegram.org"
            or request.url.port not in (None, 443)
            or request.url.userinfo
            or request.url.path not in {"/bottest/sendMessage", "/bottest/sendDocument"}
        ):
            raise AssertionError("Diagnostic fixture refuses undeclared HTTP")
        return handler(request)

    return httpx.MockTransport(guarded)


@asynccontextmanager
async def delivery_runtime(
    repo: MemoryRepository, settings: Settings, http: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[Runtime]:

    def isolate_global_logging(captured: Settings) -> None:
        return None

    monkeypatch.setattr(runtime_composition, "configure_logging", isolate_global_logging)
    runtime = runtime_composition.build_runtime(repo, resolve_versions(settings))
    await runtime.delivery.close()
    runtime.delivery = DeliveryService(repo, runtime.settings, http=http)
    try:
        yield runtime
    finally:
        await runtime.close()


def multipart(request):
    message = BytesParser(policy=policy.default).parsebytes(
        b"Content-Type: " + request.headers["content-type"].encode() + b"\r\n\r\n" + request.content
    )
    return {
        part.get_param("name", header="content-disposition"): (
            part.get_filename(),
            part.get_payload(decode=True),
        )
        for part in message.iter_parts()
    }


async def notice_for(repo, evidence, tmp_path: Path):
    notice_id = await report_entry_errors(
        repo,
        telegram_settings(tmp_path),
        diagnostic_source(),
        diagnostic_source().feeds[0],
        [{"account": "alice", "reason": "PUBLIC_PROFILE_HTTP_503", "diagnostics": evidence}],
        "2026-10-24T12:00:00Z",
    )
    return await repo.get("out.ops_notices", notice_id)


@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
def test_sanitization_removes_echoed_token_cookies_and_auth_headers(encoding):
    raw = b"apify-secret\nCookie: forbidden-cookie\nAuthorization: Bearer forbidden-auth\nbody"
    evidence = diagnostics(
        base64.b64encode(raw).decode() if encoding == "base64" else raw.decode(), encoding=encoding
    )
    evidence["attempts"][0]["response"]["headers"].update(
        {"Authorization": "forbidden-auth", "Set-Cookie": "forbidden-cookie"}
    )
    clean = sanitize(evidence, "apify-secret")
    serialized = clean.model_dump_json()
    payloads = b"".join(
        row.content
        for row in documents(
            [
                {
                    "account": "alice",
                    "reason": "PUBLIC_PROFILE_HTTP_503",
                    "diagnostics": clean.model_dump(mode="json"),
                }
            ]
        )
    )
    assert all(
        secret.encode() not in payloads and secret not in serialized
        for secret in ("apify-secret", "forbidden-cookie", "forbidden-auth")
    )
    assert clean.attempts[0].response is not None
    assert clean.attempts[0].response.headers == {
        "content-type": "text/plain",
        "x-request-id": "one",
    }


async def test_all_failed_accounts_reach_checkpoint_ops_and_preserve_cursor(tmp_path):
    repo = MemoryRepository()
    config = source_settings(
        tmp_path, [{"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": URL}]}]
    )
    config.apify_api_token = "apify-secret"
    config.ops_progress_enabled = True
    evidence = diagnostics("full upstream diagnostic body")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, json=failed_run(evidence)))
    ) as http:
        service = SourceService(
            repo, config, http=http, versions=fixture_versions(), budgets=fixture_request_budgets()
        )
        service.accounts = {
            "alice": SourceAccount(handle="alice", role="OTHER", entity_id="alice", token_id=None)
        }
        await repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"alice": "90"}})
        result = await service.poll_once("S29")
    state = await repo.get("ops.source_checkpoints", "S29:0")
    assert result.status == "DEGRADED" and result.items == 0
    assert state["account_cursors"] == {"alice": "90"}
    assert (
        state["entry_errors"][0]["diagnostics"]["attempts"][2]["response"]["body"]
        == "full upstream diagnostic body"
    )
    [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
    assert "Ошибка HTTP-сбора X" in notice["text"] and "GET request body: empty" in notice["text"]
    assert len(notice["document_manifest"]) == 4
    [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
    assert job["payload"] == {"notice_id": notice["notice_id"], "channel": "telegram:ops"}


async def test_same_body_deduplicates_despite_new_attempt_time_and_request_id_but_changed_body_is_sent(
    tmp_path: Path,
):
    repo = MemoryRepository()
    first = await notice_for(repo, diagnostics("first"), tmp_path)
    settings = telegram_settings(tmp_path)
    source = diagnostic_source()
    feed = source.feeds[0]
    error = {
        "account": "alice",
        "reason": "PUBLIC_PROFILE_HTTP_503",
        "diagnostics": diagnostics("first", at="2026-10-24T12:05:00Z", request_id="two"),
    }
    assert await report_entry_errors(repo, settings, source, feed, [error], "2026-10-24T12:05:00Z") is None
    second = await notice_for(repo, diagnostics("second"), tmp_path)
    assert second["notice_id"] != first["notice_id"]
    assert len(await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})) == 2


@pytest.mark.parametrize(
    "raw,encoding",
    [
        (b"small <html>& full body", "utf-8"),
        (b"long full diagnostic " * 1000, "utf-8"),
        (b"\x00\xff\x80binary", "base64"),
    ],
)
async def test_actual_document_upload_preserves_every_response_byte_and_receipt(
    raw, encoding, tmp_path: Path
):
    repo = MemoryRepository()
    body = base64.b64encode(raw).decode() if encoding == "base64" else raw.decode()
    notice = await notice_for(repo, diagnostics(body, encoding=encoding), tmp_path)
    sent = []

    def send(request):
        sent.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent), "date": 1}})

    async with httpx.AsyncClient(transport=telegram_transport(send)) as http:
        delivery = DeliveryService(repo, telegram_settings(tmp_path), http=http)
        payload = {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        await delivery.deliver(payload)
        await DeliveryService(repo, telegram_settings(tmp_path), http=http).deliver(payload)
    assert len(sent) == 5 and sent[0].url.path.endswith("sendMessage")
    uploads = [multipart(request) for request in sent[1:]]
    assert all(
        upload["chat_id"][1] == b"ops" and json.loads(upload["reply_parameters"][1]) == {"message_id": 1}
        for upload in uploads
    )
    assert [upload["document"][1] for upload in uploads[:3]] == [raw, raw, raw]
    metadata = json.loads(uploads[3]["document"][1])
    assert metadata["request"] == {"method": "GET", "url": "https://x.com/alice", "body": None}
    assert [a["response"]["bodySha256"] for a in metadata["attempts"]] == [sha256(raw)] * 3
    assert all("content-type" in a["response"]["headers"] for a in metadata["attempts"])
    receipts = await repo.list("out.deliveries", filters={"status": "SENT", "method": "sendDocument"})
    assert len(receipts) == 4 and [row["document_bytes"] for row in receipts[:3]] == [len(raw)] * 3


async def test_empty_http_response_body_is_delivered_explicitly_without_invalid_empty_upload(tmp_path: Path):
    repo = MemoryRepository()
    notice = await notice_for(repo, diagnostics(""), tmp_path)
    captured = []

    def send(request):
        captured.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(captured)}})

    async with httpx.AsyncClient(transport=telegram_transport(send)) as http:
        await DeliveryService(repo, telegram_settings(tmp_path), http=http).deliver(
            {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        )
    assert len(captured) == 2
    metadata = json.loads(multipart(captured[1])["document"][1])
    assert all(
        attempt["response"]["body"] == ""
        and attempt["response"]["bodyBytes"] == 0
        and (attempt["response"]["bodyDocument"] is None)
        for attempt in metadata["attempts"]
    )


async def test_oversized_response_uploads_bounded_prefix_and_explicit_truncation_metadata(tmp_path: Path):
    repo = MemoryRepository()
    raw = b"a" * 2000050
    evidence = diagnostics(raw.decode())
    evidence["attempts"] = evidence["attempts"][:1]
    evidence["attemptCount"] = 1
    evidence["attempts"][0]["response"].update(bodyTruncated=True, omittedWireBytes=50, wireBytes=2000000)
    notice = await notice_for(repo, evidence, tmp_path)
    captured = []

    def send(request):
        captured.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(captured)}})

    async with httpx.AsyncClient(transport=telegram_transport(send)) as http:
        await DeliveryService(repo, telegram_settings(tmp_path), http=http).deliver(
            {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        )
    assert len(captured) == 3
    assert multipart(captured[1])["document"][1] == raw[:2000000]
    metadata = json.loads(multipart(captured[2])["document"][1])
    response = metadata["attempts"][0]["response"]
    assert response["bodyTruncated"] and response["omittedWireBytes"] == 50
    assert response["bodyBytes"] == 2000000
    assert response["bodySha256"] == sha256(raw[:2000000])


async def test_partial_document_delivery_retry_does_not_duplicate_successful_message_or_documents(
    tmp_path: Path,
):
    repo = MemoryRepository()
    notice = await notice_for(repo, diagnostics("complete body"), tmp_path)
    sent, successful = ([], [])
    fail = True

    def send(request):
        nonlocal fail
        sent.append(request)
        if len(sent) == 3 and fail:
            fail = False
            return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 7}})
        successful.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

    payload = {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
    async with httpx.AsyncClient(transport=telegram_transport(send)) as http:
        delivery = DeliveryService(repo, telegram_settings(tmp_path), http=http)
        with pytest.raises(DeliveryError) as failure:
            await delivery.deliver(payload)
        assert failure.value.retry_after == pytest.approx(7, abs=0.1)
        await DeliveryService(repo, telegram_settings(tmp_path), http=http).deliver(payload)
        await delivery.deliver(payload)
    assert len(sent) == 6 and len(successful) == 5
    assert sum(request.url.path.endswith("sendMessage") for request in sent) == 1
    filenames = [multipart(request)["document"][0] for request in successful[1:]]
    assert len(set(filenames)) == 4


async def test_diagnostics_do_not_allow_failed_account_posts_or_cursors_to_advance():
    evidence = failed_run(diagnostics())
    evidence["health"] = "degraded"
    evidence["posts"] = [
        {
            "id": "100",
            "author": "alice",
            "text": "bad",
            "url": "https://x.com/alice/status/100",
            "publishedAt": "2026-10-24T12:00:00Z",
        }
    ]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, json=evidence))
    ) as http:
        scan = await XSensor(http, "apify-secret", admission=fixture_http_admission()).posts(
            URL, ["alice"], {"alice": "90"}
        )
    assert scan.cursors == {"alice": "90"}
    assert scan.rows == []


@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
@pytest.mark.parametrize(
    "raw,expected",
    [
        (b'{"cookie":"session","detail":"upstream A"}', b'{"cookie":"[REDACTED]","detail":"upstream A"}'),
        (b'{"ct0":"escaped\\"secret","detail":1}', b'{"ct0":"[REDACTED]","detail":1}'),
        (b"cookie=session; detail=upstream A", b"cookie=[REDACTED]; detail=upstream A"),
        (b"?auth_token=session&detail=upstream+A", b"?auth_token=[REDACTED]&detail=upstream+A"),
        (b"authorization: Basic abc123, detail=upstream A", b"authorization: [REDACTED], detail=upstream A"),
    ],
)
def test_sanitization_preserves_adjacent_nonsecret_bytes(encoding, raw, expected):
    prefix = b"\x00\xff" if encoding == "base64" else b""
    raw = prefix + raw
    evidence = sanitize(
        diagnostics(base64.b64encode(raw).decode() if prefix else raw.decode(), encoding=encoding)
    )
    response = evidence.attempts[0].response
    assert response is not None
    content = base64.b64decode(response.body) if prefix else response.body.encode()
    assert content == prefix + expected
    assert response.bodySha256 == sha256(prefix + expected)
    assert response.redacted


@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
async def test_changed_json_detail_survives_redaction_and_creates_distinct_ops_notice(
    encoding, tmp_path: Path
):
    repo = MemoryRepository()
    notices = []
    for detail in ("A", "B"):
        raw = f'{{"cookie":"session","detail":"upstream {detail}"}}'.encode()
        if encoding == "base64":
            raw = b"\x00\xff" + raw
        body = base64.b64encode(raw).decode() if encoding == "base64" else raw.decode()
        notices.append(await notice_for(repo, diagnostics(body, encoding=encoding), tmp_path))
    assert notices[0]["notice_id"] != notices[1]["notice_id"]
    for index, detail in enumerate((b"A", b"B")):
        attachments = documents(notices[index]["entry_errors"])
        assert b"session" not in attachments[0].content
        assert b'"detail":"upstream ' + detail + b'"}' in attachments[0].content
    assert len(await repo.list("ops.jobs", filters={"type": "DELIVER"})) == 2


@pytest.mark.parametrize("exhaust_first", [False, True])
async def test_job_runner_preserves_independent_document_retry_budgets(
    monkeypatch, exhaust_first, tmp_path: Path
):
    import asyncio
    from datetime import timedelta

    import tank.delivery
    import tank.runtime
    import tank.storage
    from tank.util import parse_time, utcnow

    now = utcnow()
    for module in (tank.delivery, tank.runtime, tank.storage):
        monkeypatch.setattr(module, "utcnow", lambda: now)
    repo = MemoryRepository()
    notice = await notice_for(repo, diagnostics("full body"), tmp_path)
    [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
    counts = {}
    sent = []
    message_count = 0
    filenames = [row["filename"] for row in notice["document_manifest"]]

    def send(request):
        nonlocal message_count
        if request.url.path.endswith("sendMessage"):
            message_count += 1
        else:
            filename = multipart(request)["document"][0]
            counts[filename] = counts.get(filename, 0) + 1
            failures = 5 if exhaust_first and filename == filenames[0] else 4
            if counts[filename] <= failures:
                return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 1}})
            sent.append(filename)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": message_count + len(sent)}})

    async with (
        httpx.AsyncClient(transport=telegram_transport(send)) as http,
        delivery_runtime(repo, telegram_settings(tmp_path), http, monkeypatch) as runtime,
    ):
        for _ in range(30):
            current = await repo.get("ops.jobs", job["job_id"])
            if current["status"] in {"DONE", "FAILED"}:
                break
            now = max(now, parse_time(current["run_after"])) + timedelta(microseconds=1)
            stop = asyncio.Event()

            async def deliver(payload):
                try:
                    await runtime.delivery.deliver(payload)
                finally:
                    stop.set()

            runtime.handlers = {"DELIVER": deliver}
            await runtime.worker(stop)
    current = await repo.get("ops.jobs", job["job_id"])
    assert current["status"] == ("FAILED" if exhaust_first else "DONE")
    assert counts == dict.fromkeys(filenames, 5)
    assert sent == (filenames[1:] if exhaust_first else filenames)
    assert message_count == 1
    assert current["attempts"] == (5 if exhaust_first else 1)
    ledger = await repo.list("out.deliveries", filters={"method": "sendDocument", "status": "SENT"})
    assert [row["filename"] for row in ledger] == sent
    failures = await repo.list("out.deliveries", filters={"method": "sendDocument", "terminal": True})
    assert len(failures) == int(exhaust_first)


async def test_job_runner_defers_large_successful_document_batches_without_spending_attempts(
    monkeypatch, tmp_path: Path
):
    import asyncio
    from datetime import timedelta

    import tank.delivery
    import tank.runtime
    import tank.storage
    from tank.util import parse_time, utcnow

    now = utcnow()
    elapsed = 0
    for module in (tank.delivery, tank.runtime, tank.storage):
        monkeypatch.setattr(module, "utcnow", lambda: now)
    monkeypatch.setattr(tank.delivery, "time", SimpleNamespace(monotonic=lambda: elapsed))
    repo = MemoryRepository()
    notice_id = await report_entry_errors(
        repo,
        telegram_settings(tmp_path),
        diagnostic_source(),
        diagnostic_source().feeds[0],
        [
            {"account": f"account{index}", "reason": "PUBLIC_PROFILE_HTTP_503", "diagnostics": diagnostics()}
            for index in range(86)
        ],
        "2026-10-24T12:00:00Z",
    )
    notice = await repo.get("out.ops_notices", notice_id)
    [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
    expected = [row["filename"] for row in notice["document_manifest"]]
    sent = []
    messages = 0
    batches = []

    def send(request):
        nonlocal elapsed, messages
        if request.url.path.endswith("sendMessage"):
            messages += 1
        else:
            sent.append(multipart(request)["document"][0])
            elapsed += 3
        return httpx.Response(200, json={"ok": True, "result": {"message_id": messages + len(sent)}})

    async with (
        httpx.AsyncClient(transport=telegram_transport(send)) as http,
        delivery_runtime(repo, telegram_settings(tmp_path), http, monkeypatch) as runtime,
    ):
        for _ in range(50):
            current = await repo.get("ops.jobs", job["job_id"])
            if current["status"] in {"DONE", "FAILED"}:
                break
            now = max(now, parse_time(current["run_after"])) + timedelta(microseconds=1)
            stop = asyncio.Event()

            async def deliver(payload):
                try:
                    await runtime.delivery.deliver(payload)
                finally:
                    stop.set()

            runtime.handlers = {"DELIVER": deliver}
            started = elapsed
            await runtime.worker(stop)
            batches.append(elapsed - started)
    current = await repo.get("ops.jobs", job["job_id"])
    assert current["status"] == "DONE" and current["attempts"] == 1
    assert len(expected) == 344 and sent == expected and (messages == 1)
    assert len(batches) > 5 and all(0 < duration <= 30 for duration in batches)
    receipts = await repo.list("out.deliveries", filters={"method": "sendDocument", "status": "SENT"})
    assert len(receipts) == 344 and all(row["attempt"] == 1 for row in receipts)
    assert not await repo.list("out.deliveries", filters={"status": "FAILED"})
