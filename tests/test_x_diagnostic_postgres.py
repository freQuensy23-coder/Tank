import copy
import os
from unittest.mock import patch

import httpx
import pytest
from test_x_diagnostics import diagnostics, multipart, notice_for, telegram_settings
from test_x_legacy_diagnostics import (
    BODY,
    BODY_HASH,
    METADATA,
    METADATA_HASH,
    canonical_manifest,
    legacy_byte_oracle,
    legacy_notice,
    oversized_legacy_notice,
    retained_manifest,
)
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server

from tank.delivery import DeliveryError, DeliveryService
from tank.sources import x_diagnostic_manifest
from tank.sources.x_diagnostics import documents

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


async def test_jsonb_readback_preserves_manifest_multipart_bytes_and_durable_receipts(
    postgres_template, wire_server
):
    origin, state = wire_server
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice = await notice_for(repo, diagnostics("retained PostgreSQL body"))
        expected = documents(notice["entry_errors"])
        saved = await repo.get("out.ops_notices", notice["notice_id"])
        assert documents(saved["entry_errors"]) == expected
        payload = {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
            await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
            await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
        assert len(state["telegram"]) == 5
        for received, document in zip(state["telegram"][1:], expected, strict=True):
            path, headers, body = received
            assert path.endswith("sendDocument")
            request = httpx.Request("POST", origin, headers=headers, content=body)
            assert multipart(request)["document"] == (document["filename"], document["content"])
        receipts = await repo.list("out.deliveries", filters={"status": "SENT", "method": "sendDocument"})
        assert {row["document_sha256"] for row in receipts} == {row["sha256"] for row in expected}
        assert all(row["message_ref"] and row["chat_id"] == "ops" for row in receipts)


async def test_jsonb_partial_retry_skips_durable_successful_documents(postgres_template):
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice = await notice_for(repo, diagnostics("retry body"))
        sent = []

        def send(request):
            sent.append(request)
            if len(sent) == 3:
                return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 1}})
            return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
            payload = {"notice_id": notice["notice_id"]}
            with pytest.raises(DeliveryError):
                await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
            await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
        assert len(sent) == 6
        receipts = await repo.list("out.deliveries", filters={"status": "SENT", "method": "sendDocument"})
        assert len(receipts) == 4
        assert len({row["document_id"] for row in receipts}) == 4


@pytest.mark.parametrize("kind", ["utf8", "binary", "empty"])
async def test_legacy_jsonb_readback_uploads_exact_original_documents(postgres_template, wire_server, kind):
    origin, state = wire_server
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice, expected = legacy_byte_oracle(kind)
        await repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
        saved = await repo.get("out.ops_notices", notice["notice_id"])
        assert list(saved["entry_errors"][0]["diagnostics"]) != list(notice["entry_errors"][0]["diagnostics"])
        assert documents(saved["entry_errors"])[-1]["sha256"] != saved["document_manifest"][-1]["sha256"]
        async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
            await DeliveryService(repo, telegram_settings(), http=http).deliver(
                {"notice_id": notice["notice_id"]}
            )
        uploads = [
            multipart(httpx.Request("POST", origin, headers=headers, content=body))["document"]
            for path, headers, body in state["telegram"][1:]
        ]
        assert uploads == expected
        receipts = await repo.list("out.deliveries", filters={"status": "SENT", "method": "sendDocument"})
        assert {row["document_sha256"] for row in receipts} == {
            row["sha256"] for row in notice["document_manifest"]
        }
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved


async def test_legacy_jsonb_429_commits_parent_and_prior_receipt_across_restart(postgres_template):
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice = legacy_notice()
        await repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
        saved = await repo.get("out.ops_notices", notice["notice_id"])
        sent = []

        def send(request):
            sent.append(request)
            if len(sent) == 3:
                return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 7}})
            return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

        payload = {"notice_id": notice["notice_id"]}
        async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
            with pytest.raises(DeliveryError) as failure:
                await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
            assert failure.value.retry_after == pytest.approx(7, abs=0.1)
        await repo.close()
        await repo.open()
        committed = await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]})
        parents = [
            row for row in committed if row["status"] == "SENT" and row.get("method") != "sendDocument"
        ]
        assert len(parents) == 1 and parents[0]["message_ref"] == "1"
        successful = [
            row for row in committed if row["status"] == "SENT" and row.get("method") == "sendDocument"
        ]
        assert len(successful) == 1 and successful[0]["document_sha256"] == BODY_HASH
        failed = [row for row in committed if row["status"] == "FAILED"]
        assert len(failed) == 1 and failed[0]["document_sha256"] == METADATA_HASH
        async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
            await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
            await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
        assert [request.url.path.rsplit("/", 1)[-1] for request in sent] == [
            "sendMessage",
            "sendDocument",
            "sendDocument",
            "sendDocument",
        ]
        assert [multipart(request)["document"][1] for request in sent[1:]] == [BODY, METADATA, METADATA]
        receipts = await repo.list("out.deliveries", filters={"status": "SENT", "method": "sendDocument"})
        assert {(row["document_sha256"], row["attempt"]) for row in receipts} == {
            (BODY_HASH, 1),
            (METADATA_HASH, 2),
        }
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved


@pytest.mark.parametrize("case", ["corruption", "shape", "privacy", "document_budget", "notice_budget"])
async def test_legacy_jsonb_refusal_precedes_every_attachment_upload(postgres_template, case):
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice = legacy_notice()
        evidence = notice["entry_errors"][0]["diagnostics"]
        if case == "shape":
            evidence["untrusted"] = "refuse"
        elif case == "privacy":
            evidence["attempts"][0]["response"]["headers"]["server"] = "apify-secret"
        elif case == "document_budget":
            evidence["attempts"][0]["response"]["headers"]["retry-after"] = "1"
        elif case == "notice_budget":
            notice["entry_errors"] = [copy.deepcopy(notice["entry_errors"][0]) for _ in range(6)]
        retained_manifest(notice)
        if case == "corruption":
            notice["document_manifest"][-1]["sha256"] = "0" * 64
        await repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
        saved = await repo.get("out.ops_notices", notice["notice_id"])
        sent = []

        def send(request):
            sent.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
            with pytest.raises(DeliveryError):
                await DeliveryService(repo, telegram_settings(), http=http).deliver(
                    {"notice_id": notice["notice_id"]}
                )
        assert [request.url.path.rsplit("/", 1)[-1] for request in sent] == ["sendMessage"]
        assert not await repo.list("out.deliveries", filters={"method": "sendDocument"})
        parents = await repo.list("out.deliveries", filters={"status": "SENT"})
        assert len(parents) == 1 and parents[0]["message_ref"] == "1"
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved


@pytest.mark.parametrize("case", ["attempts", "utf8", "base64", "unknown_nested", "aggregate"])
async def test_legacy_jsonb_oversize_refuses_without_materialization_or_document_transitions(
    postgres_template, case
):
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice = oversized_legacy_notice(case)
        await repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
        saved = await repo.get("out.ops_notices", notice["notice_id"])
        sent = []

        def send(request):
            sent.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

        with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
            async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
                with pytest.raises(DeliveryError):
                    await DeliveryService(repo, telegram_settings(), http=http).deliver(
                        {"notice_id": notice["notice_id"]}
                    )
        assert [request.url.path.rsplit("/", 1)[-1] for request in sent] == ["sendMessage"]
        await repo.close()
        await repo.open()
        assert not await repo.list("out.deliveries", filters={"method": "sendDocument"})
        parents = await repo.list("out.deliveries", filters={"status": "SENT"})
        assert len(parents) == 1 and parents[0]["message_ref"] == "1"
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved
        materialize.assert_not_called()


@pytest.mark.parametrize("kind", ["binary", "empty"])
async def test_canonical_jsonb_corrupt_base64_keeps_original_manifest_and_refuses(
    postgres_template, wire_server, kind
):
    origin, state = wire_server
    notice, _ = legacy_byte_oracle(kind)
    response = notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]
    response["bodyEncoding"] = "base64"
    canonical_manifest(notice)
    original_manifest = copy.deepcopy(notice["document_manifest"])
    response["body"] = "!!!!" + response["body"]
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.put("out.ops_notices", notice["notice_id"], notice, immutable=True)
        saved = await repo.get("out.ops_notices", notice["notice_id"])
        assert saved["document_manifest"] == original_manifest
        stored_response = saved["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]
        assert stored_response["body"].startswith("!!!!")
        with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
            async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
                with pytest.raises(DeliveryError):
                    await DeliveryService(repo, telegram_settings(), http=http).deliver(
                        {"notice_id": notice["notice_id"]}
                    )
        materialize.assert_not_called()
        assert [path.rsplit("/", 1)[-1] for path, _, _ in state["telegram"]] == ["sendMessage"]
        await repo.close()
        await repo.open()
        assert not await repo.list("out.deliveries", filters={"method": "sendDocument"})
        parents = await repo.list("out.deliveries", filters={"status": "SENT"})
        assert len(parents) == 1 and parents[0]["message_ref"] == "1"
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved
