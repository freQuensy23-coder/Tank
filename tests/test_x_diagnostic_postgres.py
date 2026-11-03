import os

import httpx
import pytest
from test_x_diagnostics import diagnostics, multipart, notice_for, telegram_settings
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server

from tank.delivery import DeliveryError, DeliveryService
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
