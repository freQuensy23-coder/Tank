import asyncio
import json
import os
from contextlib import suppress

import httpx
import pytest
from test_x_diagnostics import diagnostics, multipart, notice_for, telegram_settings
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server

from tank.delivery import DeliveryError, DeliveryService
from tank.delivery_jobs import parse_delivery_job
from tank.storage import MemoryRepository


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("reloaded_chat", ["-1002222222222", None], ids=["changed", "cleared"])
async def test_reopened_document_retry_uses_original_parent_route(
    postgres_template, wire_server, reloaded_chat
):
    origin, state = wire_server
    blocked = asyncio.Event()
    release = asyncio.Event()

    class InterruptedTransport(LoopbackTelegramTransport):
        uploads = 0

        async def handle_async_request(self, request):
            if request.url.path.endswith("sendDocument"):
                self.uploads += 1
                if self.uploads == 2:
                    blocked.set()
                    await release.wait()
            return await super().handle_async_request(request)

    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice = await notice_for(repo, diagnostics("original route body"))
        payload = {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        settings = telegram_settings()
        settings.telegram_ops_chat = "-1001111111111"
        producer = None
        try:
            async with httpx.AsyncClient(transport=InterruptedTransport(origin)) as http:
                producer = asyncio.create_task(
                    DeliveryService(repo, settings, http=http).deliver(payload)
                )
                await asyncio.wait_for(blocked.wait(), 3)
                producer.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await producer
            before = await repo.list("out.deliveries")
            frozen = json.dumps(before, sort_keys=True).encode()
            saved_notice = json.dumps(
                await repo.get("out.ops_notices", notice["notice_id"]), sort_keys=True
            ).encode()
            sent = [row for row in before if row["status"] == "SENT"]
            assert len(sent) == 2
            parent = [row for row in sent if row.get("method") != "sendDocument"]
            assert len(parent) == 1
            assert parent[0]["chat_id"] == "-1001111111111"
            assert parent[0]["message_ref"] == "1"
            await repo.close()
            await repo.open()
            reloaded = telegram_settings()
            reloaded.telegram_ops_chat = reloaded_chat
            async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
                await DeliveryService(repo, reloaded, http=http).deliver(payload)
            after = await repo.list("out.deliveries")
            retained = [row for row in after if row in before]
            assert json.dumps(retained, sort_keys=True).encode() == frozen
            assert (
                json.dumps(
                    await repo.get("out.ops_notices", notice["notice_id"]), sort_keys=True
                ).encode()
                == saved_notice
            )
            receipts = [
                row
                for row in after
                if row["status"] == "SENT" and row.get("method") == "sendDocument"
            ]
            assert len(receipts) == len(notice["document_manifest"])
            assert all(row["chat_id"] == "-1001111111111" for row in receipts)
            assert all(row["request_reply_to_message_ref"] == "1" for row in receipts)
            parents = [body for path, _, body in state["telegram"] if path.endswith("sendMessage")]
            assert len(parents) == 1
            assert json.loads(parents[0])["chat_id"] == "-1001111111111"
            uploads = [
                multipart(httpx.Request("POST", origin, headers=headers, content=body))
                for path, headers, body in state["telegram"]
                if path.endswith("sendDocument")
            ]
            assert len(uploads) == len(notice["document_manifest"])
            assert all(upload["chat_id"][1] == b"-1001111111111" for upload in uploads)
            assert all(
                json.loads(upload["reply_parameters"][1]) == {"message_id": 1} for upload in uploads
            )
            assert [upload["document"][0] for upload in uploads] == [
                item["filename"] for item in notice["document_manifest"]
            ]
        finally:
            release.set()
            if producer is not None and not producer.done():
                producer.cancel()
                with suppress(asyncio.CancelledError):
                    await producer


@pytest.mark.parametrize("reloaded_chat", ["new-ops", None], ids=["changed", "cleared"])
async def test_memory_documents_use_durable_parent_not_reloaded_settings(reloaded_chat):
    repo = MemoryRepository()
    notice = await notice_for(repo, diagnostics("memory route body"))
    payload = {"notice_id": notice["notice_id"]}
    captured = []

    def send(request):
        captured.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 41}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        original = DeliveryService(repo, telegram_settings(), http=http)
        await original._deliver(parse_delivery_job(payload))
        reloaded = telegram_settings()
        reloaded.telegram_ops_chat = reloaded_chat
        await DeliveryService(repo, reloaded, http=http).deliver(payload)
    uploads = [
        multipart(request) for request in captured if request.url.path.endswith("sendDocument")
    ]
    assert len(uploads) == len(notice["document_manifest"])
    assert all(upload["chat_id"][1] == b"ops" for upload in uploads)
    assert all(
        json.loads(upload["reply_parameters"][1]) == {"message_id": 41} for upload in uploads
    )


@pytest.mark.parametrize(
    "route",
    [
        {},
        {"chat_id": None, "message_ref": "41"},
        {"chat_id": {}, "message_ref": "41"},
        {"chat_id": "", "message_ref": "41"},
        {"chat_id": "ops", "message_ref": {"message_id": 41, "chat_id": "foreign"}},
        {"chat_id": "ops", "message_ref": "invalid"},
        {"chat_id": "ops", "message_ref": "0"},
    ],
    ids=[
        "missing",
        "null-chat",
        "foreign-chat-type",
        "empty-chat",
        "foreign-ref-type",
        "invalid-ref",
        "zero-ref",
    ],
)
async def test_invalid_parent_route_refuses_before_document_attempts(route):
    repo = MemoryRepository()
    notice = await notice_for(repo, diagnostics("refused route body"))
    await repo.put(
        "out.deliveries",
        "original-parent",
        {
            "notice_id": notice["notice_id"],
            "channel": "telegram:ops",
            "status": "SENT",
            "attempt": 1,
            **route,
        },
        immutable=True,
    )
    before = await repo.list("out.deliveries")
    captured = []

    def send(request):
        captured.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 99}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        with pytest.raises(DeliveryError, match="parent.*route"):
            await DeliveryService(repo, telegram_settings(), http=http).deliver(
                {"notice_id": notice["notice_id"]}
            )
    assert captured == []
    assert await repo.list("out.deliveries") == before
    assert await repo.get("out.ops_notices", notice["notice_id"]) == notice
