import asyncio
import gzip
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler

import httpx
import pytest
from test_sources import settings as source_settings
from test_x_diagnostics import multipart
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server

from tank.delivery import DeliveryService
from tank.sources import SourceService

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


async def test_real_actor_postgres_documents_survive_interrupted_delivery_and_reopen(
    tmp_path, postgres_template, wire_server
):
    origin, state = wire_server
    state["body"] = b"independent wire prefix\r\nCookie: sid=private-wire-one; refresh=private-wire-two\r\nend"
    expected = b"independent wire prefix\r\nCookie: [REDACTED]\r\nend"
    digest = hashlib.sha256(expected).hexdigest()
    names = [f"x-alice-1-attempt-{index}-{digest[:12]}.txt" for index in range(1, 4)]
    config = source_settings(
        tmp_path,
        [{"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": origin + "/actor"}]}],
    )
    config.apify_api_token = "isolated-apify-token"
    config.telegram_token = "isolated-telegram-token"
    config.telegram_ops_chat = "isolated-ops"
    config.ops_progress_enabled = True
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
        await repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"alice": "90"}})
        async with httpx.AsyncClient() as http:
            service = SourceService(repo, config, http=http)
            service.accounts = {"alice": {"handle": "alice", "entity_id": "alice", "role": "OTHER"}}
            poll_started = datetime.now(UTC)
            result = await service.poll_once("S29")
            poll_finished = datetime.now(UTC)
        assert result["status"] == "DEGRADED" and result["items"] == 0
        assert len(state["profile"]) == 3
        output = state["outputs"][0]
        assert output["health"] == "failed" and output["posts"] == []
        assert output["nextCursors"] == {"alice": "90"}
        checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
        assert checkpoint["account_cursors"] == {"alice": "90"}
        diagnostics = checkpoint["entry_errors"][0]["diagnostics"]
        assert diagnostics["outcome"] == "exhausted" and diagnostics["attemptCount"] == 3
        assert len(diagnostics["attempts"]) == 3
        wire_bytes = len(gzip.compress(state["body"], mtime=0))
        expected_attempts = []
        for index, attempt in enumerate(diagnostics["attempts"], 1):
            response = attempt["response"]
            assert response["status"] == 503 and response["body"].encode() == expected
            assert response["bodyBytes"] == len(expected) and response["bodySha256"] == digest
            timestamp = attempt["at"]
            assert isinstance(timestamp, str)
            parsed_timestamp = datetime.fromisoformat(timestamp)
            assert parsed_timestamp.utcoffset() is not None
            assert poll_started <= parsed_timestamp <= poll_finished
            date = response["headers"]["date"]
            assert isinstance(date, str)
            parsed_date = parsedate_to_datetime(date)
            assert parsed_date.utcoffset() is not None
            assert poll_started - timedelta(seconds=1) <= parsed_date <= poll_finished
            assert abs((parsed_timestamp - parsed_date).total_seconds()) < 15
            expected_attempts.append({
                "attempt": index,
                "at": timestamp,
                "status": 503,
                "error": "PUBLIC_PROFILE_HTTP_503",
                "retryable": True,
                "response": {
                    "status": 503,
                    "headers": {
                        "server": f"{BaseHTTPRequestHandler.server_version} {BaseHTTPRequestHandler.sys_version}",
                        "date": date,
                        "content-type": "text/plain",
                        "content-encoding": "gzip",
                        "x-request-id": "safe-wire-id",
                        "content-length": str(wire_bytes),
                    },
                    "body": expected.decode(),
                    "bodyEncoding": "utf-8",
                    "bodyBytes": len(expected),
                    "bodySha256": digest,
                    "bodyTruncated": False,
                    "consumerCapturedBytes": len(expected),
                    "consumerCaptureTruncated": False,
                    "consumerTransformTruncated": False,
                    "consumerBodyLimitBytes": 2_000_000,
                    "redacted": True,
                    "omittedWireBytes": 0,
                    "wireBytes": wire_bytes,
                    "captureLimitBytes": 2_000_000,
                    "serializedBodyLimitBytes": 4_000_000,
                    "readError": None,
                    "decodeError": None,
                },
            })
        expected_diagnostics = {
            "request": {"method": "GET", "url": origin + "/alice", "body": None},
            "outcome": "exhausted",
            "attempts": expected_attempts,
            "attemptCount": 3,
        }
        assert diagnostics == expected_diagnostics
        [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
        assert notice["entry_errors"] == checkpoint["entry_errors"]
        metadata = json.loads(json.dumps(expected_diagnostics))
        for attempt, name in zip(metadata["attempts"], names, strict=True):
            del attempt["response"]["body"]
            attempt["response"]["bodyDocument"] = name
        metadata_bytes = json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True).encode()
        metadata_name = "x-alice-1-diagnostics.json"
        expected_docs = [(name, expected) for name in names] + [(metadata_name, metadata_bytes)]
        assert notice["document_manifest"] == [
            {"filename": name, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            for name, body in expected_docs
        ]
        [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
        async with httpx.AsyncClient(transport=InterruptedTransport(origin)) as http:
            task = asyncio.create_task(DeliveryService(repo, config, http=http).deliver(job["payload"]))
            blocked_task = asyncio.create_task(blocked.wait())
            try:
                done, _ = await asyncio.wait(
                    {blocked_task, task}, timeout=30, return_when=asyncio.FIRST_COMPLETED
                )
                if task.done():
                    await task
                    raise AssertionError("Delivery completed before interruption")
                assert blocked_task in done, "Delivery did not reach the second upload within 30 seconds"
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                if not blocked_task.done():
                    blocked_task.cancel()
                await asyncio.gather(task, blocked_task, return_exceptions=True)
        before = await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]})
        assert len([row for row in before if row["status"] == "SENT"]) == 2
        assert len(state["telegram"]) == 2
        [before_parent] = [row for row in before if row["status"] == "SENT" and row.get("method") != "sendDocument"]
        [before_document] = [row for row in before if row["status"] == "SENT" and row.get("method") == "sendDocument"]
        assert (before_parent["chat_id"], before_parent["message_ref"]) == ("isolated-ops", "1")
        assert (before_document["chat_id"], before_document["message_ref"], before_document["filename"]) == (
            "isolated-ops", "2", names[0]
        )
        assert before_document["request_reply_to_message_ref"] == "1"
        assert state["telegram"][0][0].endswith("sendMessage")
        assert json.loads(state["telegram"][0][2])["chat_id"] == "isolated-ops"
        first_path, first_headers, first_body = state["telegram"][1]
        assert first_path.endswith("sendDocument")
        first_upload = multipart(httpx.Request("POST", origin, headers=first_headers, content=first_body))
        assert first_upload["chat_id"][1] == b"isolated-ops"
        assert first_upload["document"] == expected_docs[0]
        assert json.loads(first_upload["reply_parameters"][1]) == {"message_id": 1}
        await repo.close()
        await repo.open()
        assert await repo.get("ops.source_checkpoints", "S29:0") == checkpoint
        assert await repo.get("out.ops_notices", notice["notice_id"]) == notice
        assert await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]}) == before
        async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
            await DeliveryService(repo, config, http=http).deliver(job["payload"])
        after = await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]})
        assert all(row in after for row in before)
        sent = [row for row in after if row["status"] == "SENT"]
        parents = [row for row in sent if row.get("method") != "sendDocument"]
        receipts = [row for row in sent if row.get("method") == "sendDocument"]
        assert len(parents) == 1 and len(receipts) == 4
        assert parents[0] == before_parent and before_document in receipts
        assert (parents[0]["chat_id"], parents[0]["message_ref"]) == ("isolated-ops", "1")
        assert {row["filename"]: (row["chat_id"], row["message_ref"]) for row in receipts} == {
            name: ("isolated-ops", str(index)) for index, (name, _) in enumerate(expected_docs, 2)
        }
        assert {row["filename"]: (row["document_sha256"], row["document_bytes"]) for row in receipts} == {
            name: (hashlib.sha256(body).hexdigest(), len(body)) for name, body in expected_docs
        }
        assert all(row["request_reply_to_message_ref"] == parents[0]["message_ref"] for row in receipts)
        assert len(state["telegram"]) == 5 and state["telegram"][0][0].endswith("sendMessage")
        uploads = [
            multipart(httpx.Request("POST", origin, headers=headers, content=body))
            for path, headers, body in state["telegram"][1:]
            if path.endswith("sendDocument")
        ]
        assert [upload["document"] for upload in uploads] == expected_docs
        assert all(upload["chat_id"][1] == b"isolated-ops" for upload in uploads)
        assert all(
            json.loads(upload["reply_parameters"][1]) == {"message_id": int(parents[0]["message_ref"])}
            for upload in uploads
        )
        assert all(
            secret not in content
            for content in [
                json.dumps(checkpoint).encode(),
                json.dumps(notice).encode(),
                json.dumps(after).encode(),
                *(body for _, _, body in state["telegram"]),
            ]
            for secret in (b"private-wire-one", b"private-wire-two", b"forbidden-cookie")
        )
        assert await repo.get("out.ops_notices", notice["notice_id"]) == notice
