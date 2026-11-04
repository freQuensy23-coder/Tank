import base64
import hashlib
import json
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


@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
async def test_jsonb_cookie_header_privacy_survives_wire_delivery_and_pool_reopen(
    postgres_template, wire_server, encoding
):
    raw = (
        b"Cookie: sid=cookie-primary; refresh=cookie-secondary, tail=cookie-tail\r\n"
        b"Set-Cookie: session=set-primary; refresh=set-secondary\r\nbody"
    )
    expected = b"Cookie: [REDACTED]\r\nSet-Cookie: [REDACTED]\r\nbody"
    if encoding == "base64":
        raw += b"\x00\xff"
        expected += b"\x00\xff"
    evidence = diagnostics(
        base64.b64encode(raw).decode() if encoding == "base64" else raw.decode(), encoding=encoding
    )
    origin, state = wire_server
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice = await notice_for(repo, evidence)
        saved = await repo.get("out.ops_notices", notice["notice_id"])
        payload = {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
            await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
            await repo.close()
            await repo.open()
            committed = await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]})
            await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
        assert len(state["telegram"]) == 5
        uploads = [
            multipart(httpx.Request("POST", origin, headers=headers, content=body))
            for path, headers, body in state["telegram"][1:]
        ]
        assert [upload["document"][1] for upload in uploads[:3]] == [expected] * 3
        metadata = json.loads(uploads[3]["document"][1])
        digest = hashlib.sha256(expected).hexdigest()
        assert [attempt["response"]["bodySha256"] for attempt in metadata["attempts"]] == [digest] * 3
        parents = [
            row for row in committed if row["status"] == "SENT" and row.get("method") != "sendDocument"
        ]
        receipts = [
            row for row in committed if row["status"] == "SENT" and row.get("method") == "sendDocument"
        ]
        assert len(parents) == 1 and len(receipts) == 4
        bodies = [row for row in receipts if row["document_sha256"] == digest]
        assert len(bodies) == 3 and all(row["document_bytes"] == len(expected) for row in bodies)
        assert all(
            json.loads(upload["reply_parameters"][1]) == {"message_id": int(parents[0]["message_ref"])}
            for upload in uploads
        )
        assert all(row["request_reply_to_message_ref"] == parents[0]["message_ref"] for row in receipts)
        assert all(
            secret not in content
            for content in [json.dumps(saved).encode(), *(upload["document"][1] for upload in uploads)]
            for secret in (
                b"cookie-primary",
                b"cookie-secondary",
                b"cookie-tail",
                b"set-primary",
                b"set-secondary",
            )
        )
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved
        assert await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]}) == committed


@pytest.mark.parametrize("credential", ["auth_token", "ct0"])
@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
async def test_jsonb_query_credentials_redact_suffixes_before_manifest_and_wire_delivery(
    postgres_template, wire_server, credential, encoding
):
    raw = (
        f"safe body prefix https://x.com/alice?{credential}="
        "query-distinctprefix,query-distinctsuffix;query-distincttail&detail=safe\r\n"
        f'https://x.com/alice?{credential}="'
        'query-doubleprefix,query-doublesuffix;query-doubletail"&detail=double-safe\r\n'
        f"https://x.com/alice?before=single-safe&{credential}='"
        "query-singleprefix,query-singlesuffix;query-singletail'&detail=single-safe\r\n"
        f"https://x.com/alice?{credential}=&detail=empty-safe\r\n"
        f'https://x.com/alice?{credential}=""&detail=double-empty-safe\r\n'
        f"https://x.com/alice?{credential}=''&detail=single-empty-safe\r\n"
        "safe body suffix"
    ).encode()
    expected = (
        f"safe body prefix https://x.com/alice?{credential}=[REDACTED]&detail=safe\r\n"
        f'https://x.com/alice?{credential}="[REDACTED]"&detail=double-safe\r\n'
        f"https://x.com/alice?before=single-safe&{credential}='[REDACTED]'&detail=single-safe\r\n"
        f"https://x.com/alice?{credential}=[REDACTED]&detail=empty-safe\r\n"
        f'https://x.com/alice?{credential}="[REDACTED]"&detail=double-empty-safe\r\n'
        f"https://x.com/alice?{credential}='[REDACTED]'&detail=single-empty-safe\r\n"
        "safe body suffix"
    ).encode()
    secret_pieces = (
        b"query-distinctprefix",
        b"query-distinctsuffix",
        b"query-distincttail",
        b"query-doubleprefix",
        b"query-doublesuffix",
        b"query-doubletail",
        b"query-singleprefix",
        b"query-singlesuffix",
        b"query-singletail",
    )
    if encoding == "base64":
        raw += b"\x00\xff"
        expected += b"\x00\xff"
    evidence = diagnostics(
        base64.b64encode(raw).decode() if encoding == "base64" else raw.decode(), encoding=encoding
    )
    digest = hashlib.sha256(expected).hexdigest()
    extension = "bin" if encoding == "base64" else "txt"
    filenames = [f"x-alice-1-attempt-{index}-{digest[:12]}.{extension}" for index in range(1, 4)]
    origin, state = wire_server
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice = await notice_for(repo, evidence)
        saved = await repo.get("out.ops_notices", notice["notice_id"])
        assert saved["document_manifest"][:3] == [
            {"filename": filename, "sha256": digest, "bytes": len(expected)} for filename in filenames
        ]
        responses = [
            attempt["response"] for attempt in saved["entry_errors"][0]["diagnostics"]["attempts"]
        ]
        for response in responses:
            persisted_body = (
                base64.b64decode(response["body"], validate=True)
                if encoding == "base64"
                else response["body"].encode()
            )
            assert persisted_body == expected
            assert all(piece not in persisted_body for piece in secret_pieces)
            assert response["bodyEncoding"] == encoding
            assert response["bodyBytes"] == len(expected) and response["bodySha256"] == digest
            assert response["redacted"] and not response["bodyTruncated"]
            assert response["consumerCapturedBytes"] == len(raw)
        payload = {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
        async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
            await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
            await repo.close()
            await repo.open()
            committed = await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]})
            await DeliveryService(repo, telegram_settings(), http=http).deliver(payload)
        assert len(state["telegram"]) == 5
        assert state["telegram"][0][0].endswith("sendMessage")
        assert all(path.endswith("sendDocument") for path, _, _ in state["telegram"][1:])
        uploads = [
            multipart(httpx.Request("POST", origin, headers=headers, content=body))
            for _, headers, body in state["telegram"][1:]
        ]
        assert [upload["document"] for upload in uploads[:3]] == [
            (filename, expected) for filename in filenames
        ]
        metadata = json.loads(uploads[3]["document"][1])
        for index, attempt in enumerate(metadata["attempts"]):
            assert attempt["response"] == {
                **{key: value for key, value in responses[index].items() if key != "body"},
                "bodyDocument": filenames[index],
            }
        assert saved["document_manifest"] == [
            {
                "filename": upload["document"][0],
                "sha256": hashlib.sha256(upload["document"][1]).hexdigest(),
                "bytes": len(upload["document"][1]),
            }
            for upload in uploads
        ]
        parents = [
            row for row in committed if row["status"] == "SENT" and row.get("method") != "sendDocument"
        ]
        receipts = [
            row for row in committed if row["status"] == "SENT" and row.get("method") == "sendDocument"
        ]
        assert len(parents) == 1 and len(receipts) == 4
        assert all(row["message_ref"] and row["chat_id"] == "ops" and row["attempt"] == 1 for row in receipts)
        assert {
            row["filename"]: (row["document_sha256"], row["document_bytes"]) for row in receipts
        } == {
            item["filename"]: (item["sha256"], item["bytes"]) for item in saved["document_manifest"]
        }
        assert all(row["request_reply_to_message_ref"] == parents[0]["message_ref"] for row in receipts)
        assert all(
            json.loads(upload["reply_parameters"][1]) == {"message_id": int(parents[0]["message_ref"])}
            for upload in uploads
        )
        assert all(
            piece not in content
            for content in [
                json.dumps(saved).encode(),
                json.dumps(committed).encode(),
                *(body for _, _, body in state["telegram"]),
                *(upload["document"][1] for upload in uploads),
            ]
            for piece in secret_pieces
        )
        assert await repo.get("out.ops_notices", notice["notice_id"]) == saved
        assert await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]}) == committed


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
