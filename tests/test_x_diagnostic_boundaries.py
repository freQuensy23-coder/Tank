import base64
import hashlib
import json
import os

import httpx
import pytest
from test_x_diagnostics import diagnostics, failed_run, multipart, telegram_settings
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server
from test_xsensor import URL

from tank.delivery import DeliveryService
from tank.ops_entry_errors import report_entry_errors
from tank.sources import x_diagnostics
from tank.sources.xsensor import XSensor
from tank.storage import MemoryRepository


async def consume(evidence):
    payload = json.dumps(failed_run(evidence), ensure_ascii=True).encode()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, content=payload))
    ) as http:
        scan = await XSensor(http, "apify-secret").posts(URL, ["alice"], {"alice": "90"})
    assert scan.failed and scan.cursors == {"alice": "90"}
    return scan.failures[0].diagnostics


async def persist(repo, clean):
    return await report_entry_errors(
        repo,
        telegram_settings(),
        {"source_id": "S29", "name": "X"},
        {"url": URL},
        [{"account": "alice", "reason": "PUBLIC_PROFILE_HTTP_503", "diagnostics": clean}],
        "2026-10-24T12:00:00Z",
    )


@pytest.mark.parametrize(
    "bad", ["apify-secret", {"echo": "apify-secret"}, ["apify-secret"], -1, True]
)
async def test_untrusted_scalars_never_escape_in_uploaded_documents(bad):
    evidence = diagnostics("safe body")
    evidence["invalidEvidence"] = ["apify-secret" * 10000]
    evidence["attempts"][0].update(at="apify-secret", status=bad, retryable=bad)
    evidence["attempts"][0]["response"].update(
        status=bad,
        wireBytes=bad,
        omittedWireBytes=bad,
        captureLimitBytes=bad,
        serializedBodyLimitBytes=bad,
        bodyEncoding=bad,
        bodyTruncated=bad,
        redacted=bad,
        readError="apify-secret\u0000",
        decodeError={"echo": "apify-secret"},
    )
    clean = await consume(evidence)
    assert {"at[1]", "invalidEvidence", "readError[1]"} <= set(clean["invalidEvidence"])
    repo = MemoryRepository()
    notice_id = await persist(repo, clean)
    uploads = []

    def send(request):
        if request.url.path.endswith("sendDocument"):
            uploads.append(multipart(request)["document"])
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(uploads) + 1}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        await DeliveryService(repo, telegram_settings(), http=http).deliver({"notice_id": notice_id})
    assert uploads
    assert all(b"apify-secret" not in content for _, content in uploads)
    metadata = next(content for filename, content in uploads if filename.endswith(".json"))
    assert len(metadata) < 40000
    assert b"\\u0000" not in metadata and b"\\ud800" not in metadata


@pytest.mark.parametrize("field,value", [("request", []), ("attempts", {}), ("attempts", False)])
async def test_falsey_malformed_containers_are_explicit(field, value):
    clean = await consume({field: value})
    assert field in clean["invalidEvidence"]


@pytest.mark.parametrize(
    "body,expected", [("éé", b"\xc3\xa9\xc3"), ("🙂", b"\xf0\x9f\x99")]
)
def test_clipped_utf8_preserves_exact_prefix_without_claiming_redaction(body, expected):
    evidence = diagnostics(body)
    evidence["attempts"] = evidence["attempts"][:1]
    clean = x_diagnostics.sanitize(evidence, budget=[3])
    response = clean["attempts"][0]["response"]
    assert response["bodyEncoding"] == "base64"
    assert base64.b64decode(response["body"]) == expected
    assert response["bodySha256"] == hashlib.sha256(expected).hexdigest()
    assert response["bodyTruncated"] and not response["redacted"]
    assert x_diagnostics.documents([{"diagnostics": clean}])[0]["content"] == expected


def test_expansion_and_multibyte_share_capture_and_transformed_retention_budget():
    budget = [25]
    first = diagnostics("ct0=x;ct0=x;ct0=x;")
    first["attempts"] = first["attempts"][:1]
    second = diagnostics("🙂" * 8)
    second["attempts"] = second["attempts"][:1]
    clean = [x_diagnostics.sanitize(value, budget=budget) for value in (first, second)]
    responses = [value["attempts"][0]["response"] for value in clean]
    assert responses[0]["bodyBytes"] == 25
    assert responses[0]["consumerTransformTruncated"]
    assert responses[1]["bodyBytes"] == 0 and responses[1]["consumerCaptureTruncated"]
    assert budget == [0]
    assert sum(value["bodyBytes"] for value in responses) == 25


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize(
    "body,limit,expected", [("A\u0000é", 100, b"A\x00\xc3\xa9"), ("éé", 3, b"\xc3\xa9\xc3")]
)
async def test_malformed_text_jsonb_readback_uploads_exact_body(
    postgres_template, wire_server, monkeypatch, body, limit, expected
):
    monkeypatch.setattr(x_diagnostics, "BODY_LIMIT", limit)
    evidence = diagnostics(body)
    evidence["attempts"] = evidence["attempts"][:1]
    evidence["attempts"][0]["response"]["headers"]["server"] = "server\u0000"
    clean = await consume(evidence)
    assert clean["attempts"][0]["response"]["bodyEncoding"] == "base64"
    origin, state = wire_server
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        notice_id = await persist(repo, clean)
        saved = await repo.get("out.ops_notices", notice_id)
        stored = saved["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]
        assert base64.b64decode(stored["body"]) == expected
        assert stored["bodySha256"] == hashlib.sha256(expected).hexdigest()
        async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
            await DeliveryService(repo, telegram_settings(), http=http).deliver({"notice_id": notice_id})
        uploaded = []
        for path, headers, content in state["telegram"][1:]:
            assert path.endswith("sendDocument")
            request = httpx.Request("POST", origin, headers=headers, content=content)
            uploaded.append(multipart(request)["document"])
        assert len(uploaded) == 2
        assert uploaded[0][1] == expected
        metadata = json.loads(uploaded[1][1])
        assert metadata["attempts"][0]["response"]["bodySha256"] == hashlib.sha256(
            expected
        ).hexdigest()
        receipts = await repo.list(
            "out.deliveries", filters={"status": "SENT", "method": "sendDocument"}
        )
        assert {row["document_sha256"] for row in receipts} == {
            hashlib.sha256(content).hexdigest() for _, content in uploaded
        }
