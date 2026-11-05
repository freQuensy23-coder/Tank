import base64

import httpx
import pytest
from test_x_diagnostics import diagnostics, failed_run, notice_for, telegram_settings
from test_xsensor import URL

from tank.delivery import DeliveryService
from tank.sources.x_diagnostics import documents, sanitize
from tank.sources.xsensor import XSensor
from tank.storage import MemoryRepository


@pytest.mark.parametrize(
    "evidence,field",
    [
        ("invalid", "diagnostics"),
        ({"request": "invalid"}, "request"),
        ({"attempts": "invalid"}, "attempts"),
        ({"attempts": [None]}, "attempts[1]"),
        ({"attempts": [{"response": [1]}]}, "response[1]"),
        ({"attempts": [{"response": {"headers": "invalid"}}]}, "headers[1]"),
    ],
)
async def test_malformed_diagnostics_preserve_account_failure_and_cursor(evidence, field):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, json=failed_run(evidence)))
    ) as http:
        result = await XSensor(http, "apify-secret").posts(URL, ["alice"], {"alice": "90"})
    assert result.failed and result.rows == [] and result.cursors == {"alice": "90"}
    [failure] = result.failures
    assert failure.reason == "PUBLIC_PROFILE_HTTP_503"
    assert field in failure.diagnostics["invalidEvidence"]
    attachments = documents([{"account": failure.account, "diagnostics": failure.diagnostics}])
    assert attachments[-1]["filename"].endswith("diagnostics.json")
    assert field.encode() in attachments[-1]["content"]


@pytest.mark.parametrize("encoding", ["utf-8", "base64"])
def test_consumer_budget_is_shared_before_body_decoding(encoding):
    raw = b"abcdefghij"
    body = base64.b64encode(raw).decode() if encoding == "base64" else raw.decode()
    budget = [8]
    first = sanitize(diagnostics(body, encoding=encoding), budget=budget)
    second = sanitize(diagnostics(body, encoding=encoding), budget=budget)
    responses = [attempt["response"] for item in (first, second) for attempt in item["attempts"]]
    assert budget == [0]
    assert [response["bodyBytes"] for response in responses] == [8, 0, 0, 0, 0, 0]
    assert all(response["bodyTruncated"] for response in responses)
    assert documents([{"account": "alice", "diagnostics": first}])[0]["content"] == raw[:8]


async def test_document_receipt_does_not_satisfy_parent_message_delivery():
    repo = MemoryRepository()
    notice = await notice_for(repo, diagnostics("body"))
    await repo.put(
        "out.deliveries",
        "orphan-document",
        {
            "notice_id": notice["notice_id"],
            "channel": "telegram:ops",
            "method": "sendDocument",
            "status": "SENT",
            "attempt": 5,
            "message_ref": "999",
        },
        immutable=True,
    )
    paths = []

    def send(request):
        paths.append(request.url.path)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(paths)}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        await DeliveryService(repo, telegram_settings(), http=http).deliver(
            {"notice_id": notice["notice_id"]}
        )
    assert paths[0].endswith("sendMessage") and len(paths) == 5
    parents = await repo.list("out.deliveries", filters={"status": "SENT"})
    assert any(row.get("method") != "sendDocument" and row["attempt"] == 1 for row in parents)
