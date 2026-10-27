import json

import httpx
import pytest
from test_x_diagnostics import telegram_settings

from tank.delivery import DeliveryService
from tank.ops_entry_errors import report_entry_errors
from tank.storage import MemoryRepository


@pytest.mark.parametrize("reason", ["MISSING_FROM_RUN", "NO_CURSOR"])
@pytest.mark.parametrize("diagnostics_present", [False, True])
async def test_text_only_entry_error_without_diagnostics_delivers_once(reason, diagnostics_present):
    repo = MemoryRepository()
    error = {"account": "alice", "reason": reason}
    if diagnostics_present:
        error["diagnostics"] = None
    notice_id = await report_entry_errors(
        repo,
        telegram_settings(),
        {"source_id": "S29", "name": "X"},
        {"url": "https://example.test/actor"},
        [error],
        "2026-10-27T10:00:00Z",
    )
    notice = await repo.get("out.ops_notices", notice_id)
    assert "document_manifest" not in notice
    requests = []

    def handle(request):
        requests.append(request)
        assert request.url.path.endswith("/sendMessage")
        assert reason in json.loads(request.content)["text"]
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 91, "date": 1}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        delivery = DeliveryService(repo, telegram_settings(), http=http)
        payload = {"notice_id": notice_id, "channel": "telegram:ops"}
        await delivery.deliver(payload)
        await delivery.deliver(payload)
    assert len(requests) == 1
    [receipt] = await repo.list("out.deliveries", filters={"notice_id": notice_id, "status": "SENT"})
    assert receipt["message_ref"] == "91"
    assert receipt.get("method") != "sendDocument"
