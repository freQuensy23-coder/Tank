"""HTTP receipt provenance without substituting expected Telegram reply targets."""

from types import SimpleNamespace

import httpx
import pytest

from tank.delivery import DeliveryService
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("returned_target", [None, 41, 99])
async def test_requested_and_returned_reply_targets_are_separate(returned_target):
    repo = MemoryRepository()
    for alert_id in ("a", "b"):
        await repo.put(
            "out.alerts",
            alert_id + ":1",
            {
                "alert_id": alert_id,
                "version": 1,
                "event_id": "e",
                "card_text": alert_id,
                "decisions": [],
            },
            immutable=True,
        )
    count = 0

    def send(request):
        nonlocal count
        count += 1
        result = {"message_id": 40 + count}
        if count == 2 and returned_target is not None:
            result["reply_to_message"] = {"message_id": returned_target}
        return httpx.Response(200, json={"ok": True, "result": result})

    settings = SimpleNamespace(telegram_token="fake-token", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        service = DeliveryService(repo, settings, http=http)
        await service.deliver({"alert_id": "a", "version": 1})
        await service.deliver({"alert_id": "b", "version": 1})
    sent = await repo.list("out.deliveries", filters={"status": "SENT"})
    first, update = sent
    assert first["request_reply_to_message_ref"] is None
    assert first["response_reply_to_message_ref"] is None
    assert update["request_reply_to_message_ref"] == "41"
    assert update["response_reply_to_message_ref"] == (
        str(returned_target) if returned_target is not None else None
    )
    assert update["message_ref"] == "42"
