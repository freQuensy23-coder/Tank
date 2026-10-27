"""Actual delivered decisions cannot confer another event's call authority."""

from typing import Literal

import httpx
import pytest
from delivery_support import delivery_job
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.delivery import DeliveryService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock


class OwnershipSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.parametrize("action", ["START", "EXIT"])
@pytest.mark.parametrize("identity", ["MATCH", "FOREIGN_EVENT", "FOREIGN_DECISION"])
async def test_actual_delivery_requires_selected_decision_ownership(
    action: Literal["START", "EXIT"], identity: Literal["MATCH", "FOREIGN_EVENT", "FOREIGN_DECISION"]
) -> None:
    repo = MemoryRepository()
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": "e",
            "card_text": "Authored publication",
            "decisions": ["d"],
            **collection_clock().fields("generated_at"),
        },
        immutable=True,
    )
    decision: dict[str, JsonValue] = {
        "dec_id": "foreign" if identity == "FOREIGN_DECISION" else "d",
        "event_id": "foreign" if identity == "FOREIGN_EVENT" else "e",
        "token_id": "t",
        "action": action,
        "entry_ref_price": "100",
        "invalidation_price": "90",
    }
    await repo.put("ana.decisions", "d", decision)
    original: dict[str, JsonValue] = {
        "call_id": "c",
        "event_id": "e",
        "token_id": "t",
        "status": "OPEN",
        "retained_evidence": "actual original call",
    }
    if action == "EXIT":
        await repo.put("ana.calls", "c", original)
    before = await repo.list("ana.calls")
    requests: list[httpx.Request] = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        delivery = DeliveryService(
            repo,
            OwnershipSettings(
                telegram_token="synthetic",
                telegram_alerts_chat="10",
                ops_progress_enabled=False,
                replay=False,
            ),
            http=http,
        )
        if identity == "MATCH":
            await delivery.deliver(delivery_job({"alert_id": "a", "version": 1}))
        else:
            with pytest.raises(ValueError):
                await delivery.deliver(delivery_job({"alert_id": "a", "version": 1}))
    if identity != "MATCH":
        assert requests == []
        assert await repo.list("ana.calls") == before
        assert await repo.list("out.deliveries") == []
        assert await repo.get("ana.decisions", "d") == decision
    else:
        assert len(requests) == 1
        calls = await repo.list("ana.calls")
        assert len(calls) == 1
        assert calls[0]["event_id"] == "e"
        assert calls[0]["token_id"] == "t"
        assert calls[0]["status"] == ("OPEN" if action == "START" else "CLOSED")
        if action == "EXIT":
            assert calls[0]["retained_evidence"] == "actual original call"
