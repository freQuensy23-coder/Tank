"""Persisted call activation phases use only their actual consumed facts."""

import json

import httpx
import pytest
from delivery_support import delivery_job
from pydantic import JsonValue

from tank.delivery import DeliveryService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.telegram_api import TelegramResponseError, response_result
from tank.time_precision import Timestamp


@pytest.mark.parametrize("phase", ["EXISTING_START", "EXIT", "INACTIVE", "OTHER"])
async def test_delivery_activation_without_unused_prices(phase: str) -> None:
    repo = MemoryRepository()
    decision: dict[str, JsonValue] = {"dec_id": "d", "event_id": "e", "token_id": "t", "action": "START"}
    if phase == "EXIT":
        decision["action"] = "EXIT"
    elif phase == "INACTIVE":
        decision = {"tracking_ended": True}
    elif phase == "OTHER":
        decision["action"] = "HOLD"
    await repo.put("ana.decisions", "d", decision)
    original: dict[str, JsonValue] = {
        "call_id": "c",
        "event_id": "e",
        "token_id": "t",
        "status": "OPEN",
        "retained_evidence": "original",
    }
    await repo.put("ana.calls", "c", original)
    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = DeliveryService(repo, Settings(), http=http)
        await service._activate_calls(
            {"event_id": "e", "alert_id": "a", "version": 1, "decisions": ["d"]},
            Timestamp(value="2026-10-27T00:00:00Z", precision="EXACT"),
        )
    result = await repo.get("ana.calls", "c")
    assert result is not None
    assert result["retained_evidence"] == "original"
    assert result["status"] == ("CLOSED" if phase == "EXIT" else "OPEN")
    assert len(await repo.list("ana.calls")) == 1
    if phase == "EXIT":
        assert result["closed_at"] == "2026-10-27T00:00:00Z"
        assert result["closed_by"] == "EXIT"


def refuse_http(request: httpx.Request) -> httpx.Response:
    raise AssertionError("Call activation must not perform transport I/O")


@pytest.mark.parametrize("invalid", [None, True, "NaN", "Infinity", "", 0])
async def test_new_start_requires_real_prices_before_call_write(invalid: JsonValue) -> None:
    repo = MemoryRepository()
    await repo.put(
        "ana.decisions",
        "d",
        {
            "dec_id": "d",
            "event_id": "e",
            "token_id": "t",
            "action": "START",
            "entry_ref_price": invalid,
            "invalidation_price": "90",
        },
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = DeliveryService(repo, Settings(), http=http)
        with pytest.raises(ValueError):
            await service._activate_calls(
                {"event_id": "e", "alert_id": "a", "version": 1, "decisions": ["d"]},
                Timestamp(value="2026-10-27T00:00:00Z", precision="EXACT"),
            )
    assert await repo.list("ana.calls") == []


@pytest.mark.parametrize("event_id", ["e", "foreign"])
async def test_invalidated_correction_preserves_bound_event(event_id: str) -> None:
    repo = MemoryRepository()
    original: dict[str, JsonValue] = {
        "event_id": "e",
        "lifecycle": "INVALIDATED",
        "retained_evidence": {"source": "actual"},
    }
    await repo.put("evt.events", "e", original)
    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = DeliveryService(repo, Settings(), http=http)
        correction: dict[str, JsonValue] = {"event_id": event_id, "alert_id": "a", "version": 2}
        if event_id == "foreign":
            with pytest.raises(ValueError, match="does not belong"):
                await service._finalize_invalidated(original, correction)
            assert await repo.get("evt.events", "e") == original
        else:
            await service._finalize_invalidated(original, correction)
            stored = await repo.get("evt.events", "e")
            assert stored is not None
            assert stored["retained_evidence"] == {"source": "actual"}
            assert stored["terminal_alert_id"] == "a"
            assert stored["terminal_alert_version"] == 2
            assert stored["session_finalized_at"] is not None


@pytest.mark.parametrize("foreign_field", ["item_id", "chat_id"])
async def test_progress_delivery_refuses_foreign_stored_card_before_http(foreign_field: str) -> None:
    from tank.util import sha256

    repo = MemoryRepository()
    key = sha256({"item_id": "i", "chat_id": "2"})
    card: dict[str, JsonValue] = {"item_id": "i", "chat_id": "2", "message_ref": "9"}
    card[foreign_field] = "foreign"
    await repo.put("out.ops_cards", key, card)
    await repo.put(
        "out.ops_notices",
        "n",
        {
            "notice_id": "n",
            "kind": "ITEM_PROGRESS",
            "subject_id": "i",
        },
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = DeliveryService(repo, Settings(telegram_token="test", telegram_ops_chat="2"), http=http)
        with pytest.raises(ValueError, match="does not belong"):
            await service.deliver(delivery_job({"notice_id": "n"}))
    assert await repo.get("out.ops_cards", key) == card
    assert await repo.list("out.deliveries") == []


async def test_document_manifest_contradiction_refuses_before_parent_send() -> None:
    repo = MemoryRepository()
    notice: dict[str, JsonValue] = {
        "notice_id": "n",
        "kind": "ENTRY_ERRORS",
        "text": "Actual diagnostic notice",
        "entry_errors": [],
        "document_manifest": [{"filename": "missing.txt", "sha256": "0" * 64, "bytes": 1}],
    }
    await repo.put("out.ops_notices", "n", notice)
    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = DeliveryService(repo, Settings(telegram_token="test", telegram_ops_chat="2"), http=http)
        from tank.delivery import DeliveryError

        with pytest.raises(DeliveryError, match="manifest mismatch"):
            await service.deliver(delivery_job({"notice_id": "n"}))
    assert await repo.get("out.ops_notices", "n") == notice
    assert await repo.list("out.deliveries") == []


@pytest.mark.parametrize("retry_after", [None, 7])
async def test_typed_telegram_failure_keeps_unreported_delay_distinct(retry_after: int | None) -> None:
    wire: dict[str, JsonValue] = {"ok": False, "error_code": 503, "description": "Unavailable"}
    if retry_after is not None:
        wire["parameters"] = {"retry_after": retry_after}
    with pytest.raises(TelegramResponseError) as admitted:
        response_result(json.dumps(wire).encode(), method="sendMessage", status=503)
    rejection = admitted.value
    assert rejection.retry_after == retry_after
    repo = MemoryRepository()
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": "e",
            "kind": "FULL",
            "card_text": "Accepted card",
            "decisions": [],
        },
        immutable=True,
    )

    def send(request: httpx.Request) -> httpx.Response:
        raise rejection

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        service = DeliveryService(repo, Settings(telegram_token="test", telegram_alerts_chat="1"), http=http)
        from tank.delivery import DeliveryError

        with pytest.raises(DeliveryError) as failed:
            await service.deliver(delivery_job({"alert_id": "a", "version": 1}))
    assert failed.value.retry_after == (retry_after if retry_after is not None else 1)
    assert rejection.retry_after == retry_after
    assert [row["status"] for row in await repo.list("out.deliveries")] == ["GENERATED", "FAILED"]
    assert await repo.list("ana.calls") == []
