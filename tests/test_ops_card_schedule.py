"""Operational progress sends never borrow missing historical clocks from current time."""

from datetime import timedelta

import httpx
import pytest
from delivery_support import delivery_job
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.delivery import DeliveryDeferred, DeliveryService
from tank.ops_cards import render, snapshot
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp, collection_clock, offset_schedule
from tank.util import sha256, utcnow


class ScheduleSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def settings(delay: float) -> Settings:
    return ScheduleSettings(
        telegram_token="test",
        telegram_ops_chat="10",
        ops_progress_enabled=True,
        ops_progress_delay_s=delay,
        replay=False,
    )


async def seed(repo: MemoryRepository, clock: Timestamp | None, *, legacy: bool = False) -> str:
    await repo.put(
        "ingest.source_items",
        "i",
        {
            "item_id": "i",
            "source_id": "S29",
            "title": "Actual source",
            "relevance": {"decision": "ALWAYS"},
            **collection_clock().fields("observed_at"),
        },
    )
    await repo.put(
        "out.ops_notices",
        "n",
        {
            "notice_id": "n",
            "kind": "ITEM_PROGRESS",
            "subject_id": "i",
            "stage": "RECEIVED",
            "reason": "",
            "text": "Received",
            "format_version": 1 if legacy else 2,
            **(clock.fields("created_at") if clock is not None else {}),
        },
        immutable=True,
    )
    return sha256({"item_id": "i", "chat_id": "10"})


@pytest.mark.parametrize("phase", ["INITIAL", "UPDATE"])
async def test_unknown_consumed_clock_refuses_before_progress_effect(phase: str) -> None:
    repo = MemoryRepository()
    key = await seed(repo, None if phase == "INITIAL" else collection_clock(utcnow() - timedelta(minutes=1)))
    if phase == "UPDATE":
        await repo.put("out.ops_cards", key, {"item_id": "i", "chat_id": "10", "message_ref": "88"})
    before = await repo.get("out.ops_cards", key)
    requests: list[httpx.Request] = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 88}})

    captured: Exception | None = None
    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        try:
            await DeliveryService(repo, settings(15), http=http).deliver(delivery_job({"notice_id": "n"}))
        except Exception as error:
            captured = error
    assert requests == []
    assert await repo.list("out.deliveries") == []
    assert await repo.get("out.ops_cards", key) == before
    assert isinstance(captured, ValueError)


@pytest.mark.parametrize("phase", ["INITIAL", "UPDATE"])
async def test_declared_zero_spacing_does_not_need_unused_old_clock(phase: str) -> None:
    repo = MemoryRepository()
    key = await seed(repo, None)
    if phase == "UPDATE":
        await repo.put("out.ops_cards", key, {"item_id": "i", "chat_id": "10", "message_ref": "88"})
    requests: list[httpx.Request] = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 88}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        await DeliveryService(repo, settings(0), http=http).deliver(delivery_job({"notice_id": "n"}))
    assert len(requests) == 1
    assert requests[0].url.path.endswith("editMessageText" if phase == "UPDATE" else "sendMessage")


@pytest.mark.parametrize("phase", ["COVERED", "SAME_RENDER"])
async def test_no_effect_progress_phase_does_not_require_unused_clock(phase: str) -> None:
    repo = MemoryRepository()
    key = await seed(repo, None)
    card: dict[str, JsonValue] = {"item_id": "i", "chat_id": "10", "message_ref": "88"}
    if phase == "COVERED":
        card["covered_notice_ids"] = ["n"]
    else:
        captured = await snapshot(repo, "i")
        assert captured is not None
        card["render_digest"] = sha256(render(captured).html)
    await repo.put("out.ops_cards", key, card)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("A completed progress phase must not use HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        await DeliveryService(repo, settings(15), http=http).deliver(delivery_job({"notice_id": "n"}))
    assert await repo.list("out.deliveries") == []


async def test_adopted_receipt_actual_clock_controls_edit_spacing() -> None:
    repo = MemoryRepository()
    config = settings(15)
    await seed(repo, collection_clock(utcnow() - timedelta(minutes=1)), legacy=True)
    sent_at = collection_clock()
    parent: dict[str, JsonValue] = {
        "notice_id": "n",
        "channel": "telegram:ops",
        "status": "SENT",
        "chat_id": "10",
        "message_ref": "88",
        "attempt": 1,
        **sent_at.fields("at"),
    }
    await repo.put("out.deliveries", "parent", parent, immutable=True)
    requests: list[httpx.Request] = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 88}})

    captured: Exception | None = None
    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        try:
            await DeliveryService(repo, config, http=http).deliver(delivery_job({"notice_id": "n"}))
        except Exception as error:
            captured = error
    assert requests == []
    assert isinstance(captured, DeliveryDeferred)
    assert captured.not_before.fields("due") == offset_schedule(sent_at, timedelta(seconds=5)).fields("due")
    assert await repo.list("out.deliveries") == [parent]


async def test_actual_snapshot_does_not_admit_empty_history() -> None:
    repo = MemoryRepository()
    await repo.put("ingest.source_items", "i", {"item_id": "i", "title": "Actual source"})
    with pytest.raises(ValueError):
        await snapshot(repo, "i")
