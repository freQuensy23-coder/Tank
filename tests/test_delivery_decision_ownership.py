"""Actual delivered decisions cannot confer another event's call authority."""

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Literal

import httpx
import pytest
from delivery_support import delivery_job
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from test_event_tools import complete_decision_fixture, complete_event_fixture, complete_truth_fixture

from tank.delivery import DeliveryService
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.time_precision import collection_clock
from tank.util import sha256

if TYPE_CHECKING:
    from postgres_helpers import PostgresTemplate


class OwnershipSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.fixture
async def repository_lifetime() -> AsyncIterator[AsyncExitStack]:
    async with AsyncExitStack() as lifetime:
        yield lifetime


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
@pytest.mark.parametrize("action", ["START", "EXIT"])
@pytest.mark.parametrize("identity", ["MATCH", "FOREIGN_EVENT", "FOREIGN_DECISION", "CHANGED_DURING_SEND"])
async def test_actual_delivery_requires_selected_decision_ownership(
    action: Literal["START", "EXIT"],
    identity: Literal["MATCH", "FOREIGN_EVENT", "FOREIGN_DECISION", "CHANGED_DURING_SEND"],
    backend: Literal["memory", "postgres"],
    postgres_template: "PostgresTemplate",
    repository_lifetime: AsyncExitStack,
) -> None:
    repo: MemoryRepository | PostgresRepository
    if backend == "postgres":
        repo = await repository_lifetime.enter_async_context(
            postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"])
        )
    else:
        repo = MemoryRepository()
    stamp = collection_clock()
    assert stamp.value is not None
    for event_id in ("e", "foreign"):
        await repo.put("evt.events", event_id, complete_event_fixture(event_id))
    owner_event = "foreign" if identity == "FOREIGN_EVENT" else "e"
    await repo.put("ana.market_truth", "mta", complete_truth_fixture("mta", owner_event, "t", stamp.value))
    bundle_payload: dict[str, JsonValue] = {"fixture": "delivery-decision-ownership"}
    digest = sha256(bundle_payload)
    await repo.put(
        "out.evidence_bundles",
        digest,
        {"bundle_id": "EVB-" + digest, "sha256": digest, "payload": bundle_payload},
        immutable=True,
    )
    publication: dict[str, JsonValue] = {
        "alert_id": "a",
        "version": 1,
        "event_id": "e",
        "card_text": "Authored publication",
        "kind": "FULL",
        "card_struct": {},
        "config_version": "synthetic-ownership",
        "code_version": "synthetic-ownership",
        "evidence_bundle_id": digest,
        "decisions": ["d"],
        **collection_clock().fields("generated_at"),
    }
    await repo.put("out.alerts", "a:1", publication, immutable=True)
    decision: dict[str, JsonValue] = {
        **complete_decision_fixture("d", owner_event, "t", stamp.value, "mta"),
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
        "opened_by_alert": "authored-previous-alert",
        "opened_by_version": 1,
        **stamp.fields("opened_at"),
        "entry_ref_price": "100",
        "invalidation_price": "90",
        "retained_evidence": "actual original call",
    }
    if action == "EXIT":
        await repo.put("ana.calls", "c", original)
    before = await repo.list("ana.calls")
    requests: list[httpx.Request] = []

    async def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if identity == "CHANGED_DURING_SEND":
            await asyncio.create_task(repo.put("ana.decisions", "d", {**decision, "event_id": "foreign"}))
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
    if identity == "CHANGED_DURING_SEND":
        assert len(requests) == 1
        assert await repo.list("ana.calls") == before
        assert await repo.get("ana.decisions", "d") == {**decision, "event_id": "foreign"}
    elif identity != "MATCH":
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
