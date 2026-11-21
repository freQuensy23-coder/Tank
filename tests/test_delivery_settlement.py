import asyncio
import os
from types import SimpleNamespace

import httpx
import pytest

from tank.delivery import DeliveryDeferred, DeliveryError, DeliveryService
from tank.time_precision import collection_clock


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["database", "cancel"])
async def test_card_receipt_survives_bookkeeping_rollback(postgres_template, monkeypatch, fault):
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="42", telegram_ops_chat="43")
    posts = []

    def send(request):
        posts.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 41 + len(posts)}})

    async with (
        postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo,
        httpx.AsyncClient(transport=httpx.MockTransport(send)) as http,
    ):
        stamp = collection_clock()
        event = {
            "event_id": "e",
            "statement": "fixture",
            "theme_tags": [],
            **stamp.fields("public_at"),
            "public_at_basis": "fixture",
            **stamp.fields("known_to_us_at"),
            "freshness": "FRESH",
            "event_state": "ANNOUNCED",
            "state_verified": False,
            "certainty": "fixture",
            "timing_confidence": "fixture",
            "best_source_class": "fixture",
            "lifecycle": "ANALYZING",
        }
        await repo.put("evt.events", "e", event)
        payload = {"alert_id": "a", "version": 1}
        await repo.put(
            "out.alerts",
            "a:1",
            {
                **payload,
                "event_id": "e",
                "kind": "FULL",
                "card_struct": {},
                "card_text": "fixture",
                "decisions": [],
                "code_version": "fixture",
                "config_version": "fixture",
                "evidence_bundle_id": "a" * 64,
                **stamp.fields("generated_at"),
            },
            immutable=True,
        )
        svc = DeliveryService(repo, settings, http=http)

        async def fail(*args):
            await repo.put("evt.events", "e", {**event, "lifecycle": "CLOSED"})
            if fault == "cancel":
                asyncio.current_task().cancel()
                await asyncio.sleep(0)
            await repo._conn().execute("SELECT 1 / 0")

        monkeypatch.setattr(svc, "_activate_calls", fail)
        with pytest.raises(asyncio.CancelledError if fault == "cancel" else DeliveryError):
            await asyncio.create_task(svc.deliver(payload))
        (sent,) = await repo.list("out.deliveries", filters={**payload, "status": "SENT"})
        assert sent["message_ref"] == "42" and len(posts) == 1
        assert await repo.get("evt.events", "e") == event
        checkpoint = await repo.get("ops.scheduler_checkpoints", "delivery-settlement:e")
        assert checkpoint["state"]["error_type"] == (
            "CancelledError" if fault == "cancel" else "DivisionByZero"
        )
        later = {**(await repo.get("out.alerts", "a:1")), "version": 2}
        await repo.put("out.alerts", "a:2", later, immutable=True)
        if fault == "database":
            with pytest.raises(DeliveryDeferred):
                await svc.deliver({**payload, "version": 2})
        assert len(posts) == 1
        assert await repo.get("ops.scheduler_checkpoints", "delivery-settlement:e") == checkpoint
        subsequent = DeliveryService(repo, settings, http=http)
        await subsequent.deliver(payload)
        await subsequent.deliver(payload)
        assert len(posts) == 1
        assert await repo.get("ops.scheduler_checkpoints", "delivery-settlement:e") is None
        settled = await repo.get("evt.events", "e")
        assert settled["lifecycle"] == "ALERTED" and settled["first_card_delivered_at"] == sent["sent_at"]
