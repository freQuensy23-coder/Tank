from types import SimpleNamespace

import httpx
import pytest

from tank.delivery import DeliveryError, DeliveryService
from tank.storage import MemoryRepository


async def setup_repo():
    repo = MemoryRepository()
    await repo.put(
        "out.alerts",
        "a:1",
        {"alert_id": "a", "version": 1, "event_id": "e", "card_text": "<test>", "decisions": ["d"]},
        immutable=True,
    )
    await repo.put(
        "ana.decisions",
        "d",
        {
            "dec_id": "d",
            "event_id": "e",
            "token_id": "t",
            "action": "START",
            "entry_ref_price": "100",
            "invalidation_price": "90",
        },
    )
    return repo


@pytest.mark.asyncio
async def test_delivery_sent_opens_once_and_escapes_html():
    repo = await setup_repo()
    requests = []

    def send(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        service = DeliveryService(
            repo,
            SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2"),
            http=http,
        )
        await service.deliver({"alert_id": "a", "version": 1})
        await service.deliver({"alert_id": "a", "version": 1})
    assert len(requests) == 1
    assert "&lt;test&gt;" in requests[0].content.decode()
    assert len(await repo.list("ana.calls", filters={"status": "OPEN"})) == 1
    assert [r["status"] for r in await repo.list("out.deliveries")] == ["GENERATED", "SENT"]


@pytest.mark.asyncio
async def test_failure_never_opens_call_and_honors_retry_after():
    repo = await setup_repo()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 20}})
        )
    ) as http:
        service = DeliveryService(
            repo,
            SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2"),
            http=http,
        )
        with pytest.raises(DeliveryError) as exc:
            await service.deliver({"alert_id": "a", "version": 1})
        assert exc.value.retry_after == 20
    assert await repo.list("ana.calls") == []
    assert [r["status"] for r in await repo.list("out.deliveries")] == ["GENERATED", "FAILED"]


@pytest.mark.asyncio
async def test_update_replies_to_first_event_card():
    repo = await setup_repo()
    requests = []

    def send(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(requests) + 40}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        service = DeliveryService(
            repo,
            SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2"),
            http=http,
        )
        await service.deliver({"alert_id": "a", "version": 1})
        await repo.put(
            "out.alerts",
            "b:1",
            {"alert_id": "b", "version": 1, "event_id": "e", "card_text": "update", "decisions": []},
            immutable=True,
        )
        await service.deliver({"alert_id": "b", "version": 1})
    import json

    assert json.loads(requests[1].content)["reply_parameters"] == {"message_id": 41}


@pytest.mark.asyncio
async def test_invalidated_session_finalizes_only_after_sent_terminal_correction_without_call():
    repo = MemoryRepository()
    await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "INVALIDATED"})
    await repo.put(
        "out.alerts",
        "a:1",
        {"alert_id": "a", "version": 1, "event_id": "e", "card_text": "Original watch", "decisions": []},
        immutable=True,
    )
    await repo.put(
        "out.alerts",
        "b:1",
        {
            "alert_id": "b",
            "version": 1,
            "event_id": "e",
            "kind": "UPDATE",
            "terminal_correction": True,
            "card_text": "Official denial",
            "decisions": [],
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(500, json={"ok": False}))
    ) as http:
        with pytest.raises(DeliveryError):
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "b", "version": 1})
    assert not (await repo.get("evt.events", "e")).get("session_finalized_at")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"ok": True, "result": {"message_id": 9}})
        )
    ) as http:
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "b", "version": 1})
    event = await repo.get("evt.events", "e")
    assert event["session_finalized_at"] and event["terminal_alert_id"] == "b"
    assert any(r["status"] == "SENT" for r in await repo.list("out.deliveries"))


@pytest.mark.asyncio
async def test_invalidated_session_waits_for_every_call_to_close():
    repo = MemoryRepository()
    await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "INVALIDATED"})
    await repo.put(
        "ana.calls", "open", {"call_id": "open", "event_id": "e", "token_id": "t", "status": "OPEN"}
    )
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": "e",
            "kind": "UPDATE",
            "terminal_correction": True,
            "card_text": "Denial",
            "decisions": [],
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"ok": True, "result": {"message_id": 9}})
        )
    ) as http:
        service = DeliveryService(repo, settings, http=http)
        await service.deliver({"alert_id": "a", "version": 1})
        assert not (await repo.get("evt.events", "e")).get("session_finalized_at")
        await repo.put(
            "ana.decisions", "exit", {"dec_id": "exit", "event_id": "e", "token_id": "t", "action": "EXIT"}
        )
        await repo.put(
            "out.alerts",
            "a:2",
            {
                "alert_id": "a",
                "version": 2,
                "event_id": "e",
                "kind": "UPDATE",
                "terminal_correction": True,
                "card_text": "EXIT denial",
                "decisions": ["exit"],
            },
            immutable=True,
        )
        await service.deliver({"alert_id": "a", "version": 2})
    assert (await repo.get("ana.calls", "open"))["status"] == "CLOSED"
    assert (await repo.get("evt.events", "e"))["session_finalized_at"]


@pytest.mark.asyncio
async def test_structural_first_delivery_measures_missed_three_minute_target():
    from datetime import timedelta

    from tank.util import iso, utcnow

    repo = MemoryRepository()
    await repo.put(
        "evt.events",
        "e",
        {"event_id": "e", "lifecycle": "ANALYZING", "known_to_us_at": iso(utcnow() - timedelta(minutes=5))},
    )
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": "e",
            "kind": "PARTIAL",
            "card_struct": {"assessment": {"class": "STRUCTURAL_SHOCK"}},
            "card_text": "Verified structural fact; market pending",
            "decisions": [],
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"ok": True, "result": {"message_id": 9}})
        )
    ) as http:
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 1})
    event = await repo.get("evt.events", "e")
    assert event["first_card_delivered_at"] and event["first_card_latency_s"] >= 300
    assert event["first_card_deadline_met"] is False


@pytest.mark.asyncio
async def test_expiry_notice_never_reopens_frozen_start_and_consumes_only_delivered_ids():
    repo = MemoryRepository()
    await repo.put(
        "evt.events", "e", {"event_id": "e", "call_expired": True, "call_expired_ids": ["c", "later"]}
    )
    await repo.put(
        "ana.calls",
        "c",
        {"call_id": "c", "event_id": "e", "token_id": "T", "status": "CLOSED", "closed_by": "EXPIRED"},
    )
    await repo.put(
        "ana.decisions",
        "frozen",
        {
            "dec_id": "frozen",
            "event_id": "e",
            "token_id": "T",
            "action": "START",
            "tracking_ended": True,
            "actionable": False,
        },
    )
    await repo.put(
        "out.alerts",
        "a:2",
        {
            "alert_id": "a",
            "version": 2,
            "event_id": "e",
            "kind": "UPDATE",
            "card_text": "Tank no longer follows this call",
            "expired_call_ids": ["c"],
            "decisions": ["frozen"],
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(500, json={"ok": False}))
    ) as http:
        with pytest.raises(DeliveryError):
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 2})
    assert (await repo.get("evt.events", "e"))["call_expired_ids"] == ["c", "later"]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"ok": True, "result": {"message_id": 9}})
        )
    ) as http:
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 2})
    event = await repo.get("evt.events", "e")
    assert (
        event["call_expired"]
        and event["call_expired_ids"] == ["later"]
        and event["expiry_notices_sent"] == ["c"]
    )
    calls = await repo.list("ana.calls")
    assert len(calls) == 1 and calls[0]["status"] == "CLOSED"


@pytest.mark.asyncio
async def test_pending_update_survives_restart_waits_sent_clock_and_coalesces_before_exit(monkeypatch):
    from datetime import timedelta

    import tank.delivery as delivery_module
    from tank.delivery import DeliveryDeferred
    from tank.util import iso, utcnow

    base = utcnow()
    clock = [base + timedelta(minutes=2)]
    monkeypatch.setattr(delivery_module, "utcnow", lambda: clock[0])
    repo = MemoryRepository()
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": "e",
            "kind": "FULL",
            "generated_at": iso(base),
            "card_text": "Original",
            "decisions": [],
        },
        immutable=True,
    )
    await repo.put(
        "out.deliveries",
        "first",
        {
            "alert_id": "a",
            "version": 1,
            "channel": "telegram:alerts",
            "status": "SENT",
            "message_ref": "42",
            "attempt": 1,
            "at": iso(base),
        },
        immutable=True,
    )
    await repo.put(
        "ana.calls", "call", {"call_id": "call", "event_id": "e", "token_id": "T", "status": "OPEN"}
    )
    await repo.put(
        "ana.decisions",
        "dontchase",
        {"dec_id": "dontchase", "event_id": "e", "token_id": "T", "action": "DONT_CHASE"},
    )
    await repo.put(
        "out.alerts",
        "a:2",
        {
            "alert_id": "a",
            "version": 2,
            "event_id": "e",
            "kind": "UPDATE",
            "generated_at": iso(clock[0]),
            "card_text": "Do not chase",
            "decisions": ["dontchase"],
            "delivery_not_before": iso(base + timedelta(minutes=15)),
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2")
    requests = []

    def send(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 43}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        with pytest.raises(DeliveryDeferred) as exc:
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 2})
        assert exc.value.run_after == iso(base + timedelta(minutes=15)) and not requests
        # A new process sees the same durable pending candidate and still cannot send early.
        with pytest.raises(DeliveryDeferred):
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 2})
        clock[0] = base + timedelta(minutes=3)
        await repo.put(
            "ana.decisions", "exit", {"dec_id": "exit", "event_id": "e", "token_id": "T", "action": "EXIT"}
        )
        await repo.put(
            "out.alerts",
            "a:3",
            {
                "alert_id": "a",
                "version": 3,
                "event_id": "e",
                "kind": "UPDATE",
                "generated_at": iso(clock[0]),
                "card_text": "EXIT official denial",
                "decisions": ["exit"],
                "delivery_immediate": True,
            },
            immutable=True,
        )
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 3})
        assert len(requests) == 1 and (await repo.get("ana.calls", "call"))["status"] == "CLOSED"
        clock[0] = base + timedelta(minutes=20)
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 2})
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 2})
        assert len(requests) == 1
    suppressed = await repo.list("out.deliveries", filters={"alert_id": "a", "version": 2})
    assert len(suppressed) == 1 and suppressed[0]["suppressed"]
    import json

    assert json.loads(requests[0].content)["reply_parameters"] == {"message_id": 42}


@pytest.mark.asyncio
async def test_normal_update_sends_at_fifteen_minutes_and_is_idempotent(monkeypatch):
    from datetime import timedelta

    import tank.delivery as delivery_module
    from tank.delivery import DeliveryDeferred
    from tank.util import iso, utcnow

    base = utcnow()
    clock = [base + timedelta(minutes=2)]
    monkeypatch.setattr(delivery_module, "utcnow", lambda: clock[0])
    repo = MemoryRepository()
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": "e",
            "kind": "FULL",
            "generated_at": iso(base),
            "card_text": "Original",
            "decisions": [],
        },
        immutable=True,
    )
    await repo.put(
        "out.deliveries",
        "original",
        {
            "alert_id": "a",
            "version": 1,
            "channel": "telegram:alerts",
            "status": "SENT",
            "message_ref": "42",
            "attempt": 1,
            "at": iso(base),
        },
        immutable=True,
    )
    await repo.put(
        "out.alerts",
        "a:2",
        {
            "alert_id": "a",
            "version": 2,
            "event_id": "e",
            "kind": "UPDATE",
            "generated_at": iso(clock[0]),
            "card_text": "Material update",
            "decisions": [],
            "delivery_not_before": iso(base + timedelta(minutes=15)),
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2")
    requests = []

    def send(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 43}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        with pytest.raises(DeliveryDeferred):
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 2})
        clock[0] = base + timedelta(minutes=15)
        service = DeliveryService(repo, settings, http=http)
        await service.deliver({"alert_id": "a", "version": 2})
        await service.deliver({"alert_id": "a", "version": 2})
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cause", [ValueError("invalid frozen decision"), TimeoutError(), "EXPIRED", "RESTART_EXHAUSTED"]
)
async def test_terminal_failure_is_durable_once_and_never_opens_calls(cause):
    repo = await setup_repo()
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: pytest.fail("Unexpected send"))
    ) as http:
        service = DeliveryService(repo, settings, http=http)
        await service.terminal_failure({"alert_id": "a", "version": 1}, cause, 2)
        await service.terminal_failure({"alert_id": "a", "version": 1}, cause, 2)
    ledger = await repo.list("out.deliveries")
    assert len(ledger) == 1 and ledger[0]["terminal"] and ledger[0]["status"] == "FAILED"
    assert len(await repo.list("out.ops_notices")) == 1
    assert len(await repo.list("ops.jobs")) == 1
    assert await repo.list("ana.calls") == []


@pytest.mark.asyncio
async def test_terminal_ops_failure_never_recurses_and_sent_is_preserved():
    repo = await setup_repo()
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})
        )
    ) as http:
        service = DeliveryService(repo, settings, http=http)
        await service.deliver({"alert_id": "a", "version": 1})
        await service.terminal_failure({"alert_id": "a", "version": 1}, "EXPIRED", 5)
        await service.terminal_failure({"notice_id": "ops"}, TimeoutError(), 5)
    assert not await repo.list("out.ops_notices")
    assert not await repo.list("ops.jobs")
    assert len(await repo.list("out.deliveries", filters={"alert_id": "a"})) == 2
    assert (await repo.list("out.deliveries", filters={"notice_id": "ops"}))[0]["terminal"]


@pytest.mark.asyncio
async def test_fifth_http_failure_records_terminal_and_single_notice():
    repo = await setup_repo()
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(500, json={"ok": False}))
    ) as http:
        service = DeliveryService(repo, settings, http=http)
        for _ in range(5):
            with pytest.raises(DeliveryError):
                await service.deliver({"alert_id": "a", "version": 1})
        await service.terminal_failure({"alert_id": "a", "version": 1}, "EXHAUSTED", 5)
    assert len([r for r in await repo.list("out.deliveries") if r.get("terminal")]) == 1
    assert len(await repo.list("out.ops_notices")) == 1
    assert len(await repo.list("ops.jobs")) == 1


@pytest.mark.asyncio
async def test_delivery_uses_unbounded_iterator_for_exact_sent_idempotence(monkeypatch):
    repo = await setup_repo()
    await repo.put(
        "out.deliveries",
        "sent",
        {
            "alert_id": "a",
            "version": 1,
            "channel": "telegram:alerts",
            "status": "SENT",
            "message_ref": "42",
            "attempt": 1,
            "at": "2026-10-17T00:00:00+00:00",
        },
        immutable=True,
    )

    async def capped_list(*args, **kwargs):
        pytest.fail("Delivery lookup used capped list instead of repository iterator")

    monkeypatch.setattr(repo, "list", capped_list)
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: pytest.fail("Duplicate send"))
    ) as http:
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 1})


@pytest.mark.asyncio
@pytest.mark.parametrize("current_resolution", ["resolution-1", "resolution-2"])
async def test_milestone_resolution_consumed_only_on_matching_sent_snapshot(current_resolution):
    repo = MemoryRepository()
    history = [{"resolution_id": "resolution-1"}, {"resolution_id": "resolution-2"}]
    await repo.put(
        "evt.events",
        "e",
        {
            "event_id": "e",
            "milestone_resolved": True,
            "milestone_resolution_id": current_resolution,
            "milestone_resolutions": history,
            "next_milestone": None,
        },
    )
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": "e",
            "card_text": "Milestone resolved",
            "decisions": [],
            "milestone_resolution_id": "resolution-1",
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(500, json={"ok": False}))
    ) as http:
        with pytest.raises(DeliveryError):
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 1})
    assert (await repo.get("evt.events", "e"))["milestone_resolved"] is True
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})
        )
    ) as http:
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 1})
    event = await repo.get("evt.events", "e")
    assert event["milestone_resolved"] is (current_resolution != "resolution-1")
    assert event["milestone_resolution_id"] == current_resolution
    assert event["milestone_resolutions"] == history
    assert event["next_milestone"] is None


@pytest.mark.asyncio
async def test_first_sent_persists_alerted_until_subsequent_scheduler_transition():
    from tank.scheduler import Scheduler
    from tank.util import iso, utcnow

    repo = await setup_repo()
    await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "ANALYZING", "known_to_us_at": iso()})
    settings = SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})
        )
    ) as http:
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 1})
    assert (await repo.get("evt.events", "e"))["lifecycle"] == "ALERTED"
    await Scheduler(repo, SimpleNamespace(), None, None).lifecycle(utcnow())
    event = await repo.get("evt.events", "e")
    assert event["lifecycle"] == "MONITORING" and event["monitoring_at"] and event["alerted_at"]
