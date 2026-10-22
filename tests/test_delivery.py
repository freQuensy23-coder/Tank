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
async def test_receipt_beyond_first_ledger_page_prevents_duplicate_delivery():
    repo = await setup_repo()
    subject = {"alert_id": "a", "version": 1, "channel": "telegram:alerts"}
    for index in range(1000):
        await repo.put(
            "out.deliveries",
            f"history-{index:04}",
            {**subject, "status": "GENERATED", "attempt": 1, "at": "2026-10-17T00:00:00Z"},
            immutable=True,
        )
    receipt = {
        **subject,
        "status": "SENT",
        "attempt": 2,
        "message_ref": "42",
        "at": "2026-10-17T00:00:01Z",
    }
    await repo.put("out.deliveries", "zz-sent", receipt, immutable=True)
    ledger = await repo.list("out.deliveries", filters=subject, limit=2000)
    frozen_alert = await repo.get("out.alerts", "a:1")
    assert len(ledger) == 1001
    requests = []

    def send(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 99}})

    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        service = DeliveryService(repo, settings, http=http)
        await service.deliver({"alert_id": "a", "version": 1})
    assert requests == []
    assert await repo.list("out.deliveries", filters=subject, limit=2000) == ledger
    assert await repo.get("out.deliveries", "zz-sent") == receipt
    assert await repo.get("out.alerts", "a:1") == frozen_alert
    assert await repo.list("ana.calls") == []
    assert await repo.list("out.ops_notices") == []
    assert await repo.list("ops.jobs") == []


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
            "delivery_immediate": True,
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
            "delivery_immediate": True,
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
                "delivery_immediate": True,
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
async def test_held_update_is_never_sent_after_a_newer_immediate_exit(monkeypatch):
    """W2: a held DON'T CHASE update must not reach the operator after the EXIT
    that superseded it was sent at once."""
    import json
    from datetime import timedelta

    import tank.delivery as delivery_module
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
        for _ in range(2):
            with pytest.raises(DeliveryError) as exc:
                await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 2})
            assert exc.value.retry_after == 13 * 60
        assert not requests
        assert not await repo.list("out.deliveries", filters={"alert_id": "a", "version": 2})
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
        assert json.loads(requests[0].content)["text"] == "<b>EXIT official denial</b>"
    assert not await repo.list("out.deliveries", filters={"alert_id": "a", "version": 2})
    assert all(json.loads(request.content)["reply_parameters"] == {"message_id": 42} for request in requests)
    assert (await repo.get("ana.calls", "call"))["status"] == "CLOSED"


@pytest.mark.asyncio
async def test_normal_update_sends_at_fifteen_minutes_and_is_idempotent(monkeypatch):
    from datetime import timedelta

    import tank.delivery as delivery_module
    from tank.util import iso, utcnow

    base = utcnow()
    clock = [base + timedelta(minutes=15)]
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


@pytest.mark.integration
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


@pytest.mark.asyncio
async def test_explicit_ordinary_update_flag_preserves_rate_with_persistent_safety():
    from datetime import timedelta

    from tank.util import iso, utcnow

    repo = MemoryRepository()
    await repo.put("evt.events", "e", {"event_id": "e"})
    await repo.put(
        "ana.decisions", "exit", {"dec_id": "exit", "event_id": "e", "token_id": "T", "action": "EXIT"}
    )
    await repo.put(
        "out.alerts",
        "a:1",
        {"alert_id": "a", "version": 1, "event_id": "e", "card_text": "Denial", "decisions": ["exit"]},
        immutable=True,
    )
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
            "at": iso(),
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
            "card_text": "Class changed",
            "decisions": ["exit"],
            "delivery_immediate": False,
            "delivery_not_before": iso(utcnow() + timedelta(minutes=15)),
            "card_struct": {"event": {"contradictions": [{"type": "FATAL"}]}},
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: pytest.fail("Ordinary update sent early"))
    ) as http:
        with pytest.raises(DeliveryError) as exc:
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 2})
        assert 14 * 60 < exc.value.retry_after <= 15 * 60
    assert not await repo.list("out.deliveries", filters={"alert_id": "a", "version": 2})


@pytest.mark.asyncio
@pytest.mark.parametrize("precision", ["BUCKET(1m)", None])
async def test_delivery_ledger_and_calls_keep_source_precision_and_actual_clocks(precision):
    repo = await setup_repo()

    alert = {
        "alert_id": "precision",
        "version": 1,
        "event_id": "e",
        "card_text": "Fixture",
        "decisions": ["d"],
        "generated_at": "2026-10-17T10:00:00Z",
    }
    if precision:
        alert["generated_at_prec"] = precision
    await repo.put("out.alerts", "precision:1", alert, immutable=True)
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})
        )
    ) as http:
        await DeliveryService(repo, settings, http=http).deliver({"alert_id": "precision", "version": 1})
    ledger = await repo.list("out.deliveries", filters={"alert_id": "precision"})
    sent = next(row for row in ledger if row["status"] == "SENT")
    assert sent["generated_at"] == alert["generated_at"]
    assert sent["generated_at_prec"] == precision
    if precision is None:
        assert sent["generated_at_prec_reason"] == "INCOMPLETE"
    assert all(
        row["at_prec"] == "COLLECTION_ONLY" and row["attempted_at_prec"] == "COLLECTION_ONLY"
        for row in ledger
    )
    assert sent["sent_at_prec"] == "COLLECTION_ONLY" and sent["sent_at"] == sent["at"]
    call = (await repo.list("ana.calls"))[0]
    assert call["opened_at"] == sent["sent_at"] and call["opened_at_prec"] == "COLLECTION_ONLY"
    assert call["alert_generated_at_prec"] == precision
    assert call["alert_generated_at"] == alert["generated_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize("precision", ["BUCKET(1m)", None])
async def test_delivery_due_guard_preserves_sent_anchor_and_records_no_attempt(precision):
    from datetime import timedelta

    from tank.util import iso, utcnow

    repo = MemoryRepository()
    await repo.put(
        "out.alerts",
        "p:1",
        {"alert_id": "p", "version": 1, "event_id": "e", "card_text": "Original", "decisions": []},
        immutable=True,
    )
    stamp = {
        "alert_id": "p",
        "version": 1,
        "channel": "telegram:alerts",
        "status": "SENT",
        "message_ref": "42",
        "attempt": 1,
        "at": iso(),
    }
    if precision:
        stamp["at_prec"] = precision
    await repo.put("out.deliveries", "sent", stamp, immutable=True)
    await repo.put(
        "out.alerts",
        "p:2",
        {
            "alert_id": "p",
            "version": 2,
            "event_id": "e",
            "kind": "UPDATE",
            "card_text": "Update",
            "decisions": [],
            "delivery_immediate": False,
            "delivery_not_before": iso(utcnow() + timedelta(minutes=15)),
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: pytest.fail("Sent early"))) as http:
        with pytest.raises(DeliveryError) as exc:
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "p", "version": 2})
        assert 14 * 60 < exc.value.retry_after <= 15 * 60
    assert await repo.get("out.deliveries", "sent") == stamp
    assert not await repo.list("out.deliveries", filters={"alert_id": "p", "version": 2})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "freshness,event_class,legacy_rate_limited,reason",
    [
        ("STALE", "MAJOR", False, "STALE"),
        ("FRESH", "LOW", False, "BELOW_MAJOR"),
        ("FRESH", "MATERIAL", False, "BELOW_MAJOR"),
        ("FRESH", "MAJOR", True, "UPDATE_RATE_LIMIT"),
    ],
)
async def test_accepted_immutable_card_is_not_semantically_recensored_by_delivery(
    freshness, event_class, legacy_rate_limited, reason
):
    repo = MemoryRepository()
    await repo.put(
        "out.alerts",
        "legacy:1",
        {
            "alert_id": "legacy",
            "version": 1,
            "event_id": "event",
            "kind": "UPDATE",
            "card_text": "Historical candidate",
            "decisions": [],
            "rate_limited": legacy_rate_limited,
            "delivery_immediate": False,
            "card_struct": {"event": {"freshness": freshness}, "assessment": {"class": event_class}},
        },
        immutable=True,
    )
    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})
        )
    ) as http:
        for _ in range(2):
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "legacy", "version": 1})
    ledger = await repo.list("out.deliveries")
    assert [row["status"] for row in ledger] == ["GENERATED", "SENT"]
    assert all(not row.get("suppressed") for row in ledger)
    assert not await repo.list("ops.jobs")
    assert not await repo.list("ana.calls")


@pytest.mark.asyncio
async def test_historical_suppression_is_preserved_without_censoring_explicit_retry():
    repo = await setup_repo()
    suppressed = {
        "alert_id": "a",
        "version": 1,
        "channel": "telegram:alerts",
        "status": "FAILED",
        "suppressed": True,
        "suppression_reason": "STALE",
        "attempt": 0,
    }
    await repo.put("out.deliveries", "old-suppression", suppressed, immutable=True)
    requests = []

    def send(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

    settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        for _ in range(2):
            await DeliveryService(repo, settings, http=http).deliver({"alert_id": "a", "version": 1})
    assert len(requests) == 1
    assert await repo.get("out.deliveries", "old-suppression") == suppressed
    assert len(await repo.list("out.deliveries", filters={"status": "SENT"})) == 1
    assert len(await repo.list("ana.calls", filters={"status": "OPEN"})) == 1
    assert not await repo.list("ops.jobs")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_newer_held_update_supersedes_older_and_keeps_spacing_through_restart(
    monkeypatch, backend, postgres_template
):
    """W2 (03 §7.4, FR-A-04): every version carries the whole card, so an older
    held update is never sent once a newer version exists; the newest one waits
    for its own slot and survives a restart."""
    import copy
    import json
    import os
    from contextlib import AsyncExitStack
    from datetime import timedelta

    from tank.storage import PostgresRepository
    from tank.util import iso, new_id, sha256, utcnow

    async with AsyncExitStack() as repository_lifetime:
        if backend == "postgres":
            database_url = os.getenv("TANK_TEST_DATABASE_URL")
            if not database_url:
                pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
            repo = await repository_lifetime.enter_async_context(postgres_template.repository(database_url))
            database_url = repo.database_url
        else:
            repo = await MemoryRepository().open()
        base = utcnow() - timedelta(hours=1)
        clock = [base + timedelta(minutes=25)]
        monkeypatch.setattr("tank.delivery.utcnow", lambda: clock[0])
        aid = new_id("alert")
        event_id = new_id("event")
        settings = SimpleNamespace(telegram_token="fixture", telegram_alerts_chat="1", telegram_ops_chat="2")
        requests = []

        def send(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 42 + len(requests)}})

        try:
            await repo.put(
                "evt.events",
                event_id,
                {
                    "event_id": event_id,
                    "statement": "Synthetic accepted event",
                    "theme_tags": [],
                    "tokens_mentioned": [],
                    "public_at": iso(base),
                    "public_at_prec": "EXACT",
                    "public_at_basis": "Synthetic fixture timestamp",
                    "known_to_us_at": iso(base),
                    "freshness": "FRESH",
                    "event_state": "APPROVED",
                    "state_verified": True,
                    "certainty": "HIGH",
                    "timing_confidence": "HIGH",
                    "best_source_class": "PRIMARY",
                    "lifecycle": "MONITORING",
                },
            )
            digest = sha256({"fixture": aid})
            await repo.put(
                "out.evidence_bundles",
                digest,
                {"bundle_id": "EVB-" + digest, "sha256": digest, "payload": {"fixture": aid}},
                immutable=True,
            )
            for version, due in [(1, 0), (2, 15), (3, 30)]:
                await repo.put(
                    "out.alerts",
                    f"{aid}:{version}",
                    {
                        "alert_id": aid,
                        "version": version,
                        "event_id": event_id,
                        "kind": "FULL" if version == 1 else "UPDATE",
                        "card_text": f"Accepted immutable version {version}",
                        "decisions": [],
                        "card_struct": {"event": {"thresholds": {"update_rate_limit_s": 900}}},
                        "evidence_bundle_id": digest,
                        "config_version": "fixture",
                        "code_version": "fixture",
                        "delivery_immediate": False,
                        "generated_at": iso(base + timedelta(minutes=version)),
                        "delivery_not_before": iso(base + timedelta(minutes=due)),
                    },
                    immutable=True,
                )
            await repo.put(
                "out.deliveries",
                new_id("delivery"),
                {
                    "alert_id": aid,
                    "version": 1,
                    "channel": "telegram:alerts",
                    "status": "SENT",
                    "message_ref": "42",
                    "attempt": 1,
                    "at": iso(base),
                    "at_prec": "COLLECTION_ONLY",
                },
                immutable=True,
            )
            frozen = copy.deepcopy(await repo.get("out.alerts", f"{aid}:3"))
            async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
                await DeliveryService(repo, settings, http=http).deliver({"alert_id": aid, "version": 2})
                assert requests == []
                assert not await repo.list("out.deliveries", filters={"alert_id": aid, "version": 2})
                with pytest.raises(DeliveryError) as deferred:
                    await DeliveryService(repo, settings, http=http).deliver({"alert_id": aid, "version": 3})
                assert deferred.value.retry_after == 5 * 60
                assert requests == []
                if backend == "postgres":
                    await repo.close()
                    repo = PostgresRepository(database_url)
                    await repo.open()
                clock[0] = base + timedelta(minutes=30)
                await DeliveryService(repo, settings, http=http).deliver({"alert_id": aid, "version": 3})
                await DeliveryService(repo, settings, http=http).deliver({"alert_id": aid, "version": 3})
                await DeliveryService(repo, settings, http=http).deliver({"alert_id": aid, "version": 2})
                assert len(requests) == 1
                assert requests[0]["text"] == "<b>Accepted immutable version 3</b>"
                assert requests[0]["reply_parameters"] == {"message_id": 42}
                sent = await repo.list("out.deliveries", filters={"alert_id": aid, "status": "SENT"})
                assert [row["version"] for row in sorted(sent, key=lambda row: row["at"])] == [1, 3]
                assert await repo.get("out.alerts", f"{aid}:3") == frozen
        finally:
            await repo.close()
