"""Tank Ops AGENT_STALL alarm and the heartbeat's queue lag for admission-deferred agent turns (03 §7.6)."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from test_session_admission import fixture_repo, seed, settings, until

from tank.agents import SessionService
from tank.runtime import Runtime
from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.util import iso, parse_time, utcnow

NOW = parse_time("2026-10-27T03:00:00Z")


async def deferred_turn(repo, key, waiting_since, *, now=NOW):
    """An AGENT_MESSAGE the running-session cap keeps deferring, as runtime.AdmissionDeferred leaves it."""
    job = await repo.enqueue("AGENT_MESSAGE", {"kind": "NEW_ITEM", "item_id": key}, key)
    await repo.put(
        "ops.jobs",
        job["job_id"],
        {
            **job,
            "created_at": iso(waiting_since),
            "slot_waiting_since": iso(waiting_since),
            "run_after": iso(now + timedelta(seconds=60)),
        },
    )
    return job


async def busy_session(repo, sid, requested_at):
    await repo.put(
        "ops.agent_sessions",
        sid,
        {
            "session_id": sid,
            "status": "OPEN",
            "runtime_status": "RUNNING",
            "active_message_id": "m-" + sid,
            "message_log": [{"message_id": "m-" + sid, "requested_at": iso(requested_at)}],
        },
    )


async def stall_notices(repo):
    return await repo.list("out.ops_notices", filters={"kind": "AGENT_STALL"})


async def ops_deliveries(repo):
    return [job for job in await repo.list("ops.jobs", filters={"type": "DELIVER"})]


@pytest.mark.asyncio
async def test_heartbeat_lag_counts_admission_wait_not_the_rescheduled_run_after():
    repo = MemoryRepository()
    await deferred_turn(repo, "item-1", NOW - timedelta(hours=19))
    await Scheduler(repo, SimpleNamespace(), None, None).heartbeat(NOW)
    [notice] = await repo.list("out.ops_notices", filters={"kind": "HEARTBEAT"})
    assert notice["queue_lag_s"] == pytest.approx(19 * 3600)
    assert "queue lag=68400s" in notice["text"]


@pytest.mark.asyncio
async def test_heartbeat_lag_ignores_work_scheduled_for_later():
    repo = MemoryRepository()
    await repo.enqueue(
        "DELIVER", {"alert_id": "a", "version": 1}, "later", run_after=NOW + timedelta(minutes=5)
    )
    await Scheduler(repo, SimpleNamespace(), None, None).heartbeat(NOW)
    [notice] = await repo.list("out.ops_notices", filters={"kind": "HEARTBEAT"})
    assert notice["queue_lag_s"] == 0


@pytest.mark.asyncio
async def test_queued_agent_turn_past_threshold_raises_one_alarm_per_stall():
    repo = MemoryRepository()
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    await deferred_turn(repo, "item-1", NOW - timedelta(minutes=16))
    await deferred_turn(repo, "item-2", NOW - timedelta(minutes=3))

    await scheduler.agent_stall(NOW)
    await scheduler.agent_stall(NOW + timedelta(minutes=1))

    [notice] = await stall_notices(repo)
    assert notice["reasons"] == ["QUEUE_WAIT"]
    assert notice["created_by"] == "scheduler"
    assert notice["agent_backlog"]["queued"] == 2
    assert notice["agent_backlog"]["oldest_waiting_since"] == iso(NOW - timedelta(minutes=16))
    assert "Ждут агента: 2; самое старое ждёт 16 мин" in notice["text"]
    [delivery] = await ops_deliveries(repo)
    assert delivery["payload"] == {"notice_id": notice["notice_id"], "channel": "telegram:ops"}
    assert delivery["priority"] == 0 and delivery["max_attempts"] == 5


@pytest.mark.asyncio
async def test_alarm_rearms_after_the_queue_recovers():
    repo = MemoryRepository()
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    job = await deferred_turn(repo, "item-1", NOW - timedelta(minutes=20))
    await scheduler.agent_stall(NOW)
    await repo.put(
        "ops.jobs", job["job_id"], {**(await repo.get("ops.jobs", job["job_id"])), "status": "DONE"}
    )
    await scheduler.agent_stall(NOW + timedelta(minutes=1))
    assert len(await stall_notices(repo)) == 1

    later = NOW + timedelta(hours=1)
    await deferred_turn(repo, "item-2", later - timedelta(minutes=30), now=later)
    await scheduler.agent_stall(later)

    notices = await stall_notices(repo)
    assert len(notices) == 2
    assert len(await ops_deliveries(repo)) == 2


@pytest.mark.asyncio
async def test_busy_session_without_agent_model_calls_raises_alarm():
    repo = MemoryRepository()
    await busy_session(repo, "s-1", NOW - timedelta(minutes=40))
    await repo.put("ops.llm_usage", "old", {"at": iso(NOW - timedelta(minutes=41)), "call": "AGENT"})
    await repo.put("ops.llm_usage", "route", {"at": iso(NOW - timedelta(minutes=1)), "call": "ROUTE"})

    await Scheduler(repo, SimpleNamespace(), None, None).agent_stall(NOW)

    [notice] = await stall_notices(repo)
    assert notice["reasons"] == ["LLM_SILENCE"]
    assert notice["agent_backlog"]["busy_sessions"] == 1
    assert notice["agent_backlog"]["agent_calls"] == 0


@pytest.mark.asyncio
async def test_working_analyst_and_short_waits_raise_no_alarm():
    repo = MemoryRepository()
    await busy_session(repo, "s-1", NOW - timedelta(minutes=40))
    await busy_session(repo, "s-2", NOW - timedelta(minutes=2))
    await repo.put("ops.llm_usage", "recent", {"at": iso(NOW - timedelta(minutes=4)), "call": "AGENT"})
    await deferred_turn(repo, "item-1", NOW - timedelta(minutes=14))

    await Scheduler(repo, SimpleNamespace(), None, None).agent_stall(NOW)

    assert await stall_notices(repo) == []
    assert await ops_deliveries(repo) == []


@pytest.mark.asyncio
async def test_alarm_threshold_comes_from_configuration():
    repo = MemoryRepository()
    settings = SimpleNamespace(load_config=lambda name: {"agent_stall_alarm_min": 60})
    await deferred_turn(repo, "item-1", NOW - timedelta(minutes=30))
    await Scheduler(repo, settings, None, None).agent_stall(NOW)
    assert await stall_notices(repo) == []


@pytest.mark.asyncio
async def test_scheduler_tick_runs_the_stall_check():
    repo = MemoryRepository()
    scheduler = Scheduler(repo, SimpleNamespace(), None, None)
    for name in ("expire_calls", "demote", "lifecycle", "heartbeat", "maintenance", "detect_scout_hooks"):
        setattr(scheduler, name, AsyncMock())
    await deferred_turn(repo, "item-1", parse_time(iso()) - timedelta(minutes=30))
    try:
        await scheduler.tick()
        await scheduler.background["agent-stall"]
    finally:
        await scheduler.close()
    assert len(await stall_notices(repo)) == 1


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_wedged_sessions_behind_the_cap_raise_alarm_and_heartbeat_lag(backend, postgres_template):
    async with fixture_repo(backend, postgres_template) as repo:
        await seed(repo)
        wedged_at = utcnow() - timedelta(minutes=5)
        for sid in ("s1", "s2"):
            session = await repo.get("ops.agent_sessions", sid)
            session["message_log"] = [{"message_id": "m", "requested_at": iso(wedged_at)}]
            await repo.put("ops.agent_sessions", sid, session)

        def openma(request):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            if request.url.path == "/v1/sessions":
                return httpx.Response(200, json={"id": "s_new"})
            return httpx.Response(200, json={"data": [{"id": "in_1"}]})

        job = await repo.enqueue("AGENT_MESSAGE", {"kind": "NEW_ITEM", "item_id": "i_new"}, "new", priority=1)
        async with httpx.AsyncClient(transport=httpx.MockTransport(openma)) as http:
            sessions = SessionService(repo, settings(), None, http=http)
            sessions.start_stream = lambda sid: None
            runtime = object.__new__(Runtime)
            runtime.repo, runtime.sessions, runtime.active_jobs = repo, sessions, set()
            runtime.handlers = {"AGENT_MESSAGE": sessions.send_message}
            runtime.reconcile_delivery_failures = AsyncMock()
            stop = asyncio.Event()
            worker = asyncio.create_task(runtime.worker(stop))

            async def deferred():
                row = await repo.get("ops.jobs", job["job_id"])
                return row["status"] == "QUEUED" and bool(row.get("slot_waiting_since"))

            try:
                await until(deferred)
            finally:
                stop.set()
                await asyncio.wait_for(worker, timeout=1)

        deferred_job = await repo.get("ops.jobs", job["job_id"])
        waiting_since = parse_time(deferred_job["slot_waiting_since"])
        later = waiting_since + timedelta(minutes=20)
        scheduler = Scheduler(repo, SimpleNamespace(), None, None)
        await scheduler.heartbeat(later)
        await scheduler.agent_stall(later)
        await scheduler.agent_stall(later + timedelta(minutes=1))

        [heartbeat] = await repo.list("out.ops_notices", filters={"kind": "HEARTBEAT"})
        assert heartbeat["queue_lag_s"] == pytest.approx(1200, abs=1)
        [notice] = await stall_notices(repo)
        assert notice["reasons"] == ["QUEUE_WAIT", "LLM_SILENCE"]
        assert notice["agent_backlog"]["queued"] == 1
        assert notice["agent_backlog"]["busy_sessions"] == 2
        deliveries = [row for row in await ops_deliveries(repo) if row["idem_key"] == notice["notice_id"]]
        assert len(deliveries) == 1
