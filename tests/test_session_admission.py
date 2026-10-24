"""Running-session cap (user decision 2026-10-20): at most N sessions run a turn at once.

Turns beyond the cap wait in order and are never dropped; a message to a session that
is already running is not blocked. Exercised through the real worker and job queue.
"""

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest

from tank.agents import SessionService
from tank.runtime import Runtime
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import iso, parse_time, sha256, utcnow


@asynccontextmanager
async def fixture_repo(backend, postgres_template):
    if backend == "memory":
        yield MemoryRepository()
        return
    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Disposable administrator PostgreSQL DSN required")
    async with postgres_template.repository(url) as repo:
        await repo.migrate()
        yield repo


def settings(cap=2):
    config = {
        "openma": {
            "agent_id": "agt",
            "scout_agent_id": "agt_s",
            "environment_id": "env",
            "prompt_version": "test",
            "max_running_sessions": cap,
        }
    }
    return SimpleNamespace(
        ops_progress_enabled=False,
        openma_api_key="test",
        openma_url="http://openma:8787",
        openma_model="gpt-6.1-sol",
        config_version="test",
        code_version="test",
        replay=False,
        load_config=lambda name: config[name],
    )


async def seed(repo) -> None:
    at = iso()
    for item in ("i_new", "i_idle", "i_busy", "i_e1", "i_e2", "i_e3"):
        await repo.put(
            "ingest.source_items",
            item,
            {
                "item_id": item,
                "source_id": "S01",
                "feed": "fixture",
                "external_id": item,
                "text": "Item " + item,
                "source_class": "PRIMARY",
                "observed_at": at,
                "observed_at_prec": "COLLECTION_ONLY",
                "published_at": at,
                "published_prec": "EXACT",
                "raw_ref": "/fixture/raw",
                "raw_sha256": sha256(item),
                "content_hash": sha256(item),
                "relevance": {"decision": "ALWAYS"},
                "extraction_status": "PENDING",
            },
        )
    for eid in ("e1", "e2", "e3"):
        await repo.put(
            "evt.events",
            eid,
            {
                "event_id": eid,
                "statement": "Event " + eid,
                "lifecycle": "TRACKED",
                "event_state": "LIVE",
                "state_verified": True,
                "certainty": "HIGH",
                "best_source_class": "PRIMARY",
                "freshness": "FRESH",
                "known_to_us_at": at,
                "known_to_us_at_prec": "COLLECTION_ONLY",
                "public_at": at,
                "public_at_prec": "EXACT",
                "public_at_basis": "Fixture",
                "theme_tags": [],
                "timing_confidence": "HIGH",
                "entities": [],
                "tokens_mentioned": [],
            },
        )
    for sid, eid, state in (("s1", "e1", "RUNNING"), ("s2", "e2", "RUNNING"), ("s3", "e3", "IDLE")):
        await repo.put(
            "ops.agent_sessions",
            sid,
            {
                "session_id": sid,
                "event_id": eid,
                "status": "OPEN",
                "runtime_status": state,
                "active_message_id": "m" if state == "RUNNING" else None,
                "opened_by_item": "i_" + eid,
                "opened_at": at,
                "opened_at_prec": "COLLECTION_ONLY",
                "last_item_at": at,
                "last_item_at_prec": "COLLECTION_ONLY",
                "last_event_seq": 0,
                "agent_version": "1",
                "prompt_version": "a" * 64,
                "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
                "model_id": "gpt-6.1-sol",
                "messages": [],
                "message_log": [],
            },
        )


async def until(predicate, timeout=3) -> None:
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.05)


async def release(repo, sid) -> None:
    session = await repo.get("ops.agent_sessions", sid)
    await repo.put(
        "ops.agent_sessions", sid, {**session, "runtime_status": "IDLE", "active_message_id": None}
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_cap_queues_turns_in_order_never_drops_and_running_session_bypasses(
    backend, monkeypatch: pytest.MonkeyPatch, postgres_template
) -> None:
    async with fixture_repo(backend, postgres_template) as repo:
        await seed(repo)
        before_items = {row["item_id"]: row["observed_at"] for row in await repo.list("ingest.source_items")}
        before_known = {row["event_id"]: row["known_to_us_at"] for row in await repo.list("evt.events")}
        posted = []

        def openma(request: pytest.FixtureRequest):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            body = json.loads(request.content)
            if request.url.path == "/v1/sessions":
                return httpx.Response(200, json={"id": "s_new"})
            sid = request.url.path.split("/")[3]
            text = json.loads(body["events"][0]["content"][0]["text"])
            posted.append((sid, text["item"]["text"].removeprefix("Item ")))
            return httpx.Response(200, json={"data": [{"id": "in_" + uuid.uuid4().hex[:6]}]})

        jobs = {}
        enqueued_at = utcnow() - timedelta(seconds=1)
        for index, (name, payload) in enumerate(
            (
                ("new", {"kind": "NEW_ITEM", "item_id": "i_new"}),
                ("idle", {"kind": "ITEM_ADDED", "item_id": "i_idle", "event_id": "e3"}),
                ("busy", {"kind": "ITEM_ADDED", "item_id": "i_busy", "event_id": "e1"}),
            )
        ):
            queued_at = enqueued_at + timedelta(milliseconds=index)
            with monkeypatch.context() as clock:
                clock.setattr(
                    "tank.storage.collection_clock", lambda queued_at=queued_at: collection_clock(queued_at)
                )
                jobs[name] = (await repo.enqueue("AGENT_MESSAGE", payload, name, priority=1))["job_id"]
        expiries = {name: (await repo.get("ops.jobs", job))["expires_at"] for name, job in jobs.items()}

        async with httpx.AsyncClient(transport=httpx.MockTransport(openma)) as http:
            sessions = SessionService(repo, settings(), None, http=http)
            sessions.start_stream = lambda sid: None
            runtime = object.__new__(Runtime)
            runtime.repo, runtime.sessions, runtime.active_jobs = repo, sessions, set()
            runtime.handlers = {"AGENT_MESSAGE": sessions.send_message}

            inspected_after_new = asyncio.Event()

            async def no_delivery() -> None:

                if (await repo.get("ops.jobs", jobs["new"]))["status"] == "DONE":
                    inspected_after_new.set()

            runtime.reconcile_delivery_failures = no_delivery
            stop = asyncio.Event()
            worker = asyncio.create_task(runtime.worker(stop))

            async def status(name):
                return (await repo.get("ops.jobs", jobs[name]))["status"]

            try:
                await until(lambda: _done(status, "busy"))
                await until(lambda: _waiting(repo, jobs, "new", "idle"))
                assert posted == [("s1", "i_busy")]
                for name in ("new", "idle"):
                    job = await repo.get("ops.jobs", jobs[name])
                    assert job["status"] == "QUEUED" and job["attempts"] == 0 and job["last_error"] is None
                    assert job["expires_at"] == expiries[name]

                    assert parse_time(job["run_after"]) > utcnow() + timedelta(seconds=30)

                await release(repo, "s1")
                await sessions.wake_waiting()
                await until(lambda: _done(status, "new"))
                await asyncio.wait_for(inspected_after_new.wait(), timeout=1)
                assert await status("idle") == "QUEUED"
                assert posted[1] == ("s_new", "i_new")

                await release(repo, "s2")
                await sessions.wake_waiting()
                await until(lambda: _done(status, "idle"))
                assert posted == [("s1", "i_busy"), ("s_new", "i_new"), ("s3", "i_idle")]
            finally:
                stop.set()
                await asyncio.wait_for(worker, timeout=1)
        assert all(row["status"] == "DONE" for row in await repo.list("ops.jobs"))
        after_items = {row["item_id"]: row["observed_at"] for row in await repo.list("ingest.source_items")}
        assert after_items == before_items
        assert {
            row["event_id"]: row["known_to_us_at"] for row in await repo.list("evt.events")
        } == before_known


async def _done(status, name):
    return await status(name) == "DONE"


async def _waiting(repo, jobs, *names):
    rows = [await repo.get("ops.jobs", jobs[name]) for name in names]
    return all(row["status"] == "QUEUED" and row.get("slot_waiting_since") for row in rows)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_slow_admission_check_waits_without_spending_an_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = MemoryRepository()
    job = await repo.enqueue("AGENT_MESSAGE", {"kind": "NEW_ITEM", "item_id": "i"}, "slow", priority=1)
    expiry = (await repo.get("ops.jobs", job["job_id"]))["expires_at"]
    monkeypatch.setattr("tank.runtime.ADMISSION_TIMEOUT_S", 0.05)
    handled = []

    class SlowSessions:
        async def admit(self, job) -> None:
            await asyncio.Event().wait()

    async def handler(payload) -> None:
        handled.append(payload)

    runtime = object.__new__(Runtime)
    runtime.repo, runtime.sessions, runtime.active_jobs = repo, SlowSessions(), set()
    runtime.handlers = {"AGENT_MESSAGE": handler}

    async def no_delivery() -> None:
        return None

    runtime.reconcile_delivery_failures = no_delivery
    stop = asyncio.Event()
    worker = asyncio.create_task(runtime.worker(stop))
    try:
        await until(lambda: _waiting(repo, {"slow": job["job_id"]}, "slow"))
    finally:
        stop.set()
        await asyncio.wait_for(worker, timeout=1)
    row = await repo.get("ops.jobs", job["job_id"])
    assert row["status"] == "QUEUED" and row["attempts"] == 0 and row["last_error"] is None
    assert row["expires_at"] == expiry and handled == []
