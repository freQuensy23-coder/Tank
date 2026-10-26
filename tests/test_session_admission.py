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
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_input_fixtures import acknowledge_native_input
from native_session_fixtures import created_native_session
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from runtime_failure_fixtures import runtime_failure_fixture
from session_followup_fixtures import UnexpectedFollowupRefusals

from tank.agents import SessionService
from tank.agents.context import CONTEXT_VERSION
from tank.contracts.session_admission import AdmissionCandidate
from tank.openma_sync import agent_tools
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import clock_iso, parse_time, sha256, utcnow


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


class AdmissionSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)
    admission_cap: int

    def load_config(self, name: str) -> dict[str, JsonValue]:
        if name == "registry":
            return {"tokens": [], "entities": [], "instruments": []}
        if name == "sources":
            return {"sources": [{"source_id": "S01", "name": "Synthetic admission source"}]}
        configuration = super().load_config(name)
        if name == "openma":
            configuration.update(
                agent_id="agt",
                scout_agent_id="agt_s",
                environment_id="env",
                replay_environment_id="env_replay",
                max_running_sessions=self.admission_cap,
            )
        return configuration


def settings(cap: int = 2) -> AdmissionSettings:
    return AdmissionSettings(
        admission_cap=cap,
        ops_progress_enabled=False,
        openma_api_key="synthetic-admission-oma-key",
        openma_url="http://openma:8787",
        openma_model="gpt-6.1-sol",
        database_url="postgresql://fixture:synthetic-admission-db-key@database/fixture",
        config_version="test",
        code_version="test",
        replay=False,
    )


async def seed(repo) -> None:
    at = clock_iso()
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
                "model_context_version": CONTEXT_VERSION,
                "tools_sha256": sha256(agent_tools(settings(), "tank-event-analyst")),
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
                "message_log": [
                    {
                        "message_id": "m",
                        "remote_event_id": "prior-" + sid,
                        "model_context_version": CONTEXT_VERSION,
                        "payload": {"kind": "NEW_ITEM", "item_id": "i_" + eid},
                        "turn_status": "RUNNING",
                        "retry_count": 0,
                        "body": {"type": "user.message", "content": []},
                    }
                ]
                if state == "RUNNING"
                else [],
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
    backend, monkeypatch: pytest.MonkeyPatch, postgres_template, tmp_path: Path
) -> None:
    async with fixture_repo(backend, postgres_template) as repo:
        await seed(repo)
        before_items = {row["item_id"]: row["observed_at"] for row in await repo.list("ingest.source_items")}
        before_known = {row["event_id"]: row["known_to_us_at"] for row in await repo.list("evt.events")}
        posted = []
        opts = settings()

        def openma(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            body = json.loads(request.content)
            if request.url.path == "/v1/sessions":
                return httpx.Response(
                    200,
                    json=created_native_session(
                        request,
                        "s_new",
                        agent_snapshot={
                            "id": "agt",
                            "version": 1,
                            "model": {"id": opts.openma_model},
                            "system": "Synthetic admission prompt",
                            "skills": [],
                            "tools": agent_tools(opts, "tank-event-analyst"),
                        },
                    ),
                )
            sid = request.url.path.split("/")[3]
            text = json.loads(body["events"][0]["content"][0]["text"])
            posted.append((sid, text["item"]["text"].removeprefix("Item ")))
            return httpx.Response(200, json=acknowledge_native_input(request, "in_" + uuid.uuid4().hex[:6]))

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

        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(openma)) as http,
            runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime,
        ):
            sessions = SessionService(
                repo,
                opts,
                None,
                http=http,
                versions=declared_versions_fixture(),
                refusals=UnexpectedFollowupRefusals(),
            )
            sessions.start_stream = lambda sid: None
            await runtime.sessions.close()
            runtime.sessions = sessions

            inspected_after_new = asyncio.Event()

            reconcile = runtime.reconcile_delivery_failures

            async def observe_reconciliation() -> None:
                await reconcile()
                if (await repo.get("ops.jobs", jobs["new"]))["status"] == "DONE":
                    inspected_after_new.set()

            monkeypatch.setattr(runtime, "reconcile_delivery_failures", observe_reconciliation)
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
    tmp_path: Path,
) -> None:
    repo = MemoryRepository()
    job = await repo.enqueue("AGENT_MESSAGE", {"kind": "NEW_ITEM", "item_id": "i"}, "slow", priority=1)
    expiry = (await repo.get("ops.jobs", job["job_id"]))["expires_at"]
    monkeypatch.setattr("tank.runtime.ADMISSION_TIMEOUT_S", 0.05)
    async with runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime:

        async def slow_admission(candidate: AdmissionCandidate) -> None:
            assert candidate.job_id == job["job_id"]
            await asyncio.Event().wait()

        dispatch = AsyncMock(wraps=runtime.sessions.dispatch_message)
        monkeypatch.setattr(runtime.sessions, "admit_candidate", slow_admission)
        monkeypatch.setattr(runtime.sessions, "dispatch_message", dispatch)
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            await until(lambda: _waiting(repo, {"slow": job["job_id"]}, "slow"))
        finally:
            stop.set()
            await asyncio.wait_for(worker, timeout=1)
        row = await repo.get("ops.jobs", job["job_id"])
        assert row["status"] == "QUEUED" and row["attempts"] == 0 and row["last_error"] is None
        assert row["expires_at"] == expiry
        dispatch.assert_not_awaited()
