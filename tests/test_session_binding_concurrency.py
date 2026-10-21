"""Actual controller/domain writers must share the session binding boundary."""

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlsplit, urlunsplit

import httpx
import pytest
from test_agents_execution_barrier import config

from tank.agents.sessions import SessionService
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.util import iso, sha256


@asynccontextmanager
async def fixture_repo(backend):
    admin = None
    if backend == "postgres":
        import psycopg
        from psycopg import sql

        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Disposable administrator PostgreSQL DSN required")
        database = "tank_binding_" + uuid.uuid4().hex[:12]
        admin = await psycopg.AsyncConnection.connect(url, autocommit=True)
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        repo = await PostgresRepository(urlunsplit(urlsplit(url)._replace(path="/" + database))).open()
    else:
        repo = MemoryRepository()
    try:
        if admin:
            await repo.migrate()
        yield repo
    finally:
        await repo.close()
        if admin:
            from psycopg import sql

            await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
            await admin.close()


async def seed(repo):
    at = iso()
    await repo.put(
        "ingest.source_items",
        "i",
        {
            "item_id": "i",
            "source_id": "S01",
            "feed": "fixture",
            "external_id": "i",
            "text": "Product announced",
            "source_class": "PRIMARY",
            "observed_at": at,
            "observed_at_prec": "COLLECTION_ONLY",
            "published_at": at,
            "published_prec": "EXACT",
            "raw_ref": "/fixture/raw",
            "raw_sha256": sha256("Product announced"),
            "content_hash": sha256("Product announced"),
            "relevance": {"decision": "ALWAYS"},
            "extraction_status": "PENDING",
        },
    )
    session = {
        "session_id": "s",
        "event_id": None,
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "opened_by_item": "i",
        "opened_at": at,
        "opened_at_prec": "COLLECTION_ONLY",
        "last_item_at": at,
        "last_item_at_prec": "COLLECTION_ONLY",
        "last_event_seq": 0,
        "agent_version": "1",
        "prompt_version": "a" * 64,
        "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
        "model_id": "gpt-6.1-sol",
        "active_message_id": "m",
        "message_log": [
            {
                "message_id": "m",
                "remote_event_id": "input",
                "retry_count": 0,
                "payload": {"kind": "NEW_ITEM", "item_id": "i"},
            }
        ],
    }
    await repo.put("ops.agent_sessions", "s", session)
    args = {
        "item_id": "i",
        "is_event": True,
        "statement": "Product announced",
        "certainty": "MEDIUM",
        "certainty_reason": "Fixture",
        "entities": [],
        "tokens": [],
        "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
        "public_at": {"value": at, "precision": "EXACT", "basis": "Fixture", "quote": None},
        "freshness": "FRESH",
        "claims": [],
        "next_milestone": None,
        "checked_sources": [],
    }
    return session, args


@pytest.mark.parametrize("backend", ["memory", "postgres"])
@pytest.mark.parametrize("simulate_old_boundary", [False, True])
async def test_controller_snapshot_cannot_erase_domain_binding(tmp_path, backend, simulate_old_boundary):
    async with fixture_repo(backend) as repo:
        session, args = await seed(repo)
        tools = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        writer_ready, binding_waiting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original_put, original_lock = repo.put, repo.lock
        poll_task = domain_task = None

        async def put(collection, key, record, *args, **kwargs):
            if collection == "ops.agent_sessions" and asyncio.current_task() is poll_task:
                writer_ready.set()
                await release.wait()
            return await original_put(collection, key, record, *args, **kwargs)

        @asynccontextmanager
        async def lock(key):
            if key == "session:s" and asyncio.current_task() is domain_task:
                binding_waiting.set()
                if simulate_old_boundary:
                    yield repo
                    return
            async with original_lock(key):
                yield repo

        repo.put, repo.lock = put, lock

        def handler(request):
            return httpx.Response(
                200,
                json={
                    "data": [{"execution_id": "input", "state": "COMPLETED", "settled_outcome": "COMPLETED"}]
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            service = SessionService(repo, config(), tools.dispatch, http=client)
            poll_task = asyncio.create_task(service.execution_barrier(session, verify_remote=True))
            await writer_ready.wait()
            domain_task = asyncio.create_task(
                tools.dispatch("record_event", args, {"session_id": "s", "role": "analyst"})
            )
            waiting = asyncio.create_task(binding_waiting.wait())
            await asyncio.wait([domain_task, waiting], return_when=asyncio.FIRST_COMPLETED)
            if simulate_old_boundary:
                await domain_task
            release.set()
            await poll_task
            result = await domain_task
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
            if simulate_old_boundary:
                assert (await repo.get("ops.agent_sessions", "s"))["event_id"] is None
                return
            assert (await repo.get("ops.agent_sessions", "s"))["event_id"] == result["event_id"]
            second = await tools.dispatch("record_event", args, {"session_id": "s", "role": "analyst"})
            assert second["reason"] == "EVENT_ALREADY_BOUND"
            assert len(await repo.list("evt.events")) == 1


@pytest.mark.parametrize("backend", ["memory", "postgres"])
async def test_origin_scope_rechecked_after_two_unbound_preflight_snapshots(tmp_path, backend):
    async with fixture_repo(backend) as repo:
        _, args = await seed(repo)
        tools = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        original_get = repo.get
        both_read = asyncio.Event()
        first_reads = set()
        tasks = set()

        async def get(collection, key):
            value = await original_get(collection, key)
            task = asyncio.current_task()
            if (
                collection == "ops.agent_sessions"
                and key == "s"
                and task in tasks
                and task not in first_reads
            ):
                first_reads.add(task)
                if len(first_reads) == 2:
                    both_read.set()
                await both_read.wait()
                assert value["event_id"] is None
            return value

        repo.get = get
        for _ in range(2):
            tasks.add(
                asyncio.create_task(
                    tools.dispatch("record_event", args, {"session_id": "s", "role": "analyst"})
                )
            )
        results = await asyncio.gather(*tasks)
        accepted = [row for row in results if row.get("accepted") is not False]
        refused = [row for row in results if row.get("reason") == "EVENT_ALREADY_BOUND"]
        assert len(accepted) == len(refused) == 1
        assert refused[0]["event_id"] == accepted[0]["event_id"]
        assert len(await repo.list("evt.events")) == 1
        assert len(await repo.list("evt.event_observations")) == 1
        assert (await repo.get("ops.agent_sessions", "s"))["event_id"] == accepted[0]["event_id"]
