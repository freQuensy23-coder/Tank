import asyncio
import json
import logging
from contextlib import asynccontextmanager, suppress
from datetime import timedelta

import httpx
import pytest
from test_agents_session_audit import seed as seed_audit_session
from test_agents_session_audit import settings as session_settings
from test_event_tools import complete_event_fixture
from test_market_capture_refresh import ReleasedMarket, seed_domain
from test_market_truth_timing_gaps import timing_events
from test_session_binding_concurrency import fixture_repo
from test_session_binding_concurrency import seed as seed_typed_session

from tank import logging as telemetry
from tank.agents import SessionService
from tank.agents.context import CONTEXT_VERSION
from tank.analysis.decisions import DecisionEngine
from tank.analysis.truth import MarketTruthService
from tank.event_tools import EventToolService
from tank.market.service import MarketService
from tank.runtime import Runtime
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import chosen_boundary
from tank.util import iso, parse_time, sha256

pytestmark = pytest.mark.integration


async def seed_market_event(repo, tokens):
    await repo.put(
        "evt.events",
        "timing-event",
        {
            "event_id": "timing-event",
            "current_ems_id": "timing-ems",
            "tokens_mentioned": tokens,
            "known_to_us_at": "2026-10-28T11:00:00Z",
            "known_to_us_at_prec": "EXACT",
        },
    )
    await repo.put("evt.ems_assessments", "timing-ems", {"class": "MAJOR"})
    for token in tokens:
        backfill_id = "BF-" + sha256(
            {"event_id": "timing-event", "token_id": token, "anchor": "2026-10-28T11:00:00Z"}
        )
        await repo.put(
            "mkt.backfill_runs",
            backfill_id,
            {
                "backfill_id": backfill_id,
                "event_id": "timing-event",
                "token_id": token,
                "anchor": "2026-10-28T11:00:00Z",
                "started_at": "2026-10-28T11:00:00Z",
                "status": "COMPLETE",
            },
        )


async def test_independent_escalation_worker_reports_the_contended_hot_set_prerequisite(
    setup, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    repo, settings, instrument = setup
    (settings.config_dir / "thresholds.yaml").write_text("{}\n")
    settings.replay = False
    monkeypatch.setattr("tank.market.service.utcnow", lambda: parse_time("2026-10-28T12:00:00Z"))
    token = instrument["token_id"]
    await seed_market_event(repo, [token])
    stop = asyncio.Event()
    attempted = asyncio.Event()
    lock = repo.lock
    finish = repo.finish_job
    put_market_batch = repo.put_market_batch

    async def capture_peer(collection, records, *, keys=None):
        stored = await put_market_batch(collection, records, keys=keys)
        if collection == "mkt.capture_requests":
            completed = [
                {
                    **record,
                    "status": "COMPLETE",
                    "result": {"instruments": {}},
                    "started_at": "2026-10-28T12:00:00Z",
                    "started_at_prec": "COLLECTION_ONLY",
                    "finished_at": "2026-10-28T12:00:00Z",
                    "finished_at_prec": "COLLECTION_ONLY",
                }
                for record in stored
                if record["status"] == "PENDING"
            ]
            await put_market_batch(
                collection, completed, keys=[record["request_id"] for record in completed]
            )
        return stored

    @asynccontextmanager
    async def observed_lock(key):
        if key == "market-hot-set":
            attempted.set()
        async with lock(key):
            yield repo

    async def finished(job_id):
        await finish(job_id)
        if (await repo.get("ops.jobs", job_id))["type"] == "RUN_MARKET_TRUTH":
            stop.set()

    monkeypatch.setattr(repo, "lock", observed_lock)
    monkeypatch.setattr(repo, "finish_job", finished)
    monkeypatch.setattr(repo, "put_market_batch", capture_peer)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    ) as http:
        market = MarketService(repo, settings, http=http)
        truth = MarketTruthService(repo, settings, market)
        tools = EventToolService(repo, settings, truth, None)
        runtime = object.__new__(Runtime)
        runtime.repo, runtime.settings, runtime.market, runtime.truth = repo, settings, market, truth
        runtime.handlers = {"ESCALATE_TOKENS": runtime.escalate, "RUN_MARKET_TRUTH": runtime.assess}
        worker = asyncio.create_task(runtime.worker(stop))
        caller = None
        try:
            async with asyncio.timeout(5):
                async with lock("market-hot-set"):
                    caller = asyncio.create_task(
                        tools.get_market_truth(
                            {
                                "event_id": "timing-event",
                                "token_ids": [token],
                                "cutoff": "2026-10-28T12:00:00Z",
                            },
                            {},
                        )
                    )
                    await attempted.wait()
                    jobs = await repo.list("ops.jobs")
                    escalation = next(row for row in jobs if row["type"] == "ESCALATE_TOKENS")
                    assessment = next(row for row in jobs if row["type"] == "RUN_MARKET_TRUTH")
                    assert escalation["status"] == "RUNNING"
                    assert assessment["status"] == "QUEUED"
                    assert assessment["payload"]["_depends_on"] == [escalation["job_id"]]
                    assert await repo.list("mkt.hot_tokens") == []
                    records = timing_events(caplog)
                    escalation_id = telemetry.timing_identity(escalation["job_id"])
                    live = [row for row in records if row.get("job_id") == escalation_id]
                    assert any(
                        row["stage"] == "job.execution" and row["event"] == "stage_start" for row in live
                    ), "independent escalation worker has no timing root"
                    assert any(
                        row["stage"] == "capture.hot_set_lock_wait" and row["event"] == "stage_start"
                        for row in live
                    )
                    assert not any(
                        row["stage"] == "capture.hot_set_lock_wait" and row["event"] == "stage_end"
                        for row in live
                    )
                    assert any(row["stage"] == "job.link" and row["event"] == "stage_end" for row in live), (
                        "caller did not link the blocking prerequisite"
                    )
                result = await caller
                await worker
        finally:
            stop.set()
            for task in (caller, worker):
                if task is not None:
                    if not task.done():
                        task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
    record = result["market_truth"][0]
    assert record == await repo.get("ana.market_truth", record["mta_id"])
    assert record["cutoff"] == "2026-10-28T12:00:00Z"
    assert record["cutoff_prec"] == "EXACT"
    assert (await repo.get("ops.jobs", escalation["job_id"]))["status"] == "DONE"
    assert (await repo.get("ops.jobs", assessment["job_id"]))["result"] == record
    assert (await repo.get("mkt.hot_tokens", token + ":timing-event"))["priority"] == 3
    assert any(row["tier"] == "HOT" for row in await repo.list("ops.capture_tier_transitions"))
    ended = timing_events(caplog, "stage_end")
    prerequisite = [row for row in ended if row.get("job_id") == escalation_id]
    assert {
        "capture.escalate",
        "capture.hot_set_lock_wait",
        "capture.escalate_prepare",
        "capture.hot_set_sync",
        "capture.backfill_schedule",
        "job.persist",
    } <= {row["stage"] for row in prerequisite}
    wait = next(row for row in prerequisite if row["stage"] == "capture.hot_set_lock_wait")
    prepare = next(row for row in prerequisite if row["stage"] == "capture.escalate_prepare")
    assert wait["ended_mono_ns"] <= prepare["started_mono_ns"]
    assert "thread_cpu_ms" not in wait
    links = [row for row in ended if row["stage"] == "job.link"]
    assert {telemetry.timing_identity(row["job_id"]) for row in jobs} <= {row["job_id"] for row in links}
    assert len({row["trace_id"] for row in links}) == 1
    assert all("trace_id" not in row["payload"] for row in await repo.list("ops.jobs"))


async def test_all_token_and_prerequisite_links_exist_before_first_result_poll(setup, monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    repo, settings, _ = setup
    tokens = ["test:ABC", "bitcoin:BTC"]
    await seed_market_event(repo, tokens)
    polled = asyncio.Event()
    release = asyncio.Event()
    get = repo.get

    async def observed_get(collection, key):
        if collection == "ops.jobs":
            polled.set()
            await release.wait()
        return await get(collection, key)

    monkeypatch.setattr(repo, "get", observed_get)
    tools = EventToolService(repo, settings, None, None)
    caller = asyncio.create_task(
        tools.get_market_truth(
            {
                "event_id": "timing-event",
                "token_ids": tokens,
                "cutoff": "2026-10-28T12:00:00Z",
            },
            {},
        )
    )
    try:
        async with asyncio.timeout(2):
            await polled.wait()
        jobs = await repo.list("ops.jobs")
        assert len(jobs) == 4
        expected = {telemetry.timing_identity(row["job_id"]) for row in jobs}
        links = [row for row in timing_events(caplog, "stage_end") if row["stage"] == "job.link"]
        assert {row["job_id"] for row in links} == expected, "later-token and prerequisite links are delayed"
        assert all("trace_id" not in row["payload"] for row in jobs)
    finally:
        caller.cancel()
        with suppress(asyncio.CancelledError):
            await caller


@pytest.mark.parametrize("cancelled", [False, True])
async def test_get_decision_refresh_keeps_session_identity_through_the_real_return_path(
    tmp_path, monkeypatch, caplog, cancelled
):
    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path, _env_file=None)
    end = parse_time("2026-10-28T12:00:00Z")
    monkeypatch.setattr("tank.event_tools.utcnow", lambda: end)
    market = ReleasedMarket({"token_id": "T", "cutoff": iso(end), "cutoff_prec": "BUCKET(1m)"})
    truth = MarketTruthService(repo, settings, market)
    tools = EventToolService(repo, settings, truth, DecisionEngine())
    await seed_domain(repo, settings, end)
    first = await truth.assess("e", "T", chosen_boundary(end - timedelta(minutes=1), precision="BUCKET(1m)"))
    initial = await tools.dispatch("get_decision", {"event_id": "e", "token_id": "T"}, {})
    condition = next(row for row in initial["conditions"] if row["role"] == "UPGRADE")
    condition = await repo.get("ana.watch_conditions", condition["cond_id"])
    condition.update(status="FIRED", fired_at=iso(end), fired_at_prec="BUCKET(1m)")
    await repo.put("ana.watch_conditions", condition["cond_id"], condition)
    session = await seed_audit_session(repo)
    sent = []

    def accept(request):
        sent.append(json.loads(request.content))
        return httpx.Response(200, json={})

    runtime = object.__new__(Runtime)
    runtime.repo, runtime.settings, runtime.market, runtime.truth = repo, settings, market, truth
    runtime.handlers = {"ESCALATE_TOKENS": runtime.escalate, "RUN_MARKET_TRUTH": runtime.assess}
    stop = asyncio.Event()
    finish = repo.finish_job
    lock = repo.lock
    requested, held, attempted, release = (asyncio.Event() for _ in range(4))
    acquisitions = 0

    @asynccontextmanager
    async def observed_lock(key):
        nonlocal acquisitions
        if key == "session:s":
            acquisitions += 1
            if acquisitions == 2:
                requested.set()
                await held.wait()
                attempted.set()
        async with lock(key):
            yield repo

    async def owner():
        await requested.wait()
        async with lock("session:s"):
            held.set()
            await release.wait()

    async def finished(job_id):
        await finish(job_id)
        if (await repo.get("ops.jobs", job_id))["type"] == "RUN_MARKET_TRUTH":
            stop.set()

    monkeypatch.setattr(repo, "finish_job", finished)
    monkeypatch.setattr(repo, "lock", observed_lock)
    caplog.clear()
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    worker = asyncio.create_task(runtime.worker(stop))
    holder = asyncio.create_task(owner())
    caller = None
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(accept)) as http:
            sessions = SessionService(repo, session_settings(), tools.dispatch, http=http)
            async with asyncio.timeout(5):
                caller = asyncio.create_task(
                    sessions.tool(
                        session,
                        {"name": "get_decision", "id": "refresh-decision", "input": {"token_id": "T"}},
                    )
                )
                await attempted.wait()
                assert not caller.done()
                active = timing_events(caplog)
                waits = [row for row in active if row["stage"] == "tool.audit.mutation_lock_wait"]
                assert [row["event"] for row in waits] == ["stage_start"], (
                    "nested decision mutation advisory acquisition has no live wait span"
                )
                assert not any(row["stage"] == "tool.audit.mutation" for row in active)
                pending = await repo.get("ops.tool_calls", "s:refresh-decision")
                assert pending["status"] == "PENDING" and not pending.get("returned_at")
                assert not (await repo.get("ana.watch_conditions", condition["cond_id"])).get("consumed_by")
                if cancelled:
                    caller.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await caller
                release.set()
                await holder
                if not cancelled:
                    await caller
                await worker
    finally:
        stop.set()
        release.set()
        for task in (caller, worker, holder):
            if task is not None:
                if not task.done():
                    task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
    stored = await repo.get("ops.tool_calls", "s:refresh-decision")
    ended = timing_events(caplog, "stage_end")
    wait = next(row for row in ended if row["stage"] == "tool.audit.mutation_lock_wait")
    if cancelled:
        assert wait["status"] == "cancelled"
        assert stored["status"] == "PENDING" and not stored.get("returned_at")
        assert not (await repo.get("ana.watch_conditions", condition["cond_id"])).get("consumed_by")
        assert sent == []
        assert not any(row["stage"] == "tool.audit.mutation" for row in ended)
        async with asyncio.timeout(1):
            async with lock("session:s"), lock("tool:s:refresh-decision"):
                assert (await repo.get("ops.tool_calls", "s:refresh-decision"))["status"] == "PENDING"
        return
    body = next(row for row in ended if row["stage"] == "tool.audit.mutation")
    assert wait["ended_mono_ns"] <= body["started_mono_ns"]
    assert stored["status"] == "DONE"
    assert stored["returned_at"] is not None
    assert stored["result"]["mta_id"] != first["mta_id"]
    fresh = await repo.get("ana.market_truth", stored["result"]["mta_id"])
    assert fresh["cutoff"] == iso(end)
    assert fresh["cutoff_prec"] == "BUCKET(1m)"
    assert (await repo.get("ana.watch_conditions", condition["cond_id"]))["consumed_by"] == stored["result"][
        "dec_id"
    ]
    assert len(sent) == 1
    assert sent[0]["events"][0]["custom_tool_use_id"] == "refresh-decision"
    refresh = next(row for row in ended if row["stage"] == "get_market_truth")
    assert refresh.get("session_id") == telemetry.timing_identity("s"), (
        "nested refresh lost its caller session"
    )
    assert refresh.get("call_id") == telemetry.timing_identity("refresh-decision")
    returned = [row for row in ended if row["trace_id"] == refresh["trace_id"]]
    assert {
        "tool",
        "tool.arguments",
        "tool.audit",
        "tool.projection",
        "tool.wire_json",
        "tool.wire_audit",
        "tool.openma_acceptance",
        "tool.return_audit",
    } <= {row["stage"] for row in returned}
    assert all(row.get("session_id") == refresh["session_id"] for row in returned)
    assert all(row.get("call_id") == refresh["call_id"] for row in returned)


@pytest.mark.parametrize("backend", ["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
@pytest.mark.parametrize("phase", ["audit", "session_sequence", "wire_audit", "return_audit"])
async def test_tool_advisory_acquisition_is_not_charged_to_the_held_audit_body(
    backend, phase, postgres_template, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    async with fixture_repo(backend, postgres_template) as repo:
        await repo.put("evt.events", "e", complete_event_fixture("e"))
        session, _ = await seed_typed_session(repo)
        session["event_id"] = "e"
        session["model_context_version"] = CONTEXT_VERSION
        await repo.put("ops.agent_sessions", "s", session)
        opts = session_settings()
        lock = repo.lock
        requested, held, attempted, release = (asyncio.Event() for _ in range(4))
        target = "session:s" if phase == "session_sequence" else "tool:s:lock-timing"
        ordinal = {"audit": 1, "session_sequence": 1, "wire_audit": 2, "return_audit": 3}[phase]
        body_stage = "tool.audit.session_sequence" if phase == "session_sequence" else "tool." + phase
        wait_stage = (
            "tool.audit.session_lock_wait" if phase == "session_sequence" else "tool." + phase + ".lock_wait"
        )
        acquisitions = 0
        accepted = []

        @asynccontextmanager
        async def observed_lock(key):
            nonlocal acquisitions
            if key == target:
                acquisitions += 1
                if acquisitions == ordinal:
                    requested.set()
                    await held.wait()
                    attempted.set()
            async with lock(key):
                yield repo

        async def owner():
            await requested.wait()
            async with lock(target):
                held.set()
                await release.wait()

        async def dispatch(name, arguments, context):
            return {"market_truth": []}

        def accept(request):
            accepted.append(json.loads(request.content))
            return httpx.Response(200, json={})

        monkeypatch.setattr(repo, "lock", observed_lock)
        holder = asyncio.create_task(owner())
        task = None
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(accept)) as http:
                sessions = SessionService(repo, opts, dispatch, http=http)
                task = asyncio.create_task(
                    sessions.tool(session, {"name": "get_market_truth", "id": "lock-timing", "input": {}})
                )
                async with asyncio.timeout(5):
                    await attempted.wait()
                    assert not task.done()
                    stored = await repo.get("ops.tool_calls", "s:lock-timing")
                    if phase in {"audit", "session_sequence"}:
                        assert stored is None
                    elif phase == "wire_audit":
                        assert stored["status"] == "DONE" and not stored.get("native_result_body")
                    else:
                        assert stored["native_result_body"] and not stored.get("returned_at")
                    records = timing_events(caplog)
                    waits = [row for row in records if row["stage"] == wait_stage]
                    assert [row["event"] for row in waits] == ["stage_start"], (
                        "advisory acquisition has no live wait span"
                    )
                    assert not any(row["stage"] == body_stage for row in records), (
                        "held-body timing began before lock ownership"
                    )
                    release.set()
                    await task
                    await holder
        finally:
            release.set()
            for pending in (task, holder):
                if pending is not None:
                    if not pending.done():
                        pending.cancel()
                    with suppress(asyncio.CancelledError):
                        await pending
        stored = await repo.get("ops.tool_calls", "s:lock-timing")
        assert stored["status"] == "DONE"
        assert stored["returned_at"] is not None
        assert accepted[0]["events"][0] == stored["native_result_body"]
        ended = timing_events(caplog, "stage_end")
        wait = next(row for row in ended if row["stage"] == wait_stage)
        body = next(row for row in ended if row["stage"] == body_stage)
        assert wait["ended_mono_ns"] <= body["started_mono_ns"]
        assert wait["call_id"] == body["call_id"] == telemetry.timing_identity("lock-timing")
        assert "thread_cpu_ms" not in wait


async def test_provenance_refusal_has_an_owned_span_before_market_dispatch(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    repo = MemoryRepository()
    session = await seed_audit_session(repo)
    session["prompt_version"] = "legacy"
    session["prompt_version_basis"] = None
    await repo.put("ops.agent_sessions", "s", session)
    entered, release = asyncio.Event(), asyncio.Event()
    sent = []

    async def transport(request):
        if request.method == "GET":
            entered.set()
            await release.wait()
            return httpx.Response(200, json={"agent": {"system": "incomplete snapshot"}})
        sent.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async def dispatch(*args):
        raise AssertionError("unresolved provenance reached market dispatch")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        sessions = SessionService(repo, session_settings(), dispatch, http=http)
        task = asyncio.create_task(
            sessions.tool(session, {"name": "get_market_truth", "id": "provenance", "input": {}})
        )
        try:
            async with asyncio.timeout(2):
                await entered.wait()
                assert await repo.get("ops.tool_calls", "s:provenance") is None
                stages = [row for row in timing_events(caplog) if row["stage"] == "tool.provenance"]
                assert [row["event"] for row in stages] == ["stage_start"], (
                    "provenance transport work has no owned live span"
                )
                release.set()
                await task
        finally:
            release.set()
            if not task.done():
                task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    stored = await repo.get("ops.tool_calls", "s:provenance")
    assert stored["status"] == "REFUSED"
    assert stored["result"]["error"]["category"] == "PROVENANCE"
    assert stored["returned_at"] is not None
    assert sent[0]["events"][0]["is_error"] is True
    assert await repo.list("ops.jobs") == []
    ended = timing_events(caplog, "stage_end")
    provenance = next(row for row in ended if row["stage"] == "tool.provenance")
    assert provenance["status"] == "error"
    assert provenance["call_id"] == telemetry.timing_identity("provenance")
    assert not any(row["stage"] == "get_market_truth" for row in ended)


async def test_failed_truth_job_times_terminal_failure_persistence(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    repo = MemoryRepository()
    job = await repo.enqueue(
        "RUN_MARKET_TRUTH",
        {"event_id": "missing-event", "token_id": "T"},
        "missing-event:T",
        max_attempts=1,
    )
    stop, entered, release = (asyncio.Event() for _ in range(3))
    fail = repo.fail_job

    async def observed_fail(*args, **kwargs):
        entered.set()
        await release.wait()
        await fail(*args, **kwargs)
        stop.set()

    monkeypatch.setattr(repo, "fail_job", observed_fail)
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.handlers = {"RUN_MARKET_TRUTH": runtime.assess}
    worker = asyncio.create_task(runtime.worker(stop))
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "RUNNING"
            rows = [
                row
                for row in timing_events(caplog)
                if row["stage"] == "job.failure_persist"
                and row.get("job_id") == telemetry.timing_identity(job["job_id"])
            ]
            assert [row["event"] for row in rows] == ["stage_start"], (
                "failure persistence is outside the timing graph"
            )
            release.set()
            await worker
    finally:
        release.set()
        stop.set()
        if not worker.done():
            worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker
    stored = await repo.get("ops.jobs", job["job_id"])
    assert stored["status"] == "FAILED"
    assert stored["last_error"] == "ValueError"
    assert stored["finished_at"] is not None
    persistence = next(
        row for row in timing_events(caplog, "stage_end") if row["stage"] == "job.failure_persist"
    )
    assert persistence["status"] == "ok"
