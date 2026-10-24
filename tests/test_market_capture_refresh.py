"""Bounded capture and automatic late-data refresh through real numerical/domain paths."""

import asyncio
import copy
import json
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from test_agents_session_audit import settings as session_settings
from test_event_tools import complete_assessment_fixture, complete_event_fixture
from test_session_binding_concurrency import fixture_repo
from test_session_binding_concurrency import seed as seed_typed_session
from test_truth_start_fixture import compact_start_snapshot

from tank.agents import SessionService
from tank.analysis.decisions import DecisionEngine
from tank.analysis.truth import MarketTruthService
from tank.event_tools import EventToolService
from tank.market.capture_contracts import CaptureReadiness, RepairCompleted
from tank.runtime import Runtime
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import chosen_boundary
from tank.util import iso, parse_time, utcnow


@lru_cache(maxsize=2)
def _canonical_snapshot(full_flow_history):
    """Private immutable seed; decoded output is owned by one caller only."""
    source = compact_start_snapshot()

    end_value = parse_time(source["cutoff"])

    if not full_flow_history:
        source["flow_bars"] = [
            r
            for r in source["flow_bars"]
            if parse_time(r["minute_start"]) >= end_value - timedelta(hours=4, minutes=1)
        ]
    source["oi"] = [
        r
        for r in source["oi"]
        if (parse_time(r["ts"]).hour in {8, 12} and parse_time(r["ts"]).minute == 0)
        or parse_time(r["ts"])
        in {end_value - timedelta(hours=1), end_value - timedelta(minutes=15), end_value}
    ]
    source["funding"] = [
        r
        for r in source["funding"]
        if parse_time(r["settlement_time"]).hour % 8 == 0
        and parse_time(r["settlement_time"]).minute == 0
        or parse_time(r["settlement_time"]) == end_value
    ]
    return json.dumps(source).encode()


def shifted_snapshot(end, *, full_flow_history=True):
    source = json.loads(_canonical_snapshot(full_flow_history))
    delta = end - parse_time(source["cutoff"])

    def shift(value):
        if isinstance(value, dict):
            for key, item in value.items():
                value[key] = shift(item)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                value[index] = shift(item)
        elif isinstance(value, str) and re.match(r"^\d{4}-\d{2}-\d{2}T", value):
            return iso(parse_time(value) + delta)
        return value

    return shift(source)


class ReleasedMarket:
    """Only synthetic acquisition is controlled; numerical classification stays real."""

    def __init__(self, snapshot) -> None:
        self.source = snapshot
        self.released = False
        self.reads = []

    async def snapshot(self, token, cutoff, *, anchor=None):

        unavailable = {"flow_bars", "books", "oi", "klines", "funding"} if not self.released else set()
        result = {
            key: [] if key in unavailable else copy.deepcopy(value) for key, value in self.source.items()
        }
        cutoff_value = getattr(cutoff, "value", cutoff)
        result["cutoff"] = iso(cutoff_value)
        self.reads.append((token, iso(cutoff_value), self.released))
        return result

    async def condition_snapshot(self, token, cutoff, *, anchor=None, requirements=None):
        return await self.snapshot(token, cutoff, anchor=anchor)

    async def escalate(self, *args, **kwargs) -> None:
        return None

    async def capture_readiness(self, event_id: str, token_id: str, *, anchor=None) -> CaptureReadiness:
        return CaptureReadiness(
            event_id=event_id,
            token_id=token_id,
            status="PARTIAL",
            reason="INCOMPLETE_VENUE_WINDOWS_EXCLUDED",
            backfill_id="BF-released",
            anchor=None if anchor is None else anchor.value,
        )

    async def repair_windows(
        self, token_id: str, start: datetime, end: datetime, *, budget_s: float
    ) -> RepairCompleted:
        return RepairCompleted(status="COMPLETE", request_id="REQ-released", instruments={})

    async def wait_capture(self, *args, **kwargs) -> None:
        raise AssertionError("Terminal capture must not wait")


async def persisted_ids(repo):
    """Compare exact identities without requesting full numerical documents."""
    result = {}
    for table, field in (
        ("ops.jobs", "job_id"),
        ("ana.market_truth", "mta_id"),
        ("ana.decisions", "dec_id"),
        ("ana.watch_conditions", "cond_id"),
    ):
        result[table] = tuple(sorted([row[field] async for row in repo.iter_metadata(table, [field])]))
    return result


@asynccontextmanager
async def _refresh_repository(backend, postgres_template):
    if backend == "postgres":
        url = os.getenv("TANK_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Disposable administrator PostgreSQL DSN required")
        async with postgres_template.repository(url) as repo:
            yield repo
    else:
        async with fixture_repo(backend) as repo:
            yield repo


async def seed_bound_session(repo) -> None:
    session, _ = await seed_typed_session(repo)
    session["event_id"] = "e"
    await repo.put("ops.agent_sessions", "s", session)


async def seed_domain(repo, settings, end) -> None:
    anchor = iso(end - timedelta(hours=4))
    event = complete_event_fixture(
        "e",
        current_ems_id="ems",
        event_state="LIVE",
        public_at=anchor,
        public_at_prec="EXACT",
        known_to_us_at=anchor,
        known_to_us_at_prec="COLLECTION_ONLY",
    )
    assessment = complete_assessment_fixture(
        "ems",
        "e",
        **{"class": "MAJOR", "certainty": "HIGH"},
        impact_horizon={"impact_start": "NOW"},
    )
    edge = {
        "edge_id": "edge",
        "event_id": "e",
        "map_version": 1,
        "token_id": "T",
        "role": "PRIMARY",
        "credible": True,
        "capture_class": "DIRECT",
        "exposure_score": 8,
    }
    for table, key, row in (
        ("evt.events", "e", event),
        ("evt.ems_assessments", "ems", assessment),
        ("evt.beneficiary_maps", "e:1", {"event_id": "e", "map_version": 1, "analysis_set": ["T"]}),
        ("evt.beneficiary_edges", "edge", edge),
    ):
        await repo.put(table, key, row)


@pytest.mark.integration
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_wait_data_watch_refreshes_truth_before_decision_and_replays_lost_ack(
    tmp_path: Path, backend, postgres_template, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with _refresh_repository(backend, postgres_template) as repo:
        settings = Settings(data_dir=tmp_path, _env_file=None)
        end = utcnow().replace(second=0, microsecond=0)
        logical_now = [end]
        monkeypatch.setattr("tank.event_tools.utcnow", lambda: logical_now[0])
        market = ReleasedMarket(shifted_snapshot(end, full_flow_history=True))
        truth = MarketTruthService(repo, settings, market)
        tools = EventToolService(repo, settings, truth, DecisionEngine())
        scheduler = Scheduler(repo, settings, market, truth)
        await seed_domain(repo, settings, end)
        first = await truth.assess(
            "e", "T", chosen_boundary(end - timedelta(minutes=1), precision="BUCKET(1m)")
        )
        assert first["data_confidence"]["class"] == "LOW"
        initial = await tools.dispatch("get_decision", {"event_id": "e", "token_id": "T"}, {})
        assert initial["action"] == "WAIT"
        watch = next(row for row in initial["conditions"] if row["role"] == "UPGRADE")
        assert watch["expr"]["metric"] == "dc.class" and watch["expr"]["value"] == "HIGH"
        await scheduler.eval_conditions(chosen_boundary(end, precision="BUCKET(1m)").fields("cutoff"))
        assert (await repo.get("ana.watch_conditions", watch["cond_id"]))["status"] == "ARMED"
        market.released = True
        await scheduler.eval_conditions(chosen_boundary(end, precision="BUCKET(1m)").fields("cutoff"))
        fired = await repo.get("ana.watch_conditions", watch["cond_id"])
        assert fired["status"] == "FIRED" and not fired.get("consumed_by")
        messages = [row async for row in repo.iter_all("ops.jobs", filters={"type": "AGENT_MESSAGE"})]
        assert any(row["payload"].get("kind") == "CONDITION_FIRED" for row in messages)

        assert len(await repo.list("ana.market_truth")) == 1
        await seed_bound_session(repo)
        sent = []
        native_messages = []

        def transport(request: httpx.Request):
            body = json.loads(request.content)
            if body["events"][0]["type"] == "user.message":
                native_messages.append(json.loads(body["events"][0]["content"][0]["text"]))
                return httpx.Response(200, json={"data": [{"id": "accepted-condition"}]})
            sent.append(body)
            if len(sent) == 1:
                raise httpx.ReadError("Synthetic lost ACK", request=request)
            return httpx.Response(200, json={})

        runtime = object.__new__(Runtime)
        runtime.repo, runtime.market, runtime.truth = repo, market, truth
        runtime.handlers = {"ESCALATE_TOKENS": runtime.escalate_wire, "RUN_MARKET_TRUTH": runtime.assess_wire}
        stop = asyncio.Event()
        worker = None
        event = {
            "id": "decision",
            "type": "agent.custom_tool_use",
            "name": "get_decision",
            "input": {"token_id": "T"},
        }
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
                sessions = SessionService(repo, session_settings(), tools.dispatch, http=http)
                sessions.start_stream = lambda sid: None
                runtime.handlers["AGENT_MESSAGE"] = sessions.send_message
                worker = asyncio.create_task(runtime.worker(stop))
                async with asyncio.timeout(20):
                    with pytest.raises(httpx.ReadError):
                        await sessions.process_event("s", event, 7)
                ledger = await repo.get("ops.tool_calls", "s:decision")
                assert ledger["status"] == "DONE" and not ledger.get("returned_at")
                assert native_messages[0]["message"]["kind"] == "CONDITION_FIRED"
                assert "cond_id" not in native_messages[0]["message"]["condition"]
                assert native_messages[0]["message"]["condition"]["expr"] == watch["expr"]
                decision = ledger["result"]
                assert decision["mta_id"] != first["mta_id"]
                fresh = await repo.get("ana.market_truth", decision["mta_id"])
                assert fresh["cutoff"] == iso(end)
                assert fresh["data_confidence"]["class"] == "HIGH"
                assert decision["action"] == "START"
                assert (await repo.get("ana.watch_conditions", watch["cond_id"]))["consumed_by"] == decision[
                    "dec_id"
                ]
                before = await persisted_ids(repo)
                logical_now[0] += timedelta(minutes=1)
                await sessions.process_event("s", event, 7)
                assert sent[0] == sent[1]
                assert (await repo.get("ops.tool_calls", "s:decision"))["returned_at"]
                assert (await repo.get("ops.agent_sessions", "s"))["next_tool_call_ordinal"] == 1
                assert before == await persisted_ids(repo)
                assert await repo.get("ana.market_truth", first["mta_id"]) == first
        finally:
            stop.set()
            if worker is not None:
                await worker


async def test_runtime_waits_for_capture_release_before_numerical_assessment() -> None:
    from tank.analysis.truth_contracts import MarketTruthAssessmentRef
    from tank.runtime_contracts import MarketTruthJob

    repo = MemoryRepository()
    await repo.put("evt.events", "e", {"event_id": "e", "public_at": iso(), "known_to_us_at": iso()})
    waiting, release = asyncio.Event(), asyncio.Event()

    async def wait_capture(*args, **kwargs) -> None:
        assert kwargs["timeout_s"] == 15
        waiting.set()
        await release.wait()

    reference = MarketTruthAssessmentRef(
        mta_id="MTA-released",
        event_id="e",
        token_id="T",
        cutoff="2026-06-17T04:00:00Z",
        cutoff_prec="EXACT",
        cutoff_prec_reason=None,
    )
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.market = SimpleNamespace(
        capture_readiness=AsyncMock(
            return_value=CaptureReadiness(
                event_id="e",
                token_id="T",
                status="RUNNING",
                reason="PRE_EVENT_CAPTURE_PENDING",
                backfill_id="BF-runtime",
                anchor=None,
            )
        ),
        wait_capture=wait_capture,
        repair_windows=AsyncMock(return_value=None),
    )
    runtime.truth = SimpleNamespace(assess_for_job=AsyncMock(return_value=reference))
    payload = MarketTruthJob(
        event_id="e",
        token_id="T",
        cutoff=reference.cutoff,
        cutoff_prec=reference.cutoff_prec,
        cutoff_prec_reason=reference.cutoff_prec_reason,
    )
    job = asyncio.create_task(runtime.assess(payload))
    observing = asyncio.create_task(waiting.wait())
    try:
        done, _ = await asyncio.wait((job, observing), return_when=asyncio.FIRST_COMPLETED)
        if job in done:
            await job
            pytest.fail("Assessment completed before the required capture release")
        runtime.truth.assess_for_job.assert_not_called()
        release.set()
        assert await job == reference
        runtime.truth.assess_for_job.assert_awaited_once()
    finally:
        release.set()
        if not job.done():
            job.cancel()
        if not observing.done():
            observing.cancel()
        await asyncio.gather(job, observing, return_exceptions=True)


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
@pytest.mark.integration
async def test_cancelled_preflight_reuses_job_before_any_decision_ledger(
    tmp_path: Path, backend, postgres_template
) -> None:
    async with _refresh_repository(backend, postgres_template) as repo:
        settings = Settings(data_dir=tmp_path, _env_file=None)
        end = utcnow().replace(second=0, microsecond=0)
        market = ReleasedMarket(shifted_snapshot(end, full_flow_history=False))
        truth = MarketTruthService(repo, settings, market)
        tools = EventToolService(repo, settings, truth, DecisionEngine())
        await seed_domain(repo, settings, end)
        first = await truth.assess(
            "e", "T", chosen_boundary(end - timedelta(minutes=1), precision="BUCKET(1m)")
        )
        decision = await tools.dispatch("get_decision", {"event_id": "e", "token_id": "T"}, {})
        condition = next(row for row in decision["conditions"] if row["role"] == "UPGRADE")
        condition.update(status="FIRED", fired_at=iso())
        await repo.put("ana.watch_conditions", condition["cond_id"], condition)
        await seed_bound_session(repo)
        event = {
            "id": "cancelled",
            "type": "agent.custom_tool_use",
            "name": "get_decision",
            "input": {"token_id": "T"},
        }
        acks = []

        def accepted(request: httpx.Request):
            acks.append(json.loads(request.content))
            return httpx.Response(200, json={})

        async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as http:
            sessions = SessionService(repo, session_settings(), tools.dispatch, http=http)
            pending = asyncio.create_task(sessions.process_event("s", event, 8))
            async with asyncio.timeout(5):
                while not [
                    row async for row in repo.iter_all("ops.jobs", filters={"type": "RUN_MARKET_TRUTH"})
                ]:
                    await asyncio.sleep(0.01)
            original_jobs = [
                row async for row in repo.iter_all("ops.jobs", filters={"type": "RUN_MARKET_TRUTH"})
            ]
            assert len(original_jobs) == 1
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert await repo.get("ops.tool_calls", "s:cancelled") is None
            assert not (await repo.get("ana.watch_conditions", condition["cond_id"])).get("consumed_by")
            assert len(await repo.list("ana.decisions")) == 1
            market.released = True
            runtime = object.__new__(Runtime)
            runtime.repo, runtime.market, runtime.truth = repo, market, truth
            runtime.handlers = {
                "ESCALATE_TOKENS": runtime.escalate_wire,
                "RUN_MARKET_TRUTH": runtime.assess_wire,
            }
            stop = asyncio.Event()
            worker = asyncio.create_task(runtime.worker(stop))
            try:
                async with asyncio.timeout(20):
                    await sessions.process_event("s", event, 8)
                jobs = [row async for row in repo.iter_all("ops.jobs", filters={"type": "RUN_MARKET_TRUTH"})]
                assert len(jobs) == 1 and jobs[0]["job_id"] == original_jobs[0]["job_id"]
                ledger = await repo.get("ops.tool_calls", "s:cancelled")
                assert ledger["status"] == "DONE" and ledger["result"]["mta_id"] != first["mta_id"]
                assert len(await repo.list("ana.decisions")) == 2
                assert (await repo.get("ops.agent_sessions", "s"))["next_tool_call_ordinal"] == 1
                assert len(acks) == 1
            finally:
                stop.set()
                await worker


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
@pytest.mark.integration
async def test_condition_firing_during_refresh_wait_is_deferred_until_its_own_refresh(
    tmp_path: Path, backend, postgres_template
) -> None:
    async with _refresh_repository(backend, postgres_template) as repo:
        settings = Settings(data_dir=tmp_path, _env_file=None)
        end = utcnow().replace(second=0, microsecond=0)

        market = ReleasedMarket(
            {
                "cutoff": iso(end),
                "anchor": {"type": "T0", "ts": iso(end - timedelta(hours=4))},
                "instruments": [],
                "flow_bars": [],
                "klines": [],
                "books": [],
                "oi": [],
                "funding": [],
                "semantics": [],
            }
        )
        truth = MarketTruthService(repo, settings, market)
        engine = DecisionEngine()
        tools = EventToolService(repo, settings, truth, engine)
        await seed_domain(repo, settings, end)
        await truth.assess("e", "T", chosen_boundary(end - timedelta(minutes=1), precision="BUCKET(1m)"))
        decision = await tools.dispatch("get_decision", {"event_id": "e", "token_id": "T"}, {})
        condition = next(row for row in decision["conditions"] if row["role"] == "UPGRADE")
        condition.update(status="FIRED", fired_at=iso())
        await repo.put("ana.watch_conditions", condition["cond_id"], condition)
        second = dict(condition, cond_id="concurrent-condition", status="ARMED")
        second.pop("fired_at")
        await repo.put("ana.watch_conditions", second["cond_id"], second)
        await seed_bound_session(repo)
        entering, release = asyncio.Event(), asyncio.Event()
        original_assess = truth.assess

        async def held(*args, **kwargs):
            entering.set()
            await release.wait()
            return await original_assess(*args, **kwargs)

        truth.assess = held
        evaluated = []
        original_evaluate = engine.evaluate

        def observe(*args, **kwargs):
            evaluated.append([row["cond_id"] for row in kwargs.get("conditions", [])])
            return original_evaluate(*args, **kwargs)

        engine.evaluate = observe
        market.released = True
        runtime = object.__new__(Runtime)
        runtime.repo, runtime.market, runtime.truth = repo, market, truth
        runtime.handlers = {"ESCALATE_TOKENS": runtime.escalate_wire, "RUN_MARKET_TRUTH": runtime.assess_wire}
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
        ) as http:
            sessions = SessionService(repo, session_settings(), tools.dispatch, http=http)
            event = {
                "id": "first-condition",
                "type": "agent.custom_tool_use",
                "name": "get_decision",
                "input": {"token_id": "T"},
            }
            pending = asyncio.create_task(sessions.process_event("s", event, 8))
            try:
                await asyncio.wait_for(entering.wait(), 5)

                second.update(status="FIRED", fired_at=iso())
                await repo.put("ana.watch_conditions", second["cond_id"], second)
                release.set()
                async with asyncio.timeout(20):
                    await pending
                assert evaluated[-1] == [condition["cond_id"]]
                assert (await repo.get("ana.watch_conditions", condition["cond_id"])).get("consumed_by")
                deferred = await repo.get("ana.watch_conditions", second["cond_id"])
                assert deferred["status"] == "FIRED" and not deferred.get("consumed_by")
                before = len(await repo.list("ana.market_truth"))
                await sessions.process_event("s", {**event, "id": "second-condition"}, 9)
                assert evaluated[-1] == [second["cond_id"]]
                assert (await repo.get("ana.watch_conditions", second["cond_id"])).get("consumed_by")
                assert len(await repo.list("ana.market_truth")) == before + 1
            finally:
                release.set()
                stop.set()
                if not pending.done():
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                await worker


@pytest.mark.parametrize(
    "role,metric,value,expected",
    [
        ("INVALIDATION", "event.contradiction_fatal", True, "EXIT"),
        ("DOWNGRADE", "milestone.outcome", "FAILED", "REDUCE"),
    ],
)
@pytest.mark.integration
async def test_event_only_risk_watch_never_waits_for_market_refresh(
    tmp_path: Path, role, metric, value, expected
) -> None:
    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path, _env_file=None)
    end = utcnow().replace(second=0, microsecond=0)
    market = ReleasedMarket(shifted_snapshot(end, full_flow_history=False))
    truth = MarketTruthService(repo, settings, market)
    tools = EventToolService(repo, settings, truth, DecisionEngine())
    await seed_domain(repo, settings, end)
    first = await truth.assess("e", "T", chosen_boundary(end - timedelta(minutes=1), precision="BUCKET(1m)"))
    await repo.put(
        "ana.calls",
        "open",
        {
            "call_id": "open",
            "event_id": "e",
            "token_id": "T",
            "status": "OPEN",
            "opened_at": iso(),
            "opened_at_prec": "COLLECTION_ONLY",
            "invalidation_price": "90",
        },
    )
    await repo.put(
        "ana.watch_conditions",
        "risk",
        {
            "cond_id": "risk",
            "event_id": "e",
            "token_id": "T",
            "role": role,
            "status": "FIRED",
            "expr": {"metric": metric, "op": "==", "value": value},
            "created_at": iso(),
            "fired_at": iso(),
            "expires_at": iso(end + timedelta(days=1)),
        },
    )
    tools.get_market_truth = AsyncMock(
        side_effect=AssertionError("Event risk cannot depend on unavailable market")
    )
    result = await tools.dispatch("get_decision", {"event_id": "e", "token_id": "T"}, {})
    assert result["action"] == expected
    assert result["mta_id"] == first["mta_id"]
    tools.get_market_truth.assert_not_called()
    assert (await repo.get("ana.watch_conditions", "risk"))["consumed_by"] == result["dec_id"]
    assert not await repo.list("ops.jobs")
