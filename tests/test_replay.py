import json
from pathlib import Path

import httpx
import pytest
from model_fixture import audited_result, model_arguments
from native_input_fixtures import acknowledge_native_input
from native_session_fixtures import created_native_session, native_session_record
from scheduling_support import RecordingReactionScheduling
from source_support import fixture_http_admission, fixture_versions

from tank.analysis.page_access import LivePageAccess
from tank.replay import (
    ReplayClock,
    ReplayError,
    evaluate_frozen,
    load_inputs,
    recorded_page_access,
    replay_bundle,
)
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository


@pytest.fixture
def packet(tmp_path: Path):
    p = tmp_path / "input.json"
    p.write_text(
        json.dumps(
            {
                "cutoff": "2026-09-24T12:00:00Z",
                "config_version": "test",
                "code_version": "test",
                "registry": {},
                "config": {"thresholds": Settings(_env_file=None).load_config("thresholds")},
                "records": [],
                "tool_results": [],
            }
        )
    )
    return p


def settings():
    return Settings(config_version="test", code_version="test", replay=True)


async def test_sealed_outcomes_only_after_freeze_and_immutable(packet, tmp_path: Path) -> None:
    async def runner(inputs, repo):
        assert not hasattr(inputs, "outcomes")
        return {"action": "WAIT"}

    out = tmp_path / "frozen"
    await replay_bundle(packet, MemoryRepository(), settings(), runner=runner, output_dir=out)
    sealed = tmp_path / "sealed.json"
    sealed.write_text(json.dumps({"assertions": [{"path": ["action"], "equals": "WAIT"}]}))
    assert evaluate_frozen(out / "outputs.json", sealed)["passed"]

    async def changed(inputs, repo):
        return {"action": "START"}

    with pytest.raises(ReplayError, match="immutable"):
        await replay_bundle(packet, MemoryRepository(), settings(), runner=changed, output_dir=out)


async def test_default_runs_validated_tool(packet) -> None:
    data = json.loads(packet.read_text())
    data["config"]["replay_tool_calls"] = [
        {
            "call_id": "one",
            "tool": "get_item",
            "arguments": {"item_id": "item"},
            "available_at": data["cutoff"],
        }
    ]
    data["records"] = [
        {
            "collection": "ingest.source_items",
            "key": "item",
            "available_at": data["cutoff"],
            "record": {"item_id": "item", "text": "Synthetic test item"},
        }
    ]
    packet.write_text(json.dumps(data))
    result = await replay_bundle(packet, MemoryRepository(), settings())
    assert result["output"]["tool_calls"][0]["result"]["text"] == "Synthetic test item"


async def test_missing_captures_explicit(packet) -> None:
    with pytest.raises(ReplayError, match="insufficient_fixture"):
        await replay_bundle(packet, MemoryRepository(), settings())


@pytest.mark.parametrize(
    "change",
    [{"outcomes": {}}, {"code_version": "wrong"}, {"records": [{"available_at": "2027-01-01T00:00:00Z"}]}],
)
def test_reject_contaminated_or_unpinned(packet, change) -> None:
    data = json.loads(packet.read_text())
    data.update(change)
    packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError):
        load_inputs(packet, settings())


def test_clock_and_query_bounds(packet) -> None:
    inputs = load_inputs(packet, settings())
    with pytest.raises(ReplayError):
        inputs.records("x", cutoff="2027-01-01T00:00:00Z")
    clock = ReplayClock("2026-09-24T10:00:00Z", inputs.cutoff)
    with pytest.raises(ReplayError):
        clock.advance("2026-09-24T09:00:00Z")


async def test_bundle_reproduction_reexecutes_captured_tools(packet, tmp_path: Path) -> None:
    from tank.replay import digest, replay_bundle, reproduce_bundle

    data = json.loads(packet.read_text())
    data["config"]["replay_tool_calls"] = [
        {
            "call_id": "one",
            "tool": "get_item",
            "arguments": {"item_id": "item"},
            "available_at": data["cutoff"],
        }
    ]
    data["records"] = [
        {
            "collection": "ingest.source_items",
            "key": "item",
            "available_at": data["cutoff"],
            "record": {"item_id": "item", "text": "Synthetic reproducible input"},
        }
    ]
    root = tmp_path / "replay" / "inputs"
    root.mkdir(parents=True)
    (root / "F-01.json").write_text(json.dumps(data))
    config = settings().model_copy(update={"data_dir": tmp_path})
    first = await replay_bundle(
        root / "F-01.json",
        MemoryRepository(),
        config,
        output_dir=tmp_path / "replay" / "bundles" / digest(data),
    )
    second = await reproduce_bundle(MemoryRepository(), config, digest(data), mode="captured-tools")
    assert second["reproduced"] and second["output_sha256"] == first["output_sha256"]


async def test_reused_repository_cannot_expose_future_row(packet) -> None:
    data = json.loads(packet.read_text())
    data["config"]["replay_tool_calls"] = [
        {
            "call_id": "one",
            "tool": "get_item",
            "arguments": {"item_id": "future"},
            "available_at": data["cutoff"],
        }
    ]
    packet.write_text(json.dumps(data))
    repo = MemoryRepository()
    await repo.put(
        "ingest.source_items",
        "future",
        {"item_id": "future", "observed_at": "2027-01-01T00:00:00Z", "text": "Forbidden future evidence"},
    )
    with pytest.raises(ReplayError, match="empty isolated repository"):
        await replay_bundle(packet, repo, settings())


def test_claimed_availability_cannot_hide_future_observation(packet) -> None:
    data = json.loads(packet.read_text())
    data["records"] = [
        {
            "collection": "ingest.source_items",
            "key": "future",
            "available_at": data["cutoff"],
            "record": {"observed_at": "2027-01-01T00:00:00Z"},
        }
    ]
    packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError, match="post-cutoff observation field"):
        load_inputs(packet, settings())


async def test_replays_mutation_and_remaps_captured_generated_ids(packet) -> None:
    data = json.loads(packet.read_text())
    stamp = data["cutoff"]
    quote = "Synthetic token deployment is now live."
    data["records"] = [
        {
            "collection": "ingest.source_items",
            "key": "item",
            "available_at": stamp,
            "record": {
                "item_id": "item",
                "observed_at": stamp,
                "source_class": "PRIMARY",
                "source_id": "S01",
                "text": quote,
            },
        }
    ]
    extraction = {
        "item_id": "item",
        "is_event": True,
        "statement": quote,
        "certainty": "HIGH",
        "certainty_reason": "Synthetic primary evidence",
        "entities": [],
        "token_ids": [],
        "event_time": {"value": stamp, "precision": "EXACT", "quote": quote},
        "public_at": {"value": stamp, "precision": "EXACT", "basis": "Synthetic capture", "quote": quote},
        "freshness": "FRESH",
        "claims": [
            {
                "type": "STATE",
                "subject": "Synthetic issuer",
                "predicate": "deployed",
                "object": "Synthetic token",
                "state_claim": "LIVE",
                "verified": True,
                "directionality": "NONE",
                "evidence_class": "PRIMARY_FACT",
                "quote": quote,
            }
        ],
        "next_milestone": None,
        "checked_sources": [],
    }
    data["config"]["replay_tool_calls"] = [
        {"call_id": "create", "tool": "record_event", "arguments": extraction, "available_at": stamp},
        {
            "call_id": "read",
            "tool": "get_event",
            "arguments": {"event_id": "ORIGINAL-EVENT-ID"},
            "available_at": stamp,
        },
    ]
    data["tool_results"] = [
        {"call_id": "create", "available_at": stamp, "result": {"event_id": "ORIGINAL-EVENT-ID"}}
    ]
    packet.write_text(json.dumps(data))
    first = await replay_bundle(packet, MemoryRepository(), settings())
    second = await replay_bundle(packet, MemoryRepository(), settings())
    assert first == second
    import re

    assert re.fullmatch(r"EVT-[0-9A-HJKMNP-TV-Z]{26}", first["output"]["tool_calls"][0]["result"]["event_id"])
    assert first["output"]["tool_calls"][1]["result"]["event"]["event_state"] == "LIVE"


def test_masking_preserves_original_and_private_map_is_not_pipeline_input(packet, tmp_path: Path) -> None:
    from tank.replay import prepare_masked_packet

    data = json.loads(packet.read_text())
    data["records"] = [
        {
            "collection": "ingest.source_items",
            "key": "item",
            "available_at": data["cutoff"],
            "record": {
                "item_id": "item",
                "text": "Worldcoin uses WLD. The worldcoins substring is untouched.",
            },
        }
    ]
    packet.write_text(json.dumps(data))
    original = packet.read_bytes()
    masked = tmp_path / "masked" / "input.json"
    private = tmp_path / "private" / "names.json"
    prepare_masked_packet(packet, masked, private, {"Worldcoin": "Protocol K", "WLD": "Asset K"})
    assert packet.read_bytes() == original
    inputs = load_inputs(masked, settings())
    assert (
        inputs.records("ingest.source_items")[0]["text"]
        == "Protocol K uses Asset K. The worldcoins substring is untouched."
    )
    assert json.loads(private.read_text())["replacements"] == {"Worldcoin": "Protocol K", "WLD": "Asset K"}
    assert not hasattr(inputs, "name_map")
    with pytest.raises(ReplayError, match="outside"):
        prepare_masked_packet(packet, masked, tmp_path / "masked" / "private.json", {"WLD": "Asset K"})
    bad = json.loads(masked.read_text())
    bad["config"]["masking_map"] = {"Asset K": "WLD"}
    masked.write_text(json.dumps(bad))
    with pytest.raises(ReplayError, match="outside pipeline"):
        load_inputs(masked, settings())


@pytest.mark.integration
@pytest.mark.parametrize("captured_http_status", [None, 404, 503])
@pytest.mark.parametrize("transient_case", [None, "scope_refusal", "lost_decision_ack"])
async def test_fresh_replay_session_reruns_market_and_decision_code(
    packet, tmp_path: Path, captured_http_status, transient_case
) -> None:
    import asyncio
    import copy

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.event_tools import EventToolService
    from tank.replay import replay_agent_session
    from tank.util import new_id

    earlier = "2026-09-24T11:59:00Z"
    item_id = new_id("ITM")
    event_id = new_id("EVT")
    ems_id = new_id("EMS")
    edge_id = new_id("EDG")
    old_sid = "original-live-session"
    cfg = {
        "registry": {"tokens": [{"token_id": "T", "symbol": "T"}], "entities": [], "instruments": []},
        "sources": {"sources": [{"source_id": "S01", "name": "Synthetic replay source"}]},
        "openma": {
            "prompt_version": "synthetic-replay-prompt",
            "reasoning_effort": "medium",
            "agent_id": "captured-analyst",
            "environment_id": "original-live-env",
            "replay_environment_id": "tank-replay-fixture-env",
        },
        "thresholds": Settings(_env_file=None).load_config("thresholds"),
        "semantics": {},
        "dependency_map": {},
    }

    class ReplaySettings(Settings):
        def load_config(self, name):
            return copy.deepcopy(cfg[name])

    config = ReplaySettings(
        replay=True,
        config_version="test",
        code_version="test",
        openma_url="http://127.0.0.1:9999",
        openma_api_key="FAKE-only",
        _env_file=None,
    )
    data = json.loads(packet.read_text())
    data["registry"] = cfg["registry"]
    data["config"] = {k: v for k, v in cfg.items() if k != "registry"}
    records = [
        (
            "ingest.source_items",
            item_id,
            {
                "item_id": item_id,
                "text": "Synthetic replay source",
                "source_id": "S01",
                "source_class": "PRIMARY",
                "observed_at": earlier,
            },
        ),
        (
            "evt.events",
            event_id,
            {
                "event_id": event_id,
                "event_state": "APPROVED",
                "statement": "Synthetic replay source",
                "entities": [],
                "state_verified": True,
                "freshness": "FRESH",
                "certainty": "HIGH",
                "lifecycle": "ANALYZING",
                "known_to_us_at": earlier,
                "public_at": earlier,
                "tokens_mentioned": ["T"],
                "next_milestone": None,
                "current_ems_id": ems_id,
            },
        ),
        (
            "evt.ems_assessments",
            ems_id,
            {
                "ems_id": ems_id,
                "event_id": event_id,
                "class": "MAJOR",
                "certainty": "HIGH",
                "impact_horizon": {"impact_start": "UNKNOWN"},
            },
        ),
        (
            "evt.beneficiary_maps",
            event_id + ":1",
            {"event_id": event_id, "map_version": 1, "analysis_set": ["T"]},
        ),
        (
            "evt.beneficiary_edges",
            edge_id,
            {
                "edge_id": edge_id,
                "event_id": event_id,
                "token_id": "T",
                "role": "PRIMARY",
                "credible": False,
                "map_version": 1,
                "capture_class": "UNKNOWN",
                "exposure_score": None,
            },
        ),
        (
            "ops.agent_sessions",
            old_sid,
            {
                "session_id": old_sid,
                "event_id": event_id,
                "item_id": item_id,
                "status": "OPEN",
                "runtime_status": "IDLE",
            },
        ),
        (
            "ops.tool_calls",
            "original-live-session:old-decision",
            {
                "call_id": "old-decision",
                "session_id": old_sid,
                "event_id": event_id,
                "tool": "get_decision",
                "arguments": {"event_id": event_id, "token_id": "T"},
                "status": "DONE",
                "result": {"action": "START"},
                "returned_at": earlier,
            },
        ),
    ]
    future_job = new_id("JOB")
    records.append(
        (
            "ops.jobs",
            future_job,
            {
                "job_id": future_job,
                "type": "ESCALATE_TOKENS",
                "payload": {"event_id": event_id, "tokens": ["T"]},
                "status": "QUEUED",
                "priority": 1,
                "attempts": 0,
                "max_attempts": 3,
                "idem_key": "future-market-escalation",
                "run_after": "2026-09-25T12:00:00Z",
                "expires_at": None,
            },
        )
    )
    web_id = new_id("ITM")
    records.extend(
        [
            (
                "ingest.source_items",
                web_id,
                {
                    "item_id": web_id,
                    "text": "Synthetic captured page",
                    "observed_at": earlier,
                    "source_id": "WEB",
                    "session_id": old_sid,
                },
            ),
            (
                "ops.tool_calls",
                old_sid + ":old-web",
                {
                    "call_id": "old-web",
                    "session_id": old_sid,
                    "event_id": "different-original-domain-id",
                    "tool": "fetch_page",
                    "arguments": {"url": "https://synthetic.invalid/captured-page"},
                    "status": "DONE",
                    "result": {"item_id": web_id, "text": "Synthetic captured page"},
                    "returned_at": earlier,
                },
            ),
        ]
    )
    captured_error = {
        "ok": False,
        "error": {
            "type": "HTTPStatusError",
            "reason": f"Source request failed (HTTP {captured_http_status})",
            "category": "SOURCE_HTTP",
        },
    }
    if captured_http_status:
        for collection, key, row in records:
            if collection == "ops.tool_calls" and key == old_sid + ":old-web":
                row.update(status="ERROR", is_error=True, result=captured_error)
    data["records"] = [
        {"collection": collection, "key": key, "available_at": earlier, "record": row}
        for collection, key, row in records
    ]
    packet.write_text(json.dumps(data))
    queue = asyncio.Queue()
    requests = []
    results = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                _order, event = await queue.get()
                yield ("data: " + json.dumps(event) + "\n\n").encode()

        async def aclose(self) -> None:
            pass

    accepted_inputs = []
    decision_ack_bodies = []

    native_records = {}
    native_snapshot = {
        "version": 1,
        "system": "Fixture analyst prompt",
        "skills": [],
        "tools": [],
        "model": {"id": "fixture-model"},
    }

    async def transport(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/executions"):
            assert request.method == "GET"
            ids = request.url.params.get_list("ids[]")
            assert ids and len(ids) <= 64 and set(ids) <= set(accepted_inputs)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"execution_id": identity, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                        for identity in ids
                    ]
                },
            )
        path = request.url.path
        if request.method == "GET" and path.rsplit("/", 1)[-1] in {"fresh-session", "fresh-replay-session"}:
            return httpx.Response(200, json=native_records[path.rsplit("/", 1)[-1]])
        if path.endswith("/sessions"):
            if request.method == "GET":
                foreign = native_session_record(
                    old_sid,
                    agent_id="captured-analyst",
                    environment_id="original-live-env",
                    metadata={},
                    model_id="fixture-model",
                )
                return httpx.Response(200, json={"data": [foreign], "next_page": None})
            requests.append(json.loads(request.content))
            native_records["fresh-replay-session"] = created_native_session(
                request, "fresh-replay-session", agent_snapshot=native_snapshot
            )
            return httpx.Response(201, json=native_records["fresh-replay-session"])
        if path.endswith("/events/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream())
        if path.endswith("/events"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            event = json.loads(request.content)["events"][0]
            if event.get("custom_tool_use_id") == "replay-decision":
                decision_ack_bodies.append(copy.deepcopy(event))
                if transient_case == "lost_decision_ack" and len(decision_ack_bodies) == 1:
                    await queue.put(
                        (
                            4,
                            {
                                "id": "replay-decision",
                                "type": "agent.custom_tool_use",
                                "name": "get_decision",
                                "input": {"token_id": "T"},
                            },
                        )
                    )
                    raise httpx.ReadError("Synthetic lost replay ACK", request=request)
            accepted_id = "accepted-" + str(len(accepted_inputs) + 1)
            accepted_inputs.append(accepted_id)
            if event["type"] == "user.message" and transient_case == "scope_refusal":
                await queue.put(
                    (
                        0,
                        {
                            "id": "replay-scope",
                            "type": "agent.custom_tool_use",
                            "name": "get_decision",
                            "input": {"event_id": new_id("EVT"), "token_id": "T"},
                        },
                    )
                )
            elif event["type"] == "user.message" or event.get("custom_tool_use_id") == "replay-scope":
                if event["type"] != "user.message":
                    refusal = json.loads(event["content"][0]["text"])
                    assert refusal["ok"] is False
                    assert refusal["error"]["category"] == "VALIDATION"
                await queue.put(
                    (
                        1,
                        {
                            "id": "replay-observation",
                            "type": "agent.custom_tool_use",
                            "name": "add_observation",
                            "input": {
                                "link": "CORROBORATION",
                                "claims": [],
                                "public_at": {
                                    "value": earlier,
                                    "precision": "EXACT",
                                    "basis": "ITEM_METADATA",
                                    "quote": None,
                                },
                                "freshness": "FRESH",
                            },
                        },
                    )
                )
            else:
                result = await audited_result(repo, event)
                if event["custom_tool_use_id"] == "replay-observation":
                    assert not event.get("is_error"), result
                    await queue.put(
                        (
                            2,
                            {
                                "id": "replay-truth",
                                "type": "agent.custom_tool_use",
                                "name": "get_market_truth",
                                "input": {"token_ids": ["T"]},
                            },
                        )
                    )
                elif event["custom_tool_use_id"] == "replay-truth":
                    results.append(result)
                    await queue.put(
                        (
                            3,
                            {
                                "id": "replay-web",
                                "type": "agent.custom_tool_use",
                                "name": "fetch_page",
                                "input": {"url": "https://synthetic.invalid/captured-page"},
                            },
                        )
                    )
                elif event["custom_tool_use_id"] == "replay-web":
                    results.append(result)
                    assert bool(event.get("is_error")) == bool(captured_http_status)
                    await queue.put(
                        (
                            4,
                            {
                                "id": "replay-decision",
                                "type": "agent.custom_tool_use",
                                "name": "get_decision",
                                "input": {"token_id": "T"},
                            },
                        )
                    )
                else:
                    results.append(result)
                    await queue.put(
                        (
                            5,
                            {
                                "id": "replay-done",
                                "type": "session.status_idle",
                                "execution_id": accepted_id,
                                "stop_reason": {"type": "end_turn"},
                            },
                        )
                    )
            return httpx.Response(200, json=acknowledge_native_input(request, accepted_id))
        raise AssertionError("Unexpected control-plane request")

    class CapturedMarket:
        async def snapshot(self, token_id, cutoff, anchor=None):
            return {
                "token_id": token_id,
                "cutoff": cutoff,
                "anchor": anchor,
                "flow_bars": [],
                "klines": [],
                "books": [],
                "oi": [],
                "funding": [],
                "liquidations": [],
                "tickers": [],
                "supply": [],
                "semantics": [],
                "instruments": [],
            }

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        reaction_truth = MarketTruthService(repo, config, CapturedMarket(), versions=fixture_versions())
        reaction_scheduler = Scheduler(
            repo,
            config,
            reaction_truth.market,
            reaction_truth,
            thresholds=config.thresholds(),
            versions=fixture_versions(),
        )
        tools = EventToolService(
            repo,
            config,
            reaction_truth,
            DecisionEngine(),
            http=http,
            versions=fixture_versions(),
            scheduler=reaction_scheduler,
            pages=recorded_page_access(load_inputs(packet, config)),
        )
        report = await replay_agent_session(
            packet,
            repo,
            config,
            tools,
            http=http,
            initial_message={
                "kind": "ITEM_ADDED",
                "item_id": item_id,
                "event_id": event_id,
                "replay_at": data["cutoff"],
            },
            replay_run_id="unique-replay-run",
            timeout=5,
            output_path=tmp_path / "frozen-fresh-session.json",
        )
    assert requests[0]["environment_id"] == "tank-replay-fixture-env"
    assert report["session_id"] != old_sid
    assert report["mode"] == "FRESH_OPENMA_SESSION"
    assert report["output"]["decisions"], [
        {"tool": row["tool"], "status": row["status"], "result": row.get("result")}
        for row in await repo.list("ops.tool_calls")
        if row["status"] in {"REFUSED", "ERROR"}
    ]
    assert report["output"]["decisions"][0]["action"] == "WAIT"
    assert results[-1]["action"] == "WAIT"
    assert results[0]["market_truth"][0]["move_class"] == "UNKNOWN"
    if captured_http_status:
        assert results[1] == captured_error
        captured = await repo.get("ops.tool_calls", report["session_id"] + ":replay-web")
        assert captured["status"] == "ERROR" and captured["is_error"] is True
        assert captured["returned_at"]
    else:
        assert results[1]["text"] == "Synthetic captured page"
    assert (await repo.get("ops.jobs", future_job))["status"] == "QUEUED"
    assert (await repo.get("ops.agent_sessions", report["session_id"]))[
        "replay_run_id"
    ] == "unique-replay-run"

    assert report["status"] == "COMPLETED"
    if transient_case == "scope_refusal":
        refused = await repo.get("ops.tool_calls", report["session_id"] + ":replay-scope")
        assert refused["result"]["error"]["category"] == "VALIDATION" and refused["returned_at"]

    decision_receipts = [row for row in report["causal_tool_receipts"] if row["tool"] == "get_decision"]
    assert len(decision_receipts) == 1
    assert decision_receipts[0]["source_call_id"] == "old-decision"
    assert len(await repo.list("ana.decisions")) == 1
    if transient_case == "lost_decision_ack":
        assert len(decision_ack_bodies) == 2 and decision_ack_bodies[0] == decision_ack_bodies[1]


@pytest.mark.parametrize(
    "result",
    [
        None,
        {},
        {"error": None},
        {"accepted": False, "reason": "EVENT_SCOPE", "error": "Use the session event"},
        {"error": {"type": "ReplayError", "reason": None}},
        {"error": {"type": "ValueError", "reason": "insufficient_fixture is merely input text"}},
        {"error": {"type": "HTTPStatusError", "reason": "Source request failed (HTTP 503)"}},
    ],
)
def test_replay_error_boundary_preserves_pending_rows_and_ordinary_refusals(result) -> None:
    from tank.replay import missing_capture_error

    assert not missing_capture_error({"result": result})
    assert missing_capture_error(
        {
            "result": {
                "ok": False,
                "error": {"type": "ReplayError", "reason": "insufficient_fixture: missing original capture"},
            }
        }
    )


async def test_transcript_web_reads_use_only_captured_results(
    packet, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tank.event_tools import EventToolService

    async def forbidden(*args, **kwargs) -> None:
        raise AssertionError("external search reader must never run in replay")

    monkeypatch.setattr(EventToolService, "web_search", forbidden)
    data = json.loads(packet.read_text())
    stamp = data["cutoff"]
    data["config"]["replay_tool_calls"] = [
        {
            "call_id": "search",
            "tool": "web_search",
            "arguments": {"query": "Synthetic replay query"},
            "available_at": stamp,
        }
    ]
    data["tool_results"] = [
        {
            "call_id": "search",
            "tool": "web_search",
            "arguments": {"query": "Synthetic replay query"},
            "available_at": stamp,
            "result": {"results": [{"title": "Synthetic captured result"}]},
        }
    ]
    packet.write_text(json.dumps(data))
    report = await replay_bundle(packet, MemoryRepository(), settings())
    assert report["output"]["tool_calls"][0]["result"]["results"][0]["title"] == "Synthetic captured result"
    data["tool_results"] = []
    packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError, match="captured pre-cutoff"):
        await replay_bundle(packet, MemoryRepository(), settings())


@pytest.mark.parametrize("matching_arguments", [True, False])
async def test_recorded_fetch_result_never_spends_live_quota_or_builds_http(
    packet: Path, matching_arguments: bool
) -> None:
    from unittest.mock import AsyncMock, patch

    from tank.contracts.json import decode_json
    from tank.event_tools import EventToolService
    from tank.http_admission import ConfiguredHttpAdmission
    from tank.request_budget import RequestBudgetScope, SystemBudgetClock
    from tank.util import canonical_json

    data = decode_json(packet.read_bytes())
    assert isinstance(data, dict)
    config = data["config"]
    assert isinstance(config, dict)
    arguments = {"url": "https://api.etherscan.io/v2/api?chainid=1"}
    result = {"item_id": "captured-page", "text": "The captured publication states approval."}
    config["replay_tool_calls"] = [
        {
            "call_id": "captured-fetch",
            "tool": "fetch_page",
            "arguments": arguments,
            "available_at": data["cutoff"],
        }
    ]
    data["tool_results"] = [
        {
            "call_id": "captured-fetch",
            "tool": "fetch_page",
            "arguments": arguments if matching_arguments else {"url": "https://example.org/different"},
            "available_at": data["cutoff"],
            "result": result,
        }
    ]
    packet.write_text(canonical_json(data))
    admission = AsyncMock(side_effect=AssertionError("Live admission is forbidden"))
    acquire = AsyncMock(side_effect=AssertionError("Live quota acquisition is forbidden"))
    feedback = AsyncMock(side_effect=AssertionError("Live feedback is forbidden"))
    wait = AsyncMock(side_effect=AssertionError("Provider wait is forbidden"))
    raw_fetch = AsyncMock(side_effect=AssertionError("Captured tool result must bypass raw page fetching"))
    repo = MemoryRepository()
    with (
        patch.object(ConfiguredHttpAdmission, "admit", admission),
        patch.object(RequestBudgetScope, "acquire_many", acquire),
        patch.object(ConfiguredHttpAdmission, "block", feedback),
        patch.object(SystemBudgetClock, "sleep", wait),
        patch.object(EventToolService, "fetch_page", raw_fetch),
        patch("httpx.AsyncClient", side_effect=AssertionError("Live HTTP client is forbidden")) as clients,
    ):
        if matching_arguments:
            report = await replay_bundle(packet, repo, settings())
            assert report["output"]["tool_calls"] == [
                {"call_id": "captured-fetch", "tool": "fetch_page", "result": result}
            ]
        else:
            with pytest.raises(ReplayError, match="exact captured pre-cutoff external tool arguments"):
                await replay_bundle(packet, repo, settings())
    for trap in (admission, acquire, feedback, wait, raw_fetch):
        trap.assert_not_awaited()
    clients.assert_not_called()
    assert await repo.list("ingest.source_items") == []


def test_late_market_capture_requires_assembly_and_preserves_real_receipt(packet) -> None:
    data = json.loads(packet.read_text())
    received = "2026-10-17T00:00:00Z"
    data["assembled_at"] = "2026-10-17T01:00:00Z"
    data["records"] = [
        {
            "collection": "replay.market_snapshots",
            "available_at": received,
            "record": {
                "token_id": "SYNTHETIC",
                "cutoff": data["cutoff"],
                "klines": [
                    {
                        "open_time": "2026-09-24T11:59:00Z",
                        "interval_s": 60,
                        "received_ts": received,
                        "c": "100",
                        "h": "101",
                    }
                ],
            },
        }
    ]
    packet.write_text(json.dumps(data))
    inputs = load_inputs(packet, settings())
    assert inputs.evidence_mode == "HISTORICAL_RECONSTRUCTION"
    assert inputs.records("replay.market_snapshots")[0]["klines"][0]["received_ts"] == received
    assert not inputs.records("replay.market_snapshots", cutoff="2026-09-24T11:59:30Z")
    del data["assembled_at"]
    packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError, match="assembled_at"):
        load_inputs(packet, settings())
    data["assembled_at"] = "2026-10-16T00:00:00Z"
    packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError, match="assembled_at"):
        load_inputs(packet, settings())


@pytest.mark.parametrize(
    "record",
    [
        {"open_time": "2026-09-24T11:59:30Z", "interval_s": 60, "c": "100"},
        {"ts": "2026-09-24T12:00:01Z", "price": "100"},
        {
            "open_time": "2026-09-24T11:59:00Z",
            "interval_s": 60,
            "close_time": "2026-09-24T12:01:00Z",
            "c": "100",
        },
    ],
)
def test_assembly_never_admits_future_market_or_partial_bar(packet, record) -> None:
    data = json.loads(packet.read_text())
    data["assembled_at"] = "2026-10-17T01:00:00Z"
    data["records"] = [
        {"collection": "mkt.klines_1m", "available_at": "2026-10-17T00:00:00Z", "record": record}
    ]
    packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError, match="post-cutoff market"):
        load_inputs(packet, settings())


def test_assembly_does_not_admit_source_revision(packet) -> None:
    data = json.loads(packet.read_text())
    data["assembled_at"] = "2026-10-17T01:00:00Z"
    data["records"] = [
        {
            "collection": "ingest.source_items",
            "available_at": data["cutoff"],
            "record": {
                "published_at": data["cutoff"],
                "observed_at": "2026-10-17T00:00:00Z",
                "text": "Synthetic revision",
            },
        }
    ]
    packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError, match="post-cutoff"):
        load_inputs(packet, settings())


@pytest.mark.parametrize(
    "case",
    [
        "valid",
        "missing_assembly",
        "early_assembly",
        "future_trade",
        "future_boundary",
        "future_segment",
        "exchange_observation",
        "unknown_observation",
    ],
)
def test_reconstructed_quiet_minute_preserves_collection_clock(packet, case) -> None:
    from copy import deepcopy

    from tank.market import flow, normalize
    from tank.market.trade_contracts import FlowInstrument, QuoteUsdValuation
    from tank.util import parse_iso_time

    received = "2026-10-17T00:00:00Z"
    instrument = FlowInstrument(
        instrument_id="binance:spot:QNTUSDT",
        token_id="quant:QNT",
        venue="binance",
        market_type="SPOT",
        quote_asset="USDT",
        venue_symbol="QNTUSDT",
        contract_multiplier=None,
    )
    valuation = QuoteUsdValuation(
        rate=None, as_of=None, basis="UNAVAILABLE", unavailable_reason="QUOTE_USD_RATE_UNAVAILABLE"
    )
    accumulator = flow.FlowAccumulator(now=lambda: parse_iso_time(received))
    trade = normalize.trade(
        instrument,
        {
            "a": 1,
            "p": "100",
            "q": "2",
            "f": 1,
            "l": 1,
            "T": int(parse_iso_time("2026-09-24T11:58:30Z").timestamp() * 1000),
            "m": False,
        },
        valuation=valuation,
        received_ts=parse_iso_time(received),
        source="ARCHIVE",
    )
    accumulator.add(trade, continuity=False)
    accumulator.prove_range(
        instrument.instrument_id,
        "2026-09-24T11:58:00Z",
        "2026-09-24T12:00:00Z",
        complete=True,
        source="VERIFIED_BINANCE_DAILY_ARCHIVE",
        observed_at=received,
    )
    accumulator.empty_minute(instrument, "2026-09-24T11:59:00Z")
    produced = accumulator.record((instrument.instrument_id, "2026-09-24T11:59:00Z"), complete=True)
    reference = produced.reference_points[0]
    assert reference.complete and reference.observed_at_prec == "COLLECTION_ONLY"
    assert reference.last_trade is not None and reference.last_trade.ts == parse_iso_time(
        "2026-09-24T11:58:30Z"
    )
    bar = produced.model_dump(mode="json", by_alias=True, exclude_unset=True)
    point = bar["reference_points"][0]
    original = deepcopy(bar)
    data = json.loads(packet.read_text())
    data["assembled_at"] = "2026-10-17T01:00:00Z"
    data["records"] = [{"collection": "mkt.flow_bars_1m", "available_at": received, "record": bar}]
    if case == "missing_assembly":
        del data["assembled_at"]
    elif case == "early_assembly":
        point["observed_at"] = "2026-10-17T02:00:00Z"
    elif case == "future_trade":
        point["last_trade"]["ts"] = "2026-09-24T12:00:01Z"
    elif case == "future_boundary":
        point["boundary"] = "2026-09-24T12:00:01Z"
    elif case == "future_segment":
        bar["boundary_segments"] = [{"start": "2026-09-24T11:59:00Z", "end": "2026-09-24T12:00:01Z"}]
    elif case == "exchange_observation":
        point["observed_at_prec"] = "EXACT"
    elif case == "unknown_observation":
        del point["observed_at_prec"]
    packet.write_text(json.dumps(data))
    if case == "valid":
        admitted = load_inputs(packet, settings()).records("mkt.flow_bars_1m")
        assert admitted == [original]
    else:
        with pytest.raises(ReplayError, match="assembled_at|post-cutoff market"):
            load_inputs(packet, settings())


@pytest.mark.parametrize("representation", ["value", "ts"])
@pytest.mark.parametrize(
    "case",
    [
        "collection",
        "missing_assembly",
        "late_receipt",
        "exchange",
        "future_trade",
        "conflicting_values",
        "null_value",
        "null_ts",
    ],
)
def test_nested_market_clocks_cannot_bypass_cutoff_or_receipt_bounds(packet, representation, case) -> None:
    data = json.loads(packet.read_text())
    cutoff = data["cutoff"]
    receipt = "2026-10-17T00:00:00Z"
    observed = {representation: receipt, "precision": "COLLECTION_ONLY"}
    record = {"ts": {representation: cutoff, "precision": "EXACT"}, "observed_at": observed}
    data["assembled_at"] = "2026-10-17T01:00:00Z"
    data["records"] = [{"collection": "mkt.oi_obs", "available_at": cutoff, "record": record}]
    if case == "missing_assembly":
        del data["assembled_at"]
    elif case == "late_receipt":
        record["received_ts"] = {representation: "2026-10-17T02:00:00Z", "precision": "COLLECTION_ONLY"}
    elif case == "exchange":
        observed["precision"] = "EXACT"
    elif case == "future_trade":
        record["ts"][representation] = "2026-09-24T12:00:01Z"
    elif case == "conflicting_values":
        observed["ts" if representation == "value" else "value"] = "2026-10-17T02:00:00Z"
    elif case == "null_value":
        observed.update(value=None, ts=receipt)
    elif case == "null_ts":
        observed.update(value=receipt, ts=None)
    packet.write_text(json.dumps(data))
    if case == "collection":
        assert load_inputs(packet, settings()).records("mkt.oi_obs") == [record]
    else:
        with pytest.raises(ReplayError, match="assembled_at|post-cutoff market|conflicting market timestamp"):
            load_inputs(packet, settings())


async def test_operational_replay_composes_fresh_session_not_captured_tools(
    packet, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tank import replay
    from tank.event_tools import EventToolService

    data = json.loads(packet.read_text())
    messages = [
        {"kind": "NEW_ITEM", "item_id": "first", "replay_source_session_id": "original"},
        {"kind": "ITEM_ADDED", "item_id": "second"},
    ]
    messages[0]["replay_at"] = data["cutoff"]
    data["config"]["replay_messages"] = messages
    packet.write_text(json.dumps(data))
    repo = MemoryRepository()
    config = settings()
    transport = object()

    async def fresh(path, actual_repo, actual_settings, tools, **kwargs):
        assert actual_repo is repo and actual_settings is config
        assert isinstance(tools, EventToolService)
        assert isinstance(tools.truth.market, replay.ReplayMarketReader)
        assert kwargs["http"] is transport
        assert kwargs["initial_message"] == messages[0]
        assert kwargs["subsequent_messages"] == messages[1:]
        return {"mode": "FRESH_OPENMA_SESSION"}

    async def forbidden(*args, **kwargs) -> None:
        raise AssertionError("captured-tool diagnostic entered")

    monkeypatch.setattr(replay, "replay_agent_session", fresh)
    monkeypatch.setattr(replay, "run_captured_tools", forbidden)
    assert (await replay.run_operational_replay(packet, repo, config, http=transport))[
        "mode"
    ] == "FRESH_OPENMA_SESSION"


async def test_full_replay_refuses_missing_original_messages_without_fallback(packet, tmp_path: Path) -> None:
    from tank.replay import replay_fixture

    root = tmp_path / "replay" / "inputs"
    root.mkdir(parents=True)
    (root / "F-01.json").write_text(packet.read_text())
    with pytest.raises(ReplayError, match="missing original replay_messages"):
        await replay_fixture(MemoryRepository(), settings().model_copy(update={"data_dir": tmp_path}), "F-01")


def test_operator_cli_defaults_to_fresh_session_and_diagnostic_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from click.testing import CliRunner

    from tank import cli, replay

    seen = []

    async def fixture(repo, settings, identifier, *, mode="fresh-session") -> None:
        seen.append(("fixture", identifier, mode))

    async def reproduce(repo, settings, identifier, *, mode) -> None:
        seen.append(("bundle", identifier, mode))

    def run(fn, **kwargs) -> None:
        import asyncio

        assert kwargs == {"database_url": "postgresql://replay/isolated", "replay": True}
        asyncio.run(fn(None, None))

    monkeypatch.setattr(cli, "run_db", run)
    monkeypatch.setattr(replay, "replay_fixture", fixture)
    monkeypatch.setattr(replay, "reproduce_bundle", reproduce)
    runner = CliRunner()
    for args in (
        ["replay", "F-01"],
        ["bundle", "reproduce", "a" * 64],
    ):
        result = runner.invoke(cli.main, args + ["--database-url", "postgresql://replay/isolated"])
        assert result.exit_code == 0, result.output
    assert [row[2] for row in seen] == ["fresh-session", "fresh-session"]

    result = runner.invoke(
        cli.main,
        ["replay", "F-01", "--mode", "captured-tools", "--database-url", "postgresql://replay/isolated"],
    )
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.parametrize("timer_divergence", [False, True])
async def test_fresh_multi_message_replay_advances_prefix_without_future_domain_seeding(
    packet, timer_divergence
) -> None:
    import asyncio
    import copy

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.event_tools import EventToolService
    from tank.replay import ReplayMarketReader, replay_agent_session

    first_at, second_at = "2026-09-24T11:59:00Z", "2026-09-24T12:00:00Z"
    cfg = {
        "registry": {"tokens": [], "entities": [], "instruments": []},
        "sources": {"sources": [{"source_id": "S01", "name": "Synthetic replay source"}]},
        "thresholds": Settings(_env_file=None).load_config("thresholds"),
        "semantics": {},
        "dependency_map": {},
        "openma": {
            "prompt_version": "synthetic-replay-prompt",
            "reasoning_effort": "medium",
            "agent_id": "analyst",
            "environment_id": "live",
            "replay_environment_id": "replay",
        },
    }

    class Config(Settings):
        def load_config(self, name):
            return copy.deepcopy(cfg[name])

    config = Config(replay=True, config_version="test", code_version="test", _env_file=None)
    from tank.util import sha256

    retained = json.dumps({"data": {"code": "legacy-replay", "publishDate": first_at}}).encode()
    retained_path = packet.parent / "legacy-detail.json"
    retained_path.write_bytes(retained)
    feed = {"url": "https://captured.example/binance-cms", "kind": "json"}
    cfg["sources"] = {
        "sources": [{"source_id": "S09", "name": "Synthetic captured CMS source", "feeds": [feed]}]
    }
    data = json.loads(packet.read_text())
    data["registry"] = cfg["registry"]
    data["config"] = {key: value for key, value in cfg.items() if key != "registry"}
    data["records"] = [
        {
            "collection": "ingest.source_items",
            "key": item,
            "available_at": at,
            "record": {
                "item_id": item,
                "text": item,
                "source_class": "PRIMARY",
                "observed_at": at,
                **({"observed_at_prec": "BUCKET(1s)"} if item == "first" else {}),
            },
        }
        for item, at in [("first", first_at), ("second", second_at)]
    ] + [
        {
            "collection": "evt.events",
            "key": "future-original-output",
            "available_at": second_at,
            "record": {"event_id": "future-original-output", "statement": "Must be recomputed"},
        }
    ]
    data["records"].append(
        {
            "collection": "ingest.source_items",
            "key": "first",
            "available_at": second_at,
            "record": {
                "item_id": "first",
                "text": "first revised",
                "observed_at": first_at,
                "new_observation": "captured enrichment",
                "source_id": "S09",
                "external_id": "legacy-replay",
                "feed": feed["url"],
                "raw_ref": str(retained_path),
                "raw_sha256": sha256(retained),
                "published_at": first_at,
                "published_prec": "EXACT",
                "published_at_precision": "EXACT",
                "route_event_id": "old-original-event",
                "event_id": "old-original-event",
                "session_id": "old-original-session",
                "extraction_status": "SKIPPED",
            },
        }
    )

    data["records"].extend(
        [
            {
                "collection": "ops.agent_sessions",
                "key": "original-projection-session",
                "available_at": first_at,
                "record": {
                    "session_id": "original-projection-session",
                    "opened_by_item": "first",
                    "event_id": "original-event",
                },
            },
            {
                "collection": "ops.tool_calls",
                "key": "original-projection-session:origin",
                "available_at": first_at,
                "record": {
                    "session_id": "original-projection-session",
                    "call_id": "origin",
                    "tool": "record_event",
                    "status": "DONE",
                    "arguments": {
                        "item_id": "first",
                        "is_event": True,
                        "statement": "first",
                        "certainty": "MEDIUM",
                        "certainty_reason": "Synthetic projection actor",
                        "entities": [],
                        "token_ids": [],
                        "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
                        "public_at": {
                            "value": first_at,
                            "precision": "EXACT",
                            "basis": "Synthetic first collection",
                            "quote": None,
                        },
                        "freshness": "FRESH",
                        "claims": [],
                        "next_milestone": None,
                        "checked_sources": [],
                    },
                    "result": {"event_id": "original-event"},
                },
            },
        ]
    )
    packet.write_text(json.dumps(data))
    repo, queue, sent, results = MemoryRepository(), asyncio.Queue(), [], []
    seq = 0

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                event = await queue.get()
                yield event

    async def emit(event) -> None:
        if event.get("type") == "agent.custom_tool_use":
            event = dict(event, input=model_arguments(event["name"], event["input"]))
        nonlocal seq
        seq += 1
        await queue.put(("data: " + json.dumps(event) + "\n\n").encode())

    accepted_inputs = []
    actor_stage = None
    actor_item = None
    fresh_event_id = None

    native_records = {}
    native_snapshot = {
        "version": 1,
        "system": "Synthetic projection actor",
        "skills": [],
        "tools": [],
        "model": {"id": "fixture-model"},
    }

    async def transport(request: httpx.Request) -> httpx.Response:
        nonlocal actor_stage, actor_item, fresh_event_id
        if request.url.path.endswith("/executions"):
            assert request.method == "GET"
            ids = request.url.params.get_list("ids[]")
            assert ids and len(ids) <= 64 and set(ids) <= set(accepted_inputs)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"execution_id": identity, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                        for identity in ids
                    ]
                },
            )
        if request.method == "GET" and request.url.path.endswith("/sessions/new-session"):
            return httpx.Response(200, json=native_records["new-session"])
        if request.url.path.endswith("/sessions"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            assert json.loads(request.content)["environment_id"] == "replay"
            native_records["new-session"] = created_native_session(
                request, "new-session", agent_snapshot=native_snapshot
            )
            return httpx.Response(201, json=native_records["new-session"])
        if request.url.path.endswith("/events/stream"):
            return httpx.Response(200, stream=Stream(), headers={"content-type": "text/event-stream"})
        if request.url.path.endswith("/archive"):
            return httpx.Response(200, json={})
        if request.url.path.endswith("/events"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            event = json.loads(request.content)["events"][0]
            accepted_id = "accepted-" + str(len(accepted_inputs) + 1)
            accepted_inputs.append(accepted_id)
            if event["type"] == "user.message":
                content = json.loads(event["content"][0]["text"])
                item = content["item"]["text"]
                if not sent:
                    assert await repo.get("ingest.source_items", "second") is None
                    original = await repo.get("ingest.source_items", "first")
                    assert "new_observation" not in original

                else:
                    assert await repo.get("ingest.source_items", "second") is not None
                    revised = await repo.get("ingest.source_items", "first")
                    assert revised["new_observation"] == "captured enrichment"
                    assert revised["route_event_id"] == fresh_event_id
                    assert revised["extraction_status"] == "DONE"
                    assert "event_id" not in revised and "session_id" not in revised
                assert await repo.get("evt.events", "future-original-output") is None
                actor_item, actor_stage = item, "get"
                sent.append(item)
                await emit(
                    {
                        "id": "get-" + item,
                        "type": "agent.custom_tool_use",
                        "name": "get_item",
                        "input": {"item_id": "first" if item == "second" else item},
                    }
                )
            else:
                result = await audited_result(repo, event)
                if actor_stage == "get":
                    results.append(result)
                    actor_stage = "origin" if actor_item == "first" else "observation"
                    arguments = (
                        {
                            "item_id": actor_item,
                            "is_event": True,
                            "statement": "first",
                            "certainty": "MEDIUM",
                            "certainty_reason": "Synthetic projection actor",
                            "entities": [],
                            "token_ids": [],
                            "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
                            "public_at": {
                                "value": first_at,
                                "precision": "EXACT",
                                "basis": "Synthetic first collection",
                                "quote": None,
                            },
                            "freshness": "FRESH",
                            "claims": [],
                            "next_milestone": None,
                            "checked_sources": [],
                        }
                        if actor_item == "first"
                        else {
                            "item_id": actor_item,
                            "event_id": fresh_event_id,
                            "link": "CORROBORATION",
                            "claims": [],
                            "public_at": {
                                "value": first_at,
                                "precision": "EXACT",
                                "basis": "Synthetic first collection",
                                "quote": None,
                            },
                            "freshness": "FRESH",
                        }
                    )
                    await emit(
                        {
                            "id": actor_stage + "-" + actor_item,
                            "type": "agent.custom_tool_use",
                            "execution_id": accepted_id,
                            "name": "record_event" if actor_item == "first" else "add_observation",
                            "input": arguments,
                        }
                    )
                elif actor_stage == "origin":
                    from test_event_tools import unknown_assessment

                    assert result.get("event_id"), result
                    fresh_event_id, actor_stage = result["event_id"], "assessment"

                    source = await repo.get("ingest.source_items", "first")
                    await repo.put(
                        "ingest.source_items", "first", dict(source, route_event_id=fresh_event_id)
                    )
                    await emit(
                        {
                            "id": "assessment-first",
                            "type": "agent.custom_tool_use",
                            "execution_id": accepted_id,
                            "name": "set_assessment",
                            "input": {
                                "event_id": fresh_event_id,
                                "assessment": unknown_assessment(klass="MATERIAL"),
                            },
                        }
                    )
                else:
                    assert (
                        result.get("class") in {"LOW", "MATERIAL"}
                        or result.get("link_type") == "CORROBORATION"
                    ), result
                    await emit(
                        {
                            "id": "idle-" + str(len(results)),
                            "type": "session.status_idle",
                            "execution_id": accepted_id,
                            "stop_reason": {"type": "end_turn"},
                        }
                    )
            return httpx.Response(200, json=acknowledge_native_input(request, accepted_id))
        raise AssertionError("Unexpected request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        reaction_truth = MarketTruthService(
            repo, config, ReplayMarketReader(load_inputs(packet, config)), versions=fixture_versions()
        )
        reaction_scheduler = Scheduler(
            repo,
            config,
            reaction_truth.market,
            reaction_truth,
            thresholds=config.thresholds(),
            versions=fixture_versions(),
        )
        tools = EventToolService(
            repo,
            config,
            reaction_truth,
            DecisionEngine(),
            versions=fixture_versions(),
            scheduler=reaction_scheduler,
            pages=recorded_page_access(load_inputs(packet, config)),
        )
        report = await replay_agent_session(
            packet,
            repo,
            config,
            tools,
            http=http,
            initial_message={"kind": "NEW_ITEM", "item_id": "first"},
            subsequent_messages=[{"kind": "ITEM_ADDED", "item_id": "second"}]
            + (
                [
                    {
                        "kind": "CALL_EXPIRED",
                        "replay_at": second_at,
                        "event_id": "original-event",
                        "call": {
                            "call_id": "original-call",
                            "event_id": "original-event",
                            "token_id": "T",
                            "status": "CLOSED",
                        },
                    }
                ]
                if timer_divergence
                else []
            ),
            timeout=3,
        )
    assert report["status"] == ("DIVERGED" if timer_divergence else "COMPLETED")
    if timer_divergence:
        assert report["divergence"]["original_message"]["kind"] == "CALL_EXPIRED"
        assert "unique fresh counterpart" in report["divergence"]["reason"]
    assert sent == ["first", "second"]
    assert [row["item_id"] for row in results] == ["first", "first"]
    assert report["cleanup_sessions"] == [{"session_id": "new-session", "archived": True}]
    session = await repo.get("ops.agent_sessions", "new-session")
    assert [row["sent_at"] for row in session["message_log"]] == [first_at, second_at]
    assert session["opened_at_prec"] == "BUCKET(1s)"
    assert session["message_log"][0]["sent_at_prec"] == "BUCKET(1s)"
    assert session["last_item_at_prec"] is None
    assert session["last_item_at_prec_reason"] == "INCOMPLETE"


def test_original_messages_use_durable_log_not_inferred_tool_calls(packet) -> None:
    from tank.replay import original_replay_messages

    data = json.loads(packet.read_text())
    data["records"] = [
        {
            "collection": "ops.agent_sessions",
            "key": "old",
            "available_at": data["cutoff"],
            "record": {
                "session_id": "old",
                "message_log": [
                    {
                        "sent_at": data["cutoff"],
                        "payload": {"kind": "NEW_ITEM", "item_id": "real", "session_id": "old"},
                    }
                ],
            },
        }
    ]
    packet.write_text(json.dumps(data))
    messages = original_replay_messages(load_inputs(packet, settings()))
    assert messages == [
        {
            "kind": "NEW_ITEM",
            "item_id": "real",
            "replay_source_session_id": "old",
            "replay_at": data["cutoff"],
            "replay_at_prec": None,
            "replay_at_prec_reason": "INCOMPLETE",
        }
    ]
    assert data["records"][0]["record"]["message_log"][0]["payload"]["session_id"] == "old"


@pytest.mark.parametrize("change", [None, "action", "thesis_grade", "gate", "window", "evidence"])
def test_semantic_verdict_comparison_declares_identity_only_changes(change) -> None:
    import copy

    from tank.replay import semantic_verdict_output

    reference = {
        "decisions": [
            {
                "dec_id": "DEC-original",
                "event_id": "EVT-original",
                "token_id": "world:WLD",
                "action": "WAIT",
                "thesis_grade": "B",
                "trade_grade": "C",
                "cutoff": "2026-09-24T12:00:00Z",
                "provenance": {"mta_id": "MTA-original"},
                "evidence": [{"item_id": "ITM-real-source", "raw_sha256": "same-proof"}],
            }
        ],
        "alerts": [
            {
                "alert_id": "AL-original",
                "event_id": "EVT-original",
                "decisions": ["DEC-original"],
                "session_id": "native-original",
                "card_text": "Original actor wording",
                "gate": {"allowed": True},
                "shown_tokens": ["world:WLD"],
            }
        ],
        "verdict_inputs": [{"mta_id": "MTA-original", "windows": {"1h": {"price_change": 2}}}],
    }
    actual = copy.deepcopy(reference)
    actual["decisions"][0].update(dec_id="DEC-new", event_id="EVT-new", provenance={"mta_id": "MTA-new"})
    actual["alerts"][0].update(
        alert_id="AL-new",
        event_id="EVT-new",
        decisions=["DEC-new"],
        session_id="native-new",
        card_text="Different actor wording",
    )
    actual["verdict_inputs"][0]["mta_id"] = "MTA-new"
    if change == "action":
        actual["decisions"][0]["action"] = "START"
    elif change == "thesis_grade":
        actual["decisions"][0]["thesis_grade"] = "A"
    elif change == "gate":
        actual["alerts"][0]["gate"]["allowed"] = False
    elif change == "window":
        actual["verdict_inputs"][0]["windows"]["1h"]["price_change"] = 3
    elif change == "evidence":
        actual["decisions"][0]["evidence"][0]["raw_sha256"] = "different-proof"
    assert (semantic_verdict_output(reference) == semantic_verdict_output(actual)) == (change is None)


@pytest.mark.parametrize("wall_timeout", [None, 75])
async def test_operational_replay_accepts_recorded_timer_messages_without_item(
    packet, monkeypatch: pytest.MonkeyPatch, wall_timeout
) -> None:
    from tank import replay

    data = json.loads(packet.read_text())
    messages = [{"kind": "NEW_ITEM", "item_id": "one"}] + [
        {"kind": kind, "event_id": "original-event", "replay_at": data["cutoff"]}
        for kind in ["CONDITION", "CONDITION_FIRED", "MILESTONE", "CALL_EXPIRED"]
    ]
    messages[0]["replay_at"] = data["cutoff"]
    data["config"]["replay_messages"] = messages
    if wall_timeout is not None:
        data["config"]["replay_timeout_seconds"] = wall_timeout
    packet.write_text(json.dumps(data))

    async def fresh(*args, **kwargs):
        assert kwargs["timeout"] == (480 if wall_timeout is None else wall_timeout)
        assert kwargs["subsequent_messages"] == messages[1:]
        return {"mode": "FRESH_OPENMA_SESSION"}

    monkeypatch.setattr(replay, "replay_agent_session", fresh)
    await replay.run_operational_replay(packet, MemoryRepository(), settings(), http=object())


@pytest.mark.parametrize(
    "object_kind,id_field,collection",
    [
        ("call", "call_id", "ana.calls"),
        ("condition", "cond_id", "ana.watch_conditions"),
    ],
)
async def test_timer_references_resolve_actual_fresh_lineage_not_old_snapshot(
    packet, object_kind, id_field, collection
) -> None:
    from tank.replay import resolve_replay_message_references

    old = {
        id_field: "old-id",
        "event_id": "old-event",
        "token_id": "T",
        "status": "CLOSED" if object_kind == "call" else "FIRED",
    }
    if object_kind == "call":
        old.update(
            opened_at="2026-09-24T11:00:00Z", opened_by_version=1, entry_ref_price=10, invalidation_price=8
        )
    else:
        old.update(
            role="START",
            expr={"metric": "price", "op": ">", "value": 10},
            created_at="2026-09-24T11:00:00Z",
            expires_at="2026-09-24T12:00:00Z",
        )
    data = json.loads(packet.read_text())
    data["records"] = [
        {"collection": collection, "key": "old-id", "available_at": data["cutoff"], "record": old}
    ]
    packet.write_text(json.dumps(data))
    inputs, repo = load_inputs(packet, settings()), MemoryRepository()
    fresh = dict(
        old,
        **{
            id_field: "fresh-id",
            "event_id": "fresh-event",
            "status": "OPEN" if object_kind == "call" else "ARMED",
        },
    )
    await repo.put(collection, "fresh-id", fresh)
    message = {
        "kind": "CALL_EXPIRED" if object_kind == "call" else "CONDITION_FIRED",
        "event_id": "old-event",
        object_kind: old,
    }
    mapping = {"old-event": "fresh-event"}
    actual = await resolve_replay_message_references(message, inputs, repo, mapping)
    assert actual["event_id"] == "fresh-event"
    assert actual[object_kind] == fresh
    assert mapping["old-id"] == "fresh-id"
    assert old["event_id"] == "old-event"
    with pytest.raises(ReplayError, match="fresh session|unique fresh counterpart"):
        await resolve_replay_message_references(
            message, inputs, repo, {"old-event": "fresh-event"}, event_scope="unrelated-event"
        )
    await repo.put(collection, "second-fresh", dict(fresh, **{id_field: "another-id"}))
    with pytest.raises(ReplayError, match="unique fresh counterpart"):
        await resolve_replay_message_references(message, inputs, repo, {"old-event": "fresh-event"})
    with pytest.raises(ReplayError, match="unique fresh counterpart"):
        await resolve_replay_message_references(
            message, inputs, MemoryRepository(), {"old-event": "fresh-event"}
        )


def test_virtual_clock_preserves_selected_precision_and_does_not_invent_exactness() -> None:
    from datetime import timedelta

    from tank.time_precision import offset_schedule, propagate_timestamp

    start = propagate_timestamp({"at": "2026-10-01T00:00:00Z", "at_prec": "BUCKET(1d)"}, "at")
    clock = ReplayClock(start, "2026-10-03T00:00:00Z")
    assert clock.timestamp().precision == "BUCKET(1d)"
    clock.advance(offset_schedule(start, timedelta(hours=2)))
    assert clock.timestamp().value == "2026-10-01T02:00:00Z"
    assert clock.timestamp().precision == "BUCKET(1d)"
    clock.advance("2026-10-02T00:00:00.123456Z")
    assert clock.timestamp().precision is None
    assert clock.timestamp().precision_reason == "INCOMPLETE"


async def test_timer_evidence_maps_only_exact_accepted_claim_counterpart(packet) -> None:
    from tank.replay import resolve_replay_message_references

    old = {
        "claim_id": "old-claim",
        "event_id": "old-event",
        "subject": "Protocol",
        "state_claim": "APPROVED",
        "evidence": [{"item_id": "source", "quote": "Proposal approved."}],
        "accepted": True,
        "verified": True,
    }
    data = json.loads(packet.read_text())
    data["records"] = [
        {"collection": "evt.claims", "key": "old-claim", "available_at": data["cutoff"], "record": old}
    ]
    packet.write_text(json.dumps(data))
    inputs, repo = load_inputs(packet, settings()), MemoryRepository()
    fresh = dict(old, claim_id="fresh-claim", event_id="fresh-event")
    await repo.put("evt.claims", "fresh-claim", fresh)
    message = {"kind": "MILESTONE", "event_id": "old-event", "evidence": ["old-claim"]}
    actual = await resolve_replay_message_references(
        message, inputs, repo, {"old-event": "fresh-event"}, event_scope="fresh-event"
    )
    assert actual["evidence"] == ["fresh-claim"]
    await repo.put(
        "evt.claims",
        "fresh-claim",
        dict(fresh, evidence=[{"item_id": "source", "quote": "A different decision."}]),
    )
    with pytest.raises(ReplayError, match="unique fresh counterpart"):
        await resolve_replay_message_references(
            message, inputs, repo, {"old-event": "fresh-event"}, event_scope="fresh-event"
        )


@pytest.mark.integration
@pytest.mark.parametrize("premature_trigger", [False, True])
async def test_native_replay_captured_delivery_creates_fresh_call_and_reruns_expiry(
    packet, premature_trigger
) -> None:
    """Synthetic transport: actual delivery/call/expiry code, no Telegram or model HTTP."""
    import asyncio
    import copy

    from test_event_tools import (
        complete_assessment_fixture,
        complete_decision_fixture,
        complete_event_fixture,
        complete_truth_fixture,
        primary_only_map_fixture,
    )

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.event_tools import EventToolService
    from tank.replay import ReplayMarketReader, replay_agent_session

    initial_at, sent_at = "2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z"
    trigger = "2026-09-08T00:00:00Z" if premature_trigger else "2026-09-09T00:00:00Z"
    cutoff = "2026-09-10T00:00:00Z"
    cfg = {
        "registry": {"tokens": [{"token_id": "T", "symbol": "T"}], "entities": [], "instruments": []},
        "sources": {"sources": [{"source_id": "S01", "name": "Synthetic replay source"}]},
        "thresholds": {**Settings(_env_file=None).load_config("thresholds"), "call_expiry_days": 7},
        "semantics": {},
        "dependency_map": {},
        "openma": {
            "prompt_version": "synthetic-replay-prompt",
            "reasoning_effort": "medium",
            "agent_id": "analyst",
            "environment_id": "live",
            "replay_environment_id": "replay",
        },
    }

    class Config(Settings):
        def load_config(self, name):
            return copy.deepcopy(cfg[name])

    config = Config(replay=True, config_version="test", code_version="test", _env_file=None)
    decision = {
        "dec_id": "original-dec",
        "event_id": "event",
        "token_id": "T",
        "action": "START",
        "entry_ref_price": 10,
        "invalidation_price": 8,
    }
    alert = {
        "alert_id": "original-alert",
        "version": 1,
        "kind": "FULL",
        "event_id": "event",
        "generated_at": initial_at,
        "card_text": "Synthetic frozen card",
        "decisions": ["original-dec"],
        "shown_tokens": ["T"],
        "card_struct": {"assessment": {"class": "MAJOR"}},
    }
    canonical_event = complete_event_fixture(
        "event",
        public_at=initial_at,
        known_to_us_at=initial_at,
        lifecycle="ANALYZING",
        current_ems_id="fixture-ems",
    )
    canonical_ems = complete_assessment_fixture(
        "fixture-ems", "event", cutoff=initial_at, **{"class": "MAJOR"}
    )
    canonical_truth = complete_truth_fixture("fixture-truth", "event", "T", initial_at)
    complete_decision = complete_decision_fixture("original-dec", "event", "T", initial_at, "fixture-truth")
    complete_decision.update(decision, cutoff=initial_at)
    decision = complete_decision
    alert["card_struct"] = {"event": dict(canonical_event, contradictions=[]), "assessment": canonical_ems}
    old_call = {
        "call_id": "original-call",
        "event_id": "event",
        "token_id": "T",
        "opened_by_alert": "original-alert",
        "opened_by_version": 1,
        "opened_at": sent_at,
        "entry_ref_price": 10,
        "invalidation_price": 8,
        "status": "CLOSED",
        "closed_by": "EXPIRED",
    }
    records = [
        (
            "ingest.source_items",
            "source",
            {"item_id": "source", "text": "Synthetic source", "observed_at": initial_at},
            initial_at,
        ),
        (
            "evt.events",
            "event",
            canonical_event,
            initial_at,
        ),
        ("ana.decisions", "original-dec", decision, initial_at),
        ("out.alerts", "original-alert:1", alert, initial_at),
        (
            "ops.agent_sessions",
            "original-session",
            {"session_id": "original-session", "event_id": "event", "item_id": "source"},
            initial_at,
        ),
        (
            "ops.jobs",
            "delivery-job",
            {
                "job_id": "delivery-job",
                "idem_key": "original-alert:1:telegram:alerts",
                "type": "DELIVER",
                "status": "QUEUED",
                "payload": {"alert_id": "original-alert", "version": 1, "channel": "telegram:alerts"},
            },
            initial_at,
        ),
        (
            "out.deliveries",
            "original-sent",
            {
                "alert_id": "original-alert",
                "version": 1,
                "channel": "telegram:alerts",
                "status": "SENT",
                "message_ref": "123",
                "at": sent_at,
                "sent_at": sent_at,
                "sent_at_prec": "COLLECTION_ONLY",
            },
            sent_at,
        ),
        ("ana.calls", "original-call", old_call, trigger),
    ]
    records.extend(
        [
            ("evt.ems_assessments", "fixture-ems", canonical_ems, initial_at),
            ("ana.market_truth", "fixture-truth", canonical_truth, initial_at),
            (
                "evt.beneficiary_maps",
                "event:1",
                primary_only_map_fixture("event", "T"),
                initial_at,
            ),
            (
                "evt.beneficiary_edges",
                "fixture-edge",
                {
                    "edge_id": "fixture-edge",
                    "event_id": "event",
                    "map_version": 1,
                    "token_id": "T",
                    "role": "PRIMARY",
                    "tradable": False,
                    "layer": "ISSUER_PRODUCT",
                    "evidence": [],
                    "causal_chain": "The authored primary event is monitored without a capture estimate.",
                    "credible": False,
                    "capture_class": "UNKNOWN",
                    "exposure_score": 0,
                },
                initial_at,
            ),
        ]
    )
    if not premature_trigger:
        records.extend(
            [
                (
                    "out.alerts",
                    "original-alert:2",
                    dict(alert, version=2, kind="UPDATE", generated_at=trigger),
                    trigger,
                ),
                (
                    "out.deliveries",
                    "original-update-sent",
                    {
                        "alert_id": "original-alert",
                        "version": 2,
                        "channel": "telegram:alerts",
                        "status": "SENT",
                        "message_ref": "123",
                        "at": trigger,
                        "sent_at": trigger,
                        "sent_at_prec": "COLLECTION_ONLY",
                    },
                    trigger,
                ),
            ]
        )
    data = json.loads(packet.read_text())
    data.update(
        cutoff=cutoff,
        registry=cfg["registry"],
        config={key: value for key, value in cfg.items() if key != "registry"},
        records=[
            {"collection": collection, "key": key, "record": row, "available_at": at}
            for collection, key, row, at in records
        ],
    )
    packet.write_text(json.dumps(data))
    queue, messages, seq = asyncio.Queue(), [], 0

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                yield await queue.get()

    async def emit(event) -> None:
        if event.get("type") == "agent.custom_tool_use":
            event = dict(event, input=model_arguments(event["name"], event["input"]))
        nonlocal seq
        seq += 1
        await queue.put(("data: " + json.dumps(event) + "\n\n").encode())

    accepted_inputs = []
    actor_stage = None

    native_records = {}
    native_snapshot = {
        "version": 1,
        "system": "Fixture analyst prompt",
        "skills": [],
        "tools": [],
        "model": {"id": "fixture-model"},
    }

    async def control(request: httpx.Request) -> httpx.Response:
        nonlocal actor_stage
        if request.url.path.endswith("/executions"):
            assert request.method == "GET"
            ids = request.url.params.get_list("ids[]")
            assert ids and len(ids) <= 64 and set(ids) <= set(accepted_inputs)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"execution_id": identity, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                        for identity in ids
                    ]
                },
            )
        assert request.url.host != "api.telegram.org", (
            "Delivery must use its separate captured-only transport"
        )
        path = request.url.path
        if request.method == "GET" and path.rsplit("/", 1)[-1] in {"fresh-session", "fresh-replay-session"}:
            return httpx.Response(200, json=native_records[path.rsplit("/", 1)[-1]])
        if path.endswith("/sessions"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            native_records["fresh-session"] = created_native_session(
                request, "fresh-session", agent_snapshot=native_snapshot
            )
            return httpx.Response(201, json=native_records["fresh-session"])
        if path.endswith("/events/stream"):
            return httpx.Response(200, stream=Stream(), headers={"content-type": "text/event-stream"})
        if path.endswith("/archive"):
            return httpx.Response(200, json={})
        if path.endswith("/events"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            event = json.loads(request.content)["events"][0]
            accepted_id = "accepted-" + str(len(accepted_inputs) + 1)
            accepted_inputs.append(accepted_id)
            if event["type"] == "user.message":
                message = json.loads(event["content"][0]["text"])["message"]
                messages.append(message)
                actor_stage = (
                    "observation" if message.get("kind") in {"NEW_ITEM", "ITEM_ADDED"} else "decision"
                )
                await emit(
                    {
                        "id": "actor-" + actor_stage + "-" + str(len(messages)),
                        "type": "agent.custom_tool_use",
                        "execution_id": accepted_id,
                        "name": "add_observation"
                        if message.get("kind") in {"NEW_ITEM", "ITEM_ADDED"}
                        else "get_decision",
                        "input": {
                            "item_id": "source",
                            "event_id": "event",
                            "link": "CORROBORATION",
                            "claims": [],
                            "public_at": {
                                "value": initial_at,
                                "precision": "EXACT",
                                "basis": "Synthetic captured source",
                                "quote": None,
                            },
                            "freshness": "FRESH",
                        }
                        if message.get("kind") in {"NEW_ITEM", "ITEM_ADDED"}
                        else {"event_id": "event", "token_id": "T"},
                    }
                )
            else:
                result = await audited_result(repo, event)
                assert not result.get("error"), result
                if actor_stage == "decision":
                    actor_stage = "publication"
                    await emit(
                        {
                            "id": "actor-update",
                            "type": "agent.custom_tool_use",
                            "execution_id": accepted_id,
                            "name": "publish_card",
                            "input": {
                                "event_id": "event",
                                "kind": "UPDATE",
                                "what": "Synthetic expiry update",
                                "verdict": "Synthetic expiry update",
                                "why": [
                                    "Tracking ended at the configured expiry",
                                    "No later material source fact was supplied",
                                ],
                            },
                        }
                    )
                else:
                    if actor_stage == "publication":
                        assert result.get("publication_receipt", {}).get("disposition") in {
                            "PUBLISHED",
                            "SUPPRESSED",
                        }, result
                    await emit(
                        {
                            "id": "idle-" + str(len(messages)),
                            "type": "session.status_idle",
                            "execution_id": accepted_id,
                            "stop_reason": {"type": "end_turn"},
                        }
                    )
            return httpx.Response(200, json=acknowledge_native_input(request, accepted_id))
        raise AssertionError("Unexpected control-plane request")

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(control)) as http:
        reaction_truth = MarketTruthService(
            repo, config, ReplayMarketReader(load_inputs(packet, config)), versions=fixture_versions()
        )
        reaction_scheduler = Scheduler(
            repo,
            config,
            reaction_truth.market,
            reaction_truth,
            thresholds=config.thresholds(),
            versions=fixture_versions(),
        )
        tools = EventToolService(
            repo,
            config,
            reaction_truth,
            DecisionEngine(),
            versions=fixture_versions(),
            scheduler=reaction_scheduler,
            pages=recorded_page_access(load_inputs(packet, config)),
        )
        from pydantic import ValidationError

        schema_errors: list[str] = []
        publish = tools.publish_card

        async def inspected_publication(arguments, context):
            try:
                return await publish(arguments, context)
            except ValidationError as error:
                schema_errors.append(str(error))
                raise

        tools.publish_card = inspected_publication
        try:
            report = await replay_agent_session(
                packet,
                repo,
                config,
                tools,
                http=http,
                initial_message={
                    "kind": "ITEM_ADDED",
                    "item_id": "source",
                    "event_id": "event",
                    "replay_at": initial_at,
                },
                subsequent_messages=[
                    {"kind": "CALL_EXPIRED", "event_id": "event", "call": old_call, "replay_at": trigger}
                ],
                timeout=3,
            )
        except TimeoutError:
            failures = [
                {"tool": row["tool"], "status": row["status"], "result": row.get("result")}
                for row in await repo.list("ops.tool_calls")
                if row["status"] in {"REFUSED", "ERROR"}
            ]
            assert not schema_errors, schema_errors
            assert not failures, failures
            raise
    calls = await repo.list("ana.calls")
    assert len(calls) == 1 and calls[0]["call_id"] != "original-call"
    assert calls[0]["opened_at"] == sent_at
    assert report["delivery_receipts"][0]["transport"] == "CAPTURED_RECEIPT_NO_NETWORK"
    assert report["delivery_receipts"][0]["call_bindings"] == {"original-call": calls[0]["call_id"]}
    assert report["status"] == ("DIVERGED" if premature_trigger else "COMPLETED"), report.get("divergence")
    if premature_trigger:
        assert calls[0]["status"] == "OPEN"
        assert len(messages) == 1
        assert "predicate does not fire" in report["divergence"]["reason"]
    else:
        assert calls[0]["status"] == "CLOSED" and calls[0]["closed_by"] == "EXPIRED"
        assert "call_id" not in messages[1]["call"]
        assert messages[1]["call"]["opened_at"] == calls[0]["opened_at"]
        assert report["timer_receipts"][0]["predicate"] == "RERUN_PRODUCTION_SCHEDULER"
    assert await repo.get("ana.calls", "original-call") is None


@pytest.mark.integration
@pytest.mark.parametrize("state", ["APPROVED", "PROPOSED"])
async def test_native_condition_replay_uses_tool_cause_and_actual_watch_predicate(packet, state) -> None:
    """Fixture grade, real callback ledger/reconcile/watch execution; no live requests."""
    import asyncio
    import copy

    from test_event_tools import (
        complete_assessment_fixture,
        complete_decision_fixture,
        complete_event_fixture,
        complete_truth_fixture,
    )

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.event_tools import EventToolService
    from tank.replay import ReplayMarketReader, replay_agent_session

    start, trigger, cutoff = "2026-09-01T00:00:00Z", "2026-09-01T01:00:00Z", "2026-09-10T00:00:00Z"
    expr = {
        "all": [
            {
                "metric": "event.state",
                "op": "==",
                "value": "APPROVED",
                "window": "W1H",
                "persist": {"count": 1, "unit": "consecutive_windows"},
            }
        ]
    }
    old = {
        "cond_id": "old-condition",
        "event_id": "event",
        "token_id": "T",
        "role": "UPGRADE",
        "expr": expr,
        "created_at": start,
        "expires_at": "2026-09-07T00:00:00Z",
        "status": "FIRED",
        "created_by": "old-decision",
    }
    cfg = {
        "registry": {"tokens": [{"token_id": "T", "symbol": "T"}], "entities": [], "instruments": []},
        "sources": {"sources": [{"source_id": "S01", "name": "Synthetic replay source"}]},
        "thresholds": Settings(_env_file=None).load_config("thresholds"),
        "semantics": {},
        "dependency_map": {},
        "openma": {
            "prompt_version": "synthetic-replay-prompt",
            "reasoning_effort": "medium",
            "agent_id": "analyst",
            "environment_id": "live",
            "replay_environment_id": "replay",
        },
    }

    class Config(Settings):
        def load_config(self, name):
            return copy.deepcopy(cfg[name])

    config = Config(replay=True, config_version="test", code_version="test", _env_file=None)
    canonical_event = complete_event_fixture(
        "event",
        event_state=state,
        lifecycle="ANALYZING",
        public_at=start,
        known_to_us_at=start,
        current_ems_id="fixture-ems",
    )
    canonical_ems = complete_assessment_fixture("fixture-ems", "event", cutoff=start, **{"class": "MAJOR"})
    canonical_truth = complete_truth_fixture("fixture-truth", "event", "T", start)
    baseline_decision = complete_decision_fixture("baseline-decision", "event", "T", start, "fixture-truth")
    from tank.analysis.decision_codec import decode_decision, encode_decision
    from tank.analysis.decision_contracts import DecisionRecord

    baseline_record = decode_decision(baseline_decision)
    baseline_decision = encode_decision(
        DecisionRecord.model_validate(
            baseline_record.model_copy(update={"conditions": [], "conditions_shown": ()})
        )
    )
    rows = [
        (
            "ingest.source_items",
            "source",
            {"item_id": "source", "observed_at": start, "text": "Synthetic source"},
            start,
        ),
        ("evt.events", "event", canonical_event, start),
        (
            "ops.agent_sessions",
            "old-session",
            {"session_id": "old-session", "item_id": "source", "event_id": "event"},
            start,
        ),
        ("ana.watch_conditions", "old-condition", old, trigger),
        (
            "ops.tool_calls",
            "old-session:decision",
            {
                "call_id": "decision",
                "session_id": "old-session",
                "tool": "get_decision",
                "status": "DONE",
                "arguments": {"event_id": "event", "token_id": "T"},
                "result": {"dec_id": "old-decision", "event_id": "event", "conditions": [old]},
            },
            trigger,
        ),
    ]
    rows.extend(
        [
            (
                "replay.market_snapshots",
                "trigger-empty-market",
                {"token_id": "T", "cutoff": trigger},
                trigger,
            ),
            ("evt.ems_assessments", "fixture-ems", canonical_ems, start),
            ("ana.market_truth", "fixture-truth", canonical_truth, start),
            ("ana.decisions", "baseline-decision", baseline_decision, start),
            (
                "evt.beneficiary_maps",
                "event:1",
                {"event_id": "event", "map_version": 1, "analysis_set": ["T"]},
                start,
            ),
            (
                "evt.beneficiary_edges",
                "fixture-edge",
                {
                    "edge_id": "fixture-edge",
                    "event_id": "event",
                    "map_version": 1,
                    "token_id": "T",
                    "role": "PRIMARY",
                    "credible": False,
                    "capture_class": "UNKNOWN",
                    "exposure_score": 0,
                },
                start,
            ),
            (
                "out.alerts",
                "baseline-alert:1",
                {
                    "alert_id": "baseline-alert",
                    "version": 1,
                    "kind": "FULL",
                    "event_id": "event",
                    "generated_at": start,
                    "card_text": "Synthetic baseline",
                    "decisions": ["baseline-decision"],
                    "shown_tokens": ["T"],
                    "card_struct": {
                        "event": dict(canonical_event, contradictions=[]),
                        "assessment": canonical_ems,
                    },
                },
                start,
            ),
        ]
    )
    data = json.loads(packet.read_text())
    data.update(
        cutoff=cutoff,
        registry=cfg["registry"],
        config={key: value for key, value in cfg.items() if key != "registry"},
        records=[
            {"collection": collection, "key": key, "record": record, "available_at": at}
            for collection, key, record, at in rows
        ],
    )
    packet.write_text(json.dumps(data))
    repo, queue, messages, seq = MemoryRepository(), asyncio.Queue(), [], 0

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                yield await queue.get()

    async def emit(event) -> None:
        if event.get("type") == "agent.custom_tool_use":
            event = dict(event, input=model_arguments(event["name"], event["input"]))
        nonlocal seq
        seq += 1
        if event["type"] == "session.status_idle":
            settled_inputs.update(accepted_inputs)
        await queue.put(("data: " + json.dumps(event) + "\n\n").encode())

    accepted_inputs = []
    settled_inputs = set()
    actor_stage = None

    native_records = {}
    native_snapshot = {
        "version": 1,
        "system": "Fixture analyst prompt",
        "skills": [],
        "tools": [],
        "model": {"id": "fixture-model"},
    }

    async def control(request: httpx.Request) -> httpx.Response:
        nonlocal actor_stage
        if request.url.path.endswith("/executions"):
            assert request.method == "GET"
            ids = request.url.params.get_list("ids[]")
            assert ids and len(ids) <= 64 and set(ids) <= set(accepted_inputs)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": identity,
                            "state": "COMPLETED" if identity in settled_inputs else "RUNNING",
                            "settled_outcome": "COMPLETED" if identity in settled_inputs else None,
                        }
                        for identity in ids
                    ]
                },
            )
        path = request.url.path
        if request.method == "GET" and path.rsplit("/", 1)[-1] in {"fresh-session", "fresh-replay-session"}:
            return httpx.Response(200, json=native_records[path.rsplit("/", 1)[-1]])
        if path.endswith("/sessions"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            native_records["fresh-session"] = created_native_session(
                request, "fresh-session", agent_snapshot=native_snapshot
            )
            return httpx.Response(201, json=native_records["fresh-session"])
        if path.endswith("/events/stream"):
            return httpx.Response(200, stream=Stream(), headers={"content-type": "text/event-stream"})
        if path.endswith("/archive"):
            return httpx.Response(200, json={})
        if path.endswith("/events"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            event = json.loads(request.content)["events"][0]
            accepted_id = "accepted-" + str(len(accepted_inputs) + 1)
            accepted_inputs.append(accepted_id)
            if event["type"] == "user.message":
                message = json.loads(event["content"][0]["text"])["message"]
                messages.append(message)
                actor_stage = (
                    "observation" if message.get("kind") in {"NEW_ITEM", "ITEM_ADDED"} else "timer-truth"
                )
                await emit(
                    {
                        "id": "observation"
                        if message.get("kind") in {"NEW_ITEM", "ITEM_ADDED"}
                        else "timer-truth",
                        "type": "agent.custom_tool_use",
                        "execution_id": accepted_id,
                        "name": "add_observation"
                        if message.get("kind") in {"NEW_ITEM", "ITEM_ADDED"}
                        else "get_market_truth",
                        "input": {
                            "item_id": "source",
                            "event_id": "event",
                            "link": "CORROBORATION",
                            "claims": [],
                            "public_at": {
                                "value": start,
                                "precision": "EXACT",
                                "basis": "Synthetic captured source",
                                "quote": None,
                            },
                            "freshness": "FRESH",
                        }
                        if message.get("kind") in {"NEW_ITEM", "ITEM_ADDED"}
                        else {"event_id": "event", "token_ids": ["T"], "cutoff": trigger},
                    }
                )
            else:
                result = await audited_result(repo, event)
                assert not result.get("error"), result
                if actor_stage == "observation":
                    actor_stage = "initial-decision"
                    await emit(
                        {
                            "id": "decide",
                            "type": "agent.custom_tool_use",
                            "execution_id": accepted_id,
                            "name": "get_decision",
                            "input": {"event_id": "event", "token_id": "T"},
                        }
                    )
                elif actor_stage == "timer-truth":
                    actor_stage = "timer-decision"
                    await emit(
                        {
                            "id": "timer-decision",
                            "type": "agent.custom_tool_use",
                            "execution_id": accepted_id,
                            "name": "get_decision",
                            "input": {"event_id": "event", "token_id": "T"},
                        }
                    )
                elif actor_stage == "timer-decision":
                    actor_stage = "publication"
                    await emit(
                        {
                            "id": "timer-publication",
                            "type": "agent.custom_tool_use",
                            "execution_id": accepted_id,
                            "name": "publish_card",
                            "input": {
                                "event_id": "event",
                                "kind": "UPDATE",
                                "what": "Synthetic condition review",
                                "verdict": "Synthetic condition review",
                                "why": [
                                    "Condition predicate was rechecked",
                                    "No market evidence was supplied",
                                ],
                            },
                        }
                    )
                else:
                    if actor_stage == "publication":
                        assert result.get("publication_receipt", {}).get("disposition") in {
                            "PUBLISHED",
                            "SUPPRESSED",
                        }, result
                    await emit(
                        {
                            "id": "idle-" + str(len(messages)),
                            "type": "session.status_idle",
                            "execution_id": accepted_id,
                            "stop_reason": {"type": "end_turn"},
                        }
                    )
            return httpx.Response(200, json=acknowledge_native_input(request, accepted_id))
        raise AssertionError("Unexpected request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(control)) as http:
        reaction_truth = MarketTruthService(
            repo, config, ReplayMarketReader(load_inputs(packet, config)), versions=fixture_versions()
        )
        reaction_scheduler = Scheduler(
            repo,
            config,
            reaction_truth.market,
            reaction_truth,
            thresholds=config.thresholds(),
            versions=fixture_versions(),
        )
        tools = EventToolService(
            repo,
            config,
            reaction_truth,
            DecisionEngine(),
            versions=fixture_versions(),
            scheduler=reaction_scheduler,
            pages=recorded_page_access(load_inputs(packet, config)),
        )

        async def fixture_grade(arguments, context):

            result = dict(
                baseline_decision,
                dec_id="fresh-decision" if len(messages) == 1 else "fresh-timer-decision",
                cutoff=start if len(messages) == 1 else trigger,
                event_id="event",
                token_id="T",
                action="WAIT",
                conditions=[
                    {
                        "role": "UPGRADE",
                        "human_text": "Approval must be verified before upgrading the watch",
                        "expr": expr,
                        "status": "ARMED",
                    }
                ],
            )
            if len(messages) > 1:
                truths = await repo.list("ana.market_truth")
                latest = max(truths, key=lambda row: row["cutoff"])
                result.update(mta_id=latest["mta_id"], market_truth_id=latest["mta_id"])
            from tank.analysis.decision_watches import DecisionWatchCommand
            from tank.analysis.watch_contracts import WatchCondition, WatchProposal
            from tank.contracts.provenance import GenerationProvenance
            from tank.util import new_id

            proposals = [
                WatchProposal.model_validate({**value, "cond_id": new_id("cond")})
                for value in result["conditions"]
            ]
            prepared = context.get("_decision_prepared_condition_ids")
            reconciliation = tools._plan_conditions(
                DecisionWatchCommand(
                    decision_id=result["dec_id"],
                    event_id=result["event_id"],
                    token_id=result["token_id"],
                    generation=GenerationProvenance.model_validate(tools.provenance(context)),
                    proposals=proposals,
                ),
                [WatchCondition.model_validate(value) for value in await repo.list("ana.watch_conditions")]
                if len(messages) > 1
                else [],
                [],
                prepared_condition_ids=None if prepared is None else frozenset(prepared),
            )
            result["conditions"] = [
                value.model_dump(mode="json", exclude_unset=True) for value in reconciliation.active
            ]
            result["conditions_shown"] = list(reconciliation.shown)
            from tank.analysis.decision_codec import decode_decision, encode_decision
            from tank.analysis.decision_contracts import DecisionRecord
            from tank.analysis.decision_state import DecisionScope, DecisionStateRepository
            from tank.time_precision import collection_clock, propagate_timestamp

            baseline = decode_decision(baseline_decision)
            identity = baseline.metadata.identity.model_copy(
                update={
                    "decision_id": result["dec_id"],
                    "market_truth_id": result["mta_id"],
                    "cutoff": propagate_timestamp(result, "cutoff"),
                    "versions": tools.versions,
                    "generation": GenerationProvenance.model_validate(tools.provenance(context)),
                }
            )
            candidate = DecisionRecord(
                calculation=baseline.calculation,
                metadata=baseline.metadata.model_copy(
                    update={"identity": identity, "calculated_at": collection_clock()}
                ),
                conditions=reconciliation.active,
                conditions_shown=reconciliation.shown,
                outlook_id=result["dec_id"] + ":outlook",
            )
            states = DecisionStateRepository(repo)
            scope = DecisionScope(event_id="event", token_id="T")
            current = await states.current(scope)

            async def effects() -> None:
                await tools._persist_condition_writes(reconciliation)

            await states.advance(
                scope,
                expected=None if current is None else current[0],
                candidate=candidate,
                recorded_at=collection_clock(),
                effects=effects,
            )
            return encode_decision(candidate)

        tools.get_decision = fixture_grade
        try:
            report = await replay_agent_session(
                packet,
                repo,
                config,
                tools,
                http=http,
                initial_message={
                    "kind": "ITEM_ADDED",
                    "item_id": "source",
                    "event_id": "event",
                    "replay_at": start,
                },
                subsequent_messages=[
                    {
                        "kind": "CONDITION_FIRED",
                        "event_id": "event",
                        "condition": old,
                        "token_id": "T",
                        "replay_at": trigger,
                    }
                ],
                timeout=3,
            )
        except TimeoutError:
            failures = [
                {
                    "tool": row["tool"],
                    "status": row["status"],
                    "result": row.get("result"),
                    "error": row.get("error"),
                }
                for row in await repo.list("ops.tool_calls")
                if row["status"] in {"REFUSED", "ERROR"}
            ]
            assert not failures, failures
            raise

    all_conditions = await repo.list("ana.watch_conditions")

    conditions = [c for c in all_conditions if not c.get("rearmed_from")]
    rearmed = [c for c in all_conditions if c.get("rearmed_from")]
    assert len(conditions) == 1 and conditions[0]["cond_id"] != "old-condition"
    assert report["identity_substitutions"]["old-condition"] == conditions[0]["cond_id"]
    assert report["causal_tool_receipts"][0]["source_call_id"] == "decision"
    assert report["output"]["conditions"][0]["expires_at"] != old["expires_at"]
    assert report["status"] == ("COMPLETED" if state == "APPROVED" else "DIVERGED")
    if state == "APPROVED":
        assert conditions[0]["status"] == "FIRED"
        assert "cond_id" not in messages[1]["condition"]
        assert messages[1]["condition"]["expr"] == conditions[0]["expr"]
        assert [(c["status"], c["rearmed_from"]) for c in rearmed] == [("ARMED", conditions[0]["cond_id"])]
    else:
        assert conditions[0]["status"] == "ARMED"
        assert rearmed == []
        assert len(messages) == 1


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_captured_replay_batches_market_prefix_without_per_row_writer(
    packet, monkeypatch: pytest.MonkeyPatch, backend, postgres_template
) -> None:
    import os
    from contextlib import AsyncExitStack

    async with AsyncExitStack() as cleanup:
        if backend == "postgres":
            from psycopg.conninfo import conninfo_to_dict

            url = os.getenv("TANK_TEST_DATABASE_URL")
            if not url:
                pytest.skip("Explicit isolated replay batch PostgreSQL database not configured")
            if not conninfo_to_dict(url).get("dbname", "").startswith("tank_test"):
                pytest.fail("TANK_TEST_DATABASE_URL must select an isolated tank_test* database")
            repo = await cleanup.enter_async_context(postgres_template.repository(url))
        else:
            repo = MemoryRepository()
            cleanup.push_async_callback(repo.close)
        data = json.loads(packet.read_text())
        data["records"] = []
        for index in range(1001):
            data["records"].append(
                {
                    "collection": "mkt.tickers_24h",
                    "key": f"ticker-{index}",
                    "available_at": data["cutoff"],
                    "record": {"instrument_id": f"fixture:{index}", "ts": data["cutoff"], "last": "1"},
                }
            )
        data["config"]["replay_tool_calls"] = [
            {
                "call_id": "one",
                "tool": "get_registry",
                "arguments": {},
                "available_at": data["cutoff"],
            }
        ]
        packet.write_text(json.dumps(data))
        sizes = []
        original_batch, original_put = repo.put_market_batch, repo.put

        async def batch(collection, records, **kwargs):
            assert collection == "mkt.tickers_24h"
            sizes.append(len(records))
            return await original_batch(collection, records, **kwargs)

        async def put(collection, *args, **kwargs):
            assert not collection.startswith("mkt."), "market import used per-row writer"
            return await original_put(collection, *args, **kwargs)

        monkeypatch.setattr(repo, "put_market_batch", batch)
        monkeypatch.setattr(repo, "put", put)
        await replay_bundle(packet, repo, settings())
        assert sizes == [500, 500, 1]
        rows = [row async for row in repo.iter_all("mkt.tickers_24h")]
        assert len(rows) == 1001


def test_as_of_market_receipt_cannot_bypass_current_replay_clock() -> None:
    from tank.replay import ReplayInputs
    from tank.util import parse_time

    row = {
        "collection": "mkt.oi_obs",
        "key": "oi",
        "available_at": "2026-10-18T12:00:30Z",
        "record": {"ts": "2026-10-18T11:59:50Z", "received_ts": "2026-10-18T12:00:30Z"},
    }
    inputs = ReplayInputs("2026-10-18T12:01:00Z", "test", "test", {}, {}, (row,), ())
    assert inputs.evidence_mode == "AS_OF_CAPTURE"
    assert not inputs.record_visible(row, parse_time("2026-10-18T12:00:00Z"))
    assert inputs.record_visible(row, parse_time("2026-10-18T12:00:30Z"))


@pytest.mark.parametrize("explicit", [False, True])
async def test_operational_initial_view_uses_same_earliest_revision_clock(
    packet, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, explicit
) -> None:
    from tank import replay

    data = json.loads(packet.read_text())
    first = "2026-09-24T11:59:00Z"
    later = data["cutoff"]
    message = {"kind": "NEW_ITEM", "item_id": "same"}
    if explicit:
        message.update(replay_at=later, replay_at_prec="COLLECTION_ONLY")
    data["config"]["replay_messages"] = [message]
    data["records"] = [
        {
            "collection": "ingest.source_items",
            "key": "same",
            "available_at": later,
            "record": {"item_id": "same", "new_observation": "later partial revision"},
        },
        {
            "collection": "ingest.source_items",
            "key": "same",
            "available_at": first,
            "record": {"item_id": "same", "observed_at": first, "observed_at_prec": "BUCKET(1s)"},
        },
    ]
    packet.write_text(json.dumps(data))
    config = settings().model_copy(update={"data_dir": tmp_path})

    async def terminal(path, repo, settings, tools, **kwargs):
        inputs = replay.load_inputs(path, settings)
        selected = replay.replay_message_timestamp(inputs, kwargs["initial_message"])
        manifest = kwargs["evidence_capture"].manifest
        assert manifest.cutoff == selected.value == (later if explicit else first)
        assert manifest.cutoff_prec == selected.precision == ("COLLECTION_ONLY" if explicit else "BUCKET(1s)")
        return {"status": "TEST_TERMINAL"}

    monkeypatch.setattr(replay, "replay_agent_session", terminal)
    await replay.run_operational_replay(packet, MemoryRepository(), config, http=object())


@pytest.mark.integration
@pytest.mark.parametrize("profile", ["first_reference", "ordinary_missing_sent"])
async def test_actual_reference_publication_retains_unsent_intent_and_ordinary_requires_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, profile
) -> None:
    """Actual origin/class/map/publisher receipts; only native wire is synthetic."""
    import asyncio
    import copy

    from test_event_tools import unknown_assessment
    from test_fixture_reference import reference_packet as make_reference_packet

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.delivery import DeliveryService
    from tank.event_tools import LAYERS, EventToolService
    from tank.replay import ReplayMarketReader, digest, replay_agent_session
    from tank.util import sha256

    path, data = make_reference_packet.__wrapped__(tmp_path)
    cutoff = data["cutoff"]
    source = data["records"][0]["record"]
    body = "World access approved"
    raw = ("<html><time>21 Sep 2022</time><p>" + body + "</p></html>").encode()
    Path(source["raw_ref"]).write_bytes(raw)
    source.update(text=body, content_hash=sha256(body), raw_sha256=sha256(raw))
    data["records"][0]["as_of_evidence"].update(raw_sha256=sha256(raw), body_sha256=sha256(body))
    baseline = Settings(data_dir=tmp_path)
    cfg = {
        name: baseline.load_config(name)
        for name in ("registry", "sources", "thresholds", "semantics", "dependency_map")
    }
    cfg["openma"] = {
        "prompt_version": "synthetic-replay-prompt",
        "reasoning_effort": "medium",
        "agent_id": "fixture-agent",
        "environment_id": "analysis",
        "replay_environment_id": "replay",
    }
    data["registry"] = cfg["registry"]
    data["config"].update({key: value for key, value in cfg.items() if key != "registry"})
    if profile == "ordinary_missing_sent":
        data.pop("replay_mode")
        data.pop("fixture_reference")
        source.update(observed_at=cutoff, acquired_at=cutoff)
        data["records"][0]["available_at"] = cutoff
        for key in list(data["records"][0]):
            if key.startswith("simulated_"):
                data["records"][0].pop(key)

        data["records"].extend(
            [
                {
                    "collection": "ops.agent_sessions",
                    "key": "original-session",
                    "available_at": cutoff,
                    "record": {
                        "session_id": "original-session",
                        "opened_by_item": source["item_id"],
                        "event_id": "original-event",
                    },
                },
                {
                    "collection": "out.alerts",
                    "key": "original-alert:1",
                    "available_at": cutoff,
                    "record": {
                        "alert_id": "original-alert",
                        "event_id": "original-event",
                        "version": 1,
                        "kind": "PARTIAL",
                        "generated_at": cutoff,
                    },
                },
            ]
        )
    data.pop("input_sha256", None)
    data["input_sha256"] = digest(data)
    path.write_text(json.dumps(data))

    class Config(Settings):
        def load_config(self, name):
            return copy.deepcopy(cfg[name])

    config = Config(
        replay=True, config_version="test", code_version="test", data_dir=tmp_path, _env_file=None
    )
    repo, queue = MemoryRepository(), asyncio.Queue()
    accepted, settled = [], set()
    sequence, stage = 0, 0
    event_id = None
    publication = None
    partial_card = partial_intent = None
    delivery_attempts = []

    async def forbid_delivery(self, payload) -> None:
        delivery_attempts.append(payload)
        raise AssertionError("No reference or uncaptured delivery transport is authorized")

    monkeypatch.setattr(DeliveryService, "deliver", forbid_delivery)

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                yield await queue.get()

    async def emit(event) -> None:
        if event.get("type") == "agent.custom_tool_use":
            event = dict(event, input=model_arguments(event["name"], event["input"]))
        nonlocal sequence
        sequence += 1
        if event["type"] == "session.status_idle":
            settled.update(accepted)
        await queue.put(("data: " + json.dumps(event) + "\n\n").encode())

    async def tool(name, arguments, execution_id) -> None:
        await emit(
            {
                "id": "producer-" + name + "-" + str(stage),
                "type": "agent.custom_tool_use",
                "execution_id": execution_id,
                "name": name,
                "input": arguments,
            }
        )

    native_records = {}
    native_snapshot = {
        "version": 1,
        "system": "Synthetic publication producer",
        "skills": [],
        "tools": [],
        "model": {"id": "fixture-model"},
    }

    async def transport(request: httpx.Request) -> httpx.Response:
        nonlocal stage, event_id, publication, partial_card, partial_intent
        endpoint = request.url.path
        if endpoint.endswith("/executions"):
            ids = request.url.params.get_list("ids[]")
            assert set(ids) <= set(accepted)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": identity,
                            "state": "COMPLETED" if identity in settled else "RUNNING",
                            "settled_outcome": "COMPLETED" if identity in settled else None,
                        }
                        for identity in ids
                    ]
                },
            )
        if endpoint.endswith("/sessions"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            native_records["reference-session"] = created_native_session(
                request, "reference-session", agent_snapshot=native_snapshot
            )
            return httpx.Response(201, json=native_records["reference-session"])
        if request.method == "GET" and endpoint.endswith("/reference-session"):
            return httpx.Response(200, json=native_records["reference-session"])
        if endpoint.endswith("/events/stream"):
            return httpx.Response(200, stream=Stream(), headers={"content-type": "text/event-stream"})
        if endpoint.endswith("/archive"):
            return httpx.Response(200, json={})
        if endpoint.endswith("/events"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            event = json.loads(request.content)["events"][0]
            identity = "accepted-" + str(len(accepted) + 1)
            accepted.append(identity)
            if event["type"] == "user.message":
                assert stage == 0, "The producer must complete without corrective retries"
                await tool(
                    "record_event",
                    {
                        "item_id": source["item_id"],
                        "is_event": True,
                        "statement": body,
                        "certainty": "HIGH",
                        "certainty_reason": "Explicit synthetic primary fixture fact",
                        "entities": [],
                        "token_ids": ["world:WLD"],
                        "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
                        "public_at": {
                            "value": cutoff,
                            "precision": "EXACT",
                            "basis": "Declared fixture first-knowledge boundary; item retains publication day",
                            "quote": None,
                        },
                        "freshness": "FRESH",
                        "claims": [
                            {
                                "type": "STATE",
                                "subject": "World",
                                "predicate": "announces",
                                "object": "Product",
                                "state_claim": "APPROVED",
                                "verified": True,
                                "directionality": "BOTH",
                                "evidence_class": "PRIMARY_FACT",
                                "quote": body,
                            }
                        ],
                        "next_milestone": None,
                        "checked_sources": [],
                    },
                    identity,
                )
            else:
                result = await audited_result(repo, event)
                assert not result.get("error"), result
                stage += 1
                if stage == 1:
                    event_id = result["event_id"]
                    await tool(
                        "set_assessment",
                        {
                            "event_id": event_id,
                            "assessment": unknown_assessment(
                                criterion="G5",
                                klass="STRUCTURAL_SHOCK",
                                claim=result["accepted_claims"][0]["claim_id"],
                            ),
                        },
                        identity,
                    )
                elif stage == 2:
                    assert result["class"] == "STRUCTURAL_SHOCK", result
                    await tool(
                        "publish_card",
                        {
                            "event_id": event_id,
                            "kind": "PARTIAL",
                            "what": body,
                            "verdict": body,
                            "why": [body, "The publication does not quantify price or liquidity effects."],
                        },
                        identity,
                    )
                elif stage == 3:
                    assert result["published"] is True, result
                    publication = result
                    partial_card = await repo.get("out.alerts", result["alert_id"] + ":1")
                    partial_intent = copy.deepcopy(
                        (await repo.list("ops.jobs", filters={"type": "DELIVER"}))[0]
                    )
                    await tool(
                        "set_beneficiaries",
                        {
                            "event_id": event_id,
                            "edges": [],
                            "layer_reasons": {
                                layer: "No credible beneficiary established from this synthetic fact"
                                for layer in LAYERS
                            },
                        },
                        identity,
                    )
                elif stage == 4:
                    assert result["analysis_set"] == []
                    await tool(
                        "publish_card",
                        {
                            "event_id": event_id,
                            "kind": "FULL",
                            "what": body,
                            "verdict": body,
                            "why": ["Official primary fixture fact", "No credible beneficiary established"],
                        },
                        identity,
                    )
                else:
                    assert stage == 5 and result["published"] is True, result
                    publication = result
                    await emit(
                        {
                            "id": "final-idle",
                            "type": "session.status_idle",
                            "execution_id": identity,
                            "stop_reason": {"type": "end_turn"},
                        }
                    )
            return httpx.Response(200, json=acknowledge_native_input(request, identity))
        raise AssertionError("Unexpected native control-plane request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        from tank.fixture_reference import TrustedFixtureClock

        inputs = load_inputs(path, config)
        reaction_truth = MarketTruthService(
            repo, config, ReplayMarketReader(inputs), versions=fixture_versions()
        )
        reaction_scheduler = Scheduler(
            repo,
            config,
            reaction_truth.market,
            reaction_truth,
            thresholds=config.thresholds(),
            versions=fixture_versions(),
        )
        tools = EventToolService(
            repo,
            config,
            reaction_truth,
            DecisionEngine(),
            reference_clock=TrustedFixtureClock(inputs) if inputs.fixture_reference else None,
            versions=fixture_versions(),
            scheduler=reaction_scheduler,
            pages=recorded_page_access(load_inputs(path, config)),
        )
        if profile == "ordinary_missing_sent":
            with pytest.raises(ReplayError, match="one captured SENT delivery receipt required"):
                await replay_agent_session(
                    path,
                    repo,
                    config,
                    tools,
                    http=http,
                    initial_message={"kind": "NEW_ITEM", "item_id": source["item_id"]},
                    timeout=3,
                )
        else:
            report = await replay_agent_session(
                path,
                repo,
                config,
                tools,
                http=http,
                initial_message=data["config"]["replay_messages"][0],
                timeout=3,
            )
            assert report["status"] == "COMPLETED"
            assert report["delivery_status"] == "NOT_RUN"
            assert report["delivery_reason"] == "FIRST_REFERENCE_NO_CAPTURED_DELIVERY"
            assert report["delivery_receipts"] == []
            jobs = await repo.list("ops.jobs", filters={"type": "DELIVER"})
            assert len(jobs) == 2 and all(job["status"] == "QUEUED" for job in jobs)
            assert report["delivery_intents"] == sorted(jobs, key=lambda job: job["job_id"])
            assert {job["payload"]["version"] for job in jobs} == {1, 2}
            assert all(job["payload"]["alert_id"] == publication["alert_id"] for job in jobs)
            assert await repo.get("ops.jobs", partial_intent["job_id"]) == partial_intent
            assert await repo.get("out.alerts", partial_card["alert_id"] + ":1") == partial_card
            assert [(alert["kind"], alert["version"]) for alert in report["output"]["alerts"]] == [
                ("PARTIAL", 1),
                ("FULL", 2),
            ]
            assert report["cutoff"] == cutoff
            assert report["execution_barrier"]["complete"]
            assert (await repo.get("ingest.source_items", source["item_id"]))["extraction_status"] == "DONE"
    assert publication["publication_receipt"]["disposition"] == "PUBLISHED"
    assert not delivery_attempts
    generated = await repo.list("out.deliveries")
    assert generated and all(row["status"] == "GENERATED" for row in generated)
    assert all(not row.get("message_ref") and not row.get("sent_at") for row in generated)
    assert all(row["at"] == cutoff for row in generated)
    assert await repo.list("ana.calls") == []


@pytest.mark.integration
async def test_captured_tools_decision_refreshes_fired_market_watch_offline(
    packet, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import timedelta

    from test_market_capture_refresh import ReleasedMarket, seed_domain, shifted_snapshot

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.event_tools import EventToolService
    from tank.time_precision import chosen_boundary
    from tank.util import iso, parse_time

    data = json.loads(packet.read_text())
    end = parse_time(data["cutoff"])
    monkeypatch.setattr("tank.util.utcnow", lambda: end)
    monkeypatch.setattr("tank.event_tools.utcnow", lambda: end)
    earlier = iso(end - timedelta(minutes=1))
    source = MemoryRepository()
    cfg = Settings(data_dir=tmp_path, _env_file=None)
    await seed_domain(source, cfg, end)
    market = ReleasedMarket(shifted_snapshot(end))
    truth = MarketTruthService(source, cfg, market, versions=fixture_versions())
    first = await truth.assess("e", "T", chosen_boundary(earlier, precision="BUCKET(1m)"))
    tools = EventToolService(
        source,
        cfg,
        truth,
        DecisionEngine(),
        versions=fixture_versions(),
        scheduler=RecordingReactionScheduling(),
        pages=LivePageAccess(fixture_http_admission()),
    )
    initial = await tools.dispatch("get_decision", {"event_id": "e", "token_id": "T"}, {})
    watch = next(row for row in initial["conditions"] if row["role"] == "UPGRADE")
    watch.update(status="FIRED", fired_at=data["cutoff"])
    snapshot = shifted_snapshot(end)
    data["registry"] = {"tokens": [{"token_id": "T", "symbol": "T"}], "instruments": []}
    data["config"] = {
        "thresholds": Settings(_env_file=None).load_config("thresholds"),
        "semantics": {},
        "dependency_map": {},
        "replay_tool_calls": [
            {
                "call_id": "decision",
                "tool": "get_decision",
                "arguments": {"event_id": "e", "token_id": "T"},
                "available_at": data["cutoff"],
            }
        ],
    }
    data["records"] = []
    for table, keyname in [
        ("evt.events", "event_id"),
        ("evt.ems_assessments", "ems_id"),
        ("evt.beneficiary_maps", "event_id"),
        ("evt.beneficiary_edges", "edge_id"),
        ("ana.market_truth", "mta_id"),
    ]:
        for row in await source.list(table):
            data["records"].append(
                {"collection": table, "key": row[keyname], "available_at": earlier, "record": row}
            )
    data["records"].extend(
        [
            {
                "collection": "ana.watch_conditions",
                "key": watch["cond_id"],
                "available_at": data["cutoff"],
                "record": watch,
            },
            {
                "collection": "replay.market_snapshots",
                "key": "synthetic-current",
                "available_at": data["cutoff"],
                "record": snapshot,
            },
        ]
    )
    packet.write_text(json.dumps(data))
    repo = MemoryRepository()
    report = await replay_bundle(packet, repo, settings())
    result = report["output"]["tool_calls"][0]["result"]
    assert result["mta_id"] != first["mta_id"]
    assert result["action"] == "START"
    assert (await repo.get("ana.market_truth", result["mta_id"]))["data_confidence"]["class"] == "HIGH"
    assert (await repo.get("ana.watch_conditions", watch["cond_id"]))["consumed_by"] == result["dec_id"]
    jobs = [row for row in await repo.list("ops.jobs") if row["type"] == "RUN_MARKET_TRUTH"]
    assert len(jobs) == 1 and jobs[0]["status"] == "DONE"


def test_uncaptured_source_policy_remains_explicitly_unknown() -> None:
    from tank.contracts.source_items import PendingRelevance
    from tank.replay import ReplaySourcePolicy

    policy = ReplaySourcePolicy.from_captured({"item_id": "legacy-item"})
    relevance = policy.initial_relevance()
    assert isinstance(relevance, PendingRelevance)
    assert relevance.decision is None
    assert relevance.pending_reason == "SOURCE_INGESTION_POLICY_NOT_CAPTURED"


def test_malformed_captured_policy_is_not_treated_as_uncaptured() -> None:
    from pydantic import ValidationError

    from tank.replay import ReplaySourcePolicy

    with pytest.raises(ValidationError):
        ReplaySourcePolicy.from_captured({"source_id": ["S01"]})
