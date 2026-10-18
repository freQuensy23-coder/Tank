import json
from pathlib import Path

import pytest

from tank.replay import ReplayClock, ReplayError, evaluate_frozen, load_inputs, replay_bundle
from tank.settings import Settings
from tank.storage import MemoryRepository


@pytest.fixture
def packet(tmp_path):
    p = tmp_path / "input.json"
    p.write_text(
        json.dumps(
            {
                "cutoff": "2026-09-24T12:00:00Z",
                "config_version": "test",
                "code_version": "test",
                "registry": {},
                "config": {},
                "records": [],
                "tool_results": [],
            }
        )
    )
    return p


def settings():
    return Settings(config_version="test", code_version="test", replay=True)


async def test_sealed_outcomes_only_after_freeze_and_immutable(packet, tmp_path):
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


async def test_default_runs_validated_tool(packet):
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


async def test_missing_captures_explicit(packet):
    with pytest.raises(ReplayError, match="insufficient_fixture"):
        await replay_bundle(packet, MemoryRepository(), settings())


@pytest.mark.parametrize(
    "change",
    [{"outcomes": {}}, {"code_version": "wrong"}, {"records": [{"available_at": "2027-01-01T00:00:00Z"}]}],
)
def test_reject_contaminated_or_unpinned(packet, change):
    data = json.loads(packet.read_text())
    data.update(change)
    packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError):
        load_inputs(packet, settings())


def test_clock_and_query_bounds(packet):
    inputs = load_inputs(packet, settings())
    with pytest.raises(ReplayError):
        inputs.records("x", cutoff="2027-01-01T00:00:00Z")
    clock = ReplayClock("2026-09-24T10:00:00Z", inputs.cutoff)
    with pytest.raises(ReplayError):
        clock.advance("2026-09-24T09:00:00Z")


def test_manifest_honest():
    manifest = json.loads((Path(__file__).parent / "fixtures/manifest.json").read_text())
    assert len(manifest["fixtures"]) == 27
    assert all(f["readiness"] == "INCOMPLETE_PACKET" for f in manifest["fixtures"])
    assert manifest["fixtures"][15]["known_conflict"] is None
    assert "Market context STRESS" in manifest["fixtures"][15]["assertion_spec"]
    assert manifest["fixtures"][15]["expectation_status"] == "CURRENT_SPEC_ALIGNED"


async def test_bundle_reproduction_reexecutes_captured_tools(packet, tmp_path):
    from tank.replay import digest, replay_fixture, reproduce_bundle

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
    first = await replay_fixture(MemoryRepository(), config, "F-01")
    second = await reproduce_bundle(MemoryRepository(), config, digest(data))
    assert second["reproduced"] and second["output_sha256"] == first["output_sha256"]


async def test_reused_repository_cannot_expose_future_row(packet):
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


def test_claimed_availability_cannot_hide_future_observation(packet):
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


async def test_replays_mutation_and_remaps_captured_generated_ids(packet):
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
        "tokens": [],
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


def test_masking_preserves_original_and_private_map_is_not_pipeline_input(packet, tmp_path):
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


async def test_fresh_replay_session_reruns_market_and_decision_code(packet, tmp_path):
    import asyncio
    import copy

    import httpx

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
        "openma": {
            "agent_id": "captured-analyst",
            "environment_id": "original-live-env",
            "replay_environment_id": "tank-replay-fixture-env",
        },
        "thresholds": {},
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
                "state_verified": True,
                "freshness": "FRESH",
                "certainty": "HIGH",
                "lifecycle": "ANALYZING",
                "known_to_us_at": earlier,
                "public_at": earlier,
                "tokens_mentioned": ["T"],
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
                "arguments": {"event_id": event_id, "token": "T"},
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
                seq, event = await queue.get()
                yield ("id: " + str(seq) + "\ndata: " + json.dumps(event) + "\n\n").encode()

        async def aclose(self):
            pass

    async def transport(request):
        path = request.url.path
        if path.endswith("/sessions"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [{"id": old_sid, "metadata": {}}]})
            requests.append(json.loads(request.content))
            return httpx.Response(201, json={"id": "fresh-replay-session"})
        if path.endswith("/events/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream())
        if path.endswith("/events"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": []})
            event = json.loads(request.content)["events"][0]
            if event["type"] == "user.message":
                await queue.put(
                    (
                        1,
                        {
                            "id": "replay-truth",
                            "type": "agent.custom_tool_use",
                            "name": "get_market_truth",
                            "input": {"event_id": event_id, "tokens": ["T"]},
                        },
                    )
                )
            else:
                result = json.loads(event["content"][0]["text"])
                results.append(result)
                if event["custom_tool_use_id"] == "replay-truth":
                    await queue.put(
                        (
                            2,
                            {
                                "id": "replay-web",
                                "type": "agent.custom_tool_use",
                                "name": "fetch_page",
                                "input": {"url": "https://synthetic.invalid/captured-page"},
                            },
                        )
                    )
                elif event["custom_tool_use_id"] == "replay-web":
                    await queue.put(
                        (
                            3,
                            {
                                "id": "replay-decision",
                                "type": "agent.custom_tool_use",
                                "name": "get_decision",
                                "input": {"event_id": event_id, "token": "T"},
                            },
                        )
                    )
                else:
                    await queue.put(
                        (
                            4,
                            {
                                "id": "replay-done",
                                "type": "session.status_idle",
                                "stop_reason": {"type": "end_turn"},
                            },
                        )
                    )
            return httpx.Response(200, json={})
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
        tools = EventToolService(
            repo,
            config,
            MarketTruthService(repo, config, CapturedMarket()),
            DecisionEngine(config),
            http=http,
        )
        report = await replay_agent_session(
            packet,
            repo,
            config,
            tools,
            http=http,
            initial_message={"kind": "ITEM_ADDED", "item_id": item_id, "event_id": event_id},
            replay_run_id="unique-replay-run",
            timeout=5,
            output_path=tmp_path / "frozen-fresh-session.json",
        )
    assert requests[0]["environment_id"] == "tank-replay-fixture-env"
    assert report["session_id"] != old_sid
    assert report["mode"] == "FRESH_OPENMA_SESSION"
    assert report["output"]["decisions"][0]["action"] == "WAIT"
    assert results[-1]["action"] == "WAIT"  # deliberately wrong stored START was not reused
    assert results[0]["market_truth"][0]["move_class"] == "UNKNOWN"
    assert results[1]["text"] == "Synthetic captured page"
    assert (await repo.get("ops.jobs", future_job))["status"] == "QUEUED"
    assert (await repo.get("ops.agent_sessions", report["session_id"]))[
        "replay_run_id"
    ] == "unique-replay-run"


async def test_transcript_web_reads_use_only_captured_results(packet, monkeypatch):
    from tank.event_tools import EventToolService

    async def forbidden(*args, **kwargs):
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


def test_late_market_capture_requires_assembly_and_preserves_real_receipt(packet):
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
def test_assembly_never_admits_future_market_or_partial_bar(packet, record):
    data = json.loads(packet.read_text())
    data["assembled_at"] = "2026-10-17T01:00:00Z"
    data["records"] = [
        {"collection": "mkt.klines_1m", "available_at": "2026-10-17T00:00:00Z", "record": record}
    ]
    packet.write_text(json.dumps(data))
    with pytest.raises(ReplayError, match="post-cutoff market"):
        load_inputs(packet, settings())


def test_assembly_does_not_admit_source_revision(packet):
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
