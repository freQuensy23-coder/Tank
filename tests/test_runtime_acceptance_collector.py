"""Offline collector controls only; fixtures here are never acceptance evidence."""

import copy
import hashlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
import zstandard

spec = importlib.util.spec_from_file_location(
    "acceptance_harness", Path(__file__).parents[1] / "docs/verification/runtime-no-x-acceptance/harness.py"
)
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


def actual_shape_fixture(tmp_path):
    before = {table: [] for table in harness.TABLES}
    state = copy.deepcopy(before)
    payload = b"Offline unit-test source bytes; not live acceptance evidence."
    raw = tmp_path / "raw/items/S01/source.json.zst"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(zstandard.ZstdCompressor().compress(payload))
    state["ingest.source_items"] = [
        {
            "item_id": "fixture",
            "source_id": "S01",
            "extraction_status": "DONE",
            "relevance": {"decision": "ALWAYS"},
            "raw_ref": str(raw),
            "raw_sha256": hashlib.sha256(payload).hexdigest(),
            "observed_at": "2026-10-19T12:00:00Z",
            "observed_at_prec": "COLLECTION_ONLY",
            "published_at": "2026-10-19T11:50:00Z",
            "published_prec": "EXACT",
        }
    ]
    state["evt.events"] = [{"event_id": "e"}]
    state["evt.event_observations"] = [
        {"item_id": "fixture", "event_id": "e", "link_type": "ORIGIN", "observed_at": "2026-10-19T12:00:00Z"}
    ]
    state["ops.agent_sessions"] = [{"session_id": "s", "event_id": "e"}]
    state["ops.jobs"] = [{"type": "ROUTE_ITEM", "status": "DONE", "payload": {"item_id": "fixture"}}]
    state["ops.tool_calls"] = [
        {
            "tool": "record_event",
            "session_id": "s",
            "status": "DONE",
            "returned_at": "2026-10-19T12:01:13Z",
            "is_error": False,
            "arguments": {"extraction": {"item_id": "fixture", "is_event": True}},
            "result": {"event_id": "e"},
            "completed_at": "2026-10-19T12:01:12Z",
        },
        {
            "tool": "publish_card",
            "session_id": "s",
            "status": "DONE",
            "returned_at": "2026-10-19T12:03:01Z",
            "is_error": False,
            "arguments": {"event_id": "e", "kind": "FULL"},
            "result": {"published": True, "alert_id": "a", "version": 1},
        },
    ]
    state["ana.decisions"] = [{"event_id": "e", "dec_id": "d", "mta_id": "m"}]
    state["ana.market_truth"] = [{"event_id": "e", "mta_id": "m"}]
    state["out.alerts"] = [
        {
            "event_id": "e",
            "alert_id": "a",
            "version": 1,
            "kind": "FULL",
            "generated_at": "2026-10-19T12:03:00Z",
            "decisions": ["d"],
        }
    ]
    state["out.deliveries"] = [
        {
            "alert_id": "a",
            "version": 1,
            "channel": "telegram:alerts",
            "status": "SENT",
            "message_ref": "1",
            "at": "2026-10-19T12:03:02Z",
        }
    ]
    return before, state


def test_actual_nested_receipt_shape_and_processing_latency(tmp_path):
    before, state = actual_shape_fixture(tmp_path)
    report = harness.link_report(before, state, raw_root=tmp_path)
    assert report["complete_chains"] == 1
    chain = report["chains"][0]
    assert chain["latency_s"]["observed_to_accepted_record_event"] == [72]
    assert chain["latency_s"]["publisher_to_observed"]["value_s"] == 600
    assert chain["filter_evidence"]["source_policy"] == "ALWAYS"
    assert chain["filter_evidence"]["filter_job_receipts"] == []
    assert chain["raw_proof"]["verified"]


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_return",
        "refused",
        "error_result",
        "wrong_item",
        "wrong_event",
        "source_pending",
        "event_missing",
        "not_origin",
        "partial",
        "raw_mismatch",
        "missing_filter",
        "filtered_false_always",
        "dangling_decision",
        "wrong_channel",
    ],
)
def test_incomplete_or_refused_evidence_never_passes(tmp_path, mutation):
    before, state = actual_shape_fixture(tmp_path)
    call = state["ops.tool_calls"][0]
    if mutation == "missing_return":
        call.pop("returned_at")
    elif mutation == "refused":
        call["result"]["accepted"] = False
    elif mutation == "error_result":
        call["result"]["error"] = {}  # Even an empty error member is not acceptance.
    elif mutation == "wrong_item":
        call["arguments"]["extraction"]["item_id"] = "other"
    elif mutation == "wrong_event":
        call["result"]["event_id"] = "other"
    elif mutation == "source_pending":
        state["ingest.source_items"][0]["extraction_status"] = "PENDING"
    elif mutation == "event_missing":
        state["evt.events"] = []
    elif mutation == "not_origin":
        state["evt.event_observations"][0]["link_type"] = "REPEAT"
    elif mutation == "partial":
        state["out.alerts"][0]["kind"] = "PARTIAL"
    elif mutation == "raw_mismatch":
        state["ingest.source_items"][0]["raw_sha256"] = "0" * 64
    elif mutation == "dangling_decision":
        state["out.alerts"][0]["decisions"].append("missing")
    elif mutation == "wrong_channel":
        state["out.deliveries"][0]["channel"] = "telegram:ops"
    elif mutation in {"missing_filter", "filtered_false_always"}:
        state["ingest.source_items"][0]["source_id"] = "S31"
        if mutation == "missing_filter":
            state["ingest.source_items"][0]["relevance"]["decision"] = "SEND"
    assert harness.link_report(before, state, raw_root=tmp_path)["complete_chains"] == 0


def test_filtered_chain_requires_its_own_completed_filter_job(tmp_path):
    before, state = actual_shape_fixture(tmp_path)
    item = state["ingest.source_items"][0]
    item.update(source_id="S31", relevance={"decision": "SEND"})
    state["ops.jobs"].append({"type": "FILTER_ITEM", "status": "DONE", "payload": {"item_id": "fixture"}})
    chain = harness.link_report(before, state, raw_root=tmp_path)["chains"][0]
    assert chain["complete_receipt_chain"]
    assert chain["filter_evidence"]["source_policy"] == "FILTER_REQUIRED"
    assert len(chain["filter_evidence"]["filter_job_receipts"]) == 1


def test_publication_day_and_unknown_are_not_scalar_latencies():
    item = {
        "published_at": "2026-10-19T00:00:00Z",
        "published_prec": "BUCKET",
        "published_at_bucket_size_s": 86400,
        "observed_at": "2026-10-19T12:00:00Z",
        "observed_at_prec": "COLLECTION_ONLY",
    }
    result = harness.publisher_latency(item)
    assert result["value_s"] is None
    assert result["min_s"] == -43200 and result["max_s"] == 43200
    item.pop("published_prec")
    item.pop("published_at_bucket_size_s")
    result = harness.publisher_latency(item)
    assert result["value_s"] is result["min_s"] is result["max_s"] is None
    assert result["published_prec_reason"] == "INCOMPLETE"


def test_provider_credentials_are_refused_without_auth_file_reads(tmp_path):
    settings = SimpleNamespace(
        openrouter_api_key="FAKE_PROVIDER_KEY_12345", codex_auth_path=tmp_path / "never-read.json"
    )
    with pytest.raises(ValueError, match="Credential-bearing"):
        harness.safe_evidence({"result": "FAKE_PROVIDER_KEY_12345"}, settings)
    with pytest.raises(ValueError, match="Credential-bearing"):
        harness.safe_evidence({"url": "https://example.test/?token=fake"}, settings)
    assert harness.safe_evidence({"token_id": "token:T"}, settings)


def test_protected_raw_path_is_refused_before_reading(tmp_path, monkeypatch):
    path = tmp_path / "auth.json"
    path.write_text("fake protected secret")

    def forbidden_read(self):
        raise AssertionError("Protected bytes must never be opened")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read)
    proof = harness.raw_proof(
        {"raw_ref": str(path), "raw_sha256": "0" * 64}, raw_root=tmp_path, protected={"auth": path}
    )
    assert proof == {"verified": False, "reason": "UNSAFE_RAW_PATH"}


def test_duplicate_receipts_distinguish_legitimate_alert_versions():
    state = {
        "out.deliveries": [
            {
                "alert_id": "a",
                "version": 1,
                "channel": "telegram:alerts",
                "status": "SENT",
                "message_ref": "1",
            },
            {
                "alert_id": "a",
                "version": 2,
                "channel": "telegram:alerts",
                "status": "SENT",
                "message_ref": "2",
            },
        ]
    }
    assert harness.duplicates(state) == []
    state["out.deliveries"].append(dict(state["out.deliveries"][0], message_ref="3"))
    assert harness.duplicates(state)[0]["message_refs"] == ["1", "3"]


@pytest.mark.asyncio
async def test_cleanup_disposes_only_new_qa_sessions_and_never_updates_local_state(monkeypatch):
    from tank.storage import MemoryRepository
    from tank.util import sha256

    repo = MemoryRepository()
    await repo.put("ingest.source_items", "qa-item", {"item_id": "qa-item"})
    await repo.put("ops.agent_sessions", "qa-local", {"session_id": "qa-local", "item_id": "qa-item"})
    await repo.put("ops.agent_sessions", "old", {"session_id": "old", "item_id": "other"})
    before = {table: [] for table in harness.TABLES}
    initial_catalog = [{"id": "old", "status": "running", "tank_identity": "not-this-run"}]
    untouched = await harness.snapshot(repo)
    calls = []
    statuses = {"old": "running", "qa-local": "running", "qa-orphan": "running", "unrelated": "running"}
    markers = {"qa-orphan": sha256({"identity": "qa-item", "scout": False}), "unrelated": "someone-else"}

    class FakeControlClient:
        async def request(self, method, path, **kwargs):
            calls.append((method, path, kwargs))
            if path == "sessions":
                return {
                    "data": [
                        {"id": sid, "status": status, "metadata": {"tank_identity": markers.get(sid)}}
                        for sid, status in statuses.items()
                    ]
                }
            sid = path.split("/")[1]
            if path.endswith("/events"):
                statuses[sid] = "idle"
                return {"accepted": True}
            if path.endswith("/archive"):
                statuses[sid] = "terminated"
                return {"archived": True, "id": sid}
            return {"data": {"status": statuses[sid]}}

        async def close(self):
            calls.append(("CLOSE", "control-client", {}))

    monkeypatch.setattr("tank.agents.sessions.SessionService", lambda *args: FakeControlClient())
    cleanup = await harness.cleanup_owned(repo, SimpleNamespace(), before, initial_catalog, wait_s=1)
    assert cleanup["owned_session_ids"] == ["qa-local", "qa-orphan"]
    assert all(row["archive_acknowledged"] for row in cleanup["sessions"])
    assert cleanup["reused_preexisting_session_ids"] == ["old"]
    assert cleanup["native_catalog_status"] == "PENDING_CONFIGURATION_HANDOFF"
    mutations = [(method, path) for method, path, _ in calls if method == "POST"]
    assert mutations == [
        ("POST", "sessions/qa-local/events"),
        ("POST", "sessions/qa-local/archive"),
        ("POST", "sessions/qa-orphan/events"),
        ("POST", "sessions/qa-orphan/archive"),
    ]
    assert statuses["old"] == statuses["unrelated"] == "running"
    assert await harness.snapshot(repo) == untouched
