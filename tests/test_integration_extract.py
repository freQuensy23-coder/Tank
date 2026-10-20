import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tank.integration_extract import captured_items, isolated_database


def test_component_probe_accepts_only_explicit_isolated_database():
    settings = SimpleNamespace(database_url="postgresql://user:FAKE@postgres/tank_integration_20261018")
    assert isolated_database(settings, "tank_integration_20261018") == "tank_integration_20261018"
    for name in ["tank", "tank_integration_other"]:
        with pytest.raises(ValueError, match="explicitly provisioned"):
            isolated_database(settings, name)


def test_capture_probe_preserves_original_source_timestamps_and_hashes(tmp_path):
    row = {
        "item_id": "realcaptured",
        "source_id": "S24",
        "content_hash": "hash",
        "raw_sha256": "raw",
        "published_at": "2026-10-01T01:02:03Z",
        "observed_at": "2026-10-18T01:02:03Z",
        "text": "actualcapturedtext",
    }
    path = tmp_path / "raw.jsonl"
    path.write_text(json.dumps(row) + "\n")
    assert captured_items(path, ["realcaptured"]) == [row]
    for ids in [[], ["realcaptured"] * 2, ["a", "b", "c", "d"]]:
        with pytest.raises(ValueError, match="one to three"):
            captured_items(path, ids)
    del row["published_at"]
    path.write_text(json.dumps(row) + "\n")
    with pytest.raises(ValueError, match="publication"):
        captured_items(path, ["realcaptured"])


def test_verified_raw_staging_preserves_bytes_timestamps_and_records_relocation(tmp_path):
    import hashlib

    import zstandard

    from tank.integration_extract import session_status, stage_raw_items

    payload = b"actual collected page bytes"
    source = tmp_path / "captured.zst"
    source.write_bytes(zstandard.ZstdCompressor().compress(payload))
    row = {
        "item_id": "real",
        "raw_ref": str(source),
        "raw_sha256": hashlib.sha256(payload).hexdigest(),
        "published_at": "2026-10-01",
        "observed_at": "2026-10-18",
        "content_hash": "unchanged",
    }
    staged, relocations = stage_raw_items([row], tmp_path / "out")
    assert staged[0]["published_at"] == row["published_at"] and staged[0]["observed_at"] == row["observed_at"]
    assert staged[0]["content_hash"] == row["content_hash"] and staged[0]["original_raw_ref"] == str(source)
    assert staged[0]["raw_ref"] != str(source)
    assert source.read_bytes() == Path(staged[0]["raw_ref"]).read_bytes()
    assert relocations[0]["raw_sha256"] == row["raw_sha256"]
    assert session_status({"data": {"status": "idle"}}) == "idle"
    assert session_status({"status": "running"}) == "running"
    row["raw_sha256"] = "wrong"
    with pytest.raises(ValueError, match="hash differs"):
        stage_raw_items([row], tmp_path / "other")
    source.unlink()
    with pytest.raises(ValueError, match="unavailable before model"):
        stage_raw_items([row], tmp_path / "missing")


async def test_cleanup_finds_persisted_session_when_send_failed_before_id_returned():
    from tank.integration_extract import interrupt_owned
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    await repo.put(
        "ops.agent_sessions",
        "created-before-send-timeout",
        {"session_id": "created-before-send-timeout", "item_id": "owned-item", "status": "OPEN"},
    )
    await repo.put(
        "ops.agent_sessions",
        "unrelated",
        {"session_id": "unrelated", "item_id": "another-item", "status": "OPEN"},
    )
    requests = []

    class Sessions:
        async def request(self, method, path, **kwargs):
            requests.append((method, path, kwargs))
            return {"data": {"status": "idle" if len(requests) > 1 else "running"}}

    result = await interrupt_owned(repo, Sessions(), ["owned-item"])
    assert result == [
        {"session_id": "created-before-send-timeout", "remote_status": "idle", "archived": True}
    ]
    assert any(
        method == "POST" and kwargs["json"]["events"] == [{"type": "user.interrupt"}]
        for method, _, kwargs in requests
    )
    assert all("unrelated" not in path for _, path, _ in requests)


def test_explicit_probe_selector_changes_only_database_not_credentials():
    from tank.integration_extract import probe_settings
    from tank.settings import Settings

    original = Settings(
        _env_file=None, database_url="postgresql://tank:FAKE-owner-password@postgres:5432/tank"
    )
    probe = probe_settings(original, "tank_integration_extract_20261018")
    assert original.database_url.endswith("/tank")
    assert (
        probe.database_url
        == "postgresql://tank:FAKE-owner-password@postgres:5432/tank_integration_extract_20261018"
    )
    for name in ["tank", "tank_integration_bad/name"]:
        with pytest.raises(ValueError, match="explicitly named"):
            probe_settings(original, name)


def test_raw_staging_same_basename_cannot_overwrite_distinct_payloads(tmp_path):
    import hashlib

    from tank.integration_extract import stage_raw_items

    rows = []
    for name, payload in [("one", b"first actual payload"), ("two", b"second actual payload")]:
        directory = tmp_path / name
        directory.mkdir()
        source = directory / "capture.bin"
        source.write_bytes(payload)
        rows.append(
            {"item_id": name, "raw_ref": str(source), "raw_sha256": hashlib.sha256(payload).hexdigest()}
        )
    staged, relocations = stage_raw_items(rows, tmp_path / "out")
    assert staged[0]["raw_ref"] != staged[1]["raw_ref"]
    assert Path(staged[0]["raw_ref"]).read_bytes() == b"first actual payload"
    assert Path(staged[1]["raw_ref"]).read_bytes() == b"second actual payload"
    assert all(Path(row["staged_raw_ref"]).stem == row["compressed_sha256"] for row in relocations)


async def test_probe_rejects_actual_application_refusal_even_with_done_returned_ledger():
    from tank.event_tools import EventToolService
    from tank.integration_extract import completed_extraction
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", {"session_id": "session", "event_id": "event"})
    await repo.put("evt.events", "event", {"event_id": "event"})
    await repo.put("ingest.source_items", "item", {"item_id": "item", "extraction_status": "DONE"})
    await repo.put(
        "evt.event_observations",
        "origin",
        {"event_id": "event", "item_id": "item", "link_type": "ORIGIN"},
    )
    arguments = {"item_id": "item", "is_event": True}
    tools = EventToolService(repo, None, None, None)
    refusal = await tools.dispatch("record_event", arguments, {"session_id": "session"})
    assert refusal["reason"] == "EVENT_ALREADY_BOUND"
    call = {
        "tool": "record_event",
        "status": "DONE",
        "returned_at": "2026-10-18T20:00:00Z",
        "arguments": arguments,
        "result": refusal,
    }
    # The previous predicate accepted this real dispatcher refusal.
    assert call["status"] == "DONE" and call["returned_at"]
    assert await completed_extraction(repo, [call], "item") is None


async def test_probe_requires_selected_item_origin_and_durable_event_before_success():
    from copy import deepcopy

    from tank.integration_extract import completed_extraction
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    call = {
        "tool": "record_event",
        "status": "DONE",
        "returned_at": "2026-10-18T20:00:00Z",
        "arguments": {"extraction": {"item_id": "selected", "is_event": True}},
        "result": {"event_id": "event", "accepted_claims": [], "dropped_claims": []},
    }
    assert await completed_extraction(repo, [call], "selected") is None
    await repo.put("ingest.source_items", "selected", {"item_id": "selected", "extraction_status": "DONE"})
    await repo.put("evt.events", "event", {"event_id": "event"})
    await repo.put(
        "evt.event_observations",
        "wrong-origin",
        {"event_id": "event", "item_id": "unselected", "link_type": "ORIGIN"},
    )
    assert await completed_extraction(repo, [call], "selected") is None
    await repo.put(
        "evt.event_observations",
        "correct-origin",
        {"event_id": "event", "item_id": "selected", "link_type": "ORIGIN"},
    )
    assert await completed_extraction(repo, [call], "selected") == call
    assert await completed_extraction(repo, [call], "unselected") is None
    for update in ({"returned_at": None}, {"status": "REFUSED"}, {"is_error": True}):
        changed = deepcopy(call)
        changed.update(update)
        assert await completed_extraction(repo, [changed], "selected") is None


async def test_probe_accepts_legitimate_non_event_only_after_durable_skip():
    from tank.integration_extract import completed_extraction
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    call = {
        "tool": "record_event",
        "status": "DONE",
        "returned_at": "2026-10-18T20:00:00Z",
        "arguments": {"item_id": "selected", "is_event": False},
        "result": {"is_event": False},
    }
    await repo.put("ingest.source_items", "selected", {"item_id": "selected", "extraction_status": "PENDING"})
    assert await completed_extraction(repo, [call], "selected") is None
    await repo.put("ingest.source_items", "selected", {"item_id": "selected", "extraction_status": "SKIPPED"})
    assert await completed_extraction(repo, [call], "selected") == call


def test_staging_repairs_legacy_metadata_only_from_matching_retained_feed(tmp_path):
    from copy import deepcopy

    from tank.integration_extract import stage_raw_items
    from tank.util import sha256

    payload = b"""<?xml version="1.0"?><rss version="2.0"><channel><title>Feed</title>
    <item><link>https://forum.example/topic/1</link><title>Original</title>
    <pubDate>Fri, 02 Oct 2026 12:35:48 +0000</pubDate><description>Authored text</description>
    </item></channel></rss>"""
    source = tmp_path / "retained.rss"
    source.write_bytes(payload)
    row = {
        "item_id": "selected",
        "source_id": "S23",
        "external_id": "https://forum.example/topic/1",
        "feed": "https://forum.example/latest.rss",
        "raw_ref": str(source),
        "raw_sha256": sha256(payload),
        "text": "Preserve captured text verbatim",
        "published_at": "2026-10-02T12:35:48.000000Z",
        "published_prec": "EXACT",
        "published_at_precision": "SECOND",
        "content_hash": "unchanged",
        "route_event_id": "prior-semantic-record",
    }
    before = deepcopy(row)
    sources = [{"source_id": "S23", "feeds": [{"url": row["feed"], "kind": "rss"}]}]
    staged, _ = stage_raw_items([row], tmp_path / "out", sources=sources)
    repaired = staged[0]
    assert row == before and source.read_bytes() == payload
    assert repaired["published_at"] == row["published_at"]
    assert repaired["published_prec"] == repaired["published_at_precision"] == "EXACT"
    assert repaired["text"] == row["text"] and repaired["content_hash"] == row["content_hash"]
    receipt = repaired["source_metadata_repair"]
    assert receipt["outcome"] == "VERIFIED"
    assert receipt["original_publication_fields"]["published_at_precision"] == "SECOND"
    assert "original_record" not in receipt and "prior-semantic-record" not in json.dumps(receipt)
    with pytest.raises(ValueError, match="matching configured feed"):
        stage_raw_items([row], tmp_path / "no-config")
    row["published_at"] = "2026-10-01T12:35:48Z"
    with pytest.raises(ValueError, match="unverified before model"):
        stage_raw_items([row], tmp_path / "unproven", sources=sources)
