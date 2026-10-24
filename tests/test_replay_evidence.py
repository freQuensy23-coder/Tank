import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tank.replay_evidence import ReplayEvidenceError, ReplayEvidenceView, seal_replay_evidence
from tank.time_precision import propagate_timestamp


def print_row(trade_id, ts, received_ts, instrument_id="I"):
    return {
        "collection": "mkt.trades",
        "key": f"{instrument_id}:{trade_id}",
        "record": {
            "instrument_id": instrument_id,
            "trade_id": trade_id,
            "ts": ts,
            "received_ts": received_ts,
            "price": "1.5",
            "qty_native": "2",
            "side": "BUY",
        },
    }


def read_sealed(root, entry):
    content = (root / entry["relative_path"]).read_bytes()
    assert hashlib.sha256(content).hexdigest() == entry["sha256"]
    return [json.loads(line) for line in content.splitlines()]


def test_sealed_empty_run_binds_cutoff_and_rejects_reuse(tmp_path: Path) -> None:
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z", cutoff="2026-10-18T12:00:00Z", config={}, _records=[]
    )
    result = seal_replay_evidence(inputs, "RPL-test", tmp_path / "RPL-test", input_sha256="a" * 64)
    assert result["manifest"]["files"] == []
    assert result["manifest"]["format"] == "jsonl"
    assert result["manifest"]["cutoff"] == inputs.cutoff
    assert result["manifest"]["cutoff_prec"] is None
    assert result["manifest"]["cutoff_prec_reason"]
    assert (tmp_path / "RPL-test.source-audit.json").exists()
    with pytest.raises(ReplayEvidenceError, match="immutable"):
        seal_replay_evidence(inputs, "RPL-test", tmp_path / "RPL-test", input_sha256="a" * 64)


def test_sealed_evidence_refuses_path_escape(tmp_path: Path) -> None:
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z", cutoff="2026-10-18T12:00:00Z", config={}, _records=[]
    )
    with pytest.raises(ReplayEvidenceError, match="identity"):
        seal_replay_evidence(inputs, "../escape", tmp_path / "out", input_sha256="a" * 64)


def test_packet_prints_are_sealed_before_the_cutoff_only(tmp_path: Path) -> None:
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z",
        cutoff="2026-10-18T12:00:17Z",
        config={},
        _records=[
            print_row("2", "2026-10-18T12:00:20Z", "2026-10-18T12:00:20Z"),
            print_row("1", "2026-10-18T12:00:10Z", "2026-10-18T12:00:10Z"),
        ],
    )
    result = seal_replay_evidence(inputs, "RPL-prints", tmp_path / "RPL-prints", input_sha256="b" * 64)
    (entry,) = result["manifest"]["files"]
    assert entry["relative_path"] == entry["sha256"] + ".jsonl"
    assert entry["row_count"] == 1
    assert [row["trade_id"] for row in read_sealed(tmp_path / "RPL-prints", entry)] == ["1"]


def test_conflicting_prints_of_one_identity_are_refused(tmp_path: Path) -> None:
    first = print_row("1", "2026-10-18T12:00:10Z", "2026-10-18T12:00:10Z")
    second = print_row("1", "2026-10-18T12:00:10Z", "2026-10-18T12:00:10Z")
    second["record"]["price"] = "9"
    second["key"] = "other"
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z",
        cutoff="2026-10-18T12:01:00Z",
        config={},
        _records=[first, second],
    )
    with pytest.raises(ReplayEvidenceError, match="Conflicting"):
        seal_replay_evidence(inputs, "RPL-conflict", tmp_path / "RPL-conflict", input_sha256="c" * 64)


def test_same_guest_directory_exposes_only_each_message_prefix(tmp_path: Path) -> None:
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z",
        cutoff="2026-10-17T12:01:00Z",
        config={},
        _records=[
            print_row("1", "2026-10-17T11:59:50Z", "2026-10-17T11:59:50Z"),
            print_row("2", "2026-10-17T12:00:30Z", "2026-10-17T12:00:30Z"),
        ],
    )
    visible = tmp_path / "guest" / "RPL-prefix-two"
    view = ReplayEvidenceView(
        inputs, "RPL-prefix-two", visible, tmp_path / "private-projections", input_sha256="d" * 64
    )

    def read_guest():
        manifest = json.loads((visible / "manifest.json").read_text())
        rows = [row for entry in manifest["files"] for row in read_sealed(visible, entry)]
        assert {path.name for path in visible.iterdir()} == {
            "manifest.json",
            *[entry["relative_path"] for entry in manifest["files"]],
        }
        return rows

    first = view.advance(propagate_timestamp({"at": "2026-10-17T12:00:00Z"}, "at"))
    inode = visible.stat().st_ino
    assert len(read_guest()) == 1
    first_hash = first["manifest_sha256"]
    view.advance(propagate_timestamp({"at": inputs.cutoff}, "at"))
    assert visible.stat().st_ino == inode
    assert len(read_guest()) == 2
    assert view.capture["manifest_sha256"] != first_hash
    assert len(view.receipts) == 2


def test_as_of_projection_waits_for_actual_receipt(tmp_path: Path) -> None:
    inputs = SimpleNamespace(
        cutoff="2026-10-18T12:01:00Z",
        assembled_at=None,
        config={},
        _records=[print_row("late", "2026-10-18T11:59:50Z", "2026-10-18T12:00:30Z")],
    )
    view = ReplayEvidenceView(
        inputs, "RPL-receipt", tmp_path / "guest", tmp_path / "private", input_sha256="e" * 64
    )
    early = view.advance(propagate_timestamp({"at": "2026-10-18T12:00:00Z"}, "at"))
    assert early["manifest"]["files"][0]["row_count"] == 0
    late = view.advance(propagate_timestamp({"at": inputs.cutoff}, "at"))
    assert late["manifest"]["files"][0]["row_count"] == 1
