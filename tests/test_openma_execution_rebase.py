import importlib.util
import json
from pathlib import Path

import pytest

BASE = Path(__file__).parents[1] / "deploy/openma_execution"
SPEC = importlib.util.spec_from_file_location("execution_overlay", BASE / "apply.py")
overlay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(overlay)


def fixture(tmp_path, *, rebased):
    rows = json.loads((BASE / "overlay.json").read_text())
    rebase = json.loads((BASE / "replay-dns-rebase.json").read_text())
    for row in rows:
        path = tmp_path / row["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        text = row["old"]
        if rebased and row["path"] == rebase["target"]:
            text = text.replace(rebase["original_runtime_identity"], rebase["runtime_identity"])
        path.write_text(text)
    (tmp_path / "tank-overlay.json").write_text(
        json.dumps({"retained_lifecycle": {"runtime_identity": rebase["runtime_identity"]}})
    )
    return rows, rebase


def test_manifest_identity_mismatch_is_refused_before_any_output(tmp_path, monkeypatch):
    rows, _ = fixture(tmp_path, rebased=True)
    monkeypatch.setattr(
        overlay,
        "verify_runtime",
        lambda root: {"source_build_verification": "PINNED_LOCAL_BUILD_VERIFIED_NATIVE_NOT_RUN"},
    )
    before = {row["path"]: (tmp_path / row["path"]).read_bytes() for row in rows}
    (tmp_path / "tank-overlay.json").write_text(
        json.dumps({"retained_lifecycle": {"runtime_identity": "0" * 64}})
    )
    with pytest.raises(ValueError, match="source guard mismatch"):
        overlay.apply(tmp_path, replay_dns=True)
    assert {name: (tmp_path / name).read_bytes() for name in before} == before


@pytest.mark.parametrize("identity", [None, "a" * 64])
def test_verified_architecture_build_changes_only_native_identity(tmp_path, monkeypatch, identity):
    rows, rebase = fixture(tmp_path, rebased=True)
    identity = identity or rebase["runtime_identity"]
    target = tmp_path / rebase["target"]
    target.write_text(target.read_text().replace(rebase["runtime_identity"], identity))
    (tmp_path / "tank-overlay.json").write_text(
        json.dumps({"retained_lifecycle": {"runtime_identity": identity}})
    )
    monkeypatch.setattr(
        overlay,
        "verify_runtime",
        lambda root: {"source_build_verification": "PINNED_LOCAL_BUILD_VERIFIED_NATIVE_NOT_RUN"},
    )
    overlay.apply(tmp_path, replay_dns=True)
    for row in rows:
        expected = row["new"]
        if row["path"] == rebase["target"]:
            expected = expected.replace(rebase["original_runtime_identity"], identity)
        assert (tmp_path / row["path"]).read_text() == expected


def test_native_contract_failure_precedes_source_writes(tmp_path, monkeypatch):
    rows, _ = fixture(tmp_path, rebased=True)
    before = {row["path"]: (tmp_path / row["path"]).read_bytes() for row in rows}

    def refuse(root):
        raise ValueError("native DNS source contract mismatch")

    monkeypatch.setattr(overlay, "verify_runtime", refuse)
    with pytest.raises(ValueError, match="DNS source contract"):
        overlay.apply(tmp_path, replay_dns=True)
    assert {name: (tmp_path / name).read_bytes() for name in before} == before
