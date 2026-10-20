import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "deploy/openma_execution"
TARGET = "packages/control-plane-node/src/lib/node-managed-harness-runtime.ts"


def fixture(tmp_path):
    rows = json.loads((BASE / "overlay.json").read_text())
    for row in rows:
        path = tmp_path / row["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(row["old"])
    spec = importlib.util.spec_from_file_location("output_lifetime_overlay", BASE / "apply.py")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper, rows


def test_output_lifetime_uses_exact_upstream_file_and_canonical_apply(tmp_path):
    helper, rows = fixture(tmp_path)
    (row,) = [r for r in rows if r["path"] == TARGET]
    assert row["sha256"] == "258395cac7cb72b51c7c5aaf90d7658aa52a78919e78074fc2f1cbe9868a7e1e"
    helper.apply(tmp_path)
    assert (tmp_path / TARGET).read_text() == row["new"]
    for item in rows:
        assert hashlib.sha256((tmp_path / item["path"]).read_bytes()).hexdigest() == item["patched_sha256"]
    with pytest.raises(ValueError, match="source guard"):
        helper.apply(tmp_path)


def test_changed_lifetime_input_refuses_before_any_patch_writes(tmp_path):
    helper, rows = fixture(tmp_path)
    path = tmp_path / TARGET
    path.write_text(path.read_text() + "\nchanged input\n")
    snapshots = {row["path"]: (tmp_path / row["path"]).read_bytes() for row in rows}
    with pytest.raises(ValueError, match="source guard"):
        helper.apply(tmp_path)
    assert snapshots == {row["path"]: (tmp_path / row["path"]).read_bytes() for row in rows}
