"""Offline composite provenance checks; creates no containers or native resources."""

import importlib.util
import json
from hashlib import sha256
from pathlib import Path

spec = importlib.util.spec_from_file_location("retained_prepare", Path(__file__).with_name("prepare.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_composite_preserves_base_and_hashes_only_final_files(tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    original = {
        "patched_file_hashes": {"old.ts": "stale"},
        "added_file_hashes": {"added.ts": "stale"},
        "sandbox_browser": {"worker_sha256": "historical_worker"},
    }
    raw = json.dumps(original).encode()
    (source / "tank-overlay.json").write_bytes(raw)
    paths = [
        "old.ts",
        "added.ts",
        "packages/control-plane-node/src/lib/node-managed-session-runner.ts",
        "packages/control-plane-node/src/modules/node-managed.ts",
        "packages/control-plane-node/package.json",
        "pnpm-lock.yaml",
        "apps/main-node/Dockerfile.tank",
    ]
    paths += [
        "packages/control-plane-node/src/lib/tank-retained/" + name
        for name in [
            "retained-session.ts",
            "boxlite-provider.ts",
            "node-lifecycle.ts",
            "replay-attachment.ts",
        ]
    ]
    for name in paths:
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("final:" + name)
    result = module.refresh_composite_manifest(source, destination, "identity")
    assert (source / "tank-overlay.json").read_bytes() == raw
    assert result["base_stage_provenance"] == {
        "manifest_sha256": sha256(raw).hexdigest(),
        "manifest": original,
    }
    assert result["sandbox_browser"] == original["sandbox_browser"]
    for field in ["patched_file_hashes", "added_file_hashes"]:
        for name, digest in result[field].items():
            assert digest == sha256((destination / name).read_bytes()).hexdigest()
    assert (
        result["dockerfile_sha256"]
        == sha256((destination / "apps/main-node/Dockerfile.tank").read_bytes()).hexdigest()
    )
    assert result["retained_lifecycle"]["native_vm_proof"] == "NOT_RUN"
    assert not result["retained_lifecycle"]["production_adoption"]
    assert json.loads((destination / "tank-overlay.json").read_text()) == result
