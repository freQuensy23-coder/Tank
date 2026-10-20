import importlib.util
import json
import shutil
from pathlib import Path

import pytest

DEPLOY = Path(__file__).parents[1] / "deploy"


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def stage(tmp_path):
    assets = tmp_path / "assets"
    shutil.copytree(DEPLOY / "openma_continuation", assets)
    return module(assets / "prepare_overlay.py", "continuation_test")


def input_tree(tmp_path, overlay):
    # Tiny labeled source fixtures exercise all fifteen preflight boundaries.
    # The separate preserved-source composition verifies the frozen real hashes.
    root = tmp_path / "source"
    root.mkdir()
    manifest = json.loads((overlay.BASE / "manifest.json").read_text())
    (root / "tank-overlay.json").write_text(
        json.dumps({"retained_lifecycle": {"runtime_identity": manifest["fixed_pre_final_runtime_identity"]}})
    )
    for row in json.loads((overlay.BASE / "source-guards.json").read_text()):
        if row["before_sha256"] is None:
            continue
        target = root / row["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("guard fixture " + row["path"])
    return root


def guarded_fixture(tmp_path, overlay):
    """Exercise the real preflight algorithm with tiny exact fifteen-file guards."""
    root = input_tree(tmp_path, overlay)
    guards = json.loads((overlay.BASE / "source-guards.json").read_text())
    for row in guards:
        if row["before_sha256"] is not None:
            row["before_sha256"] = overlay.digest(root / row["path"])
    (overlay.BASE / "source-guards.json").write_text(json.dumps(guards))
    manifest = json.loads((overlay.BASE / "manifest.json").read_text())
    manifest["assets_sha256"]["source-guards.json"] = overlay.digest(overlay.BASE / "source-guards.json")
    (overlay.BASE / "manifest.json").write_text(json.dumps(manifest))
    return root, guards


def snapshot(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_all_fifteen_guards_then_exact_outputs_and_repeat_refusal(tmp_path):
    overlay = stage(tmp_path)
    root, guards = guarded_fixture(tmp_path, overlay)
    receipt = overlay.apply(root)
    assert len(receipt["patched_file_hashes"]) == 13
    assert len(receipt["added_file_hashes"]) == 2
    for row in guards:
        assert overlay.digest(root / row["path"]) == row["after_sha256"]
    before = snapshot(root)
    with pytest.raises(ValueError, match="guard mismatch|already exists"):
        overlay.apply(root)
    assert snapshot(root) == before


def test_last_source_guard_failure_changes_nothing(tmp_path):
    overlay = stage(tmp_path)
    root, guards = guarded_fixture(tmp_path, overlay)
    (root / guards[-1]["path"]).write_text("unexpected upstream bytes")
    before = snapshot(root)
    with pytest.raises(ValueError, match="source guard mismatch"):
        overlay.apply(root)
    assert snapshot(root) == before


@pytest.mark.parametrize("kind", ["existing", "dangling", "directory"])
def test_generated_target_refusal_precedes_all_writes(tmp_path, kind):
    overlay = stage(tmp_path)
    root, guards = guarded_fixture(tmp_path, overlay)
    target = root / next(row["path"] for row in guards if row["before_sha256"] is None)
    if kind == "existing":
        target.write_text("preserve")
    elif kind == "dangling":
        target.symlink_to(tmp_path / "absent")
    else:
        target.mkdir()
    original = (root / guards[0]["path"]).read_bytes()
    with pytest.raises(ValueError, match="already exists|symlink"):
        overlay.apply(root)
    assert (root / guards[0]["path"]).read_bytes() == original


@pytest.mark.parametrize("parent", [False, True])
def test_target_or_parent_symlink_never_writes_outside_root(tmp_path, parent):
    overlay = stage(tmp_path)
    root, guards = guarded_fixture(tmp_path, overlay)
    target = root / guards[0]["path"]
    outside = tmp_path / "outside"
    if parent:
        shutil.move(target.parent, outside)
        target.parent.symlink_to(outside, target_is_directory=True)
    else:
        shutil.move(target, outside)
        target.symlink_to(outside)
    before = snapshot(outside) if parent else outside.read_bytes()
    with pytest.raises(ValueError, match="symlink"):
        overlay.apply(root)
    assert (snapshot(outside) if parent else outside.read_bytes()) == before


def test_frozen_asset_tampering_refused_before_any_source_write(tmp_path):
    overlay = stage(tmp_path)
    root, guards = guarded_fixture(tmp_path, overlay)
    (overlay.BASE / guards[-1]["asset"]).write_text("altered replacement")
    before = snapshot(root)
    with pytest.raises(ValueError, match="asset hash mismatch"):
        overlay.apply(root)
    assert snapshot(root) == before


def test_stage_metadata_rejects_missing_guard_and_wrong_pin(tmp_path):
    overlay = stage(tmp_path)
    root, _ = guarded_fixture(tmp_path, overlay)
    manifest_path = overlay.BASE / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["upstream_openma_commit"] = "unknown"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="pin mismatch"):
        overlay.apply(root)


def test_path_traversal_absolute_and_root_symlink_refused(tmp_path):
    overlay = stage(tmp_path)
    root, _ = guarded_fixture(tmp_path, overlay)
    for relative in ("../escape", "/outside"):
        with pytest.raises(ValueError, match="Unsafe"):
            overlay.safe_path(root, relative)
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(ValueError, match="root must not"):
        overlay.apply(alias)


@pytest.mark.parametrize("invalid", ["missing", "duplicate"])
def test_all_fifteen_unique_guards_are_mandatory_before_writes(tmp_path, invalid):
    overlay = stage(tmp_path)
    root, guards = guarded_fixture(tmp_path, overlay)
    if invalid == "missing":
        guards.pop()
    else:
        guards[-1] = guards[0]
    path = overlay.BASE / "source-guards.json"
    path.write_text(json.dumps(guards))
    manifest_path = overlay.BASE / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["assets_sha256"]["source-guards.json"] = overlay.digest(path)
    manifest_path.write_text(json.dumps(manifest))
    before = snapshot(root)
    with pytest.raises(ValueError, match="fifteen unique"):
        overlay.apply(root)
    assert snapshot(root) == before


@pytest.mark.parametrize("tamper", [False, True])
def test_new_architecture_varies_only_the_single_guarded_identity(tmp_path, tamper):
    import hashlib

    overlay = stage(tmp_path)
    root, guards = guarded_fixture(tmp_path, overlay)
    manifest_path = overlay.BASE / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    fixed = manifest["fixed_pre_final_runtime_identity"]
    identity = "b" * 64
    rebase = json.loads((DEPLOY / "openma_execution/replay-dns-rebase.json").read_text())
    execution = next(
        row
        for row in json.loads((DEPLOY / "openma_execution/overlay.json").read_text())
        if row["path"] == rebase["target"]
    )
    original = execution["new"].replace(rebase["original_runtime_identity"], fixed)
    row = next(row for row in guards if row["path"] == rebase["target"])
    row["before_sha256"] = hashlib.sha256(original.encode()).hexdigest()
    (root / row["path"]).write_text(
        original.replace(fixed, identity) + ("// unrelated change\n" if tamper else "")
    )
    (root / "tank-overlay.json").write_text(
        json.dumps({"retained_lifecycle": {"runtime_identity": identity}})
    )
    (overlay.BASE / "source-guards.json").write_text(json.dumps(guards))
    manifest["assets_sha256"]["source-guards.json"] = overlay.digest(overlay.BASE / "source-guards.json")
    manifest_path.write_text(json.dumps(manifest))
    before = snapshot(root)
    if tamper:
        with pytest.raises(ValueError, match="source guard mismatch"):
            overlay.apply(root)
        assert snapshot(root) == before
    else:
        result = overlay.apply(root)
        expected = (overlay.BASE / row["asset"]).read_bytes().replace(fixed.encode(), identity.encode())
        assert (root / row["path"]).read_bytes() == expected
        assert result["patched_file_hashes"][row["path"]] == hashlib.sha256(expected).hexdigest()
