import copy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

SPEC = importlib.util.spec_from_file_location(
    "deployment_composer", Path(__file__).parents[1] / "deploy/prepare_openma_deployment.py"
)
composer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(composer)


def write(root, relative, value):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)


def prepared(tmp_path, monkeypatch, name="tree"):
    root = tmp_path / name
    deploy = tmp_path / "deploy"
    write(deploy, "openma_execution/overlay.json", json.dumps([{"path": composer.MANAGED}]))
    write(deploy, "openma_execution/assets.json", json.dumps([{"path": "new-api.ts"}]))
    monkeypatch.setattr(composer, "BASE", deploy)
    monkeypatch.setattr(
        composer, "load_module", lambda name: SimpleNamespace(verify_runtime=lambda root: None)
    )
    for relative, value in {
        composer.MANAGED: 'const identity = "old-identity";\n',
        "skills.ts": "validated skill root\n",
        "new-api.ts": "authenticated bounded read\n",
        "apps/main-node/Dockerfile.tank": "FROM pinned-image\n",
        "pnpm-workspace.yaml": "pinned workspace\n",
        "pnpm-lock.yaml": "exact sdk lock\n",
        "tank-deploy/openma_entrypoint.mjs": "public app startup\n",
        "tank-deploy/boxlite-runtime/provenance.json": '{"source":"pinned"}\n',
        "tank-deploy/boxlite-runtime/SHA256SUMS": "qualified checksums\n",
    }.items():
        write(root, relative, value)
    manifest = {
        "upstream_commit": composer.PIN,
        "patched_file_hashes": {p: composer.digest(root / p) for p in (composer.MANAGED, "skills.ts")},
        "added_file_hashes": {},
        "dockerfile_sha256": composer.digest(root / "apps/main-node/Dockerfile.tank"),
        "retained_lifecycle": {"runtime_identity": "old-identity"},
    }
    write(root, "tank-overlay.json", json.dumps(manifest))
    write(root, "tank-retained-candidate.json", '{"composite_manifest":{}}')
    stages = {"retained": {"manifest": copy.deepcopy(manifest)}, "execution": {"guard": "exact"}}
    return root, stages


def test_identity_is_directory_independent_and_verifies(tmp_path, monkeypatch):
    first, stages = prepared(tmp_path, monkeypatch, "one")
    second, _ = prepared(tmp_path, monkeypatch, "two")
    result = composer.finalize(first, stages, {"asset": "same-hash"})
    other = composer.finalize(second, stages, {"asset": "same-hash"})
    assert result == other
    assert composer.verify(first) == result
    assert composer.verify(second) == other
    assert result["native_qualification"] == "NOT_RUN"
    assert not result["production_adoption"]


@pytest.mark.parametrize("changed", ["skills.ts", "new-api.ts", "pnpm-lock.yaml"])
def test_any_final_stage_source_tamper_is_refused(tmp_path, monkeypatch, changed):
    root, stages = prepared(tmp_path, monkeypatch)
    composer.finalize(root, stages, {})
    (root / changed).write_text("unexpected source")
    with pytest.raises(ValueError, match="source hash mismatch"):
        composer.verify(root)


def test_source_and_stage_assets_both_change_runtime_identity(tmp_path, monkeypatch):
    first, stages = prepared(tmp_path, monkeypatch, "one")
    second, _ = prepared(tmp_path, monkeypatch, "two")
    third, _ = prepared(tmp_path, monkeypatch, "three")
    (second / "skills.ts").write_text("changed skill resolution\n")
    identities = {
        composer.finalize(first, stages, {"asset": "one"})["runtime_identity"],
        composer.finalize(second, stages, {"asset": "one"})["runtime_identity"],
        composer.finalize(third, stages, {"asset": "two"})["runtime_identity"],
    }
    assert len(identities) == 3


def test_metadata_and_native_provenance_tamper_are_refused(tmp_path, monkeypatch):
    root, stages = prepared(tmp_path, monkeypatch)
    composer.finalize(root, stages, {})
    manifest = root / "tank-overlay.json"
    original = manifest.read_bytes()
    manifest.write_text("{}")
    with pytest.raises((ValueError, KeyError)):
        composer.verify(root)
    manifest.write_bytes(original)
    (root / "tank-deploy/boxlite-runtime/SHA256SUMS").write_text("different native artifact")
    with pytest.raises(ValueError, match="native provenance mismatch"):
        composer.verify(root)


def test_failed_guard_publishes_nothing_and_preserves_source(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    write(source, "input", "immutable original")
    destination = tmp_path / "prepared"
    monkeypatch.setattr(composer, "asset_hashes", dict)

    def fail(source, destination, runtime):
        destination.mkdir()
        write(destination, "partial", "not publishable")
        raise ValueError("Pinned source changed")

    monkeypatch.setattr(
        composer,
        "load_module",
        lambda name: SimpleNamespace(
            prepare=fail,
            verify_runtime=lambda path: {
                "boxlite_version": "0.10.6",
                "external_qualification": {"status": "PASS"},
            },
        ),
    )
    with pytest.raises(ValueError, match="Pinned source changed"):
        composer.prepare(source, destination, tmp_path / "runtime")
    assert not destination.exists()
    assert not list(tmp_path.glob(".tank-openma-compose-*"))
    assert (source / "input").read_text() == "immutable original"


def test_existing_destination_is_never_overwritten(tmp_path):
    destination = tmp_path / "existing"
    destination.write_text("preserve")
    with pytest.raises(ValueError, match="Destination must be new"):
        composer.prepare(tmp_path / "source", destination, tmp_path / "runtime")
    assert destination.read_text() == "preserve"


def test_dangling_destination_symlink_is_refused_before_resolution(tmp_path):
    destination = tmp_path / "dangling"
    destination.symlink_to(tmp_path / "absent-target", target_is_directory=True)
    with pytest.raises(ValueError, match="Destination must be new"):
        composer.prepare(tmp_path / "source", destination, tmp_path / "runtime")
    assert destination.is_symlink()
    assert not (tmp_path / "absent-target").exists()


def test_publish_refuses_racing_directory_and_cleans_own_failed_reservation(tmp_path, monkeypatch):
    source, destination = tmp_path / "ready", tmp_path / "output"
    source.mkdir()
    destination.mkdir()
    with pytest.raises(FileExistsError):
        composer.publish_new_directory(source, destination)
    assert source.is_dir() and destination.is_dir()
    destination.rmdir()

    def fail(*args):
        raise OSError("publish failed")

    monkeypatch.setattr(composer.os, "rename", fail)
    with pytest.raises(OSError, match="publish failed"):
        composer.publish_new_directory(source, destination)
    assert source.is_dir() and not destination.exists()


@pytest.mark.parametrize("owner", ["patched_file_hashes", "added_file_hashes"])
def test_final_stage_preserves_original_file_map_owner(tmp_path, monkeypatch, owner):
    root, _ = prepared(tmp_path, monkeypatch)
    target = "skills.ts"
    manifest_path = root / "tank-overlay.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["patched_file_hashes"].pop(target)
    manifest[owner][target] = composer.digest(root / target)
    manifest_path.write_text(json.dumps(manifest))
    write(root, "expected", "new source\n")
    write(root, "helper-expected", "helper\n")
    stage = {
        "target": target,
        "patched_sha256": composer.digest(root / "expected"),
        "generated_modules_sha256": {"helper.ts": composer.digest(root / "helper-expected")},
    }
    write(composer.BASE, "test-stage/manifest.json", json.dumps(stage))

    def apply(directory):
        write(directory, target, "new source\n")
        write(directory, "helper.ts", "helper\n")

    monkeypatch.setattr(composer, "load_module", lambda name: SimpleNamespace(apply=apply))
    receipt = composer.apply_final_stage(root, "test-stage")
    final = composer.verify_stage(root)
    assert final[owner][target] == stage["patched_sha256"]
    assert not (set(final["patched_file_hashes"]) & set(final["added_file_hashes"]))
    assert final["added_file_hashes"]["helper.ts"] == stage["generated_modules_sha256"]["helper.ts"]
    assert receipt["manifest"] == stage


def test_duplicate_file_map_ownership_is_refused(tmp_path, monkeypatch):
    root, _ = prepared(tmp_path, monkeypatch)
    manifest_path = root / "tank-overlay.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["added_file_hashes"]["skills.ts"] = manifest["patched_file_hashes"]["skills.ts"]
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="both file maps"):
        composer.verify_stage(root)


@pytest.mark.parametrize("runtime", [{"boxlite_version": "0.9.0"}, {"boxlite_version": "0.10.6"}])
def test_normal_composer_rejects_historical_runtime_before_preparing(tmp_path, monkeypatch, runtime):
    monkeypatch.setattr(
        composer, "load_module", lambda name: SimpleNamespace(verify_runtime=lambda path: runtime)
    )
    with pytest.raises(ValueError, match="natively qualified|has not passed native qualification"):
        composer.prepare(tmp_path / "source", tmp_path / "output", tmp_path / "runtime")
    assert not (tmp_path / "output").exists()


LOCAL_AMD64 = {
    "boxlite_version": "0.10.6",
    "architecture": "amd64",
    "artifact_sha256": "a" * 64,
    "source_build_verification": "PINNED_LOCAL_BUILD_VERIFIED_NATIVE_NOT_RUN",
}
PASSED = {"kvm": "PASS", "state_persistence": "PASS", "dns": "PASS", "replay_isolation": "PASS"}


def gate_fixture(tmp_path, monkeypatch, registry):
    deploy = tmp_path / "deploy"
    if registry is not None:
        write(deploy, composer.NATIVE_QUALIFICATION, json.dumps(registry))
    monkeypatch.setattr(composer, "BASE", deploy)
    monkeypatch.setattr(
        composer, "load_module", lambda name: SimpleNamespace(verify_runtime=lambda path: dict(LOCAL_AMD64))
    )

    def reached(*args, **kwargs):
        raise RuntimeError("gate passed")

    monkeypatch.setattr(composer, "asset_hashes", reached)


@pytest.mark.parametrize(
    "registry",
    [
        None,
        {},
        {"arm64": {"status": "PASS", "shim_sha256": "a" * 64, "checks": PASSED}},
        {"amd64": {"status": "FAIL", "shim_sha256": "a" * 64, "checks": PASSED}},
        {"amd64": {"status": "PASS", "shim_sha256": "b" * 64, "checks": PASSED}},
        {"amd64": {"status": "PASS", "shim_sha256": "a" * 64, "checks": {**PASSED, "dns": "NOT_RUN"}}},
        {"amd64": {"status": "PASS", "shim_sha256": "a" * 64, "checks": {"kvm": "PASS"}}},
    ],
)
def test_deploy_refuses_local_runtime_without_native_qualification(tmp_path, monkeypatch, registry):
    gate_fixture(tmp_path, monkeypatch, registry)
    with pytest.raises(ValueError, match="amd64 has not passed native qualification"):
        composer.prepare(tmp_path / "source", tmp_path / "output", tmp_path / "runtime")
    assert not (tmp_path / "output").exists()


def test_deploy_admits_local_runtime_after_native_qualification(tmp_path, monkeypatch):
    gate_fixture(
        tmp_path, monkeypatch, {"amd64": {"status": "PASS", "shim_sha256": "a" * 64, "checks": PASSED}}
    )
    with pytest.raises(RuntimeError, match="gate passed"):
        composer.prepare(tmp_path / "source", tmp_path / "output", tmp_path / "runtime")


def test_qualification_candidate_is_composed_only_on_explicit_request(tmp_path, monkeypatch):
    gate_fixture(tmp_path, monkeypatch, None)
    with pytest.raises(RuntimeError, match="gate passed"):
        composer.prepare(
            tmp_path / "source", tmp_path / "output", tmp_path / "runtime", qualification_candidate=True
        )


def test_deployment_verify_refuses_unqualified_composition(tmp_path, monkeypatch):
    gate_fixture(tmp_path, monkeypatch, None)
    monkeypatch.setattr(composer, "verify", lambda root: {"status": "PREPARED_NOT_BUILT_OR_NATIVE_QUALIFIED"})
    with pytest.raises(ValueError, match="has not passed native qualification"):
        composer.deployment_verify(tmp_path)
    write(
        composer.BASE,
        composer.NATIVE_QUALIFICATION,
        json.dumps({"amd64": {"status": "PASS", "shim_sha256": "a" * 64, "checks": PASSED}}),
    )
    assert composer.deployment_verify(tmp_path)["native_qualification"]["status"] == "PASS"


def continuation_fixture(tmp_path, monkeypatch):
    root, _ = prepared(tmp_path, monkeypatch)
    rows = [
        {"path": "skills.ts", "before_sha256": "exact", "after_sha256": "one"},
        {"path": "native-added.ts", "before_sha256": "exact", "after_sha256": "two"},
        {"path": "previously-unmodified.ts", "before_sha256": "exact", "after_sha256": "three"},
        {"path": "new-continuation.ts", "before_sha256": None, "after_sha256": "four"},
    ]
    values = ["patched existing", "modified prior addition", "new upstream patch", "new module"]
    for row, value in zip(rows, values, strict=True):
        row["after_sha256"] = composer.canonical_digest(value)
    write(composer.BASE, "openma_continuation/source-guards.json", json.dumps(rows))
    write(composer.BASE, "openma_continuation/manifest.json", "{}")
    manifest_path = root / "tank-overlay.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["added_file_hashes"]["native-added.ts"] = "previous bytes"
    manifest_path.write_text(json.dumps(manifest))

    def apply(directory):
        for row, value in zip(rows, values, strict=True):
            write(directory, row["path"], json.dumps(value, sort_keys=True, separators=(",", ":")))
        return {"mock": "file-map classification only"}

    monkeypatch.setattr(composer, "load_module", lambda name: SimpleNamespace(apply=apply))
    return root, rows


def test_continuation_preserves_prior_owner_and_classifies_new_upstream_patch(tmp_path, monkeypatch):
    root, rows = continuation_fixture(tmp_path, monkeypatch)
    composer.apply_continuation_stage(root)
    manifest = json.loads((root / "tank-overlay.json").read_text())
    assert manifest["patched_file_hashes"]["skills.ts"] == rows[0]["after_sha256"]
    assert manifest["added_file_hashes"]["native-added.ts"] == rows[1]["after_sha256"]
    assert manifest["patched_file_hashes"]["previously-unmodified.ts"] == rows[2]["after_sha256"]
    assert manifest["added_file_hashes"]["new-continuation.ts"] == rows[3]["after_sha256"]
    assert not set(manifest["patched_file_hashes"]) & set(manifest["added_file_hashes"])


def test_continuation_conflicting_owner_refused_before_stage_mutation(tmp_path, monkeypatch):
    root, _ = continuation_fixture(tmp_path, monkeypatch)
    path = root / "tank-overlay.json"
    manifest = json.loads(path.read_text())
    manifest["added_file_hashes"]["skills.ts"] = "conflict"
    path.write_text(json.dumps(manifest))
    before = (root / "skills.ts").read_bytes()
    with pytest.raises(ValueError, match="conflicting file-map ownership"):
        composer.apply_continuation_stage(root)
    assert (root / "skills.ts").read_bytes() == before


def test_tool_kind_multifile_stage_preserves_file_map_ownership(tmp_path, monkeypatch):
    root, rows = continuation_fixture(tmp_path, monkeypatch)
    stage = composer.BASE / composer.TOOL_KIND_STAGE
    stage.mkdir()
    (stage / "source-guards.json").write_text(json.dumps(rows))
    (stage / "manifest.json").write_text("{}")
    composer.apply_continuation_stage(root, composer.TOOL_KIND_STAGE)
    manifest = json.loads((root / "tank-overlay.json").read_text())
    assert manifest["patched_file_hashes"]["skills.ts"] == rows[0]["after_sha256"]
    assert manifest["added_file_hashes"]["native-added.ts"] == rows[1]["after_sha256"]
    assert manifest["patched_file_hashes"]["previously-unmodified.ts"] == rows[2]["after_sha256"]
    assert not set(manifest["patched_file_hashes"]) & set(manifest["added_file_hashes"])


def test_queue_only_upgrade_preserves_guest_identity_and_changes_controller(tmp_path, monkeypatch):
    root, stages = prepared(tmp_path, monkeypatch)
    queue = "packages/session-runtime-sql/src/coordination.ts"
    write(root, queue, "old queue ordering")
    manifest = json.loads((root / "tank-overlay.json").read_text())
    manifest["patched_file_hashes"][queue] = composer.digest(root / queue)
    write(root, "tank-overlay.json", json.dumps(manifest))
    previous = composer.finalize(root, stages, {})
    old_guest_module = (root / composer.MANAGED).read_bytes()
    (root / queue).write_text("callback priority; same guest and continuation protocol")
    manifest = json.loads((root / "tank-overlay.json").read_text())
    manifest["patched_file_hashes"][queue] = composer.digest(root / queue)
    write(root, "tank-overlay.json", json.dumps(manifest))
    result = composer.finalize(root, stages, {}, compatible_controller=previous)
    assert result["runtime_identity"] != previous["runtime_identity"]
    assert (root / composer.MANAGED).read_bytes() == old_guest_module
    assert composer.verify(root) == result
    checksums = root / "tank-deploy/boxlite-runtime/SHA256SUMS"
    original_checksums = checksums.read_bytes()
    checksums.write_text("different native binaries")
    with pytest.raises(ValueError, match="unchanged native artifacts"):
        composer.finalize(root, stages, {}, compatible_controller=previous)
    checksums.write_bytes(original_checksums)
    (root / "skills.ts").write_text("guest-facing change")
    with pytest.raises(ValueError, match="only control-plane queue changes"):
        composer.finalize(root, stages, {}, compatible_controller=previous)


def test_queue_compatibility_rejects_forged_parent(tmp_path, monkeypatch):
    root, stages = prepared(tmp_path, monkeypatch)
    previous = composer.finalize(root, stages, {})
    fake = copy.deepcopy(previous)
    fake["identity_evidence"]["upstream_commit"] = "untrusted"
    with pytest.raises(ValueError, match="Previous controller identity mismatch"):
        composer.finalize(root, stages, {}, compatible_controller=fake)
