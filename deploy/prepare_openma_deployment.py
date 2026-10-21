"""Compose reviewed OpenMA source stages; never build, deploy, or start a VM."""

import argparse
import hashlib
import importlib.util
import json
import os
import tempfile
from pathlib import Path

BASE = Path(__file__).resolve().parent
PIN = "85d5361f535c7221de975838dc48444e468cc02e"
MANAGED = "packages/control-plane-node/src/modules/node-managed.ts"
IDENTITY_MARKER = "TANK_COMPOSITE_RUNTIME_IDENTITY"
RECEIPT = "tank-deployment-composition.json"
FINAL_STAGES = ("openma_skill_aliases", "openma_file_io", "openma_replay_network")
CONTINUATION_STAGE = "openma_continuation"
TOOL_KIND_STAGE = "openma_tool_kind"
SKILL_MOUNT_STAGE = "openma_skill_mount"
NATIVE_QUALIFICATION = "boxlite/v0.10.6/native-qualification.json"
NATIVE_CHECKS = ("kvm", "state_persistence", "dns", "replay_isolation")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_module(relative):
    path = BASE / relative
    spec = importlib.util.spec_from_file_location("tank_prepare_" + path.parent.name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def asset_hashes():
    paths = {
        "prepare_openma_deployment.py",
        "prepare_openma_source.py",
        "prepare_boxlite_runtime.py",
        "openma-source-hashes.json",
        "openma_entrypoint.mjs",
        "openma-pg-time-fixes.json",
        "openma-tool-config-fixes.json",
        "openma-opaque-input-fixes.json",
        "openma-tank-network.test.ts",
        "openma_replay/replay-evidence.ts",
        "boxlite/v0.10.6/openma-sdk-patch.json",
        "openma_skills/manifest.json",
        "openma_lifecycle/prepare.py",
        "openma_lifecycle/source-guards.json",
        "openma_execution/apply.py",
        "openma_execution/overlay.json",
        "openma_execution/replay-dns-rebase.json",
        "openma_execution/assets.json",
    }
    sdk = json.loads((BASE / "boxlite/v0.10.6/openma-sdk-patch.json").read_text())
    paths.add("boxlite/v0.10.6/" + sdk["lockfile_asset"])
    paths.update(
        "boxlite/v0.10.6/" + name
        for name in (
            "qualified-artifact.json",
            "replay-dns-qualified-artifact.json",
            "source-pin.json",
            "platforms.json",
            "submodules.txt",
            "Dockerfile",
            "build.sh",
            "finalize-runtime.py",
        )
    )
    native_manifest = "boxlite/v0.10.6/replay-dns/asset-manifest.json"
    paths.add(native_manifest)
    for name, expected in json.loads((BASE / native_manifest).read_text())["assets_sha256"].items():
        relative = "boxlite/v0.10.6/replay-dns/" + name
        if digest(BASE / relative) != expected:
            raise ValueError("Frozen native build asset hash mismatch: " + relative)
        paths.add(relative)
    skill = json.loads((BASE / "openma_skills/manifest.json").read_text())
    for name, expected in skill["assets_sha256"].items():
        relative = "openma_skills/" + name
        if digest(BASE / relative) != expected:
            raise ValueError("Frozen skill asset hash mismatch: " + relative)
        paths.add(relative)
    paths.update(
        "openma_lifecycle/" + name
        for name in (
            "retained-session.ts",
            "boxlite-provider.ts",
            "node-lifecycle.ts",
            "replay-attachment.ts",
        )
    )
    paths.update(
        "openma_execution/" + row["asset"]
        for row in json.loads((BASE / "openma_execution/assets.json").read_text())
    )
    for stage in (*FINAL_STAGES, CONTINUATION_STAGE, TOOL_KIND_STAGE, SKILL_MOUNT_STAGE):
        paths.add(stage + "/manifest.json")
        manifest = json.loads((BASE / stage / "manifest.json").read_text())
        for name, expected in manifest["assets_sha256"].items():
            relative = stage + "/" + name
            if digest(BASE / relative) != expected:
                raise ValueError("Frozen stage asset hash mismatch: " + relative)
            paths.add(relative)
    paths.update(
        str(path.relative_to(BASE)) for path in (BASE / "openma_browser").iterdir() if path.is_file()
    )
    return {relative: digest(BASE / relative) for relative in sorted(paths)}


def verify_file_map(root, files):
    for relative, expected in files.items():
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError("Unsafe composition path")
        target = root / path
        if target.is_symlink() or digest(target) != expected:
            raise ValueError("Composite source hash mismatch: " + relative)


def verify_stage(root):
    manifest = json.loads((root / "tank-overlay.json").read_text())
    if manifest.get("upstream_commit") != PIN:
        raise ValueError("Composite upstream pin mismatch")
    if set(manifest.get("patched_file_hashes", {})) & set(manifest.get("added_file_hashes", {})):
        raise ValueError("Composite source appears in both file maps")
    verify_file_map(root, manifest.get("patched_file_hashes", {}))
    verify_file_map(root, manifest.get("added_file_hashes", {}))
    if digest(root / "apps/main-node/Dockerfile.tank") != manifest["dockerfile_sha256"]:
        raise ValueError("Composite Dockerfile hash mismatch")
    return manifest


def apply_final_stage(root, name):
    """Apply exact stage inputs and record their final bytes in the owning map."""
    stage = json.loads((BASE / name / "manifest.json").read_text())
    manifest_path = root / "tank-overlay.json"
    manifest = json.loads(manifest_path.read_text())
    target = stage["target"]
    owners = [key for key in ("patched_file_hashes", "added_file_hashes") if target in manifest[key]]
    if len(owners) != 1:
        raise ValueError("Stage target requires exactly one existing file-map owner: " + target)
    generated = stage.get("generated_modules_sha256", {})
    tracked = set(manifest["patched_file_hashes"]) | set(manifest["added_file_hashes"])
    if tracked & set(generated):
        raise ValueError("Stage generated file already has an owner")
    load_module(name + "/prepare_overlay.py").apply(root)
    verify_file_map(root, {target: stage["patched_sha256"], **generated})
    manifest[owners[0]][target] = stage["patched_sha256"]
    manifest["added_file_hashes"].update(generated)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {"manifest_sha256": digest(BASE / name / "manifest.json"), "manifest": stage}


def apply_continuation_stage(root, name=CONTINUATION_STAGE):
    """Preserve each existing owner; newly modified upstream files are patched."""
    stage_path = BASE / name
    guards = json.loads((stage_path / "source-guards.json").read_text())
    manifest_path = root / "tank-overlay.json"
    manifest = json.loads(manifest_path.read_text())
    ownership = {}
    for row in guards:
        target = row["path"]
        owners = [key for key in ("patched_file_hashes", "added_file_hashes") if target in manifest[key]]
        if len(owners) > 1 or (row["before_sha256"] is None and owners):
            raise ValueError("Continuation target has conflicting file-map ownership: " + target)
        ownership[target] = (
            owners[0]
            if owners
            else ("added_file_hashes" if row["before_sha256"] is None else "patched_file_hashes")
        )
    result = load_module(name + "/prepare_overlay.py").apply(root)
    expected = {row["path"]: row["after_sha256"] for row in guards}
    stage = json.loads((stage_path / "manifest.json").read_text())
    fixed = stage.get("fixed_pre_final_runtime_identity")
    identity = manifest["retained_lifecycle"]["runtime_identity"]
    if fixed and fixed != identity and MANAGED in expected:
        row = next(row for row in guards if row["path"] == MANAGED)
        raw = (stage_path / row["asset"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != row["after_sha256"] or raw.count(fixed.encode()) != 1:
            raise ValueError("Continuation identity template guard mismatch")
        expected[MANAGED] = hashlib.sha256(raw.replace(fixed.encode(), identity.encode())).hexdigest()
    verify_file_map(root, expected)
    for row in guards:
        manifest[ownership[row["path"]]][row["path"]] = expected[row["path"]]
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {"manifest_sha256": digest(stage_path / "manifest.json"), "source_hashes": result}


def sandbox_identity(evidence, controller_identity):
    """Allow a verified queue-only upgrade to retain already allocated guests."""
    previous = evidence.get("sandbox_compatibility")
    if previous is None:
        return controller_identity
    parent = previous["identity_evidence"]
    parent_identity = previous["controller_identity"]
    if canonical_digest(parent) != parent_identity:
        raise ValueError("Previous controller identity mismatch")
    current_files = evidence["normalized_source_sha256"]
    previous_files = parent["normalized_source_sha256"]
    changed = {name for name in current_files if current_files[name] != previous_files.get(name)}
    if (
        set(current_files) != set(previous_files)
        or not changed
        or not changed
        <= {
            "packages/session-runtime-sql/src/coordination.ts",
            "packages/control-plane-node/src/lib/node-session-execution-worker.ts",
        }
    ):
        raise ValueError("Sandbox compatibility requires only control-plane queue changes")
    for name in ("upstream_commit", "native_provenance_sha256", "native_checksums_sha256"):
        if evidence[name] != parent[name]:
            raise ValueError("Sandbox compatibility requires unchanged native artifacts")
    return sandbox_identity(parent, parent_identity)


def finalize(root, stages, assets, *, compatible_controller=None):
    """Bind all final source bytes, including skills, to one non-circular identity."""
    manifest = json.loads((root / "tank-overlay.json").read_text())
    old_identity = manifest["retained_lifecycle"]["runtime_identity"]
    managed = root / MANAGED
    text = managed.read_text()
    if text.count(old_identity) != 1:
        raise ValueError("Expected exactly one retained runtime identity")
    files = set(manifest["patched_file_hashes"]) | set(manifest["added_file_hashes"])
    replacements = json.loads((BASE / "openma_execution/overlay.json").read_text())
    additions = json.loads((BASE / "openma_execution/assets.json").read_text())
    files.update(row["path"] for row in replacements + additions)
    files.update(
        {
            "apps/main-node/Dockerfile.tank",
            "pnpm-workspace.yaml",
            "pnpm-lock.yaml",
            "tank-deploy/openma_entrypoint.mjs",
        }
    )
    normalized = {name: digest(root / name) for name in sorted(files)}
    normalized[MANAGED] = hashlib.sha256(text.replace(old_identity, IDENTITY_MARKER).encode()).hexdigest()
    evidence = {
        "version": 1,
        "upstream_commit": PIN,
        "stages": stages,
        "preparation_assets_sha256": assets,
        "normalized_source_sha256": normalized,
        "identity_marker": IDENTITY_MARKER,
        "native_provenance_sha256": digest(root / "tank-deploy/boxlite-runtime/provenance.json"),
        "native_checksums_sha256": digest(root / "tank-deploy/boxlite-runtime/SHA256SUMS"),
    }
    if compatible_controller is not None:
        evidence["sandbox_compatibility"] = {
            "controller_identity": compatible_controller["runtime_identity"],
            "identity_evidence": compatible_controller["identity_evidence"],
        }
    identity = canonical_digest(evidence)
    guest_identity = sandbox_identity(evidence, identity)
    managed.write_text(text.replace(old_identity, guest_identity))
    final_files = {name: digest(root / name) for name in sorted(files)}
    manifest["manifest_stage"] = "tank-deployment-composite-v1"
    manifest["pre_execution_stage_provenance"] = stages["retained"]
    manifest["deployment_composition"] = {"receipt": RECEIPT, "runtime_identity": identity}
    manifest["retained_lifecycle"]["runtime_identity"] = guest_identity
    for row in replacements:
        manifest["patched_file_hashes"][row["path"]] = final_files[row["path"]]
    for row in additions:
        manifest["added_file_hashes"][row["path"]] = final_files[row["path"]]
    manifest["patched_file_hashes"][MANAGED] = final_files[MANAGED]
    manifest["execution_protocol"] = {**stages["execution"], "runtime_identity": identity}
    (root / "tank-overlay.json").write_text(json.dumps(manifest, indent=2) + "\n")
    candidate = json.loads((root / "tank-retained-candidate.json").read_text())
    candidate.update(
        runtime_identity=identity,
        deployment_composition={"receipt": RECEIPT},
        source_sha256=final_files,
        execution_protocol=manifest["execution_protocol"],
    )
    candidate["composite_manifest"].update(
        sha256=digest(root / "tank-overlay.json"),
        final_source_count=len(final_files),
    )
    (root / "tank-retained-candidate.json").write_text(json.dumps(candidate, indent=2) + "\n")
    receipt = {
        "status": "PREPARED_NOT_BUILT_OR_NATIVE_QUALIFIED",
        "runtime_identity": identity,
        "identity_evidence": evidence,
        "final_file_sha256": final_files,
        "manifest_sha256": digest(root / "tank-overlay.json"),
        "candidate_sha256": digest(root / "tank-retained-candidate.json"),
        "native_qualification": "NOT_RUN",
        "production_adoption": False,
        "excluded_stages": [],
    }
    (root / RECEIPT).write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def verify(root):
    root = Path(root).resolve()
    receipt = json.loads((root / RECEIPT).read_text())
    identity = receipt["runtime_identity"]
    evidence = receipt["identity_evidence"]
    if evidence["upstream_commit"] != PIN or canonical_digest(evidence) != identity:
        raise ValueError("Composite identity evidence mismatch")
    verify_file_map(root, receipt["final_file_sha256"])
    manifest = verify_stage(root)
    if (
        digest(root / "tank-overlay.json") != receipt["manifest_sha256"]
        or digest(root / "tank-retained-candidate.json") != receipt["candidate_sha256"]
    ):
        raise ValueError("Composite metadata hash mismatch")
    text = (root / MANAGED).read_text()
    guest_identity = sandbox_identity(evidence, identity)
    if text.count(guest_identity) != 1:
        raise ValueError("Composite embedded identity mismatch")
    normalized = dict(receipt["final_file_sha256"])
    normalized[MANAGED] = hashlib.sha256(
        text.replace(guest_identity, evidence["identity_marker"]).encode()
    ).hexdigest()
    if normalized != evidence["normalized_source_sha256"]:
        raise ValueError("Composite normalized source mismatch")
    if manifest["retained_lifecycle"]["runtime_identity"] != guest_identity:
        raise ValueError("Composite manifest runtime identity mismatch")
    runtime = root / "tank-deploy/boxlite-runtime"
    if (
        digest(runtime / "provenance.json") != evidence["native_provenance_sha256"]
        or digest(runtime / "SHA256SUMS") != evidence["native_checksums_sha256"]
    ):
        raise ValueError("Composite native provenance mismatch")
    load_module("prepare_boxlite_runtime.py").verify_runtime(runtime)
    return receipt


def require_native_qualification(runtime):
    """Refuse a BoxLite runtime that has not passed native qualification on its architecture."""
    if runtime.get("boxlite_version") != "0.10.6":
        raise ValueError("Normal deployment requires a natively qualified BoxLite 0.10.6 runtime")
    if runtime.get("external_qualification", {}).get("status") == "PASS":
        return runtime["external_qualification"]
    architecture = runtime.get("architecture")
    path = BASE / NATIVE_QUALIFICATION
    entry = json.loads(path.read_text()).get(architecture) if path.is_file() else None
    if (
        runtime.get("source_build_verification") != "PINNED_LOCAL_BUILD_VERIFIED_NATIVE_NOT_RUN"
        or not entry
        or entry.get("status") != "PASS"
        or entry.get("shim_sha256") != runtime.get("artifact_sha256")
        or any(entry.get("checks", {}).get(check) != "PASS" for check in NATIVE_CHECKS)
    ):
        raise ValueError(
            f"Normal deployment refused: BoxLite runtime for {architecture} has not passed native qualification"
        )
    return entry


def deployment_verify(root):
    """Verify a composition and admit it for deployment only with a natively qualified runtime."""
    receipt = verify(root)
    runtime = load_module("prepare_boxlite_runtime.py").verify_runtime(
        Path(root).resolve() / "tank-deploy/boxlite-runtime"
    )
    return {**receipt, "native_qualification": require_native_qualification(runtime)}


def publish_new_directory(source, destination):
    """Atomically reserve a new name; preserve any pre-existing destination."""
    destination.mkdir()
    reserved = destination.stat()
    try:
        os.rename(source, destination)
    except BaseException:
        try:
            current = destination.stat(follow_symlinks=False)
            if (current.st_dev, current.st_ino) == (reserved.st_dev, reserved.st_ino):
                destination.rmdir()
        except OSError:
            pass
        raise


def prepare(source, destination, boxlite_runtime, *, qualification_candidate=False):
    destination = Path(destination).absolute()
    if destination.is_symlink() or destination.exists():
        raise ValueError("Destination must be new")
    source, destination = Path(source).resolve(), destination.resolve()
    if destination == source or source in destination.parents:
        raise ValueError("Destination must be outside the source tree")
    runtime = load_module("prepare_boxlite_runtime.py").verify_runtime(boxlite_runtime)
    if qualification_candidate:
        if runtime.get("boxlite_version") != "0.10.6" or (
            runtime.get("source_build_verification") != "PINNED_LOCAL_BUILD_VERIFIED_NATIVE_NOT_RUN"
        ):
            raise ValueError("A native qualification candidate requires a verified pinned local build")
    else:
        require_native_qualification(runtime)
    assets = asset_hashes()
    with tempfile.TemporaryDirectory(prefix=".tank-openma-compose-", dir=destination.parent) as temporary:
        staging = Path(temporary)
        base = staging / "base"
        load_module("prepare_openma_source.py").prepare(source, base, boxlite_runtime)
        base_manifest = verify_stage(base)
        skills = json.loads((BASE / "openma_skills/manifest.json").read_text())
        if digest(base / skills["target"]) != skills["patched_sha256"]:
            raise ValueError("Prepared skill stage hash mismatch")
        retained = staging / "retained"
        load_module("openma_lifecycle/prepare.py").prepare(base, retained)
        retained_manifest = verify_stage(retained)
        stages = {
            "order": ["base-tank8-with-skill-manifest", "retained-lifecycle-direct-sdk", "execution-status"],
            "base": {"manifest_sha256": digest(base / "tank-overlay.json"), "manifest": base_manifest},
            "skills": skills,
            "retained": {
                "manifest_sha256": digest(retained / "tank-overlay.json"),
                "manifest": retained_manifest,
            },
            "execution": {
                "overlay_sha256": digest(BASE / "openma_execution/overlay.json"),
                "assets_sha256": digest(BASE / "openma_execution/assets.json"),
                "native_identity_rebase_sha256": digest(BASE / "openma_execution/replay-dns-rebase.json"),
            },
        }
        load_module("openma_execution/apply.py").apply(retained, replay_dns=True)
        for name in FINAL_STAGES:
            stages[name] = apply_final_stage(retained, name)
            stages["order"].append(name)
        stages[CONTINUATION_STAGE] = apply_continuation_stage(retained)
        stages["order"].append(CONTINUATION_STAGE)
        stages[TOOL_KIND_STAGE] = apply_continuation_stage(retained, TOOL_KIND_STAGE)
        stages["order"].append(TOOL_KIND_STAGE)
        stages[SKILL_MOUNT_STAGE] = apply_continuation_stage(retained, SKILL_MOUNT_STAGE)
        stages["order"].append(SKILL_MOUNT_STAGE)
        if asset_hashes() != assets:
            raise ValueError("Preparation assets changed during composition")
        finalize(retained, stages, assets)
        receipt = verify(retained)
        publish_new_directory(retained, destination)
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    make = sub.add_parser("prepare")
    make.add_argument("source", type=Path)
    make.add_argument("destination", type=Path)
    make.add_argument("--boxlite-runtime", type=Path, required=True)
    make.add_argument(
        "--native-qualification-candidate",
        action="store_true",
        help="compose an unqualified local build only to run native qualification on it",
    )
    check = sub.add_parser("verify")
    check.add_argument("directory", type=Path)
    check.add_argument("--deployment", action="store_true", help="also require native qualification")
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare(
            args.source,
            args.destination,
            args.boxlite_runtime,
            qualification_candidate=args.native_qualification_candidate,
        )
    else:
        result = deployment_verify(args.directory) if args.deployment else verify(args.directory)
    print(
        json.dumps(
            {
                "status": result["status"],
                "runtime_identity": result["runtime_identity"],
                "final_files": len(result["final_file_sha256"]),
            }
        )
    )
