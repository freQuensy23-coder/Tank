"""Fixture support for the real pinned OpenMA dependency build."""

import hashlib
import importlib.util
import json
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def preparer():
    spec = importlib.util.spec_from_file_location(
        "tank_openma_preparer", ROOT / "deploy/prepare_openma_source.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(source, destination):
    source, destination = Path(source).resolve(strict=True), Path(destination)
    if destination.exists() or destination.is_symlink():
        raise ValueError("Destination must be new")
    destination = destination.resolve()
    if destination == source or source in destination.parents:
        raise ValueError("Dependency build destination must be outside the upstream checkout")
    helper = preparer()
    commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    if commit != helper.PIN:
        raise ValueError("OpenMA checkout does not match pinned commit")
    status = subprocess.check_output(
        ["git", "-C", str(source), "status", "--porcelain", "--untracked-files=all"], text=True
    )
    if status:
        raise ValueError("OpenMA dependency build requires a clean pinned checkout")
    sdk_root = ROOT / "deploy/boxlite/v0.10.6"
    sdk = json.loads((sdk_root / "openma-sdk-patch.json").read_text())
    if sdk["upstream_openma_commit"] != helper.PIN:
        raise ValueError("SDK dependency source pin mismatch")
    for relative, expected in sdk["source_sha256"].items():
        if digest(source / relative) != expected:
            raise ValueError("SDK dependency source hash mismatch: " + relative)
    lockfile = sdk_root / sdk["lockfile_asset"]
    if digest(lockfile) != sdk["lockfile_sha256"]:
        raise ValueError("SDK dependency lock asset hash mismatch")
    # Use the same production renderer. Later native-image instructions are not
    # part of the deps target and this context contains no substitute runtime.
    dockerfile = helper.prepare_dockerfile((source / "apps/main-node/Dockerfile").read_text())
    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns(".git", "node_modules", ".env", ".env.*", "data"),
    )
    workspace = destination / "pnpm-workspace.yaml"
    workspace.write_text(workspace.read_text() + sdk["workspace_append"])
    shutil.copyfile(lockfile, destination / "pnpm-lock.yaml")
    output = destination / "apps/main-node/Dockerfile.tank-ci"
    output.write_text(dockerfile)
    receipt = {
        "scope": "OpenMA dependency build only",
        "upstream_commit": helper.PIN,
        "sdk_version": sdk["sdk_version"],
        "pnpm_version": sdk["pnpm_version"],
        "frozen_lock_sha256": digest(destination / "pnpm-lock.yaml"),
        "dockerfile_sha256": digest(output),
        "native_runtime_bundle_supplied": False,
        "native_image_qualification": "NOT_RUN",
    }
    (destination / "tank-dependency-build.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt
