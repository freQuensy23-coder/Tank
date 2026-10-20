"""Apply the exact reviewed native callback-continuation repair to prepared source."""

import hashlib
import json
import re
from pathlib import Path

BASE = Path(__file__).resolve().parent
PIN = "85d5361f535c7221de975838dc48444e468cc02e"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def safe_path(root, relative):
    path = Path(relative)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ValueError("Unsafe continuation path")
    current = root
    for part in path.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("Continuation path contains a symlink: " + relative)
    if not current.resolve().is_relative_to(root):
        raise ValueError("Continuation path escapes prepared source")
    return current


def apply(root):
    original = Path(root).absolute()
    if original.is_symlink():
        raise ValueError("Continuation root must not be a symlink")
    root = original.resolve(strict=True)
    manifest = json.loads((BASE / "manifest.json").read_text())
    if manifest["upstream_openma_commit"] != PIN:
        raise ValueError("Continuation upstream pin mismatch")
    for relative, expected in manifest["assets_sha256"].items():
        asset = safe_path(BASE, relative)
        if not asset.is_file() or digest(asset) != expected:
            raise ValueError("Continuation asset hash mismatch: " + relative)
    guards = json.loads((BASE / "source-guards.json").read_text())
    if len(guards) != 15 or len({row["path"] for row in guards}) != 15:
        raise ValueError("Continuation requires all fifteen unique source guards")
    writes = []
    identity = json.loads((root / "tank-overlay.json").read_text())["retained_lifecycle"]["runtime_identity"]
    fixed = manifest["fixed_pre_final_runtime_identity"]
    if not re.fullmatch(r"[a-f0-9]{64}", identity):
        raise ValueError("Continuation requires the actual prepared runtime identity")
    # Validate every source, destination and replacement before changing any file.
    for row in guards:
        target = safe_path(root, row["path"])
        asset = safe_path(BASE, row["asset"])
        if digest(asset) != row["after_sha256"]:
            raise ValueError("Continuation replacement hash mismatch: " + row["path"])
        replacement = asset.read_bytes()
        before = target.read_bytes() if target.is_file() else None
        if row["path"] == "packages/control-plane-node/src/modules/node-managed.ts" and identity != fixed:
            if (
                before is None
                or before.count(identity.encode()) != 1
                or replacement.count(fixed.encode()) != 1
            ):
                raise ValueError("Continuation requires exactly one guarded runtime identity")
            before = before.replace(identity.encode(), fixed.encode())
            replacement = replacement.replace(fixed.encode(), identity.encode())
        if row["before_sha256"] is None:
            if target.exists() or target.is_symlink():
                raise ValueError("Continuation generated target already exists: " + row["path"])
        elif before is None or hashlib.sha256(before).hexdigest() != row["before_sha256"]:
            raise ValueError("Continuation source guard mismatch: " + row["path"])
        writes.append((target, replacement))
    for target, data in writes:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    return {
        "stage": manifest["schema"],
        "patched_file_hashes": {
            row["path"]: digest(safe_path(root, row["path"]))
            for row in guards
            if row["before_sha256"] is not None
        },
        "added_file_hashes": {
            row["path"]: digest(safe_path(root, row["path"]))
            for row in guards
            if row["before_sha256"] is None
        },
    }
