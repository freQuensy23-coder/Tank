"""Apply the exact registered-tool-kind repair to a prepared pinned source tree."""

import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent


def digest(value):
    return hashlib.sha256(value).hexdigest()


def apply(root):
    root = Path(root).resolve()
    manifest = json.loads((BASE / "manifest.json").read_text())
    for name, expected in manifest["assets_sha256"].items():
        if digest((BASE / name).read_bytes()) != expected:
            raise ValueError("Tool-kind asset hash mismatch: " + name)
    replacements = json.loads((BASE / "replacements.json").read_text())
    planned = []
    for row in json.loads((BASE / "source-guards.json").read_text()):
        relative = Path(row["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Unsafe tool-kind target")
        target = root / relative
        if any(parent.is_symlink() for parent in [target, *target.parents] if parent != root.parent):
            raise ValueError("Tool-kind target or parent must not be a symlink")
        before = target.read_bytes()
        if digest(before) != row["before_sha256"]:
            raise ValueError("Tool-kind source guard mismatch: " + row["path"])
        text = before.decode()
        for change in replacements[row["path"]]:
            if text.count(change["before"]) != 1:
                raise ValueError("Tool-kind replacement must match exactly once")
            text = text.replace(change["before"], change["after"])
        after = text.encode()
        if digest(after) != row["after_sha256"]:
            raise ValueError("Tool-kind output hash mismatch: " + row["path"])
        planned.append((target, after, row["path"]))
    for target, after, _ in planned:
        target.write_bytes(after)
    return {name: digest(after) for _, after, name in planned}
