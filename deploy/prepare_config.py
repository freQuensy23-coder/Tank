#!/usr/bin/env python3
"""Record verified host config identity for a read-only container mount."""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

SUFFIXES = {".yaml", ".yml", ".json", ".md"}


def prepare_config(config_dir):
    root = Path(config_dir).resolve()
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if any(x.startswith(".") or x == "__pycache__" for x in relative.parts):
            continue
        if path.is_file() and path.suffix in SUFFIXES:
            if path.is_symlink():
                raise ValueError("Versioned config cannot contain symlinks")
            digest.update(relative.as_posix().encode() + b"\0" + path.read_bytes() + b"\0")
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain", "--", "."],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    )
    result = {"git_commit": commit, "content_sha256": digest.hexdigest(), "git_dirty": dirty}
    (root / ".tank-config-version.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_dir", nargs="?", default="config")
    args = parser.parse_args()
    print(json.dumps(prepare_config(args.config_dir)))
