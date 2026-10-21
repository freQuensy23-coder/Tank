#!/usr/bin/env python3
"""Check actual BuildKit context filtering with synthetic forbidden inputs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def source_files():
    files = []
    for directory, dirs, names in os.walk(ROOT):
        dirs[:] = sorted(name for name in dirs if name != ".git")
        for name in sorted(names):
            path = Path(directory) / name
            if path.is_symlink():
                raise ValueError("Context proof requires regular input: " + str(path))
            files.append(path.relative_to(ROOT))
    return files


def ignored_main(path):
    parts = path.parts
    leaf = parts[-1]
    return (
        any(part in {".git", "__pycache__", ".venv", "node_modules", "secrets"} or part.endswith(".egg-info") for part in parts)
        or leaf == ".env" or leaf.startswith(".env.") or leaf == "auth.json"
        or ("storage-state" in leaf and leaf.endswith(".json"))
        or leaf.endswith((".pyc", ".pyo", ".pyd"))
        or path.as_posix() == "deploy/operator.env" or path.as_posix().startswith("deploy/generated/")
    )


def check(kind):
    files = source_files()
    before = {p: digest(ROOT / p) for p in files}
    # Same filesystem permits hardlinks without copying historical evidence.
    # The recipe is unlinked before writing, so candidate/source inodes stay intact.
    with tempfile.TemporaryDirectory(prefix=".docker-context-ci-", dir=ROOT) as temporary:
        temporary = Path(temporary)
        context = temporary / "context"
        context.mkdir()
        for path in files:
            target = context / path
            target.parent.mkdir(parents=True, exist_ok=True)
            os.link(ROOT / path, target)
        recipe = Path("Dockerfile" if kind == "tank" else "deploy/sandbox.Dockerfile")
        (context / recipe).unlink()
        (context / recipe).write_text("FROM scratch\nCOPY . /\n")
        mark = "tank_ci_" + uuid.uuid4().hex
        forbidden = [
            ".env." + mark, "src/.env." + mark, "deploy/.env." + mark,
            "deploy/" + mark + "/auth.json", "deploy/" + mark + "-storage-state.json",
            "deploy/secrets/" + mark, "deploy/generated/" + mark, "deploy/.git/" + mark,
            "deploy/.venv/" + mark, "deploy/node_modules/" + mark,
            "src/tank/__pycache__/" + mark + ".pyc", ".git/" + mark,
            "tests/" + mark, "docs/" + mark, "data/" + mark, "secrets/" + mark,
        ]
        for path in forbidden:
            target = context / path
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as stream:
                stream.write(b"SYNTHETIC CI SENTINEL, NO CREDENTIALS\n")
        export = temporary / "export"
        subprocess.run([
            "docker", "buildx", "build", "--progress=plain", "--file", str(context / recipe),
            "--output", "type=local,dest=" + str(export), str(context),
        ], check=True)
        actual = {p.relative_to(export) for p in export.rglob("*") if p.is_file()}
        assert not any((export / p).exists() for p in forbidden), "Forbidden synthetic input exported"
        if kind == "tank":
            expected = {p for p in files if (p.parts[0] in {"src", "deploy"} or p.as_posix() in {"pyproject.toml", "uv.lock"}) and not ignored_main(p)}
            metadata = {recipe}
        else:
            expected = {Path("deploy/openma_browser/browser-worker.py"), Path("deploy/openma_browser/prepare-font-cache.py")}
            metadata = {recipe, Path(str(recipe) + ".dockerignore")}
        assert expected <= actual, "Required inputs missing: " + repr(expected - actual)
        assert actual <= expected | metadata, "Unexpected inputs exported: " + repr(actual - expected - metadata)
        for path in expected:
            assert digest(export / path) == before[path], "Input changed: " + str(path)
        assert all(digest(ROOT / p) == value for p, value in before.items()), "Original source was changed"
        print(json.dumps({"kind": kind, "files": len(actual), "logical_bytes": sum((export / p).stat().st_size for p in actual), "forbidden_sentinels_excluded": len(forbidden), "original_source_unchanged": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("tank", "sandbox"))
    check(parser.parse_args().kind)
