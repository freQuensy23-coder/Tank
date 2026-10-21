"""Check the exact BuildKit ignore semantics with actual scratch exports."""

import hashlib
import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def ignored_main(path):
    parts, leaf = path.parts, path.name
    return (
        any(
            part in {".git", "__pycache__", ".venv", "node_modules", "secrets"} or part.endswith(".egg-info")
            for part in parts
        )
        or leaf == ".env"
        or leaf.startswith(".env.")
        or leaf == "auth.json"
        or ("storage-state" in leaf and leaf.endswith(".json"))
        or leaf.endswith((".pyc", ".pyo", ".pyd"))
        or path.as_posix() == "deploy/operator.env"
        or path.as_posix().startswith("deploy/generated/")
    )


def source_files(repository_root):
    files = []
    for directory, directories, names in os.walk(repository_root):
        directories[:] = sorted(
            name
            for name in directories
            if name not in {".git", ".venv", "node_modules", "__pycache__", ".pytest_cache"}
        )
        for name in sorted(names):
            path = Path(directory) / name
            assert not path.is_symlink(), "Context proof requires regular inputs: " + str(path)
            files.append(path.relative_to(repository_root))
    return files


@dataclass
class ContextExport:
    directory: Path
    actual: set[Path]
    expected: set[Path]
    metadata: set[Path]
    forbidden: list[str]
    original_hashes: dict[Path, str]


@pytest.fixture(scope="module")
def context_export(request, repository_root, docker):
    kind = request.config.getoption("--context-kind")
    if kind is None:
        raise pytest.UsageError("Context integration test requires --context-kind tank|sandbox")
    files = source_files(repository_root)
    before = {path: digest(repository_root / path) for path in files}
    # A temporary sibling permits hardlinks. Unlink only the copied recipe before
    # replacing it; all real source inodes remain untouched.
    with tempfile.TemporaryDirectory(prefix=".docker-context-test-", dir=repository_root) as temporary:
        temporary = Path(temporary)
        context = temporary / "context"
        context.mkdir()
        for path in files:
            target = context / path
            target.parent.mkdir(parents=True, exist_ok=True)
            os.link(repository_root / path, target)
        recipe = Path("Dockerfile" if kind == "tank" else "deploy/sandbox.Dockerfile")
        (context / recipe).unlink()
        (context / recipe).write_text("FROM scratch\nCOPY . /\n")
        mark = "tank_test_" + uuid.uuid4().hex
        forbidden = [
            ".env." + mark,
            "src/.env." + mark,
            "deploy/.env." + mark,
            "deploy/" + mark + "/auth.json",
            "deploy/" + mark + "-storage-state.json",
            "deploy/secrets/" + mark,
            "deploy/generated/" + mark,
            "deploy/.git/" + mark,
            "deploy/.venv/" + mark,
            "deploy/node_modules/" + mark,
            "src/tank/__pycache__/" + mark + ".pyc",
            ".git/" + mark,
            "tests/" + mark,
            "docs/" + mark,
            "data/" + mark,
            "secrets/" + mark,
        ]
        for path in forbidden:
            target = context / path
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as stream:
                stream.write(b"SYNTHETIC TEST SENTINEL, NO CREDENTIALS\n")
        exported = temporary / "export"
        docker(
            "buildx",
            "build",
            "--progress=plain",
            "--file",
            context / recipe,
            "--output",
            "type=local,dest=" + str(exported),
            context,
        )
        actual = {path.relative_to(exported) for path in exported.rglob("*") if path.is_file()}
        if kind == "tank":
            expected = {
                path
                for path in files
                if (path.parts[0] in {"src", "deploy"} or path.as_posix() in {"pyproject.toml", "uv.lock"})
                and not ignored_main(path)
            }
            metadata = {recipe}
        else:
            expected = {
                Path("deploy/openma_browser/browser-worker.py"),
                Path("deploy/openma_browser/prepare-font-cache.py"),
            }
            metadata = {recipe, Path(str(recipe) + ".dockerignore")}
        yield ContextExport(exported, actual, expected, metadata, forbidden, before)


def test_context_contains_exact_runtime_inputs(context_export):
    assert context_export.expected <= context_export.actual
    assert context_export.actual <= context_export.expected | context_export.metadata
    for path in context_export.expected:
        assert digest(context_export.directory / path) == context_export.original_hashes[path]


def test_context_excludes_secrets_caches_and_generated_inputs(context_export):
    for path in context_export.forbidden:
        assert not (context_export.directory / path).exists(), path


def test_context_probe_preserves_original_source(context_export, repository_root):
    for path, expected in context_export.original_hashes.items():
        assert digest(repository_root / path) == expected, path
