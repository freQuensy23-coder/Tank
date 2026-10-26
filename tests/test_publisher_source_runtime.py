import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from tank.deployment.runtime import PublisherRuntimeError
from tank.deployment.runtime_contracts import RuntimeInvocation
from tank.deployment.source_runtime import verify_source_archive

ROOT = Path(__file__).resolve().parents[1]


def git(repository: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", *arguments], cwd=repository, text=True).strip()


def commit(repository: Path) -> str:
    git(repository, "add", ".")
    git(
        repository,
        "-c",
        "user.name=Codex",
        "-c",
        "user.email=codex@openai.com",
        "commit",
        "-m",
        "Authored runtime source",
    )
    return git(repository, "rev-parse", "HEAD")


@dataclass(frozen=True)
class ArchiveFixture:
    repository: Path
    archive: Path
    invocation: RuntimeInvocation


@pytest.fixture
def source_archive(tmp_path: Path) -> ArchiveFixture:
    repository = tmp_path / "repository"
    repository.mkdir()
    git(repository, "init", "-q")
    for name, body in {
        "src/tank/deployment/cli.py": b'raise SystemExit("source evidence must not execute")\n',
        "scripts/deploy/publisher_worker.sh": b"#!/bin/sh\nexit 1\n",
        "pyproject.toml": (ROOT / "pyproject.toml").read_bytes(),
        "uv.lock": (ROOT / "uv.lock").read_bytes(),
    }.items():
        path = repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    orchestrator = commit(repository)
    archive = tmp_path / "archive"
    archive.mkdir()
    for name in ("src", "scripts"):
        shutil.copytree(repository / name, archive / name)
    for name in ("pyproject.toml", "uv.lock"):
        shutil.copyfile(repository / name, archive / name)
    (repository / "src/tank/deployment/cli.py").write_bytes(b'raise SystemExit("selected old release")\n')
    selected = commit(repository)
    return ArchiveFixture(
        repository,
        archive,
        RuntimeInvocation(orchestrator_sha=orchestrator, selected_release_sha=selected),
    )


def test_source_archive_uses_verified_orchestrator_blobs_after_selected_checkout(
    source_archive: ArchiveFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TANK_BUILD_SHA", "not-an-image-claim")
    fixture = source_archive
    observed = verify_source_archive(fixture.repository, fixture.archive, fixture.invocation)
    assert observed.origin == "SOURCE_ARCHIVE"
    assert observed.runtime.orchestrator_sha != git(fixture.repository, "rev-parse", "HEAD")
    assert observed.runtime.selected_release_sha == git(fixture.repository, "rev-parse", "HEAD")
    assert observed.runtime.python_version == "3.13.13"
    assert observed.runtime.uv_version == "0.11.7"


@pytest.mark.parametrize("tamper", ["changed", "extra", "missing", "symlink", "ancestor"])
def test_source_archive_refuses_mismatched_program_bytes_or_file_set(
    source_archive: ArchiveFixture, tamper: str
) -> None:
    fixture = source_archive
    path = fixture.archive / "src/tank/deployment/cli.py"
    if tamper == "changed":
        path.write_bytes(b"changed source")
    elif tamper == "extra":
        (path.parent / "injected.py").write_bytes(b"extra executable")
    elif tamper == "missing":
        path.unlink()
    elif tamper == "symlink":
        path.unlink()
        path.symlink_to(fixture.repository / "src/tank/deployment/cli.py")
    elif tamper == "ancestor":
        shutil.rmtree(fixture.archive / "scripts")
        (fixture.archive / "scripts").symlink_to(fixture.repository / "scripts")
    with pytest.raises(PublisherRuntimeError):
        verify_source_archive(fixture.repository, fixture.archive, fixture.invocation)


def test_source_archive_refuses_unknown_or_selected_identity_as_orchestrator(
    source_archive: ArchiveFixture,
) -> None:
    fixture = source_archive
    for claimed in ("f" * 40, fixture.invocation.selected_release_sha):
        with pytest.raises(PublisherRuntimeError):
            verify_source_archive(
                fixture.repository,
                fixture.archive,
                RuntimeInvocation(
                    orchestrator_sha=claimed, selected_release_sha=fixture.invocation.selected_release_sha
                ),
            )
