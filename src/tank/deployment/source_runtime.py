"""Verify a main-orchestrator source archive independently of selected release code."""

import stat
import subprocess
from pathlib import Path
from typing import Annotated

from pydantic import Field

from tank.contracts import DomainModel
from tank.deployment.api_contracts import SourcePath
from tank.deployment.package_runtime import read_control_file
from tank.deployment.runtime import PublisherRuntimeError, observe_runtime
from tank.deployment.runtime_contracts import RuntimeInvocation, SourceArchiveEvidence

SOURCE_ROOTS = ("src", "scripts/deploy", "pyproject.toml", "uv.lock")


type GitObjectID = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]


class GitSource(DomainModel):
    path: SourcePath
    object_id: GitObjectID


def git_output(repository: Path, *arguments: str) -> bytes:
    process = subprocess.run(["git", *arguments], cwd=repository, capture_output=True, check=False)
    if process.returncode != 0:
        raise PublisherRuntimeError("Publisher source identity cannot be verified")
    return process.stdout


def archive_sources(repository: Path, invocation: RuntimeInvocation) -> tuple[GitSource, ...]:
    resolved = git_output(repository, "rev-parse", "--verify", invocation.orchestrator_sha + "^{commit}")
    if resolved.decode("ascii").strip() != invocation.orchestrator_sha:
        raise PublisherRuntimeError("Publisher source commit identity mismatch")
    listing = git_output(repository, "ls-tree", "-r", "-z", invocation.orchestrator_sha, "--", *SOURCE_ROOTS)
    sources: list[GitSource] = []
    for record in listing.split(b"\0"):
        if not record:
            continue
        metadata, raw_path = record.split(b"\t", 1)
        mode, kind, object_id = metadata.split(b" ")
        path = raw_path.decode("utf-8")
        if mode not in {b"100644", b"100755"} or kind != b"blob":
            raise PublisherRuntimeError("Publisher source archive contains a nonregular entry")
        sources.append(GitSource(path=path, object_id=object_id.decode("ascii")))
    names = {source.path for source in sources}
    if not {"pyproject.toml", "uv.lock", "src/tank/deployment/cli.py"} <= names:
        raise PublisherRuntimeError("Publisher source archive is incomplete")
    return tuple(sources)


def verify_source_archive(
    repository: Path, archive: Path, invocation: RuntimeInvocation
) -> SourceArchiveEvidence:
    if archive.is_symlink() or not archive.is_dir():
        raise PublisherRuntimeError("Publisher source archive root must be a regular directory")
    sources = archive_sources(repository, invocation)
    expected = {source.path for source in sources}
    observed: set[str] = set()
    for root_name in SOURCE_ROOTS:
        root = archive
        for part in Path(root_name).parts:
            root = root / part
            if root.is_symlink():
                raise PublisherRuntimeError("Publisher source archive contains a symbolic entry")
        paths = root.rglob("*") if root.is_dir() else (root,)
        for path in paths:
            if path.is_symlink():
                raise PublisherRuntimeError("Publisher source archive contains a symbolic entry")
            if path.is_dir():
                continue
            if not stat.S_ISREG(path.stat().st_mode):
                raise PublisherRuntimeError("Publisher source archive contains a nonregular entry")
            observed.add(path.relative_to(archive).as_posix())
    if observed != expected:
        raise PublisherRuntimeError("Publisher source archive file set mismatch")
    for source in sources:
        actual = read_control_file(archive / source.path)
        if actual != git_output(repository, "cat-file", "blob", source.object_id):
            raise PublisherRuntimeError("Publisher source archive bytes mismatch")
    return SourceArchiveEvidence(origin="SOURCE_ARCHIVE", runtime=observe_runtime(invocation, archive))
