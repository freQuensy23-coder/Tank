"""Verify the declared publisher runtime before publisher credentials or effects."""

import argparse
import hashlib
import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Annotated

from pydantic import Field

from tank.contracts import DomainModel

type CommitSHA = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
type SHA256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class PublisherRuntimeError(RuntimeError):
    pass


class RuntimeInvocation(DomainModel):
    orchestrator_sha: CommitSHA
    selected_release_sha: CommitSHA


class RuntimeEvidence(DomainModel):
    orchestrator_sha: CommitSHA
    selected_release_sha: CommitSHA
    python_version: str
    uv_version: str
    lock_sha256: SHA256
    project_sha256: SHA256


def artifact_digest(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise PublisherRuntimeError("Publisher runtime metadata must be a regular file")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_runtime(invocation: RuntimeInvocation, root: Path) -> RuntimeEvidence:
    actual_identity = os.environ.get("TANK_BUILD_SHA")
    if actual_identity != invocation.orchestrator_sha:
        raise PublisherRuntimeError("Publisher image does not match the declared orchestrator")
    python_version = platform.python_version()
    if python_version != "3.13.13":
        raise PublisherRuntimeError("Publisher runtime requires Python 3.13.13")
    executable = shutil.which("uv")
    if executable is None:
        raise PublisherRuntimeError("Publisher runtime requires uv 0.11.7")
    result = subprocess.run([executable, "--version"], capture_output=True, check=False)
    if result.returncode != 0:
        raise PublisherRuntimeError("Publisher runtime uv version probe failed")
    fields = result.stdout.decode("utf-8").split()
    if fields[:2] != ["uv", "0.11.7"]:
        raise PublisherRuntimeError("Publisher runtime requires uv 0.11.7")
    return RuntimeEvidence(
        orchestrator_sha=invocation.orchestrator_sha,
        selected_release_sha=invocation.selected_release_sha,
        python_version=python_version,
        uv_version=fields[1],
        lock_sha256=artifact_digest(root / "uv.lock"),
        project_sha256=artifact_digest(root / "pyproject.toml"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orchestrator-sha", required=True)
    parser.add_argument("--selected-release-sha", required=True)
    parser.add_argument("--root", type=Path, required=True)
    arguments = parser.parse_args()
    invocation = RuntimeInvocation(
        orchestrator_sha=arguments.orchestrator_sha,
        selected_release_sha=arguments.selected_release_sha,
    )
    print(verify_runtime(invocation, arguments.root).model_dump_json())


if __name__ == "__main__":
    main()
