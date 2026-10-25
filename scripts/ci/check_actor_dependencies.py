"""Check the versioned actor requirements against its frozen root-lock group."""

import argparse
import hashlib
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class ActorDependencyError(RuntimeError):
    pass


type Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class ActorDependencyEvidence(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    lock_sha256: Digest
    requirements_sha256: Digest


class CheckRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    root: Path


class ActorDependencyGroup(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    x_reader: list[str] = Field(alias="x-reader")


class ProjectDependencyGroups(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    dependency_groups: ActorDependencyGroup = Field(alias="dependency-groups")


def require_direct_actor_dependencies(root: Path) -> None:
    metadata = ProjectDependencyGroups.model_validate(tomllib.loads((root / "pyproject.toml").read_text()))
    declared: set[str] = set()
    for requirement in metadata.dependency_groups.x_reader:
        match = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)", requirement)
        if match is None:
            raise ActorDependencyError("Actor direct dependencies require exact version pins")
        name = re.sub(r"[-_.]+", "-", match[1]).lower()
        if name in declared:
            raise ActorDependencyError("Actor direct dependency is declared more than once")
        declared.add(name)
    if not {"apify", "beautifulsoup4", "httpx", "pydantic"} <= declared:
        raise ActorDependencyError("Actor direct runtime dependency declaration is missing")


def locate_uv() -> Path:
    executable = shutil.which("uv")
    if executable is None:
        raise ActorDependencyError("The pinned uv executable is required")
    return Path(executable)


def tool_output(arguments: list[str], root: Path) -> bytes:
    result = subprocess.run(arguments, cwd=root, capture_output=True, check=False)
    if result.returncode != 0:
        raise ActorDependencyError("Frozen actor dependency tool failed with exit " + str(result.returncode))
    return result.stdout


def frozen_actor_requirements(root: Path, uv: Path) -> bytes:
    version = tool_output([str(uv), "--version"], root).decode().split()
    if version[:2] != ["uv", "0.11.7"]:
        raise ActorDependencyError("Actor dependency export requires uv 0.11.7")
    require_direct_actor_dependencies(root)
    tool_output(
        [str(uv), "lock", "--offline", "--check", "--python", sys.executable],
        root,
    )
    exported = tool_output(
        [
            str(uv),
            "export",
            "--offline",
            "--frozen",
            "--only-group",
            "x-reader",
            "--no-emit-project",
            "--no-header",
            "--no-annotate",
        ],
        root,
    )
    if b"--hash=sha256:" not in exported:
        raise ActorDependencyError("Actor dependency export has no locked artifact hashes")
    return exported


def check_actor_dependencies(root: Path, uv: Path) -> ActorDependencyEvidence:
    expected = frozen_actor_requirements(root, uv)
    target = root / "apify" / "x-reader" / "requirements.txt"
    actual = target.read_bytes()
    if actual != expected:
        raise ActorDependencyError("Versioned actor requirements differ from the frozen actor-only export")
    return ActorDependencyEvidence(
        lock_sha256=hashlib.sha256((root / "uv.lock").read_bytes()).hexdigest(),
        requirements_sha256=hashlib.sha256(actual).hexdigest(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    request = CheckRequest.model_validate(vars(parser.parse_args()))
    print(check_actor_dependencies(request.root, locate_uv()).model_dump_json())


if __name__ == "__main__":
    main()
