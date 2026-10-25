"""Verify fixed-path code provenance without loading or executing archived code."""

import hashlib
import subprocess
from pathlib import Path
from typing import Annotated, Literal, Protocol

from pydantic import Field

from tank.contracts import DomainModel
from tank.fixture_reference_sources import RetainedInputOrigin, SourceDigest
from tank.util import sha256

type GitObjectId = Annotated[str, Field(pattern=r"^[a-f0-9]{40}$")]
type ProjectionSourcePath = Literal["src/tank/fixture_reference.py"]


class ProjectionCodeEvidence(Protocol):
    def verify(self, expected_sha256: str) -> None: ...


class CurrentProjectionCode:
    def __init__(self, source_path: Path) -> None:
        self.source_path = source_path

    def verify(self, expected_sha256: str) -> None:
        if sha256(self.source_path.read_bytes()) != expected_sha256:
            raise ValueError("Fixture original projection module bytes differ")


CURRENT_PROJECTION_CODE = CurrentProjectionCode(Path(__file__).with_name("fixture_reference.py"))


class VerifiedGitProjectionCode(DomainModel):
    projection_module_sha256: SourceDigest
    source_path: ProjectionSourcePath
    status: Literal["VERIFIED_GIT_BYTES"]
    git_commit: GitObjectId
    git_blob_sha1: GitObjectId
    archive_path: Path
    source_bytes: int = Field(gt=0)
    origin_claim: Literal["BYTES_PRESENT_AT_GIT_PATH_NOT_DEPLOYMENT"]
    origins: list[RetainedInputOrigin] = Field(min_length=1)


class UnresolvedProjectionCode(DomainModel):
    projection_module_sha256: SourceDigest
    source_path: ProjectionSourcePath
    status: Literal["UNRESOLVED"]
    origin_claim: Literal["NO_MATCHING_FIXED_PATH_GIT_BLOB_LOCATED"]
    origins: list[RetainedInputOrigin] = Field(min_length=1)


class CapturedProjectionCodeInventory(DomainModel):
    fixed_source_path: ProjectionSourcePath
    verified_input_packets: int = Field(gt=0)
    excluded_fixture: Literal["F-02_MANIFEST_MISMATCH"]
    archived_code_executed: Literal[False]
    entries: list[VerifiedGitProjectionCode | UnresolvedProjectionCode] = Field(min_length=1)


class ArchivedProjectionCodes:
    def __init__(self, inventory_path: Path, *, repository_root: Path, archives_root: Path) -> None:
        inventory = CapturedProjectionCodeInventory.model_validate_json(inventory_path.read_bytes())
        keys = [entry.projection_module_sha256 for entry in inventory.entries]
        if len(keys) != len(set(keys)):
            raise ValueError("Captured code inventory repeats a module digest")
        self._repository = repository_root.resolve(strict=True)
        self._root = archives_root.resolve(strict=True)
        self._entries = {
            entry.projection_module_sha256: entry
            for entry in inventory.entries
            if isinstance(entry, VerifiedGitProjectionCode)
        }
        for checksum in self._entries:
            self.verify(checksum)

    def verify(self, expected_sha256: str) -> None:
        try:
            entry = self._entries[expected_sha256]
        except KeyError:
            raise ValueError(
                "Captured projection code hash has no verified fixed-path Git artifact"
            ) from None
        if entry.archive_path.is_absolute():
            raise ValueError("Captured code artifact path must be relative to its declared repository")
        path = (self._repository / entry.archive_path).resolve(strict=True)
        if not path.is_relative_to(self._root):
            raise ValueError("Captured code artifact escapes its declared evidence root")
        data = path.read_bytes()
        if len(data) != entry.source_bytes or sha256(data) != expected_sha256:
            raise ValueError("Archived projection code differs from its recorded bytes")
        git_blob = b"blob " + str(len(data)).encode("ascii") + b"\0" + data
        if hashlib.sha1(git_blob, usedforsecurity=False).hexdigest() != entry.git_blob_sha1:
            raise ValueError("Archived projection code differs from its recorded Git object")
        bound_blob = (
            self._git("rev-parse", entry.git_commit + ":" + entry.source_path).decode("ascii").strip()
        )
        if bound_blob != entry.git_blob_sha1:
            raise ValueError("Captured projection code Git commit does not bind its fixed source path")
        if self._git("cat-file", "blob", entry.git_blob_sha1) != data:
            raise ValueError("Captured projection code differs from its actual Git history bytes")

    def _git(self, *arguments: str) -> bytes:
        result = subprocess.run(
            ["git", "-C", str(self._repository), *arguments], capture_output=True, check=False
        )
        if result.returncode != 0:
            raise ValueError("Captured projection code Git origin is unavailable")
        return result.stdout
