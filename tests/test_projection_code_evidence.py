import subprocess
from pathlib import Path

import pytest

from tank.fixture_reference_code import (
    ArchivedProjectionCodes,
    CapturedProjectionCodeInventory,
    VerifiedGitProjectionCode,
)
from tank.fixture_reference_sources import RetainedInputOrigin
from tank.util import sha256


def git(repository: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", "-C", str(repository), *arguments], text=True).strip()


def evidence(tmp_path: Path) -> tuple[ArchivedProjectionCodes, Path, Path, str]:
    repository = tmp_path / "repository"
    repository.mkdir()
    git(repository, "init", "--quiet")
    source = repository / "src/tank/fixture_reference.py"
    source.parent.mkdir(parents=True)
    body = b'raise RuntimeError("Archived code must never execute")\n'
    source.write_bytes(body)
    git(repository, "add", "src/tank/fixture_reference.py")
    git(
        repository,
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "commit.gpgsign=false",
        "-c",
        "user.name=Codex",
        "-c",
        "user.email=codex@openai.com",
        "commit",
        "--quiet",
        "-m",
        "Synthetic fixed-path code evidence",
    )
    commit = git(repository, "rev-parse", "HEAD")
    blob = git(repository, "rev-parse", "HEAD:src/tank/fixture_reference.py")
    archives = repository / "evidence"
    archives.mkdir()
    archive = archives / "module.source"
    archive.write_bytes(body)
    inventory = CapturedProjectionCodeInventory(
        fixed_source_path="src/tank/fixture_reference.py",
        verified_input_packets=1,
        excluded_fixture="F-02_MANIFEST_MISMATCH",
        archived_code_executed=False,
        entries=[
            VerifiedGitProjectionCode(
                projection_module_sha256=sha256(body),
                source_path="src/tank/fixture_reference.py",
                status="VERIFIED_GIT_BYTES",
                git_commit=commit,
                git_blob_sha1=blob,
                archive_path=Path("evidence/module.source"),
                source_bytes=len(body),
                origin_claim="BYTES_PRESENT_AT_GIT_PATH_NOT_DEPLOYMENT",
                origins=[
                    RetainedInputOrigin(
                        fixture_id="F-01",
                        packet_path="synthetic/input.json.zst",
                        compressed_sha256="a" * 64,
                        field_path="$.projection_module_sha256",
                    )
                ],
            )
        ],
    )
    path = repository / "code.json"
    path.write_text(inventory.model_dump_json())
    return (
        ArchivedProjectionCodes(path, repository_root=repository, archives_root=archives),
        archive,
        path,
        sha256(body),
    )


def test_actual_fixed_path_git_bytes_are_verified_without_execution(tmp_path: Path) -> None:
    reader, _, _, checksum = evidence(tmp_path)
    reader.verify(checksum)
    with pytest.raises(ValueError, match="no verified fixed-path Git artifact"):
        reader.verify("0" * 64)


def test_archived_code_is_rechecked_after_construction(tmp_path: Path) -> None:
    reader, archive, _, checksum = evidence(tmp_path)
    archive.write_bytes(b"changed")
    with pytest.raises(ValueError, match="differs from its recorded bytes"):
        reader.verify(checksum)


def test_valid_archive_digest_cannot_claim_a_nonexistent_git_origin(tmp_path: Path) -> None:
    _, _, path, _ = evidence(tmp_path)
    inventory = CapturedProjectionCodeInventory.model_validate_json(path.read_bytes())
    original = inventory.entries[0]
    assert isinstance(original, VerifiedGitProjectionCode)
    forged = VerifiedGitProjectionCode(
        projection_module_sha256=original.projection_module_sha256,
        source_path=original.source_path,
        status=original.status,
        git_commit="0" * 40,
        git_blob_sha1=original.git_blob_sha1,
        archive_path=original.archive_path,
        source_bytes=original.source_bytes,
        origin_claim=original.origin_claim,
        origins=original.origins,
    )
    altered = CapturedProjectionCodeInventory(
        fixed_source_path=inventory.fixed_source_path,
        verified_input_packets=inventory.verified_input_packets,
        excluded_fixture=inventory.excluded_fixture,
        archived_code_executed=False,
        entries=[forged],
    )
    path.write_text(altered.model_dump_json())
    with pytest.raises(ValueError, match="Git origin is unavailable"):
        ArchivedProjectionCodes(path, repository_root=path.parent, archives_root=path.parent / "evidence")
