import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tank.deployment.runtime import RuntimeEvidence

ROOT = Path(__file__).resolve().parents[1]


def runtime_probe(root: Path, image_sha: str, orchestrator_sha: str) -> subprocess.CompletedProcess[bytes]:
    environment = os.environ.copy()
    environment["TANK_BUILD_SHA"] = image_sha
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "tank.deployment.runtime",
            "--root",
            str(root),
            "--orchestrator-sha",
            orchestrator_sha,
            "--selected-release-sha",
            "b" * 40,
        ],
        capture_output=True,
        check=False,
        env=environment,
    )


def test_runtime_probe_reports_actual_artifacts_without_cloud_state() -> None:
    result = runtime_probe(ROOT, "a" * 40, "a" * 40)
    assert result.returncode == 0, result.stderr.decode()
    evidence = RuntimeEvidence.model_validate_json(result.stdout)
    assert evidence.orchestrator_sha == "a" * 40
    assert evidence.selected_release_sha == "b" * 40
    assert evidence.python_version == "3.13.13"
    assert evidence.uv_version == "0.11.7"
    assert evidence.lock_sha256 == hashlib.sha256((ROOT / "uv.lock").read_bytes()).hexdigest()
    assert evidence.project_sha256 == hashlib.sha256((ROOT / "pyproject.toml").read_bytes()).hexdigest()


@pytest.mark.parametrize("image_sha", ["", "dev", "b" * 40])
def test_runtime_probe_refuses_wrong_image_before_metadata_or_credentials(
    tmp_path: Path, image_sha: str
) -> None:
    result = runtime_probe(tmp_path / "absent", image_sha, "a" * 40)
    assert result.returncode != 0
    assert b"does not match the declared orchestrator" in result.stderr
    assert b"FileNotFoundError" not in result.stderr
    assert not (tmp_path / "absent").exists()


def test_runtime_probe_refuses_symlinked_runtime_metadata(tmp_path: Path) -> None:
    (tmp_path / "uv.lock").symlink_to(ROOT / "uv.lock")
    (tmp_path / "pyproject.toml").write_bytes((ROOT / "pyproject.toml").read_bytes())
    result = runtime_probe(tmp_path, "a" * 40, "a" * 40)
    assert result.returncode != 0
    assert b"must be a regular file" in result.stderr
