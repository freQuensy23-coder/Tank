import io
import os
import subprocess
import sys
import tarfile
from pathlib import Path

from tank.deployment.artifact_contracts import PublisherArtifactRuntime
from tank.deployment.contracts import SourceRelease
from tank.deployment.runtime import artifact_digest

ROOT = Path(__file__).resolve().parents[1]


def test_actual_git_archive_executes_packaging_without_host_image_or_credential_claim(
    tmp_path: Path,
) -> None:
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    archive_bytes = subprocess.check_output(
        ["git", "archive", sha, "src", "scripts/deploy", "pyproject.toml", "uv.lock"], cwd=ROOT
    )
    archive = tmp_path / "orchestrator"
    archive.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archive_bytes)) as bundle:
        bundle.extractall(archive, filter="data")
    destination = tmp_path / "selected"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(archive / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["TANK_BUILD_SHA"] = "not-an-executed-image"
    environment["APIFY_API_TOKEN"] = "ambient-must-not-select"
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "tank.deployment.cli",
            "package",
            "--orchestrator-sha",
            sha,
            "--selected-release-sha",
            sha,
            "--runtime-root",
            str(archive),
            "--repository",
            str(ROOT),
            "--destination",
            str(destination),
            "--image-repository",
            "ghcr.io/example/project/tank",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        check=False,
    )
    assert process.returncode == 0, process.stderr.decode()
    release = SourceRelease.model_validate_json(process.stdout)
    assert release.release_sha == sha
    assert (destination / "worker/actor_contracts.py").read_bytes() == (
        ROOT / "apify/x-reader/actor_contracts.py"
    ).read_bytes()
    declared = PublisherArtifactRuntime.model_validate_json(
        (destination / "publisher-runtime.json").read_bytes()
    )
    assert declared.runtime.orchestrator_sha == sha
    assert declared.runtime.selected_release_sha == sha
    assert declared.runtime.lock_sha256 == artifact_digest(archive / "uv.lock")
    assert declared.runtime.project_sha256 == artifact_digest(archive / "pyproject.toml")
    assert (destination / "publisher-image").read_text() == declared.image_reference + "\n"
    assert not (tmp_path / "apify-state").exists()
    assert b"ambient-must-not-select" not in process.stdout + process.stderr
