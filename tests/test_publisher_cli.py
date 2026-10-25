import os
import subprocess
import sys
from pathlib import Path

import pytest

from tank.deployment.runtime_contracts import RuntimeEvidence

ROOT = Path(__file__).resolve().parents[1]


def invoke(*arguments: str, image_sha: str) -> subprocess.CompletedProcess[bytes]:
    environment = os.environ.copy()
    environment["TANK_BUILD_SHA"] = image_sha
    environment["APIFY_API_TOKEN"] = "ambient-token-must-not-select"
    return subprocess.run(
        [sys.executable, "-m", "tank.deployment.cli", *arguments],
        capture_output=True,
        check=False,
        env=environment,
    )


def test_runtime_info_needs_no_host_credential_state_or_worker_artifact(tmp_path: Path) -> None:
    result = invoke(
        "runtime-info",
        "--orchestrator-sha",
        "a" * 40,
        "--selected-release-sha",
        "b" * 40,
        "--runtime-root",
        str(ROOT),
        image_sha="a" * 40,
    )
    assert result.returncode == 0, result.stderr.decode()
    observed = RuntimeEvidence.model_validate_json(result.stdout)
    assert observed.runtime.orchestrator_sha == "a" * 40
    assert observed.runtime.selected_release_sha == "b" * 40
    assert not (tmp_path / "apify-state").exists()
    assert not (tmp_path / ".env").exists()
    assert b"ambient-token" not in result.stdout + result.stderr


@pytest.mark.parametrize("action", ["__init__", "provenance", "unknown"])
def test_cli_refuses_noncommand_methods_without_runtime_or_host_access(action: str) -> None:
    result = invoke(action, image_sha="a" * 40)
    assert result.returncode == 2
    assert b"invalid choice" in result.stderr
    assert b"ambient-token" not in result.stderr


@pytest.mark.parametrize(
    "action",
    [
        "prepare",
        "activate",
        "commit",
        "finish",
        "rollback",
        "rollback_finish",
        "previous_release",
        "recovery_release",
    ],
)
def test_worker_arguments_cannot_select_ambient_paths_or_timeout(action: str) -> None:
    result = invoke(
        action,
        "--orchestrator-sha",
        "a" * 40,
        "--selected-release-sha",
        "b" * 40,
        "--runtime-root",
        str(ROOT),
        image_sha="a" * 40,
    )
    assert result.returncode == 2
    for required in [b"--package", b"--state", b"--credential-file", b"--tank-marker", b"--build-timeout-s"]:
        assert required in result.stderr


def test_forward_worker_refuses_old_transaction_identity_before_host_state(tmp_path: Path) -> None:
    from tank.deployment.cli import Arguments, run
    from tank.deployment.publisher import PublisherStateError

    arguments = Arguments()
    arguments.action = "activate"
    arguments.orchestrator_sha = "a" * 40
    arguments.selected_release_sha = "b" * 40
    arguments.transaction_release_sha = "c" * 40
    arguments.runtime_root = ROOT
    arguments.package = tmp_path / "absent-package"
    arguments.state = tmp_path / "absent-state"
    arguments.credential_file = tmp_path / "absent-env"
    arguments.tank_marker = tmp_path / "absent-marker"
    arguments.build_timeout_s = 600.0
    with pytest.raises(PublisherStateError, match="selected release identity"):
        run(arguments)
    assert not arguments.state.exists()
    assert not arguments.package.exists()
