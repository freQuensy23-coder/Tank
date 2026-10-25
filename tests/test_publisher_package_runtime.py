import hashlib
import os
from pathlib import Path

import pytest

from tank.contracts.json import DuplicateJsonKeyError
from tank.deployment.artifact_contracts import PublisherArtifactRuntime
from tank.deployment.package_runtime import verify_package_runtime
from tank.deployment.runtime import PublisherRuntimeError
from tank.deployment.runtime_contracts import RuntimeEvidence, RuntimeInvocation

ROOT = Path(__file__).resolve().parents[1]


def declared_package(root: Path) -> PublisherArtifactRuntime:
    declared = PublisherArtifactRuntime(
        contract="tank-publisher-runtime-v1",
        image_repository="ghcr.io/example/project/tank",
        runtime=RuntimeEvidence(
            orchestrator_sha="a" * 40,
            selected_release_sha="b" * 40,
            python_version="3.13.13",
            uv_version="0.11.7",
            lock_sha256=hashlib.sha256((ROOT / "uv.lock").read_bytes()).hexdigest(),
            project_sha256=hashlib.sha256((ROOT / "pyproject.toml").read_bytes()).hexdigest(),
        ),
    )
    (root / "publisher-runtime.json").write_text(declared.model_dump_json())
    (root / "publisher-image").write_text(declared.image_reference + "\n")
    return declared


def test_package_control_files_bind_actual_runtime_metadata_and_both_identities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    declared = declared_package(tmp_path)
    monkeypatch.setenv("TANK_BUILD_SHA", declared.runtime.orchestrator_sha)
    observed = verify_package_runtime(
        tmp_path,
        RuntimeInvocation(orchestrator_sha="a" * 40, selected_release_sha="b" * 40),
        ROOT,
    )
    assert observed == declared


@pytest.mark.parametrize("tamper", ["image", "selected", "orchestrator", "lock"])
def test_package_runtime_refuses_inconsistent_control_before_publisher_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tamper: str
) -> None:
    declared_package(tmp_path)
    monkeypatch.setenv("TANK_BUILD_SHA", "a" * 40)
    invocation = RuntimeInvocation(orchestrator_sha="a" * 40, selected_release_sha="b" * 40)
    runtime_root = ROOT
    if tamper == "image":
        (tmp_path / "publisher-image").write_text("ghcr.io/example/project/tank:" + "c" * 40 + "\n")
    elif tamper == "selected":
        invocation = RuntimeInvocation(orchestrator_sha="a" * 40, selected_release_sha="c" * 40)
    elif tamper == "orchestrator":
        monkeypatch.setenv("TANK_BUILD_SHA", "c" * 40)
    elif tamper == "lock":
        runtime_root = tmp_path / "runtime"
        runtime_root.mkdir()
        (runtime_root / "uv.lock").write_bytes(b"changed physical lock")
        (runtime_root / "pyproject.toml").write_bytes((ROOT / "pyproject.toml").read_bytes())
    with pytest.raises(PublisherRuntimeError):
        verify_package_runtime(tmp_path, invocation, runtime_root)
    assert not (tmp_path / "pending.json").exists()
    assert not (tmp_path / "active.json").exists()


def test_duplicate_control_and_nonregular_bootstrap_are_refused(tmp_path: Path) -> None:
    declared = declared_package(tmp_path)
    path = tmp_path / "publisher-runtime.json"
    path.write_text('{"contract":"contradictory",' + declared.model_dump_json()[1:])
    invocation = RuntimeInvocation(orchestrator_sha="a" * 40, selected_release_sha="b" * 40)
    with pytest.raises(DuplicateJsonKeyError):
        verify_package_runtime(tmp_path, invocation, ROOT)
    path.write_text(declared.model_dump_json())
    image = tmp_path / "publisher-image"
    image.unlink()
    os.mkfifo(image, 0o600)
    with pytest.raises(PublisherRuntimeError, match="regular file"):
        verify_package_runtime(tmp_path, invocation, ROOT)
