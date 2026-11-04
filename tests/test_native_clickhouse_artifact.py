import hashlib
import re
import runpy
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from truth_native_test_safety import NATIVE_URL_REFUSAL, require_loopback_native_url

ROOT = Path(__file__).resolve().parents[1]
HELPER = runpy.run_path(str(ROOT / "scripts/ci/setup_native_clickhouse.py"))
IMAGE = (
    "clickhouse/clickhouse-server:25.8.33.6@"
    "sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77"
)
HASHES = {
    "amd64": "6a96266c0f00897225f9bb2ab0f965111298da75d7ec1b67465c6c9d5cf5ae81",
    "arm64": "6a23992b3035628dd1fc0fc981453db82b2f35fcf0f5c588293ce7c4918841fd",
}


def test_shipping_binary_and_ci_service_use_the_same_immutable_artifact():
    dockerfile = (ROOT / "Dockerfile").read_text()
    workflow = yaml.safe_load((ROOT / ".github/workflows/test-suite.yml").read_text())
    integration = workflow["jobs"]["python-integration"]
    assert HELPER["IMAGE"] == IMAGE
    assert HELPER["BINARY_SHA256"] == HASHES
    assert f"FROM {IMAGE} AS native-clickhouse" in dockerfile
    assert integration["services"]["clickhouse"]["image"] == IMAGE
    copy = "COPY --from=native-clickhouse /usr/bin/clickhouse /opt/tank/venv/bin/clickhouse"
    assert dockerfile.index("COPY --from=dependencies /opt/tank/venv /opt/tank/venv") < dockerfile.index(copy)
    assert dockerfile.index(copy) < dockerfile.index("USER tank")
    for digest in HASHES.values():
        assert digest in dockerfile
    assert dockerfile.endswith('USER tank\nENTRYPOINT ["/usr/bin/tini", "--"]\nCMD ["tank", "serve"]\n')
    runs = [step.get("run", "") for step in integration["steps"]]
    setup = (
        "uv run --frozen --extra dev python scripts/ci/setup_native_clickhouse.py "
        '"${{ job.services.clickhouse.id }}"'
    )
    assert runs.index("uv sync --python 3.13.13 --frozen --extra dev") < runs.index(setup)
    assert runs.index(setup) < next(i for i, run in enumerate(runs) if "pytest " in run)
    assert not any(
        "setup_native_clickhouse.py" in step.get("run", "")
        for step in workflow["jobs"]["python-unit"]["steps"]
    )


def test_destination_is_the_unresolved_project_interpreter_sibling(monkeypatch):
    monkeypatch.setattr(sys, "prefix", "/project/.venv")
    monkeypatch.setattr(sys, "base_prefix", "/python")
    monkeypatch.setattr(sys, "executable", "/project/.venv/bin/python")
    assert HELPER["destination"]() == Path("/project/.venv/bin/clickhouse")
    monkeypatch.setattr(sys, "prefix", sys.base_prefix)
    with pytest.raises(RuntimeError, match="virtual environment"):
        HELPER["destination"]()


def test_unknown_architecture_is_refused(monkeypatch):
    monkeypatch.setattr(HELPER["architecture"].__globals__["platform"], "machine", lambda: "riscv64")
    with pytest.raises(RuntimeError, match="architecture"):
        HELPER["architecture"]()


SERVICE = "a" * 64


@pytest.fixture
def staging(monkeypatch, tmp_path):
    artifact_directory = tmp_path / "artifact"
    artifact_directory.mkdir()
    target = artifact_directory / "clickhouse"
    target.write_bytes(b"previous installation")
    calls = []

    def output(command, **kwargs):
        calls.append(command)
        assert command == ["docker", "inspect", "--format", "{{.Config.Image}}", SERVICE]
        return IMAGE + "\n"

    def run(command, **kwargs):
        calls.append(command)
        assert command[:3] == ["docker", "cp", f"{SERVICE}:/usr/bin/clickhouse"]
        assert Path(command[3]).parent == target.parent
        assert kwargs == {"check": True}
        Path(command[3]).write_bytes(b"unqualified executable")

    monkeypatch.setitem(HELPER["main"].__globals__, "destination", lambda: target)
    monkeypatch.setitem(HELPER["main"].__globals__, "architecture", lambda: "amd64")
    monkeypatch.setattr(subprocess, "check_output", output)
    monkeypatch.setattr(subprocess, "run", run)
    return target, calls


def test_unqualified_binary_is_never_executed_and_service_is_untouched(staging):
    target, calls = staging
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        HELPER["main"](SERVICE)
    assert target.read_bytes() == b"previous installation"
    assert list(target.parent.iterdir()) == [target]
    assert [call[1] for call in calls] == ["inspect", "cp"]


@pytest.mark.parametrize(
    "container", ["", "a" * 63, "A" * 64, "a" * 64 + "\n", "tcp://remote:2375", "--all"]
)
def test_invalid_service_descriptor_refuses_before_docker(staging, container):
    target, calls = staging
    with pytest.raises(RuntimeError, match="exact CI service container ID"):
        HELPER["main"](container)
    assert calls == []
    assert target.read_bytes() == b"previous installation"
    assert list(target.parent.iterdir()) == [target]


@pytest.mark.parametrize(
    "image",
    [
        "clickhouse/clickhouse-server:25.8",
        IMAGE.replace("25.8.33.6", "25.8.33.60"),
        "other@sha256:" + "0" * 64,
    ],
)
def test_mismatched_service_image_preserves_artifact(monkeypatch, staging, image):
    target, calls = staging

    def output(command, **kwargs):
        calls.append(command)
        assert command == ["docker", "inspect", "--format", "{{.Config.Image}}", SERVICE]
        return image + "\n"

    monkeypatch.setattr(subprocess, "check_output", output)
    with pytest.raises(RuntimeError, match="image mismatch"):
        HELPER["main"](SERVICE)
    assert [call[1] for call in calls] == ["inspect"]
    assert target.read_bytes() == b"previous installation"
    assert list(target.parent.iterdir()) == [target]


@pytest.mark.parametrize("failure", [KeyboardInterrupt(), subprocess.CalledProcessError(1, "inspect")])
def test_metadata_interruption_never_acquires_a_container(monkeypatch, staging, failure):
    target, calls = staging

    def output(command, **kwargs):
        calls.append(command)
        assert command == ["docker", "inspect", "--format", "{{.Config.Image}}", SERVICE]
        raise failure

    monkeypatch.setattr(subprocess, "check_output", output)
    with pytest.raises(type(failure)) as caught:
        HELPER["main"](SERVICE)
    assert caught.value is failure
    assert [call[1] for call in calls] == ["inspect"]
    assert target.read_bytes() == b"previous installation"
    assert list(target.parent.iterdir()) == [target]


@pytest.mark.parametrize("failure", [KeyboardInterrupt(), subprocess.CalledProcessError(1, "cp")])
def test_copy_failure_cleans_only_owned_candidate(monkeypatch, staging, failure):
    target, calls = staging

    def run(command, **kwargs):
        calls.append(command)
        assert command[:3] == ["docker", "cp", f"{SERVICE}:/usr/bin/clickhouse"]
        Path(command[3]).write_bytes(b"partial copy")
        raise failure

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(type(failure)) as caught:
        HELPER["main"](SERVICE)
    assert caught.value is failure
    assert target.read_bytes() == b"previous installation"
    assert list(target.parent.iterdir()) == [target]
    assert [call[1] for call in calls] == ["inspect", "cp"]


@pytest.mark.parametrize("stage", ["allocation", "chmod", "validation"])
@pytest.mark.parametrize("failure", [OSError("preparation failed"), KeyboardInterrupt()])
def test_prepublication_failure_retains_prior_artifact(monkeypatch, staging, stage, failure):
    target, calls = staging

    def fail(*args, **kwargs):
        raise failure

    if stage == "allocation":
        monkeypatch.setattr(HELPER["main"].__globals__["tempfile"], "NamedTemporaryFile", fail)
    elif stage == "chmod":
        monkeypatch.setattr(Path, "chmod", fail)
    else:
        monkeypatch.setitem(HELPER["main"].__globals__, "verify_binary", fail)
    with pytest.raises(type(failure)) as caught:
        HELPER["main"](SERVICE)
    assert caught.value is failure
    assert target.read_bytes() == b"previous installation"
    assert list(target.parent.iterdir()) == [target]
    expected = ["inspect"] if stage == "allocation" else ["inspect", "cp"]
    assert [call[1] for call in calls] == expected


@pytest.mark.parametrize("cleanup_failure", [OSError("cleanup failed"), KeyboardInterrupt()])
def test_cleanup_failure_preserves_primary_and_previous_artifact(monkeypatch, staging, cleanup_failure):
    target, calls = staging
    removed = []

    def unlink(path, **kwargs):
        removed.append(path)
        raise cleanup_failure

    monkeypatch.setattr(Path, "unlink", unlink)
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        HELPER["main"](SERVICE)
    assert removed == [Path(calls[1][3])]
    assert removed[0] != target
    assert target.read_bytes() == b"previous installation"
    assert [call[1] for call in calls] == ["inspect", "cp"]


def test_candidate_close_failure_prevents_publication(monkeypatch, staging):
    target, calls = staging
    real_temporary = HELPER["main"].__globals__["tempfile"].NamedTemporaryFile
    closed = []

    def temporary(**kwargs):
        owned = real_temporary(**kwargs)
        owned.close()

        def close():
            closed.append(owned.name)
            raise OSError("candidate close failed")

        return SimpleNamespace(name=owned.name, close=close)

    monkeypatch.setattr(HELPER["main"].__globals__["tempfile"], "NamedTemporaryFile", temporary)
    with pytest.raises(OSError, match="candidate close failed"):
        HELPER["main"](SERVICE)
    assert len(closed) == 2
    assert target.read_bytes() == b"previous installation"
    assert list(target.parent.iterdir()) == [target]
    assert [call[1] for call in calls] == ["inspect"]


def test_version_failure_preserves_prior_artifact(monkeypatch, staging):
    target, calls = staging
    digest = hashlib.sha256(b"unqualified executable").hexdigest()
    monkeypatch.setitem(HELPER["BINARY_SHA256"], "amd64", digest)
    original_output = subprocess.check_output

    def output(command, **kwargs):
        if command[0] == "docker":
            return original_output(command, **kwargs)
        calls.append(command)
        assert command == [calls[1][3], "--version"]
        return "ClickHouse local version 25.8.33.60"

    monkeypatch.setattr(subprocess, "check_output", output)
    with pytest.raises(RuntimeError, match="version mismatch"):
        HELPER["main"](SERVICE)
    assert target.read_bytes() == b"previous installation"
    assert list(target.parent.iterdir()) == [target]
    assert [call[1] for call in calls] == ["inspect", "cp", "--version"]


def test_version_mismatch_is_refused_after_checksum_validation(monkeypatch, tmp_path):
    binary = tmp_path / "clickhouse"
    binary.write_bytes(b"version validation fixture")
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    monkeypatch.setitem(HELPER["BINARY_SHA256"], "amd64", digest)
    monkeypatch.setattr(
        subprocess, "check_output", lambda *args, **kwargs: "ClickHouse local version 25.8.33.60"
    )
    with pytest.raises(RuntimeError, match="version mismatch"):
        HELPER["verify_binary"](binary, "amd64")


async def test_artifact_qualification_refuses_remote_before_any_probe(monkeypatch):
    calls = []

    def forbidden(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("probe or client constructed before loopback refusal")

    monkeypatch.setenv("CLICKHOUSE_TEST_URL", "https://not-owned.example:8123")
    monkeypatch.setitem(
        sys.modules, "tank.analysis.truth_native_local", SimpleNamespace(ReplayNativeRows=forbidden)
    )
    monkeypatch.setitem(sys.modules, "tank.clickhouse_market", SimpleNamespace(ClickHouse=forbidden))
    monkeypatch.setitem(HELPER, "destination", forbidden)
    monkeypatch.setitem(HELPER, "verify_binary", forbidden)
    monkeypatch.setattr(subprocess, "check_output", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    with pytest.raises(ValueError, match=r"\A" + re.escape(NATIVE_URL_REFUSAL) + r"\Z"):
        await test_installed_native_binary_and_service_match_the_pinned_release_and_source()
    assert calls == []


@pytest.mark.integration
async def test_installed_native_binary_and_service_match_the_pinned_release_and_source():
    url = require_loopback_native_url()

    from tank.analysis.truth_native_local import ReplayNativeRows
    from tank.clickhouse_market import ClickHouse

    binary = HELPER["destination"]()
    assert binary.is_file(), "CI must stage the pinned native executable before integration"
    HELPER["verify_binary"](binary, HELPER["architecture"]())
    query = (
        "SELECT version() AS version, toUInt32(1) AS scalar, "
        "(SELECT value FROM system.build_options WHERE name = 'VERSION_GITHASH') AS source"
    )
    expected = {"version": "25.8.33.6", "scalar": 1, "source": "65eaf18b4a939f3e02aad50b7fbb2711b09495a0"}
    provider = ReplayNativeRows()
    try:
        assert await provider.rows(query) == [expected]
    finally:
        await provider.close()
    client = ClickHouse(url)
    try:
        assert await client.rows(query) == [expected]
    finally:
        await client.close()
