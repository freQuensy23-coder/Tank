import os
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

import pytest
from publisher_docker_stand import ContainerLabels, ContainerState, DockerState, state
from publisher_stand_protocol import read_endpoint
from publisher_stand_support import OwnedBackend

ROOT = Path(__file__).resolve().parents[1]


pytestmark = [pytest.mark.integration, pytest.mark.process, pytest.mark.timeout(10)]


@dataclass(frozen=True)
class WorkerStand:
    root: Path
    host: Path
    environment: dict[str, str]
    image: str
    backend: OwnedBackend

    def launch(self) -> subprocess.Popen[bytes]:
        return self.backend.launch_frontend(
            [
                "bash",
                str(ROOT / "scripts/deploy/publisher_worker.sh"),
                self.image,
                "a" * 40,
                "b" * 40,
                "b" * 40,
                "prepare",
                str(self.host),
            ],
            self.environment,
        )


def worker_stand(
    tmp_path: Path, fault: Literal["NORMAL", "BLOCK", "DRAIN_FAIL"], request: pytest.FixtureRequest
) -> WorkerStand:
    host = tmp_path / "host"
    host.mkdir()
    directory = tmp_path / "stand"
    directory.mkdir()
    (directory / "state.json").write_text(
        DockerState(fault=fault, containers={}, calls=[], drain_attempts=0).model_dump_json()
    )
    backend = OwnedBackend(directory)
    request.addfinalizer(backend.close)
    binaries = tmp_path / "bin"
    binaries.mkdir()
    executable = binaries / "docker"
    executable.write_text(
        "#!/bin/bash\nexec "
        + shlex.quote(sys.executable)
        + " "
        + shlex.quote(str(ROOT / "tests/publisher_docker_client.py"))
        + ' "$@"\n'
    )
    executable.chmod(0o755)
    environment = os.environ.copy()
    environment["PATH"] = str(binaries) + ":" + environment["PATH"]
    environment["TANK_PUBLISHER_DOCKER_STAND"] = str(directory)
    return WorkerStand(
        directory, host, environment, "ghcr.io/frequensy23-coder/tank/tank:" + "a" * 40, backend
    )


def wait_for_effect(stand: WorkerStand) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        path = stand.root / "effects"
        if path.exists() and path.stat().st_size:
            return
        time.sleep(0.01)
    raise AssertionError("Owned worker did not begin its observed effect loop")


def test_successful_worker_uses_known_owned_identity_and_leaves_no_active_lease(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    stand = worker_stand(tmp_path, "NORMAL", request)
    process = stand.launch()
    stdout, stderr = process.communicate(timeout=4)
    assert process.returncode == 0, stderr.decode()
    assert stdout == b"worker completed\n"
    with state(stand.root) as observed:
        assert observed.containers == {}
        assert [call.command for call in observed.calls] == [
            "create",
            "inspect",
            "start",
            "inspect",
            "inspect",
            "rm",
        ]
        created = observed.calls[0].arguments
        assert created[created.index("--user") + 1] == f"{os.geteuid()}:{os.getegid()}"
        assert created[created.index("--transaction-release-sha") + 1] == "b" * 40
        assert created[created.index("--entrypoint") + 1] == "/opt/tank/venv/bin/python"
    assert not (stand.host / "apify-state/.worker-active").exists()


@pytest.mark.parametrize(
    ("signal_number", "expected_status"),
    [(signal.SIGTERM, 143), (signal.SIGHUP, 129), (signal.SIGINT, 130)],
)
def test_interrupt_drains_actual_child_before_return_and_stops_further_effects(
    tmp_path: Path, request: pytest.FixtureRequest, signal_number: signal.Signals, expected_status: int
) -> None:
    stand = worker_stand(tmp_path, "BLOCK", request)
    process = stand.launch()
    try:
        wait_for_effect(stand)
        process.send_signal(signal_number)
        _, stderr = process.communicate(timeout=4)
        assert process.returncode == expected_status, stderr.decode()
        before = (stand.root / "effects").read_bytes()
        time.sleep(0.05)
        assert (stand.root / "effects").read_bytes() == before
        with state(stand.root) as observed:
            assert observed.containers == {}
            assert observed.drain_attempts == 1
        assert not (stand.host / "apify-state/.worker-active").exists()
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            process.communicate(timeout=4)


def test_failed_drain_keeps_lease_and_next_command_drains_before_starting(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    stand = worker_stand(tmp_path, "DRAIN_FAIL", request)
    process = stand.launch()
    wait_for_effect(stand)
    process.send_signal(signal.SIGTERM)
    _, stderr = process.communicate(timeout=4)
    assert process.returncode == 125, stderr.decode()
    assert (stand.host / "apify-state/.worker-active").exists()
    with state(stand.root) as observed:
        assert len(observed.containers) == 1
        old_id = next(iter(observed.containers))
        observed.fault = "NORMAL"
    replacement = stand.launch()
    _, stderr = replacement.communicate(timeout=4)
    assert replacement.returncode == 0, stderr.decode()
    before = (stand.root / "effects").read_bytes()
    time.sleep(0.05)
    assert (stand.root / "effects").read_bytes() == before
    with state(stand.root) as observed:
        assert observed.containers == {}
        removals = [
            i for i, call in enumerate(observed.calls) if call.command == "rm" and old_id in call.arguments
        ]
        creations = [i for i, call in enumerate(observed.calls) if call.command == "create"]
        assert len(creations) == 2
        assert removals[-1] < creations[-1]
    assert not (stand.host / "apify-state/.worker-active").exists()


def test_unverified_prior_container_is_preserved_before_any_new_worker_effect(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    stand = worker_stand(tmp_path, "NORMAL", request)
    state_directory = stand.host / "apify-state"
    state_directory.mkdir(mode=0o700)
    lease = state_directory / ".worker-active"
    image = stand.image
    identifier = "d" * 64
    lease.write_text(
        "\n".join(("tank-publisher-worker-v1", identifier, ".publisher-run.ownedXYZ", image)) + "\n"
    )
    lease.chmod(0o600)
    with state(stand.root) as observed:
        observed.containers[identifier] = ContainerState(
            labels=ContainerLabels(
                role="worker-v1",
                owner="foreign",
                image=image,
                root=str(stand.host),
            ),
            status="created",
            exit_code=0,
            process_id=None,
        )
    original = lease.read_bytes()
    process = stand.launch()
    _, stderr = process.communicate(timeout=4)
    assert process.returncode == 125
    assert b"ownership is unverified" in stderr
    assert lease.read_bytes() == original
    with state(stand.root) as observed:
        assert identifier in observed.containers
        assert [call.command for call in observed.calls] == ["inspect"]
        assert observed.drain_attempts == 0


def test_unknown_image_repository_is_refused_before_any_docker_or_host_effect(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    stand = worker_stand(tmp_path, "NORMAL", request)
    unverified = replace(stand, image="ghcr.io/unverified/other/tank:" + "a" * 40)
    process = unverified.launch()
    _, stderr = process.communicate(timeout=4)
    assert process.returncode == 1
    assert b"invalid publisher image" in stderr
    assert not (stand.host / "apify-state").exists()
    with state(stand.root) as observed:
        assert observed.calls == []
        assert observed.containers == {}


def client(stand: WorkerStand, *arguments: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [sys.executable, str(ROOT / "tests/publisher_docker_client.py"), *arguments],
        env=stand.environment,
        capture_output=True,
        timeout=4,
        check=False,
    )


def control_container(stand: WorkerStand) -> str:
    cid_file = stand.host / "control.cid"
    arguments = ["create", "--cidfile", str(cid_file)]
    for key, value in (
        ("role", "worker-v1"),
        ("owner", "synthetic-control"),
        ("image", stand.image),
        ("root", str(stand.host)),
    ):
        arguments.extend(["--label", "org.tank.publisher." + key + "=" + value])
    created = client(stand, *arguments, stand.image)
    assert created.returncode == 0, created.stderr.decode()
    return cid_file.read_text().strip()


def test_concurrent_inspect_and_stop_progress_while_actual_attach_waits(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    stand = worker_stand(tmp_path, "BLOCK", request)
    identifier = control_container(stand)
    attached = stand.backend.launch_frontend(
        [sys.executable, str(ROOT / "tests/publisher_docker_client.py"), "start", "--attach", identifier],
        stand.environment,
    )
    wait_for_effect(stand)
    assert attached.poll() is None
    inspected = client(stand, "inspect", "--format", "{{.State.Status}}|{{.State.ExitCode}}", identifier)
    assert inspected.returncode == 0 and inspected.stdout == b"running|0\n"
    removed = client(stand, "rm", "--force", identifier)
    assert removed.returncode == 0
    attached.communicate(timeout=4)
    assert attached.returncode == 1
    before = (stand.root / "effects").read_bytes()
    time.sleep(0.05)
    assert (stand.root / "effects").read_bytes() == before
    stand.backend.close()
    assert stand.backend.receipt is not None and len(stand.backend.receipt.children) == 1
    assert stand.backend.receipt.children[0].container_id == identifier
    assert stand.backend.receipt.children[0].returncode == -signal.SIGTERM
    assert stand.backend.process.poll() == 0


@pytest.mark.parametrize("change", ["nonce", "process_id"])
def test_fresh_backend_identity_refuses_before_business_command(
    tmp_path: Path, request: pytest.FixtureRequest, change: str
) -> None:
    import json

    stand = worker_stand(tmp_path, "NORMAL", request)
    endpoint = read_endpoint(stand.root)
    path = stand.root / "endpoint.json"
    original = path.read_bytes()
    values = {
        "schema_version": "publisher-stand-v1",
        "root": str(endpoint.root),
        "socket_path": str(endpoint.socket_path),
        "process_id": endpoint.process_id,
        "nonce": endpoint.nonce,
    }
    values[change] = "f" * 32 if change == "nonce" else endpoint.process_id + 1
    try:
        path.write_text(json.dumps(values))
        refused = client(stand, "inspect", "--format", "{{.State.Status}}|{{.State.ExitCode}}", "a" * 64)
        assert refused.returncode == 1 and refused.stdout == b""
        with state(stand.root) as observed:
            assert observed.calls == [] and observed.containers == {}
    finally:
        path.write_bytes(original)


@pytest.mark.parametrize("reported", ["foreign", "missing"])
def test_unowned_state_pid_is_not_a_signal_capability(
    tmp_path: Path, request: pytest.FixtureRequest, reported: str
) -> None:
    stand = worker_stand(tmp_path, "NORMAL", request)
    foreign = subprocess.Popen([sys.executable, "-c", "import signal; signal.pause()"])
    identifier = "d" * 64
    try:
        with state(stand.root) as observed:
            observed.containers[identifier] = ContainerState(
                labels=ContainerLabels(
                    role="worker-v1", owner="synthetic", image=stand.image, root=str(stand.host)
                ),
                status="running",
                exit_code=0,
                process_id=foreign.pid if reported == "foreign" else None,
            )
        refused = client(stand, "rm", "--force", identifier)
        assert refused.returncode == 1 and foreign.poll() is None
        with state(stand.root) as observed:
            assert observed.containers[identifier].process_id == (
                foreign.pid if reported == "foreign" else None
            )
        stand.backend.close()
        assert foreign.poll() is None
    finally:
        foreign.terminate()
        foreign.wait(timeout=4)
