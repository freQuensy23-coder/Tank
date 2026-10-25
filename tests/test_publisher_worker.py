import os
import shlex
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest
from publisher_docker_stand import DockerState, state

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def finish_owned_stand_processes(tmp_path: Path) -> Iterator[None]:
    yield
    root = tmp_path / "stand"
    if not (root / "state.json").exists():
        return
    with state(root) as observed:
        for container in observed.containers.values():
            if container.status != "running" or container.process_id is None:
                continue
            actual = int((root / "started").read_text())
            assert actual == container.process_id
            try:
                os.kill(actual, signal.SIGTERM)
            except ProcessLookupError:
                pass


@dataclass(frozen=True)
class WorkerStand:
    root: Path
    host: Path
    environment: dict[str, str]

    def launch(self) -> subprocess.Popen[bytes]:
        return subprocess.Popen(
            [
                "bash",
                str(ROOT / "scripts/deploy/publisher_worker.sh"),
                "ghcr.io/example/project/tank:" + "a" * 40,
                "a" * 40,
                "b" * 40,
                "b" * 40,
                "prepare",
                str(self.host),
            ],
            env=self.environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


def worker_stand(tmp_path: Path, fault: Literal["NORMAL", "BLOCK", "DRAIN_FAIL"]) -> WorkerStand:
    host = tmp_path / "host"
    host.mkdir()
    directory = tmp_path / "stand"
    directory.mkdir()
    (directory / "state.json").write_text(
        DockerState(fault=fault, containers={}, calls=[], drain_attempts=0).model_dump_json()
    )
    binaries = tmp_path / "bin"
    binaries.mkdir()
    executable = binaries / "docker"
    executable.write_text(
        "#!/bin/bash\nexec "
        + shlex.quote(sys.executable)
        + " "
        + shlex.quote(str(ROOT / "tests/publisher_docker_stand.py"))
        + ' "$@"\n'
    )
    executable.chmod(0o755)
    environment = os.environ.copy()
    environment["PATH"] = str(binaries) + ":" + environment["PATH"]
    environment["TANK_PUBLISHER_DOCKER_STAND"] = str(directory)
    return WorkerStand(directory, host, environment)


def wait_for_effect(stand: WorkerStand) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        path = stand.root / "effects"
        if path.exists() and path.stat().st_size:
            return
        time.sleep(0.01)
    raise AssertionError("Owned worker did not begin its observed effect loop")


def test_successful_worker_uses_known_owned_identity_and_leaves_no_active_lease(tmp_path: Path) -> None:
    stand = worker_stand(tmp_path, "NORMAL")
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
        assert created[created.index("--user") + 1] == "0:0"
        assert created[created.index("--transaction-release-sha") + 1] == "b" * 40
        assert created[created.index("--entrypoint") + 1] == "/opt/tank/venv/bin/python"
    assert not (stand.host / "apify-state/.worker-active").exists()


def test_interrupt_drains_actual_child_before_return_and_stops_further_effects(tmp_path: Path) -> None:
    stand = worker_stand(tmp_path, "BLOCK")
    process = stand.launch()
    try:
        wait_for_effect(stand)
        process.send_signal(signal.SIGTERM)
        _, stderr = process.communicate(timeout=4)
        assert process.returncode == 143, stderr.decode()
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


def test_failed_drain_keeps_lease_and_next_command_drains_before_starting(tmp_path: Path) -> None:
    stand = worker_stand(tmp_path, "DRAIN_FAIL")
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
