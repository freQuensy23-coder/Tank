"""Owned process stand for the publisher's Docker command and drain boundary."""

import fcntl
import os
import signal
import subprocess
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict


class StandModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)


class ContainerLabels(StandModel):
    role: str
    owner: str
    image: str
    root: str


class ContainerState(StandModel):
    labels: ContainerLabels
    status: Literal["created", "running", "exited"]
    exit_code: int
    process_id: int | None


class DockerCall(StandModel):
    command: str
    arguments: list[str]


class DockerState(StandModel):
    fault: Literal["NORMAL", "BLOCK", "DRAIN_FAIL"]
    containers: dict[str, ContainerState]
    calls: list[DockerCall]
    drain_attempts: int


@contextmanager
def state(root: Path) -> Iterator[DockerState]:
    with (root / "state.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        document = root / "state.json"
        observed = DockerState.model_validate_json(document.read_bytes())
        yield observed
        temporary = root / "state.next"
        temporary.write_text(observed.model_dump_json())
        temporary.replace(document)


def create(root: Path, arguments: list[str]) -> int:
    labels: dict[str, str] = {}
    cid_file: Path | None = None
    index = 0
    while index < len(arguments):
        flag = arguments[index]
        if flag == "--read-only":
            index += 1
            continue
        if not flag.startswith("--"):
            break
        value = arguments[index + 1]
        if flag == "--label":
            key, assigned = value.split("=", 1)
            if key in labels:
                raise ValueError("Duplicate stand label")
            labels[key] = assigned
        elif flag == "--cidfile":
            cid_file = Path(value)
        index += 2
    if cid_file is None:
        raise ValueError("Stand requires the actual CID file")
    container = ContainerState(
        labels=ContainerLabels(
            role=labels["org.tank.publisher.role"],
            owner=labels["org.tank.publisher.owner"],
            image=labels["org.tank.publisher.image"],
            root=labels["org.tank.publisher.root"],
        ),
        status="created",
        exit_code=0,
        process_id=None,
    )
    identifier = uuid.uuid4().hex + uuid.uuid4().hex
    with state(root) as observed:
        observed.containers[identifier] = container
    cid_file.write_text(identifier + "\n")
    return 0


WORKER = """import os,sys,time
from pathlib import Path
root = Path(sys.argv[1])
(root / "started").write_text(str(os.getpid()))
while True:
    with (root / "effects").open("a") as stream:
        stream.write("attempt\\n")
    time.sleep(0.01)
"""


def start(root: Path, identifier: str) -> int:
    with state(root) as observed:
        container = observed.containers[identifier]
        if observed.fault == "NORMAL":
            container.status = "exited"
            container.exit_code = 0
            print("worker completed")
            return 0
        process = subprocess.Popen(
            [sys.executable, "-c", WORKER, str(root)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        container.status = "running"
        container.process_id = process.pid
    result = process.wait()
    with state(root) as observed:
        if identifier in observed.containers:
            observed.containers[identifier].status = "exited"
            observed.containers[identifier].exit_code = result
    return 0 if result == 0 else 1


def inspect(root: Path, arguments: list[str]) -> int:
    format_text, identifier = arguments[1], arguments[2]
    with state(root) as observed:
        if identifier not in observed.containers:
            return 1
        container = observed.containers[identifier]
        if format_text == "{{.State.Status}}|{{.State.ExitCode}}":
            print(f"{container.status}|{container.exit_code}")
        elif "org.tank.publisher.role" in format_text:
            labels = container.labels
            print("|".join((labels.role, labels.owner, labels.image, labels.root)))
        else:
            raise ValueError("Unexpected stand inspect format")
    return 0


def remove(root: Path, identifier: str) -> int:
    with state(root) as observed:
        observed.drain_attempts += 1
        if observed.fault == "DRAIN_FAIL" and observed.drain_attempts == 1:
            return 1
        container = observed.containers[identifier]
        if container.process_id is not None:
            os.kill(container.process_id, signal.SIGTERM)
        del observed.containers[identifier]
    return 0


def main() -> int:
    root = Path(os.environ["TANK_PUBLISHER_DOCKER_STAND"])
    command, arguments = sys.argv[1], sys.argv[2:]
    with state(root) as observed:
        observed.calls.append(DockerCall(command=command, arguments=arguments))
    match command:
        case "create":
            return create(root, arguments)
        case "start":
            if arguments[0] != "--attach":
                raise ValueError("Stand requires attach")
            return start(root, arguments[1])
        case "inspect":
            return inspect(root, arguments)
        case "rm":
            if arguments[0] != "--force":
                raise ValueError("Stand requires bounded removal")
            return remove(root, arguments[1])
        case _:
            raise ValueError("Unsupported Docker stand command")


if __name__ == "__main__":
    raise SystemExit(main())
