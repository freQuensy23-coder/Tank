"""Owned process stand for the publisher's Docker command and drain boundary."""

import argparse
import fcntl
import io
import os
import re
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from types import FrameType
from typing import IO, Literal

from publisher_stand_protocol import (
    MAX_PACKET,
    ChildExit,
    CommandReply,
    CommandRequest,
    Endpoint,
    ReadyReply,
    ReadyRequest,
    ShutdownReply,
    decode_request,
    encode_reply,
    write_endpoint,
)
from pydantic import BaseModel, ConfigDict


class OwnedChildren:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.drain_lock = threading.Lock()
        self.closed = False
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.receipt: tuple[ChildExit, ...] | None = None

    def spawn(self, identifier: str, root: Path) -> subprocess.Popen[bytes]:
        with self.lock:
            if self.closed or identifier in self.processes:
                raise ValueError("Effect launch is closed or already owned")
            process = subprocess.Popen(
                [sys.executable, "-c", WORKER, str(root)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            self.processes[identifier] = process
            return process

    def stop(
        self, identifier: str, reported_pid: int | None, status: Literal["created", "running", "exited"]
    ) -> None:
        with self.lock:
            process = self.processes.get(identifier)
            if process is None:
                if reported_pid is not None or status == "running":
                    raise ValueError("Effect lacks its owned process handle")
                return
            if reported_pid is not None and process.pid != reported_pid:
                raise ValueError("Effect PID is not an owned process handle")
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=4)

    def drain(self) -> tuple[ChildExit, ...]:
        with self.drain_lock:
            if self.receipt is not None:
                return self.receipt
            with self.lock:
                self.closed = True
                processes = tuple(self.processes.items())
            deadline = time.monotonic() + 4
            for _, process in processes:
                if process.poll() is None:
                    process.terminate()
            rows = []
            for identifier, process in processes:
                code = process.wait(timeout=max(0, deadline - time.monotonic()))
                rows.append(ChildExit(identifier, process.pid, code))
            self.receipt = tuple(rows)
            return self.receipt


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
        elif flag not in {"--pull", "--user", "--network", "--volume", "--env", "--workdir", "--entrypoint"}:
            raise ValueError("Unexpected stand create flag")
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


def start(root: Path, identifier: str, children: OwnedChildren, output: IO[str]) -> int:
    with state(root) as observed:
        container = observed.containers[identifier]
        if observed.fault == "NORMAL":
            container.status = "exited"
            container.exit_code = 0
            print("worker completed", file=output)
            return 0
        process = children.spawn(identifier, root)
        container.status = "running"
        container.process_id = process.pid
    result = process.wait()
    with state(root) as observed:
        if identifier in observed.containers:
            observed.containers[identifier].status = "exited"
            observed.containers[identifier].exit_code = result
    return 0 if result == 0 else 1


def inspect(root: Path, arguments: list[str], output: IO[str]) -> int:
    format_text, identifier = arguments[1], arguments[2]
    with state(root) as observed:
        if identifier not in observed.containers:
            return 1
        container = observed.containers[identifier]
        if format_text == "{{.State.Status}}|{{.State.ExitCode}}":
            print(f"{container.status}|{container.exit_code}", file=output)
        elif "org.tank.publisher.role" in format_text:
            labels = container.labels
            print("|".join((labels.role, labels.owner, labels.image, labels.root)), file=output)
        else:
            raise ValueError("Unexpected stand inspect format")
    return 0


def remove(root: Path, identifier: str, children: OwnedChildren) -> int:
    with state(root) as observed:
        observed.drain_attempts += 1
        if observed.fault == "DRAIN_FAIL" and observed.drain_attempts == 1:
            return 1
        container = observed.containers[identifier]
        children.stop(identifier, container.process_id, container.status)
        del observed.containers[identifier]
    return 0


def dispatch(root: Path, request: CommandRequest, children: OwnedChildren, output: IO[str]) -> int:
    command, arguments = request.command, list(request.arguments)
    with state(root) as observed:
        observed.calls.append(DockerCall(command=command, arguments=arguments))
    match command:
        case "create":
            return create(root, arguments)
        case "start":
            if arguments[0] != "--attach":
                raise ValueError("Stand requires attach")
            return start(root, arguments[1], children, output)
        case "inspect":
            return inspect(root, arguments, output)
        case "rm":
            if arguments[0] != "--force":
                raise ValueError("Stand requires bounded removal")
            return remove(root, arguments[1], children)


class StandHandler(socketserver.StreamRequestHandler):
    def setup(self) -> None:
        super().setup()
        if not isinstance(self.server, StandServer):
            raise TypeError("Actual owned stand server required")
        with self.server.connections_lock:
            self.server.connections.add(self.connection)

    def finish(self) -> None:
        try:
            super().finish()
        finally:
            if isinstance(self.server, StandServer):
                with self.server.connections_lock:
                    self.server.connections.discard(self.connection)

    def handle(self) -> None:
        server = self.server
        if not isinstance(server, StandServer):
            raise TypeError("Actual owned stand server required")
        output = io.StringIO()
        try:
            request = decode_request(self.rfile.read(MAX_PACKET + 1))
            if request.nonce != server.endpoint.nonce:
                raise ValueError("Stand request ownership refused")
            if isinstance(request, ReadyRequest):
                reply = encode_reply(ReadyReply(os.getpid()))
            elif isinstance(request, CommandRequest):
                code = dispatch(server.endpoint.root, request, server.children, output)
                reply = encode_reply(CommandReply(os.getpid(), code, output.getvalue(), ""))
            else:
                children = server.close_owned()
                reply = encode_reply(ShutdownReply(os.getpid(), children))
        except Exception as error:
            reply = encode_reply(
                CommandReply(
                    os.getpid(), 1, output.getvalue(), type(error).__name__ + ": " + str(error) + "\n"
                )
            )
        try:
            self.wfile.write(reply)
        except (BrokenPipeError, ConnectionResetError):
            return


class StandServer(socketserver.ThreadingUnixStreamServer):
    def __init__(self, endpoint: Endpoint, parent_pid: int) -> None:
        self.endpoint = endpoint
        self.parent_pid = parent_pid
        self.children = OwnedChildren()
        self.connections_lock = threading.Lock()
        self.connections: set[socket.socket] = set()
        self.owner_gone = False
        super().__init__(str(endpoint.socket_path), StandHandler)
        endpoint.socket_path.chmod(0o600)

    def close_owned(self) -> tuple[ChildExit, ...]:
        children = self.children.drain()
        with self.connections_lock:
            connections = tuple(self.connections)
        for connection in connections:
            with suppress(OSError):
                connection.shutdown(socket.SHUT_RD)
        self.shutdown()
        return children

    def service_actions(self) -> None:
        if not self.owner_gone and os.getppid() != self.parent_pid:
            self.owner_gone = True
            threading.Thread(target=self.close_owned).start()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--parent-pid", type=int, required=True)
    args = parser.parse_args()
    if (
        os.getppid() != args.parent_pid
        or not args.root.is_absolute()
        or args.root.is_symlink()
        or args.root.stat().st_uid != os.geteuid()
    ):
        raise ValueError("Actual stand owner/root required")
    if (
        args.socket.exists()
        or args.socket.is_symlink()
        or args.socket.parent.stat().st_uid != os.geteuid()
        or args.socket.parent.stat().st_mode & 0o777 != 0o700
    ):
        raise ValueError("Actual private fresh socket required")
    if (
        re.fullmatch("[0-9a-f]{32}", args.nonce) is None
        or len(str(args.socket).encode()) >= 104
        or args.socket.parent.is_symlink()
    ):
        raise ValueError("Actual bounded private socket/nonce required")
    endpoint = Endpoint(args.root, args.socket, os.getpid(), args.nonce)
    with StandServer(endpoint, args.parent_pid) as server:

        def stopped(number: int, frame: FrameType | None) -> None:
            threading.Thread(target=server.close_owned).start()

        signal.signal(signal.SIGTERM, stopped)
        signal.signal(signal.SIGINT, stopped)
        write_endpoint(endpoint)
        try:
            server.serve_forever(poll_interval=0.05)
        finally:
            server.children.drain()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
