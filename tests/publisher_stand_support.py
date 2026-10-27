"""Per-case backend ownership; no global warm process or mutable result cache."""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

from publisher_stand_protocol import (
    ReadyReply,
    ReadyRequest,
    ShutdownReply,
    ShutdownRequest,
    exchange,
    read_endpoint,
)


class OwnedBackend:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.directory = tempfile.TemporaryDirectory(prefix="pds-")
        self.socket = Path(self.directory.name) / "endpoint"
        self.nonce = uuid.uuid4().hex
        self.frontends: list[subprocess.Popen[bytes]] = []
        self.receipt: ShutdownReply | None = None
        self.closed = False
        self.closing = False
        self.frontends_lock = threading.Lock()
        self.process = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).with_name("publisher_docker_stand.py")),
                "--root",
                str(root),
                "--socket",
                str(self.socket),
                "--nonce",
                self.nonce,
                "--parent-pid",
                str(os.getpid()),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 4
            while not (root / "endpoint.json").exists():
                if self.process.poll() is not None:
                    _, error = self.process.communicate(timeout=4)
                    raise RuntimeError("Owned backend exited before readiness: " + error.decode())
                if time.monotonic() >= deadline:
                    raise TimeoutError("Owned backend did not publish actual readiness")
                time.sleep(0.01)
            endpoint = read_endpoint(root)
            if endpoint.process_id != self.process.pid or endpoint.nonce != self.nonce:
                raise ValueError("Owned backend readiness identity differs")
            reply = exchange(endpoint, ReadyRequest(self.nonce), timeout=4)
            if not isinstance(reply, ReadyReply) or reply.process_id != self.process.pid:
                raise ValueError("Actual owned backend readiness required")
        except BaseException:
            if self.process.poll() is None:
                self.process.terminate()
            self.process.communicate(timeout=4)
            self.directory.cleanup()
            raise

    def launch_frontend(self, arguments: list[str], environment: dict[str, str]) -> subprocess.Popen[bytes]:
        with self.frontends_lock:
            if self.closed or self.closing:
                raise ValueError("The owned backend is closed")
            process = subprocess.Popen(
                arguments,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.frontends.append(process)
            return process

    def close(self) -> None:
        with self.frontends_lock:
            if self.closed:
                return
            self.closing = True
            shells = tuple(self.frontends)
        try:
            for shell in shells:
                if shell.poll() is None:
                    shell.send_signal(signal.SIGTERM)
            deadline = time.monotonic() + 4
            for shell in shells:
                shell.communicate(timeout=max(0, deadline - time.monotonic()))
            endpoint = read_endpoint(self.root)
            if (
                endpoint.process_id != self.process.pid
                or endpoint.nonce != self.nonce
                or endpoint.socket_path != self.socket
                or endpoint.root != self.root
            ):
                raise ValueError("Shutdown must retain the independently owned backend identity")
            reply = exchange(endpoint, ShutdownRequest(self.nonce), timeout=4)
            if not isinstance(reply, ShutdownReply) or reply.process_id != self.process.pid:
                raise ValueError("Actual owned drain receipt required")
            stdout, stderr = self.process.communicate(timeout=4)
            if self.process.returncode != 0 or stdout:
                raise RuntimeError("Owned backend did not close cleanly: " + stderr.decode())
            record = {
                "backend_pid": self.process.pid,
                "backend_exit": self.process.returncode,
                "children": [
                    {"container_id": child.container_id, "pid": child.process_id, "exit": child.returncode}
                    for child in reply.children
                ],
            }
            (self.root / "backend-closed.json").write_text(json.dumps(record))
            self.receipt = reply
            self.closed = True
        finally:
            if self.process.poll() is None:
                self.process.terminate()
                self.process.communicate(timeout=4)
            self.directory.cleanup()
