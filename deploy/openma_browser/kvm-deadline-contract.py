"""Probe an already-open VM browser: absolute deadline, fatal lock and exact cleanup.

Fixture requests carry shorter absolute deadlines on the private Unix protocol.
The installed worker's production75-second constant and outer90-second exec
limit remain unchanged. The host checks real profile recovery after this probe.
"""

import argparse
import hashlib
import json
import os
import select
import socket
import struct
import subprocess
import time
from pathlib import Path


def endpoint(state):
    identity = hashlib.sha256(str(state).encode()).hexdigest()[:32]
    return Path("/tmp") / ("tank-browser-" + identity) / "rpc.sock"


def client(path, timeout=30):
    connection = socket.socket(socket.AF_UNIX)
    connection.settimeout(timeout)
    connection.connect(str(path))
    return connection


def peer(path):
    with client(path) as connection:
        pid = struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[0]
        send(connection, "diagnostic_peer_identity", time.monotonic() + 5)
        rejected = reply(connection)
        assert rejected and not rejected["ok"] and "Unsupported browser operation" in rejected["error"]
    assert pid > 0, "Peer PID unavailable in this diagnostic's namespace"
    return pid


def children(pid):
    try:
        direct = [int(n) for n in Path(f"/proc/{pid}/task/{pid}/children").read_text().split()]
    except FileNotFoundError:
        return []
    return direct + [child for parent in direct for child in children(parent)]


def active(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def send(connection, operation, deadline, **args):
    request = {"version": 1, "op": operation, "args": args, "deadline": deadline}
    connection.sendall((json.dumps(request) + "\n").encode())


def reply(connection):
    try:
        raw = connection.makefile("rb").readline(65536)
    except ConnectionResetError:
        return None
    return json.loads(raw) if raw else None


def run(state, other_state, evidence):
    started = time.monotonic()
    evidence.parent.mkdir(parents=True, exist_ok=True)
    path = endpoint(state)
    daemon = peer(path)
    other_daemon = peer(endpoint(other_state))
    owned = [daemon, *children(daemon)]
    handles = [os.pidfd_open(pid) for pid in owned]
    unrelated = subprocess.Popen(["sleep", "120"])
    baseline = {int(p.name) for p in Path("/proc").iterdir() if p.name.isdigit()}

    def mark(phase, **extra):
        record = {"phase": phase, "elapsed": time.monotonic() - started, **extra}
        temporary = evidence.with_suffix(".new")
        temporary.write_text(json.dumps(record))
        temporary.replace(evidence)
        print(json.dumps(record), flush=True)

    try:
        mark("fatal_probe_started", daemon=daemon, other_daemon=other_daemon, owned_pids=owned)
        with (
            client(path) as hang,
            client(path) as expired,
            client(path) as queued_open,
            client(path) as queued_mutation,
        ):
            send(hang, "evaluate", time.monotonic() + 5, expression="(()=>{while(true){}})()")
            time.sleep(0.2)
            send(
                expired,
                "evaluate",
                time.monotonic() + 0.6,
                expression="localStorage.setItem('expired_queue_mutation','bad')",
            )
            denied = reply(expired)
            assert denied and not denied["ok"] and "action queue" in denied["error"], denied

            send(queued_open, "open", time.monotonic() + 25)
            send(
                queued_mutation,
                "evaluate",
                time.monotonic() + 25,
                expression="localStorage.setItem('fatal_queue_mutation','bad')",
            )
            mark("expired_queue_refused", response=denied)
            timed_out = reply(hang)
            assert timed_out and not timed_out["ok"] and "worker recycled" in timed_out["error"], timed_out
            for queued in (queued_open, queued_mutation):
                result = reply(queued)
                assert result is None or not result["ok"], result
        end = time.monotonic() + 8
        while any(active(pid) for pid in owned) and time.monotonic() < end:
            time.sleep(0.02)
        assert not any(active(pid) for pid in owned), [(pid, active(pid)) for pid in owned]
        poller = select.poll()
        for handle in handles:
            poller.register(handle, select.POLLIN)
        assert len(poller.poll(0)) == len(handles), "A stable owned PID handle has not exited"
        assert active(other_daemon), "Another browser profile daemon was killed"
        assert unrelated.poll() is None, "An unrelated process was killed"
        orphans = [
            int(p.name)
            for p in Path("/proc").iterdir()
            if p.name.isdigit() and int(p.name) not in baseline and active(int(p.name))
        ]
        assert not orphans, ("New live processes survived fatal cleanup", orphans)
        mark(
            "fatal_probe_passed",
            old_daemon=daemon,
            owned_processes=len(owned),
            new_live_orphans=len(orphans),
            other_profile_alive=True,
            unrelated_alive=True,
            expired_queue_refused=True,
            valid_queued_open_never_succeeded=True,
            valid_queued_mutation_never_succeeded=True,
            fixture_operation_deadline_seconds=5,
            production_worker_timeout_seconds=75,
        )
    finally:
        for handle in handles:
            os.close(handle)
        unrelated.terminate()
        unrelated.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("state", type=Path)
    parser.add_argument("other_state", type=Path)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    run(args.state.resolve(), args.other_state.resolve(), args.evidence)
