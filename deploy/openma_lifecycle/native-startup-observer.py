"""Read-only guest telemetry. Does not launch, signal, patch or configure browsers."""

import argparse
import hashlib
import json
import os
import resource
import time
from pathlib import Path


def process(root, pid):
    base = root / str(pid)
    try:
        raw = (base / "stat").read_text()
        name, fields = raw.split("(", 1)[1].rsplit(")", 1)
        f = fields.split()
        row = {
            "pid": pid,
            "name": name,
            "state": f[0],
            "ppid": int(f[1]),
            "utime_ticks": int(f[11]),
            "stime_ticks": int(f[12]),
            "start_ticks": int(f[19]),
            "threads": int(f[17]),
            "rss_pages": int(f[21]),
        }
        for file in ("schedstat", "io"):
            try:
                row[file] = (base / file).read_text().strip()
            except OSError as error:
                row[file] = type(error).__name__
        return row
    except (OSError, ValueError, IndexError):
        return None


def all_processes(root):
    return {
        p.name: row
        for p in root.iterdir()
        if p.name.isdigit()
        if (row := process(root, int(p.name))) is not None
    }


def role(root, pid, worker, state):
    # Read only this credential-free guest's new-process argv to match the exact
    # worker path/state. Never retain argument strings, request data or env.
    try:
        args = (root / str(pid) / "cmdline").read_bytes().split(b"\0")
        if len(args) < 4 or args[1] != str(worker).encode() or b"--state-dir" not in args:
            return None
        index = args.index(b"--state-dir") + 1
        if index >= len(args) or args[index] not in {str(state).encode(), str(state.resolve()).encode()}:
            return None
        return "daemon" if b"--serve" in args else "client" if b"--request" in args else None
    except OSError:
        return None


def stat_path(path):
    try:
        st = path.lstat()
        return {
            "exists": True,
            "mode": st.st_mode,
            "size": st.st_size,
            "mtime_ns": st.st_mtime_ns,
            "inode": st.st_ino,
            "uid": st.st_uid,
            "gid": st.st_gid,
        }
    except FileNotFoundError:
        return {"exists": False}


def run(args):
    root = Path("/proc")
    assert hashlib.sha256(args.worker.read_bytes()).hexdigest() == args.worker_sha
    resolved_state = args.state.resolve()
    profile = resolved_state / "profile"
    initial_profile = stat_path(profile)
    baseline = {(row["pid"], row["start_ticks"]) for row in all_processes(root).values()}
    sock = (
        Path("/tmp")
        / ("tank-browser-" + hashlib.sha256(str(resolved_state).encode()).hexdigest()[:32])
        / "rpc.sock"
    )
    start = time.monotonic()
    known = {}
    roots = {}
    args.output.mkdir(exist_ok=False)
    (args.output / "ready.json").write_text(
        json.dumps(
            {
                "observer_pid": os.getpid(),
                "guest_monotonic": start,
                "initial_profile": initial_profile,
                "worker_sha256": args.worker_sha,
                "state_argument": str(args.state),
                "state_resolved": str(resolved_state),
                "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
                "page_size": os.sysconf("SC_PAGE_SIZE"),
                "resource_limits": {
                    "nofile": resource.getrlimit(resource.RLIMIT_NOFILE),
                    "nproc": resource.getrlimit(resource.RLIMIT_NPROC),
                },
                "mountinfo": (root / "self/mountinfo").read_text(),
                "path_stats": {
                    str(p): stat_path(p) for p in (Path("/workspace"), args.state.parent, args.state)
                },
                "browser_environment_modified": False,
                "worker_invocation_modified": False,
                "daemon_stderr_limitation": "8e sends daemon stderr to DEVNULL; not intercepted",
            }
        )
    )
    while time.monotonic() - start < args.max_seconds:
        rows = all_processes(root)
        current = {(row["pid"], row["start_ticks"]): row for row in rows.values()}
        new = {key: row for key, row in current.items() if key not in baseline}
        for key, row in new.items():
            label = role(root, row["pid"], args.worker, args.state)
            if label:
                roots[key] = label
                known[key] = label
        changed = True
        while changed:
            changed = False
            for key, row in new.items():
                if key not in known and any(
                    parent[0] == row["ppid"] for parent in known if parent in current
                ):
                    known[key] = "descendant"
                    changed = True
        new_live = [row for row in new.values() if row["state"] not in ("Z", "X")]
        try:
            phase = json.loads(args.phase.read_text())
        except (OSError, ValueError):
            phase = {"phase": "not_started"}
        snapshot = {
            "guest_elapsed": time.monotonic() - start,
            "guest_monotonic": time.monotonic(),
            "guest_epoch": time.time(),
            "phase": phase,
            "observer_self": process(root, os.getpid()),
            "socket": stat_path(sock),
            "profile": stat_path(profile),
            "profile_lock": stat_path(profile / "SingletonLock"),
            "worker_roots_seen": [
                {"pid": key[0], "start_ticks": key[1], "role": value} for key, value in roots.items()
            ],
            "observed_owned_processes": [
                {**current[key], "role": label} for key, label in known.items() if key in current
            ],
            "all_new_live_guest_processes": new_live,
            "all_new_guest_processes": list(new.values()),
            "cleanup_empty_snapshot": bool(roots) and phase.get("phase") == "after_first_failure" and not new,
        }
        for file in ("meminfo", "pressure/cpu", "pressure/memory", "pressure/io", "stat"):
            try:
                snapshot[file] = (root / file).read_text()
            except OSError as error:
                snapshot[file] = type(error).__name__
        encoded = json.dumps(snapshot)
        temporary = args.output / "latest.new"
        temporary.write_text(encoded)
        temporary.replace(args.output / "latest.json")
        with (args.output / "timeline.jsonl").open("a") as log:
            log.write(encoded + "\n")
        if args.stop.exists():
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--worker", type=Path, required=True)
    p.add_argument("--worker-sha", required=True)
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--phase", type=Path, required=True)
    p.add_argument("--stop", type=Path, required=True)
    p.add_argument("--interval", type=float, default=5)
    p.add_argument("--max-seconds", type=float, default=240)
    run(p.parse_args())
