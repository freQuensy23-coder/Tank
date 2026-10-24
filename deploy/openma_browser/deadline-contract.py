"""Actual Chromium in isolated Linux fixture: timeout, queue, exact cleanup, recovery."""

import base64
import hashlib
import json
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

worker = Path("/tmp/deadline-worker.py")
worker.write_text(
    Path("/opt/tank/browser-worker.py").read_text().replace("REQUEST_TIMEOUT = 75.0", "REQUEST_TIMEOUT = 2.5")
)
state = Path("/workspace/deadline-contract-" + uuid.uuid4().hex)
args = [sys.executable, str(worker), "--state-dir", str(state)]
prefix = "__TANK_BROWSER_RPC_V1__ "


def command(op, **values):
    payload = base64.b64encode(json.dumps({"version": 1, "op": op, "args": values}).encode()).decode()
    return [*args, "--request", payload]


def call(op, **values):
    start = time.monotonic()
    run = subprocess.run(command(op, **values), check=False, capture_output=True, text=True, timeout=6)
    assert run.returncode == 0, (run.returncode, run.stderr)
    assert run.stdout.startswith(prefix), run.stdout
    return json.loads(run.stdout[len(prefix) :]), time.monotonic() - start


def ok(op, **values):
    reply, _elapsed = call(op, **values)
    assert reply["ok"], reply
    return reply["result"]


def sockpath():
    return (
        Path("/tmp") / ("tank-browser-" + hashlib.sha256(str(state).encode()).hexdigest()[:32]) / "rpc.sock"
    )


def daemon_pid():
    with socket.socket(socket.AF_UNIX) as s:
        s.connect(str(sockpath()))
        return struct.unpack("3i", s.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[0]


def tree(pid):
    try:
        children = [int(x) for x in Path(f"/proc/{pid}/task/{pid}/children").read_text().split()]
    except FileNotFoundError:
        return []
    return children + [c for p in children for c in tree(p)]


def active(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"<html><body>Persistent profile deadline contract</body></html>")

    def log_message(self, *args) -> None:
        pass


http = HTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=http.serve_forever, daemon=True).start()
url = f"http://127.0.0.1:{http.server_port}/"
unrelated = subprocess.Popen(["sleep", "30"])
baseline = {int(p.name) for p in Path("/proc").iterdir() if p.name.isdigit()}
try:
    ok("open")
    ok("goto", url=url)
    ok(
        "evaluate",
        expression="localStorage.setItem('saved','yes');document.cookie='saved=yes;Max-Age=3600;Path=/'",
    )
    ok("close")
    ok("open")
    ok("goto", url=url)
    pid = daemon_pid()
    owned = [pid, *tree(pid)]

    hang = subprocess.Popen(
        command("evaluate", expression="(()=>{while(true){}})()"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(0.2)
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(1)
        s.connect(str(sockpath()))
        s.sendall(
            (
                json.dumps(
                    {
                        "version": 1,
                        "op": "evaluate",
                        "args": {"expression": "localStorage.setItem('late','bad')"},
                        "deadline": time.monotonic() + 0.15,
                    }
                )
                + "\n"
            ).encode()
        )
        reply = json.loads(s.makefile("rb").readline())
        assert not reply["ok"] and "action queue" in reply["error"], reply
    started = time.monotonic()
    out, err = hang.communicate(timeout=4)
    assert hang.returncode == 0, (hang.returncode, err)
    assert not json.loads(out.removeprefix(prefix))["ok"], out
    end = time.monotonic() + 1
    while any(active(p) for p in owned) and time.monotonic() < end:
        time.sleep(0.01)
    assert not any(active(p) for p in owned), [(p, active(p)) for p in owned]
    survivors = [
        int(p.name)
        for p in Path("/proc").iterdir()
        if p.name.isdigit() and int(p.name) not in baseline and active(int(p.name))
    ]
    assert not survivors, ("Unowned orphan browser process survived", survivors)
    assert unrelated.poll() is None, "Unrelated process was killed"
    ok("open")
    ok("goto", url=url)
    assert ok("evaluate", expression="localStorage.getItem('saved')") == "yes"
    assert "saved=yes" in ok("evaluate", expression="document.cookie")
    assert ok("evaluate", expression="localStorage.getItem('late')") is None
    assert daemon_pid() != pid
    ok("close")
    print(
        json.dumps(
            {
                "contract": "real_chromium_deadline_queue_scope_profile_recovery",
                "passed": True,
                "previous_daemon_pid": pid,
                "new_daemon_pid": daemon_pid(),
                "owned_processes_checked": len(owned),
                "new_live_orphans": len(survivors),
                "unrelated_alive": unrelated.poll() is None,
                "budget_seconds": 2.5,
            }
        )
    )
finally:
    unrelated.terminate()
    unrelated.wait()
    http.shutdown()
