"""Injected native launch errors, with real Linux subprocess cleanup and queue refusal."""

import base64
import json
import os
import socket
import struct
import subprocess
import sys
import time
import uuid
from pathlib import Path


def active(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


for failure in ("timeout", "error"):
    root = Path("/tmp/launch-failure-" + uuid.uuid4().hex)
    package = root / "playwright"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "async_api.py").write_text("""import asyncio,subprocess
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(__file__).parent.parent
class Page:
 url='about:blank'
 def is_closed(self):return False
 async def evaluate(self,*args):ROOT.joinpath('late-mutation').touch()
class Context:
 pages=[Page()]
 def set_default_timeout(self,*args):pass
 async def close(self):pass
class Chromium:
 async def launch_persistent_context(self,*args,**kwargs):
  if not ROOT.joinpath('failed-once').exists():
   child=subprocess.Popen(['sleep','60'],start_new_session=True)
   ROOT.joinpath('helper-pid').write_text(str(child.pid))
   ROOT.joinpath('failed-once').touch()
   await asyncio.sleep(.4)
   if ROOT.joinpath('kind').read_text()=='timeout':raise TimeoutError('injected native launch timeout')
   raise RuntimeError('injected native launch error')
  return Context()
class Playwright:
 chromium=Chromium()
class Startup:
 async def __aenter__(self):return Playwright()
 async def __aexit__(self,*args):pass
def async_playwright():return Startup()
""")
    (root / "kind").write_text(failure)
    state = root / "profile-state"
    worker = root / "worker.py"
    worker.write_text(
        Path("/opt/tank/browser-worker.py")
        .read_text()
        .replace("REQUEST_TIMEOUT = 75.0", "REQUEST_TIMEOUT = 2.5")
    )
    env = {**os.environ, "PYTHONPATH": str(root)}

    def command(op, worker=worker, state=state):
        payload = base64.b64encode(json.dumps({"version": 1, "op": op}).encode()).decode()
        return [sys.executable, str(worker), "--state-dir", str(state), "--request", payload]

    unrelated = subprocess.Popen(["sleep", "30"])
    baseline = {int(p.name) for p in Path("/proc").iterdir() if p.name.isdigit()}
    try:
        started = time.monotonic()
        client = subprocess.Popen(
            command("open"), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        while not (root / "helper-pid").exists():
            assert time.monotonic() - started < 2
            time.sleep(0.005)
        import hashlib

        sock = Path("/tmp/tank-browser-" + hashlib.sha256(str(state).encode()).hexdigest()[:32]) / "rpc.sock"
        with socket.socket(socket.AF_UNIX) as queued:
            queued.settimeout(3)
            queued.connect(str(sock))
            old_daemon = struct.unpack("3i", queued.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[0]
            queued.sendall(
                (
                    json.dumps(
                        {
                            "version": 1,
                            "op": "evaluate",
                            "args": {"expression": "late"},
                            "deadline": time.monotonic() + 2,
                        }
                    )
                    + "\n"
                ).encode()
            )
            try:
                reply = queued.recv(4096)
            except ConnectionResetError:
                reply = b""
            assert not reply or not json.loads(reply)["ok"], reply
        out, err = client.communicate(timeout=3)
        assert client.returncode == 0, err
        result = json.loads(out.removeprefix("__TANK_BROWSER_RPC_V1__ "))
        assert not result["ok"] and "injected native launch " + failure in result["error"], result
        assert time.monotonic() - started < 2, "Native failure was masked until outer deadline"
        time.sleep(0.05)
        assert not active(old_daemon)
        assert not active(int((root / "helper-pid").read_text()))
        assert not (root / "late-mutation").exists()
        assert unrelated.poll() is None
        recovered = subprocess.run(
            command("open"), check=False, env=env, capture_output=True, text=True, timeout=3
        )
        assert json.loads(recovered.stdout.removeprefix("__TANK_BROWSER_RPC_V1__ "))["ok"], recovered.stdout
        with socket.socket(socket.AF_UNIX) as next_socket:
            next_socket.connect(str(sock))
            new_daemon = struct.unpack(
                "3i", next_socket.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
            )[0]
        assert new_daemon != old_daemon

        os.kill(new_daemon, 15)
        time.sleep(0.05)
        survivors = [
            int(p.name)
            for p in Path("/proc").iterdir()
            if p.name.isdigit() and int(p.name) not in baseline and active(int(p.name))
        ]
        assert not survivors, survivors
        print(
            json.dumps(
                {
                    "contract": "injected_native_launch_" + failure,
                    "passed": True,
                    "queue_mutations": 0,
                    "new_live_orphans": len(survivors),
                    "unrelated_alive": True,
                }
            )
        )
    finally:
        unrelated.terminate()
        unrelated.wait()
