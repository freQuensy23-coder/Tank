"""Linux startup expiry: detached/double-fork children remain exactly supervised."""

import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path

worker = Path("/tmp/startup-worker.py")
worker.write_text(
    Path("/opt/tank/browser-worker.py").read_text().replace("REQUEST_TIMEOUT = 75.0", "REQUEST_TIMEOUT = 1.0")
)
package = Path("/tmp/startup-fake/playwright")
package.mkdir(parents=True)
(package / "__init__.py").write_text("")
(package / "async_api.py").write_text('''import asyncio,subprocess,sys
class Startup:
 async def __aenter__(self):
  code="""import os,time
from pathlib import Path
if os.fork()==0:
 os.setsid()
 if os.fork()==0:
  Path('/tmp/tank-double-fork-pid').write_text(str(os.getpid()))
  time.sleep(60)
  os._exit(0)
 os._exit(0)
os.waitpid(-1,0)
time.sleep(60)
"""
  subprocess.Popen([sys.executable,'-c',code])
  await asyncio.sleep(60)
 async def __aexit__(self,*args):pass
def async_playwright():return Startup()
''')
if "--exited-parent" in sys.argv:
    p = package / "async_api.py"
    p.write_text(
        p.read_text()
        .replace(
            "import asyncio,subprocess,sys", "import asyncio,subprocess,sys,os,time\nfrom pathlib import Path"
        )
        .replace(
            "  await asyncio.sleep(60)",
            "  while not Path('/tmp/tank-double-fork-pid').exists():time.sleep(.001)\n  os._exit(9)",
        )
    )


def active(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


unrelated = subprocess.Popen(["sleep", "30"])
baseline = {int(p.name) for p in Path("/proc").iterdir() if p.name.isdigit()}
try:
    encoded = base64.b64encode(json.dumps({"version": 1, "op": "open"}).encode()).decode()
    before = time.monotonic()
    result = subprocess.run(
        [sys.executable, str(worker), "--state-dir", "/workspace/startup-expiry", "--request", encoded],
        check=False,
        env={**os.environ, "PYTHONPATH": "/tmp/startup-fake"},
        capture_output=True,
        text=True,
        timeout=4,
    )
    elapsed = time.monotonic() - before
    assert result.returncode == 0, (result.returncode, result.stderr)
    reply = json.loads(result.stdout.removeprefix("__TANK_BROWSER_RPC_V1__ "))
    expected = "exited during startup" if "--exited-parent" in sys.argv else "budget"
    assert not reply["ok"] and expected in reply["error"], reply
    assert elapsed < 2, (elapsed, result.stdout)
    assert Path("/tmp/tank-double-fork-pid").exists(), "No double-fork child created"
    remaining = [
        int(p.name)
        for p in Path("/proc").iterdir()
        if p.name.isdigit() and int(p.name) not in baseline and active(int(p.name))
    ]
    assert not remaining, remaining
    assert unrelated.poll() is None
    print(
        json.dumps(
            {
                "contract": "startup_exited_parent_cleanup"
                if "--exited-parent" in sys.argv
                else "startup_deadline_double_fork_cleanup",
                "passed": True,
                "elapsed": elapsed,
                "new_live_orphans": len(remaining),
                "unrelated_alive": True,
            }
        )
    )
finally:
    unrelated.terminate()
    unrelated.wait()
