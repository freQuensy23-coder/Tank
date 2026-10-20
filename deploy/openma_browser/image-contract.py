"""Real image check: Python 3.12, analysis libraries, Chromium and worker RPC.

Run inside the sandbox image, with no credentials/network and a local fixture.
The container is a fake VM executor for this test; KVM is not certified here.
"""

import base64
import hashlib
import importlib
import importlib.metadata
import json
import subprocess
import sys
import tempfile
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def main():
    assert sys.version_info[:2] == (3, 12), sys.version
    cache = json.loads(Path("/opt/tank/browser-font-cache.json").read_text())
    assert cache["engine"] == "Playwright default bundled Chromium headless shell"
    assert cache["cache_format"] == 12 and cache["chromium"] == "156.0.8078.4"
    assert cache["temporary_profile_removed"] and cache["original_cache8_unchanged"]
    assert (
        cache["worker_sha256"] == hashlib.sha256(Path("/opt/tank/browser-worker.py").read_bytes()).hexdigest()
    )
    assert cache["cache12"]
    for name, expected in cache["cache12"].items():
        actual = hashlib.sha256((Path("/var/cache/fontconfig") / name).read_bytes()).hexdigest()
        assert actual == expected, name
    for name in (
        "pandas",
        "numpy",
        "scipy",
        "statsmodels",
        "pyarrow",
        "duckdb",
        "psycopg",
        "httpx",
        "websockets",
    ):
        importlib.import_module(name)
    assert importlib.metadata.version("playwright") == "1.64.0"
    worker = "/opt/tank/browser-worker.py"
    with tempfile.TemporaryDirectory(prefix="tank-browser-image-") as root:
        root = Path(root)
        (root / "index.html").write_text(
            "<h1>Sandbox Chromium fixture</h1><input id='field'><p id='out'></p>"
            '<button onclick=\'document.querySelector("#out").textContent=document.querySelector("#field").value\'>Apply</button>'
        )
        handler = partial(SimpleHTTPRequestHandler, directory=str(root))
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_port}/"

        def rpc(session, op, **args):
            payload = base64.b64encode(json.dumps({"version": 1, "op": op, "args": args}).encode()).decode()
            result = subprocess.run(
                [sys.executable, worker, "--state-dir", str(root / session), "--request", payload],
                check=True,
                capture_output=True,
                text=True,
                timeout=90,
            )
            prefix = "__TANK_BROWSER_RPC_V1__ "
            replies = [line[len(prefix) :] for line in result.stdout.splitlines() if line.startswith(prefix)]
            assert len(replies) == 1, result.stdout
            response = json.loads(replies[0])
            assert response["ok"], response
            return response["result"]

        try:
            assert rpc("one", "goto", url=url)["status"] == 200
            rpc("one", "fill", selector="#field", text="works inside image")
            rpc("one", "click", selector="button")
            assert rpc("one", "inner_text", selector="#out") == "works inside image"
            png = base64.b64decode(rpc("one", "screenshot", fullPage=True, type="png"))
            assert png[:8] == b"\x89PNG\r\n\x1a\n"
            rpc("one", "evaluate", expression="localStorage.setItem('identity','one')")
            rpc("two", "goto", url=url)
            assert rpc("two", "evaluate", expression="localStorage.getItem('identity')") is None
            rpc("one", "close")
            rpc("one", "goto", url=url)
            assert rpc("one", "evaluate", expression="localStorage.getItem('identity')") == "one"
            print(
                json.dumps(
                    {
                        "python": sys.version.split()[0],
                        "playwright": importlib.metadata.version("playwright"),
                        "checks": [
                            "font-cache-ABI-and-worker-binding",
                            "analysis-libraries",
                            "navigation",
                            "fill",
                            "click",
                            "text",
                            "PNG",
                            "evaluate",
                            "session-isolation",
                            "persistent-profile",
                        ],
                        "result": "passed",
                        "kvm": "unverified",
                    },
                    indent=2,
                )
            )
        finally:
            for session in ("one", "two"):
                rpc(session, "close")
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    main()
