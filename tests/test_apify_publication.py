import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
ACTOR = "rbf5CQKQgKNVl4SfO"


@pytest.mark.integration
@pytest.mark.parametrize(
    "mode", ["success", "unchanged", "failed", "wrong_source", "roll_failed", "term"]
)
def test_worker_publication_transaction(tmp_path, mode):
    source = tmp_path / "worker"
    source.mkdir()
    (source / "main.py").write_text("print('selected revision')\n")
    (source / ".actor").mkdir()
    (source / ".actor/actor.json").write_text('{"version":"0.2"}')
    files = [{"name": "main.py", "format": "TEXT", "content": "print('selected revision')\n"}]
    files.append({"name": ".actor/actor.json", "format": "TEXT", "content": '{"version":"0.2"}'})
    state = {"direct": "old", "files": files if mode == "unchanged" else [], "writes": []}

    class Provider(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_GET(self):
            if self.path == f"/v2/acts/{ACTOR}":
                data = {
                    "id": ACTOR,
                    "versions": [],
                    "taggedBuilds": {"direct": {"buildId": state["direct"]}},
                }
            else:
                build = self.path.split("/")[-1].split("?")[0]
                data = {
                    "id": build,
                    "actId": ACTOR,
                    "status": "FAILED" if mode == "failed" and build == "new" else "SUCCEEDED",
                    "actVersion": {
                        "sourceType": "SOURCE_FILES",
                        "sourceFiles": state["files"] if build == "old" else state["uploaded"],
                    },
                }
                if mode == "wrong_source" and build == "new":
                    data["actVersion"]["sourceFiles"] = []
            self.reply(data)

        def do_POST(self):
            self.mutate()

        def do_PUT(self):
            self.mutate()

        def mutate(self):
            body = json.loads(
                self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}"
            )
            state["writes"].append((self.path, body))
            if "/versions" in self.path:
                state["uploaded"] = body["sourceFiles"]
                self.reply(body)
            elif "/builds" in self.path:
                self.reply({"id": "new", "status": "READY"})
            else:
                state["direct"] = body["taggedBuilds"]["direct"]["buildId"]
                self.reply({"id": ACTOR})

        def reply(self, data):
            payload = json.dumps({"data": data}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        helper = ROOT / "scripts/deploy/apify_deploy.py"
        assert helper.exists(), "versioned worker publication is missing"
        (tmp_path / "apify_deploy.py").write_text(
            helper.read_text().replace("https://api.apify.com", f"http://127.0.0.1:{server.server_port}")
        )
        (tmp_path / ".env").write_text("APIFY_API_TOKEN=fixture-only\n")
        (tmp_path / "worker-source.yaml").write_text(
            f"url: https://api.apify.com/v2/actors/{ACTOR}/run-sync?build=direct&memory=256\n"
        )
        script = (ROOT / "scripts/deploy/deploy.sh").read_text()
        hook = """roll() {
  printf '%s\\n' "$1" >> rolls
  if [ "$1" = "$SHA" ]; then
    case "$MODE" in
      roll_failed) return 1 ;;
      term) kill -TERM $$ ;;
    esac
  fi
}
verify() { return 0; }
docker() { return 0; }
"""
        marker = "previous=$(cat deployed 2>/dev/null || true)"
        script = script.replace(marker, hook + marker)
        (tmp_path / "deploy.sh").write_text(script)
        (tmp_path / "deployed").write_text("b" * 40 + "\n")
        result = subprocess.run(
            ["bash", str(tmp_path / "deploy.sh"), "a" * 40],
            env={**os.environ, "MODE": mode},
            capture_output=True,
            text=True,
            timeout=8,
        )
        success = mode in {"success", "unchanged"}
        assert (result.returncode == 0) == success, result.stdout + result.stderr
        assert state["direct"] == ("new" if mode == "success" else "old")
        assert (tmp_path / "deployed").read_text().strip() == ("a" * 40 if success else "b" * 40)
        if mode == "unchanged":
            assert state["writes"] == []
        if mode in {"failed", "wrong_source"}:
            assert not (tmp_path / "rolls").exists()
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
