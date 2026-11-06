import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
ACTOR = "rbf5CQKQgKNVl4SfO"


@pytest.mark.integration
@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize(
    "source_type,actor_path", [("SOURCE_FILES", None), ("SOURCE_FILES", "nested/actor"), ("GIT_REPO", None)]
)
def test_existing_version_settings_survive_source_update(tmp_path, failed, source_type, actor_path):
    settings = {
        "versionNumber": "0.2",
        "sourceType": source_type,
        "envVars": [{"name": "SECRET", "isSecret": True, "valueHash": "fixture-hash"}],
        "buildTag": "configured",
        "applyEnvVarsToBuild": True,
        "actorPath": actor_path,
    }
    state = {"version": dict(settings), "writes": []}

    class Provider(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def reply(self, data):
            payload = json.dumps({"data": data}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path.endswith("/versions/0.2"):
                self.reply(state["version"])
            elif self.path == f"/v2/acts/{ACTOR}":
                self.reply(
                    {
                        "id": ACTOR,
                        "versions": [{"versionNumber": "0.2"}],
                        "taggedBuilds": {"direct": {"buildId": "old"}},
                    }
                )
            else:
                build = self.path.split("/")[-1].split("?")[0]
                self.reply(
                    {
                        "id": build,
                        "actId": ACTOR,
                        "status": "FAILED" if failed and build == "new" else "SUCCEEDED",
                        "actVersion": {
                            "sourceType": "SOURCE_FILES",
                            "sourceFiles": [] if build == "old" else state["version"]["sourceFiles"],
                        },
                    }
                )

        def do_PUT(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            state["writes"].append((self.command, self.path, body))
            state["version"].update(body)
            if (
                any(key in body for key in ("sourceType", "sourceFiles", "tarballUrl"))
                and "actorPath" not in body
            ):
                state["version"].pop("actorPath", None)
            self.reply(state["version"])

        def do_POST(self):
            state["writes"].append((self.command, self.path, None))
            self.reply({"id": "new", "status": "READY"})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        helper = (
            (ROOT / "scripts/deploy/apify_deploy.py")
            .read_text()
            .replace("https://api.apify.com", f"http://127.0.0.1:{server.server_port}")
        )
        (tmp_path / "helper.py").write_text(helper)
        (tmp_path / ".env").write_text("APIFY_API_TOKEN=fixture-only\n")
        (tmp_path / "worker-source.yaml").write_text(
            f"https://api.apify.com/v2/actors/{ACTOR}/run-sync?build=direct\n"
        )
        (tmp_path / "worker/.actor").mkdir(parents=True)
        (tmp_path / "worker/.actor/actor.json").write_text('{"version":"0.2"}')
        (tmp_path / "worker/main.py").write_text("selected\n")
        (tmp_path / "deployed").write_text("b" * 40)
        (tmp_path / "worker.previous-compose.yaml").write_text("previous")
        result = subprocess.run(
            ["python3", "helper.py", "prepare", "a" * 40],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=10,
        )
        unsafe = source_type != "SOURCE_FILES" or actor_path is not None
        assert result.returncode == (1 if failed or unsafe else 0)
        assert "fixture-only" not in result.stdout + result.stderr
        assert all(state["version"].get(key) == value for key, value in settings.items())
        version_writes = [
            body for method, path, body in state["writes"] if method == "PUT" and "/versions/" in path
        ]
        if unsafe:
            assert not state["writes"]
        else:
            assert len(version_writes) == 1
            assert not {"envVars", "buildTag", "applyEnvVarsToBuild"} & version_writes[0].keys()
        assert (tmp_path / "worker.pending").exists() is not (failed or unsafe)
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
