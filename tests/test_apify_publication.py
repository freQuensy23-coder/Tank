import json
import os
import shutil
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
ACTOR = "rbf5CQKQgKNVl4SfO"


def worker_journal(committed=False):
    return {
        "version": 1,
        "actor": "/acts/" + ACTOR,
        "tag": "direct",
        "previous": "old",
        "candidate": "new",
        "previous_release": "b" * 40,
        "previous_compose": "worker.previous-compose.yaml",
        "release": "a" * 40,
        "committed": committed,
    }


@pytest.mark.integration
@pytest.mark.parametrize(
    "mode",
    [
        "success",
        "unchanged",
        "failed",
        "wrong_source",
        "roll_failed",
        "term",
        "int",
        "ack_lost",
        "readback_failed",
        "rollback_refused",
        "second",
        "crash_marker",
        "changed_actor",
        "ownership_unreadable",
        "changed_compose",
        "post_finish_term",
        "post_finish_crash",
        "finish_child_failed",
        "finish_child_killed",
        "settlement_503",
        "settlement_disconnected",
        "settlement_marker_mismatch",
        "settlement_ownership_mismatch",
        "post_finish_main_term",
        "post_finish_term_observed",
        "cleanup_selection",
        "cleanup_previous_empty",
        "cleanup_empty",
        "cleanup_listing_failed",
        "cleanup_listing_partial",
        "cleanup_rmi_failed",
        "cleanup_batched",
    ],
)
def test_worker_publication_transaction(tmp_path, mode, request):
    observed = mode == "post_finish_term_observed"
    if observed:
        mode = "post_finish_term"
    source = tmp_path / "worker"
    source.mkdir()
    (source / "main.py").write_text("print('selected revision')\n")
    (source / ".actor").mkdir()
    (source / ".actor/actor.json").write_text('{"version":"0.2"}')
    files = [{"name": "main.py", "format": "TEXT", "content": "print('selected revision')\n"}]
    files.append({"name": ".actor/actor.json", "format": "TEXT", "content": '{"version":"0.2"}'})
    state = {
        "direct": "old",
        "next": "other-old",
        "files": files if mode == "unchanged" else [],
        "writes": [],
        "reads": [],
    }

    class Provider(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_GET(self):
            state["reads"].append(self.path)
            if state.get("settlement_failure"):
                failure = state.pop("settlement_failure")
                if failure == "settlement_disconnected":
                    self.close_connection = True
                else:
                    self.send_error(503)
                return
            if mode == "ownership_unreadable" and state["direct"] == "new":
                self.send_error(503)
                return
            if mode == "readback_failed" and state.get("read_fail"):
                state["read_fail"] = False
                self.send_error(503)
                return
            if self.path.startswith("/v2/acts/"):
                actor = self.path.rsplit("/", 1)[-1]
                tag = "direct" if actor == ACTOR else "next"
                data = {
                    "id": actor,
                    "versions": [],
                    "taggedBuilds": {tag: {"buildId": state[tag]}},
                }
            else:
                build = self.path.split("/")[-1].split("?")[0]
                data = {
                    "id": build,
                    "actId": "otherActor" if build.startswith("other-") else ACTOR,
                    "status": "FAILED" if mode == "failed" and build == "new" else "SUCCEEDED",
                    "actVersion": {
                        "sourceType": "SOURCE_FILES",
                        "sourceFiles": state["files"] if build.endswith("old") else state["uploaded"],
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
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
            state["writes"].append((self.path, body))
            if "/versions" in self.path:
                state["uploaded"] = body["sourceFiles"]
                self.reply(body)
            elif "/builds" in self.path:
                build = "other-new" if "/otherActor/" in self.path else "new"
                self.reply({"id": build, "status": "READY"})
            else:
                tag = next(iter(body["taggedBuilds"]))
                target = body["taggedBuilds"][tag]["buildId"]
                if target == "old" and mode in {"finish_child_failed", "finish_child_killed"}:
                    state.setdefault("rollback_committed", []).append(
                        json.loads((tmp_path / "worker.pending").read_text())["committed"]
                    )
                if target == "old" and mode in {
                    "rollback_refused",
                    "post_finish_term",
                    "post_finish_main_term",
                    "post_finish_crash",
                    "finish_child_failed",
                    "finish_child_killed",
                }:
                    self.send_error(503)
                    return
                state[tag] = target
                if target == "new" and mode == "ack_lost":
                    self.close_connection = True
                    return
                if target == "new" and mode == "readback_failed" and not state.get("injected"):
                    state["read_fail"] = True
                    state["injected"] = True
                self.reply({"id": ACTOR})

        def reply(self, data):
            payload = json.dumps({"data": data}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        helper = ROOT / "scripts/deploy/apify_deploy.py"
        assert helper.exists(), "versioned worker publication is missing"
        (tmp_path / "apify_deploy.py").write_text(
            helper.read_text().replace("https://api.apify.com", f"http://127.0.0.1:{server.server_port}")
        )
        if mode in {"finish_child_failed", "finish_child_killed"}:
            helper_path = tmp_path / "apify_deploy.py"
            failure = (
                "os._exit(1)" if mode == "finish_child_failed" else "os.kill(os.getpid(), signal.SIGKILL)"
            )
            helper_path.write_text(
                helper_path.read_text().replace(
                    '        temporary.replace(PENDING)\n    elif action == "clear":',
                    "        temporary.replace(PENDING)\n"
                    '        Path("finish-committed").write_text(PENDING.read_text())\n'
                    f'        {failure}\n    elif action == "clear":',
                )
            )
        (tmp_path / ".env").write_text(
            "UNRELATED=$(touch injected)\nAPIFY_API_TOKEN='fixture-only' # literal dotenv\n"
        )
        (tmp_path / "worker-source.yaml").write_text(
            f"url: https://api.apify.com/v2/actors/{ACTOR}/run-sync?build=direct&memory=256\n"
        )
        script = (ROOT / "scripts/deploy/deploy.sh").read_text()
        hook = """roll() {
  printf '%s\\n' "$1" >> rolls
  cat compose.yaml >> compose-rolls
  if [ "$1" = "$SHA" ]; then
    case "$MODE" in
      roll_failed|rollback_refused) return 1 ;;
      term) kill -TERM $$ ;;
      int) kill -INT $$ ;;
    esac
  fi
  printf '%s\\n' "$1" > running-release
}
verify() { return 0; }
docker() {
  case "$MODE" in
    post_finish_term) kill -TERM $$ ;;
    post_finish_crash) kill -KILL $$ ;;
  esac
  return 0
}
"""
        marker = "previous=$(cat deployed 2>/dev/null || true)"
        script = script.replace(marker, hook + marker)
        cleanup_mode = mode.startswith("cleanup_")
        selected = []
        if cleanup_mode:
            repository = "ghcr.io/frequensy23-coder/tank/"
            selected = [
                repository + "tank:old",
                repository + "tank-capture:latest",
                repository + "tank:none",
                repository + "tank:<none>",
                repository + "tank:" + "a" * 40 + "0",
                repository + "tank:old",
                "ghcrXio/frequensy23-coder/tank/tank:old",
            ]
            listing = [
                repository + "tank:" + "a" * 40,
                repository + "tank-capture:" + "b" * 40,
                "other.example/tank/tank:old",
                "prefix-" + repository + "tank:old",
                "ghcr.io/frequensy23-coder/tank-other/tank:old",
                *selected,
            ]
            if mode == "cleanup_previous_empty":
                selected.remove(repository + "tank:none")
                selected.insert(0, repository + "tank-capture:" + "b" * 40)
                cleanup = "      docker image ls --format"
                assert script.count(cleanup) == 1
                script = script.replace(cleanup, "      previous=\n" + cleanup)
            elif mode == "cleanup_empty":
                listing = listing[:5]
                selected = []
            elif mode == "cleanup_listing_failed":
                listing = []
                selected = []
            elif mode == "cleanup_batched":
                selected = [repository + f"tank:{index:080d}" for index in range(3000)]
                listing = selected
            (tmp_path / "image-listing").write_text("".join(image + "\n" for image in listing))
            executable = tmp_path / "bin"
            executable.mkdir()
            docker = executable / "docker"
            docker.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "args = sys.argv[1:]\n"
                "with Path('image-commands').open('a') as output:\n"
                "    output.write(json.dumps(args) + '\\n')\n"
                "if args == ['image', 'ls', '--format', '{{.Repository}}:{{.Tag}}']:\n"
                "    sys.stdout.write(Path('image-listing').read_text())\n"
                "    sys.exit(17 if os.environ['MODE'] in "
                "{'cleanup_listing_failed', 'cleanup_listing_partial'} else 0)\n"
                "sys.exit(19 if os.environ['MODE'] == 'cleanup_rmi_failed' else 0)\n"
            )
            docker.chmod(0o755)
            script = script.replace(marker, 'docker() { command docker "$@"; }\n' + marker)
        if observed:
            replacements = {
                "  local status=$1 worker_restored=yes\n": """  local status=$1 worker_restored=yes
  case "$2" in
    term) printf '%s\\n' term_entered >> term-observation ;;
    exit) printf '%s\\n' exit_entered >> term-observation ;;
  esac
  if [ "$BASHPID" = "$$" ]; then
    printf '%s\\n' primary_shell >> term-observation
  fi
  if [ "$status" -eq 143 ]; then
    printf '%s\\n' input_143 >> term-observation
  fi
  if [ "$worker_started" = no ] && [ "$tank_started" = no ]; then
    printf '%s\\n' flags_cleared >> term-observation
  fi
""",
                '  recover "$status"\n': '  recover "$status" term\n',
                "trap 'recover \"$?\"' EXIT": "trap 'recover \"$?\" exit' EXIT",
                '  exit "$status"\n': """  case "$status" in
    143) printf '%s\\n' exit_143_requested >> term-observation ;;
    1) printf '%s\\n' exit_one_requested >> term-observation ;;
    *) printf '%s\\n' exit_other_requested >> term-observation ;;
  esac
  exit "$status"
""",
            }
            for original, replacement in replacements.items():
                assert script.count(original) == 1
                script = script.replace(original, replacement)
        if mode == "post_finish_main_term":
            cleanup = "      docker image ls --format"
            assert script.count(cleanup) == 1
            script = script.replace(cleanup, "      kill -TERM $$\n" + cleanup)
        if mode in {"crash_marker", "changed_actor", "changed_compose"}:
            script = script.replace(
                "      apify finish",
                '      if [ "$MODE" != success ]; then kill -KILL $$; fi\n      apify finish',
            )
        (tmp_path / "deploy.sh").write_text(script)
        (tmp_path / "compose.yaml").write_text("prior compose\n")
        entry = tmp_path / "deploy.sh"
        if mode == "changed_compose":
            stage = tmp_path / "incoming-first"
            stage.mkdir()
            for name in ("deploy.sh", "apify_deploy.py", "worker-source.yaml"):
                shutil.copyfile(tmp_path / name, stage / name)
            shutil.copytree(source, stage / "worker")
            (stage / "compose.yaml").write_text("first candidate compose\n")
            entry = stage / "deploy.sh"
        (tmp_path / "deployed").write_text("b" * 40 + "\n")
        environment = {**os.environ, "MODE": mode}
        if cleanup_mode:
            environment["PATH"] = str(tmp_path / "bin") + os.pathsep + os.environ["PATH"]
        result = subprocess.run(
            ["bash", str(entry), "a" * 40],
            env=environment,
            capture_output=True,
            text=True,
            timeout=8,
        )
        if mode != "post_finish_crash":
            assert list(tmp_path.glob(".deploy-images.*")) == []
        if cleanup_mode:
            commands = [json.loads(line) for line in (tmp_path / "image-commands").read_text().splitlines()]
            assert commands[0] == ["image", "ls", "--format", "{{.Repository}}:{{.Tag}}"]
            assert all(command[0] == "rmi" and len(command) > 1 for command in commands[1:])
            assert [image for command in commands[1:] for image in command[1:]] == selected
            if mode == "cleanup_batched":
                assert len(commands) > 2
            elif selected:
                assert len(commands) == 2
            else:
                assert len(commands) == 1
            assert json.loads((tmp_path / "worker.pending").read_text()) == worker_journal(True)
            assert (tmp_path / "running-release").read_text() == "a" * 40 + "\n"
            assert (tmp_path / "rolls").read_text().splitlines() == ["a" * 40]
            tag_writes = [body for path, body in state["writes"] if path == f"/v2/acts/{ACTOR}"]
            assert tag_writes == [{"taggedBuilds": {"direct": {"buildId": "new"}}}]
        if mode == "post_finish_term":
            request.node.user_properties.extend(
                [
                    (
                        "term_observation_context",
                        "instrumented_sibling" if observed else "original",
                    ),
                    ("term_primary_exit", {143: "143", 1: "one"}.get(result.returncode, "other")),
                ]
            )
            if observed:
                observation = tmp_path / "term-observation"
                events = observation.read_text().splitlines() if observation.exists() else []
                for event in (
                    "term_entered",
                    "exit_entered",
                    "primary_shell",
                    "input_143",
                    "flags_cleared",
                    "exit_143_requested",
                    "exit_one_requested",
                    "exit_other_requested",
                ):
                    request.node.user_properties.append(("term_" + event, int(event in events)))
        success = (
            mode in {"success", "unchanged", "ack_lost", "second"}
            or mode.startswith("settlement_")
            or cleanup_mode
        )
        assert (result.returncode == 0) == success, result.stdout + result.stderr
        remains_new = success and mode != "unchanged"
        remains_new = remains_new or mode in {
            "rollback_refused",
            "crash_marker",
            "changed_actor",
            "ownership_unreadable",
            "changed_compose",
            "post_finish_term",
            "post_finish_main_term",
            "post_finish_crash",
            "finish_child_failed",
            "finish_child_killed",
        }
        assert state["direct"] == ("new" if remains_new else "old")
        assert not (tmp_path / "injected").exists()
        marker_written = success or mode in {
            "crash_marker",
            "changed_actor",
            "changed_compose",
            "post_finish_term",
            "post_finish_main_term",
            "post_finish_crash",
            "finish_child_failed",
            "finish_child_killed",
        }
        assert (tmp_path / "deployed").read_text().strip() == ("a" * 40 if marker_written else "b" * 40)
        if mode == "unchanged":
            assert state["writes"] == []
        if mode in {"failed", "wrong_source"}:
            assert not (tmp_path / "rolls").exists()
        if mode == "rollback_refused":
            assert (tmp_path / "worker.pending").exists()
            assert (tmp_path / "rolls").read_text().splitlines() == ["a" * 40, "b" * 40]
        if mode in {"finish_child_failed", "finish_child_killed"}:
            committed = json.loads((tmp_path / "finish-committed").read_text())
            assert committed["committed"] is True
            pending_path = tmp_path / "worker.pending"
            pending = pending_path.read_bytes()
            assert json.loads(pending)["committed"] is False
            assert (tmp_path / "rolls").read_text().splitlines() == ["a" * 40, "b" * 40]
            assert (tmp_path / "running-release").read_text().strip() == "b" * 40
            retry = subprocess.run(
                ["bash", str(entry), "c" * 40],
                env={**os.environ, "MODE": "success"},
                capture_output=True,
                text=True,
                timeout=8,
            )
            assert retry.returncode != 0
            assert "unfinished deployment" in retry.stderr
            assert "VERIFIED" not in retry.stdout
            assert pending_path.read_bytes() == pending
            assert state["direct"] == "new"
            assert (tmp_path / "running-release").read_text().strip() == "b" * 40
            assert state["rollback_committed"] and not any(state["rollback_committed"])
            assert (tmp_path / "deployed").read_text().strip() == "a" * 40
            assert (tmp_path / "rolls").read_text().splitlines() == ["a" * 40, "b" * 40, "b" * 40]
            tag_writes = [body for path, body in state["writes"] if path == f"/v2/acts/{ACTOR}"]
            assert tag_writes[0] == {"taggedBuilds": {"direct": {"buildId": "new"}}}
            assert all(body == {"taggedBuilds": {"direct": {"buildId": "old"}}} for body in tag_writes[1:])
        if mode == "ownership_unreadable":
            assert (tmp_path / "worker.pending").exists()
            assert not (tmp_path / "rolls").exists()
            tag_writes = [body for path, body in state["writes"] if path == f"/v2/acts/{ACTOR}"]
            assert tag_writes == [{"taggedBuilds": {"direct": {"buildId": "new"}}}]
        if mode in {"crash_marker", "changed_actor", "changed_compose"}:
            pending = json.loads((tmp_path / "worker.pending").read_text())
            assert result.returncode == -9
            assert pending["version"] == 1
            assert pending["committed"] is False
            assert pending["previous_release"] == "b" * 40
            assert pending["release"] == "a" * 40
        if mode == "changed_actor":
            (tmp_path / "worker-source.yaml").write_text(
                "url: https://api.apify.com/v2/actors/otherActor/run-sync?build=next&memory=256\n"
            )
        if mode in {"post_finish_term", "post_finish_crash"}:
            pending = json.loads((tmp_path / "worker.pending").read_text())
            assert pending["committed"] is True
            assert (tmp_path / "rolls").read_text().splitlines() == ["a" * 40]
            assert result.returncode == (143 if mode == "post_finish_term" else -9)
            tag_writes = [body for path, body in state["writes"] if path == f"/v2/acts/{ACTOR}"]
            assert tag_writes == [{"taggedBuilds": {"direct": {"buildId": "new"}}}]
        if mode == "post_finish_main_term":
            assert json.loads((tmp_path / "worker.pending").read_text()) == worker_journal(True)
            assert (tmp_path / "deployed").read_text() == "a" * 40 + "\n"
            assert (tmp_path / "running-release").read_text() == "a" * 40 + "\n"
            assert (tmp_path / "rolls").read_text().splitlines() == ["a" * 40]
            assert result.returncode == 143
            tag_writes = [body for path, body in state["writes"] if path == f"/v2/acts/{ACTOR}"]
            assert tag_writes == [{"taggedBuilds": {"direct": {"buildId": "new"}}}]
        if mode.startswith("settlement_"):
            pending_path = tmp_path / "worker.pending"
            journal = pending_path.read_bytes()
            assert json.loads(journal) == worker_journal(True)
            assert (tmp_path / "deployed").read_text() == "a" * 40 + "\n"
            assert (tmp_path / "running-release").read_text() == "a" * 40 + "\n"
            stage = tmp_path / "incoming"
            stage.mkdir()
            for name in ("deploy.sh", "apify_deploy.py"):
                shutil.copyfile(tmp_path / name, stage / name)
            (stage / "worker").mkdir()
            (stage / "worker/main.py").write_text("incoming worker\n")
            (stage / "worker-source.yaml").write_text(
                "https://api.apify.com/v2/actors/otherActor/run-sync?build=next\n"
            )
            (stage / "compose.yaml").write_text("incoming compose\n")
            if mode in {"settlement_503", "settlement_disconnected"}:
                state["settlement_failure"] = mode
            elif mode == "settlement_marker_mismatch":
                (tmp_path / "deployed").write_text("d" * 40 + "\n")
            else:
                state["direct"] = "third-party"
            entries = set(tmp_path.rglob("*"))
            before = {path: path.read_bytes() for path in entries if path.is_file()}
            writes = list(state["writes"])
            reads = len(state["reads"])
            active = state["direct"]
            retry = subprocess.run(
                ["bash", str(stage / "deploy.sh"), "c" * 40],
                env={**os.environ, "MODE": "success"},
                capture_output=True,
                text=True,
                timeout=8,
            )
            assert retry.returncode == 1, retry.stdout + retry.stderr
            assert "committed deployment settlement refused" in retry.stderr
            assert "fixture-only" not in retry.stdout + retry.stderr
            assert "VERIFIED" not in retry.stdout
            expected_reads = [] if mode == "settlement_marker_mismatch" else [f"/v2/acts/{ACTOR}"]
            assert state["reads"][reads:] == expected_reads
            assert state["writes"] == writes
            assert state["direct"] == active
            assert pending_path.read_bytes() == journal
            assert set(tmp_path.rglob("*")) == entries
            assert {path: path.read_bytes() for path in before} == before
        if mode == "changed_compose":
            stage = tmp_path / "incoming-next"
            stage.mkdir()
            for name in ("deploy.sh", "apify_deploy.py", "worker-source.yaml"):
                shutil.copyfile(tmp_path / name, stage / name)
            shutil.copytree(source, stage / "worker")
            (stage / "compose.yaml").write_text("later candidate compose\n")
            entry = stage / "deploy.sh"
        if mode in {
            "second",
            "readback_failed",
            "crash_marker",
            "changed_actor",
            "changed_compose",
            "post_finish_term",
            "post_finish_crash",
        }:
            writes = len(state["writes"])
            reads = len(state["reads"])
            retry = subprocess.run(
                ["bash", str(entry), "c" * 40],
                env={**os.environ, "MODE": "success"},
                capture_output=True,
                text=True,
                timeout=8,
            )
            assert retry.returncode == 0, retry.stdout + retry.stderr
            assert (tmp_path / "deployed").read_text().strip() == "c" * 40
            if mode == "second":
                assert len(state["writes"]) == writes
            if mode in {"post_finish_term", "post_finish_crash"}:
                assert (tmp_path / "rolls").read_text().splitlines() == ["a" * 40, "c" * 40]
                pending = json.loads((tmp_path / "worker.pending").read_text())
                assert pending["previous_release"] == "a" * 40
                assert len(state["writes"]) == writes
            if mode in {"crash_marker", "changed_actor", "changed_compose"}:
                assert (tmp_path / "rolls").read_text().splitlines() == ["a" * 40, "b" * 40, "c" * 40]
                assert state["writes"][writes] == (
                    f"/v2/acts/{ACTOR}",
                    {"taggedBuilds": {"direct": {"buildId": "old"}}},
                )
                assert state["reads"][reads] == f"/v2/acts/{ACTOR}"
                pending = json.loads((tmp_path / "worker.pending").read_text())
                assert pending["committed"] is True
                assert pending["previous_release"] == "b" * 40
                if mode == "changed_compose":
                    assert (tmp_path / "compose-rolls").read_text().splitlines() == [
                        "first candidate compose",
                        "prior compose",
                        "later candidate compose",
                    ]
                    assert (tmp_path / "worker.previous-compose.yaml").read_text() == "prior compose\n"
                if mode == "changed_actor":
                    assert state["direct"] == "old"
                    assert state["next"] == "other-new"
                    assert pending["actor"] == "/acts/otherActor"
                    assert pending["tag"] == "next"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


@pytest.mark.integration
@pytest.mark.parametrize("action", ["settled", "rollback", "rollback-intent", "shell"])
@pytest.mark.parametrize(
    "damage",
    [
        "malformed_json",
        "array",
        "old_committed",
        "old_uncommitted",
        "committed_string_false",
        "committed_string_true",
        "committed_number",
        "committed_null",
        "version_unknown",
        "version_boolean",
        "version_string",
        "compose_missing",
        "compose_symlink",
        "extra_field",
    ]
    + [f"missing_{key}" for key in worker_journal()]
    + [
        f"invalid_{key}"
        for key in (
            "actor",
            "tag",
            "previous",
            "candidate",
            "previous_release",
            "release",
            "previous_compose",
        )
    ],
)
def test_invalid_worker_journal_refuses_recovery_effects(tmp_path, action, damage):
    requests = []

    class Provider(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_GET(self):
            requests.append((self.command, self.path))
            data = {"id": ACTOR, "taggedBuilds": {"direct": {"buildId": "new"}}}
            payload = json.dumps({"data": data}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_PUT(self):
            requests.append((self.command, self.path))
            self.send_error(503)

        def do_POST(self):
            requests.append((self.command, self.path))
            self.send_error(503)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        stage = tmp_path / "incoming"
        stage.mkdir()
        helper = (ROOT / "scripts/deploy/apify_deploy.py").read_text()
        (stage / "apify_deploy.py").write_text(
            helper.replace("https://api.apify.com", f"http://127.0.0.1:{server.server_port}")
        )
        script = (ROOT / "scripts/deploy/deploy.sh").read_text()
        hook = 'roll() { printf "%s\\n" "$1" >> rolls; }\nverify() { return 0; }\n'
        marker = "previous=$(cat deployed 2>/dev/null || true)"
        (stage / "deploy.sh").write_text(script.replace(marker, hook + marker))
        for root, label in ((tmp_path, "active"), (stage, "incoming")):
            (root / "worker").mkdir()
            (root / "worker/main.py").write_text(label + " worker\n")
            (root / "worker-source.yaml").write_text(
                f"https://api.apify.com/v2/actors/{ACTOR}/run-sync?build=direct\n{label}\n"
            )
            (root / "compose.yaml").write_text(label + " compose\n")
        (tmp_path / ".env").write_text("APIFY_API_TOKEN=fixture-only\n")
        (tmp_path / ".deploy.lock").touch()
        (tmp_path / "deployed").write_text("a" * 40 + "\n")
        compose = tmp_path / "worker.previous-compose.yaml"
        compose.write_text("prior compose\n")
        pending = worker_journal(True)
        if damage in {"old_committed", "old_uncommitted"}:
            pending.pop("version")
            pending.pop("previous_compose")
            pending["committed"] = damage == "old_committed"
        elif damage.startswith("missing_"):
            pending.pop(damage.removeprefix("missing_"))
        elif damage.startswith("invalid_"):
            pending[damage.removeprefix("invalid_")] = "../invalid?secret=fixture-only"
        elif damage.startswith("committed_"):
            pending["committed"] = {
                "committed_string_false": "false",
                "committed_string_true": "true",
                "committed_number": 1,
                "committed_null": None,
            }[damage]
        elif damage.startswith("version_"):
            pending["version"] = {
                "version_unknown": 2,
                "version_boolean": True,
                "version_string": "1",
            }[damage]
        elif damage == "compose_missing":
            compose.unlink()
        elif damage == "compose_symlink":
            compose.unlink()
            compose.symlink_to("compose.yaml")
        elif damage == "extra_field":
            pending["unexpected"] = "fixture-only"
        journal = json.dumps([] if damage == "array" else pending)
        if damage == "malformed_json":
            journal = '{"fixture-only":'
        pending_path = tmp_path / "worker.pending"
        pending_path.write_text(journal)
        entries = set(tmp_path.rglob("*"))
        before = {path: path.read_bytes() for path in entries if path.is_file()}
        command = (
            ["bash", str(stage / "deploy.sh"), "c" * 40]
            if action == "shell"
            else ["python3", str(stage / "apify_deploy.py"), action, "c" * 40]
        )
        result = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=8)
        assert result.returncode != 0
        assert "unsupported or invalid worker journal" in result.stderr
        assert "fixture-only" not in result.stdout + result.stderr
        assert "VERIFIED" not in result.stdout
        assert requests == []
        assert not (tmp_path / "rolls").exists()
        assert pending_path.read_bytes() == journal.encode()
        assert set(tmp_path.rglob("*")) == entries
        assert {path: path.read_bytes() for path in before} == before
        if damage == "compose_symlink":
            assert compose.is_symlink()
            assert compose.readlink() == Path("compose.yaml")
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


@pytest.mark.integration
@pytest.mark.parametrize("phase", ["headers", "body", "chunk"])
@pytest.mark.parametrize(
    ("action", "committed"),
    [("prepare", False), ("rollback", False), ("rollback", True)],
)
def test_worker_http_wall_deadline_disconnects_dripping_peer(tmp_path, phase, action, committed):
    disconnected = threading.Event()
    stopped = threading.Event()
    requests = []

    class Provider(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_GET(self):
            requests.append((self.command, self.path))
            prefixes = {
                "headers": b"HTTP/1.1 200 OK\r\nX-Drip: ",
                "body": b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n",
                "chunk": b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
            }
            try:
                self.connection.sendall(prefixes[phase])
                while not stopped.wait(0.01):
                    self.connection.sendall(b"0" if phase == "chunk" else b" ")
            except OSError:
                disconnected.set()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        helper = (ROOT / "scripts/deploy/apify_deploy.py").read_text()
        helper = helper.replace("https://api.apify.com", f"http://127.0.0.1:{server.server_port}")
        helper = helper.replace('(300 if sys.argv[1] == "prepare" else 20)', "0.25")
        (tmp_path / "apify_deploy.py").write_text(helper)
        (tmp_path / ".env").write_text("APIFY_API_TOKEN=fixture-only\n")
        (tmp_path / "worker-source.yaml").write_text(
            f"https://api.apify.com/v2/actors/{ACTOR}/run-sync?build=direct\n"
        )
        (tmp_path / "worker").mkdir()
        (tmp_path / "worker/main.py").write_text("selected\n")
        journal = json.dumps(worker_journal(committed))
        if action == "rollback":
            (tmp_path / "worker.previous-compose.yaml").write_text("prior compose\n")
            (tmp_path / "worker.pending").write_text(journal)
        entries = set(tmp_path.iterdir())
        started = time.monotonic()
        result = subprocess.run(
            ["python3", str(tmp_path / "apify_deploy.py"), action, "a" * 40],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=3,
        )
        assert time.monotonic() - started < 2
        assert result.returncode == 1
        assert "TimeoutError" in result.stderr
        assert "fixture-only" not in result.stdout + result.stderr
        assert disconnected.wait(1)
        assert requests == [("GET", f"/v2/acts/{ACTOR}")]
        assert set(tmp_path.iterdir()) == entries
        if action == "rollback":
            expected = json.dumps(worker_journal(False)) if committed else journal
            assert (tmp_path / "worker.pending").read_bytes() == expected.encode()
    finally:
        stopped.set()
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
