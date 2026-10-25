import base64
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("apify_deploy", ROOT / "scripts/deploy/apify_deploy.py")
assert SPEC is not None and SPEC.loader is not None
deploy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deploy)
SHA = "a" * 40
NEXT_SHA = "b" * 40
BUILD = "A" * 17
OTHER_BUILD = "B" * 17
HISTORICAL_RELEASES = [
    "ec4f1e1ff6785d1e4aa39f13efefef0cfa4390ff",
    "002605350ca05fe3ea52d82dbdf74d169ebc954d",
    "36731eba17d50cdb462bc1fb03b104640ff1f145",
    "8d2dd4431e6dd40afc2328f44a2a175ee912d179",
    "fb66082796bed342116c04710c2bccd921312404",
]


@pytest.fixture
def release(tmp_path):
    path = tmp_path / "release"
    worker = path / "worker"
    worker.mkdir(parents=True)
    for name in ["main.py", "direct_fetch.py", "public_profile.py"]:
        (worker / name).write_text("value = 1\n")
    (worker / "requirements.txt").write_text("apify==4.1.0\n")
    (worker / "Dockerfile").write_text('FROM python:3.13.13-slim\nCMD ["python", "main.py"]\n')
    (worker / ".actor").mkdir()
    (worker / ".actor/actor.json").write_text('{"actorSpecification":1,"name":"tank-x-notifier"}')
    manifest = {
        "release_sha": SHA,
        "source_digest": deploy.digest(deploy.source_files(worker)),
        "contract": deploy.CONTRACT,
        "actor_id": deploy.ACTOR_ID,
    }
    (path / "manifest.json").write_bytes(deploy.canonical(manifest))
    return path


@pytest.mark.parametrize("mode", [0o644, 0o660, 0o400])
def test_host_secret_permissions_fail_closed(tmp_path, mode):
    path = tmp_path / ".env"
    path.write_text("APIFY_API_TOKEN=synthetic\n")
    path.chmod(mode)
    with pytest.raises(deploy.DeployError, match="permissions"):
        deploy.host_token(path)


def test_host_secret_owner_and_symlink_fail_closed(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("APIFY_API_TOKEN=synthetic\n")
    path.chmod(0o600)
    monkeypatch.setattr(deploy.os, "geteuid", lambda: path.stat().st_uid + 1)
    with pytest.raises(deploy.DeployError, match="owner"):
        deploy.host_token(path)
    monkeypatch.undo()
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(deploy.DeployError, match="type"):
        deploy.host_token(link)


@pytest.mark.parametrize("value", ["synthetic_token-123", "'synthetic_token-123'", '"synthetic_token-123"'])
def test_only_apify_token_is_read_from_single_host_store(tmp_path, value):
    path = tmp_path / ".env"
    path.write_text("UNRELATED_SECRET=never-copy\nAPIFY_API_TOKEN=" + value + "\n")
    path.chmod(0o600)
    assert deploy.host_token(path) == "synthetic_token-123"


@pytest.mark.parametrize(
    "contents", ["", "APIFY_API_TOKEN=a\nAPIFY_API_TOKEN=b", "APIFY_API_TOKEN=$(command)"]
)
def test_missing_ambiguous_or_shell_token_is_refused(tmp_path, contents):
    path = tmp_path / ".env"
    path.write_text(contents)
    path.chmod(0o600)
    with pytest.raises(deploy.DeployError):
        deploy.host_token(path)


def test_source_digest_includes_paths_bytes_and_build_manifest(release):
    files = deploy.source_files(release / "worker")
    baseline = deploy.digest(files)
    assert deploy.digest(dict(reversed(list(files.items())))) == baseline
    assert deploy.digest({**files, "Dockerfile": b"different"}) != baseline
    renamed = dict(files)
    renamed["renamed.py"] = renamed.pop("main.py")
    assert deploy.digest(renamed) != baseline


def test_journal_replacement_and_removal_sync_parent_after_effect(tmp_path, monkeypatch):
    instance = deploy.Publisher(None, tmp_path, tmp_path / "apify-state", SHA)
    instance.write("pending.json", {"old": True})
    events = []
    original = deploy.os.fsync

    def sync(descriptor):
        original(descriptor)
        events.append((stat.S_ISDIR(os.fstat(descriptor).st_mode), instance.read("pending.json")))

    monkeypatch.setattr(deploy.os, "fsync", sync)
    instance.write("pending.json", {"new": True})
    instance.remove("pending.json")
    assert events == [(False, {"old": True}), (True, {"new": True}), (True, None)]
    assert not list(instance.state.glob("*.tmp"))


@pytest.mark.parametrize("change", ["permissions", "replacement"])
def test_invalid_staging_is_not_published_or_unowned_staging_removed(tmp_path, monkeypatch, change):
    instance = deploy.Publisher(None, tmp_path, tmp_path / "apify-state", SHA)
    instance.write("active.json", {"old": True})
    witness = tmp_path / "witness"
    witness.write_text("untouched")
    original = deploy.os.fsync
    staging = []

    def sync(descriptor):
        original(descriptor)
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
            staging.append(path)
            if change == "permissions":
                path.chmod(0o644)
            else:
                path.unlink()
                path.symlink_to(witness)

    monkeypatch.setattr(deploy.os, "fsync", sync)
    with pytest.raises(deploy.DeployError):
        instance.write("active.json", {"new": True})
    assert instance.read("active.json") == {"old": True}
    assert witness.read_text() == "untouched"
    assert len(staging) == 1
    if change == "replacement":
        assert staging[0].is_symlink()
    else:
        assert not staging[0].exists()


class ApifyStand:
    def __init__(self):
        self.direct = deploy.BASELINE_BUILD
        self.versions = {}
        self.builds = {
            deploy.BASELINE_BUILD: {
                "id": deploy.BASELINE_BUILD,
                "actId": deploy.ACTOR_ID,
                "status": "SUCCEEDED",
                "buildNumber": "0.2.2",
                "actVersion": {
                    "sourceType": "SOURCE_FILES",
                    "versionNumber": "0.2",
                    "buildTag": "direct",
                    "sourceFiles": [{"name": "main.py", "format": "TEXT", "content": "legacy fixture\n"}],
                },
            }
        }
        self.requests = []
        self.fail_build = False
        self.fail_upload = False
        self.activation = "ok"
        self.fail_rollback = False
        self.snapshot_tamper = False
        self.poll_status = None

    def handle(self, method, path, body):
        self.requests.append((method, path, body))
        actor = "/v2/actors/" + deploy.ACTOR_ID
        route = urlsplit(path).path
        if route == actor:
            if method == "PUT":
                target = body["taggedBuilds"]["direct"]["buildId"]
                if target == deploy.BASELINE_BUILD and self.fail_rollback:
                    return 500, {"error": {"message": "synthetic failure"}}
                if self.activation != "ignore":
                    self.direct = target
                if self.activation == "uncertain" and target != deploy.BASELINE_BUILD:
                    return 500, {"error": {"message": "synthetic failure"}}
            return 200, {
                "data": {
                    "id": deploy.ACTOR_ID,
                    "taggedBuilds": {"direct": {"buildId": self.direct}},
                    "versions": list(self.versions.values()),
                }
            }
        if route == actor + "/versions" or route == actor + "/versions/" + deploy.VERSION:
            if method in {"PUT", "POST"}:
                if self.fail_upload:
                    return 403, {"error": {"message": "must not log synthetic-token"}}
                self.versions[deploy.VERSION] = body
            return 200, {"data": self.versions[deploy.VERSION]}
        if route == actor + "/builds" and method == "POST":
            build_id = BUILD if BUILD not in self.builds else OTHER_BUILD
            snapshot = json.loads(json.dumps(self.versions[deploy.VERSION]))
            if self.snapshot_tamper:
                snapshot["sourceFiles"][0]["content"] = base64.b64encode(b"tampered").decode()
            self.builds[build_id] = {
                "id": build_id,
                "actId": deploy.ACTOR_ID,
                "status": "FAILED" if self.fail_build else "SUCCEEDED",
                "actVersion": snapshot,
            }
            response = dict(self.builds[build_id])
            if self.poll_status:
                response["status"] = "RUNNING"
            return 201, {"data": response}
        if route.startswith("/v2/actor-builds/"):
            build_id = route.rsplit("/", 1)[1]
            if "waitForFinish" in path and self.poll_status:
                return 200, {"data": {**self.builds[build_id], "status": self.poll_status}}
            return 200, {"data": self.builds[build_id]}
        return 404, {"error": {"message": "unknown route"}}


@pytest.fixture
def apify_stand(monkeypatch):
    stand = ApifyStand()
    monkeypatch.setattr(deploy, "BASELINE_DIGEST", deploy.digest({"main.py": b"legacy fixture\n"}))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def do_PUT(self):
            self.respond()

        def respond(self):
            assert self.headers["Authorization"] == "Bearer synthetic-token"
            assert "token=" not in self.path
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"null")
            code, value = stand.handle(self.command, self.path, body)
            encoded = json.dumps(value).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    stand.base = f"http://127.0.0.1:{server.server_port}/v2"
    try:
        yield stand
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
        assert not thread.is_alive()


def publisher(stand, release, tmp_path, sha=SHA):
    return deploy.Publisher(deploy.Api("synthetic-token", stand.base), release, tmp_path / "apify-state", sha)


def finish_release(instance, tmp_path):
    instance.activate()
    instance.commit()
    (tmp_path / "deployed").write_text(instance.sha + "\n")
    instance.finish()


@pytest.mark.integration
def test_changed_worker_publishes_verifies_then_activates_and_reuses_unchanged(
    apify_stand, release, tmp_path
):
    instance = publisher(apify_stand, release, tmp_path)
    instance.prepare()
    assert apify_stand.direct == deploy.BASELINE_BUILD
    assert instance.read("active.json") is None
    assert instance.read("pending.json")["candidate"]["release_sha"] == SHA
    version = apify_stand.versions[deploy.VERSION]
    assert version["buildTag"] is None and version["envVars"] == []
    assert not version["applyEnvVarsToBuild"]
    finish_release(instance, tmp_path)
    assert apify_stand.direct == BUILD
    assert instance.read("active.json")["release_sha"] == SHA
    assert instance.read("pending.json") is None
    assert stat.S_IMODE((tmp_path / "apify-state/active.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "apify-state").stat().st_mode) == 0o700
    manifest = json.loads((release / "manifest.json").read_bytes())
    manifest["release_sha"] = NEXT_SHA
    (release / "manifest.json").write_bytes(deploy.canonical(manifest))
    newer = publisher(apify_stand, release, tmp_path, NEXT_SHA)
    newer.prepare()
    finish_release(newer, tmp_path)
    assert newer.read("active.json")["release_sha"] == NEXT_SHA
    assert newer.read("active.json")["build"]["release_sha"] == SHA
    assert len([item for item in apify_stand.requests if item[0] == "POST" and "/builds?" in item[1]]) == 1
    assert (
        len([item for item in apify_stand.requests if item[0] == "PUT" and item[1].endswith(deploy.ACTOR_ID)])
        == 1
    )


@pytest.mark.integration
@pytest.mark.parametrize("failure", ["fail_upload", "fail_build", "snapshot_tamper"])
def test_publication_errors_do_not_activate_or_commit(apify_stand, release, tmp_path, failure):
    setattr(apify_stand, failure, True)
    instance = publisher(apify_stand, release, tmp_path)
    with pytest.raises((deploy.DeployError, ValueError)) as error:
        instance.prepare()
    assert "synthetic-token" not in str(error.value)
    assert apify_stand.direct == deploy.BASELINE_BUILD
    assert instance.read("active.json") is None
    assert instance.read("pending.json") is None


@pytest.mark.integration
@pytest.mark.parametrize("failure", ["ignore", "uncertain"])
def test_failed_tag_readback_or_uncertain_update_restores_previous_build(
    apify_stand, release, tmp_path, failure
):
    instance = publisher(apify_stand, release, tmp_path)
    instance.prepare()
    apify_stand.activation = failure
    with pytest.raises(deploy.DeployError):
        instance.activate()
    assert instance.read("pending.json")["activation_attempted"]
    apify_stand.activation = "ok"
    instance.rollback()
    instance.rollback_finish()
    assert apify_stand.direct == deploy.BASELINE_BUILD
    assert instance.read("pending.json") is None
    assert instance.read("active.json") is None


@pytest.mark.integration
def test_build_poll_failure_is_not_treated_as_success(apify_stand, release, tmp_path):
    apify_stand.poll_status = "TIMED-OUT"
    with pytest.raises(deploy.DeployError, match="failed"):
        publisher(apify_stand, release, tmp_path).prepare()
    assert apify_stand.direct == deploy.BASELINE_BUILD
    assert any("waitForFinish=60" in path for _, path, _ in apify_stand.requests)


@pytest.mark.integration
def test_build_deadline_cannot_activate_incomplete_build(apify_stand, release, tmp_path):
    apify_stand.poll_status = "RUNNING"
    instance = publisher(apify_stand, release, tmp_path)
    times = iter([0, 601])
    instance.clock = lambda: next(times)
    with pytest.raises(deploy.DeployError, match="deadline"):
        instance.prepare()
    assert apify_stand.direct == deploy.BASELINE_BUILD
    assert instance.read("pending.json") is None


@pytest.mark.integration
def test_worker_rollback_failure_retains_recovery_transaction(apify_stand, release, tmp_path):
    instance = publisher(apify_stand, release, tmp_path)
    instance.prepare()
    instance.activate()
    apify_stand.fail_rollback = True
    with pytest.raises(deploy.DeployError):
        instance.rollback()
    assert apify_stand.direct == BUILD
    assert instance.read("pending.json")["previous"] == deploy.BASELINE_BUILD
    with pytest.raises(deploy.DeployError, match="unfinished"):
        instance.prepare()


@pytest.mark.integration
def test_committed_release_can_rollback_before_tank_marker_finishes(apify_stand, release, tmp_path):
    instance = publisher(apify_stand, release, tmp_path)
    instance.prepare()
    finish_release(instance, tmp_path)
    original_active = instance.read("active.json")
    (release / "worker/main.py").write_text("value = 2\n")
    manifest = json.loads((release / "manifest.json").read_bytes())
    manifest.update(
        release_sha=NEXT_SHA, source_digest=deploy.digest(deploy.source_files(release / "worker"))
    )
    (release / "manifest.json").write_bytes(deploy.canonical(manifest))
    newer = publisher(apify_stand, release, tmp_path, NEXT_SHA)
    newer.prepare()
    newer.activate()
    newer.commit()
    assert apify_stand.direct == OTHER_BUILD
    with pytest.raises(deploy.DeployError, match="Tank release marker"):
        newer.finish()
    newer.rollback()
    newer.rollback_finish()
    assert apify_stand.direct == BUILD
    assert newer.read("active.json") == original_active
    assert (tmp_path / "deployed").read_text().strip() == SHA


@pytest.mark.integration
@pytest.mark.parametrize("change", ["artifact", "active_tag", "cached_build", "actor_identity", "contract"])
def test_provenance_drift_fails_closed(apify_stand, release, tmp_path, change):
    instance = publisher(apify_stand, release, tmp_path)
    instance.prepare()
    finish_release(instance, tmp_path)
    if change == "artifact":
        (release / "worker/Dockerfile").write_text("tampered")
    elif change == "active_tag":
        apify_stand.direct = OTHER_BUILD
    elif change == "cached_build":
        apify_stand.builds[BUILD]["actVersion"]["sourceFiles"][0]["content"] = "dGFtcGVyZWQ="
    elif change == "actor_identity":
        apify_stand.builds[BUILD]["actId"] = "wrong-actor"
    else:
        snapshot = apify_stand.builds[BUILD]["actVersion"]["sourceFiles"]
        marker = next(item for item in snapshot if item["name"] == deploy.MARKER)
        value = json.loads(base64.b64decode(marker["content"]))
        value["contract"] = "incompatible"
        marker["content"] = base64.b64encode(deploy.canonical(value)).decode()
    with pytest.raises(deploy.DeployError):
        instance.prepare()
    assert instance.read("pending.json") is None


@pytest.mark.integration
@pytest.mark.process
def test_package_uses_selected_git_bytes_not_untracked_or_dirty_source(release, tmp_path):
    repository = tmp_path / "repository"
    repository.mkdir()
    shutil.copytree(release / "worker", repository / "apify/x-reader")
    (repository / "scripts/deploy").mkdir(parents=True)
    shutil.copy(ROOT / "scripts/deploy/apify_deploy.py", repository / "scripts/deploy/apify_deploy.py")
    commands = [
        ["git", "init", "-q"],
        ["git", "config", "user.email", "ci@example.invalid"],
        ["git", "config", "user.name", "CI"],
        ["git", "add", "."],
        ["git", "commit", "-qm", "fixture"],
    ]
    for command in commands:
        subprocess.run(command, cwd=repository, check=True, capture_output=True)
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    (repository / "apify/x-reader/main.py").write_text("dirty\n")
    (repository / "apify/x-reader/untracked.env").write_text("secret-like-test-data")
    destination = tmp_path / "artifact"
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/deploy/apify_deploy.py"), "package", sha, str(destination)],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    assert (destination / "worker/main.py").read_text() == "value = 1\n"
    assert not (destination / "worker/untracked.env").exists()
    manifest = json.loads((destination / "manifest.json").read_bytes())
    assert manifest["release_sha"] == sha
    assert manifest["source_digest"] == deploy.digest(deploy.source_files(destination / "worker"))


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.parametrize(
    "rollback_outcome", ["success", "tank_failure", "worker_failure", "interrupted_after_marker"]
)
def test_actual_deploy_shell_restores_actor_before_tank_rollback(
    apify_stand, release, tmp_path, rollback_outcome
):
    host = tmp_path / "host"
    host.mkdir()
    shutil.copy(ROOT / "scripts/deploy/deploy.sh", host / "deploy.sh")
    shutil.copytree(release, host / "releases" / SHA)
    (host / "deployed").write_text(NEXT_SHA + "\n")
    if rollback_outcome == "interrupted_after_marker":
        instance = publisher(apify_stand, host / "releases" / SHA, host)
        instance.prepare()
        instance.activate()
        instance.commit()
        (host / "deployed").write_text(SHA + "\n")
    apify_stand.fail_rollback = rollback_outcome == "worker_failure"
    binary = tmp_path / "bin"
    binary.mkdir()
    wrapper = tmp_path / "publisher_wrapper.py"
    wrapper.write_text(
        "import importlib.util, pathlib, sys\n"
        f"spec = importlib.util.spec_from_file_location('publisher', {str(ROOT / 'scripts/deploy/apify_deploy.py')!r})\n"
        "module = importlib.util.module_from_spec(spec)\nspec.loader.exec_module(module)\n"
        f"module.BASELINE_DIGEST = {deploy.BASELINE_DIGEST!r}\n"
        f"api = module.Api('synthetic-token', {apify_stand.base!r})\n"
        "publisher = module.Publisher(api, pathlib.Path(sys.argv[4]), pathlib.Path('apify-state'), sys.argv[3])\n"
        "getattr(publisher, sys.argv[2])()\n"
    )
    (binary / "python3").write_text(f'#!/bin/bash\nexec {sys.executable} {wrapper} "$@"\n')
    docker = binary / "docker"
    docker.write_text(
        '#!/bin/bash\nprintf \'%s %s\\n\' "$TANK_VERSION" "$*" >> docker-events\n'
        f'if [[ "$*" == *\'pull -q\'* && "$TANK_VERSION" == {SHA} ]]; then exit 1; fi\n'
        + ("if [[ \"$*\" == *'pull -q'* ]]; then exit 1; fi\n" if rollback_outcome == "tank_failure" else "")
        + 'if [[ "$1" == inspect ]]; then if [[ "$*" == *State.Pid* ]]; then echo 42; else echo healthy; fi; fi\n'
        'if [[ "$1" == ps && -f capture-created ]]; then echo capture-new; fi\n'
        'if [[ "$*" == *--scale* ]]; then touch capture-created; fi\nexit 0\n'
    )
    (binary / "nsenter").write_text("#!/bin/bash\nexit 0\n")
    for path in binary.iterdir():
        path.chmod(0o755)
    result = subprocess.run(
        ["bash", str(host / "deploy.sh"), SHA],
        cwd=host,
        env={**os.environ, "PATH": str(binary) + os.pathsep + os.environ["PATH"]},
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 1
    if rollback_outcome == "tank_failure":
        assert "Tank rollback failed" in result.stdout
    elif rollback_outcome == "worker_failure":
        assert "Tank rollback refused" in result.stdout
    else:
        assert f"rolled back to {NEXT_SHA}" in result.stdout
    assert "VERIFIED" not in result.stdout
    assert apify_stand.direct == (BUILD if rollback_outcome == "worker_failure" else deploy.BASELINE_BUILD)
    events = (host / "docker-events").read_text().splitlines()
    pulls = [event for event in events if event.endswith("pull -q")]
    expected_pulls = (
        [] if rollback_outcome == "interrupted_after_marker" else [f"{SHA} compose -f compose.yaml pull -q"]
    )
    if rollback_outcome != "worker_failure":
        expected_pulls.append(f"{NEXT_SHA} compose -f compose.yaml pull -q")
    assert pulls == expected_pulls
    assert (host / "deployed").read_text().strip() == NEXT_SHA
    if rollback_outcome in {"tank_failure", "worker_failure"}:
        pending = json.loads((host / "apify-state/pending.json").read_bytes())
        assert pending.get("rolled_back", False) == (rollback_outcome == "tank_failure")
        assert pending["previous_release_sha"] == NEXT_SHA
    else:
        assert not (host / "apify-state/pending.json").exists()
    tag_updates = [
        body["taggedBuilds"]["direct"]["buildId"]
        for method, path, body in apify_stand.requests
        if method == "PUT" and path.endswith(deploy.ACTOR_ID)
    ]
    assert tag_updates == [BUILD, deploy.BASELINE_BUILD]


def shell_host(tmp_path, stand, artifact, sha=SHA):
    host = tmp_path / "host"
    host.mkdir()
    shutil.copy(ROOT / "scripts/deploy/deploy.sh", host / "deploy.sh")
    shutil.copytree(artifact, host / "releases" / sha)
    shutil.copy(ROOT / "scripts/deploy/apify_deploy.py", host / "releases" / sha / "apify_deploy.py")
    binary = tmp_path / "bin"
    binary.mkdir()
    wrapper = tmp_path / "publisher_wrapper.py"
    wrapper.write_text(
        "import importlib.util, os, pathlib, stat, sys\n"
        "spec = importlib.util.spec_from_file_location('publisher', sys.argv[1])\n"
        "module = importlib.util.module_from_spec(spec)\nspec.loader.exec_module(module)\n"
        f"module.BASELINE_DIGEST = {deploy.BASELINE_DIGEST!r}\n"
        f"api = module.Api('synthetic-token', {stand.base!r})\n"
        "publisher = module.Publisher(api, pathlib.Path(sys.argv[4]), pathlib.Path('apify-state'), sys.argv[3])\n"
        "if os.environ.get('INTERRUPT_JOURNAL'):\n"
        "    original = module.os.fsync\n"
        "    def interrupted(descriptor):\n"
        "        original(descriptor)\n"
        "        if stat.S_ISREG(os.fstat(descriptor).st_mode): os._exit(73)\n"
        "    module.os.fsync = interrupted\n"
        "getattr(publisher, sys.argv[2])()\n"
    )
    (binary / "python3").write_text(f'#!/bin/bash\nexec {sys.executable} {wrapper} "$@"\n')
    (binary / "docker").write_text(
        '#!/bin/bash\nprintf \'%s %s\\n\' "$TANK_VERSION" "$*" >> docker-events\n'
        f'if [[ "$*" == *\'pull -q\'* && "$TANK_VERSION" == {sha} ]]; then exit 1; fi\n'
        'if [[ "$1" == inspect ]]; then if [[ "$*" == *State.Pid* ]]; then echo 42; else echo healthy; fi; fi\n'
        'if [[ "$1" == ps && -f capture-created ]]; then echo capture-new; fi\n'
        'if [[ "$*" == *--scale* ]]; then touch capture-created; fi\nexit 0\n'
    )
    (binary / "nsenter").write_text("#!/bin/bash\nexit 0\n")
    for path in binary.iterdir():
        path.chmod(0o755)
    environment = {**os.environ, "PATH": str(binary) + os.pathsep + os.environ["PATH"]}
    return host, environment


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.parametrize("journal", ["pending", "active", "cache"])
@pytest.mark.parametrize("retry_sha", [SHA, "c" * 40])
def test_deploy_retry_recovers_process_death_before_journal_rename(
    apify_stand, release, tmp_path, journal, retry_sha
):
    host, environment = shell_host(tmp_path, apify_stand, release, sha=retry_sha)
    artifact = host / "releases" / retry_sha
    manifest = json.loads((artifact / "manifest.json").read_bytes())
    manifest["release_sha"] = NEXT_SHA
    (artifact / "manifest.json").write_bytes(deploy.canonical(manifest))
    original = publisher(apify_stand, artifact, host, NEXT_SHA)
    original.prepare()
    finish_release(original, host)
    active = original.read("active.json")
    (artifact / "worker/main.py").write_text("value = 2\n")
    manifest.update(release_sha=SHA, source_digest=deploy.digest(deploy.source_files(artifact / "worker")))
    (artifact / "manifest.json").write_bytes(deploy.canonical(manifest))
    newer = publisher(apify_stand, artifact, host)
    if journal != "cache":
        newer.prepare()
    if journal == "active":
        newer.activate()
    action = {"cache": "prepare", "pending": "activate", "active": "commit"}[journal]
    interrupted = subprocess.run(
        ["python3", str(artifact / "apify_deploy.py"), action, SHA, str(artifact)],
        cwd=host,
        env={**environment, "INTERRUPT_JOURNAL": "yes"},
        capture_output=True,
        timeout=10,
    )
    assert interrupted.returncode == 73
    staging = list(newer.state.glob("*.tmp"))
    assert len(staging) == 1
    abandoned = staging[0].read_bytes()
    assert abandoned
    fixed_staging = newer.state / (
        manifest["source_digest"] + ".tmp" if journal == "cache" else journal + ".tmp"
    )
    fixed_staging.write_text("abandoned old publisher staging")
    fixed_staging.chmod(0o600)
    manifest["release_sha"] = retry_sha
    (artifact / "manifest.json").write_bytes(deploy.canonical(manifest))
    result = subprocess.run(
        ["bash", str(host / "deploy.sh"), retry_sha],
        cwd=host,
        env=environment,
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 1
    assert f"rolled back to {NEXT_SHA}" in result.stdout
    assert "worker rollback failed" not in result.stdout
    assert apify_stand.direct == BUILD
    assert newer.read("active.json") == active
    assert newer.read("pending.json") is None
    assert (host / "deployed").read_text().strip() == NEXT_SHA
    assert staging[0].read_bytes() == abandoned
    assert fixed_staging.read_text() == "abandoned old publisher staging"
    if journal == "cache":
        assert newer.read(manifest["source_digest"] + ".json")["build_id"] == OTHER_BUILD


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.parametrize("sha", HISTORICAL_RELEASES)
def test_workflow_packages_historical_main_with_current_orchestrator(tmp_path, sha):
    import yaml

    workflow = yaml.safe_load((ROOT / ".github/workflows/deploy.yml").read_text())
    steps = workflow["jobs"]["deploy"]["steps"]
    target = next(step for step in steps if step.get("id") == "target")
    package_step = next(step for step in steps if step.get("name", "").startswith("Package the selected"))
    production_step = next(step for step in steps if step.get("name") == "Deploy on the production host")
    repository = tmp_path / "repository"
    subprocess.run(
        ["git", "clone", "--shared", "--no-checkout", str(ROOT), str(repository)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "checkout", "-q", "--detach", "HEAD"], cwd=repository, check=True, capture_output=True
    )
    subprocess.run(
        ["git", "update-ref", "refs/remotes/origin/main", sha],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    runner = tmp_path / "runner"
    runner.mkdir()
    output = tmp_path / "output"
    environment = {**os.environ, "INPUT_SHA": sha, "RUNNER_TEMP": str(runner), "GITHUB_OUTPUT": str(output)}
    subprocess.run(
        ["bash", "-euo", "pipefail", "-c", target["run"]],
        cwd=repository,
        env=environment,
        check=True,
        capture_output=True,
    )
    assert output.read_text() == f"sha={sha}\n"
    assert not (repository / "scripts/deploy/apify_deploy.py").exists()
    assert not (repository / "apify/x-reader").exists()
    subprocess.run(
        ["bash", "-euo", "pipefail", "-c", package_step["run"]],
        cwd=repository,
        env={**environment, "SHA": sha},
        check=True,
        capture_output=True,
    )
    artifact = runner / sha
    manifest = json.loads((artifact / "manifest.json").read_bytes())
    assert manifest["release_sha"] == sha
    assert manifest["contract"] == deploy.LEGACY_CONTRACT
    assert manifest["worker_mode"] == ("unused" if sha == HISTORICAL_RELEASES[-1] else "legacy")
    assert manifest["build_id"] == deploy.BASELINE_BUILD
    assert manifest["source_digest"] == deploy.BASELINE_DIGEST
    assert not (artifact / "worker").exists()
    assert (artifact / "apify_deploy.py").read_bytes() == (
        ROOT / "scripts/deploy/apify_deploy.py"
    ).read_bytes()
    assert (runner / "tank-orchestrator/deploy.sh").read_bytes() == (
        ROOT / "scripts/deploy/deploy.sh"
    ).read_bytes()
    assert 'compose.yaml "$RUNNER_TEMP/tank-orchestrator/deploy.sh"' in production_step["run"]
    with (repository / "config/sources.yaml").open("a") as stream:
        stream.write("\nunreviewed: changed\n")
    subprocess.run(["git", "add", "config/sources.yaml"], cwd=repository, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=CI",
            "-c",
            "user.email=ci@example.invalid",
            "commit",
            "-qm",
            "unreviewed compatibility",
        ],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    changed = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    rejected = subprocess.run(
        [
            sys.executable,
            str(runner / "tank-orchestrator/apify_deploy.py"),
            "package",
            changed,
            str(runner / changed),
        ],
        cwd=repository,
        text=True,
        capture_output=True,
    )
    assert rejected.returncode != 0
    assert "historical worker compatibility not verified" in rejected.stderr
    assert not (runner / changed).exists()


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["legacy", "unused"])
def test_historical_worker_selection_never_rebuilds_or_relabels_source(apify_stand, release, tmp_path, mode):
    current = publisher(apify_stand, release, tmp_path)
    current.prepare()
    finish_release(current, tmp_path)
    original = current.read("active.json")
    historical = tmp_path / "historical"
    historical.mkdir()
    compatibility = next(key for key, value in deploy.LEGACY_INPUTS.items() if value == mode)
    (historical / "manifest.json").write_bytes(
        deploy.canonical(
            {
                "release_sha": NEXT_SHA,
                "actor_id": deploy.ACTOR_ID,
                "contract": deploy.LEGACY_CONTRACT,
                "worker_mode": mode,
                "compatibility_digest": compatibility,
                "source_digest": deploy.BASELINE_DIGEST,
                "build_id": deploy.BASELINE_BUILD,
            }
        )
    )
    instance = publisher(apify_stand, historical, tmp_path, NEXT_SHA)
    instance.prepare()
    instance.activate()
    instance.commit()
    instance.rollback()
    instance.rollback_finish()
    assert instance.read("active.json") == original
    assert apify_stand.direct == BUILD
    assert (tmp_path / "deployed").read_text().strip() == SHA
    instance.prepare()
    finish_release(instance, tmp_path)
    expected_build = deploy.BASELINE_BUILD if mode == "legacy" else BUILD
    assert apify_stand.direct == expected_build
    active = instance.read("active.json")
    assert active["release_sha"] == NEXT_SHA
    assert active["build_id"] == expected_build
    if mode == "legacy":
        assert "release_sha" not in active["build"]
        assert active["build"]["build_number"] == "0.2.2"
    else:
        assert active["build"] == original["build"]
    assert len([item for item in apify_stand.requests if item[0] == "POST" and "/builds?" in item[1]]) == 1
    instance.prepare()
    instance.activate()
    instance.rollback()
    instance.rollback_finish()
    assert instance.read("active.json") == active
    assert apify_stand.direct == expected_build


@pytest.mark.integration
@pytest.mark.parametrize("change", ["source", "version", "build_number", "status", "actor"])
def test_legacy_build_drift_blocks_bootstrap_and_rollback(apify_stand, release, tmp_path, change):
    instance = publisher(apify_stand, release, tmp_path)
    instance.prepare()
    instance.activate()
    baseline = apify_stand.builds[deploy.BASELINE_BUILD]
    if change == "source":
        baseline["actVersion"]["sourceFiles"][0]["content"] = "changed"
    elif change == "version":
        baseline["actVersion"]["versionNumber"] = "9.9"
    elif change == "build_number":
        baseline["buildNumber"] = "0.2.3"
    elif change == "status":
        baseline["status"] = "FAILED"
    else:
        baseline["actId"] = "unrelated"
    with pytest.raises(deploy.DeployError):
        instance.rollback()
    assert apify_stand.direct == BUILD
    assert instance.read("pending.json")["activation_attempted"]
    assert not instance.read("pending.json").get("rolled_back")
    (tmp_path / "separate").mkdir()
    separate = publisher(apify_stand, release, tmp_path / "separate")
    apify_stand.direct = deploy.BASELINE_BUILD
    with pytest.raises(deploy.DeployError):
        separate.prepare()
    assert separate.read("pending.json") is None
