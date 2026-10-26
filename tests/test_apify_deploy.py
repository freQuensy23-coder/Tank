import base64
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from publisher_http_support import (
    publisher_http_peer,
    publisher_upload_failure_peer,
    publisher_wrong_actor_peer,
)
from publisher_state_support import BuildState, rig, source_artifact
from publisher_transition_support import HTTPPublisherRig, http_rig, write_artifact
from pydantic import SecretStr

from tank.deployment.api_contracts import Base64SourceFile, SourceSnapshot
from tank.deployment.apify_api import ApifyAPI, ApifyAPIError, ApifyEndpoint
from tank.deployment.artifact_contracts import SourceArtifact, SourceBundle
from tank.deployment.artifacts import ArtifactError, FilesystemArtifacts, source_files
from tank.deployment.contracts import ActiveRelease, SourceBuild, SourceRelease
from tank.deployment.credentials import PublisherCredentialError, host_token
from tank.deployment.journal import JournalError
from tank.deployment.markers import FilesystemTankReleaseState
from tank.deployment.publisher import Publisher, PublisherStateError

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
def release(tmp_path: Path) -> Path:
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
def test_host_secret_permissions_fail_closed(tmp_path: Path, mode: int) -> None:
    path = tmp_path / ".env"
    path.write_text("APIFY_API_TOKEN=synthetic\n")
    path.chmod(mode)
    with pytest.raises(PublisherCredentialError, match="owned and private"):
        host_token(path, owner_uid=os.geteuid())


def test_host_secret_owner_and_symlink_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / ".env"
    path.write_text("APIFY_API_TOKEN=synthetic\n")
    path.chmod(0o600)
    with pytest.raises(PublisherCredentialError, match="owned and private"):
        host_token(path, owner_uid=path.stat().st_uid + 1)
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(OSError):
        host_token(link, owner_uid=os.geteuid())


@pytest.mark.parametrize("value", ["synthetic_token-123", "'synthetic_token-123'", '"synthetic_token-123"'])
def test_only_apify_token_is_read_from_single_host_store(tmp_path: Path, value: str) -> None:
    path = tmp_path / ".env"
    path.write_text("UNRELATED_SECRET=never-copy\nAPIFY_API_TOKEN=" + value + "\n")
    path.chmod(0o600)
    assert host_token(path, owner_uid=os.geteuid()).get_secret_value() == "synthetic_token-123"


@pytest.mark.parametrize(
    "contents", ["", "APIFY_API_TOKEN=a\nAPIFY_API_TOKEN=b", "APIFY_API_TOKEN=$(command)"]
)
def test_missing_ambiguous_or_shell_token_is_refused(tmp_path: Path, contents: str) -> None:
    path = tmp_path / ".env"
    path.write_text(contents)
    path.chmod(0o600)
    with pytest.raises(PublisherCredentialError):
        host_token(path, owner_uid=os.geteuid())


def test_source_digest_includes_paths_bytes_and_build_manifest(release: Path) -> None:
    files = source_files(release / "worker")
    baseline = files.digest()
    assert SourceBundle(files=dict(reversed(list(files.files.items())))).digest() == baseline
    assert SourceBundle(files={**files.files, "Dockerfile": b"different"}).digest() != baseline
    renamed = dict(files.files)
    renamed["renamed.py"] = renamed.pop("main.py")
    assert SourceBundle(files=renamed).digest() != baseline


def test_journal_replacement_and_removal_sync_parent_after_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = rig(tmp_path, fault="NONE")
    old = fixture.journal.read_active()
    assert old is not None
    updated = ActiveRelease(release_sha="c" * 40, build_id=old.build_id, build=old.build)
    events: list[tuple[bool, ActiveRelease | None]] = []
    original = os.fsync

    def sync(descriptor: int) -> None:
        original(descriptor)
        events.append((stat.S_ISDIR(os.fstat(descriptor).st_mode), fixture.journal.read_active()))

    monkeypatch.setattr(os, "fsync", sync)
    fixture.journal.write_active(updated)
    fixture.journal.remove_active()
    assert events == [(False, old), (True, updated), (True, None)]
    assert not list(fixture.journal.state.glob("*.tmp"))


@pytest.mark.parametrize("change", ["permissions", "replacement"])
def test_invalid_staging_is_not_published_or_unowned_staging_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    fixture = rig(tmp_path, fault="NONE")
    old = fixture.journal.read_active()
    assert old is not None
    updated = ActiveRelease(release_sha="c" * 40, build_id=old.build_id, build=old.build)
    witness = tmp_path / "witness"
    witness.write_text("untouched")
    original = os.fsync
    staging: list[Path] = []

    def sync(descriptor: int) -> None:
        original(descriptor)
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            candidates = list(fixture.journal.state.glob(".active.json.*.tmp"))
            assert len(candidates) == 1
            path = candidates[0]
            staging.append(path)
            if change == "permissions":
                path.chmod(0o644)
            else:
                path.unlink()
                path.symlink_to(witness)

    monkeypatch.setattr(os, "fsync", sync)
    with pytest.raises(JournalError):
        fixture.journal.write_active(updated)
    assert fixture.journal.read_active() == old
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
            tag = parse_qs(urlsplit(path).query).get("tag", [""])[0]
            if len(tag) > 30:
                return 400, {
                    "error": {
                        "type": "invalid-input",
                        "message": f"tag must be at most 30 characters long Received {tag!r}",
                    }
                }
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


@pytest.fixture
def typed_publisher(tmp_path: Path) -> Iterator[HTTPPublisherRig]:
    with http_rig(tmp_path, fault="NONE") as fixture:
        yield fixture


def actual_publisher(
    fixture: HTTPPublisherRig, artifact: Path, sha: str, endpoint: ApifyEndpoint
) -> Publisher:
    return Publisher(
        ApifyAPI(SecretStr("synthetic-publisher-image-token"), endpoint),
        FilesystemArtifacts(artifact),
        fixture.journal,
        fixture.marker,
        sha,
        clock=fixture.publisher.clock,
        policy=fixture.publisher.policy,
    )


def finish_typed(instance: Publisher, marker: FilesystemTankReleaseState) -> None:
    instance.activate()
    instance.commit()
    marker.path.write_text(instance.sha + "\n")
    instance.finish()


@pytest.mark.integration
def test_changed_worker_publishes_verifies_then_activates_and_reuses_unchanged(
    typed_publisher: HTTPPublisherRig,
) -> None:
    fixture = typed_publisher
    instance = fixture.publisher
    previous = fixture.journal.read_active()
    assert previous is not None
    prepared = instance.prepare()
    assert fixture.api.direct == previous.build_id
    pending = fixture.journal.read_transaction()
    assert pending is not None and pending.candidate == prepared.candidate
    assert fixture.api.version is not None
    assert fixture.api.version.buildTag is None and fixture.api.version.envVars == []
    assert fixture.api.version.applyEnvVarsToBuild is False
    finish_typed(instance, fixture.marker)
    active = fixture.journal.read_active()
    assert active is not None and active.release_sha == instance.sha
    assert fixture.journal.read_transaction() is None
    assert stat.S_IMODE((fixture.journal.state / "active.json").stat().st_mode) == 0o600
    assert stat.S_IMODE(fixture.journal.state.stat().st_mode) == 0o700
    selected = fixture.selected
    next_release = SourceArtifact(
        release=SourceRelease(
            release_sha="c" * 40,
            source_digest=selected.release.source_digest,
            contract=selected.release.contract,
            actor_id=selected.release.actor_id,
        ),
        bundle=selected.bundle,
    )
    path = fixture.artifact.parent / "next"
    write_artifact(path, next_release)
    newer = actual_publisher(fixture, path, next_release.release.release_sha, fixture.peer.endpoint)
    newer.prepare()
    finish_typed(newer, fixture.marker)
    reused = fixture.journal.read_active()
    assert reused is not None and reused.release_sha == next_release.release.release_sha
    assert isinstance(reused.build, SourceBuild) and reused.build.release_sha == instance.sha
    assert len(fixture.api.started) == 1
    assert len(fixture.api.tag_updates) == 1


@pytest.mark.integration
@pytest.mark.parametrize("failure", ["fail_upload", "fail_build", "snapshot_tamper"])
def test_publication_errors_do_not_activate_or_commit(
    typed_publisher: HTTPPublisherRig, failure: str
) -> None:
    fixture = typed_publisher
    previous = fixture.journal.read_active()
    if failure == "fail_upload":
        with publisher_upload_failure_peer(fixture.api, fixture.journal, fixture.publisher.sha) as peer:
            instance = actual_publisher(fixture, fixture.artifact, fixture.publisher.sha, peer.endpoint)
            with pytest.raises(ApifyAPIError):
                instance.prepare()
    else:
        fixture.api.fault = "BUILD_FAIL" if failure == "fail_build" else "UPLOAD_TAMPER"
        with pytest.raises((ApifyAPIError, PublisherStateError, ValueError)) as error:
            fixture.publisher.prepare()
        assert "synthetic-publisher-image-token" not in str(error.value)
    assert fixture.api.direct == "A" * 17
    assert fixture.journal.read_active() == previous
    assert fixture.journal.read_transaction() is None


@pytest.mark.integration
@pytest.mark.parametrize("failure", ["ignore", "uncertain"])
def test_failed_tag_readback_or_uncertain_update_restores_previous_build(
    typed_publisher: HTTPPublisherRig, failure: str
) -> None:
    fixture = typed_publisher
    previous = fixture.journal.read_active()
    fixture.publisher.prepare()
    fixture.api.fault = "TAG_IGNORE" if failure == "ignore" else "TAG_UNCERTAIN"
    with pytest.raises((ApifyAPIError, PublisherStateError)):
        fixture.publisher.activate()
    pending = fixture.journal.read_transaction()
    assert pending is not None and pending.activation_attempted
    fixture.api.fault = "NONE"
    fixture.publisher.rollback()
    fixture.publisher.rollback_finish()
    assert fixture.api.direct == "A" * 17
    assert fixture.journal.read_transaction() is None
    assert fixture.journal.read_active() == previous


@pytest.mark.integration
def test_build_poll_failure_is_not_treated_as_success(typed_publisher: HTTPPublisherRig) -> None:
    fixture = typed_publisher
    fixture.api.fault = "BUILD_FAIL"
    with pytest.raises(PublisherStateError, match="failed"):
        fixture.publisher.prepare()
    assert fixture.api.direct == "A" * 17
    assert any("waitForFinish=60" in request.path for request in fixture.peer.requests)


@pytest.mark.integration
def test_build_deadline_cannot_activate_incomplete_build(typed_publisher: HTTPPublisherRig) -> None:
    fixture = typed_publisher
    fixture.api.fault = "BUILD_RUNNING"
    times = iter((0.0, 601.0))
    fixture.publisher.clock = lambda: next(times)
    with pytest.raises(PublisherStateError, match="deadline"):
        fixture.publisher.prepare()
    assert fixture.api.direct == "A" * 17
    assert fixture.journal.read_transaction() is None


@pytest.mark.integration
def test_worker_rollback_failure_retains_recovery_transaction(typed_publisher: HTTPPublisherRig) -> None:
    fixture = typed_publisher
    fixture.publisher.prepare()
    fixture.publisher.activate()
    fixture.api.fault = "ROLLBACK_FAIL"
    with pytest.raises((ApifyAPIError, PublisherStateError)):
        fixture.publisher.rollback()
    assert fixture.api.direct == "B" * 17
    pending = fixture.journal.read_transaction()
    assert pending is not None and pending.previous == "A" * 17
    with pytest.raises(PublisherStateError, match="Unfinished"):
        fixture.publisher.prepare()


@pytest.mark.integration
def test_committed_release_can_rollback_before_tank_marker_finishes(
    typed_publisher: HTTPPublisherRig,
) -> None:
    fixture = typed_publisher
    fixture.publisher.prepare()
    finish_typed(fixture.publisher, fixture.marker)
    previous = fixture.journal.read_active()
    selected = source_artifact("c" * 40, b"next-body")
    path = fixture.artifact.parent / "next"
    write_artifact(path, selected)
    with publisher_http_peer(fixture.api, fixture.journal, selected.release.release_sha) as peer:
        newer = actual_publisher(fixture, path, selected.release.release_sha, peer.endpoint)
        newer.prepare()
        newer.activate()
        newer.commit()
        assert fixture.api.direct == "C" * 17
        with pytest.raises(PublisherStateError, match="Tank release marker"):
            newer.finish()
        newer.rollback()
        newer.rollback_finish()
    assert fixture.api.direct == "B" * 17
    assert fixture.journal.read_active() == previous
    assert fixture.marker.deployed() == fixture.publisher.sha


@pytest.mark.integration
@pytest.mark.parametrize("change", ["artifact", "active_tag", "cached_build", "actor_identity", "contract"])
def test_provenance_drift_fails_closed(typed_publisher: HTTPPublisherRig, change: str) -> None:
    fixture = typed_publisher
    fixture.publisher.prepare()
    finish_typed(fixture.publisher, fixture.marker)
    active = fixture.journal.read_active()
    if change == "actor_identity":
        with publisher_wrong_actor_peer(fixture.api, fixture.journal, fixture.publisher.sha) as peer:
            altered = actual_publisher(fixture, fixture.artifact, fixture.publisher.sha, peer.endpoint)
            with pytest.raises(ValueError):
                altered.prepare()
    else:
        if change == "artifact":
            (fixture.artifact / "worker/Dockerfile").write_text("tampered")
        elif change == "active_tag":
            fixture.api.direct = "Z" * 17
        else:
            observed = fixture.api.builds["B" * 17]
            entries = list(observed.source.sourceFiles)
            index = next(
                i
                for i, entry in enumerate(entries)
                if entry.name == ("tank-release.json" if change == "contract" else "main.py")
            )
            entry = entries[index]
            body = b"tampered"
            if change == "contract":
                marker = fixture.selected.release.model_dump(mode="json")
                marker["contract"] = "incompatible"
                body = json.dumps(marker).encode()
            entries[index] = Base64SourceFile(
                name=entry.name, format="BASE64", content=base64.b64encode(body).decode()
            )
            fixture.api.builds[observed.id] = BuildState(
                id=observed.id,
                status=observed.status,
                source=SourceSnapshot(sourceType="SOURCE_FILES", sourceFiles=entries),
            )
        with pytest.raises((ArtifactError, ApifyAPIError, PublisherStateError, ValueError)):
            fixture.publisher.prepare()
    assert fixture.journal.read_transaction() is None
    assert fixture.journal.read_active() == active


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


def test_apify_error_names_reason_without_token(apify_stand):
    """The fake rejects tags over Apify's 30-character limit, so every publishing test also checks the tag length."""

    api = deploy.Api("synthetic-token", apify_stand.base)
    with pytest.raises(deploy.DeployError) as error:
        api.request("POST", f"/actors/{deploy.ACTOR_ID}/builds?version={deploy.VERSION}&tag={'x' * 31}")
    assert "HTTP 400: invalid-input tag must be at most 30 characters long" in str(error.value)
    assert "synthetic-token" not in str(error.value)
