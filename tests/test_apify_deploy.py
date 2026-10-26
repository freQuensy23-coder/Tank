import base64
import io
import json
import os
import shutil
import stat
import subprocess
import sys
import tarfile
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import pytest
from publisher_http_support import (
    LegacyFault,
    publisher_http_peer,
    publisher_legacy_fault_peer,
    publisher_tag_limit_peer,
    publisher_upload_failure_peer,
    publisher_wrong_actor_peer,
)
from publisher_shell_support import shell_host as owned_shell_host
from publisher_state_support import BuildState, rig, source_artifact
from publisher_transition_support import HTTPPublisherRig, http_rig, write_artifact
from publisher_workflow_support import execute, step
from pydantic import SecretStr

from tank.deployment.api_contracts import Base64SourceFile, BuildObservation, SourceSnapshot
from tank.deployment.apify_api import ApifyAPI, ApifyAPIError, ApifyEndpoint
from tank.deployment.artifact_contracts import PublisherArtifactRuntime, SourceArtifact, SourceBundle
from tank.deployment.artifacts import ArtifactError, FilesystemArtifacts, source_files
from tank.deployment.contracts import (
    ActiveRelease,
    BuildCacheKey,
    CompatibilityDigest,
    DeploymentTransaction,
    HistoricalRelease,
    RolledBackTransaction,
    SourceBuild,
    SourceRelease,
)
from tank.deployment.credentials import PublisherCredentialError, host_token
from tank.deployment.journal import JournalError, PublisherJournal
from tank.deployment.markers import FilesystemTankReleaseState
from tank.deployment.publisher import BASELINE_BUILD, BASELINE_DIGEST, Publisher, PublisherStateError
from tank.time_precision import collection_clock

ROOT = Path(__file__).resolve().parents[1]
SHA = "a" * 40
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
    manifest = SourceRelease(
        release_sha=SHA,
        source_digest=source_files(worker).digest(),
        contract="tank-x-reader-v1",
        actor_id="rbf5CQKQgKNVl4SfO",
    )
    (path / "manifest.json").write_text(manifest.model_dump_json())
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
        collection_clock=collection_clock,
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
    fixture.api.fault = "BUILD_TIMED_OUT"
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
def test_package_uses_selected_git_bytes_not_untracked_or_dirty_source(release: Path, tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    shutil.copytree(release / "worker", repository / "apify/x-reader")
    for command in (
        ["git", "init", "-q"],
        ["git", "add", "."],
        ["git", "-c", "user.name=CI", "-c", "user.email=ci@example.invalid", "commit", "-qm", "fixture"],
    ):
        subprocess.run(command, cwd=repository, check=True, capture_output=True)
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    orchestrator = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    subprocess.run(
        ["git", "fetch", "--quiet", "--no-tags", str(ROOT), orchestrator],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    archive = tmp_path / "orchestrator"
    archive.mkdir()
    archive_bytes = subprocess.check_output(
        ["git", "archive", orchestrator, "src", "scripts/deploy", "pyproject.toml", "uv.lock"], cwd=ROOT
    )
    with tarfile.open(fileobj=io.BytesIO(archive_bytes)) as bundle:
        bundle.extractall(archive, filter="data")
    (repository / "apify/x-reader/main.py").write_text("dirty\n")
    (repository / "apify/x-reader/untracked.env").write_text("secret-like-test-data")
    destination = tmp_path / "artifact"
    environment = dict(os.environ)
    environment.update(PYTHONPATH=str(archive / "src"), PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tank.deployment.cli",
            "package",
            "--orchestrator-sha",
            orchestrator,
            "--selected-release-sha",
            sha,
            "--runtime-root",
            str(archive),
            "--repository",
            str(repository),
            "--destination",
            str(destination),
            "--image-repository",
            "ghcr.io/frequensy23-coder/tank/tank",
        ],
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    assert (destination / "worker/main.py").read_text() == "value = 1\n"
    assert not (destination / "worker/untracked.env").exists()
    manifest = SourceRelease.model_validate_json(result.stdout)
    assert manifest.release_sha == sha
    assert manifest.source_digest == source_files(destination / "worker").digest()


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.parametrize(
    "rollback_outcome", ["success", "tank_failure", "worker_failure", "interrupted_after_marker"]
)
def test_actual_deploy_shell_restores_actor_before_tank_rollback(
    typed_publisher: HTTPPublisherRig,
    rollback_outcome: str,
) -> None:
    fixture = typed_publisher
    if rollback_outcome == "interrupted_after_marker":
        fixture.publisher.prepare()
        fixture.publisher.activate()
        fixture.publisher.commit()
        fixture.marker.path.write_text(fixture.publisher.sha + "\n")
    fixture.api.fault = "ROLLBACK_FAIL" if rollback_outcome == "worker_failure" else "NONE"
    host = owned_shell_host(
        fixture,
        fixture.peer.endpoint,
        selected=fixture.publisher.sha,
        artifact=fixture.selected,
        failure="ALL" if rollback_outcome == "tank_failure" else "SELECTED",
    )
    result = host.deploy(fixture.publisher.sha)
    assert result.returncode == 1
    previous = fixture.previous.release.release_sha
    if rollback_outcome == "tank_failure":
        assert "Tank rollback failed" in result.stdout
    elif rollback_outcome == "worker_failure":
        assert "Tank rollback refused" in result.stdout
    else:
        assert f"rolled back to {previous}" in result.stdout
    assert "VERIFIED" not in result.stdout
    assert fixture.api.direct == ("B" * 17 if rollback_outcome == "worker_failure" else "A" * 17)
    events = (host.root / "effects.log").read_text().splitlines()
    pulls = [event for event in events if event.endswith("pull -q")]
    expected = (
        []
        if rollback_outcome == "interrupted_after_marker"
        else [f"DOCKER {fixture.publisher.sha} compose -f compose.yaml pull -q"]
    )
    if rollback_outcome != "worker_failure":
        expected.append(f"DOCKER {previous} compose -f compose.yaml pull -q")
        restored = next(i for i, line in enumerate(events) if line.startswith("PUBLISHER_DONE rollback "))
        old_pull = events.index(f"DOCKER {previous} compose -f compose.yaml pull -q")
        assert restored < old_pull
    assert pulls == expected
    assert fixture.marker.deployed() == previous
    pending = fixture.journal.read_transaction()
    if rollback_outcome in {"tank_failure", "worker_failure"}:
        assert pending is not None and pending.previous_release_sha == previous
        assert isinstance(pending, RolledBackTransaction) is (rollback_outcome == "tank_failure")
    else:
        assert pending is None
    assert fixture.api.tag_updates == ["B" * 17, "A" * 17]


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.parametrize("journal", ["pending", "active", "cache"])
@pytest.mark.parametrize("retry_sha", [SHA, "c" * 40])
def test_deploy_retry_recovers_process_death_before_journal_rename(
    typed_publisher: HTTPPublisherRig,
    journal: Literal["pending", "active", "cache"],
    retry_sha: str,
) -> None:
    fixture = typed_publisher
    fixture.publisher.prepare()
    finish_typed(fixture.publisher, fixture.marker)
    active = fixture.journal.read_active()
    candidate = fixture.previous
    retry_artifact = SourceArtifact(
        release=SourceRelease(
            release_sha=retry_sha,
            source_digest=candidate.release.source_digest,
            contract=candidate.release.contract,
            actor_id=candidate.release.actor_id,
        ),
        bundle=candidate.bundle,
    )
    with publisher_http_peer(fixture.api, fixture.journal, candidate.release.release_sha) as peer:
        host = owned_shell_host(
            fixture, peer.endpoint, selected=retry_sha, artifact=retry_artifact, failure="SELECTED"
        )
        interrupted_artifact = host.root / "releases" / candidate.release.release_sha
        if retry_sha != candidate.release.release_sha:
            write_artifact(interrupted_artifact, candidate)
        newer = actual_publisher(fixture, interrupted_artifact, candidate.release.release_sha, peer.endpoint)
        if journal != "cache":
            newer.prepare()
        if journal == "active":
            newer.activate()
        action = {"cache": "prepare", "pending": "activate", "active": "commit"}[journal]
        environment = dict(host.environment)
        environment["TANK_PUBLISHER_STAND_INTERRUPT"] = journal
        interrupted = subprocess.run(
            [
                sys.executable,
                str(ROOT / "tests/publisher_shell_worker.py"),
                "ghcr.io/frequensy23-coder/tank/tank:" + host.orchestrator,
                host.orchestrator,
                candidate.release.release_sha,
                candidate.release.release_sha,
                action,
                str(host.root),
            ],
            cwd=host.root,
            env=environment,
            capture_output=True,
            timeout=10,
        )
        assert interrupted.returncode == 73, interrupted.stderr
    staging = list(fixture.journal.state.glob("*.tmp"))
    assert len(staging) == 1
    abandoned = staging[0].read_bytes()
    assert abandoned
    fixed_staging = fixture.journal.state / (
        candidate.release.source_digest + ".tmp" if journal == "cache" else journal + ".tmp"
    )
    fixed_staging.write_text("abandoned old publisher staging")
    fixed_staging.chmod(0o600)
    with publisher_http_peer(fixture.api, fixture.journal, retry_sha) as peer:
        host.environment["TANK_PUBLISHER_STAND_ENDPOINT"] = peer.endpoint.base_url
        result = host.deploy(retry_sha)
    assert result.returncode == 1
    assert f"rolled back to {fixture.publisher.sha}" in result.stdout
    assert "worker rollback failed" not in result.stdout
    assert fixture.api.direct == "B" * 17
    assert fixture.journal.read_active() == active
    assert fixture.journal.read_transaction() is None
    assert fixture.marker.deployed() == fixture.publisher.sha
    assert staging[0].read_bytes() == abandoned
    assert fixed_staging.read_text() == "abandoned old publisher staging"
    if journal == "cache":
        cached = fixture.journal.read_cached_build(
            BuildCacheKey(source_digest=candidate.release.source_digest)
        )
        assert cached is not None
        observed = fixture.api.completed_build(cached.build_id)
        assert observed.actVersion.decoded()["main.py"] == candidate.bundle.files["main.py"]
        assert len(fixture.api.started) == 2
    else:
        assert len(fixture.api.started) == 2


@pytest.mark.integration(reason="Actual selected historical Git bytes and current archived publisher CLI")
@pytest.mark.parametrize("sha", HISTORICAL_RELEASES)
def test_workflow_packages_historical_main_with_current_orchestrator(
    tmp_path: Path,
    sha: str,
) -> None:
    repository = tmp_path / "repository"
    subprocess.run(
        ["git", "clone", "--shared", "--no-checkout", str(ROOT), str(repository)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "checkout", "-q", "--detach", "HEAD"], cwd=repository, check=True, capture_output=True
    )
    orchestrator = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    subprocess.run(
        ["git", "update-ref", "refs/remotes/origin/main", sha],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    runner = tmp_path / "runner"
    runner.mkdir()
    output = tmp_path / "target-output"
    environment = dict(os.environ)
    environment.update(INPUT_SHA=sha, RUNNER_TEMP=str(runner), GITHUB_OUTPUT=str(output))
    targeted = execute(step(identifier="target", name=None), repository, environment)
    assert targeted.returncode == 0, targeted.stderr
    assert output.read_text() == f"sha={sha}\norchestrator_sha={orchestrator}\n"
    archive = runner / "tank-orchestrator"
    environment.update(
        SHA=sha,
        ORCHESTRATOR_SHA=orchestrator,
        GITHUB_WORKSPACE=str(repository),
        GITHUB_REPOSITORY="freQuensy23-coder/Tank",
        UV_PROJECT_ENVIRONMENT=sys.prefix,
        PYTHONPATH=str(archive / "src"),
        PYTHONDONTWRITEBYTECODE="1",
    )
    packaged = execute(
        step(identifier=None, name="Package the selected X worker with the main orchestrator"),
        repository,
        environment,
    )
    assert packaged.returncode == 0, packaged.stderr
    release = HistoricalRelease.model_validate_json(packaged.stdout)
    assert release.release_sha == sha
    assert release.worker_mode == ("unused" if sha == HISTORICAL_RELEASES[-1] else "legacy")
    package = runner / sha
    assert not (package / "worker").exists()
    runtime = PublisherArtifactRuntime.model_validate_json((package / "publisher-runtime.json").read_bytes())
    assert runtime.runtime.orchestrator_sha == orchestrator
    assert runtime.runtime.selected_release_sha == sha
    assert orchestrator != sha
    assert (package / "publisher-image").read_text() == runtime.image_reference + "\n"
    assert not (tmp_path / "apify-state").exists()
    assert (archive / "scripts/deploy/publisher_worker.sh").read_bytes() == (
        ROOT / "scripts/deploy/publisher_worker.sh"
    ).read_bytes()


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["legacy", "unused"])
def test_historical_worker_selection_never_rebuilds_or_relabels_source(
    typed_publisher: HTTPPublisherRig, mode: Literal["legacy", "unused"]
) -> None:
    fixture = typed_publisher
    fixture.publisher.prepare()
    finish_typed(fixture.publisher, fixture.marker)
    previous = fixture.journal.read_active()
    compatibility: CompatibilityDigest = (
        "67141bdf567e9c73678efb290e3cae04bae0f11bbd5ad2087db8520bd035035a"
        if mode == "unused"
        else "1dcfa5489d2f3622ddcad2d8d45a5515d9734407c4d5a3533e15e3fce6b3ba39"
    )
    release = HistoricalRelease(
        release_sha="c" * 40,
        source_digest=BASELINE_DIGEST,
        contract="tank-x-reader-legacy-0.2.2",
        actor_id="rbf5CQKQgKNVl4SfO",
        worker_mode=mode,
        compatibility_digest=compatibility,
        build_id=BASELINE_BUILD,
    )
    historical = fixture.artifact.parent / "historical"
    historical.mkdir()
    (historical / "manifest.json").write_text(release.model_dump_json())
    instance = actual_publisher(fixture, historical, release.release_sha, fixture.peer.endpoint)
    if mode == "legacy":
        with pytest.raises(PublisherStateError, match="Legacy build provenance mismatch"):
            instance.prepare()
        assert fixture.journal.read_transaction() is None
        assert fixture.journal.read_active() == previous
    else:
        instance.prepare()
        instance.activate()
        instance.commit()
        instance.rollback()
        instance.rollback_finish()
        assert fixture.journal.read_active() == previous
        assert fixture.marker.deployed() == fixture.publisher.sha
        instance.prepare()
        finish_typed(instance, fixture.marker)
        active = fixture.journal.read_active()
        assert active is not None and previous is not None
        assert active.release_sha == release.release_sha
        assert active.build_id == previous.build_id and active.build == previous.build
        instance.prepare()
        instance.activate()
        instance.rollback()
        instance.rollback_finish()
        assert fixture.journal.read_active() == active
    assert fixture.api.direct == "B" * 17
    assert len(fixture.api.started) == 1
    assert fixture.api.tag_updates == ["B" * 17]


@pytest.mark.integration
@pytest.mark.parametrize("change", ["source", "version", "build_number", "status", "actor"])
def test_legacy_build_drift_blocks_bootstrap_and_rollback(
    typed_publisher: HTTPPublisherRig, change: LegacyFault
) -> None:
    """Authored invalid historical claims never stand in for unavailable original baseline bytes."""
    fixture = typed_publisher
    prepared = fixture.publisher.prepare()
    fixture.publisher.activate()
    claimed = DeploymentTransaction(
        release_sha=fixture.publisher.sha,
        previous=BASELINE_BUILD,
        previous_active=None,
        previous_release_sha="",
        candidate=prepared.candidate,
        activation_attempted=True,
    )
    fixture.journal.write_transaction(claimed)
    original = fixture.journal.read_active()
    with publisher_legacy_fault_peer(fixture.api, fixture.journal, fixture.publisher.sha, change) as peer:
        instance = actual_publisher(fixture, fixture.artifact, fixture.publisher.sha, peer.endpoint)
        with pytest.raises((PublisherStateError, ValueError)) as refused:
            instance.rollback()
        expected = {
            "source": "provenance mismatch",
            "version": "versionNumber",
            "build_number": "buildNumber",
            "status": "status",
            "actor": "actId",
        }
        assert expected[change] in str(refused.value)
        assert fixture.api.direct == "B" * 17
        assert fixture.journal.read_active() == original
        assert fixture.journal.read_transaction() == claimed
        separate = fixture.artifact.parent / "separate"
        separate.mkdir()
        state = PublisherJournal(separate / "apify-state")
        marker = FilesystemTankReleaseState(separate / "deployed")
        fixture.api.direct = BASELINE_BUILD
        bootstrap = Publisher(
            ApifyAPI(SecretStr("synthetic-publisher-image-token"), peer.endpoint),
            FilesystemArtifacts(fixture.artifact),
            state,
            marker,
            fixture.publisher.sha,
            clock=fixture.publisher.clock,
            collection_clock=collection_clock,
            policy=fixture.publisher.policy,
        )
        with pytest.raises((PublisherStateError, ValueError)):
            bootstrap.prepare()
        assert state.read_transaction() is None
    assert fixture.api.tag_updates == ["B" * 17]


def test_apify_error_names_reason_without_token(tmp_path: Path) -> None:
    """The actual HTTP adapter preserves the provider's 30-character build-tag diagnostic."""
    fixture = rig(tmp_path, fault="NONE")
    with publisher_tag_limit_peer(fixture.api, fixture.journal, fixture.publisher.sha) as peer:
        api = ApifyAPI(SecretStr("synthetic-publisher-image-token"), peer.endpoint)
        with pytest.raises(ApifyAPIError) as error:
            api._request("POST", "/actors/rbf5CQKQgKNVl4SfO/builds?version=0.3&tag=" + "x" * 31, None)
        assert "HTTP 400: invalid-input tag must be at most 30 characters long" in str(error.value)
        assert "synthetic-publisher-image-token" not in str(error.value)
        assert not fixture.api.started


@pytest.mark.integration(
    reason="Actual Linux deployment script must refuse malformed marker before any publisher or Docker effect"
)
@pytest.mark.parametrize("marker", ["empty", "invalid", "symlink", "fifo"])
def test_invalid_previous_marker_refuses_before_effects(
    typed_publisher: HTTPPublisherRig, marker: str
) -> None:
    fixture = typed_publisher
    path = fixture.marker.path
    path.unlink()
    if marker == "empty":
        path.write_bytes(b"")
    elif marker == "invalid":
        path.write_text("not-a-release")
    elif marker == "symlink":
        target = path.with_name("marker-target")
        target.write_text(fixture.previous.release.release_sha + "\n")
        path.symlink_to(target)
    else:
        os.mkfifo(path)
    host = owned_shell_host(
        fixture,
        fixture.peer.endpoint,
        selected=fixture.publisher.sha,
        artifact=fixture.selected,
        failure="SELECTED",
    )
    result = host.deploy(fixture.publisher.sha)
    assert result.returncode == 1
    assert "invalid previous release" in result.stdout
    assert not fixture.peer.requests
    assert not (host.root / "effects.log").exists()
    assert fixture.journal.read_transaction() is None


@pytest.mark.integration(reason="Actual HTTP completion must remain inside the declared build deadline")
@pytest.mark.parametrize("completed_at,accepted", [(599.0, True), (600.0, False), (601.0, False)])
def test_late_successful_build_cannot_authorize_activation(
    typed_publisher: HTTPPublisherRig,
    monkeypatch: pytest.MonkeyPatch,
    completed_at: float,
    accepted: bool,
) -> None:
    fixture = typed_publisher
    now = [0.0]
    fixture.publisher.clock = lambda: now[0]
    wait = fixture.api.wait_build

    def completed(build_id: str) -> BuildObservation:
        now[0] = completed_at
        return wait(build_id)

    monkeypatch.setattr(fixture.api, "wait_build", completed)
    previous = fixture.journal.read_active()
    if accepted:
        prepared = fixture.publisher.prepare()
        assert prepared.candidate.build_id == "B" * 17
        assert fixture.journal.read_transaction() is not None
    else:
        with pytest.raises(PublisherStateError, match="deadline"):
            fixture.publisher.prepare()
        assert fixture.journal.read_transaction() is None
        assert (
            fixture.journal.read_cached_build(
                BuildCacheKey(source_digest=fixture.selected.release.source_digest)
            )
            is None
        )
    assert fixture.api.builds["B" * 17].status == "SUCCEEDED"
    assert fixture.api.direct == "A" * 17
    assert fixture.journal.read_active() == previous
    assert not fixture.api.tag_updates
