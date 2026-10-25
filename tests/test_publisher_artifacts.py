import hashlib
import subprocess
from pathlib import Path

import pytest

from tank.deployment.artifacts import (
    ArtifactError,
    PackageRequest,
    PublisherArtifactRuntime,
    load_release,
    package_release,
)
from tank.deployment.contracts import SourceRelease
from tank.deployment.runtime_contracts import RuntimeEvidence

ROOT = Path(__file__).resolve().parents[1]


def git(repository: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", *arguments], cwd=repository, text=True).strip()


def commit(repository: Path) -> str:
    git(repository, "add", ".")
    git(
        repository,
        "-c",
        "user.name=Codex",
        "-c",
        "user.email=codex@openai.com",
        "commit",
        "-m",
        "Synthetic actor artifact",
    )
    return git(repository, "rev-parse", "HEAD")


@pytest.fixture
def actor_repository(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    repository.mkdir()
    git(repository, "init", "-q")
    worker = repository / "apify/x-reader"
    files = {
        "main.py": b"from actor_contracts import value\n",
        "direct_fetch.py": b"value = 1\n",
        "public_profile.py": b"value = 1\n",
        "actor_contracts.py": b"value: int = 1\n",
        "requirements.txt": b"apify==4.1.0\n",
        "Dockerfile": b"FROM python:3.13.13-slim-bookworm\n",
        ".actor/actor.json": b'{"actorSpecification":1,"name":"tank-x-notifier"}',
    }
    for name, body in files.items():
        path = worker / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    commit(repository)
    return repository


def package_request(repository: Path, destination: Path) -> PackageRequest:
    selected = git(repository, "rev-parse", "HEAD")
    orchestrator = git(ROOT, "rev-parse", "HEAD")
    return PackageRequest(
        repository=repository,
        destination=destination,
        publisher=PublisherArtifactRuntime(
            contract="tank-publisher-runtime-v1",
            image_repository="ghcr.io/frequensy23-coder/tank/tank",
            runtime=RuntimeEvidence(
                orchestrator_sha=orchestrator,
                selected_release_sha=selected,
                python_version="3.13.13",
                uv_version="0.11.7",
                lock_sha256=hashlib.sha256((ROOT / "uv.lock").read_bytes()).hexdigest(),
                project_sha256=hashlib.sha256((ROOT / "pyproject.toml").read_bytes()).hexdigest(),
            ),
        ),
    )


def test_selected_git_blob_closure_includes_new_actor_module_and_excludes_untracked_bytes(
    actor_repository: Path, tmp_path: Path
) -> None:
    worker = actor_repository / "apify/x-reader"
    (worker / "actor_contracts.py").write_bytes(b"uncommitted change\n")
    (worker / "untracked.py").write_bytes(b"not selected\n")
    request = package_request(actor_repository, tmp_path / "release")
    release = package_release(request)
    assert isinstance(release, SourceRelease)
    assert (request.destination / "worker/actor_contracts.py").read_bytes() == b"value: int = 1\n"
    assert not (request.destination / "worker/untracked.py").exists()
    assert load_release(request.destination, selected_sha=release.release_sha) == release
    runtime = PublisherArtifactRuntime.model_validate_json(
        (request.destination / "publisher-runtime.json").read_bytes()
    )
    assert runtime.runtime.selected_release_sha == git(actor_repository, "rev-parse", "HEAD")
    assert runtime.runtime.orchestrator_sha == git(ROOT, "rev-parse", "HEAD")
    assert runtime.runtime.selected_release_sha != runtime.runtime.orchestrator_sha
    assert (request.destination / "publisher-image").read_text().strip() == runtime.image_reference
    (request.destination / "worker/actor_contracts.py").write_bytes(b"changed artifact\n")
    with pytest.raises(ArtifactError, match="source digest mismatch"):
        load_release(request.destination, selected_sha=release.release_sha)


def test_tracked_symlink_is_refused_before_artifact_effects(actor_repository: Path, tmp_path: Path) -> None:
    worker = actor_repository / "apify/x-reader"
    (worker / "module.py").symlink_to("main.py")
    commit(actor_repository)
    request = package_request(actor_repository, tmp_path / "release")
    with pytest.raises(ArtifactError, match="regular non-executable"):
        package_release(request)
    assert not request.destination.exists()


def test_unverified_historical_inputs_are_refused_before_artifact_effects(tmp_path: Path) -> None:
    repository = tmp_path / "legacy"
    (repository / "config").mkdir(parents=True)
    (repository / "src/tank/sources").mkdir(parents=True)
    (repository / "config/sources.yaml").write_text("version: synthetic-unknown\n")
    (repository / "src/tank/sources/xsensor.py").write_text("value = 1\n")
    git(repository, "init", "-q")
    commit(repository)
    request = package_request(repository, tmp_path / "release")
    with pytest.raises(ArtifactError, match="compatibility not verified"):
        package_release(request)
    assert not request.destination.exists()


def test_explicit_null_build_tag_is_refused_before_artifact_effects(
    actor_repository: Path, tmp_path: Path
) -> None:
    definition = actor_repository / "apify/x-reader/.actor/actor.json"
    definition.write_text('{"actorSpecification":1,"name":"tank-x-notifier","buildTag":null}')
    commit(actor_repository)
    request = package_request(actor_repository, tmp_path / "release")
    with pytest.raises(ValueError, match="must not activate a build tag"):
        package_release(request)
    assert not request.destination.exists()
