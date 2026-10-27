import subprocess
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field

from tank.contracts import DomainModel
from tank.deployment.artifact_contracts import PackageRequest, PublisherArtifactRuntime
from tank.deployment.artifacts import package_release
from tank.deployment.runtime import observe_runtime
from tank.deployment.runtime_contracts import RuntimeInvocation

ROOT = Path(__file__).resolve().parents[1]


class SelectedActorFile(DomainModel):
    relative_name: str
    body: bytes


class PackagedManifest(DomainModel):
    release_sha: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    source_digest: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    contract: Literal["tank-x-reader-v1"]
    actor_id: Literal["rbf5CQKQgKNVl4SfO"]


class PackagedActor(DomainModel):
    root: Path
    manifest: PackagedManifest


def git_bytes(repository: Path, *arguments: str) -> bytes:
    return subprocess.check_output(["git", *arguments], cwd=repository)


def git_text(repository: Path, *arguments: str) -> str:
    return git_bytes(repository, *arguments).decode("utf-8").strip()


def selected_actor_files() -> list[SelectedActorFile]:
    names = git_bytes(ROOT, "ls-tree", "-r", "-z", "--name-only", "HEAD", "apify/x-reader")
    return [
        SelectedActorFile(
            relative_name=name.decode("utf-8").removeprefix("apify/x-reader/"),
            body=git_bytes(ROOT, "show", "HEAD:" + name.decode("utf-8")),
        )
        for name in names.split(b"\x00")
        if name
    ]


def commit(repository: Path) -> str:
    git_bytes(repository, "add", ".")
    git_bytes(
        repository,
        "-c",
        "user.name=Codex",
        "-c",
        "user.email=codex@openai.com",
        "commit",
        "-m",
        "Synthetic selected actor closure",
    )
    return git_text(repository, "rev-parse", "HEAD")


def package(repository: Path, destination: Path) -> PackagedActor:
    sha = git_text(repository, "rev-parse", "HEAD")
    orchestrator = git_text(ROOT, "rev-parse", "HEAD")
    runtime = observe_runtime(
        RuntimeInvocation(orchestrator_sha=orchestrator, selected_release_sha=sha), ROOT
    )
    package_release(
        PackageRequest(
            repository=repository,
            destination=destination,
            publisher=PublisherArtifactRuntime(
                contract="tank-publisher-runtime-v1",
                image_repository="ghcr.io/frequensy23-coder/tank/tank",
                runtime=runtime,
            ),
        )
    )
    return PackagedActor(
        root=destination,
        manifest=PackagedManifest.model_validate_json((destination / "manifest.json").read_bytes()),
    )


def test_actual_selected_actor_package_contains_contract_and_frozen_requirements(tmp_path: Path) -> None:
    artifact = package(ROOT, tmp_path / "current-release")
    module = artifact.root / "worker/actor_contracts.py"
    assert module.read_bytes() == git_bytes(ROOT, "show", "HEAD:apify/x-reader/actor_contracts.py")
    requirements = (artifact.root / "worker/requirements.txt").read_bytes()
    assert requirements == git_bytes(ROOT, "show", "HEAD:apify/x-reader/requirements.txt")
    assert b"--hash=sha256:" in requirements
    assert artifact.manifest.release_sha == git_text(ROOT, "rev-parse", "HEAD")


def test_contract_module_bytes_bind_digest_while_unrelated_commit_does_not(tmp_path: Path) -> None:
    repository = tmp_path / "selected"
    repository.mkdir()
    git_bytes(repository, "init", "-q")
    for resource in selected_actor_files():
        target = repository / "apify/x-reader" / resource.relative_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(resource.body)
    first_sha = commit(repository)
    first = package(repository, tmp_path / "first-release")
    module = repository / "apify/x-reader/actor_contracts.py"
    original_module = module.read_bytes()
    modified_module = original_module + b"\n_package_closure_probe = True\n"
    module.write_bytes(modified_module)
    second_sha = commit(repository)
    second = package(repository, tmp_path / "second-release")
    (repository / "unrelated.txt").write_bytes(b"Outside the actor source tree.\n")
    third_sha = commit(repository)
    third = package(repository, tmp_path / "third-release")
    assert (first.root / "worker/actor_contracts.py").read_bytes() == original_module
    assert (second.root / "worker/actor_contracts.py").read_bytes() == modified_module
    assert (third.root / "worker/actor_contracts.py").read_bytes() == modified_module
    assert first.manifest.source_digest != second.manifest.source_digest
    assert second.manifest.source_digest == third.manifest.source_digest
    assert [
        first.manifest.release_sha,
        second.manifest.release_sha,
        third.manifest.release_sha,
    ] == [first_sha, second_sha, third_sha]
