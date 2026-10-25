"""Package selected Git actor blobs with independently identified orchestrator runtime."""

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import OneInt
from tank.deployment.api_contracts import ApifyProjection, SourcePath
from tank.deployment.contracts import HistoricalRelease, Release, ReleaseDocument, SourceRelease
from tank.deployment.runtime_contracts import RuntimeEvidence

type ImageRepository = Annotated[
    str,
    Field(
        pattern=r"^ghcr\.io/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*/tank$"
    ),
]


class ArtifactError(RuntimeError):
    pass


class ActorDefinition(ApifyProjection):
    actorSpecification: OneInt
    name: str = Field(min_length=1)

    @model_validator(mode="before")
    @classmethod
    def refuse_build_tag(cls, value: JsonValue) -> JsonValue:
        if isinstance(value, dict) and "buildTag" in value:
            raise ValueError("Worker definition must not activate a build tag")
        return value


class SourceBundle(DomainModel):
    files: dict[SourcePath, bytes]

    def digest(self) -> str:
        hashes = {name: hashlib.sha256(body).hexdigest() for name, body in self.files.items()}
        document = json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(document).hexdigest()


class PublisherArtifactRuntime(DomainModel):
    contract: Literal["tank-publisher-runtime-v1"]
    image_repository: ImageRepository
    runtime: RuntimeEvidence

    @property
    def image_reference(self) -> str:
        return self.image_repository + ":" + self.runtime.orchestrator_sha


class PackageRequest(DomainModel):
    repository: Path
    destination: Path
    publisher: PublisherArtifactRuntime


def validate_source(bundle: SourceBundle) -> None:
    required = {
        "main.py",
        "direct_fetch.py",
        "public_profile.py",
        "requirements.txt",
        "Dockerfile",
        ".actor/actor.json",
    }
    if not required <= bundle.files.keys():
        raise ArtifactError("Worker source incomplete")
    if "tank-release.json" in bundle.files:
        raise ArtifactError("Reserved release marker in worker source")
    ActorDefinition.model_validate_json(bundle.files[".actor/actor.json"])


def source_files(root: Path) -> SourceBundle:
    if root.is_symlink() or not root.is_dir():
        raise ArtifactError("Worker source must be a regular directory")
    files: dict[str, bytes] = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ArtifactError("Worker source contains symlink")
        if path.is_file():
            files[path.relative_to(root).as_posix()] = path.read_bytes()
    bundle = SourceBundle(files=files)
    validate_source(bundle)
    return bundle


def git_bytes(repository: Path, *arguments: str) -> bytes:
    return subprocess.check_output(["git", *arguments], cwd=repository)


def historical_release(sha: str, digest: str) -> HistoricalRelease:
    mode: Literal["legacy", "unused"]
    match digest:
        case (
            "1dcfa5489d2f3622ddcad2d8d45a5515d9734407c4d5a3533e15e3fce6b3ba39"
            | "1be5c4cadef74c3bf1dd77c10fde968d068ef1baad6bbb3969836ccd74a54800"
            | "431febb05a9303605ba57962214df687363f223b33a0aaaca85e15e9033c0061"
            | "0da234b567166f735ae2c1c33e6de74890a0681088d06b640b1edf470d84a36e"
        ):
            mode = "legacy"
        case "67141bdf567e9c73678efb290e3cae04bae0f11bbd5ad2087db8520bd035035a":
            mode = "unused"
        case _:
            raise ArtifactError("Historical worker compatibility not verified")
    return HistoricalRelease(
        release_sha=sha,
        source_digest="a9a53d45f1cdc30634da29a71ae9d6a0a4a5b57e1f60eaf8a443c7cb7568e310",
        contract="tank-x-reader-legacy-0.2.2",
        actor_id="rbf5CQKQgKNVl4SfO",
        worker_mode=mode,
        compatibility_digest=digest,
        build_id="c99YRMlKXube5V0gV",
    )


def write_model(path: Path, value: Release | PublisherArtifactRuntime) -> None:
    document = json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    with path.open("xb") as stream:
        stream.write(document)


def package_release(request: PackageRequest) -> Release:
    sha = request.publisher.runtime.selected_release_sha
    head = git_bytes(request.repository, "rev-parse", "HEAD").decode().strip()
    if head != sha:
        raise ArtifactError("Selected release is not checked out")
    raw_names = git_bytes(request.repository, "ls-tree", "-r", "-z", "--name-only", sha, "apify/x-reader")
    names = [name.decode("utf-8") for name in raw_names.split(b"\x00") if name]
    release: Release
    bundle: SourceBundle | None
    if names:
        files: dict[str, bytes] = {}
        for name in names:
            mode = git_bytes(request.repository, "ls-tree", sha, name).split()[0]
            if mode != b"100644":
                raise ArtifactError("Worker source must contain regular non-executable files")
            files[name.removeprefix("apify/x-reader/")] = git_bytes(
                request.repository, "show", sha + ":" + name
            )
        bundle = SourceBundle(files=files)
        validate_source(bundle)
        release = SourceRelease(
            release_sha=sha,
            source_digest=bundle.digest(),
            contract="tank-x-reader-v1",
            actor_id="rbf5CQKQgKNVl4SfO",
        )
    else:
        inputs = {
            name: git_bytes(request.repository, "show", sha + ":" + name)
            for name in ("config/sources.yaml", "src/tank/sources/xsensor.py")
        }
        release = historical_release(sha, SourceBundle(files=inputs).digest())
        bundle = None
    request.destination.mkdir(parents=True, exist_ok=False)
    if bundle is not None:
        worker = request.destination / "worker"
        for name, body in bundle.files.items():
            path = worker / name
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as stream:
                stream.write(body)
    write_model(request.destination / "manifest.json", release)
    write_model(request.destination / "publisher-runtime.json", request.publisher)
    with (request.destination / "publisher-image").open("x") as stream:
        stream.write(request.publisher.image_reference + "\n")
    return release


def load_release(root: Path, *, selected_sha: str) -> Release:
    manifest = root / "manifest.json"
    if manifest.is_symlink() or not manifest.is_file():
        raise ArtifactError("Release manifest must be a regular file")
    release = ReleaseDocument.model_validate_json(manifest.read_bytes()).root
    if release.release_sha != selected_sha:
        raise ArtifactError("Release artifact identity mismatch")
    if isinstance(release, SourceRelease) and source_files(root / "worker").digest() != release.source_digest:
        raise ArtifactError("Release artifact source digest mismatch")
    return release
