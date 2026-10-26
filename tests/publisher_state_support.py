"""Typed provider state and real private journals for publisher transition tests."""

import base64
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from tank.contracts import DomainModel
from tank.deployment.api_contracts import (
    ActorSnapshot,
    ActorTags,
    ActorTagUpdate,
    Base64SourceFile,
    BuildObservation,
    CompletedBuildSnapshot,
    DirectTag,
    HistoricalBuildSnapshot,
    HistoricalSourceSnapshot,
    SourceEntry,
    SourceSnapshot,
    TextSourceFile,
    UploadedVersion,
    VersionSummary,
    VersionUpload,
)
from tank.deployment.artifact_contracts import SelectedArtifact, SourceArtifact, SourceBundle
from tank.deployment.artifacts import source_files
from tank.deployment.contracts import ActiveRelease, BuildID, PublisherPolicy, SourceBuild, SourceRelease
from tank.deployment.identities import CommitSHA
from tank.deployment.journal import PublisherJournal
from tank.deployment.markers import FilesystemTankReleaseState
from tank.deployment.publisher import Publisher

ROOT = Path(__file__).resolve().parents[1]
type Fault = Literal[
    "NONE",
    "UPLOAD_TAMPER",
    "BUILD_FAIL",
    "BUILD_TIMED_OUT",
    "BUILD_RUNNING",
    "TAG_IGNORE",
    "TAG_UNCERTAIN",
    "ROLLBACK_FAIL",
]


class BuildState(DomainModel):
    id: BuildID
    status: Literal["RUNNING", "SUCCEEDED", "FAILED", "TIMED-OUT"]
    source: SourceSnapshot


def snapshot(bundle: SourceBundle) -> SourceSnapshot:
    return SourceSnapshot(
        sourceType="SOURCE_FILES",
        sourceFiles=[
            Base64SourceFile(name=name, format="BASE64", content=base64.b64encode(body).decode("ascii"))
            for name, body in bundle.files.items()
        ],
    )


def source_artifact(sha: CommitSHA, revision: bytes) -> SourceArtifact:
    actual = source_files(ROOT / "apify/x-reader")
    files = dict(actual.files)
    files["main.py"] += b"\n_publisher_fixture_revision = " + repr(revision.decode("ascii")).encode() + b"\n"
    bundle = SourceBundle(files=files)
    return SourceArtifact(
        release=SourceRelease(
            release_sha=sha,
            source_digest=bundle.digest(),
            contract="tank-x-reader-v1",
            actor_id="rbf5CQKQgKNVl4SfO",
        ),
        bundle=bundle,
    )


def build_source(artifact: SourceArtifact) -> SourceBundle:
    marker = json.dumps(
        artifact.release.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()
    return SourceBundle(files={**artifact.bundle.files, "tank-release.json": marker})


class FixedArtifacts:
    def __init__(self, selected: SelectedArtifact) -> None:
        self.selected = selected

    def artifact(self, selected_sha: CommitSHA) -> SelectedArtifact:
        return self.selected


class ProviderState:
    def __init__(self, initial: SourceArtifact, *, fault: Fault) -> None:
        self.direct = "A" * 17
        self.fault = fault
        self.version: VersionUpload | None = None
        self.builds: dict[BuildID, BuildState] = {
            self.direct: BuildState(
                id=self.direct, status="SUCCEEDED", source=snapshot(build_source(initial))
            )
        }
        self.tag_updates: list[BuildID] = []
        self.uploads: list[VersionUpload] = []
        self.started: list[CommitSHA] = []
        self.lookups: list[str] = []

    def actor(self) -> ActorSnapshot:
        self.lookups.append("actor")
        return ActorSnapshot(
            id="rbf5CQKQgKNVl4SfO",
            taggedBuilds=ActorTags(direct=DirectTag(buildId=self.direct)),
            versions=[]
            if self.version is None
            else [VersionSummary(versionNumber=self.version.versionNumber)],
        )

    def completed_build(self, build_id: BuildID) -> CompletedBuildSnapshot:
        self.lookups.append(build_id)
        observed = self.builds[build_id]
        if observed.status != "SUCCEEDED":
            raise AssertionError("Completed build requested before actual success")
        return CompletedBuildSnapshot(
            id=observed.id,
            actId="rbf5CQKQgKNVl4SfO",
            status="SUCCEEDED",
            actVersion=observed.source,
        )

    def historical_build(self) -> HistoricalBuildSnapshot:
        self.lookups.append("unverified-baseline")
        return HistoricalBuildSnapshot(
            id="c99YRMlKXube5V0gV",
            actId="rbf5CQKQgKNVl4SfO",
            status="SUCCEEDED",
            buildNumber="0.2.2",
            actVersion=HistoricalSourceSnapshot(
                sourceType="SOURCE_FILES",
                versionNumber="0.2",
                buildTag="direct",
                sourceFiles=[TextSourceFile(name="main.py", format="TEXT", content="unverified fixture")],
            ),
        )

    def upload_version(self, version: VersionUpload, *, replace: bool) -> None:
        self.uploads.append(version)
        self.version = version

    def uploaded_version(self) -> UploadedVersion:
        if self.version is None:
            raise AssertionError("Version requested before upload")
        files: list[SourceEntry] = [
            Base64SourceFile(name=entry.name, format=entry.format, content=entry.content)
            for entry in self.version.sourceFiles
        ]
        if self.fault == "UPLOAD_TAMPER":
            files[0] = Base64SourceFile(name=files[0].name, format="BASE64", content="eA==")
        return UploadedVersion(
            sourceType="SOURCE_FILES",
            buildTag=None,
            envVars=[],
            applyEnvVarsToBuild=False,
            sourceFiles=files,
        )

    def start_build(self, release_sha: CommitSHA) -> BuildObservation:
        if self.version is None:
            raise AssertionError("Build started before upload")
        self.started.append(release_sha)
        build_id = chr(ord("B") + len(self.started) - 1) * 17
        self.builds[build_id] = BuildState(
            id=build_id,
            status="RUNNING",
            source=SourceSnapshot(
                sourceType="SOURCE_FILES",
                sourceFiles=[
                    Base64SourceFile(name=entry.name, format=entry.format, content=entry.content)
                    for entry in self.version.sourceFiles
                ],
            ),
        )
        return BuildObservation(id=build_id, status="RUNNING")

    def wait_build(self, build_id: BuildID) -> BuildObservation:
        old = self.builds[build_id]
        status: Literal["RUNNING", "SUCCEEDED", "FAILED", "TIMED-OUT"]
        status = (
            "TIMED-OUT"
            if self.fault == "BUILD_TIMED_OUT"
            else "FAILED"
            if self.fault == "BUILD_FAIL"
            else "RUNNING"
            if self.fault == "BUILD_RUNNING"
            else "SUCCEEDED"
        )
        self.builds[build_id] = BuildState(id=old.id, status=status, source=old.source)
        return BuildObservation(id=build_id, status=status)

    def update_direct_tag(self, update: ActorTagUpdate) -> None:
        build_id = update.taggedBuilds.direct.buildId
        self.tag_updates.append(build_id)
        if self.fault == "ROLLBACK_FAIL" and build_id == "A" * 17:
            raise RuntimeError("Synthetic provider rollback failure")
        if self.fault != "TAG_IGNORE":
            self.direct = build_id
        if self.fault == "TAG_UNCERTAIN":
            raise RuntimeError("Synthetic lost update acknowledgement")


@dataclass(frozen=True)
class PublisherRig:
    publisher: Publisher
    api: ProviderState
    journal: PublisherJournal
    marker: FilesystemTankReleaseState
    previous: SourceArtifact
    selected: SourceArtifact


def rig(root: Path, *, fault: Fault) -> PublisherRig:
    previous = source_artifact("a" * 40, b"old")
    selected = source_artifact("b" * 40, b"new")
    journal = PublisherJournal(root / "apify-state")
    marker = FilesystemTankReleaseState(root / "deployed")
    marker.path.write_text(previous.release.release_sha + "\n")
    initial = SourceBuild(
        release_sha=previous.release.release_sha,
        source_digest=previous.release.source_digest,
        contract=previous.release.contract,
        actor_id=previous.release.actor_id,
        build_id="A" * 17,
    )
    journal.write_active(
        ActiveRelease(release_sha=previous.release.release_sha, build_id=initial.build_id, build=initial)
    )
    api = ProviderState(previous, fault=fault)
    publisher = Publisher(
        api,
        FixedArtifacts(selected),
        journal,
        marker,
        selected.release.release_sha,
        clock=lambda: 0.0,
        policy=PublisherPolicy(build_timeout_s=600.0),
    )
    return PublisherRig(publisher, api, journal, marker, previous, selected)
