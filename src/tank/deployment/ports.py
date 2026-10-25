"""Inward publisher ports with named effects and consumed provider observations."""

from typing import Protocol

from tank.deployment.api_contracts import (
    ActorSnapshot,
    ActorTagUpdate,
    BuildObservation,
    CompletedBuildSnapshot,
    HistoricalBuildSnapshot,
    UploadedVersion,
    VersionUpload,
)
from tank.deployment.artifact_contracts import SelectedArtifact
from tank.deployment.contracts import ActiveRelease, BuildCacheKey, BuildID, BuildProvenance, Transaction
from tank.deployment.identities import CommitSHA


class PublisherAPI(Protocol):
    def actor(self) -> ActorSnapshot: ...

    def completed_build(self, build_id: BuildID) -> CompletedBuildSnapshot: ...

    def historical_build(self) -> HistoricalBuildSnapshot: ...

    def upload_version(self, version: VersionUpload, *, replace: bool) -> None: ...

    def uploaded_version(self) -> UploadedVersion: ...

    def start_build(self, release_sha: CommitSHA) -> BuildObservation: ...

    def wait_build(self, build_id: BuildID) -> BuildObservation: ...

    def update_direct_tag(self, update: ActorTagUpdate) -> None: ...


class PublisherArtifacts(Protocol):
    def artifact(self, selected_sha: CommitSHA) -> SelectedArtifact: ...


class PublisherState(Protocol):
    def read_active(self) -> ActiveRelease | None: ...

    def write_active(self, release: ActiveRelease) -> None: ...

    def remove_active(self) -> None: ...

    def read_transaction(self) -> Transaction | None: ...

    def write_transaction(self, transaction: Transaction) -> None: ...

    def remove_transaction(self) -> None: ...

    def read_cached_build(self, key: BuildCacheKey) -> BuildProvenance | None: ...

    def write_cached_build(self, build: BuildProvenance) -> None: ...


class TankReleaseState(Protocol):
    def deployed(self) -> CommitSHA | None: ...
