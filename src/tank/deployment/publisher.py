"""Publish, confirm and recover one actor release through explicit typed ports."""

import base64
import json
from collections.abc import Callable
from typing import Final

from tank.contracts.json import decode_unique_json
from tank.deployment.api_contracts import (
    ActorTagUpdate,
    DirectTagsUpdate,
    DirectTagUpdate,
    SourceUpload,
    VersionUpload,
)
from tank.deployment.artifact_contracts import HistoricalArtifact, SourceArtifact, SourceBundle
from tank.deployment.contracts import (
    ActiveRelease,
    BuildCacheKey,
    BuildID,
    BuildProvenance,
    DeploymentTransaction,
    HistoricalBuild,
    NoPendingTransaction,
    PreparedPublication,
    PublisherPolicy,
    RollbackOutcome,
    RolledBackTransaction,
    SourceBuild,
    SourceRelease,
    Transaction,
    WorkerRestored,
)
from tank.deployment.identities import SHA256, CommitSHA
from tank.deployment.ports import PublisherAPI, PublisherArtifacts, PublisherState, TankReleaseState
from tank.deployment.submissions import ReceivedBuildSubmission, UnknownBuildSubmission
from tank.time_precision import Timestamp

BASELINE_BUILD: Final = "c99YRMlKXube5V0gV"
BASELINE_DIGEST: Final = "a9a53d45f1cdc30634da29a71ae9d6a0a4a5b57e1f60eaf8a443c7cb7568e310"
MARKER: Final = "tank-release.json"


class PublisherStateError(RuntimeError):
    pass


class Publisher:
    def __init__(
        self,
        api: PublisherAPI,
        artifacts: PublisherArtifacts,
        state: PublisherState,
        releases: TankReleaseState,
        sha: CommitSHA,
        *,
        clock: Callable[[], float],
        collection_clock: Callable[[], Timestamp],
        policy: PublisherPolicy,
    ) -> None:
        self.api = api
        self.artifacts = artifacts
        self.state = state
        self.releases = releases
        self.sha = sha
        self.clock = clock
        self.collection_clock = collection_clock
        self.policy = policy

    def current(self) -> BuildID:
        return self.api.actor().taggedBuilds.direct.buildId

    def provenance(self, build_id: BuildID, expected_digest: SHA256 | None) -> BuildProvenance:
        if build_id == BASELINE_BUILD:
            baseline = self.api.historical_build()
            actual = SourceBundle(files=baseline.actVersion.decoded()).digest()
            if actual != BASELINE_DIGEST:
                raise PublisherStateError("Legacy build provenance mismatch")
            if expected_digest is not None and expected_digest != BASELINE_DIGEST:
                raise PublisherStateError("Legacy build does not match release source")
            return HistoricalBuild(
                actor_id="rbf5CQKQgKNVl4SfO",
                contract="tank-x-reader-legacy-0.2.2",
                source_digest="a9a53d45f1cdc30634da29a71ae9d6a0a4a5b57e1f60eaf8a443c7cb7568e310",
                build_id="c99YRMlKXube5V0gV",
                build_number="0.2.2",
            )
        observed = self.api.completed_build(build_id)
        if observed.id != build_id:
            raise PublisherStateError("Build response identity mismatch")
        files = observed.actVersion.decoded()
        if MARKER not in files:
            raise PublisherStateError("Build release provenance missing")
        marker = files[MARKER]
        decode_unique_json(marker)
        release = SourceRelease.model_validate_json(marker)
        source = SourceBundle(files={name: body for name, body in files.items() if name != MARKER})
        if source.digest() != release.source_digest:
            raise PublisherStateError("Build source digest mismatch")
        if expected_digest is not None and release.source_digest != expected_digest:
            raise PublisherStateError("Build does not match release source")
        return SourceBuild(
            release_sha=release.release_sha,
            source_digest=release.source_digest,
            contract=release.contract,
            actor_id=release.actor_id,
            build_id=observed.id,
        )

    def _source_provenance(self, build_id: BuildID, expected_digest: SHA256) -> SourceBuild:
        candidate = self.provenance(build_id, expected_digest)
        if not isinstance(candidate, SourceBuild):
            raise PublisherStateError("Current source release requires its captured release marker")
        return candidate

    def _upload(self, artifact: SourceArtifact) -> None:
        marker = json.dumps(
            artifact.release.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        uploaded = SourceBundle(files={**artifact.bundle.files, MARKER: marker})
        version = VersionUpload(
            versionNumber="0.3",
            sourceType="SOURCE_FILES",
            buildTag=None,
            envVars=[],
            applyEnvVarsToBuild=False,
            sourceFiles=[
                SourceUpload(name=name, format="BASE64", content=base64.b64encode(body).decode("ascii"))
                for name, body in sorted(uploaded.files.items())
            ],
        )
        exists = any(entry.versionNumber == "0.3" for entry in self.api.actor().versions)
        self.api.upload_version(version, replace=exists)
        actual = self.api.uploaded_version()
        if actual.decoded() != uploaded.files:
            raise PublisherStateError("Uploaded version verification failed")

    def _source_candidate(
        self, artifact: SourceArtifact, previous: BuildID, active: ActiveRelease | None
    ) -> SourceBuild:
        key = BuildCacheKey(source_digest=artifact.release.source_digest)
        submission = self.state.read_submission(key)
        if submission is not None and submission.release_sha != self.sha:
            raise PublisherStateError("Pending build submission belongs to a different release")
        if isinstance(submission, UnknownBuildSubmission):
            raise PublisherStateError("Build acceptance is unknown; another submission is refused")
        cached = self.state.read_cached_build(key)
        if cached is not None:
            candidate = self._source_provenance(cached.build_id, artifact.release.source_digest)
            if candidate != cached:
                raise PublisherStateError("Cached build provenance mismatch")
            if submission is not None:
                if submission.observation.id != candidate.build_id:
                    raise PublisherStateError("Received build differs from its cached candidate")
                self.state.remove_submission(key)
            return candidate
        if (
            submission is None
            and active is not None
            and active.build.source_digest == artifact.release.source_digest
        ):
            candidate = self._source_provenance(previous, artifact.release.source_digest)
        else:
            if submission is None:
                self._upload(artifact)
                intent = UnknownBuildSubmission(
                    phase="ACCEPTANCE_UNKNOWN",
                    release_sha=self.sha,
                    source_digest=artifact.release.source_digest,
                    requested_at=self.collection_clock(),
                    budget_s=self.policy.build_timeout_s,
                )
                self.state.write_submission(intent)
                started = self.clock()
                build = self.api.start_build(self.sha)
                submission = ReceivedBuildSubmission(
                    phase="BUILD_RECEIVED",
                    release_sha=intent.release_sha,
                    source_digest=intent.source_digest,
                    requested_at=intent.requested_at,
                    budget_s=intent.budget_s,
                    observation=build,
                )
                self.state.write_submission(submission)
                deadline = started + intent.budget_s
            else:
                build = submission.observation
                deadline = self.clock() + max(0.0, submission.remaining(self.collection_clock()))
            build_id = build.id
            while build.status in {"READY", "RUNNING"} and self.clock() < deadline:
                build = self.api.wait_build(build_id)
                if build.id != build_id:
                    raise PublisherStateError("Build response identity mismatch")
                submission = ReceivedBuildSubmission(
                    phase="BUILD_RECEIVED",
                    release_sha=submission.release_sha,
                    source_digest=submission.source_digest,
                    requested_at=submission.requested_at,
                    budget_s=submission.budget_s,
                    observation=build,
                )
                self.state.write_submission(submission)
            if (
                build.status != "SUCCEEDED"
                or self.clock() >= deadline
                or submission.remaining(self.collection_clock()) <= 0
            ):
                raise PublisherStateError("Worker build failed or exceeded deadline")
            candidate = self._source_provenance(build_id, artifact.release.source_digest)
            if candidate.release_sha != self.sha:
                raise PublisherStateError("New build release SHA mismatch")
        self.state.write_cached_build(candidate)
        if submission is not None:
            self.state.remove_submission(key)
        return candidate

    def prepare(self) -> PreparedPublication:
        if self.state.read_transaction() is not None:
            raise PublisherStateError("Unfinished worker deployment requires recovery")
        artifact = self.artifacts.artifact(self.sha)
        if artifact.release.release_sha != self.sha:
            raise PublisherStateError("Release artifact identity mismatch")
        previous_release = self.releases.deployed()
        previous = self.current()
        active = self.state.read_active()
        if active is None:
            if previous != BASELINE_BUILD:
                raise PublisherStateError("Unknown legacy direct build")
            self.provenance(previous, None)
        else:
            if active.build_id != previous:
                raise PublisherStateError("Active worker changed outside deployment")
            if active.release_sha != previous_release:
                raise PublisherStateError("Tank and worker release markers disagree")
            if self.provenance(previous, None) != active.build:
                raise PublisherStateError("Active worker provenance mismatch")
        if isinstance(artifact, HistoricalArtifact):
            target = previous if artifact.release.worker_mode == "unused" else BASELINE_BUILD
            candidate = self.provenance(target, None)
        else:
            artifact = SourceArtifact.model_validate(artifact)
            candidate = self._source_candidate(artifact, previous, active)
        if self.current() != previous:
            raise PublisherStateError("Direct tag changed while publishing")
        self.state.write_transaction(
            DeploymentTransaction(
                release_sha=self.sha,
                previous=previous,
                previous_active=active,
                previous_release_sha="" if previous_release is None else previous_release,
                candidate=candidate,
                activation_attempted=False,
            )
        )
        return PreparedPublication(release_sha=self.sha, candidate=candidate)

    def pending(self) -> Transaction:
        transaction = self.state.read_transaction()
        if transaction is None or transaction.release_sha != self.sha:
            raise PublisherStateError("Worker transaction missing or release mismatch")
        return transaction

    def _active_transaction(self) -> DeploymentTransaction:
        transaction = self.pending()
        if isinstance(transaction, RolledBackTransaction):
            raise PublisherStateError("Worker transaction has already rolled back")
        return transaction

    def _tag(self, build_id: BuildID) -> None:
        self.api.update_direct_tag(
            ActorTagUpdate(taggedBuilds=DirectTagsUpdate(direct=DirectTagUpdate(buildId=build_id)))
        )
        if self.current() != build_id:
            raise PublisherStateError("Direct tag activation verification failed")

    def activate(self) -> None:
        transaction = self._active_transaction()
        candidate = transaction.candidate
        if self.current() != transaction.previous:
            raise PublisherStateError("Direct tag changed before activation")
        if self.provenance(candidate.build_id, None) != candidate:
            raise PublisherStateError("Candidate provenance changed")
        self.state.write_transaction(
            DeploymentTransaction(
                release_sha=transaction.release_sha,
                previous=transaction.previous,
                previous_active=transaction.previous_active,
                previous_release_sha=transaction.previous_release_sha,
                candidate=candidate,
                activation_attempted=True,
            )
        )
        if candidate.build_id != transaction.previous:
            self._tag(candidate.build_id)

    def commit(self) -> ActiveRelease:
        transaction = self._active_transaction()
        candidate = transaction.candidate
        if not transaction.activation_attempted or self.current() != candidate.build_id:
            raise PublisherStateError("Worker activation not verified")
        if self.provenance(candidate.build_id, None) != candidate:
            raise PublisherStateError("Worker provenance changed before commit")
        active = ActiveRelease(release_sha=self.sha, build_id=candidate.build_id, build=candidate)
        self.state.write_active(active)
        return active

    def finish(self) -> None:
        transaction = self._active_transaction()
        if self.releases.deployed() != self.sha:
            raise PublisherStateError("Tank release marker not committed")
        expected = ActiveRelease(
            release_sha=self.sha,
            build_id=transaction.candidate.build_id,
            build=transaction.candidate,
        )
        if self.state.read_active() != expected:
            raise PublisherStateError("Worker release marker not committed")
        if self.current() != transaction.candidate.build_id:
            raise PublisherStateError("Worker tag changed before completion")
        self.state.remove_transaction()

    def rollback(self) -> RollbackOutcome:
        if self.state.read_transaction() is None:
            return NoPendingTransaction(state="NO_PENDING_TRANSACTION")
        transaction = self.pending()
        previous = transaction.previous
        if transaction.previous_active is None:
            if previous != BASELINE_BUILD:
                raise PublisherStateError("Invalid bootstrap rollback build")
            self.provenance(previous, None)
        elif self.provenance(previous, None) != transaction.previous_active.build:
            raise PublisherStateError("Rollback provenance mismatch")
        if transaction.activation_attempted:
            current = self.current()
            if current not in {previous, transaction.candidate.build_id}:
                raise PublisherStateError("Refusing to overwrite unrelated direct tag")
            if current != previous:
                self._tag(previous)
            if self.current() != previous:
                raise PublisherStateError("Worker rollback verification failed")
        if transaction.previous_active is None:
            self.state.remove_active()
        else:
            self.state.write_active(transaction.previous_active)
        self.state.write_transaction(
            RolledBackTransaction(
                release_sha=transaction.release_sha,
                previous=transaction.previous,
                previous_active=transaction.previous_active,
                previous_release_sha=transaction.previous_release_sha,
                candidate=transaction.candidate,
                activation_attempted=transaction.activation_attempted,
                rolled_back=True,
            )
        )
        return WorkerRestored(state="RESTORED", build_id=previous)

    def previous_release(self) -> CommitSHA | None:
        previous = self.pending().previous_release_sha
        return None if previous == "" else previous

    def recovery_release(self) -> CommitSHA:
        transaction = self.state.read_transaction()
        if transaction is None:
            raise PublisherStateError("Interrupted worker transaction is missing")
        return transaction.release_sha

    def rollback_finish(self) -> None:
        if self.state.read_transaction() is None:
            return
        transaction = self.pending()
        if not isinstance(transaction, RolledBackTransaction):
            raise PublisherStateError("Worker rollback incomplete")
        if self.current() != transaction.previous:
            raise PublisherStateError("Worker rollback incomplete")
        if self.state.read_active() != transaction.previous_active:
            raise PublisherStateError("Worker rollback marker mismatch")
        previous = self.releases.deployed()
        if ("" if previous is None else previous) != transaction.previous_release_sha:
            raise PublisherStateError("Tank rollback marker mismatch")
        self.state.remove_transaction()
