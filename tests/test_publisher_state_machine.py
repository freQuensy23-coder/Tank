from pathlib import Path

import pytest
from publisher_state_support import Fault, FixedArtifacts, rig

from tank.contracts.json import DuplicateJsonKeyError
from tank.deployment.api_contracts import ActorTagUpdate
from tank.deployment.artifact_contracts import SourceArtifact
from tank.deployment.contracts import (
    BuildCacheKey,
    PublisherPolicy,
    RolledBackTransaction,
    SourceRelease,
    WorkerRestored,
)
from tank.deployment.publisher import Publisher, PublisherStateError


def test_real_journal_transitions_record_attempt_before_external_tag_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = rig(tmp_path, fault="NONE")
    previous = fixture.journal.read_active()
    prepared = fixture.publisher.prepare()
    assert fixture.api.direct == "A" * 17
    assert fixture.api.tag_updates == []
    assert fixture.journal.read_active() == previous
    assert prepared.candidate.build_id == "B" * 17
    update = fixture.api.update_direct_tag

    def observe_attempt(request: ActorTagUpdate) -> None:
        transaction = fixture.journal.read_transaction()
        assert transaction is not None and transaction.activation_attempted is True
        assert transaction.candidate.build_id == request.taggedBuilds.direct.buildId
        update(request)

    monkeypatch.setattr(fixture.api, "update_direct_tag", observe_attempt)
    fixture.publisher.activate()
    assert fixture.api.direct == "B" * 17
    committed = fixture.publisher.commit()
    assert fixture.journal.read_active() == committed
    with pytest.raises(PublisherStateError, match="Tank release marker not committed"):
        fixture.publisher.finish()
    assert fixture.journal.read_transaction() is not None
    fixture.marker.path.write_text(committed.release_sha + "\n")
    fixture.publisher.finish()
    assert fixture.journal.read_transaction() is None
    cached = fixture.journal.read_cached_build(
        BuildCacheKey(source_digest=fixture.selected.release.source_digest)
    )
    assert cached == prepared.candidate


@pytest.mark.parametrize("fault", ["UPLOAD_TAMPER", "BUILD_FAIL"])
def test_failed_publication_has_no_tag_or_active_marker_effect(tmp_path: Path, fault: Fault) -> None:
    fixture = rig(tmp_path, fault=fault)
    previous = fixture.journal.read_active()
    with pytest.raises(PublisherStateError):
        fixture.publisher.prepare()
    assert fixture.api.direct == "A" * 17
    assert fixture.api.tag_updates == []
    assert fixture.journal.read_transaction() is None
    assert fixture.journal.read_active() == previous
    assert (
        fixture.journal.read_cached_build(BuildCacheKey(source_digest=fixture.selected.release.source_digest))
        is None
    )


def test_lost_update_acknowledgement_recovers_from_durable_attempt_and_reuses_build(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="TAG_UNCERTAIN")
    previous = fixture.journal.read_active()
    fixture.publisher.prepare()
    with pytest.raises(RuntimeError, match="lost update acknowledgement"):
        fixture.publisher.activate()
    assert fixture.api.direct == "B" * 17
    attempted = fixture.journal.read_transaction()
    assert attempted is not None and attempted.activation_attempted is True
    fixture.api.fault = "NONE"
    restored = fixture.publisher.rollback()
    assert isinstance(restored, WorkerRestored)
    assert restored.build_id == fixture.api.direct == "A" * 17
    assert fixture.journal.read_active() == previous
    assert isinstance(fixture.journal.read_transaction(), RolledBackTransaction)
    fixture.publisher.rollback_finish()
    assert fixture.journal.read_transaction() is None
    fixture.publisher.prepare()
    assert len(fixture.api.started) == 1
    fixture.publisher.activate()
    committed = fixture.publisher.commit()
    fixture.marker.path.write_text(committed.release_sha + "\n")
    fixture.publisher.finish()
    assert fixture.journal.read_active() == committed


def test_ignored_tag_update_never_becomes_a_committed_release(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="TAG_IGNORE")
    previous = fixture.journal.read_active()
    fixture.publisher.prepare()
    with pytest.raises(PublisherStateError, match="activation verification failed"):
        fixture.publisher.activate()
    assert fixture.api.direct == "A" * 17
    fixture.api.fault = "NONE"
    fixture.publisher.rollback()
    fixture.publisher.rollback_finish()
    assert fixture.journal.read_active() == previous
    assert fixture.journal.read_transaction() is None


def test_unrelated_external_tag_is_not_overwritten_during_recovery(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    fixture.publisher.prepare()
    fixture.publisher.activate()
    fixture.api.direct = "C" * 17
    before = list(fixture.api.tag_updates)
    with pytest.raises(PublisherStateError, match="unrelated direct tag"):
        fixture.publisher.rollback()
    assert fixture.api.direct == "C" * 17
    assert fixture.api.tag_updates == before
    assert fixture.journal.read_transaction() is not None


def test_failed_rollback_preserves_unfinished_recovery_state(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    fixture.publisher.prepare()
    fixture.publisher.activate()
    fixture.api.fault = "ROLLBACK_FAIL"
    with pytest.raises(RuntimeError, match="rollback failure"):
        fixture.publisher.rollback()
    transaction = fixture.journal.read_transaction()
    assert transaction is not None and not isinstance(transaction, RolledBackTransaction)
    assert fixture.api.direct == "B" * 17
    with pytest.raises(PublisherStateError, match="rollback incomplete"):
        fixture.publisher.rollback_finish()
    assert fixture.journal.read_transaction() == transaction


def test_same_source_new_release_preserves_original_build_provenance_without_building(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    fixture.publisher.prepare()
    fixture.publisher.activate()
    active = fixture.publisher.commit()
    fixture.marker.path.write_text(active.release_sha + "\n")
    fixture.publisher.finish()
    selected = SourceArtifact(
        release=SourceRelease(
            release_sha="c" * 40,
            source_digest=fixture.selected.release.source_digest,
            contract=fixture.selected.release.contract,
            actor_id=fixture.selected.release.actor_id,
        ),
        bundle=fixture.selected.bundle,
    )
    following = Publisher(
        fixture.api,
        FixedArtifacts(selected),
        fixture.journal,
        fixture.marker,
        selected.release.release_sha,
        clock=lambda: 0.0,
        policy=PublisherPolicy(build_timeout_s=600.0),
    )
    prepared = following.prepare()
    assert len(fixture.api.started) == 1
    assert prepared.candidate.build_id == active.build_id
    before = list(fixture.api.tag_updates)
    following.activate()
    committed = following.commit()
    assert fixture.api.tag_updates == before
    assert committed.release_sha == selected.release.release_sha
    assert committed.build == active.build
    assert fixture.journal.read_active() == committed


def test_mismatched_selected_identity_refuses_before_provider_effects(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    publisher = Publisher(
        fixture.api,
        FixedArtifacts(fixture.selected),
        fixture.journal,
        fixture.marker,
        "c" * 40,
        clock=lambda: 0.0,
        policy=PublisherPolicy(build_timeout_s=600.0),
    )
    with pytest.raises(PublisherStateError, match="artifact identity mismatch"):
        publisher.prepare()
    assert fixture.api.lookups == []
    assert fixture.api.uploads == []
    assert fixture.api.started == []
    assert fixture.journal.read_transaction() is None


def test_expired_build_wait_never_activates_running_provider_state(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="BUILD_RUNNING")
    times = iter((0.0, 601.0))
    publisher = Publisher(
        fixture.api,
        FixedArtifacts(fixture.selected),
        fixture.journal,
        fixture.marker,
        fixture.selected.release.release_sha,
        clock=lambda: next(times),
        policy=PublisherPolicy(build_timeout_s=600.0),
    )
    with pytest.raises(PublisherStateError, match="exceeded deadline"):
        publisher.prepare()
    assert fixture.api.direct == "A" * 17
    assert fixture.api.tag_updates == []
    assert fixture.journal.read_transaction() is None


def test_missing_original_baseline_payload_is_not_faked_as_positive_provenance(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    fixture.journal.remove_active()
    fixture.marker.path.unlink()
    fixture.api.direct = "c99YRMlKXube5V0gV"
    with pytest.raises(PublisherStateError, match="Legacy build provenance mismatch"):
        fixture.publisher.prepare()
    assert fixture.api.direct == "c99YRMlKXube5V0gV"
    assert fixture.api.uploads == []
    assert fixture.api.tag_updates == []
    assert fixture.journal.read_transaction() is None
    assert fixture.journal.read_active() is None


def test_duplicate_pending_fact_refuses_before_external_activation(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    fixture.publisher.prepare()
    path = fixture.journal.state / "pending.json"
    original = path.read_bytes()
    path.write_bytes(
        original.replace(
            b'"activation_attempted":false', b'"activation_attempted":true,"activation_attempted":false'
        )
    )
    lookups = list(fixture.api.lookups)
    with pytest.raises(DuplicateJsonKeyError):
        fixture.publisher.activate()
    assert fixture.api.lookups == lookups
    assert fixture.api.tag_updates == []
    assert fixture.api.direct == "A" * 17


def test_present_empty_tank_marker_is_not_treated_as_absence(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    fixture.marker.path.write_bytes(b"")
    with pytest.raises(ValueError):
        fixture.publisher.prepare()
    assert fixture.api.lookups == []
    assert fixture.api.started == []
    assert fixture.journal.read_transaction() is None


def test_new_selected_release_discovers_actual_interrupted_older_transaction(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    fixture.publisher.prepare()
    interrupted = fixture.journal.read_transaction()
    assert interrupted is not None
    replacement = Publisher(
        fixture.api,
        FixedArtifacts(fixture.selected),
        fixture.journal,
        fixture.marker,
        "c" * 40,
        clock=lambda: 0.0,
        policy=PublisherPolicy(build_timeout_s=600.0),
    )
    before = list(fixture.api.lookups)
    assert replacement.recovery_release() == interrupted.release_sha
    assert fixture.api.lookups == before
    with pytest.raises(PublisherStateError, match="release mismatch"):
        replacement.previous_release()
    assert fixture.journal.read_transaction() == interrupted
    fixture.publisher.rollback()
    fixture.publisher.rollback_finish()
    with pytest.raises(PublisherStateError, match="transaction is missing"):
        replacement.recovery_release()
