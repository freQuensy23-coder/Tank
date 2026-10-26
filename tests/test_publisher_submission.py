"""Real durable journals prevent duplicate builds across acceptance crash gaps."""

from datetime import timedelta
from pathlib import Path

import pytest
from publisher_state_support import PublisherRig, rig

from tank.deployment.api_contracts import BuildObservation
from tank.deployment.contracts import BuildCacheKey, BuildProvenance
from tank.deployment.publisher import Publisher, PublisherStateError
from tank.deployment.submissions import ReceivedBuildSubmission, UnknownBuildSubmission
from tank.time_precision import Timestamp, collection_clock
from tank.util import parse_utc_time


def restart(fixture: PublisherRig, observed: Timestamp) -> Publisher:
    return Publisher(
        fixture.api,
        fixture.publisher.artifacts,
        fixture.journal,
        fixture.marker,
        fixture.publisher.sha,
        clock=lambda: 0.0,
        collection_clock=lambda: observed,
        policy=fixture.publisher.policy,
    )


def test_unknown_intent_is_durable_before_provider_receives_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = rig(tmp_path, fault="NONE")
    key = BuildCacheKey(source_digest=fixture.selected.release.source_digest)
    start = fixture.api.start_build

    def accepted_lost_response(sha: str) -> BuildObservation:
        assert isinstance(fixture.journal.read_submission(key), UnknownBuildSubmission)
        start(sha)
        raise ConnectionResetError("synthetic lost response after acceptance")

    monkeypatch.setattr(fixture.api, "start_build", accepted_lost_response)
    with pytest.raises(ConnectionResetError):
        fixture.publisher.prepare()
    unknown = fixture.journal.read_submission(key)
    assert isinstance(unknown, UnknownBuildSubmission)
    assert len(fixture.api.started) == 1
    before = list(fixture.api.started)
    with pytest.raises(PublisherStateError, match="acceptance is unknown"):
        restart(fixture, collection_clock()).prepare()
    assert fixture.api.started == before
    assert fixture.journal.read_submission(key) == unknown
    assert fixture.journal.read_transaction() is None
    assert fixture.api.direct == "A" * 17


def test_intent_write_failure_refuses_before_provider_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = rig(tmp_path, fault="NONE")

    def refuse_intent(submission: UnknownBuildSubmission | ReceivedBuildSubmission) -> None:
        raise OSError("synthetic durable storage refusal")

    monkeypatch.setattr(fixture.journal, "write_submission", refuse_intent)
    with pytest.raises(OSError):
        fixture.publisher.prepare()
    assert fixture.api.started == []
    assert fixture.journal.read_transaction() is None


def test_received_build_survives_cache_crash_without_another_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = rig(tmp_path, fault="NONE")
    key = BuildCacheKey(source_digest=fixture.selected.release.source_digest)
    write = fixture.journal.write_cached_build

    def interrupted_cache(build: BuildProvenance) -> None:
        raise OSError("synthetic interruption before cache rename")

    monkeypatch.setattr(fixture.journal, "write_cached_build", interrupted_cache)
    with pytest.raises(OSError):
        fixture.publisher.prepare()
    receipt = fixture.journal.read_submission(key)
    assert isinstance(receipt, ReceivedBuildSubmission)
    assert receipt.observation.id == "B" * 17
    assert receipt.observation.status == "SUCCEEDED"
    assert len(fixture.api.started) == 1
    monkeypatch.setattr(fixture.journal, "write_cached_build", write)
    prepared = restart(fixture, collection_clock()).prepare()
    assert prepared.candidate.build_id == receipt.observation.id
    assert len(fixture.api.started) == 1
    assert fixture.journal.read_submission(key) is None
    assert fixture.journal.read_cached_build(key) == prepared.candidate


def test_receipt_write_loss_preserves_unknown_and_refuses_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = rig(tmp_path, fault="NONE")
    key = BuildCacheKey(source_digest=fixture.selected.release.source_digest)
    write = fixture.journal.write_submission

    def lose_receipt(submission: UnknownBuildSubmission | ReceivedBuildSubmission) -> None:
        if isinstance(submission, ReceivedBuildSubmission):
            raise OSError("synthetic receipt persistence interruption")
        write(submission)

    monkeypatch.setattr(fixture.journal, "write_submission", lose_receipt)
    with pytest.raises(OSError):
        fixture.publisher.prepare()
    assert isinstance(fixture.journal.read_submission(key), UnknownBuildSubmission)
    assert len(fixture.api.started) == 1
    with pytest.raises(PublisherStateError, match="acceptance is unknown"):
        restart(fixture, collection_clock()).prepare()
    assert len(fixture.api.started) == 1


@pytest.mark.parametrize("elapsed", [600, 601])
def test_restart_keeps_original_budget_and_remote_success_fact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, elapsed: int
) -> None:
    fixture = rig(tmp_path, fault="NONE")
    key = BuildCacheKey(source_digest=fixture.selected.release.source_digest)

    def interrupted_cache(build: BuildProvenance) -> None:
        raise OSError("synthetic interruption before cache rename")

    monkeypatch.setattr(fixture.journal, "write_cached_build", interrupted_cache)
    with pytest.raises(OSError):
        fixture.publisher.prepare()
    receipt = fixture.journal.read_submission(key)
    assert isinstance(receipt, ReceivedBuildSubmission)
    value = receipt.requested_at.value
    assert value is not None
    observed = collection_clock(parse_utc_time(value) + timedelta(seconds=elapsed))
    with pytest.raises(PublisherStateError, match="deadline"):
        restart(fixture, observed).prepare()
    assert fixture.journal.read_submission(key) == receipt
    assert receipt.observation.status == fixture.api.builds[receipt.observation.id].status == "SUCCEEDED"
    assert len(fixture.api.started) == 1
    assert fixture.api.tag_updates == []
    assert fixture.journal.read_cached_build(key) is None
