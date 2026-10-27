import os
import stat
from pathlib import Path

import pytest
from pydantic import ValidationError

from tank.contracts.json import DuplicateJsonKeyError
from tank.deployment.contracts import (
    ActiveRelease,
    BuildCacheKey,
    DeploymentTransaction,
    HistoricalBuild,
    SourceBuild,
)
from tank.deployment.journal import JournalError, PublisherJournal


def build() -> SourceBuild:
    return SourceBuild(
        actor_id="rbf5CQKQgKNVl4SfO",
        contract="tank-x-reader-v1",
        release_sha="a" * 40,
        source_digest="c" * 64,
        build_id="A" * 17,
    )


def active() -> ActiveRelease:
    candidate = build()
    return ActiveRelease(release_sha="b" * 40, build_id=candidate.build_id, build=candidate)


def transaction() -> DeploymentTransaction:
    return DeploymentTransaction(
        release_sha="b" * 40,
        previous="c99YRMlKXube5V0gV",
        previous_active=None,
        previous_release_sha="",
        candidate=build(),
        activation_attempted=False,
    )


def test_actual_journal_keeps_reused_build_origin_and_private_permissions(tmp_path: Path) -> None:
    journal = PublisherJournal(tmp_path / "state")
    release = active()
    journal.write_active(release)
    observed = journal.read_active()
    assert observed is not None
    assert isinstance(observed.build, SourceBuild)
    assert observed.release_sha != observed.build.release_sha
    assert observed == release
    assert stat.S_IMODE(journal.state.stat().st_mode) == 0o700
    assert stat.S_IMODE((journal.state / "active.json").stat().st_mode) == 0o600
    journal.remove_active()
    assert journal.read_active() is None


def test_failed_file_sync_preserves_previous_journal_and_removes_owned_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = PublisherJournal(tmp_path / "state")
    previous = active()
    journal.write_active(previous)
    sync = os.fsync

    def fail_file_sync(descriptor: int) -> None:
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("synthetic disk failure")
        sync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_file_sync)
    with pytest.raises(OSError, match="synthetic disk failure"):
        journal.write_active(
            ActiveRelease(release_sha="d" * 40, build_id=previous.build_id, build=previous.build)
        )
    assert journal.read_active() == previous
    assert sorted(path.name for path in journal.state.iterdir()) == ["active.json"]


def test_journal_distinguishes_missing_from_json_null_and_missing_required_keys(tmp_path: Path) -> None:
    journal = PublisherJournal(tmp_path / "state")
    assert journal.read_transaction() is None
    path = journal.state / "pending.json"
    path.write_bytes(b"null")
    path.chmod(0o600)
    with pytest.raises(ValidationError):
        journal.read_transaction()
    journal.write_transaction(transaction())
    document = path.read_bytes()
    path.write_bytes(document.replace(b'"previous_active":null,', b""))
    with pytest.raises(ValidationError):
        journal.read_transaction()


@pytest.mark.parametrize("flag", [b"1", b"0", b"1.0", b"null", b'"false"'])
def test_raw_journal_activation_flag_cannot_coerce_to_boolean(tmp_path: Path, flag: bytes) -> None:
    journal = PublisherJournal(tmp_path / "state")
    journal.write_transaction(transaction())
    path = journal.state / "pending.json"
    path.write_bytes(
        path.read_bytes().replace(b'"activation_attempted":false', b'"activation_attempted":' + flag)
    )
    with pytest.raises(ValidationError):
        journal.read_transaction()


def test_cache_key_is_bound_to_observed_build_digest(tmp_path: Path) -> None:
    journal = PublisherJournal(tmp_path / "state")
    candidate = build()
    journal.write_cached_build(candidate)
    key = BuildCacheKey(source_digest=candidate.source_digest)
    assert journal.read_cached_build(key) == candidate
    path = journal.state / (candidate.source_digest + ".json")
    path.write_bytes(path.read_bytes().replace(candidate.source_digest.encode(), ("d" * 64).encode()))
    with pytest.raises(JournalError, match="differs from its journal key"):
        journal.read_cached_build(key)


def test_dangling_link_and_fifo_are_refused_without_following_or_waiting(tmp_path: Path) -> None:
    journal = PublisherJournal(tmp_path / "state")
    target = tmp_path / "absent-target"
    path = journal.state / "active.json"
    path.symlink_to(target)
    with pytest.raises(OSError):
        journal.read_active()
    assert not target.exists()
    path.unlink()
    os.mkfifo(path, 0o600)
    with pytest.raises(JournalError, match="wrong type"):
        journal.read_active()


def test_legacy_baseline_keeps_its_actual_nonrelease_provenance(tmp_path: Path) -> None:
    journal = PublisherJournal(tmp_path / "state")
    baseline = HistoricalBuild(
        actor_id="rbf5CQKQgKNVl4SfO",
        contract="tank-x-reader-legacy-0.2.2",
        source_digest="a9a53d45f1cdc30634da29a71ae9d6a0a4a5b57e1f60eaf8a443c7cb7568e310",
        build_id="c99YRMlKXube5V0gV",
        build_number="0.2.2",
    )
    release = ActiveRelease(release_sha="a" * 40, build_id=baseline.build_id, build=baseline)
    journal.write_active(release)
    assert journal.read_active() == release
    document = (journal.state / "active.json").read_bytes()
    assert b'"build_number":"0.2.2"' in document
    assert document.count(b'"release_sha"') == 1


def test_duplicate_activation_fact_cannot_hide_an_attempted_transaction(tmp_path: Path) -> None:
    journal = PublisherJournal(tmp_path / "state")
    journal.write_transaction(transaction())
    path = journal.state / "pending.json"
    path.write_bytes(
        path.read_bytes().replace(
            b'"activation_attempted":false',
            b'"activation_attempted":true,"activation_attempted":false',
        )
    )
    with pytest.raises(DuplicateJsonKeyError):
        journal.read_transaction()
    assert b'"activation_attempted":true,"activation_attempted":false' in path.read_bytes()


def test_duplicate_cache_digest_is_refused_before_provenance_projection(tmp_path: Path) -> None:
    journal = PublisherJournal(tmp_path / "state")
    candidate = build()
    journal.write_cached_build(candidate)
    path = journal.state / (candidate.source_digest + ".json")
    path.write_bytes(
        path.read_bytes().replace(
            b'"source_digest":',
            b'"source_digest":"' + b"d" * 64 + b'","source_digest":',
        )
    )
    with pytest.raises(DuplicateJsonKeyError):
        journal.read_cached_build(BuildCacheKey(source_digest=candidate.source_digest))
