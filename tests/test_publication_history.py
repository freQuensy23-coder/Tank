from datetime import UTC, datetime

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.publication_history import (
    PublicationHistory,
    PublicationRevisionRef,
    publication_history_from_records,
)
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import sha256

AT = datetime(2026, 10, 26, 10, tzinfo=UTC)


def record(version: int, kind: str = "UPDATE") -> dict[str, JsonValue]:
    return {
        "alert_id": "alert-equal-clock",
        "event_id": "event-equal-clock",
        "version": version,
        "kind": kind,
        **collection_clock(AT).fields("generated_at"),
        "card_text": "Authored immutable selector witness " + str(version),
        "card_struct": {"fixture": "publication-history"},
        "decisions": [],
        "evidence_bundle_id": sha256({"fixture": "publication-history", "version": version}),
        "supersedes_version": version - 1,
        "config_version": "declared-history-fixture",
        "code_version": "declared-history-fixture",
    }


@pytest.mark.parametrize("versions", [(9, 10), (10, 9)])
async def test_actual_repository_equal_clock_history_keeps_numeric_newest_and_originals(
    versions: tuple[int, int],
) -> None:
    repo = MemoryRepository()
    await repo.open()
    try:
        originals: dict[str, str] = {}
        for version in versions:
            row = record(version)
            key = "alert-equal-clock:" + str(version)
            await repo.put("out.alerts", key, row, immutable=True)
            originals[key] = sha256(row)
        lexical = await repo.list_latest(
            "out.alerts",
            "generated_at",
            filters={"event_id": "event-equal-clock"},
            limit=1,
        )
        assert lexical[0]["version"] == 9
        metadata = [
            row
            async for row in repo.iter_metadata(
                "out.alerts",
                ["event_id", "alert_id", "version", "kind"],
                filters={"event_id": "event-equal-clock"},
                batch_size=1,
            )
        ]
        history = publication_history_from_records("event-equal-clock", metadata)
        newest = history.latest_revision()
        assert newest is not None and newest.version == 10
        assert newest.storage_key() == "alert-equal-clock:10"
        assert history.latest_ordinary() == newest
        for key, digest in originals.items():
            assert sha256(await repo.get("out.alerts", key)) == digest
    finally:
        await repo.close()


def test_ordinary_history_and_first_full_are_numeric_and_keep_shared_chain() -> None:
    history = publication_history_from_records(
        "event-equal-clock",
        [record(12, "FULL"), record(10, "FULL"), record(9, "PARTIAL"), record(13, "PRELIMINARY")],
    )
    newest, ordinary, first = history.latest_revision(), history.latest_ordinary(), history.first_full()
    assert newest is not None and newest.version == 13
    assert ordinary is not None and ordinary.version == 12
    assert first is not None and first.version == 10
    assert newest.alert_id == ordinary.alert_id == first.alert_id == "alert-equal-clock"


def test_empty_history_preserves_actual_absence() -> None:
    history = PublicationHistory(event_id="event-equal-clock", revisions=())
    assert history.latest_revision() is None
    assert history.latest_ordinary() is None
    assert history.first_full() is None


@pytest.mark.parametrize("field", ["event_id", "alert_id", "version", "kind"])
def test_missing_revision_facts_refuse_without_filling_them(field: str) -> None:
    row = record(10)
    del row[field]
    original = sha256(row)
    with pytest.raises(ValueError):
        publication_history_from_records("event-equal-clock", [row])
    assert sha256(row) == original


@pytest.mark.parametrize("version", [True, False, 10.0, "10", 0, -1])
def test_revision_requires_actual_positive_integer(version: JsonValue) -> None:
    row = record(10)
    row["version"] = version
    with pytest.raises(ValidationError):
        publication_history_from_records("event-equal-clock", [row])


@pytest.mark.parametrize("difference", ["event_id", "alert_id", "duplicate"])
def test_ambiguous_event_chain_or_duplicate_revision_refuses(difference: str) -> None:
    left, right = record(9), record(10)
    if difference == "duplicate":
        right["version"] = 9
    else:
        right[difference] = "different"
    originals = (sha256(left), sha256(right))
    with pytest.raises(ValueError):
        publication_history_from_records("event-equal-clock", [left, right])
    assert (sha256(left), sha256(right)) == originals


def test_unknown_kind_and_blank_identity_are_not_inferred() -> None:
    with pytest.raises(ValidationError):
        PublicationRevisionRef(event_id="event", alert_id=" ", version=10, kind="FULL")
    row = record(10)
    row["kind"] = "LEGACY_UNKNOWN"
    with pytest.raises(ValidationError):
        publication_history_from_records("event-equal-clock", [row])
