"""Terminal archive selection requires actual card kind, revision and correction facts."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.agents.archive_contracts import (
    ArchiveFinalization,
    OrdinaryArchiveCard,
    PreliminaryArchiveCard,
    archive_card,
    terminal_archive_card,
)


def test_ordinary_chain_owns_finalization_even_after_later_preliminary_correction() -> None:
    ordinary = OrdinaryArchiveCard(alert_id="ordinary", version=1, kind="FULL")
    preliminary = PreliminaryArchiveCard(
        alert_id="preliminary", version=9, kind="PRELIMINARY", terminal_correction=True
    )
    assert terminal_archive_card([ordinary, preliminary]) is None
    update = OrdinaryArchiveCard(alert_id="ordinary", version=2, kind="UPDATE")
    assert terminal_archive_card([ordinary, preliminary, update]) == update


def test_latest_preliminary_revision_must_itself_be_a_terminal_correction() -> None:
    correction = PreliminaryArchiveCard(
        alert_id="preliminary", version=2, kind="PRELIMINARY", terminal_correction=True
    )
    later = PreliminaryArchiveCard(
        alert_id="preliminary", version=3, kind="PRELIMINARY", terminal_correction=False
    )
    assert terminal_archive_card([correction]) == correction
    assert terminal_archive_card([later, correction]) is None


def test_empty_card_chain_has_no_terminal_publication_evidence() -> None:
    assert terminal_archive_card([]) is None


@pytest.mark.parametrize("version", [True, 1.0, 0, "1"])
def test_card_revision_cannot_coerce_missing_or_invalid_evidence(version: JsonValue) -> None:
    with pytest.raises(ValidationError):
        archive_card({"alert_id": "ordinary", "version": version, "kind": "UPDATE"})


@pytest.mark.parametrize("flag", [1, "true", None])
def test_preliminary_correction_flag_must_be_an_actual_boolean(flag: JsonValue) -> None:
    with pytest.raises(ValidationError):
        archive_card(
            {"alert_id": "preliminary", "version": 1, "kind": "PRELIMINARY", "terminal_correction": flag}
        )


@pytest.mark.parametrize(
    "record",
    [
        {"alert_id": "card", "version": 1},
        {"alert_id": "card", "kind": "UPDATE"},
        {"alert_id": "card", "version": 1, "kind": "PRELIMINARY"},
    ],
)
def test_missing_card_facts_do_not_default_to_ordinary_zero_or_false(record: dict[str, JsonValue]) -> None:
    with pytest.raises(KeyError):
        archive_card(record)


def test_unknown_card_kind_cannot_enter_the_ordinary_chain() -> None:
    with pytest.raises(ValidationError):
        archive_card({"alert_id": "card", "version": 1, "kind": "UNKNOWN"})


@pytest.mark.parametrize("clock", [True, "", "2026-10-26T12:00:00", "invalid"])
def test_finalization_requires_its_actual_aware_clock(clock: JsonValue) -> None:
    with pytest.raises(ValidationError):
        ArchiveFinalization.model_validate({"session_finalized_at": clock})


def test_missing_finalization_is_unknown_and_observed_offset_is_preserved() -> None:
    assert ArchiveFinalization(session_finalized_at=None).session_finalized_at is None
    clock = "2026-10-26T14:00:00+02:00"
    assert ArchiveFinalization(session_finalized_at=clock).session_finalized_at == clock
