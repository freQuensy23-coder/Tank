"""Selected funding witnesses retain their own clocks, receipts and original source identity."""

from decimal import Decimal

import pytest
from funding_input_fixtures import funding_record_fixture, funding_scope_fixture
from pydantic import JsonValue, ValidationError

from tank.analysis.funding_selection import (
    FundingCalculator,
    FundingWindowRequest,
    SelectedFundingObservation,
    UnavailableFundingSelection,
)
from tank.time_precision import Timestamp


def window() -> FundingWindowRequest:
    return FundingWindowRequest(
        start=Timestamp(value="2026-10-16T06:00:00Z", precision="EXACT", precision_reason=None),
        cutoff=Timestamp(value="2026-10-16T12:00:00Z", precision="EXACT", precision_reason=None),
        assembled_at=Timestamp(
            value="2026-10-16T12:05:00Z", precision="COLLECTION_ONLY", precision_reason=None
        ),
    )


def calculator(records: list[dict[str, JsonValue]]) -> FundingCalculator:
    return FundingCalculator(token_id="T", instruments=[funding_scope_fixture()], records=records)


def test_funding_selects_actual_latest_kinds_and_settled_start_independently() -> None:
    prior, settled = funding_record_fixture("SETTLED"), funding_record_fixture("SETTLED")
    prior.update(settlement_time="2026-10-16T00:00:00Z", funding_time="2026-10-16T00:00:00Z")
    first, latest = funding_record_fixture("CURRENT_ESTIMATE"), funding_record_fixture("CURRENT_ESTIMATE")
    first.update(as_of="2026-10-16T10:00:00Z", rate="0.001")
    latest.update(as_of="2026-10-16T11:00:00Z", rate="0.002")
    owned = calculator([latest, prior, first, settled])
    result = owned.select(window())[0]
    assert isinstance(result.settled, SelectedFundingObservation)
    assert isinstance(result.estimate, SelectedFundingObservation)
    assert isinstance(result.start, SelectedFundingObservation)
    assert result.settled.observation.clock.timestamp.value == "2026-10-16T08:00:00Z"
    assert result.start.observation.clock.timestamp.value == "2026-10-16T00:00:00Z"
    assert result.estimate.observation.rate.value == Decimal("0.002")
    assert result.estimate.observation.evidence == owned.source.observations[0].evidence
    assert owned.observation(result.estimate.observation.evidence) == result.estimate.observation
    assert result.excluded == ()


@pytest.mark.parametrize("visibility", ["UNKNOWN", "FUTURE_RECEIPT", "FUTURE_SOURCE"])
def test_ineligible_duplicate_cannot_supply_or_ambiguate_selected_funding(visibility: str) -> None:
    actual = funding_record_fixture("CURRENT_ESTIMATE")
    duplicate = dict(actual, rate="0.999")
    if visibility == "UNKNOWN":
        duplicate.pop("received_ts")
        duplicate.pop("received_ts_prec")
        expected = "RECEIPT_NOT_CAPTURED"
    elif visibility == "FUTURE_RECEIPT":
        duplicate["received_ts"] = "2026-10-16T12:10:00Z"
        expected = "RECEIPT_AFTER_ASSEMBLY"
    else:
        duplicate["as_of"] = "2026-10-16T13:00:00Z"
        expected = "SOURCE_AFTER_CUTOFF"
    owned = calculator([duplicate, actual])
    result = owned.select(window())[0]
    assert isinstance(result.estimate, SelectedFundingObservation)
    assert result.estimate.observation.rate.value == Decimal("-0.001")
    assert result.estimate.observation.evidence == owned.source.observations[1].evidence
    assert len(result.excluded) == 1
    assert result.excluded[0].source == owned.source.observations[0].evidence
    assert result.excluded[0].reason == expected


def test_equal_time_different_source_evidence_does_not_become_false_agreement() -> None:
    first = funding_record_fixture("CURRENT_ESTIMATE")
    second: dict[str, JsonValue] = dict(first)
    second["source_path"] = "fixture:normalized-funding:second"
    owned = calculator([first, second])
    result = owned.select(window())[0]
    assert isinstance(result.estimate, UnavailableFundingSelection)
    assert result.estimate.reason == "AMBIGUOUS_LATEST_SOURCE"
    assert set(result.estimate.candidates) == {point.evidence for point in owned.source.observations}


def test_identical_overlapping_read_aliases_keep_both_records_and_one_deterministic_witness() -> None:
    row = funding_record_fixture("CURRENT_ESTIMATE")
    owned = calculator([row, dict(row)])
    assert len(owned.source.source_projections) == 2
    result = owned.select(window())[0]
    assert isinstance(result.estimate, SelectedFundingObservation)
    assert result.estimate.observation.evidence == owned.source.observations[0].evidence


def test_latest_unusable_raw_rate_is_preserved_instead_of_silently_selecting_an_older_source() -> None:
    older, latest = funding_record_fixture("CURRENT_ESTIMATE"), funding_record_fixture("CURRENT_ESTIMATE")
    older["as_of"] = "2026-10-16T10:00:00Z"
    latest.update(as_of="2026-10-16T11:00:00Z", rate="NaN")
    result = calculator([older, latest]).select(window())[0]
    assert isinstance(result.estimate, SelectedFundingObservation)
    assert result.estimate.observation.clock.timestamp.value == "2026-10-16T11:00:00Z"
    assert result.estimate.observation.rate.status == "NOT_ESTIMABLE"
    assert result.estimate.observation.rate.value is None


def test_missing_settlement_clock_does_not_use_as_of_to_create_window_membership() -> None:
    row = funding_record_fixture("SETTLED")
    for key in ("settlement_time", "settlement_time_prec", "funding_time", "funding_time_prec"):
        row.pop(key)
    result = calculator([row]).select(window())[0]
    assert isinstance(result.settled, UnavailableFundingSelection)
    assert result.settled.reason == "NO_ELIGIBLE_OBSERVATION"
    assert result.excluded[0].reason == "SOURCE_CLOCK_NOT_CAPTURED"


def test_missing_assembly_clock_preserves_sources_without_available_selected_facts() -> None:
    owned = calculator([funding_record_fixture("CURRENT_ESTIMATE")])
    request = window().model_copy(
        update={"assembled_at": Timestamp(value=None, precision=None, precision_reason="INCOMPLETE")}
    )
    result = owned.select(request)[0]
    assert isinstance(result.estimate, UnavailableFundingSelection)
    assert result.excluded[0].reason == "ASSEMBLY_CLOCK_NOT_CAPTURED"
    assert owned.source.observations[0].rate.value == Decimal("-0.001")


def test_funding_calculator_owns_immutable_read_and_revalidates_public_window_copies() -> None:
    row = funding_record_fixture("CURRENT_ESTIMATE")
    owned = calculator([row])
    row["rate"] = "0.999"
    result = owned.select(window())[0]
    assert isinstance(result.estimate, SelectedFundingObservation)
    assert result.estimate.observation.rate.value == Decimal("-0.001")
    bad = window().model_copy(update={"start": window().cutoff})
    with pytest.raises(ValidationError, match="forward window"):
        owned.select(bad)


def test_empty_captured_funding_read_does_not_create_a_zero_rate_or_observation() -> None:
    result = calculator([]).select(window())[0]
    for selection in (result.settled, result.estimate, result.start):
        assert isinstance(selection, UnavailableFundingSelection)
        assert selection.reason == "NO_ELIGIBLE_OBSERVATION"
        assert selection.candidates == ()
    assert result.excluded == ()
