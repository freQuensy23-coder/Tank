"""Actual admitted historical funding remains separate from current corrections."""

from decimal import Decimal

from funding_input_fixtures import funding_record_fixture, funding_scope_fixture
from pydantic import JsonValue

from tank.analysis.funding_history import FundingHistoryPolicy, funding_historical_population
from tank.analysis.funding_rate import FundingNormalizationRequest
from tank.analysis.funding_selection import FundingCalculator, FundingWindowRequest
from tank.time_precision import Timestamp


def request() -> FundingWindowRequest:
    return FundingWindowRequest(
        start=Timestamp(value="2026-10-16T06:00:00Z", precision="EXACT", precision_reason=None),
        cutoff=Timestamp(value="2026-10-16T12:00:00Z", precision="EXACT", precision_reason=None),
        assembled_at=Timestamp(
            value="2026-10-16T12:05:00Z", precision="COLLECTION_ONLY", precision_reason=None
        ),
    )


def policy() -> FundingHistoryPolicy:
    return FundingHistoryPolicy(
        minimum_history_days=7,
        lookback_days=30,
        quantiles=(0.025, 0.975),
        normalization=FundingNormalizationRequest(mode="RECORDED_ONLY"),
    )


def rows() -> list[dict[str, JsonValue]]:
    first, last = funding_record_fixture("SETTLED"), funding_record_fixture("SETTLED")
    first.update(settlement_time="2026-10-01T08:00:00Z", funding_time="2026-10-01T08:00:00Z", rate_8h_eq="1")
    last.update(settlement_time="2026-10-09T08:00:00Z", funding_time="2026-10-09T08:00:00Z", rate_8h_eq="3")
    return [first, last, funding_record_fixture("CURRENT_ESTIMATE")]


def calculator(records: list[dict[str, JsonValue]]) -> FundingCalculator:
    return FundingCalculator(token_id="T", instruments=[funding_scope_fixture()], records=records)


def test_current_only_correction_reuses_exact_historical_identity() -> None:
    original_rows = rows()
    corrected_rows = rows()
    corrected_rows[-1]["rate_8h_eq"] = "999"
    original, corrected = calculator(original_rows), calculator(corrected_rows)
    first = funding_historical_population(
        original, request(), instrument=original.source.instruments[0], policy=policy()
    )
    second = funding_historical_population(
        corrected, request(), instrument=corrected.source.instruments[0], policy=policy()
    )
    assert first.identity() == second.identity()
    assert len(first.values) == 2
    bounds = first.normal_range().bounds
    assert bounds is not None
    assert bounds.lower == Decimal("1.05")
    assert bounds.upper == Decimal("2.95")
    corrected_rows[0]["rate_8h_eq"] = "2"
    historical = calculator(corrected_rows)
    third = funding_historical_population(
        historical, request(), instrument=historical.source.instruments[0], policy=policy()
    )
    assert third.identity() != first.identity()
    assert third.normal_range() != first.normal_range()


def test_dense_extreme_baseline_retains_all_sources_and_explicit_absence() -> None:
    records = rows()
    records[1]["rate_8h_eq"] = "1E1000000000"
    owned = calculator(records)
    history = funding_historical_population(
        owned, request(), instrument=owned.source.instruments[0], policy=policy()
    )
    assert len(history.sources) == len(history.values) == 2
    assert history.values[1].value == Decimal("1E1000000000")
    assert history.normal_range().bounds is None
    assert history.normal_range().reason == "NOT_ESTIMABLE"


def test_conflicting_historical_revision_does_not_become_an_extra_sample() -> None:
    records = rows()
    records.append(dict(records[0], rate_8h_eq="99"))
    owned = calculator(records)
    history = funding_historical_population(
        owned, request(), instrument=owned.source.instruments[0], policy=policy()
    )
    assert len(history.sources) == 3
    assert len(history.values) == 1
    assert len(history.ambiguous_clocks) == 1
    assert history.normal_range().reason == "INCOMPLETE"


def test_identical_read_alias_preserves_evidence_without_double_weighting() -> None:
    records = rows()
    records.append(dict(records[0]))
    owned = calculator(records)
    history = funding_historical_population(
        owned, request(), instrument=owned.source.instruments[0], policy=policy()
    )
    assert len(history.sources) == 3
    assert len(history.values) == 2
    assert history.ambiguous_clocks == ()
    bounds = history.normal_range().bounds
    assert bounds is not None
    assert bounds.lower == Decimal("1.05")


def test_future_receipt_cannot_extend_historical_span() -> None:
    records = rows()
    records[0]["received_ts"] = "2026-10-16T13:00:00Z"
    owned = calculator(records)
    history = funding_historical_population(
        owned, request(), instrument=owned.source.instruments[0], policy=policy()
    )
    assert len(history.sources) == len(history.values) == 1
    assert history.normal_range().reason == "INSUFFICIENT_HISTORY"
