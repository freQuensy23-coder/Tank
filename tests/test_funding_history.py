"""Actual admitted historical funding remains separate from current corrections."""

from datetime import timedelta
from decimal import MAX_EMAX, Decimal

import pytest
from funding_history_fixtures import calculator, policy, request, rows
from funding_input_fixtures import funding_record_fixture

from tank.analysis.baseline_cache import BaselineCaches
from tank.analysis.condition_graph_contracts import AvailableConditionFunding
from tank.analysis.funding_family import condition_funding_family
from tank.analysis.funding_history import FundingHistoryPolicy, funding_historical_population
from tank.analysis.funding_selection import FundingWindowRequest
from tank.contracts.funding import FundingNormalizationRequest
from tank.time_precision import Timestamp
from tank.util import iso, parse_iso_time


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


@pytest.mark.parametrize("sample_count,expected", [(18, "INSUFFICIENT_HISTORY"), (22, None)])
def test_unavailable_older_raw_components_cannot_extend_known_sample_maturity(
    sample_count: int,
    expected: str | None,
) -> None:
    start = parse_iso_time("2026-10-10T00:00:00Z")
    end = start + timedelta(days=1)
    records = []
    for index in range(1, sample_count + 1):
        point = funding_record_fixture("SETTLED")
        at = iso(start - timedelta(hours=8 * index))
        point.update(
            settlement_time=at,
            funding_time=at,
            as_of=at,
            rate="0.0001",
            interval_h="8",
            rate_8h_eq=None,
            received_ts=iso(end),
        )
        records.append(point)
    unavailable = funding_record_fixture("SETTLED")
    oldest = iso(start - timedelta(days=30))
    unavailable.update(
        settlement_time=oldest,
        funding_time=oldest,
        as_of=oldest,
        rate=None,
        interval_h="8",
        rate_8h_eq=None,
        received_ts=iso(end),
    )
    records.append(unavailable)
    current = funding_record_fixture("SETTLED")
    current.update(
        settlement_time=iso(end),
        funding_time=iso(end),
        as_of=iso(end),
        rate="0.01",
        interval_h="8",
        rate_8h_eq=None,
        received_ts=iso(end),
    )
    records.append(current)
    owned = calculator(records)
    interval = FundingWindowRequest(
        start=Timestamp(value=iso(start), precision="EXACT", precision_reason=None),
        cutoff=Timestamp(value=iso(end), precision="EXACT", precision_reason=None),
        assembled_at=Timestamp(value=iso(end), precision="COLLECTION_ONLY", precision_reason=None),
    )
    actual_policy = FundingHistoryPolicy(
        minimum_history_days=7,
        lookback_days=90,
        quantiles=(0.025, 0.975),
        normalization=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW"),
    )
    history = funding_historical_population(
        owned,
        interval,
        instrument=owned.source.instruments[0],
        policy=actual_policy,
    )
    assert len(history.sources) == len(history.values) == sample_count + 1
    assert sum(point.value is not None for point in history.values) == sample_count
    unavailable_values = [point for point in history.values if point.value is None]
    assert len(unavailable_values) == 1
    assert unavailable_values[0].source.rate.status == "NOT_CAPTURED"
    normal = history.normal_range()
    assert normal.reason == expected
    if normal.bounds is not None:
        assert normal.bounds.lower == normal.bounds.upper == Decimal("0.0001")
        assert Decimal("0.01") > normal.bounds.upper
    calculation = condition_funding_family(
        owned,
        interval,
        history_policy=actual_policy,
        history_cache=BaselineCaches.with_capacity(128).funding_history,
        window="W24H",
    )
    funding = calculation.result.per_venue[0].funding
    assert isinstance(funding, AvailableConditionFunding)
    assert funding.funding_vs_normal == ("ABOVE" if sample_count == 22 else None)


def test_unmaterializable_finite_contribution_cannot_be_filtered_from_known_population() -> None:
    records = rows()
    failed = funding_record_fixture("SETTLED")
    failed.update(
        settlement_time="2026-10-05T08:00:00Z",
        funding_time="2026-10-05T08:00:00Z",
        rate=f"1E{MAX_EMAX}",
        interval_h=f"1E-{MAX_EMAX}",
        rate_8h_eq=None,
    )
    records.append(failed)
    owned = calculator(records)
    original = policy()
    actual_policy = FundingHistoryPolicy(
        minimum_history_days=original.minimum_history_days,
        lookback_days=original.lookback_days,
        quantiles=original.quantiles,
        normalization=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW"),
    )
    history = funding_historical_population(
        owned,
        request(),
        instrument=owned.source.instruments[0],
        policy=actual_policy,
    )
    assert len(history.values) == 3
    assert sum(point.value is not None for point in history.values) == 2
    assert history.normal_range().reason == "NOT_ESTIMABLE"
    assert history.normal_range().bounds is None
