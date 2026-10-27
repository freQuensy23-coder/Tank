"""Historical cache reuse observes actual calculation work and source corrections."""

import pytest
from funding_history_fixtures import calculator, policy, request, rows

from tank.analysis import funding_history_cache
from tank.analysis.baseline_cache import BaselineCaches
from tank.analysis.condition_derivative_inputs import DerivativeInstrument
from tank.analysis.funding_history import (
    FundingHistoricalPopulation,
    FundingHistoryPolicy,
    funding_historical_population,
)
from tank.analysis.funding_selection import FundingCalculator, FundingWindowRequest
from tank.time_precision import Timestamp


def test_cache_reuses_current_only_corrections_and_recalculates_historical_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = BaselineCaches.with_capacity(128).funding_history
    actual = funding_historical_population
    calls: list[str] = []

    def observe(
        owned: FundingCalculator,
        window: FundingWindowRequest,
        *,
        instrument: DerivativeInstrument,
        policy: FundingHistoryPolicy,
    ) -> FundingHistoricalPopulation:
        result = actual(owned, window, instrument=instrument, policy=policy)
        calls.append(result.identity())
        return result

    monkeypatch.setattr(funding_history_cache, "funding_historical_population", observe)
    records = rows()
    first_read = calculator(records)
    first = cache.baseline(
        first_read, request(), instrument=first_read.source.instruments[0], policy=policy()
    )
    records[-1]["rate_8h_eq"] = "999"
    corrected = calculator(records)
    second = cache.baseline(corrected, request(), instrument=corrected.source.instruments[0], policy=policy())
    assert second is first
    assert len(calls) == 1
    records[0]["rate_8h_eq"] = "2"
    historical = calculator(records)
    third = cache.baseline(
        historical, request(), instrument=historical.source.instruments[0], policy=policy()
    )
    assert len(calls) == 2
    assert third.normal_range != first.normal_range


def test_later_receipt_eligibility_changes_population_without_relabeling_prior_assembly() -> None:
    cache = BaselineCaches.with_capacity(128).funding_history
    records = rows()
    records[0]["received_ts"] = "2026-10-16T13:00:00Z"
    owned = calculator(records)
    earlier = request()
    first = cache.baseline(owned, earlier, instrument=owned.source.instruments[0], policy=policy())
    later = FundingWindowRequest(
        start=earlier.start,
        cutoff=earlier.cutoff,
        assembled_at=Timestamp(
            value="2026-10-16T14:00:00Z", precision="COLLECTION_ONLY", precision_reason=None
        ),
    )
    second = cache.baseline(owned, later, instrument=owned.source.instruments[0], policy=policy())
    assert first.normal_range.reason == "INSUFFICIENT_HISTORY"
    assert second.normal_range.reason is None
    assert len(first.population.sources) == 1
    assert len(second.population.sources) == 2
    assert first.population.before == second.population.before == earlier.start
    assert second.population.sources[0].received_at.value == "2026-10-16T13:00:00Z"


def test_new_history_cache_shares_existing_128_entry_fifo_budget() -> None:
    from tank.analysis.statistics_contracts import FundingBaseline

    composed = BaselineCaches.with_capacity(128)
    owned = calculator(rows())
    first = composed.funding_history.baseline(
        owned, request(), instrument=owned.source.instruments[0], policy=policy()
    )
    for index in range(128):
        composed.funding.put(str(index), FundingBaseline(equivalent_eight_hour_rate=first.normal_range))
    second = composed.funding_history.baseline(
        owned, request(), instrument=owned.source.instruments[0], policy=policy()
    )
    assert second is not first
    assert second.normal_range == first.normal_range
    assert composed.funding.get("0") is None
    assert composed.funding.get("127") is not None
