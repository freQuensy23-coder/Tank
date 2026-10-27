"""Funding-only graph construction preserves actual source phases and selected witnesses."""

from decimal import Decimal

import pytest
from funding_history_fixtures import calculator, policy, request, rows
from pydantic import ValidationError

from tank.analysis.baseline_cache import BaselineCaches
from tank.analysis.condition_graph_codec import encode_condition_funding
from tank.analysis.condition_graph_contracts import AvailableConditionFunding, UnavailableConditionFunding
from tank.analysis.funding_family import FundingConditionCalculation, condition_funding_family


def test_recorded_funding_family_uses_actual_selected_witness_and_historical_range() -> None:
    owned = calculator(rows())
    calculation = condition_funding_family(
        owned,
        request(),
        history_policy=policy(),
        history_cache=BaselineCaches.with_capacity(128).funding_history,
        window="W15M",
    )
    funding = calculation.result.per_venue[0].funding
    assert isinstance(funding, AvailableConditionFunding)
    assert funding.selected_kind == "CURRENT_ESTIMATE"
    assert funding.estimate is not None
    assert funding.estimate.equivalent_eight_hour_rate == Decimal("-0.002")
    assert funding.funding_vs_normal == "BELOW"
    selected = calculation.selected_reference(owned.source.instruments[0].instrument_id)
    assert selected == owned.source.observations[-1].evidence
    wire = encode_condition_funding(funding)
    assert wire["funding_8h_basis"] == "RECORDED_EIGHT_HOUR_EQUIVALENT"
    assert calculation.result.synchronized.status == "NOT_REQUESTED"
    assert calculation.result.per_venue[0].open_interest.status == "NOT_REQUESTED"
    assert calculation.result.incremental_volume.status == "NOT_REQUESTED"


def test_unavailable_partial_components_are_retained_without_manufactured_normalization() -> None:
    records = rows()
    records[-1].update(rate=None, interval_h="-4", rate_8h_eq=None)
    owned = calculator(records)
    calculation = condition_funding_family(
        owned,
        request(),
        history_policy=policy(),
        history_cache=BaselineCaches.with_capacity(128).funding_history,
        window="W15M",
    )
    source = next(point for point in calculation.selected if point.role == "LATEST_ESTIMATE")
    assert source.equivalent.source.rate.status == "NOT_CAPTURED"
    assert source.equivalent.source.interval_h.value == -4
    assert source.equivalent.status == "UNAVAILABLE"
    funding = calculation.result.per_venue[0].funding
    assert isinstance(funding, AvailableConditionFunding)
    assert funding.selected_kind == "SETTLED"
    assert funding.estimate is None


def test_invalid_copied_selected_source_facts_cannot_admit_an_observed_graph() -> None:
    owned = calculator(rows())
    calculation = condition_funding_family(
        owned,
        request(),
        history_policy=policy(),
        history_cache=BaselineCaches.with_capacity(128).funding_history,
        window="W15M",
    )
    with pytest.raises(ValidationError):
        FundingConditionCalculation.model_validate(calculation.model_copy(update={"selected": ()}))


def test_known_zero_current_rate_survives_unavailable_historical_range() -> None:
    records = rows()[-1:]
    records[0]["rate_8h_eq"] = "0"
    owned = calculator(records)
    calculation = condition_funding_family(
        owned,
        request(),
        history_policy=policy(),
        history_cache=BaselineCaches.with_capacity(128).funding_history,
        window="W15M",
    )
    funding = calculation.result.per_venue[0].funding
    assert isinstance(funding, AvailableConditionFunding)
    assert funding.estimate is not None
    assert funding.estimate.equivalent_eight_hour_rate == 0
    assert funding.normal_range.reason == "INSUFFICIENT_HISTORY"
    assert funding.funding_vs_normal is None


def test_missing_receipt_cannot_supply_current_funding_or_freshness_witness() -> None:
    records = rows()[-1:]
    records[0].pop("received_ts")
    records[0].pop("received_ts_prec")
    owned = calculator(records)
    calculation = condition_funding_family(
        owned,
        request(),
        history_policy=policy(),
        history_cache=BaselineCaches.with_capacity(128).funding_history,
        window="W15M",
    )
    assert isinstance(calculation.result.per_venue[0].funding, UnavailableConditionFunding)
    assert calculation.selected_reference(owned.source.instruments[0].instrument_id) is None
    assert calculation.selected == ()
