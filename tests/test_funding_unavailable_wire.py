"""An unavailable estimate keeps its independently captured raw facts at the wire boundary."""

from funding_history_fixtures import calculator, policy, request, rows

from tank.analysis.baseline_cache import BaselineCaches
from tank.analysis.condition_graph_codec import encode_condition_funding
from tank.analysis.funding_family import condition_funding_family
from tank.analysis.funding_history import FundingHistoryPolicy
from tank.contracts.funding import FundingNormalizationRequest


def test_partial_estimate_does_not_hide_raw_facts_or_relabel_settled_as_current() -> None:
    records = rows()
    records[-1].update(rate=None, interval_h="-4", rate_8h_eq=None)
    owned = calculator(records)
    original = policy()
    actual_policy = FundingHistoryPolicy(
        minimum_history_days=original.minimum_history_days,
        lookback_days=original.lookback_days,
        quantiles=original.quantiles,
        normalization=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW"),
    )
    calculation = condition_funding_family(
        owned,
        request(),
        history_policy=actual_policy,
        history_cache=BaselineCaches.with_capacity(128).funding_history,
        window="W1H",
    )
    wire = encode_condition_funding(calculation.result.per_venue[0].funding)
    assert wire["funding_8h_kind"] == "SETTLED"
    assert (
        calculation.selected_reference(owned.source.instruments[0].instrument_id)
        == owned.source.observations[1].evidence
    )
    assert wire["funding_settled_last_observed_at"] == "2026-10-09T08:00:00Z"
    assert wire["funding_estimate_raw_rate"] == {"status": "NOT_CAPTURED", "value": None}
    assert wire["funding_estimate_raw_interval_h"] == {"status": "OBSERVED", "value": "-4"}
    assert wire["funding_estimate_normalization_reason"] == "RATE_NOT_CAPTURED"
    assert wire["funding_estimate_observed_at"] == "2026-10-16T12:00:00Z"
