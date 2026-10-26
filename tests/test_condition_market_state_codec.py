"""State wire phases preserve genuine unavailable facts without padded observations."""

from collections.abc import Mapping
from datetime import timedelta

import pytest
from pydantic import JsonValue, ValidationError
from test_condition_graph_codec import UnrequestedRelativeEvidence, condition_wire_clocks
from test_condition_graph_contracts import (
    AT,
    empty_supply_graph,
    enriched_derivatives_without_observations,
    enriched_liquidity,
    relative_graph_without_event,
)
from test_spot_enrichment import cohort_phase

from tank.analysis.condition_graph_codec import (
    encode_full_condition_market_state,
    encode_nonforward_condition_market_state,
)
from tank.analysis.condition_graph_contracts import (
    CapturedConditionLending,
    ConditionAggregateLiquidations,
    ConditionCoverageFailure,
    ConditionDivergenceEvaluation,
    ConditionIncrementalVolumeCoverage,
    ConditionIncrementalVolumes,
    ConditionLiquidationTotals,
    ConditionMoveCandidates,
    ConditionScheduledUnlock,
    ConditionUnavailableIncrementalVolume,
    ConditionUnavailablePredicate,
    ConditionWalletFlow,
    ConditionWalletFlowEvidenceRef,
    FullConditionConfidence,
    FullConditionMarketState,
    NonForwardConditionInterval,
    NonForwardConditionMarketState,
    UnavailableConditionSizeProfile,
)
from tank.analysis.spot_enrichment import finish_spot_enrichment


class NoSourceRejoin(UnrequestedRelativeEvidence):
    def resolve_wallet_flow(
        self, reference: ConditionWalletFlowEvidenceRef, *, expected: ConditionWalletFlow
    ) -> Mapping[str, JsonValue]:
        raise AssertionError("No wallet observation exists in this explicitly unavailable supply fixture")

    def resolve_unlock(self, *, expected: ConditionScheduledUnlock) -> Mapping[str, JsonValue]:
        raise AssertionError("No unlock observation exists in this explicitly unavailable supply fixture")

    def resolve_lending_observation(self, *, expected: CapturedConditionLending) -> Mapping[str, JsonValue]:
        raise AssertionError("No lending observation exists in this explicitly unavailable supply fixture")


def full_state() -> FullConditionMarketState:
    spot = finish_spot_enrichment(
        cohort_phase(),
        primary_size_profile=UnavailableConditionSizeProfile(status="UNAVAILABLE", reason="INCOMPLETE"),
    )
    primary = spot.spots[spot.primary].interval
    basic_liquidity = enriched_liquidity()
    scan_scope = type(basic_liquidity.replenishment.scope).model_validate(
        basic_liquidity.replenishment.scope.model_copy(update={"interval": primary})
    )
    scan = type(basic_liquidity.replenishment).model_validate(
        basic_liquidity.replenishment.model_copy(update={"scope": scan_scope})
    )
    liquidity = type(basic_liquidity).model_validate(
        basic_liquidity.model_copy(update={"interval": primary, "replenishment": scan})
    )
    derivative = enriched_derivatives_without_observations()
    support = derivative.support_features
    support = type(support).model_validate(
        support.model_copy(
            update={
                "spot_volume": type(support.spot_volume).model_validate(
                    support.spot_volume.model_copy(update={"window": primary.window})
                ),
                "perp_volume": type(support.perp_volume).model_validate(
                    support.perp_volume.model_copy(update={"window": primary.window})
                ),
            }
        )
    )
    missing_liquidations = ConditionLiquidationTotals(
        long=None, short=None, long_reason="INCOMPLETE", short_reason="INCOMPLETE"
    )
    missing_volume = ConditionIncrementalVolumeCoverage(
        instruments=(), complete=False, unit="USD", reason="INCOMPLETE"
    )
    derivative = type(derivative).model_validate(
        derivative.model_copy(
            update={
                "interval": primary,
                "lead_lag": type(derivative.lead_lag).model_validate(
                    derivative.lead_lag.model_copy(update={"interval": primary})
                ),
                "support_features": support,
                "liquidations": ConditionAggregateLiquidations(
                    status="REQUESTED",
                    complete_venues_total=missing_liquidations,
                    sampled_venues_min=missing_liquidations,
                ),
                "incremental_volume": ConditionIncrementalVolumes(
                    status="REQUESTED",
                    spot_coverage=missing_volume,
                    perp_coverage=missing_volume,
                    values=ConditionUnavailableIncrementalVolume(
                        status="UNAVAILABLE", reason="INCOMPLETE", spot_usd=None, perp_usd=None
                    ),
                ),
            }
        )
    )
    supply = empty_supply_graph()
    supply = type(supply).model_validate(supply.model_copy(update={"interval": primary}))
    failure = ConditionCoverageFailure(kind="COVERAGE", condition="derivatives_coverage", reason="INCOMPLETE")
    return FullConditionMarketState(
        kind="FULL_STATE",
        token_id="T",
        cutoff=AT,
        spot=spot.spots,
        primary_window=spot.primary,
        derivatives=derivative,
        liquidity=liquidity,
        supply=supply,
        relative=relative_graph_without_event(),
        data_confidence=FullConditionConfidence(
            kind="FULL",
            classification="LOW",
            failed=(failure,),
            limiting=failure,
            derivatives_native_coverage_ready=False,
            derivatives_classification_ready=False,
            freshness=(),
        ),
        candidates=ConditionMoveCandidates(
            eligible_before_divergence={},
            flags=(),
            divergence=ConditionDivergenceEvaluation(
                trigger=ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE"),
                candidates=(),
            ),
        ),
    )


def test_full_state_wire_keeps_low_confidence_unknown_without_zero_support_or_false_trigger() -> None:
    state = full_state()
    wire = encode_full_condition_market_state(state, NoSourceRejoin(), clocks=condition_wire_clocks())
    assert wire["move_class"] == "UNKNOWN" and wire["class_confidence"] == "LOW"
    assert wire["support"] is None and wire["support_reason"] == "LOW_DATA_CONFIDENCE"
    divergence = wire["divergence"]
    assert isinstance(divergence, dict) and divergence["state"] == "UNKNOWN"
    assert divergence["triggered"] is None and divergence["candidates"] == []
    assert wire["primary_window"] == "W4H"
    spot = wire["spot"]
    assert isinstance(spot, dict) and set(spot) == {"W15M", "W1H", "W4H", "W24H", "EVENT"}
    supply = wire["supply"]
    assert isinstance(supply, dict) and supply["supply_in_evidence"] == "UNKNOWN"
    relative = wire["relative"]
    assert isinstance(relative, dict) and relative["expected_direction"] is None
    assert wire["cutoff_prec"] == "COLLECTION_ONLY"


def test_full_state_wire_refuses_a_foreign_subject_before_source_lookup() -> None:
    state = full_state()
    foreign = state.model_copy(update={"token_id": "OTHER"})
    with pytest.raises(ValidationError, match="actual token"):
        encode_full_condition_market_state(foreign, NoSourceRejoin(), clocks=condition_wire_clocks())


def test_nonforward_state_needs_no_source_handle_or_dummy_financial_graph() -> None:
    state = NonForwardConditionMarketState(
        kind="NON_FORWARD",
        token_id="T",
        interval=NonForwardConditionInterval(start=AT + timedelta(minutes=1), cutoff=AT),
        move_class="UNKNOWN",
        divergence_state="UNKNOWN",
        confidence="LOW",
        reason="NON_FORWARD_ANALYSIS_WINDOW",
    )
    wire = encode_nonforward_condition_market_state(state, clocks=condition_wire_clocks())
    assert wire["move_class"] == "UNKNOWN"
    assert wire["divergence"] == {"state": "UNKNOWN"}
    assert wire["data_confidence"] == {"class": "LOW"}
    assert "spot" not in wire and "derivatives" not in wire and "supply" not in wire
    assert wire["cutoff"] == "2026-10-17T12:00:00Z"
    assert wire["analysis_window"] == {
        "start": "2026-10-17T12:01:00Z",
        "cutoff": "2026-10-17T12:00:00Z",
        "reason": "NON_FORWARD_ANALYSIS_WINDOW",
    }
