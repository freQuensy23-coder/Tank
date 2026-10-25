from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_graph_contracts import (
    ConditionAbsorptionDivergenceCandidate,
    ConditionDirectionalDivergenceCandidate,
    ConditionDivergenceEvaluation,
    ConditionMoveCandidates,
    ConditionObservedPredicate,
    ConditionProposedMove,
    ConditionUnavailablePredicate,
    UnavailableConditionProposedMove,
)


def observed(value: bool) -> ConditionObservedPredicate:
    return ConditionObservedPredicate(status="OBSERVED", value=value)


def unknown() -> ConditionUnavailablePredicate:
    return ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")


def triggered() -> ConditionDivergenceEvaluation:
    return ConditionDivergenceEvaluation(
        trigger=observed(True),
        candidates=(
            ConditionDirectionalDivergenceCandidate(
                state="PERP_LED_MARKUP",
                eligibility=observed(False),
                refutation=unknown(),
                support=None,
                support_reason="NOT_ELIGIBLE",
            ),
            ConditionAbsorptionDivergenceCandidate(
                state="ABSORPTION_CANDIDATE",
                eligibility=observed(True),
                refutation=unknown(),
                confirmation=unknown(),
                support=0,
                support_reason=None,
            ),
            ConditionDirectionalDivergenceCandidate(
                state="LIQUIDITY_VACUUM",
                eligibility=observed(False),
                refutation=unknown(),
                support=None,
                support_reason="NOT_ELIGIBLE",
            ),
            ConditionDirectionalDivergenceCandidate(
                state="FRAGMENTATION",
                eligibility=unknown(),
                refutation=unknown(),
                support=None,
                support_reason="NOT_ELIGIBLE",
            ),
        ),
    )


def test_missing_confirmation_preserves_candidate_and_known_zero_support() -> None:
    evaluation = triggered()
    assert evaluation.state() == "ABSORPTION_CANDIDATE"
    chosen = evaluation.selected()
    assert chosen is not None and chosen.support == 0
    assert evaluation.unresolved() == ()
    result = ConditionMoveCandidates(
        eligible_before_divergence={},
        divergence=evaluation,
        flags=(),
    ).proposal(fragmentation=unknown())
    assert isinstance(result, ConditionProposedMove)
    assert result.move_class == "PASSIVE_SPOT_ABSORPTION" and result.support == 0
    candidate = evaluation.candidates[1]
    with pytest.raises(ValidationError):
        ConditionAbsorptionDivergenceCandidate.model_validate(
            candidate.model_copy(update={"state": "ABSORPTION_CONFIRMED"})
        )


def test_missing_trigger_is_unknown_and_observed_false_is_none() -> None:
    missing = ConditionDivergenceEvaluation(trigger=unknown(), candidates=())
    inactive = ConditionDivergenceEvaluation(trigger=observed(False), candidates=())
    assert missing.state() == "UNKNOWN"
    assert inactive.state() == "NONE"
    result = ConditionMoveCandidates(
        eligible_before_divergence={"ORGANIC_MIXED": 2},
        divergence=missing,
        flags=(),
    ).proposal(fragmentation=unknown())
    assert isinstance(result, UnavailableConditionProposedMove)
    assert result.support is None and result.reason == "TRIGGER_INPUTS_UNAVAILABLE"


def test_direct_tie_uses_written_risk_order_and_no_eligible_does_not_become_zero() -> None:
    evaluation = ConditionDivergenceEvaluation(trigger=observed(False), candidates=())
    values = ConditionMoveCandidates(
        eligible_before_divergence={"SPOT_LED_ACCUMULATION": 2, "DISTRIBUTION": 2},
        divergence=evaluation,
        flags=(),
    )
    assert values.direct_order() == ("DISTRIBUTION", "SPOT_LED_ACCUMULATION")
    result = values.proposal(fragmentation=unknown())
    assert isinstance(result, ConditionProposedMove) and result.move_class == "DISTRIBUTION"
    absent = ConditionMoveCandidates(
        eligible_before_divergence={},
        divergence=evaluation,
        flags=(),
    ).proposal(fragmentation=unknown())
    assert isinstance(absent, UnavailableConditionProposedMove)
    assert absent.support is None and absent.reason == "NO_ELIGIBLE_CLASS"


def test_fragmentation_needs_its_actual_second_path_validation() -> None:
    values = ConditionMoveCandidates(
        eligible_before_divergence={"REAL_MARKET_FRAGMENTATION": 2},
        divergence=ConditionDivergenceEvaluation(trigger=observed(False), candidates=()),
        flags=(),
    )
    refused = values.proposal(fragmentation=unknown())
    assert isinstance(refused, UnavailableConditionProposedMove)
    assert refused.reason == "UNVALIDATED_FRAGMENTATION" and refused.support is None
    accepted = values.proposal(fragmentation=observed(True))
    assert isinstance(accepted, ConditionProposedMove)
    assert accepted.move_class == "REAL_MARKET_FRAGMENTATION" and accepted.support == 2


def test_triggered_evaluation_cannot_omit_untestable_families() -> None:
    evaluation = triggered()
    with pytest.raises(ValidationError):
        ConditionDivergenceEvaluation.model_validate(
            evaluation.model_copy(update={"candidates": evaluation.candidates[:2]})
        )


@pytest.mark.parametrize(
    ("current", "previous"),
    [("1e1000", "1"), ("1e-1000", "1")],
)
def test_unrepresentable_depth_ratio_preserves_unknown_instead_of_infinity_or_zero(
    current: str,
    previous: str,
) -> None:
    from tank.analysis.condition_graph_contracts import ConditionDepthValues, UnavailableConditionRatio

    result = ConditionDepthValues(
        aggregate_depth_p1_usd=Decimal(current),
        pre_event_depth_p1_usd=Decimal(previous),
        spread_ratio=None,
    ).comparison()
    assert isinstance(result, UnavailableConditionRatio)
    assert result.value is None and result.reason == "NOT_ESTIMABLE"


def test_depth_ratio_preserves_actual_zero_numerator() -> None:
    from tank.analysis.condition_graph_contracts import ConditionDepthValues, ObservedConditionRatio

    result = ConditionDepthValues(
        aggregate_depth_p1_usd=Decimal(0),
        pre_event_depth_p1_usd=Decimal(2),
        spread_ratio=None,
    ).comparison()
    assert isinstance(result, ObservedConditionRatio) and result.value == 0
