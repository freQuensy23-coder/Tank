from datetime import UTC, datetime, timedelta
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
    SelectiveConditionValues,
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


def test_cache_identity_keeps_subject_and_all_actual_dependency_coordinates() -> None:
    from tank.analysis.condition_graph_contracts import ConditionStateNodeKey, ConditionTopCacheKey

    cutoff = datetime(2026, 10, 17, 12, tzinfo=UTC)
    key = ConditionTopCacheKey(
        domain="spot",
        token_id="opaque-token",
        window="W1H",
        requested_duration_s=3600,
        cutoff=cutoff,
        dependency_identity="explicit-provider-head",
        config_sha256="a" * 64,
        expected_direction_sha256="b" * 64,
    )
    assert key in {key}
    changes: tuple[dict[str, str | float], ...] = (
        {"token_id": "other-token"},
        {"domain": "oi"},
        {"window": "W4H"},
        {"requested_duration_s": 7200.0},
        {"dependency_identity": "another-head"},
        {"config_sha256": "c" * 64},
        {"expected_direction_sha256": "d" * 64},
    )
    for change in changes:
        different = ConditionTopCacheKey.model_validate(key.model_copy(update=change))
        assert different != key and different not in {key}
    later = ConditionTopCacheKey.model_validate(
        key.model_copy(update={"cutoff": cutoff + timedelta(seconds=1)})
    )
    assert later != key
    node = ConditionStateNodeKey(
        node="spot",
        token_id=key.token_id,
        start=cutoff - timedelta(hours=1),
        cutoff=cutoff,
        dependency_identity=key.dependency_identity,
        config_sha256=key.config_sha256,
        expected_direction_sha256=key.expected_direction_sha256,
    )
    assert node in {node}
    with pytest.raises(ValidationError):
        ConditionStateNodeKey.model_validate(node.model_copy(update={"start": cutoff}))
    with pytest.raises(ValidationError):
        ConditionTopCacheKey.model_validate(key.model_copy(update={"cutoff": cutoff.replace(tzinfo=None)}))
    with pytest.raises(ValidationError):
        ConditionTopCacheKey.model_validate(key.model_copy(update={"config_sha256": "current"}))


def selective_close() -> SelectiveConditionValues:
    from tank.analysis.condition_graph_contracts import (
        ConditionEvaluationProof,
        ConditionRequirement,
        ConditionTopCacheKey,
        ConditionUnavailableRequirement,
        SelectiveConditionValues,
        UnappliedConditionProofFreshness,
        UnavailableConditionClose,
        UnrequestedConditionFacet,
    )
    from tank.time_precision import Timestamp

    stamp = Timestamp(value="2026-10-17T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED")
    requirement = ConditionRequirement(metric="price.close_vs_level", window="W1H")
    return SelectiveConditionValues(
        token_id="T",
        cutoff=stamp,
        requested=(requirement,),
        spot={},
        derivatives={},
        close=UnavailableConditionClose(
            status="UNAVAILABLE",
            reference_instrument_id=None,
            price=None,
            observed_at=Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
            reason="INCOMPLETE",
        ),
        confidence=UnrequestedConditionFacet(status="NOT_REQUESTED"),
        depth=UnrequestedConditionFacet(status="NOT_REQUESTED"),
        state=UnrequestedConditionFacet(status="NOT_REQUESTED"),
        relative=UnrequestedConditionFacet(status="NOT_REQUESTED"),
        proofs=(
            ConditionEvaluationProof(
                requirement=requirement,
                cache_key=ConditionTopCacheKey(
                    domain="close",
                    token_id="T",
                    window="W1H",
                    requested_duration_s=3600,
                    cutoff=datetime(2026, 10, 17, 12, tzinfo=UTC),
                    dependency_identity="actual-head",
                    config_sha256="a" * 64,
                    expected_direction_sha256="b" * 64,
                ),
                reused=False,
                available=False,
                reason="INCOMPLETE",
                freshness=UnappliedConditionProofFreshness(
                    kind="NOT_APPLIED", reason="NO_ADDITIONAL_FRESHNESS_GATE"
                ),
                coverage=None,
            ),
        ),
        unavailable_metrics=(ConditionUnavailableRequirement(requirement=requirement, reason="INCOMPLETE"),),
        request_sha256="c" * 64,
        transient=True,
    )


def test_selected_missing_close_is_unavailable_instead_of_not_requested() -> None:
    from tank.analysis.condition_graph_contracts import SelectiveConditionValues, UnrequestedConditionFacet

    graph = selective_close()
    assert graph.proofs[0].available is False
    assert graph.close.status == "UNAVAILABLE"
    with pytest.raises(ValidationError, match="selected fact"):
        SelectiveConditionValues.model_validate(
            graph.model_copy(update={"close": UnrequestedConditionFacet(status="NOT_REQUESTED")})
        )
    with pytest.raises(ValidationError, match="evaluated proof"):
        SelectiveConditionValues.model_validate(graph.model_copy(update={"proofs": ()}))
    with pytest.raises(ValidationError, match="subject or cutoff"):
        SelectiveConditionValues.model_validate(graph.model_copy(update={"token_id": "OTHER"}))


def test_proof_freshness_requires_real_observations_and_binds_actual_cutoff() -> None:
    from test_condition_graph_contracts import AT, scoped_field_observation

    from tank.analysis.condition_graph_contracts import (
        AssessedConditionProofFreshness,
        ConditionEvaluationProof,
        ConditionRequirement,
        ConditionTopCacheKey,
        UnappliedConditionProofFreshness,
    )

    with pytest.raises(ValidationError):
        AssessedConditionProofFreshness(kind="ASSESSED", observations=())
    observation = scoped_field_observation("INSTRUMENT")
    proof = ConditionEvaluationProof(
        requirement=ConditionRequirement(metric="liq.spread_ratio", window="W1H"),
        cache_key=ConditionTopCacheKey(
            domain="depth",
            token_id="T",
            window="W1H",
            requested_duration_s=3600,
            cutoff=AT,
            dependency_identity="book-head",
            config_sha256="a" * 64,
            expected_direction_sha256="b" * 64,
        ),
        reused=True,
        available=True,
        reason=None,
        freshness=AssessedConditionProofFreshness(kind="ASSESSED", observations=(observation,)),
        coverage=None,
    )
    with pytest.raises(ValidationError, match="cutoff"):
        ConditionEvaluationProof.model_validate(
            proof.model_copy(
                update={"cache_key": proof.cache_key.model_copy(update={"cutoff": AT + timedelta(seconds=1)})}
            )
        )
    with pytest.raises(ValidationError, match="applicability"):
        ConditionEvaluationProof.model_validate(
            proof.model_copy(
                update={
                    "freshness": UnappliedConditionProofFreshness(
                        kind="NOT_APPLIED", reason="NO_ADDITIONAL_FRESHNESS_GATE"
                    )
                }
            )
        )


def test_selected_proof_cannot_claim_a_missing_financial_fact_is_available() -> None:
    from tank.analysis.condition_graph_contracts import ConditionEvaluationProof, SelectiveConditionValues

    graph = selective_close()
    false_success = ConditionEvaluationProof.model_validate(
        graph.proofs[0].model_copy(update={"available": True, "reason": None})
    )
    with pytest.raises(ValidationError, match="financial facts"):
        SelectiveConditionValues.model_validate(
            graph.model_copy(update={"proofs": (false_success,), "unavailable_metrics": ()})
        )


def test_selected_unsupported_fact_is_distinct_from_unrequested_and_evaluated_missing() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionUnavailableRequirement,
        SelectiveConditionValues,
        UnsupportedConditionFacet,
    )

    graph = selective_close()
    requirement = graph.requested[0].model_copy(update={"window": "EVENT"})
    unsupported = SelectiveConditionValues.model_validate(
        graph.model_copy(
            update={
                "requested": (requirement,),
                "proofs": (),
                "close": UnsupportedConditionFacet(status="NOT_SUPPORTED", requirements=(requirement,)),
                "unavailable_metrics": (
                    ConditionUnavailableRequirement(requirement=requirement, reason="NOT_SUPPORTED"),
                ),
            }
        )
    )
    assert unsupported.close.status == "NOT_SUPPORTED"
    assert unsupported.proofs == ()


def test_selected_metric_cannot_pass_a_required_freshness_gate_without_observations() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionEvaluationProof,
        ConditionRequirement,
        ConditionTopCacheKey,
        UnavailableConditionProofFreshness,
    )

    proof = ConditionEvaluationProof(
        requirement=ConditionRequirement(metric="deriv.funding_vs_normal", window="W1H"),
        cache_key=ConditionTopCacheKey(
            domain="funding",
            token_id="T",
            window="W1H",
            requested_duration_s=3600,
            cutoff=datetime(2026, 10, 17, 12, tzinfo=UTC),
            dependency_identity="funding-head",
            config_sha256="a" * 64,
            expected_direction_sha256="b" * 64,
        ),
        reused=False,
        available=False,
        reason="INCOMPLETE",
        freshness=UnavailableConditionProofFreshness(kind="UNAVAILABLE", reason="NO_REQUIRED_OBSERVATIONS"),
        coverage=None,
    )
    with pytest.raises(ValidationError, match="unproven freshness"):
        ConditionEvaluationProof.model_validate(proof.model_copy(update={"available": True}))
