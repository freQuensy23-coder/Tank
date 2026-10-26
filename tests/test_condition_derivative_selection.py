"""Requested derivative gates cannot be revived by a richer state graph."""

from datetime import timedelta
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import ValidationError
from test_condition_graph_contracts import (
    AT,
    enriched_derivatives_without_observations,
    observed_oi_without_usd,
)

from tank.analysis.condition_graph_codec import encode_requested_condition_derivatives
from tank.analysis.condition_graph_contracts import (
    AssessedConditionProofFreshness,
    AvailableConditionFunding,
    BasicConditionDerivatives,
    ConditionDerivativeVenue,
    ConditionDerivativeVenueSupport,
    ConditionEvaluationProof,
    ConditionFieldObservation,
    ConditionFundingObservation,
    ConditionObservedPredicate,
    ConditionOpenInterestCoverage,
    ConditionRequestedDerivativeView,
    ConditionRequirement,
    ConditionSynchronizedOpenInterest,
    ConditionTopCacheKey,
    ConditionUnavailableFundingDispersion,
    ConditionUnavailablePredicate,
    DeclaredConditionCadence,
    DeclaredMetadataCadenceOrigin,
    EnrichedConditionDerivativeVenue,
    InstrumentCadenceScope,
    UnavailableConditionFunding,
    UnavailableConditionOpenInterest,
    UncapturedCadenceIdentity,
    UnrequestedConditionFunding,
    UnrequestedConditionFundingDispersion,
    UnrequestedConditionIncrementalVolumes,
    UnrequestedConditionLiquidations,
    UnrequestedConditionOpenInterest,
    UnrequestedConditionSynchronizedOpenInterest,
)
from tank.analysis.statistics_contracts import DecimalBounds, DecimalRange
from tank.time_precision import Timestamp, collection_clock
from tank.util import iso


def derivative_view(
    domain: Literal["oi", "funding"], age_s: Literal[0, 60]
) -> ConditionRequestedDerivativeView:
    base = enriched_derivatives_without_observations()
    at = AT - timedelta(seconds=age_s)
    stamp = Timestamp(value=iso(at), precision=None, precision_reason="SOURCE_PRECISION_NOT_CAPTURED")
    oi = observed_oi_without_usd()
    oi = type(oi).model_validate(
        oi.model_copy(
            update={
                "start_observed_at": Timestamp(
                    value=iso(at - timedelta(hours=1)),
                    precision=None,
                    precision_reason="SOURCE_PRECISION_NOT_CAPTURED",
                ),
                "end_observed_at": stamp,
            }
        )
    )
    funding = AvailableConditionFunding(
        status="OBSERVED",
        settled=ConditionFundingObservation(
            kind="SETTLED",
            rate=Decimal(0),
            interval_h=Decimal(8),
            equivalent_eight_hour_rate=Decimal(0),
            equivalent_rate_reason=None,
            observed_at=stamp,
        ),
        estimate=None,
        start=None,
        selected_kind="SETTLED",
        normal_range=DecimalRange(
            bounds=DecimalBounds(lower=Decimal("-.01"), upper=Decimal(".01")), reason=None
        ),
        funding_vs_normal="WITHIN",
    )
    source_venue = ConditionDerivativeVenue(
        venue="bybit",
        instrument_id="opaque-perp-instrument",
        open_interest=oi if domain == "oi" else UnrequestedConditionOpenInterest(status="NOT_REQUESTED"),
        funding=funding if domain == "funding" else UnrequestedConditionFunding(status="NOT_REQUESTED"),
        liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
        oi_temporal_exclusions=None,
        basis_bps=None,
        basis_reason="INCOMPLETE",
    )
    source = BasicConditionDerivatives(
        kind="BASIC",
        token_id="T",
        interval=base.interval,
        per_venue=(source_venue,),
        synchronized=ConditionSynchronizedOpenInterest(status="OBSERVED", venues=("bybit",), values=oi)
        if domain == "oi"
        else UnrequestedConditionSynchronizedOpenInterest(status="NOT_REQUESTED"),
        funding_dispersion=ConditionUnavailableFundingDispersion(
            status="UNAVAILABLE", reason="INSUFFICIENT_OBSERVATIONS", observation_count=1
        )
        if domain == "funding"
        else UnrequestedConditionFundingDispersion(status="NOT_REQUESTED"),
        liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
        incremental_volume=UnrequestedConditionIncrementalVolumes(status="NOT_REQUESTED"),
    )
    missing = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
    support = ConditionDerivativeVenueSupport(
        status="COMPUTED",
        perp_taker_buy_share=0.75,
        perp_taker_buy_share_reason=None,
        perp_volume_usd=Decimal(123),
        perp_volume_reason=None,
        basis_normal_range=DecimalRange(
            bounds=DecimalBounds(lower=Decimal(0), upper=Decimal(10)), reason=None
        ),
        basis_vs_normal="WITHIN",
        basis_widening=missing,
        funding_rising=missing,
    )
    state_venue = EnrichedConditionDerivativeVenue(
        venue=source_venue.venue,
        instrument_id=source_venue.instrument_id,
        open_interest=oi
        if domain == "oi"
        else UnavailableConditionOpenInterest(status="UNAVAILABLE", reason="INCOMPLETE", detail=None),
        funding=funding
        if domain == "funding"
        else UnavailableConditionFunding(
            status="UNAVAILABLE",
            reason="INCOMPLETE",
            settled=None,
            estimate=None,
            start=None,
            normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        ),
        liquidations=source_venue.liquidations,
        oi_temporal_exclusions=None,
        basis_bps=Decimal(7),
        basis_reason=None,
        support=support,
    )
    state = type(base).model_validate(
        base.model_copy(
            update={
                "per_venue": (state_venue,),
                "synchronized": source.synchronized if domain == "oi" else base.synchronized,
                "funding_dispersion": source.funding_dispersion
                if domain == "funding"
                else ConditionUnavailableFundingDispersion(
                    status="UNAVAILABLE", reason="INSUFFICIENT_OBSERVATIONS", observation_count=0
                ),
                "support_features": type(base.support_features).model_validate(
                    base.support_features.model_copy(
                        update={
                            "basis_within_normal": ConditionObservedPredicate(status="OBSERVED", value=True)
                        }
                    )
                ),
            }
        )
    )
    cadence = DeclaredConditionCadence(
        status="DECLARED",
        seconds=10.0,
        scope=InstrumentCadenceScope(
            kind="INSTRUMENT", collection=domain, token_id="T", instrument_id=source_venue.instrument_id
        ),
        origin=DeclaredMetadataCadenceOrigin(
            kind="DECLARED_METADATA",
            selector="fixture.capture.cadence_s",
            declaration_identity=UncapturedCadenceIdentity(
                status="UNRECORDED", reason="DECLARATION_IDENTITY_NOT_CAPTURED"
            ),
        ),
    )
    observed = ConditionFieldObservation(
        collection=domain,
        token_id="T",
        instrument_id=source_venue.instrument_id,
        metric_key=None,
        observation_kind=None,
        chain=None,
        observed_at=stamp,
        cadence=cadence,
        age_s=float(age_s),
        fresh=age_s == 0,
        reason=None if age_s == 0 else "INCOMPLETE",
        stream=None,
    )
    requirement = ConditionRequirement(
        metric="deriv.native_oi_chg_pct" if domain == "oi" else "deriv.funding_vs_normal", window="W1H"
    )
    proof = ConditionEvaluationProof(
        requirement=requirement,
        cache_key=ConditionTopCacheKey(
            domain=domain,
            token_id="T",
            window="W1H",
            requested_duration_s=3600.0,
            cutoff=AT,
            content_sha256="a" * 64,
            visibility_sha256="b" * 64,
            dependency_identity="captured-perp-head",
            config_sha256="c" * 64,
            expected_direction_sha256="d" * 64,
        ),
        assembled_at=collection_clock(),
        reused=True,
        available=age_s == 0,
        reason=None if age_s == 0 else "INCOMPLETE",
        freshness=AssessedConditionProofFreshness(kind="ASSESSED", observations=(observed,)),
        coverage=ConditionOpenInterestCoverage(venues=("bybit",)) if domain == "oi" else None,
    )
    return ConditionRequestedDerivativeView(requested=source, state=state, proofs=(proof,))


@pytest.mark.parametrize("domain", ["oi", "funding"])
def test_stale_requested_metric_stays_blocked_without_losing_unrelated_state_values(
    domain: Literal["oi", "funding"],
) -> None:
    view = derivative_view(domain, 60)
    other: Literal["oi", "funding"] = "funding" if domain == "oi" else "oi"
    assert not view.requested_domain(other)
    with pytest.raises(ValueError, match="actually requested"):
        view.freshness_passes(other)
    wire = encode_requested_condition_derivatives(view)
    per = wire["per_venue"]
    assert isinstance(per, list) and isinstance(per[0], dict)
    assert per[0]["basis_bps"] == "7" and per[0]["perp_volume_usd"] == "123"
    if domain == "oi":
        assert wire["synchronized"] is None and wire["synchronized_reason"] == "FRESHNESS_GATE_FAILED"
        source = wire["oi_source_facts"]
        assert isinstance(source, dict) and source["status"] == "OBSERVED"
    else:
        assert per[0]["funding_vs_normal"] is None
        assert per[0]["funding_evaluation_reason"] == "FRESHNESS_GATE_FAILED"
        source = per[0]["funding_source_facts"]
        assert isinstance(source, dict) and source["funding_vs_normal"] == "WITHIN"
        assert isinstance(view.requested.per_venue[0].funding, AvailableConditionFunding)
        assert view.requested.per_venue[0].funding.funding_vs_normal == "WITHIN"


@pytest.mark.parametrize("domain", ["oi", "funding"])
def test_fresh_requested_metric_keeps_known_zero_and_actual_financial_result(
    domain: Literal["oi", "funding"],
) -> None:
    wire = encode_requested_condition_derivatives(derivative_view(domain, 0))
    if domain == "oi":
        source = wire["synchronized"]
        assert (
            isinstance(source, dict)
            and source["oi_native_end"] == "0"
            and source["oi_native_chg_pct"] == -1.0
        )
    else:
        rows = wire["per_venue"]
        assert isinstance(rows, list) and isinstance(rows[0], dict)
        assert rows[0]["funding_vs_normal"] == "WITHIN" and rows[0]["funding_8h_eq"] == "0"


def test_requested_view_rejects_foreign_freshness_instrument_and_other_observation_clock() -> None:
    view = derivative_view("funding", 60)
    proof = view.proofs[0]
    assert isinstance(proof.freshness, AssessedConditionProofFreshness)
    observation = proof.freshness.observations[0]
    assert isinstance(observation, ConditionFieldObservation)
    cadence = type(observation.cadence).model_validate(
        observation.cadence.model_copy(
            update={"scope": observation.cadence.scope.model_copy(update={"instrument_id": "foreign"})}
        )
    )
    foreign = type(observation).model_validate(
        observation.model_copy(update={"instrument_id": "foreign", "cadence": cadence})
    )
    changed = type(proof).model_validate(
        proof.model_copy(
            update={"freshness": AssessedConditionProofFreshness(kind="ASSESSED", observations=(foreign,))}
        )
    )
    with pytest.raises(ValidationError, match="selected derivative cohort"):
        type(view).model_validate(view.model_copy(update={"proofs": (changed,)}))
    other_clock = type(observation).model_validate(
        observation.model_copy(
            update={
                "observed_at": Timestamp(
                    value="2026-10-17T11:58:00Z",
                    precision=None,
                    precision_reason="SOURCE_PRECISION_NOT_CAPTURED",
                ),
                "age_s": 120.0,
            }
        )
    )
    changed = type(proof).model_validate(
        proof.model_copy(
            update={
                "freshness": AssessedConditionProofFreshness(kind="ASSESSED", observations=(other_clock,))
            }
        )
    )
    with pytest.raises(ValidationError, match="another financial observation clock"):
        type(view).model_validate(view.model_copy(update={"proofs": (changed,)}))


def test_observed_synchronized_oi_requires_an_actual_nonempty_distinct_cohort() -> None:
    point = observed_oi_without_usd()
    with pytest.raises(ValidationError):
        ConditionSynchronizedOpenInterest(status="OBSERVED", venues=(), values=point)
    with pytest.raises(ValidationError, match="distinct actual cohort"):
        ConditionSynchronizedOpenInterest(status="OBSERVED", venues=("bybit", "bybit"), values=point)


@pytest.mark.parametrize("domain", ["oi", "funding"])
def test_shared_observed_derivatives_cannot_change_native_endpoints_or_raw_rates(
    domain: Literal["oi", "funding"],
) -> None:
    view = derivative_view(domain, 0)
    state = view.state
    assert state is not None
    venue = state.per_venue[0]
    if domain == "funding":
        funding = venue.funding
        assert isinstance(funding, AvailableConditionFunding) and funding.settled is not None
        point = type(funding.settled).model_validate(
            funding.settled.model_copy(
                update={"rate": Decimal(".01"), "equivalent_eight_hour_rate": Decimal(".01")}
            )
        )
        changed = type(funding).model_validate(funding.model_copy(update={"settled": point}))
        venue = type(venue).model_validate(venue.model_copy(update={"funding": changed}))
        state = type(state).model_validate(state.model_copy(update={"per_venue": (venue,)}))
        message = "Shared funding observations"
    else:
        from tank.analysis.condition_graph_contracts import ObservedConditionOpenInterest

        native_point = venue.open_interest
        assert isinstance(native_point, ObservedConditionOpenInterest)
        changed_oi = type(native_point).model_validate(
            native_point.model_copy(update={"oi_native_end": Decimal(1), "oi_native_chg_pct": -0.9})
        )
        venue = type(venue).model_validate(venue.model_copy(update={"open_interest": changed_oi}))
        state = type(state).model_validate(
            state.model_copy(
                update={
                    "per_venue": (venue,),
                    "synchronized": ConditionSynchronizedOpenInterest(
                        status="OBSERVED", venues=(venue.venue,), values=changed_oi
                    ),
                }
            )
        )
        message = "Shared observed native OI"
    with pytest.raises(ValidationError, match=message):
        type(view).model_validate(view.model_copy(update={"state": state}))


def test_requested_view_does_not_coerce_another_state_analysis_window() -> None:
    from tank.analysis.condition_graph_contracts import ConditionInterval

    view = derivative_view("funding", 0)
    state = view.state
    assert state is not None
    window = ConditionInterval(window="W4H", start=AT - timedelta(hours=4), cutoff=AT)
    support = state.support_features
    support = type(support).model_validate(
        support.model_copy(
            update={
                "spot_volume": type(support.spot_volume).model_validate(
                    support.spot_volume.model_copy(update={"window": "W4H"})
                ),
                "perp_volume": type(support.perp_volume).model_validate(
                    support.perp_volume.model_copy(update={"window": "W4H"})
                ),
            }
        )
    )
    state = type(state).model_validate(
        state.model_copy(
            update={
                "interval": window,
                "lead_lag": type(state.lead_lag).model_validate(
                    state.lead_lag.model_copy(update={"interval": window})
                ),
                "support_features": support,
            }
        )
    )
    with pytest.raises(ValidationError, match="different subject or interval"):
        type(view).model_validate(view.model_copy(update={"state": state}))


def test_missing_requested_funding_is_not_replaced_by_observed_state_data() -> None:
    view = derivative_view("funding", 0)
    source = view.requested
    row = source.per_venue[0]
    missing = UnavailableConditionFunding(
        status="UNAVAILABLE",
        reason="INCOMPLETE",
        settled=None,
        estimate=None,
        start=None,
        normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
    )
    row = type(row).model_validate(row.model_copy(update={"funding": missing}))
    source = type(source).model_validate(
        source.model_copy(
            update={
                "per_venue": (row,),
                "funding_dispersion": ConditionUnavailableFundingDispersion(
                    status="UNAVAILABLE", reason="INSUFFICIENT_OBSERVATIONS", observation_count=0
                ),
            }
        )
    )
    proof = type(view.proofs[0]).model_validate(
        view.proofs[0].model_copy(update={"available": False, "reason": "INCOMPLETE"})
    )
    view = type(view).model_validate(view.model_copy(update={"requested": source, "proofs": (proof,)}))
    wire = encode_requested_condition_derivatives(view)
    rows = wire["per_venue"]
    assert isinstance(rows, list) and isinstance(rows[0], dict)
    assert rows[0]["funding_vs_normal"] is None
    assert rows[0]["funding_evaluation_reason"] == "INCOMPLETE"
    assert rows[0]["basis_bps"] == "7"
    state = rows[0]["state_funding_facts"]
    assert isinstance(state, dict) and state["funding_vs_normal"] == "WITHIN"


@pytest.mark.parametrize("case", ["MISSING", "EXTRA"])
def test_same_window_views_cannot_silently_change_the_token_instrument_universe(case: str) -> None:
    from tank.analysis.condition_graph_contracts import ConditionObservedFundingDispersion

    view = derivative_view("funding", 0)
    state = view.state
    assert state is not None
    if case == "MISSING":
        support = type(state.support_features).model_validate(
            state.support_features.model_copy(
                update={
                    "basis_within_normal": ConditionUnavailablePredicate(
                        status="UNAVAILABLE", reason="INCOMPLETE"
                    )
                }
            )
        )
        state = type(state).model_validate(
            state.model_copy(
                update={
                    "per_venue": (),
                    "support_features": support,
                    "funding_dispersion": ConditionUnavailableFundingDispersion(
                        status="UNAVAILABLE", reason="INSUFFICIENT_OBSERVATIONS", observation_count=0
                    ),
                }
            )
        )
    else:
        other = type(state.per_venue[0]).model_validate(
            state.per_venue[0].model_copy(update={"venue": "okx", "instrument_id": "other-opaque-instrument"})
        )
        state = type(state).model_validate(
            state.model_copy(
                update={
                    "per_venue": (*state.per_venue, other),
                    "funding_dispersion": ConditionObservedFundingDispersion(
                        status="OBSERVED", value=Decimal(0), observation_count=2
                    ),
                }
            )
        )
    with pytest.raises(ValidationError, match="same actual instrument universe"):
        type(view).model_validate(view.model_copy(update={"state": state}))
