from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_graph_contracts import (
    AvailableConditionFunding,
    BasicConditionLiquidity,
    BasicConditionSpot,
    CapturedConditionChainLiquidity,
    CapturedConditionHoldersRevenue,
    CapturedConditionUnlocks,
    CapturedConditionWalletFlows,
    ComparableConditionExchangeInventory,
    ConditionBookDepthBounds,
    ConditionCadenceScope,
    ConditionCollection,
    ConditionDepthValues,
    ConditionDerivativeSupportFeatures,
    ConditionElasticityInputs,
    ConditionFieldObservation,
    ConditionFlowAggregate,
    ConditionFlowWindowQualification,
    ConditionFundingConfirmationGraph,
    ConditionFundingObservation,
    ConditionIndependentRestQuote,
    ConditionInterval,
    ConditionLendingPoolCalculation,
    ConditionPrimaryBookQuote,
    ConditionReceiptVisibility,
    ConditionRelativeGraph,
    ConditionRelativeWindow,
    ConditionReplenishmentBurst,
    ConditionReplenishmentScope,
    ConditionSplitExecution,
    ConditionSupplyGraph,
    ConditionTopVenueSignHistory,
    DeclaredConditionCadence,
    NonForwardConditionInterval,
    ObservedConditionClose,
    ObservedConditionOpenInterest,
    UnavailableConditionClose,
    UnavailableConditionFunding,
    WindowedConditionSpot,
)
from tank.analysis.positioning import MissingOIDecomposition
from tank.analysis.statistics_contracts import DecimalBounds, DecimalRange, FloatBounds, FloatRange
from tank.market.trade_contracts import QuoteUsdValuation
from tank.time_precision import Timestamp

AT = datetime(2026, 10, 17, 12, tzinfo=UTC)


def test_known_zero_current_depth_is_not_missing_or_a_zero_baseline() -> None:
    depth = ConditionDepthValues(
        aggregate_depth_p1_usd=Decimal(0), pre_event_depth_p1_usd=Decimal(12), spread_ratio=0.0
    )
    ratio = depth.comparison()
    assert ratio.status == "OBSERVED" and ratio.value == 0.0
    baseline_zero = depth.model_copy(update={"pre_event_depth_p1_usd": Decimal(0)})
    undefined = baseline_zero.comparison()
    assert undefined.status == "UNAVAILABLE" and undefined.value is None
    assert undefined.reason == "ZERO_BASELINE"
    absent = depth.model_copy(update={"aggregate_depth_p1_usd": None}).comparison()
    assert absent.status == "UNAVAILABLE" and absent.reason == "INCOMPLETE"


def test_reference_price_preserves_unknown_observation_precision() -> None:
    unknown_clock = Timestamp(
        value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
    )
    close = ObservedConditionClose(
        status="OBSERVED",
        reference_instrument_id="venue:spot:T",
        price=Decimal("12.123456789123456789"),
        observed_at=unknown_clock,
    )
    assert close.price == Decimal("12.123456789123456789")
    assert close.observed_at.precision is None
    missing_clock = Timestamp(value=None, precision=None, precision_reason="OBSERVATION_METADATA_UNAVAILABLE")
    known_price = close.model_copy(update={"observed_at": missing_clock})
    validated = ObservedConditionClose.model_validate(known_price)
    assert validated.price == close.price and validated.observed_at.value is None


def test_missing_close_does_not_gain_a_price_reference_or_clock() -> None:
    missing = Timestamp(value=None, precision=None, precision_reason="INCOMPLETE")
    result = UnavailableConditionClose(
        status="UNAVAILABLE",
        reference_instrument_id=None,
        price=None,
        observed_at=missing,
        reason="INCOMPLETE",
    )
    assert result.reference_instrument_id is None and result.price is None
    observed = Timestamp(value="2026-10-17T12:00:00Z", precision="EXACT")
    with pytest.raises(ValidationError, match="cannot claim an observed close clock"):
        UnavailableConditionClose.model_validate(result.model_copy(update={"observed_at": observed}))


def test_interval_states_preserve_actual_bounds_without_inventing_a_window() -> None:
    valid = ConditionInterval(window="EVENT", start=AT - timedelta(hours=1), cutoff=AT)
    assert valid.start == AT - timedelta(hours=1)
    with pytest.raises(ValidationError):
        ConditionInterval.model_validate(valid.model_copy(update={"start": AT}))
    non_forward = NonForwardConditionInterval(start=AT + timedelta(seconds=1), cutoff=AT)
    assert non_forward.start > non_forward.cutoff
    with pytest.raises(ValidationError):
        NonForwardConditionInterval.model_validate(
            non_forward.model_copy(update={"start": AT - timedelta(seconds=1)})
        )
    with pytest.raises(ValidationError):
        ConditionInterval.model_validate(valid.model_copy(update={"start": AT.replace(tzinfo=None)}))


def test_mutated_nonfinite_depth_cannot_produce_a_valid_ratio() -> None:
    depth = ConditionDepthValues(
        aggregate_depth_p1_usd=Decimal(10), pre_event_depth_p1_usd=Decimal(5), spread_ratio=None
    )
    invalid = depth.model_copy(update={"aggregate_depth_p1_usd": Decimal("NaN")})
    with pytest.raises(ValidationError):
        invalid.comparison()


def test_historical_plan_keeps_recorded_cadence_without_inventing_collector_identity() -> None:
    from tank.analysis.condition_graph_contracts import (
        CapturedPlanCadenceOrigin,
        DeclaredConditionCadence,
        InstrumentCadenceScope,
        UncapturedCadenceIdentity,
    )

    origin = CapturedPlanCadenceOrigin(
        kind="CAPTURED_PLAN",
        token_id="T",
        profile_field="oi_funding_s",
        transition_id="tier-transition",
        effective_at=Timestamp(value="2026-10-17T11:00:00Z", precision="COLLECTION_ONLY"),
        tier="HOT",
        plan_identity=UncapturedCadenceIdentity(status="UNRECORDED", reason="PLAN_IDENTITY_NOT_CAPTURED"),
    )
    scope = InstrumentCadenceScope(
        kind="INSTRUMENT", collection="oi", token_id="T", instrument_id="venue:perp:T"
    )
    cadence = DeclaredConditionCadence(status="DECLARED", seconds=60.0, scope=scope, origin=origin)
    assert cadence.seconds == 60.0
    assert isinstance(cadence.origin, CapturedPlanCadenceOrigin)
    assert cadence.origin.plan_identity.status == "UNRECORDED"
    wrong_token = scope.model_copy(update={"token_id": "OTHER"})
    with pytest.raises(ValidationError, match="cannot qualify another token"):
        DeclaredConditionCadence.model_validate(cadence.model_copy(update={"scope": wrong_token}))


def test_current_configured_plan_cannot_masquerade_as_a_historical_capture_plan() -> None:
    from source_support import fixture_versions

    from tank.analysis.condition_graph_contracts import (
        CollectionCadenceScope,
        CurrentPlanCadenceOrigin,
        DeclaredConditionCadence,
    )

    scope = CollectionCadenceScope(kind="COLLECTION", collection="books", token_id="T")
    origin = CurrentPlanCadenceOrigin(
        kind="CURRENT_CONFIGURED_PLAN", versions=fixture_versions(), rule="book_interval_s"
    )
    cadence = DeclaredConditionCadence(status="DECLARED", seconds=10.0, scope=scope, origin=origin)
    assert cadence.origin.kind == "CURRENT_CONFIGURED_PLAN"
    wrong_collection = scope.model_copy(update={"collection": "oi"})
    with pytest.raises(ValidationError, match="rule disagrees"):
        DeclaredConditionCadence.model_validate(cadence.model_copy(update={"scope": wrong_collection}))


def test_missing_publication_lag_remains_unknown_and_explicit_zero_is_valid() -> None:
    from tank.analysis.condition_graph_contracts import (
        DeclaredConditionCadence,
        MetricCadenceScope,
        SourceIntervalCadenceOrigin,
        UnavailableConditionCadence,
    )

    scope = MetricCadenceScope(
        kind="METRIC", collection="oi", token_id="T", metric_key="venue.perp.oi", instrument_id="venue:perp:T"
    )
    origin = SourceIntervalCadenceOrigin(
        kind="SOURCE_INTERVAL_PLUS_PUBLISH_LAG", interval_s=300.0, publish_lag_s=None
    )
    unavailable = UnavailableConditionCadence(
        status="UNAVAILABLE", seconds=None, scope=scope, origin=origin, reason="PUBLISH_LAG_NOT_CAPTURED"
    )
    assert unavailable.seconds is None and unavailable.origin == origin
    with pytest.raises(ValidationError, match="undeclared publication lag"):
        DeclaredConditionCadence(status="DECLARED", seconds=600.0, scope=scope, origin=origin)
    declared = origin.model_copy(update={"publish_lag_s": 0.0})
    valid = DeclaredConditionCadence(status="DECLARED", seconds=600.0, scope=scope, origin=declared)
    assert valid.seconds == 600.0
    with pytest.raises(ValidationError, match="actual declared publication lag"):
        DeclaredConditionCadence.model_validate(valid.model_copy(update={"seconds": 300.0}))


def declared_book_cadence() -> DeclaredConditionCadence:
    from tank.analysis.condition_graph_contracts import (
        DeclaredMetadataCadenceOrigin,
        InstrumentCadenceScope,
        UncapturedCadenceIdentity,
    )

    return DeclaredConditionCadence(
        status="DECLARED",
        seconds=10.0,
        scope=InstrumentCadenceScope(
            kind="INSTRUMENT", collection="books", token_id="T", instrument_id="venue:spot:T"
        ),
        origin=DeclaredMetadataCadenceOrigin(
            kind="DECLARED_METADATA",
            selector="capture_metadata.collections.books.cadence_s",
            declaration_identity=UncapturedCadenceIdentity(
                status="UNRECORDED", reason="DECLARATION_IDENTITY_NOT_CAPTURED"
            ),
        ),
    )


def test_book_freshness_uses_actual_declared_allowance_and_cutoff() -> None:
    from tank.analysis.condition_graph_contracts import ConditionDepthNode, ConditionDepthObservation

    observation = ConditionDepthObservation(
        token_id="T",
        instrument_id="venue:spot:T",
        metric_key=None,
        venue="venue",
        observed_at=Timestamp(
            value="2026-10-17T11:59:55Z", precision=None, precision_reason="BOOK_PRECISION_NOT_CAPTURED"
        ),
        cadence=declared_book_cadence(),
        fresh=True,
    )
    node = ConditionDepthNode(
        aggregate_depth_p1_usd=Decimal(10),
        pre_event_depth_p1_usd=Decimal(20),
        spread_ratio=1.0,
        cutoff=AT,
        observations=(observation,),
    )
    ratio = node.comparison()
    assert ratio.status == "OBSERVED" and ratio.value == 0.5
    assert node.observations[0].observed_at.precision is None
    with pytest.raises(ValidationError, match="actual cutoff and cadence"):
        ConditionDepthNode.model_validate(node.model_copy(update={"cutoff": AT + timedelta(minutes=1)}))
    assert node.cutoff == AT and node.aggregate_depth_p1_usd == Decimal(10)


def test_unknown_cadence_cannot_authorize_a_fresh_field() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionFieldObservation,
        UnavailableConditionCadence,
    )

    known = declared_book_cadence()
    unknown = UnavailableConditionCadence(
        status="UNAVAILABLE", seconds=None, scope=known.scope, origin=None, reason="CADENCE_NOT_CAPTURED"
    )
    observation = ConditionFieldObservation(
        collection="books",
        token_id="T",
        instrument_id="venue:spot:T",
        metric_key=None,
        observation_kind=None,
        chain=None,
        observed_at=Timestamp(value="2026-10-17T11:59:55Z", precision="EXACT"),
        cadence=unknown,
        age_s=5.0,
        fresh=False,
        reason="NOT_ESTIMABLE",
        stream=None,
    )
    assert not observation.fresh and observation.cadence.seconds is None
    with pytest.raises(ValidationError, match="actual age and declared cadence"):
        ConditionFieldObservation.model_validate(
            observation.model_copy(update={"fresh": True, "reason": None})
        )
    future = observation.model_copy(update={"cadence": known, "age_s": -1.0, "reason": "INCOMPLETE"})
    validated = ConditionFieldObservation.model_validate(future)
    assert validated.age_s == -1.0 and not validated.fresh


def test_selective_confidence_never_invents_derivative_classification_readiness() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionCoverageFailure,
        FullConditionConfidence,
        SelectiveConditionConfidence,
    )

    failure = ConditionCoverageFailure(kind="COVERAGE", condition="spot_coverage", reason="INCOMPLETE")
    selective = SelectiveConditionConfidence(
        kind="SELECTIVE",
        classification="LOW",
        failed=(failure,),
        limiting=failure,
        derivatives_native_coverage_ready=True,
        freshness=(),
        derivatives_classification_ready=None,
        derivatives_classification_ready_reason="NOT_ESTIMABLE",
        analysis_window=ConditionInterval(window="W4H", start=AT - timedelta(hours=4), cutoff=AT),
    )
    assert selective.derivatives_classification_ready is None
    assert selective.derivatives_classification_ready_reason == "NOT_ESTIMABLE"
    with pytest.raises(ValidationError, match="HIGH data confidence"):
        SelectiveConditionConfidence.model_validate(selective.model_copy(update={"classification": "HIGH"}))
    full = FullConditionConfidence(
        kind="FULL",
        classification="HIGH",
        failed=(),
        limiting=None,
        derivatives_native_coverage_ready=True,
        derivatives_classification_ready=True,
        freshness=(),
    )
    assert full.derivatives_classification_ready


def scoped_field_observation(kind: Literal["INSTRUMENT", "METRIC", "SUPPLY"]) -> ConditionFieldObservation:
    from tank.analysis.condition_graph_contracts import (
        DeclaredMetadataCadenceOrigin,
        InstrumentCadenceScope,
        MetricCadenceScope,
        SupplyCadenceScope,
        UncapturedCadenceIdentity,
    )

    scope: ConditionCadenceScope
    if kind == "SUPPLY":
        collection: ConditionCollection = "supply"
        instrument = None
        metric = "token.unlocks"
        observation_kind = "UNLOCKS"
        chain = "ethereum"
        scope = SupplyCadenceScope(
            kind="SUPPLY", collection="supply", token_id="T", observation_kind="UNLOCKS", chain="ethereum"
        )
    else:
        collection = "books"
        instrument = "venue:spot:T"
        metric = "venue.spot.depth"
        observation_kind = None
        chain = None
        scope = (
            InstrumentCadenceScope(
                kind="INSTRUMENT", collection="books", token_id="T", instrument_id=instrument
            )
            if kind == "INSTRUMENT"
            else MetricCadenceScope(
                kind="METRIC", collection="books", token_id="T", metric_key=metric, instrument_id=instrument
            )
        )
    cadence = DeclaredConditionCadence(
        status="DECLARED",
        seconds=10.0,
        scope=scope,
        origin=DeclaredMetadataCadenceOrigin(
            kind="DECLARED_METADATA",
            selector="capture_metadata.collections." + collection + ".cadence_s",
            declaration_identity=UncapturedCadenceIdentity(
                status="UNRECORDED", reason="DECLARATION_IDENTITY_NOT_CAPTURED"
            ),
        ),
    )
    return ConditionFieldObservation(
        collection=collection,
        token_id="T",
        instrument_id=instrument,
        metric_key=metric,
        observation_kind=observation_kind,
        chain=chain,
        observed_at=Timestamp(value="2026-10-17T11:59:55Z", precision="EXACT"),
        cadence=cadence,
        age_s=5.0,
        fresh=True,
        reason=None,
        stream=None,
    )


@pytest.mark.parametrize(
    "kind,field,replacement,reason",
    [
        ("INSTRUMENT", "instrument_id", "other:spot:T", "another instrument"),
        ("METRIC", "metric_key", "venue.spot.price", "another metric"),
        ("METRIC", "instrument_id", "other:spot:T", "another instrument"),
        ("SUPPLY", "observation_kind", "LENDING", "another supply kind"),
        ("SUPPLY", "chain", "solana", "another supply chain"),
        ("INSTRUMENT", "token_id", "OTHER", "another token"),
        ("INSTRUMENT", "instrument_id", None, "another instrument"),
        ("METRIC", "metric_key", None, "another metric"),
        ("SUPPLY", "observation_kind", None, "another supply kind"),
        ("SUPPLY", "chain", None, "another supply chain"),
    ],
)
def test_declared_cadence_cannot_qualify_another_observation_subject(
    kind: Literal["INSTRUMENT", "METRIC", "SUPPLY"], field: str, replacement: str | None, reason: str
) -> None:
    matching = scoped_field_observation(kind)
    assert matching.fresh
    contradictory = matching.model_copy(update={field: replacement})
    with pytest.raises(ValidationError, match=reason):
        ConditionFieldObservation.model_validate(contradictory)


def test_broader_collection_and_global_metric_cadences_remain_applicable() -> None:
    from tank.analysis.condition_graph_contracts import (
        CollectionCadenceScope,
        MetricCadenceScope,
        SupplyCadenceScope,
    )

    instrument = scoped_field_observation("INSTRUMENT")
    global_scope = CollectionCadenceScope(kind="COLLECTION", collection="books", token_id=None)
    cadence = instrument.cadence.model_copy(update={"scope": global_scope})
    broad = ConditionFieldObservation.model_validate(
        instrument.model_copy(
            update={
                "cadence": cadence,
                "instrument_id": "other:spot:OTHER",
                "token_id": "OTHER",
                "metric_key": None,
            }
        )
    )
    assert broad.fresh
    metric = scoped_field_observation("METRIC")
    scope = MetricCadenceScope(
        kind="METRIC", collection="books", token_id="T", metric_key="venue.spot.depth", instrument_id=None
    )
    cadence = metric.cadence.model_copy(update={"scope": scope})
    global_metric = ConditionFieldObservation.model_validate(
        metric.model_copy(update={"cadence": cadence, "instrument_id": "other:spot:T"})
    )
    assert global_metric.fresh
    supply = scoped_field_observation("SUPPLY")
    all_chains = SupplyCadenceScope(
        kind="SUPPLY", collection="supply", token_id="T", observation_kind="UNLOCKS", chain=None
    )
    cadence = supply.cadence.model_copy(update={"scope": all_chains})
    global_kind = ConditionFieldObservation.model_validate(
        supply.model_copy(update={"cadence": cadence, "chain": "solana"})
    )
    assert global_kind.fresh


def test_depth_observation_binds_actual_identity_instead_of_copying_cadence_scope() -> None:
    from tank.analysis.condition_graph_contracts import ConditionDepthObservation

    observation = ConditionDepthObservation(
        token_id="T",
        instrument_id="venue:spot:T",
        metric_key=None,
        venue="venue",
        observed_at=Timestamp(value="2026-10-17T11:59:55Z", precision="EXACT"),
        cadence=declared_book_cadence(),
        fresh=True,
    )
    assert observation.fresh
    with pytest.raises(ValidationError, match="another instrument"):
        ConditionDepthObservation.model_validate(
            observation.model_copy(update={"instrument_id": "other:spot:T"})
        )


def legacy_boundary_calculation() -> ConditionFlowAggregate:
    from tank.analysis.condition_graph_contracts import (
        FlowAggregateEvidenceRef,
        IncompleteConditionTradeCounts,
    )

    return ConditionFlowAggregate(
        token_id="T",
        venue="venue",
        market_type="SPOT",
        instrument_id="venue:spot:T",
        start=AT - timedelta(seconds=30),
        end=AT,
        complete=True,
        buy_qty=Decimal(10),
        sell_qty=Decimal(5),
        sum_sq_qty=Decimal(125),
        buy_notional=None,
        sell_notional=None,
        notional_reason="INCOMPLETE",
        low=None,
        low_reason="INCOMPLETE",
        counts=IncompleteConditionTradeCounts(
            status="UNAVAILABLE", n_buy=None, n_sell=None, reason="INCOMPLETE"
        ),
        evidence=FlowAggregateEvidenceRef(projection_sha256="e" * 64, source_evidence_sha256="f" * 64),
    )


def test_legacy_boundary_calculation_does_not_require_or_invent_writer_metadata() -> None:
    from tank.analysis.condition_graph_contracts import ConditionBoundaryGroup

    aggregate = legacy_boundary_calculation()
    group = ConditionBoundaryGroup(start=aggregate.start, cutoff=aggregate.end, aggregates=(aggregate,))
    count = group.trade_count()
    assert count.status == "UNAVAILABLE" and count.value is None and count.reason == "INCOMPLETE"
    assert aggregate.buy_qty == Decimal(10) and aggregate.sell_qty == Decimal(5)
    assert aggregate.low is None and aggregate.buy_notional is None


def test_partial_counts_stay_unknown_until_both_real_side_counts_are_known() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionBoundaryGroup,
        IncompleteConditionTradeCounts,
        ObservedConditionTradeCounts,
    )

    aggregate = legacy_boundary_calculation()
    partial = IncompleteConditionTradeCounts(status="UNAVAILABLE", n_buy=1, n_sell=None, reason="INCOMPLETE")
    aggregate = aggregate.model_copy(update={"counts": partial})
    group = ConditionBoundaryGroup(start=aggregate.start, cutoff=aggregate.end, aggregates=(aggregate,))
    assert group.trade_count().value is None and aggregate.counts.n_buy == 1
    observed = ObservedConditionTradeCounts(status="OBSERVED", n_buy=1, n_sell=1)
    complete = aggregate.model_copy(update={"counts": observed})
    group = group.model_copy(update={"aggregates": (complete,)})
    count = group.trade_count()
    assert count.status == "OBSERVED" and count.value == 2
    bad = observed.model_copy(update={"n_buy": 0})
    with pytest.raises(ValidationError, match="side counts disagree"):
        ConditionBoundaryGroup.model_validate(
            group.model_copy(update={"aggregates": (complete.model_copy(update={"counts": bad}),)})
        )


def test_missing_squared_quantity_cannot_authorize_significant_positive_flow() -> None:
    aggregate = legacy_boundary_calculation().model_copy(update={"sum_sq_qty": Decimal(0)})
    with pytest.raises(ValidationError, match="cannot invent or erase"):
        ConditionFlowAggregate.model_validate(aggregate)


def test_observed_zero_volume_is_distinct_from_an_unavailable_composite() -> None:
    from tank.analysis.condition_graph_contracts import (
        ObservedConditionComposite,
        UnavailableConditionComposite,
    )

    observed = ObservedConditionComposite(
        status="OBSERVED",
        cvd=Decimal(0),
        volume=Decimal(0),
        buy_share=None,
        bs_ratio=None,
        venues=("venue",),
        sign=0,
        null_reasons={"buy_share": "ZERO_TOTAL_VOLUME", "bs_ratio": "ZERO_TOTAL_VOLUME"},
    )
    unavailable = UnavailableConditionComposite(
        status="UNAVAILABLE",
        cvd=None,
        volume=None,
        buy_share=None,
        bs_ratio=None,
        venues=(),
        sign=None,
        reason="INCOMPLETE",
    )
    assert observed.volume == 0 and unavailable.volume is None
    assert observed.sign == 0 and unavailable.sign is None
    assert observed.venues == ("venue",) and unavailable.venues == ()
    with pytest.raises(ValidationError, match="ratios are undefined"):
        ObservedConditionComposite.model_validate(
            observed.model_copy(update={"buy_share": 0.0, "null_reasons": {"bs_ratio": "ZERO_TOTAL_VOLUME"}})
        )


def basic_spot_graph() -> BasicConditionSpot:
    from tank.analysis.condition_graph_contracts import (
        ConditionSpotBreadth,
        ConditionSpotConcentration,
        ConditionSpotCoverage,
        ConditionSpotMajority,
        ConditionSpotTopN,
        ConditionSpotVenue,
        ConditionWeightedBreadth,
        ObservedConditionComposite,
    )

    left = ConditionSpotVenue(
        venue="A",
        complete=True,
        volume_native=Decimal(15),
        buy_qty=Decimal(15),
        sell_qty=Decimal(0),
        cvd=Decimal(15),
        buy_share=1.0,
        bs_ratio=None,
        sign=1,
        sum_sq_qty=Decimal(15),
        boundary_segments=(),
        volume_share=0.5,
        null_reasons={"bs_ratio": "ZERO_SELL_QUANTITY"},
    )
    right = left.model_copy(update={"venue": "B"})
    return BasicConditionSpot(
        token_id="T",
        kind="BASIC",
        venues=(left, right),
        composite_max=ObservedConditionComposite(
            status="OBSERVED",
            cvd=Decimal(30),
            volume=Decimal(30),
            buy_share=1.0,
            bs_ratio=None,
            venues=("A", "B"),
            sign=1,
            null_reasons={"bs_ratio": "ZERO_SELL_QUANTITY"},
        ),
        breadth=ConditionSpotBreadth(
            n_pos=2, n_neg=0, n_neutral=0, pct_pos=1.0, pct_neg=0.0, null_reasons={}
        ),
        cw_breadth=ConditionWeightedBreadth(buy=1.0, sell=0.0, neutral=0.0, null_reasons={}),
        exact_majority=ConditionSpotMajority(
            buy=True, sell=False, neutral=False, covered=True, null_reasons={}
        ),
        topn=ConditionSpotTopN(n=2, venues=("A", "B"), agreement=1.0, agreement_reason=None),
        concentration=ConditionSpotConcentration(
            hhi=0.5,
            top_volume_venue="A",
            top_volume_share=0.5,
            top_contrib_venue="A",
            top_contrib_share=0.5,
            others_cwb_dir=1.0,
            null_reasons={},
        ),
        direction="BUY",
        direction_reason=None,
        classification="BROAD",
        coverage=ConditionSpotCoverage(
            covered_share_est=1.0, exact_majority=True, n_complete=2, excluded=(), null_reasons={}
        ),
    )


def windowed_spot_graph() -> WindowedConditionSpot:
    from tank.analysis.condition_graph_contracts import UnavailableConditionCohort

    base = basic_spot_graph()
    return WindowedConditionSpot(
        token_id=base.token_id,
        kind="WINDOWED",
        venues=base.venues,
        composite_max=base.composite_max,
        breadth=base.breadth,
        cw_breadth=base.cw_breadth,
        exact_majority=base.exact_majority,
        topn=base.topn,
        concentration=base.concentration,
        direction=base.direction,
        direction_reason=base.direction_reason,
        classification=base.classification,
        coverage=base.coverage,
        interval=ConditionInterval(window="W1H", start=AT - timedelta(hours=1), cutoff=AT),
        coverage_through=Timestamp(
            value="2026-10-17T12:00:00Z", precision="BUCKET(60s)", bucket_anchor="END"
        ),
        composite_cohort=UnavailableConditionCohort(status="UNAVAILABLE", reason="NOT_COHORT_CONSISTENT"),
        cohort_consistent={"W15M": False, "W4H": False, "W24H": False, "EVENT": False},
    )


def test_spot_graph_preserves_known_zero_sides_and_exact_native_totals() -> None:
    graph = basic_spot_graph()
    assert graph.cw_breadth.sell == 0.0 and graph.exact_majority.sell is False
    assert graph.composite_max.status == "OBSERVED" and graph.composite_max.volume == Decimal(30)
    bad = graph.coverage.model_copy(update={"n_complete": 1})
    with pytest.raises(ValidationError, match="actual venue observations"):
        BasicConditionSpot.model_validate(graph.model_copy(update={"coverage": bad}))
    with pytest.raises(ValidationError, match="actual composite sign"):
        BasicConditionSpot.model_validate(graph.model_copy(update={"direction": "SELL"}))
    stale_reason = graph.cw_breadth.model_copy(update={"null_reasons": {"sell": "INCOMPLETE"}})
    with pytest.raises(ValidationError, match="exactly the unavailable fields"):
        BasicConditionSpot.model_validate(graph.model_copy(update={"cw_breadth": stale_reason}))


def test_requested_primary_size_evidence_is_unavailable_rather_than_unrequested() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionCohortComparison,
        ConditionWindow,
        EnrichedConditionSpot,
        UnavailableConditionCohortComparison,
        UnavailableConditionSizeProfile,
        UnrequestedConditionSizeProfile,
    )

    base = windowed_spot_graph()
    comparison = UnavailableConditionCohortComparison(
        status="UNAVAILABLE",
        cohort=(),
        share_of_max=None,
        share_majority=None,
        reason="NOT_COHORT_CONSISTENT",
    )
    comparisons: dict[ConditionWindow, ConditionCohortComparison] = {
        name: comparison for name in base.cohort_consistent
    }
    enriched = EnrichedConditionSpot(
        token_id=base.token_id,
        kind="ENRICHED",
        venues=base.venues,
        composite_max=base.composite_max,
        breadth=base.breadth,
        cw_breadth=base.cw_breadth,
        exact_majority=base.exact_majority,
        topn=base.topn,
        concentration=base.concentration,
        direction=base.direction,
        direction_reason=base.direction_reason,
        classification=base.classification,
        coverage=base.coverage,
        interval=base.interval,
        coverage_through=base.coverage_through,
        composite_cohort=base.composite_cohort,
        cohort_consistent=base.cohort_consistent,
        enrichment_role="PRIMARY",
        cohort_comparisons=comparisons,
        size_profile=UnavailableConditionSizeProfile(status="UNAVAILABLE", reason="INCOMPLETE"),
    )
    assert enriched.size_profile.status == "UNAVAILABLE"
    unrequested = UnrequestedConditionSizeProfile(status="NOT_REQUESTED")
    with pytest.raises(ValidationError, match="unavailable, not unrequested"):
        EnrichedConditionSpot.model_validate(enriched.model_copy(update={"size_profile": unrequested}))
    other = EnrichedConditionSpot.model_validate(
        enriched.model_copy(update={"enrichment_role": "OTHER_WINDOW", "size_profile": unrequested})
    )
    assert other.size_profile.status == "NOT_REQUESTED"


def test_spot_native_validation_does_not_round_away_a_wide_precision_unit() -> None:
    from tank.analysis.condition_graph_contracts import ConditionSpotVenue

    buy = Decimal("1e100")
    total = Decimal("1" + "0" * 99 + "1")
    cvd = Decimal("9" * 100)
    squared = Decimal("1" + "0" * 197 + "1")
    venue = ConditionSpotVenue(
        venue="wide",
        complete=True,
        volume_native=total,
        buy_qty=buy,
        sell_qty=Decimal(1),
        cvd=cvd,
        buy_share=1.0,
        bs_ratio=1e100,
        sign=1,
        sum_sq_qty=squared,
        boundary_segments=(),
        volume_share=1.0,
        null_reasons={},
    )
    assert venue.volume_native == total and venue.cvd == cvd
    with pytest.raises(ValidationError, match="actual native buy and sell"):
        ConditionSpotVenue.model_validate(venue.model_copy(update={"volume_native": buy}))


def observed_oi_without_usd() -> ObservedConditionOpenInterest:
    return ObservedConditionOpenInterest(
        status="OBSERVED",
        requested_window_duration_s=3600.0,
        observed_window_duration_s=3600.0,
        oi_native_start=Decimal(10),
        oi_native_end=Decimal(0),
        oi_native_chg_pct=-1.0,
        oi_usd_start=Decimal(0),
        oi_usd_end=Decimal(0),
        oi_usd_chg_pct=None,
        oi_usd_reason="INCOMPLETE",
        oi_usd_detail="SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE",
        decomposition=MissingOIDecomposition(),
        native_normal_range=FloatRange(bounds=FloatBounds(lower=-0.1, upper=0.1), reason=None),
        usd_normal_range=FloatRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        native_oi_vs_normal="BELOW",
        native_beyond=True,
        usd_beyond=None,
        oi_class=None,
        classification_ready=False,
        start_observed_at=Timestamp(
            value="2026-10-17T11:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
        end_observed_at=Timestamp(
            value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
    )


def test_native_oi_zero_end_preserves_unknown_usd_and_classification() -> None:
    observed = observed_oi_without_usd()
    assert observed.oi_native_end == 0 and observed.oi_native_chg_pct == -1
    assert observed.oi_usd_start == 0 and observed.oi_usd_end == 0
    assert observed.oi_usd_chg_pct is None and not observed.classification_ready
    assert observed.end_observed_at.precision is None
    with pytest.raises(ValidationError, match="USD OI change requires"):
        ObservedConditionOpenInterest.model_validate(observed.model_copy(update={"oi_usd_chg_pct": 0.0}))
    with pytest.raises(ValidationError, match="classification readiness"):
        ObservedConditionOpenInterest.model_validate(
            observed.model_copy(update={"classification_ready": True})
        )
    with pytest.raises(ValidationError, match="actual endpoint clocks"):
        ObservedConditionOpenInterest.model_validate(
            observed.model_copy(update={"observed_window_duration_s": 60.0})
        )


def test_oi_baseline_absence_cannot_be_relabelled_as_a_known_normal_comparison() -> None:
    observed = observed_oi_without_usd()
    with pytest.raises(ValidationError, match="actual available baseline"):
        ObservedConditionOpenInterest.model_validate(
            observed.model_copy(
                update={"native_normal_range": FloatRange(bounds=None, reason="INSUFFICIENT_HISTORY")}
            )
        )


def funding_observation(
    kind: Literal["SETTLED", "CURRENT_ESTIMATE"], interval: Decimal | None
) -> ConditionFundingObservation:
    return ConditionFundingObservation(
        kind=kind,
        rate=Decimal(0),
        interval_h=interval,
        equivalent_eight_hour_rate=None if interval is None else Decimal(0),
        equivalent_rate_reason="INCOMPLETE" if interval is None else None,
        observed_at=Timestamp(
            value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
    )


def test_funding_known_zero_estimate_takes_precedence_without_inventing_history() -> None:
    baseline = DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY")
    settled = funding_observation("SETTLED", Decimal(8))
    estimate = funding_observation("CURRENT_ESTIMATE", Decimal(1))
    funding = AvailableConditionFunding(
        status="OBSERVED",
        settled=settled,
        estimate=estimate,
        start=None,
        selected_kind="CURRENT_ESTIMATE",
        normal_range=baseline,
        funding_vs_normal=None,
    )
    assert funding.estimate is not None and funding.estimate.equivalent_eight_hour_rate == 0
    assert funding.funding_vs_normal is None
    with pytest.raises(ValidationError, match="takes precedence"):
        AvailableConditionFunding.model_validate(funding.model_copy(update={"selected_kind": "SETTLED"}))
    with pytest.raises(ValidationError, match="actual normalized rate and baseline"):
        AvailableConditionFunding.model_validate(funding.model_copy(update={"funding_vs_normal": "WITHIN"}))


def test_funding_unknown_interval_preserves_observation_but_never_selects_a_kind() -> None:
    unsettled = funding_observation("CURRENT_ESTIMATE", None)
    unavailable = UnavailableConditionFunding(
        status="UNAVAILABLE",
        reason="INCOMPLETE",
        settled=None,
        estimate=unsettled,
        start=None,
        normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
    )
    assert unavailable.estimate is not None and unavailable.estimate.rate == 0
    assert unavailable.estimate.equivalent_eight_hour_rate is None
    with pytest.raises(ValidationError, match="cannot be classified as missing"):
        UnavailableConditionFunding.model_validate(
            unavailable.model_copy(update={"estimate": funding_observation("CURRENT_ESTIMATE", Decimal(8))})
        )
    with pytest.raises(ValidationError, match="Unknown funding settlement duration"):
        ConditionFundingObservation.model_validate(
            unsettled.model_copy(update={"equivalent_eight_hour_rate": Decimal(0)})
        )


def test_liquidation_capability_does_not_manufacture_an_empty_captured_window() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionLiquidationCaptureWindow,
        ConditionLiquidations,
        ConditionLiquidationSide,
        ConditionRecordedLiquidationCapability,
        UncapturedCadenceIdentity,
    )

    interval = ConditionInterval(window="W1H", start=AT - timedelta(hours=1), cutoff=AT)
    absent_clock = Timestamp(value=None, precision=None, precision_reason="INCOMPLETE")
    unknown_side = ConditionLiquidationSide(
        observed_record_count=0, native=None, usd=None, usd_reason="INCOMPLETE"
    )
    unproven = ConditionLiquidations(
        status="REQUESTED",
        long=unknown_side,
        short=unknown_side,
        source_coverage="COMPLETE",
        coverage="UNKNOWN",
        source_origin=ConditionRecordedLiquidationCapability(
            kind="RECORDED_METADATA",
            identity=UncapturedCadenceIdentity(
                status="UNRECORDED", reason="DECLARATION_IDENTITY_NOT_CAPTURED"
            ),
        ),
        capture_window=ConditionLiquidationCaptureWindow(
            requested=interval,
            start=absent_clock,
            observed_through=absent_clock,
            complete=False,
            reason="INCOMPLETE",
        ),
    )
    assert unproven.source_coverage == "COMPLETE" and unproven.coverage == "UNKNOWN"
    assert unproven.long.observed_record_count == 0 and unproven.long.usd is None
    with pytest.raises(ValidationError, match="unproven capture window"):
        ConditionLiquidations.model_validate(unproven.model_copy(update={"coverage": "COMPLETE"}))
    false_zero = ConditionLiquidationSide(
        observed_record_count=0, native=Decimal(0), usd=Decimal(0), usd_reason=None
    )
    with pytest.raises(ValidationError, match="manufacture zero amounts"):
        ConditionLiquidations.model_validate(unproven.model_copy(update={"long": false_zero}))
    window = ConditionLiquidationCaptureWindow(
        requested=interval,
        start=Timestamp(value="2026-10-17T11:00:00Z", precision="COLLECTION_ONLY", precision_reason=None),
        observed_through=Timestamp(
            value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY", precision_reason=None
        ),
        complete=True,
        reason=None,
    )
    captured = ConditionLiquidations.model_validate(
        unproven.model_copy(
            update={"capture_window": window, "coverage": "COMPLETE", "long": false_zero, "short": false_zero}
        )
    )
    assert captured.long.usd == 0 and captured.short.native == 0
    with pytest.raises(ValidationError, match="both requested bounds"):
        ConditionLiquidationCaptureWindow.model_validate(
            window.model_copy(
                update={
                    "start": Timestamp(
                        value="2026-10-17T11:00:01Z", precision="COLLECTION_ONLY", precision_reason=None
                    )
                }
            )
        )


def test_incremental_volume_zero_total_is_not_missing_and_never_a_market_share() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionIncrementalVolumeCoverage,
        ConditionIncrementalVolumes,
        ConditionObservedIncrementalVolume,
        ConditionUnavailableIncrementalVolume,
    )

    no_increment = ConditionUnavailableIncrementalVolume(
        status="UNAVAILABLE", reason="ZERO_TOTAL_VOLUME", spot_usd=Decimal(0), perp_usd=Decimal(0)
    )
    assert no_increment.spot_usd == 0 and no_increment.perp_usd == 0
    with pytest.raises(ValidationError, match="cannot produce market shares"):
        ConditionObservedIncrementalVolume(
            status="OBSERVED", spot_usd=Decimal(0), perp_usd=Decimal(0), spot_share=0.0, perp_share=0.0
        )
    coverage = ConditionIncrementalVolumeCoverage(instruments=("i",), complete=True, unit="USD", reason=None)
    ConditionIncrementalVolumes(
        status="REQUESTED", spot_coverage=coverage, perp_coverage=coverage, values=no_increment
    )
    missing = ConditionUnavailableIncrementalVolume(
        status="UNAVAILABLE", reason="INCOMPLETE", spot_usd=None, perp_usd=Decimal(0)
    )
    with pytest.raises(ValidationError, match="complete current and preceding"):
        ConditionIncrementalVolumes(
            status="REQUESTED", spot_coverage=coverage, perp_coverage=coverage, values=missing
        )
    positive = ConditionObservedIncrementalVolume(
        status="OBSERVED", spot_usd=Decimal(1), perp_usd=Decimal(3), spot_share=0.25, perp_share=0.75
    )
    assert positive.spot_share == 0.25
    with pytest.raises(ValidationError, match="actual positive USD increments"):
        ConditionObservedIncrementalVolume.model_validate(positive.model_copy(update={"perp_share": 0.25}))


def test_derivative_window_refuses_invented_funding_dispersion_and_absent_sync_venue() -> None:
    from tank.analysis.condition_graph_contracts import (
        BasicConditionDerivatives,
        ConditionDerivativeVenue,
        ConditionObservedFundingDispersion,
        ConditionSynchronizedOpenInterest,
        ConditionUnavailableFundingDispersion,
        UnavailableConditionSynchronizedOpenInterest,
        UnrequestedConditionIncrementalVolumes,
        UnrequestedConditionLiquidations,
    )

    funding = AvailableConditionFunding(
        status="OBSERVED",
        settled=funding_observation("SETTLED", Decimal(8)),
        estimate=None,
        start=None,
        selected_kind="SETTLED",
        normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        funding_vs_normal=None,
    )
    venue = ConditionDerivativeVenue(
        venue="bybit",
        instrument_id="i",
        open_interest=observed_oi_without_usd(),
        funding=funding,
        liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
        oi_temporal_exclusions=None,
        basis_bps=None,
        basis_reason="INCOMPLETE",
    )
    graph = BasicConditionDerivatives(
        kind="BASIC",
        interval=ConditionInterval(window="W1H", start=AT - timedelta(hours=1), cutoff=AT),
        per_venue=(venue,),
        synchronized=UnavailableConditionSynchronizedOpenInterest(status="UNAVAILABLE", reason="INCOMPLETE"),
        funding_dispersion=ConditionUnavailableFundingDispersion(
            status="UNAVAILABLE", reason="INSUFFICIENT_OBSERVATIONS", observation_count=1
        ),
        liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
        incremental_volume=UnrequestedConditionIncrementalVolumes(status="NOT_REQUESTED"),
    )
    assert graph.per_venue[0].funding == funding
    with pytest.raises(ValidationError, match="actual normalized observations"):
        BasicConditionDerivatives.model_validate(
            graph.model_copy(
                update={
                    "funding_dispersion": ConditionObservedFundingDispersion(
                        status="OBSERVED", value=Decimal(0), observation_count=2
                    )
                }
            )
        )
    with pytest.raises(ValidationError, match="absent derivative venue"):
        BasicConditionDerivatives.model_validate(
            graph.model_copy(
                update={
                    "synchronized": ConditionSynchronizedOpenInterest(
                        status="OBSERVED", venues=("unobserved",), values=observed_oi_without_usd()
                    )
                }
            )
        )


def test_lead_lag_uses_actual_market_detection_clocks_and_explicit_scope() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionLeadLagPoint,
        ConditionUnavailablePredicate,
        ObservedConditionLeadLag,
    )

    interval = ConditionInterval(window="W1H", start=AT - timedelta(hours=1), cutoff=AT)
    spot = ConditionLeadLagPoint(
        venue="spot",
        market="SPOT",
        first_observed_at=Timestamp(
            value="2026-10-17T11:10:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
    )
    perp = ConditionLeadLagPoint(
        venue="perp",
        market="PERP",
        first_observed_at=Timestamp(
            value="2026-10-17T11:11:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
    )
    lag = ObservedConditionLeadLag(
        status="OBSERVED",
        interval=interval,
        evaluated_markets=("SPOT", "PERP"),
        first_mover="SPOT",
        spot_confirmation=ConditionUnavailablePredicate(status="UNAVAILABLE", reason="NOT_ESTIMABLE"),
        sequence=(perp, spot),
    )
    assert lag.first_mover == "SPOT" and lag.spot_confirmation.status == "UNAVAILABLE"
    with pytest.raises(ValidationError, match="actual detection order"):
        ObservedConditionLeadLag.model_validate(lag.model_copy(update={"first_mover": "SIMULTANEOUS"}))
    with pytest.raises(ValidationError, match="actual evaluated market"):
        ObservedConditionLeadLag.model_validate(lag.model_copy(update={"evaluated_markets": ("SPOT",)}))
    later = perp.model_copy(
        update={
            "first_observed_at": Timestamp(
                value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
            )
        }
    )
    with pytest.raises(ValidationError, match="actual requested interval"):
        ObservedConditionLeadLag.model_validate(lag.model_copy(update={"sequence": (spot, later)}))


def unknown_derivative_support() -> ConditionDerivativeSupportFeatures:
    from tank.analysis.condition_graph_contracts import (
        ConditionDerivativeSupportFeatures,
        ConditionNativeVolumeSupport,
        ConditionUnavailablePredicate,
    )

    absent = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
    volume = ConditionNativeVolumeSupport(
        value=None,
        previous=None,
        normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        unit="NATIVE_TOKEN",
        window="W1H",
        reason="NOT_ESTIMABLE",
    )
    return ConditionDerivativeSupportFeatures(
        spot_volume_up_beyond_normal=absent,
        spot_volume_within_normal=absent,
        spot_volume=volume,
        perp_volume_up_beyond_normal=absent,
        perp_volume_within_normal=absent,
        perp_volume=volume,
        perp_taker_flow_buy=absent,
        basis_within_normal=absent,
        basis_widening=absent,
        funding_rising=absent,
        liquidations_first_half_majority=absent,
        large_ticket_buy_majority=absent,
        large_tickets_minority=absent,
        large_ticket_sell_share_rising=absent,
        exchange_inflows=absent,
        localized_inventory_transfer=absent,
        ask_depth_building=absent,
        ask_replenishment=absent,
    )


def test_volume_support_keeps_known_zero_within_range_and_unknown_prior_distinct() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionDerivativeSupportFeatures,
        ConditionNativeVolumeSupport,
        ConditionObservedPredicate,
    )

    unknown = unknown_derivative_support()
    zero = ConditionNativeVolumeSupport(
        value=Decimal(0),
        previous=None,
        normal_range=DecimalRange(bounds=DecimalBounds(lower=Decimal(0), upper=Decimal(0)), reason=None),
        unit="NATIVE_TOKEN",
        window="W1H",
        reason=None,
    )
    support = ConditionDerivativeSupportFeatures.model_validate(
        unknown.model_copy(
            update={
                "spot_volume": zero,
                "spot_volume_within_normal": ConditionObservedPredicate(status="OBSERVED", value=True),
            }
        )
    )
    assert support.spot_volume.value == 0 and support.spot_volume.previous is None
    with pytest.raises(ValidationError, match="replace unavailable inputs"):
        ConditionDerivativeSupportFeatures.model_validate(
            support.model_copy(
                update={
                    "spot_volume_up_beyond_normal": ConditionObservedPredicate(status="OBSERVED", value=False)
                }
            )
        )


def test_enriched_empty_derivative_venues_cannot_create_a_negative_basis_predicate() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionObservedPredicate,
        ConditionUnavailableFundingDispersion,
        ConditionUnavailablePredicate,
        EnrichedConditionDerivatives,
        UnavailableConditionLeadLag,
        UnavailableConditionSynchronizedOpenInterest,
        UnrequestedConditionIncrementalVolumes,
        UnrequestedConditionLiquidations,
    )

    interval = ConditionInterval(window="W1H", start=AT - timedelta(hours=1), cutoff=AT)
    support = unknown_derivative_support()
    graph = EnrichedConditionDerivatives(
        kind="ENRICHED",
        funding_dispersion_normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        funding_dispersion_beyond_normal=ConditionUnavailablePredicate(
            status="UNAVAILABLE", reason="INSUFFICIENT_HISTORY"
        ),
        interval=interval,
        per_venue=(),
        synchronized=UnavailableConditionSynchronizedOpenInterest(status="UNAVAILABLE", reason="INCOMPLETE"),
        funding_dispersion=ConditionUnavailableFundingDispersion(
            status="UNAVAILABLE", reason="INSUFFICIENT_OBSERVATIONS", observation_count=0
        ),
        liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
        incremental_volume=UnrequestedConditionIncrementalVolumes(status="NOT_REQUESTED"),
        lead_lag=UnavailableConditionLeadLag(
            status="UNAVAILABLE",
            first_mover="UNKNOWN",
            reason="INCOMPLETE",
            interval=interval,
            evaluated_markets=(),
            sequence=(),
            spot_confirmation=ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE"),
        ),
        support_features=support,
    )
    assert graph.support_features.basis_within_normal.status == "UNAVAILABLE"
    with pytest.raises(ValidationError, match="replace unavailable inputs"):
        EnrichedConditionDerivatives.model_validate(
            graph.model_copy(
                update={
                    "support_features": support.model_copy(
                        update={
                            "basis_within_normal": ConditionObservedPredicate(status="OBSERVED", value=False)
                        }
                    )
                }
            )
        )


def relative_without_beta() -> ConditionRelativeWindow:
    from tank.analysis.condition_graph_contracts import ConditionRelativeNormalRange, ConditionRelativeReturn
    from tank.analysis.price_reference import ReferencePriceReturn, UnavailableReferencePrice
    from tank.analysis.statistics_contracts import UnavailableBeta, UnavailableHistoryDiagnostics

    no_price = UnavailableReferencePrice(
        status="UNAVAILABLE", reason="NO_OBSERVATION", observed_at=None, age_s=None, cadence=None
    )
    no_reference = ReferencePriceReturn(value=None, start=no_price, end=no_price, reason="NO_FRESH_ENDPOINTS")
    beta = UnavailableBeta(
        kind="UNAVAILABLE", value=None, reason="INSUFFICIENT_HISTORY", paired_sample_count=0
    )
    missing_adjustment = ConditionRelativeReturn(value=None, reason="INSUFFICIENT_HISTORY")
    return ConditionRelativeWindow(
        interval=ConditionInterval(window="W1H", start=AT - timedelta(hours=1), cutoff=AT),
        observed_at=Timestamp(
            value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
        reference_instrument_id="spot:i",
        raw=ConditionRelativeReturn(value=0.05, reason=None),
        btc_adjusted=missing_adjustment,
        eth_adjusted=missing_adjustment,
        sector_adjusted=ConditionRelativeReturn(value=None, reason="NOT_ESTIMABLE"),
        measure="R_BTC_ADJ",
        btc_reference=no_reference,
        eth_reference=no_reference,
        btc_beta=beta,
        eth_beta=beta,
        percentile=None,
        percentile_reason="NOT_ESTIMABLE",
        normal_range=ConditionRelativeNormalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        history=UnavailableHistoryDiagnostics(
            target_window_duration_s=3600.0,
            exact=False,
            reason="INSUFFICIENT_HISTORY",
            resolution_s=None,
            sample_count=0,
        ),
    )


def test_relative_window_preserves_raw_return_without_inventing_beta_adjustment() -> None:
    from tank.analysis.condition_graph_contracts import ConditionRelativeReturn

    window = relative_without_beta()
    assert window.raw.value == 0.05 and window.relative_return() is None
    assert window.beyond_normal() is None and window.observed_at.precision is None
    with pytest.raises(ValidationError, match="Unavailable beta cannot manufacture"):
        ConditionRelativeWindow.model_validate(
            window.model_copy(update={"btc_adjusted": ConditionRelativeReturn(value=0.05, reason=None)})
        )
    with pytest.raises(ValidationError, match="actual return and historical samples"):
        ConditionRelativeWindow.model_validate(
            window.model_copy(update={"percentile": 0.5, "percentile_reason": None})
        )


def test_known_zero_beta_is_valid_and_missing_fresh_benchmark_is_still_unavailable() -> None:
    from tank.analysis.condition_graph_contracts import ConditionRelativeReturn
    from tank.analysis.price_reference import ObservedReferencePrice, PriceCadence, ReferencePriceReturn
    from tank.analysis.statistics_contracts import EstimatedBeta

    window = relative_without_beta()
    endpoint = ObservedReferencePrice(
        status="OBSERVED",
        observed_at=AT,
        age_s=0.0,
        cadence=PriceCadence(seconds=60.0, source="BAR_INTERVAL"),
        price=1.0,
    )
    known_zero = EstimatedBeta(kind="ESTIMATED", value=0.0, paired_sample_count=24)
    still_missing = ConditionRelativeWindow.model_validate(
        window.model_copy(
            update={
                "btc_beta": known_zero,
                "btc_adjusted": ConditionRelativeReturn(value=None, reason="NO_FRESH_ENDPOINTS"),
            }
        )
    )
    assert still_missing.relative_return() is None
    observed = ConditionRelativeWindow.model_validate(
        still_missing.model_copy(
            update={
                "btc_reference": ReferencePriceReturn(value=0.0, start=endpoint, end=endpoint, reason=None),
                "btc_adjusted": ConditionRelativeReturn(value=0.05, reason=None),
            }
        )
    )
    assert observed.relative_return() == 0.05
    with pytest.raises(ValidationError, match="actual beta and fresh benchmark"):
        ConditionRelativeWindow.model_validate(
            observed.model_copy(update={"btc_adjusted": ConditionRelativeReturn(value=0.0, reason=None)})
        )


def test_relative_calculation_revalidates_a_constructed_or_mutated_nested_return() -> None:
    from tank.analysis.condition_graph_contracts import ConditionRelativeReturn

    window = relative_without_beta()
    invalid = window.model_copy(update={"btc_adjusted": ConditionRelativeReturn(value=1.0, reason=None)})
    with pytest.raises(ValidationError, match="Unavailable beta cannot manufacture"):
        invalid.relative_return()
    with pytest.raises(ValidationError, match="Unavailable beta cannot manufacture"):
        invalid.beyond_normal()


def test_market_context_known_zero_is_neutral_and_missing_volatility_is_unavailable() -> None:
    from tank.analysis.condition_graph_contracts import ConditionMarketContextInputs

    zero_range = FloatRange(bounds=FloatBounds(lower=0.0, upper=0.0), reason=None)
    inputs = ConditionMarketContextInputs(
        btc_return_24h=0.0,
        rv24=0.0,
        rv24_bound=AT,
        rv24_resolution_s=3600,
        return_normal_range=zero_range,
        return_lower_quartile=0.0,
        rv_normal_range=zero_range,
        rule="normal_range",
    )
    result = inputs.classification()
    assert result.status == "OBSERVED" and result.value == "NEUTRAL"
    absent = inputs.model_copy(update={"rv24": None}).classification()
    assert absent.status == "UNAVAILABLE" and absent.value is None
    with pytest.raises(ValidationError, match="completed UTC hour"):
        inputs.model_copy(update={"rv24_bound": AT + timedelta(seconds=1)}).classification()
    with pytest.raises(ValidationError, match="actual available return history"):
        inputs.model_copy(update={"return_lower_quartile": None}).classification()


def test_repricing_requires_declared_direction_and_preserves_signed_return_orientation() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionRelativeNormalRange,
        ConditionRelativeReturn,
        DeclaredConditionRepricing,
        UndeclaredConditionRepricingDirection,
    )

    unknown = UndeclaredConditionRepricingDirection(
        status="UNAVAILABLE", reason="DIRECTION_NOT_DECLARED"
    ).band()
    assert unknown.value is None and unknown.reason == "DIRECTION_NOT_DECLARED"
    inputs = DeclaredConditionRepricing(
        status="DIRECTION_DECLARED",
        direction="BUY",
        event_return=ConditionRelativeReturn(value=-0.3, reason=None),
        event_range=ConditionRelativeNormalRange(bounds=FloatBounds(lower=-0.1, upper=0.2), reason=None),
        day_range=ConditionRelativeNormalRange(bounds=FloatBounds(lower=-0.2, upper=0.4), reason=None),
        event_lower_quartile=-0.05,
        event_upper_quartile=0.1,
    )
    assert inputs.band().value == "NEGATIVE"
    assert inputs.model_copy(update={"direction": "SELL"}).band().value == "HEAVY"
    with pytest.raises(ValidationError, match="actual directional quartile evidence"):
        inputs.model_copy(update={"event_lower_quartile": None, "event_upper_quartile": None}).band()


def test_repricing_preserves_heavy_day_threshold_without_inventing_event_history() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionRelativeNormalRange,
        ConditionRelativeReturn,
        DeclaredConditionRepricing,
    )

    inputs = DeclaredConditionRepricing(
        status="DIRECTION_DECLARED",
        direction="BUY",
        event_return=ConditionRelativeReturn(value=0.3, reason=None),
        event_range=ConditionRelativeNormalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        day_range=ConditionRelativeNormalRange(bounds=FloatBounds(lower=-0.1, upper=0.2), reason=None),
        event_lower_quartile=None,
        event_upper_quartile=None,
    )
    assert inputs.band().value == "HEAVY"
    unavailable = inputs.model_copy(
        update={"event_return": ConditionRelativeReturn(value=0.0, reason=None)}
    ).band()
    assert unavailable.status == "UNAVAILABLE" and unavailable.reason == "INSUFFICIENT_HISTORY"


def test_degenerate_event_history_retains_observed_quartiles_without_a_false_band() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionRelativeNormalRange,
        ConditionRelativeReturn,
        DeclaredConditionRepricing,
    )

    evidence = DeclaredConditionRepricing(
        status="DIRECTION_DECLARED",
        direction="BUY",
        event_return=ConditionRelativeReturn(value=0.0, reason=None),
        event_range=ConditionRelativeNormalRange(bounds=None, reason="DEGENERATE_HISTORY"),
        day_range=ConditionRelativeNormalRange(bounds=None, reason="INCOMPLETE"),
        event_lower_quartile=0.0,
        event_upper_quartile=0.0,
    )
    result = evidence.band()
    assert evidence.event_upper_quartile == 0 and result.status == "UNAVAILABLE"
    assert result.reason == "DEGENERATE_HISTORY"


def relative_graph_without_event() -> ConditionRelativeGraph:
    from tank.analysis.condition_graph_contracts import (
        ConditionAnalysisWindowStart,
        ConditionMarketContextInputs,
        ConditionPreEventRunup,
        ConditionPriceReferenceCoverage,
        ConditionRelativeAnchor,
        ConditionRelativeReturn,
        ConditionUnavailablePredicate,
        ObservedConditionReferencePrice,
        UnavailableConditionEventRepricing,
        UnavailableConditionReferencePrice,
        UnrequestedConditionOutcomeRanges,
    )
    from tank.analysis.statistics_contracts import UnavailableHistoryDiagnostics

    sample = relative_without_beta()
    windows: dict[Literal["W15M", "W1H", "W4H", "W24H", "EVENT"], ConditionRelativeWindow] = {}
    durations: tuple[tuple[Literal["W15M", "W1H", "W4H", "W24H", "EVENT"], int], ...] = (
        ("W15M", 900),
        ("W1H", 3600),
        ("W4H", 14400),
        ("W24H", 86400),
        ("EVENT", 14400),
    )
    for name, duration in durations:
        window = sample.model_copy(
            update={
                "interval": ConditionInterval(window=name, start=AT - timedelta(seconds=duration), cutoff=AT),
                "history": UnavailableHistoryDiagnostics(
                    target_window_duration_s=float(duration),
                    exact=False,
                    reason="INSUFFICIENT_HISTORY",
                    resolution_s=None,
                    sample_count=0,
                ),
            }
        )
        if name == "EVENT":
            window = window.model_copy(
                update={"raw": ConditionRelativeReturn(value=None, reason="INCOMPLETE")}
            )
        windows[name] = ConditionRelativeWindow.model_validate(window)
    absent = Timestamp(value=None, precision=None, precision_reason="INCOMPLETE")
    missing_price = UnavailableConditionReferencePrice(
        status="UNAVAILABLE",
        instrument_id="spot:i",
        value=None,
        source=None,
        observed_at=absent,
        reason="INCOMPLETE",
    )
    anchor_at = AT - timedelta(hours=4)
    return ConditionRelativeGraph(
        reference_instrument_id="spot:i",
        reference_reason=None,
        reference_coverage={
            "spot:i": ConditionPriceReferenceCoverage(
                complete=True,
                expected_duration_s=86400,
                boundary_segments=(),
                excluded=(),
                volume_unit="NATIVE_TOKEN",
                covered_volume_native=Decimal(10),
            )
        },
        entry=ObservedConditionReferencePrice(
            status="OBSERVED",
            instrument_id="spot:i",
            value=Decimal(10),
            source="KLINE_CLOSE",
            observed_at=Timestamp(
                value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
            ),
        ),
        close_1h=ObservedConditionClose(
            status="OBSERVED",
            reference_instrument_id="spot:i",
            price=Decimal(10),
            observed_at=Timestamp(
                value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
            ),
        ),
        anchor=ConditionRelativeAnchor(
            origin=ConditionAnalysisWindowStart(kind="ANALYSIS_WINDOW_START", at=anchor_at),
            price=missing_price,
            trade_evidence_ref=None,
        ),
        windows=windows,
        pre_event_runup=ConditionPreEventRunup(
            window="PRE_EVENT_W24H",
            start=anchor_at - timedelta(days=1),
            cutoff=anchor_at,
            value=ConditionRelativeReturn(value=None, reason="INSUFFICIENT_HISTORY"),
            percentile=None,
            percentile_reason="NOT_ESTIMABLE",
            beyond_normal=ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INSUFFICIENT_HISTORY"),
        ),
        repricing=UnavailableConditionEventRepricing(status="UNAVAILABLE", reason="NO_EVENT_ANCHOR"),
        context=ConditionMarketContextInputs(
            btc_return_24h=None,
            rv24=None,
            rv24_bound=AT,
            rv24_resolution_s=3600,
            return_normal_range=FloatRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
            return_lower_quartile=None,
            rv_normal_range=FloatRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
            rule="normal_range",
        ),
        outcome_btc_reference=UnavailableConditionReferencePrice(
            status="UNAVAILABLE",
            instrument_id=None,
            value=None,
            source=None,
            observed_at=absent,
            reason="INCOMPLETE",
        ),
        outcome_ranges=UnrequestedConditionOutcomeRanges(status="NOT_REQUESTED"),
    )


def test_relative_graph_preserves_analysis_boundary_without_fabricating_event_anchor() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionDeclaredEventAnchor,
        UndeclaredConditionRepricingDirection,
    )

    graph = relative_graph_without_event()
    assert graph.windows["W1H"].raw.value == 0.05
    assert graph.repricing.band().value is None and graph.anchor.origin.kind == "ANALYSIS_WINDOW_START"
    with pytest.raises(ValidationError, match="Missing event anchor must remain"):
        ConditionRelativeGraph.model_validate(
            graph.model_copy(
                update={
                    "repricing": UndeclaredConditionRepricingDirection(
                        status="UNAVAILABLE", reason="DIRECTION_NOT_DECLARED"
                    )
                }
            )
        )
    with pytest.raises(ValidationError, match="real captured event anchor"):
        ConditionRelativeGraph.model_validate(
            graph.model_copy(
                update={
                    "anchor": graph.anchor.model_copy(
                        update={
                            "origin": ConditionDeclaredEventAnchor(
                                kind="EVENT",
                                source="KNOWN_TO_US_AT",
                                at=Timestamp(
                                    value="2026-10-17T08:00:00Z",
                                    precision=None,
                                    precision_reason="PRECISION_NOT_CAPTURED",
                                ),
                            )
                        }
                    )
                }
            )
        )


def test_relative_graph_selection_uses_actual_complete_native_volume_and_shared_references() -> None:
    from tank.analysis.condition_graph_contracts import ConditionPriceReferenceCoverage

    graph = relative_graph_without_event()
    better = ConditionPriceReferenceCoverage(
        complete=True,
        expected_duration_s=86400,
        boundary_segments=(),
        excluded=(),
        volume_unit="NATIVE_TOKEN",
        covered_volume_native=Decimal(20),
    )
    with pytest.raises(ValidationError, match="greatest covered native volume"):
        ConditionRelativeGraph.model_validate(
            graph.model_copy(
                update={
                    "reference_coverage": {"spot:i": graph.reference_coverage["spot:i"], "better": better}
                }
            )
        )
    with pytest.raises(ValidationError, match="actual selected reference instrument"):
        ConditionRelativeGraph.model_validate(
            graph.model_copy(update={"entry": graph.entry.model_copy(update={"instrument_id": "other"})})
        )
    mutated = dict(graph.windows)
    mutated["W1H"] = mutated["W1H"].model_copy(update={"reference_instrument_id": "other"})
    with pytest.raises(ValidationError, match="actual selected price reference"):
        ConditionRelativeGraph.model_validate(graph.model_copy(update={"windows": mutated}))


def test_outcome_baseline_request_is_not_an_empty_unrequested_map_or_a_fabricated_clock() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionComputedOutcomeRanges,
        ConditionOutcomeRange,
        ConditionRelativeNormalRange,
    )
    from tank.analysis.statistics_contracts import UnavailableHistoryDiagnostics

    baseline = ConditionOutcomeRange(
        window="W1H",
        unit="DECIMAL_RETURN",
        measure="R_BTC_ADJ",
        start=AT - timedelta(days=30),
        cutoff=AT,
        observed_at=Timestamp(value=None, precision=None, precision_reason="INCOMPLETE"),
        normal_range=ConditionRelativeNormalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        history=UnavailableHistoryDiagnostics(
            target_window_duration_s=3600.0,
            exact=False,
            reason="INSUFFICIENT_HISTORY",
            resolution_s=None,
            sample_count=0,
        ),
    )
    assert baseline.history.sample_count == 0 and baseline.observed_at.value is None
    with pytest.raises(ValidationError, match="actual six requested horizons"):
        ConditionComputedOutcomeRanges(status="COMPUTED", by_horizon={"W1H": baseline})
    with pytest.raises(ValidationError, match="normal bounds or an observed source clock"):
        ConditionOutcomeRange.model_validate(
            baseline.model_copy(
                update={
                    "observed_at": Timestamp(
                        value="2026-10-17T11:00:00Z",
                        precision=None,
                        precision_reason="PRECISION_NOT_CAPTURED",
                    )
                }
            )
        )
    with pytest.raises(ValidationError, match="actual horizon duration"):
        ConditionOutcomeRange.model_validate(baseline.model_copy(update={"window": "W4H"}))


def test_impact_tier_retains_signed_execution_and_distinguishes_zero_notional() -> None:
    from tank.analysis.condition_graph_contracts import ConditionImpactTier
    from tank.analysis.execution import BookWalkRequest, execute_book_walk
    from tank.market.book_contracts import BookLevel

    request = BookWalkRequest(
        levels=[BookLevel.from_wire("9", "10")],
        quote_notional=Decimal(10),
        mid_quote_price=Decimal(10),
        side="ask",
    )
    execution = execute_book_walk(request)
    tier = ConditionImpactTier(
        notional_usd=Decimal(10), notional_reason=None, execution=execution, execution_reason=None
    )
    assert tier.slippage_bps() == -1000.0
    zero = ConditionImpactTier(
        notional_usd=Decimal(0), notional_reason=None, execution=None, execution_reason="ZERO_NOTIONAL"
    )
    assert zero.notional_usd == 0 and zero.slippage_bps() is None
    with pytest.raises(ValidationError, match="zero notional cannot produce"):
        ConditionImpactTier.model_validate(tier.model_copy(update={"notional_usd": Decimal(0)}))
    corrupt = execution.model_copy(update={"complete": False, "reason": "INSUFFICIENT_BOOK_DEPTH"})
    with pytest.raises(ValidationError, match="cannot invent a complete slippage"):
        tier.model_copy(update={"execution": corrupt}).slippage_bps()


def test_depth_cohort_preserves_zero_and_exact_majority_without_fake_percentages() -> None:
    from tank.analysis.condition_graph_contracts import ObservedConditionDepthCohort

    empty_now = ObservedConditionDepthCohort(
        status="OBSERVED", venues=("a",), current_usd=Decimal(0), previous_usd=Decimal(10)
    )
    assert empty_now.drop().value == 1.0
    majority = empty_now.majority_lost()
    assert majority.status == "OBSERVED" and majority.value is True
    half = empty_now.model_copy(update={"current_usd": Decimal(5)})
    assert half.drop().value == 0.5
    half_majority = half.majority_lost()
    assert half_majority.status == "OBSERVED" and half_majority.value is False
    zero_base = empty_now.model_copy(update={"previous_usd": Decimal(0)})
    assert zero_base.drop().value is None and zero_base.majority_lost().status == "UNAVAILABLE"
    unrepresentable = empty_now.model_copy(
        update={"current_usd": Decimal("1e300"), "previous_usd": Decimal("1e-300")}
    )
    undefined = unrepresentable.drop()
    assert undefined.status == "UNAVAILABLE" and undefined.reason == "NOT_ESTIMABLE"
    assert unrepresentable.current_usd == Decimal("1e300")


def test_book_depth_does_not_replace_unknown_conversion_with_zero() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionBookSideDepth,
        ConditionNativeDepthAmount,
        ConditionUSDDepthAmount,
    )

    partial = ConditionBookSideDepth(
        usd=ConditionUSDDepthAmount(value=None, reason="INCOMPLETE"),
        native=ConditionNativeDepthAmount(value=Decimal(3), reason=None),
    )
    assert partial.native.value == 3 and partial.usd.value is None
    with pytest.raises(ValidationError, match="same zero-quantity fact"):
        ConditionBookSideDepth.model_validate(
            partial.model_copy(update={"usd": ConditionUSDDepthAmount(value=Decimal(0), reason=None)})
        )


def two_venue_depth_bounds(value: Decimal, covered: tuple[str, ...]) -> ConditionBookDepthBounds:
    from tank.analysis.condition_graph_contracts import ConditionBookSideBounds, ConditionDepthLowerBound

    sides = ConditionBookSideBounds(
        usd=ConditionDepthLowerBound(
            unit="USD", value=value, reason=None, considered_venues=("a", "b"), covered_venues=covered
        ),
        native=ConditionDepthLowerBound(
            unit="NATIVE", value=value, reason=None, considered_venues=("a", "b"), covered_venues=covered
        ),
    )
    return ConditionBookDepthBounds(bid=sides, ask=sides)


def elasticity_inputs() -> ConditionElasticityInputs:
    from tank.analysis.condition_graph_contracts import (
        ConditionLiquidityNetFlow,
        ConditionRelativeReturn,
        ConditionUnavailablePredicate,
        ConditionUSDDepthAmount,
        UnavailableConditionPreDepthBounds,
    )

    return ConditionElasticityInputs(
        flow=ConditionLiquidityNetFlow(
            native=Decimal(10), native_reason=None, usd=Decimal(10), usd_reason=None
        ),
        raw_return=ConditionRelativeReturn(value=0.001, reason=None),
        price_beyond_normal=ConditionUnavailablePredicate(
            status="UNAVAILABLE", reason="INSUFFICIENT_HISTORY"
        ),
        measurement_band_percent=1.0,
        current=two_venue_depth_bounds(Decimal(1), ("a",)),
        previous=UnavailableConditionPreDepthBounds(status="UNAVAILABLE", reason="NO_EVENT_ANCHOR"),
        average_hit_side_depth_usd=ConditionUSDDepthAmount(value=Decimal(10), reason=None),
    )


def test_partial_depth_proves_only_a_conclusive_lower_bound_and_zero_order_is_not_a_test() -> None:
    partial = two_venue_depth_bounds(Decimal(10), ("a",)).bid.usd
    known_not_thin = partial.thinness(Decimal(5))
    assert known_not_thin.status == "OBSERVED" and known_not_thin.value is False
    assert partial.thinness(Decimal(20)).status == "UNAVAILABLE"
    fully_thin = two_venue_depth_bounds(Decimal(10), ("a", "b")).bid.usd.thinness(Decimal(20))
    assert fully_thin.status == "OBSERVED" and fully_thin.value is True
    assert partial.thinness(Decimal(0)).status == "UNAVAILABLE"


def test_financial_predicate_logic_keeps_unknown_without_inventing_empty_truth() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionObservedPredicate,
        ConditionUnavailablePredicate,
        condition_predicates_all,
        condition_predicates_any,
    )

    known_true = ConditionObservedPredicate(status="OBSERVED", value=True)
    known_false = ConditionObservedPredicate(status="OBSERVED", value=False)
    unknown = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
    assert condition_predicates_all((known_false, unknown)) == known_false
    assert condition_predicates_all((known_true, unknown)) == unknown
    assert condition_predicates_any((known_true, unknown)) == known_true
    assert condition_predicates_any((known_false, unknown)) == unknown
    with pytest.raises(ValueError, match="actual evaluated component"):
        condition_predicates_all(())
    with pytest.raises(ValueError, match="actual evaluated component"):
        condition_predicates_any(())


def test_elasticity_preserves_native_zero_and_requires_actual_usd_flow_for_bps() -> None:
    from tank.analysis.condition_graph_contracts import ConditionLiquidityNetFlow

    inputs = elasticity_inputs().model_copy(
        update={
            "flow": ConditionLiquidityNetFlow(
                native=Decimal(0), native_reason=None, usd=Decimal(1), usd_reason=None
            )
        }
    )
    selected = inputs.flow.comparison_flow()
    assert selected.status == "OBSERVED" and selected.unit == "NATIVE" and selected.value == 0
    weak = inputs.weak_response()
    assert weak.status == "UNAVAILABLE" and weak.reason == "ZERO_NET_FLOW"
    response = inputs.bps_per_depth_unit()
    assert response.status == "OBSERVED" and response.bps_per_depth_unit == 100.0
    unconverted = inputs.model_copy(
        update={
            "flow": ConditionLiquidityNetFlow(
                native=Decimal(10), native_reason=None, usd=None, usd_reason="INCOMPLETE"
            )
        }
    ).bps_per_depth_unit()
    assert unconverted.status == "UNAVAILABLE" and unconverted.reason == "INCOMPLETE"


def test_weak_and_excess_response_require_actual_tested_flow_depth_and_price() -> None:
    from tank.analysis.condition_graph_contracts import ConditionObservedPredicate

    inputs = elasticity_inputs()
    assert inputs.weak_response().status == "UNAVAILABLE"
    complete = inputs.model_copy(update={"current": two_venue_depth_bounds(Decimal(1), ("a", "b"))})
    weak = complete.weak_response()
    assert weak.status == "OBSERVED" and weak.value is True
    no_beyond = inputs.model_copy(
        update={"price_beyond_normal": ConditionObservedPredicate(status="OBSERVED", value=False)}
    )
    excess_false = no_beyond.excess_response()
    assert excess_false.status == "OBSERVED" and excess_false.value is False
    proven = inputs.model_copy(
        update={
            "price_beyond_normal": ConditionObservedPredicate(status="OBSERVED", value=True),
            "previous": two_venue_depth_bounds(Decimal(20), ("a",)),
        }
    )
    excess = proven.excess_response()
    assert excess.status == "OBSERVED" and excess.value is True


def test_depth_ratio_failure_preserves_exact_inputs_and_never_returns_infinity_or_zero() -> None:
    from tank.analysis.condition_graph_contracts import ConditionLiquidityNetFlow, ConditionUSDDepthAmount

    inputs = elasticity_inputs().model_copy(
        update={
            "flow": ConditionLiquidityNetFlow(
                native=Decimal("1e-300"), native_reason=None, usd=Decimal("1e-300"), usd_reason=None
            ),
            "average_hit_side_depth_usd": ConditionUSDDepthAmount(value=Decimal("1e300"), reason=None),
        }
    )
    result = inputs.bps_per_depth_unit()
    assert result.status == "UNAVAILABLE" and result.reason == "NOT_ESTIMABLE"
    assert inputs.flow.usd == Decimal("1e-300") and inputs.average_hit_side_depth_usd.value == Decimal(
        "1e300"
    )


def test_native_depth_comparison_retains_quantity_differences_beyond_default_decimal_precision() -> None:
    from tank.analysis.condition_graph_contracts import ConditionLiquidityNetFlow

    flow = Decimal("1" + "0" * 99 + "2")
    bound = Decimal("1" + "0" * 99 + "1")
    inputs = elasticity_inputs().model_copy(
        update={
            "flow": ConditionLiquidityNetFlow(native=flow, native_reason=None, usd=flow, usd_reason=None),
            "current": two_venue_depth_bounds(bound, ("a",)),
        }
    )
    assert flow > bound
    result = inputs.weak_response()
    assert result.status == "UNAVAILABLE" and result.reason == "INCOMPLETE"


def replenishment_scope() -> ConditionReplenishmentScope:
    return ConditionReplenishmentScope(
        interval=ConditionInterval(window="W1H", start=AT - timedelta(hours=1), cutoff=AT),
        refill_window_s=60.0,
        resilience_window_s=900.0,
        basis="CAPTURED_BOOK_AND_FLOW_SAMPLES",
        window_origin="FLOW_START",
    )


def recovered_burst() -> ConditionReplenishmentBurst:
    return ConditionReplenishmentBurst(
        venue="a",
        flow_instrument_id="spot:i",
        book_instrument_id="spot:i",
        flow_start=Timestamp(value="2026-10-17T11:59:30Z", precision="EXACT", precision_reason=None),
        flow_end=AT - timedelta(seconds=10),
        last_trade_at=Timestamp(value="2026-10-17T11:59:40Z", precision="EXACT", precision_reason=None),
        baseline_bid_depth_usd=Decimal(10),
        net_sell_usd=Decimal(12),
        baseline_low_price=Decimal(100),
        baseline_low_reason=None,
        recovery_at=Timestamp(
            value="2026-10-17T11:59:45Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
        flow_start_to_recovery_s=15.0,
        followup_book_sample_count=2,
        post_flow_book_sample_count=1,
        lowest_post_flow_price=Decimal(99),
        post_flow_price_sample_count=1,
        post_flow_price_start=AT - timedelta(seconds=10),
        post_flow_price_cutoff=AT,
    )


def test_observed_recovery_and_lower_low_survive_an_incomplete_followup_window() -> None:
    from tank.analysis.condition_graph_contracts import ConditionReplenishmentScan

    burst, scope = recovered_burst(), replenishment_scope()
    recovered = burst.replenished(scope)
    failed_resilience = burst.resilient(scope)
    assert recovered.status == "OBSERVED" and recovered.value is True
    assert failed_resilience.status == "OBSERVED" and failed_resilience.value is False
    summary = ConditionReplenishmentScan(
        scope=scope,
        considered_candidate_count=1,
        observed_non_burst_count=0,
        unqualified_candidate_counts={},
        bursts=(burst,),
    ).summary()
    assert summary.status == "COMPUTED" and summary.observed_replenished == 1
    assert summary.observed_resilient_replenished == 0 and summary.median_flow_start_to_recovery_s == 15.0


def test_recovery_timing_never_guesses_exact_last_trade_from_a_formatted_clock() -> None:
    burst = recovered_burst()
    unclocked_precision = burst.last_trade_at.model_copy(
        update={"precision": None, "precision_reason": "PRECISION_NOT_CAPTURED"}
    )
    with pytest.raises(ValidationError, match="actual exact last trade or complete flow interval"):
        ConditionReplenishmentBurst.model_validate(
            burst.model_copy(update={"last_trade_at": unclocked_precision})
        )
    late_burst = burst.model_copy(
        update={
            "last_trade_at": Timestamp(value="2026-10-17T11:59:46Z", precision="EXACT", precision_reason=None)
        }
    )
    with pytest.raises(ValidationError, match="actual exact last trade or complete flow interval"):
        late_burst.replenished(replenishment_scope())
    overlapping_price = burst.model_copy(update={"post_flow_price_start": AT - timedelta(seconds=15)})
    with pytest.raises(ValidationError, match="actual non-overlapping interval"):
        overlapping_price.resilient(replenishment_scope())


def test_incomplete_replenishment_is_unknown_and_zero_baseline_is_not_a_burst() -> None:
    from tank.analysis.condition_graph_contracts import ConditionReplenishmentScan

    burst = recovered_burst().model_copy(
        update={
            "recovery_at": Timestamp(value=None, precision=None, precision_reason="INCOMPLETE"),
            "flow_start_to_recovery_s": None,
        }
    )
    pending = burst.replenished(replenishment_scope())
    assert pending.status == "UNAVAILABLE" and pending.reason == "INCOMPLETE"
    no_test = ConditionReplenishmentScan(
        scope=replenishment_scope(),
        considered_candidate_count=1,
        observed_non_burst_count=0,
        unqualified_candidate_counts={"ZERO_BASELINE": 1},
        bursts=(),
    ).summary()
    assert no_test.status == "UNAVAILABLE" and no_test.reason == "NO_EVALUABLE_CANDIDATES"
    with pytest.raises(ValidationError):
        ConditionReplenishmentBurst.model_validate(
            burst.model_copy(update={"baseline_bid_depth_usd": Decimal(0)})
        )


def test_replenishment_scan_accounts_for_actual_candidates_and_prevents_duplicate_bursts() -> None:
    from tank.analysis.condition_graph_contracts import ConditionReplenishmentScan

    burst, scope = recovered_burst(), replenishment_scope()
    with pytest.raises(ValidationError, match="actual tested and unavailable counts"):
        ConditionReplenishmentScan(
            scope=scope,
            considered_candidate_count=2,
            observed_non_burst_count=0,
            unqualified_candidate_counts={},
            bursts=(burst,),
        )
    with pytest.raises(ValidationError, match="duplicate a detected flow burst"):
        ConditionReplenishmentScan(
            scope=scope,
            considered_candidate_count=2,
            observed_non_burst_count=0,
            unqualified_candidate_counts={},
            bursts=(burst, burst),
        )
    known_no_burst = ConditionReplenishmentScan(
        scope=scope,
        considered_candidate_count=1,
        observed_non_burst_count=1,
        unqualified_candidate_counts={},
        bursts=(),
    ).summary()
    assert known_no_burst.status == "COMPUTED" and known_no_burst.observed_bursts == 0


def single_venue_liquidity() -> BasicConditionLiquidity:
    from tank.analysis.condition_graph_contracts import (
        ConditionBookSideBounds,
        ConditionBookSideDepth,
        ConditionCoveredBookRange,
        ConditionDepthLowerBound,
        ConditionImpactTier,
        ConditionImpactTiers,
        ConditionLiquidityDepthTotals,
        ConditionLiquidityNetFlow,
        ConditionLiquidityVenue,
        ConditionNativeDepthAmount,
        ConditionRelativeReturn,
        ConditionReplenishmentScan,
        ConditionUnavailablePredicate,
        ConditionUSDDepthAmount,
        UnavailableConditionPreDepth,
        UnavailableConditionPreDepthBounds,
        UnavailableConditionPreDepthSides,
    )
    from tank.market.trade_contracts import QuoteUsdValuation

    scope = replenishment_scope()
    side = ConditionBookSideDepth(
        usd=ConditionUSDDepthAmount(value=Decimal(100), reason=None),
        native=ConditionNativeDepthAmount(value=Decimal(100), reason=None),
    )
    ranges = ConditionCoveredBookRange(percent=1.0, reason=None)
    tiers = ConditionImpactTiers(
        T1=ConditionImpactTier(
            notional_usd=Decimal(1), notional_reason=None, execution=None, execution_reason="INCOMPLETE"
        ),
        T2=ConditionImpactTier(
            notional_usd=Decimal(10), notional_reason=None, execution=None, execution_reason="INCOMPLETE"
        ),
        T3=ConditionImpactTier(
            notional_usd=Decimal(20), notional_reason=None, execution=None, execution_reason="INCOMPLETE"
        ),
    )
    venue = ConditionLiquidityVenue(
        venue="a",
        instrument_id="spot:i",
        observed_at=Timestamp(
            value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
        integrity_ok=True,
        bid_range=ranges,
        ask_range=ranges,
        spread_bps=1.0,
        spread_reason=None,
        bid=side,
        ask=side,
        pre_depth=UnavailableConditionPreDepthSides(status="UNAVAILABLE", reason="NO_EVENT_ANCHOR"),
        impact_tiers=tiers,
        quote_asset="USD",
        quote_valuation=QuoteUsdValuation(
            rate=Decimal(1), as_of=None, basis="USD_UNIT", unavailable_reason=None
        ),
    )
    bounds = ConditionBookSideBounds(
        usd=ConditionDepthLowerBound(
            unit="USD", value=Decimal(100), reason=None, considered_venues=("a",), covered_venues=("a",)
        ),
        native=ConditionDepthLowerBound(
            unit="NATIVE", value=Decimal(100), reason=None, considered_venues=("a",), covered_venues=("a",)
        ),
    )
    return BasicConditionLiquidity(
        kind="BASIC",
        interval=scope.interval,
        measurement_band_percent=1.0,
        per_venue=(venue,),
        current_depth=ConditionLiquidityDepthTotals(
            bid=side.usd, ask=side.usd, combined=ConditionUSDDepthAmount(value=Decimal(200), reason=None)
        ),
        previous_depth=UnavailableConditionPreDepth(status="UNAVAILABLE", reason="NO_EVENT_ANCHOR"),
        elasticity=ConditionElasticityInputs(
            flow=ConditionLiquidityNetFlow(
                native=Decimal(10), native_reason=None, usd=Decimal(10), usd_reason=None
            ),
            raw_return=ConditionRelativeReturn(value=0.001, reason=None),
            price_beyond_normal=ConditionUnavailablePredicate(
                status="UNAVAILABLE", reason="INSUFFICIENT_HISTORY"
            ),
            measurement_band_percent=1.0,
            current=ConditionBookDepthBounds(bid=bounds, ask=bounds),
            previous=UnavailableConditionPreDepthBounds(status="UNAVAILABLE", reason="NO_EVENT_ANCHOR"),
            average_hit_side_depth_usd=ConditionUSDDepthAmount(value=Decimal(100), reason=None),
        ),
        replenishment=ConditionReplenishmentScan(
            scope=scope,
            considered_candidate_count=1,
            observed_non_burst_count=1,
            unqualified_candidate_counts={},
            bursts=(),
        ),
    )


def test_liquidity_state_does_not_turn_unknown_vacuum_into_normal_depth() -> None:
    from tank.analysis.condition_graph_contracts import ConditionObservedPredicate

    graph = single_venue_liquidity()
    thin = graph.thinness()
    assert thin.status == "OBSERVED" and thin.value is False
    unknown = graph.state()
    assert unknown.status == "UNAVAILABLE" and unknown.value is None
    disproven_vacuum = graph.model_copy(
        update={
            "elasticity": graph.elasticity.model_copy(
                update={"price_beyond_normal": ConditionObservedPredicate(status="OBSERVED", value=False)}
            )
        }
    )
    normal = disproven_vacuum.state()
    assert normal.status == "OBSERVED" and normal.value == "NORMAL"


def test_liquidity_graph_binds_actual_depth_coverage_totals_and_book_cutoff() -> None:
    from tank.analysis.condition_graph_contracts import ConditionCoveredBookRange, ConditionUSDDepthAmount

    graph = single_venue_liquidity()
    venue = graph.per_venue[0]
    uncovered = venue.model_copy(update={"bid_range": ConditionCoveredBookRange(percent=0.5, reason=None)})
    with pytest.raises(ValidationError, match="actual declared side coverage"):
        BasicConditionLiquidity.model_validate(graph.model_copy(update={"per_venue": (uncovered,)}))
    inconsistent_total = graph.current_depth.model_copy(
        update={"combined": ConditionUSDDepthAmount(value=Decimal(199), reason=None)}
    )
    with pytest.raises(ValidationError, match="every actual required venue amount"):
        BasicConditionLiquidity.model_validate(graph.model_copy(update={"current_depth": inconsistent_total}))
    future = venue.model_copy(
        update={
            "observed_at": Timestamp(
                value="2026-10-17T12:00:01Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
            )
        }
    )
    with pytest.raises(ValidationError, match="future or unclocked book"):
        BasicConditionLiquidity.model_validate(graph.model_copy(update={"per_venue": (future,)}))


def test_zero_liquidity_test_order_cannot_establish_normal_or_not_thin() -> None:
    from tank.analysis.condition_graph_contracts import ConditionImpactTier, ConditionObservedPredicate

    graph = single_venue_liquidity()
    venue = graph.per_venue[0]
    zero = ConditionImpactTier(
        notional_usd=Decimal(0), notional_reason=None, execution=None, execution_reason="ZERO_NOTIONAL"
    )
    updated = BasicConditionLiquidity.model_validate(
        graph.model_copy(
            update={
                "per_venue": (
                    venue.model_copy(
                        update={"impact_tiers": venue.impact_tiers.model_copy(update={"T2": zero})}
                    ),
                ),
                "elasticity": graph.elasticity.model_copy(
                    update={"price_beyond_normal": ConditionObservedPredicate(status="OBSERVED", value=False)}
                ),
            }
        )
    )
    thin = updated.thinness()
    state = updated.state()
    assert thin.status == "UNAVAILABLE" and thin.reason == "ZERO_NOTIONAL"
    assert state.status == "UNAVAILABLE" and state.reason == "ZERO_NOTIONAL"


def test_pre_depth_means_preserve_different_qualified_observation_cohorts() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionDepthSampleCohort,
        ConditionPreBookSideDepth,
        ObservedConditionDepthMean,
    )
    from tank.util import sha256

    one = ConditionDepthSampleCohort(observation_count=1, fingerprint=sha256(("book:1",)))
    two = ConditionDepthSampleCohort(observation_count=2, fingerprint=sha256(("book:1", "book:2")))
    usd = ObservedConditionDepthMean(status="OBSERVED", value=Decimal(0), cohort=one)
    native = ObservedConditionDepthMean(status="OBSERVED", value=Decimal(3), cohort=two)
    partial = ConditionPreBookSideDepth(usd=usd, native=native)
    assert partial.usd.value == 0 and partial.native.value == 3
    with pytest.raises(ValidationError, match="same zero-quantity fact"):
        ConditionPreBookSideDepth.model_validate(
            partial.model_copy(update={"native": native.model_copy(update={"cohort": one})})
        )


def cadence_for_book(instrument: str) -> DeclaredConditionCadence:
    from tank.analysis.condition_graph_contracts import InstrumentCadenceScope

    return DeclaredConditionCadence.model_validate(
        declared_book_cadence().model_copy(
            update={
                "scope": InstrumentCadenceScope(
                    kind="INSTRUMENT", collection="books", token_id="T", instrument_id=instrument
                )
            }
        )
    )


def primary_quote(
    venue: str, quote_asset: str, mid: Decimal, valuation: QuoteUsdValuation
) -> ConditionPrimaryBookQuote:
    from tank.analysis.condition_graph_contracts import UnrecordedConditionCaptureIdentity

    return ConditionPrimaryBookQuote(
        token_id="T",
        venue=venue,
        instrument_id=venue + ":spot:T",
        metric_key=None,
        quote_asset=quote_asset,
        mid_quote=mid,
        spread_bps=Decimal(1),
        observed_at=Timestamp(
            value="2026-10-17T11:59:55Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
        quote_valuation=valuation,
        capture_identity=UnrecordedConditionCaptureIdentity(
            status="UNRECORDED", reason="CAPTURE_IDENTITY_NOT_CAPTURED"
        ),
        integrity_ok=True,
    )


def independent_quote(primary: ConditionPrimaryBookQuote) -> ConditionIndependentRestQuote:
    return ConditionIndependentRestQuote(
        token_id=primary.token_id,
        venue=primary.venue,
        instrument_id=primary.instrument_id,
        metric_key=primary.metric_key,
        quote_asset=primary.quote_asset,
        mid_quote=primary.mid_quote,
        spread_bps=primary.spread_bps,
        observed_at=Timestamp(
            value="2026-10-17T11:59:56Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
        quote_valuation=primary.quote_valuation,
        capture_identity=primary.capture_identity,
        source="PUBLIC_REST",
        visibility=ConditionReceiptVisibility(
            received_at=Timestamp(value="2026-10-17T11:59:57Z", precision="COLLECTION_ONLY"),
            assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
        ),
        primary_observed_at=primary.observed_at,
        proximity_policy=cadence_for_book(primary.instrument_id),
        capture_confirmed_independent=True,
        confirmation_reason=None,
    )


def test_book_window_coverage_uses_actual_scoped_cadence_and_measured_gaps() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionBookSampleCoverage,
        UnavailableConditionCadence,
    )

    cadence = cadence_for_book("a:spot:T")
    scope = ConditionInterval(window="W15M", start=AT - timedelta(minutes=15), cutoff=AT)
    coverage = ConditionBookSampleCoverage(
        token_id="T",
        venue="a",
        instrument_id="a:spot:T",
        metric_key=None,
        cadence=cadence,
        sample_count=91,
        initial_observation=Timestamp(
            value="2026-10-17T11:45:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
        latest_observation=Timestamp(
            value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
        maximum_gap_s=10.0,
    )
    complete = coverage.covers(scope)
    assert complete.status == "OBSERVED" and complete.value is True
    gap = coverage.model_copy(update={"maximum_gap_s": 11.0}).covers(scope)
    assert gap.status == "OBSERVED" and gap.value is False
    unknown = coverage.model_copy(
        update={
            "cadence": UnavailableConditionCadence(
                status="UNAVAILABLE",
                seconds=None,
                scope=cadence.scope,
                origin=None,
                reason="CADENCE_NOT_CAPTURED",
            )
        }
    ).covers(scope)
    assert unknown.status == "UNAVAILABLE"
    with pytest.raises(ValidationError, match="another instrument"):
        ConditionBookSampleCoverage.model_validate(
            coverage.model_copy(update={"cadence": cadence_for_book("different")})
        )


def test_same_currency_gap_does_not_require_or_invent_usd_conversion() -> None:
    from tank.analysis.condition_graph_contracts import ConditionBookPairSample

    missing_fx = QuoteUsdValuation(
        rate=None, as_of=None, basis="UNAVAILABLE", unavailable_reason="QUOTE_USD_RATE_UNAVAILABLE"
    )
    left, right = (
        primary_quote("a", "USDT", Decimal(103), missing_fx),
        primary_quote("b", "USDT", Decimal(100), missing_fx),
    )
    sample = ConditionBookPairSample(
        left=left,
        right=right,
        left_cadence=cadence_for_book(left.instrument_id),
        right_cadence=cadence_for_book(right.instrument_id),
        evaluated_at=AT,
        quote_conversion_uncertainty_bps=Decimal(2),
    )
    gap = sample.gap()
    assert gap.status == "OBSERVED" and gap.value is True
    assert sample.left.quote_valuation.rate is None
    with pytest.raises(ValidationError):
        ConditionBookPairSample.model_validate(
            sample.model_copy(update={"left": left.model_copy(update={"integrity_ok": 1})})
        )


def test_cross_currency_gap_requires_actual_fx_instead_of_a_stablecoin_peg_default() -> None:
    from tank.analysis.condition_graph_contracts import ConditionBookPairSample

    missing_fx = QuoteUsdValuation(
        rate=None, as_of=None, basis="UNAVAILABLE", unavailable_reason="QUOTE_USD_RATE_UNAVAILABLE"
    )
    usd = QuoteUsdValuation(rate=Decimal(1), as_of=None, basis="USD_UNIT", unavailable_reason=None)
    left, right = (
        primary_quote("a", "USDT", Decimal("100.1"), missing_fx),
        primary_quote("b", "USD", Decimal(100), usd),
    )
    sample = ConditionBookPairSample(
        left=left,
        right=right,
        left_cadence=cadence_for_book(left.instrument_id),
        right_cadence=cadence_for_book(right.instrument_id),
        evaluated_at=AT,
        quote_conversion_uncertainty_bps=Decimal(2),
    )
    unknown = sample.gap()
    assert unknown.status == "UNAVAILABLE" and unknown.reason == "QUOTE_CONVERSION_UNAVAILABLE"
    actual = left.model_copy(
        update={
            "quote_valuation": QuoteUsdValuation(
                rate=Decimal("0.999"),
                as_of=AT - timedelta(seconds=5),
                basis="HISTORICAL_SAMPLE",
                unavailable_reason=None,
            )
        }
    )
    converted = sample.model_copy(update={"left": actual}).gap()
    assert converted.status == "OBSERVED" and converted.value is False


def test_independent_confirmation_absence_is_unknown_and_captured_false_stays_false() -> None:
    from tank.analysis.condition_graph_contracts import ConditionIndependentPairConfirmation

    no_fx = QuoteUsdValuation(
        rate=None, as_of=None, basis="UNAVAILABLE", unavailable_reason="QUOTE_USD_RATE_UNAVAILABLE"
    )
    left, right = (
        independent_quote(primary_quote("a", "USDT", Decimal(103), no_fx)),
        independent_quote(primary_quote("b", "USDT", Decimal(100), no_fx)),
    )
    partial = ConditionIndependentPairConfirmation(
        left=left, right=None, cutoff=AT, quote_conversion_uncertainty_bps=Decimal(2)
    )
    assert partial.confirmed_gap().status == "UNAVAILABLE"
    complete = partial.model_copy(update={"right": right})
    confirmed = complete.confirmed_gap()
    assert confirmed.status == "OBSERVED" and confirmed.value is True
    unconfirmed = left.model_copy(
        update={"capture_confirmed_independent": False, "confirmation_reason": "NOT_CONFIRMED"}
    )
    failed = complete.model_copy(update={"left": unconfirmed}).confirmed_gap()
    assert failed.status == "OBSERVED" and failed.value is False
    missing = left.model_copy(
        update={"capture_confirmed_independent": None, "confirmation_reason": "NOT_CAPTURED"}
    )
    assert complete.model_copy(update={"left": missing}).confirmed_gap().status == "UNAVAILABLE"


def test_independent_confirmation_requires_actual_clock_proximity_and_instrument_scope() -> None:
    from tank.analysis.condition_graph_contracts import ConditionIndependentPairConfirmation

    no_fx = QuoteUsdValuation(
        rate=None, as_of=None, basis="UNAVAILABLE", unavailable_reason="QUOTE_USD_RATE_UNAVAILABLE"
    )
    left, right = (
        independent_quote(primary_quote("a", "USDT", Decimal(103), no_fx)),
        independent_quote(primary_quote("b", "USDT", Decimal(100), no_fx)),
    )
    confirmation = ConditionIndependentPairConfirmation(
        left=left, right=right, cutoff=AT, quote_conversion_uncertainty_bps=Decimal(2)
    )
    stale = left.model_copy(
        update={
            "observed_at": Timestamp(
                value="2026-10-17T11:59:40Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
            )
        }
    )
    assert confirmation.model_copy(update={"left": stale}).confirmed_gap().status == "UNAVAILABLE"
    with pytest.raises(ValidationError, match="another instrument"):
        ConditionIndependentPairConfirmation.model_validate(
            confirmation.model_copy(
                update={"left": left.model_copy(update={"proximity_policy": cadence_for_book("other")})}
            )
        )


def test_discrete_direction_and_missing_composite_sign_are_not_boolean_or_neutral_surrogates() -> None:
    from tank.analysis.condition_graph_contracts import (
        ObservedConditionComposite,
        UnavailableConditionComposite,
    )

    observed = ObservedConditionComposite(
        status="OBSERVED",
        cvd=Decimal(0),
        volume=Decimal(0),
        buy_share=None,
        bs_ratio=None,
        venues=("a",),
        sign=0,
        null_reasons={"buy_share": "ZERO_TOTAL_VOLUME", "bs_ratio": "ZERO_TOTAL_VOLUME"},
    )
    with pytest.raises(ValidationError, match="integer"):
        ObservedConditionComposite.model_validate(observed.model_copy(update={"sign": False}))
    missing = UnavailableConditionComposite(
        status="UNAVAILABLE",
        cvd=None,
        volume=None,
        buy_share=None,
        bs_ratio=None,
        venues=(),
        sign=None,
        reason="INCOMPLETE",
    )
    assert observed.sign == 0 and missing.sign is None
    with pytest.raises(ValidationError):
        UnavailableConditionComposite.model_validate(missing.model_copy(update={"sign": 0}))


def test_volatility_compression_preserves_measured_zero_and_actual_completed_hour() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionVolatilityCompression,
        ObservedConditionVolatility,
    )

    measured = ObservedConditionVolatility(
        status="OBSERVED",
        value=0.0,
        completed_hour=Timestamp(
            value="2026-10-17T10:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
        ),
        return_count=24,
        resolution_s=3600,
    )
    graph = ConditionVolatilityCompression(
        token_id="T",
        reference_venue="v",
        cutoff=AT,
        window="W24H",
        realized=measured,
        normal_range=FloatRange(bounds=FloatBounds(lower=0.0, upper=0.0), reason=None),
    )
    assert graph.classification().value == "NORMAL"
    assert graph.realized == measured
    assert measured.completed_hour.value == "2026-10-17T10:00:00Z"
    assert measured.completed_hour.precision is None
    float_resolution = ObservedConditionVolatility.model_validate(
        measured.model_copy(update={"resolution_s": 3600.0})
    )
    assert float_resolution.resolution_s == 3600
    with pytest.raises(ValidationError, match="after its actual cutoff"):
        ConditionVolatilityCompression.model_validate(
            graph.model_copy(update={"cutoff": AT - timedelta(hours=3)})
        )
    with pytest.raises(ValidationError, match="integer"):
        ObservedConditionVolatility.model_validate(measured.model_copy(update={"return_count": 24.0}))


def test_volatility_absence_and_missing_baseline_do_not_become_normal_state() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionVolatilityCompression,
        ObservedConditionVolatility,
        UnavailableConditionVolatility,
    )

    graph = ConditionVolatilityCompression(
        token_id="T",
        reference_venue="v",
        cutoff=AT,
        window="W24H",
        realized=UnavailableConditionVolatility(status="UNAVAILABLE", value=None, reason="INCOMPLETE"),
        normal_range=FloatRange(bounds=FloatBounds(lower=0.01, upper=0.03), reason=None),
    )
    from tank.analysis.condition_graph_contracts import UnavailableConditionVolatilityState

    unavailable = graph.classification()
    assert isinstance(unavailable, UnavailableConditionVolatilityState)
    assert unavailable.reason == "INCOMPLETE"
    missing_history = ConditionVolatilityCompression.model_validate(
        graph.model_copy(
            update={
                "realized": ObservedConditionVolatility(
                    status="OBSERVED",
                    value=0.02,
                    completed_hour=Timestamp(
                        value="2026-10-17T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
                    ),
                    return_count=24,
                    resolution_s=3600,
                ),
                "normal_range": FloatRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
            }
        )
    )
    missing = missing_history.classification()
    assert isinstance(missing, UnavailableConditionVolatilityState)
    assert missing.reason == "INSUFFICIENT_HISTORY"
    with pytest.raises(ValidationError, match="negative bounds"):
        ConditionVolatilityCompression.model_validate(
            graph.model_copy(
                update={
                    "normal_range": FloatRange(bounds=FloatBounds(lower=-0.01, upper=0.03), reason=None),
                }
            )
        )


def test_fragmentation_history_replays_actual_quote_changes_and_terminal_gap_closure() -> None:
    from tank.analysis.condition_graph_contracts import ConditionBookPairHistory, ConditionObservedPredicate
    from tank.util import iso

    valuation = QuoteUsdValuation(rate=Decimal(1), basis="USD_UNIT", as_of=None, unavailable_reason=None)
    left, right = (
        primary_quote("a", "USD", Decimal(103), valuation),
        primary_quote("b", "USD", Decimal(100), valuation),
    )
    start = AT - timedelta(minutes=15)
    left_rows: list[ConditionPrimaryBookQuote] = []
    right_rows: list[ConditionPrimaryBookQuote] = []
    for index in range(91):
        clock = Timestamp(
            value=iso(start + timedelta(seconds=index * 10)), precision=None, precision_reason="NOT_CAPTURED"
        )
        left_rows.append(
            ConditionPrimaryBookQuote.model_validate(
                left.model_copy(
                    update={"observed_at": clock, "mid_quote": Decimal(100) if index == 90 else Decimal(103)}
                )
            )
        )
        right_rows.append(
            ConditionPrimaryBookQuote.model_validate(right.model_copy(update={"observed_at": clock}))
        )
    history = ConditionBookPairHistory(
        token_id="T",
        left_venue="a",
        right_venue="b",
        left_instrument_id=left.instrument_id,
        right_instrument_id=right.instrument_id,
        left_metric_key=None,
        right_metric_key=None,
        interval=ConditionInterval(window="W15M", start=start, cutoff=AT),
        left=tuple(left_rows),
        right=tuple(right_rows),
        left_cadence=cadence_for_book(left.instrument_id),
        right_cadence=cadence_for_book(right.instrument_id),
        quote_conversion_uncertainty_bps=Decimal(2),
    )
    assert len(history.gap_series()) == 91
    persistent, closed = history.persistent_gap(), history.gap_closed_quickly()
    assert isinstance(persistent, ConditionObservedPredicate) and persistent.value is True
    assert isinstance(closed, ConditionObservedPredicate) and closed.value is True
    from tank.analysis.condition_graph_contracts import ConditionUnavailablePredicate

    sparse = ConditionBookPairHistory.model_validate(
        history.model_copy(
            update={
                "left": (left_rows[0], left_rows[-1]),
                "right": (right_rows[0], right_rows[-1]),
            }
        )
    )
    assert isinstance(sparse.persistent_gap(), ConditionUnavailablePredicate)
    with pytest.raises(ValidationError, match="ordered, distinct"):
        ConditionBookPairHistory.model_validate(
            history.model_copy(update={"left": (left_rows[1], left_rows[0])})
        )


def test_missing_fragmentation_quotes_are_unavailable_not_a_negative_gap_witness() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionBookPairHistory,
        ConditionUnavailablePredicate,
    )

    history = ConditionBookPairHistory(
        token_id="T",
        left_venue="a",
        right_venue="b",
        left_instrument_id="a:spot:T",
        right_instrument_id="b:spot:T",
        left_metric_key=None,
        right_metric_key=None,
        interval=ConditionInterval(window="W15M", start=AT - timedelta(minutes=15), cutoff=AT),
        left=(),
        right=(),
        left_cadence=cadence_for_book("a:spot:T"),
        right_cadence=cadence_for_book("b:spot:T"),
        quote_conversion_uncertainty_bps=Decimal(2),
    )
    assert isinstance(history.persistent_gap(), ConditionUnavailablePredicate)
    assert isinstance(history.gap_closed_quickly(), ConditionUnavailablePredicate)
    with pytest.raises(ValidationError, match="instrument"):
        ConditionBookPairHistory.model_validate(
            history.model_copy(update={"left_cadence": cadence_for_book("wrong")})
        )


def test_receipt_visibility_requires_actual_receipt_and_assembly_clocks() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionObservedPredicate,
        ConditionUnavailablePredicate,
    )

    known = ConditionReceiptVisibility(
        received_at=Timestamp(value="2026-10-17T11:59:57Z", precision="COLLECTION_ONLY"),
        assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
    )
    eligible = known.eligible()
    assert isinstance(eligible, ConditionObservedPredicate) and eligible.value is True
    future = ConditionReceiptVisibility.model_validate(
        known.model_copy(
            update={
                "received_at": Timestamp(value="2026-10-17T12:00:01Z", precision="COLLECTION_ONLY"),
            }
        )
    ).eligible()
    assert isinstance(future, ConditionObservedPredicate) and future.value is False
    for field in ("received_at", "assembled_at"):
        unknown = ConditionReceiptVisibility.model_validate(
            known.model_copy(
                update={
                    field: Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
                }
            )
        )
        assert isinstance(unknown.eligible(), ConditionUnavailablePredicate)


def test_independent_rest_confirmation_cannot_assume_missing_receipt_is_visible() -> None:
    from tank.analysis.condition_graph_contracts import ConditionUnavailablePredicate

    valuation = QuoteUsdValuation(rate=Decimal(1), basis="USD_UNIT", as_of=None, unavailable_reason=None)
    rest = independent_quote(primary_quote("a", "USD", Decimal(100), valuation))
    unknown = ConditionIndependentRestQuote.model_validate(
        rest.model_copy(
            update={
                "visibility": ConditionReceiptVisibility(
                    received_at=Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
                    assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
                ),
            }
        )
    )
    assert isinstance(unknown.eligible_at(AT), ConditionUnavailablePredicate)


def funding_confirmation_graph() -> ConditionFundingConfirmationGraph:
    from tank.analysis.condition_graph_contracts import (
        ConditionFundingConfirmationGraph,
        ConditionIndependentFundingRate,
        ConditionScopedFundingRate,
    )

    primary: list[ConditionScopedFundingRate] = []
    independent: list[ConditionIndependentFundingRate] = []
    for venue, rate in (("a", Decimal("0.01")), ("b", Decimal("0.04"))):
        primary.append(
            ConditionScopedFundingRate(
                token_id="T",
                instrument_id=venue + ":perp:T",
                venue=venue,
                rate=ConditionFundingObservation(
                    kind="CURRENT_ESTIMATE",
                    rate=rate,
                    interval_h=Decimal(8),
                    equivalent_eight_hour_rate=rate,
                    equivalent_rate_reason=None,
                    observed_at=Timestamp(
                        value="2026-10-17T11:59:00Z", precision=None, precision_reason="NOT_CAPTURED"
                    ),
                ),
            )
        )
        independent.append(
            ConditionIndependentFundingRate(
                token_id="T",
                instrument_id=venue + ":perp:T",
                venue=venue,
                rate_8h_eq=rate,
                observed_at=Timestamp(
                    value="2026-10-17T11:59:30Z", precision=None, precision_reason="NOT_CAPTURED"
                ),
                visibility=ConditionReceiptVisibility(
                    received_at=Timestamp(value="2026-10-17T11:59:31Z", precision="COLLECTION_ONLY"),
                    assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
                ),
                source_path=venue + "/rest",
                primary_source_path=venue + "/ws",
                independent_path_verified=True,
                verification_reason=None,
            )
        )
    return ConditionFundingConfirmationGraph(
        assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
        token_id="T",
        cutoff=AT,
        primary=tuple(primary),
        independent=tuple(independent),
        normal_range_8h=DecimalRange(
            bounds=DecimalBounds(lower=Decimal(0), upper=Decimal("0.02")), reason=None
        ),
    )


def test_funding_confirmation_uses_same_actual_instrument_cohort_and_distinct_paths() -> None:
    from tank.analysis.condition_graph_contracts import ConditionObservedPredicate

    graph = funding_confirmation_graph()
    primary, independent = graph.primary_beyond_normal(), graph.independent_beyond_normal()
    assert isinstance(primary, ConditionObservedPredicate) and primary.value is True
    assert isinstance(independent, ConditionObservedPredicate) and independent.value is True
    with pytest.raises(ValidationError, match="another token"):
        type(graph).model_validate(
            graph.model_copy(
                update={
                    "independent": (
                        graph.independent[0].model_copy(update={"token_id": "OTHER"}),
                        graph.independent[1],
                    )
                }
            )
        )
    with pytest.raises(ValidationError, match="different actual source"):
        type(graph).model_validate(
            graph.model_copy(
                update={
                    "independent": (
                        graph.independent[0].model_copy(
                            update={"source_path": graph.independent[0].primary_source_path}
                        ),
                        graph.independent[1],
                    )
                }
            )
        )


def test_funding_confirmation_missing_secondary_or_history_stays_unavailable() -> None:
    from tank.analysis.condition_graph_contracts import ConditionUnavailablePredicate

    graph = funding_confirmation_graph()
    partial = type(graph).model_validate(graph.model_copy(update={"independent": (graph.independent[0],)}))
    assert isinstance(partial.independent_beyond_normal(), ConditionUnavailablePredicate)
    unknown = type(graph).model_validate(
        graph.model_copy(
            update={
                "normal_range_8h": DecimalRange(bounds=None, reason="INCOMPLETE"),
            }
        )
    )
    result = unknown.primary_beyond_normal()
    assert isinstance(result, ConditionUnavailablePredicate) and result.reason == "INCOMPLETE"
    assert isinstance(unknown.independent_beyond_normal(), ConditionUnavailablePredicate)


def complete_perp_flow_window(instrument: str, venue: str) -> ConditionFlowWindowQualification:
    from tank.analysis.condition_graph_contracts import ConditionFlowCapturePiece

    pieces: list[ConditionFlowCapturePiece] = []
    prototype = legacy_boundary_calculation()
    for minute in range(15):
        start = AT - timedelta(minutes=15 - minute)
        aggregate = ConditionFlowAggregate.model_validate(
            prototype.model_copy(
                update={
                    "instrument_id": instrument,
                    "venue": venue,
                    "market_type": "PERP",
                    "start": start,
                    "end": start + timedelta(minutes=1),
                }
            )
        )
        pieces.append(
            ConditionFlowCapturePiece(
                basis="FULL_MINUTE",
                aggregate=aggregate,
                visibility=ConditionReceiptVisibility(
                    received_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
                    assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
                ),
            )
        )
    return ConditionFlowWindowQualification(
        assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
        token_id="T",
        instrument_id=instrument,
        venue=venue,
        market_type="PERP",
        interval=ConditionInterval(window="W15M", start=AT - timedelta(minutes=15), cutoff=AT),
        pieces=tuple(pieces),
    )


def test_fragmentation_flow_capture_requires_actual_minute_grid_and_visible_evidence() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionObservedPredicate,
        ConditionUnavailablePredicate,
    )

    window = complete_perp_flow_window("a:perp:T", "a")
    known = window.complete()
    assert isinstance(known, ConditionObservedPredicate) and known.value is True
    partial = type(window).model_validate(window.model_copy(update={"pieces": window.pieces[:-1]}))
    assert isinstance(partial.complete(), ConditionUnavailablePredicate)
    hidden_piece = window.pieces[0].model_copy(
        update={
            "visibility": ConditionReceiptVisibility(
                received_at=Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
                assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
            )
        }
    )
    hidden = type(window).model_validate(
        window.model_copy(update={"pieces": (hidden_piece, *window.pieces[1:])})
    )
    assert isinstance(hidden.complete(), ConditionUnavailablePredicate)
    with pytest.raises(ValidationError, match="cannot replace missing full-minute"):
        type(window).model_validate(
            window.model_copy(
                update={
                    "pieces": (
                        window.pieces[0].model_copy(update={"basis": "BOUNDARY"}),
                        *window.pieces[1:],
                    )
                }
            )
        )


def test_fragmentation_funding_validation_requires_actual_scoped_flow_windows() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionFragmentationGraph,
        ConditionObservedPredicate,
        ConditionUnavailablePredicate,
    )

    graph = ConditionFragmentationGraph(
        assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
        token_id="T",
        interval=ConditionInterval(window="W15M", start=AT - timedelta(minutes=15), cutoff=AT),
        top_sign_history=ConditionTopVenueSignHistory(
            token_id="T",
            interval=ConditionInterval(window="W15M", start=AT - timedelta(minutes=15), cutoff=AT),
            selected_venues=(),
            windows=(),
        ),
        price_pairs=(),
        price_confirmations=(),
        funding=funding_confirmation_graph(),
        flow_capture=(complete_perp_flow_window("a:perp:T", "a"), complete_perp_flow_window("b:perp:T", "b")),
    )
    assert isinstance(graph.persistent_gap(), ConditionUnavailablePredicate)
    assert isinstance(graph.price_path_validated(), ConditionUnavailablePredicate)
    validated = graph.validated()
    assert isinstance(validated, ConditionObservedPredicate) and validated.value is True
    no_capture = type(graph).model_validate(graph.model_copy(update={"flow_capture": ()}))
    assert isinstance(no_capture.funding_path_validated(), ConditionUnavailablePredicate)
    assert isinstance(no_capture.validated(), ConditionUnavailablePredicate)
    with pytest.raises(ValidationError, match="unrelated fragmentation"):
        type(graph).model_validate(
            graph.model_copy(update={"flow_capture": (complete_perp_flow_window("other:perp:T", "other"),)})
        )


def test_unaligned_fragmentation_window_requires_both_actual_boundary_chains() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionFlowCapturePiece,
        ConditionObservedPredicate,
        ConditionUnavailablePredicate,
    )

    prototype = complete_perp_flow_window("a:perp:T", "a")
    assembled = Timestamp(value="2026-10-17T12:00:10Z", precision="COLLECTION_ONLY")
    visibility = ConditionReceiptVisibility(
        received_at=Timestamp(value="2026-10-17T12:00:06Z", precision="COLLECTION_ONLY"),
        assembled_at=assembled,
    )
    middle = tuple(
        type(piece).model_validate(piece.model_copy(update={"visibility": visibility}))
        for piece in prototype.pieces[1:]
    )
    template = prototype.pieces[0].aggregate
    left = ConditionFlowCapturePiece(
        basis="BOUNDARY",
        visibility=visibility,
        aggregate=ConditionFlowAggregate.model_validate(
            template.model_copy(
                update={
                    "start": AT - timedelta(minutes=15) + timedelta(seconds=5),
                    "end": AT - timedelta(minutes=14),
                }
            )
        ),
    )
    right = ConditionFlowCapturePiece(
        basis="BOUNDARY",
        visibility=visibility,
        aggregate=ConditionFlowAggregate.model_validate(
            template.model_copy(update={"start": AT, "end": AT + timedelta(seconds=5)})
        ),
    )
    interval = ConditionInterval(
        window="W15M",
        start=AT - timedelta(minutes=15) + timedelta(seconds=5),
        cutoff=AT + timedelta(seconds=5),
    )
    complete = type(prototype).model_validate(
        prototype.model_copy(
            update={
                "assembled_at": assembled,
                "interval": interval,
                "pieces": (*middle, left, right),
            }
        )
    )
    result = complete.complete()
    assert isinstance(result, ConditionObservedPredicate) and result.value is True
    partial = type(prototype).model_validate(complete.model_copy(update={"pieces": (*middle, left)}))
    assert isinstance(partial.complete(), ConditionUnavailablePredicate)
    with pytest.raises(ValidationError, match="actual snapshot assembly"):
        type(prototype).model_validate(complete.model_copy(update={"assembled_at": prototype.assembled_at}))


def sign_history(values: tuple[Literal[-1, 0, 1] | None, ...]) -> ConditionTopVenueSignHistory:
    from tank.analysis.condition_graph_contracts import (
        ConditionSignHistoryWindow,
        ConditionVenueSignObservation,
    )

    start = AT - timedelta(hours=1)
    windows = tuple(
        ConditionSignHistoryWindow(
            interval=ConditionInterval(
                window="W15M",
                start=start + timedelta(minutes=15 * index),
                cutoff=start + timedelta(minutes=15 * (index + 1)),
            ),
            observations=(
                ConditionVenueSignObservation(
                    venue="a", value=value, reason="INCOMPLETE" if value is None else None
                ),
            ),
        )
        for index, value in enumerate(values)
    )
    return ConditionTopVenueSignHistory(
        token_id="T",
        interval=ConditionInterval(window="W1H", start=start, cutoff=AT),
        selected_venues=("a",),
        windows=windows,
    )


def test_top_sign_history_distinguishes_observed_neutral_from_missing_direction() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionObservedPredicate,
        ConditionUnavailablePredicate,
        ConditionVenueSignObservation,
    )

    actual = sign_history((1, 0, -1, 1)).multiple_flips()
    assert isinstance(actual, ConditionObservedPredicate) and actual.value is True
    neutral = sign_history((0, 0, 0, 0)).multiple_flips()
    assert isinstance(neutral, ConditionObservedPredicate) and neutral.value is False
    assert isinstance(sign_history((1, None, -1, 1)).multiple_flips(), ConditionUnavailablePredicate)
    assert isinstance(sign_history((1, -1)).multiple_flips(), ConditionUnavailablePredicate)
    with pytest.raises(ValidationError, match="integer"):
        ConditionVenueSignObservation.model_validate({"venue": "a", "value": True, "reason": None})
    original = sign_history((1, 0, -1, 1))
    with pytest.raises(ValidationError, match="consecutive"):
        type(original).model_validate(original.model_copy(update={"windows": original.windows[1:]}))


def split_execution() -> ConditionSplitExecution:
    from tank.analysis.condition_graph_contracts import (
        ConditionImpactOrder,
        ConditionImpactOrders,
        ConditionSplitExecutionBook,
    )
    from tank.market.book_contracts import BookLevel

    assembled = Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY")
    book = ConditionSplitExecutionBook(
        token_id="T",
        instrument_id="a:spot:T",
        venue="a",
        observed_at=Timestamp(value="2026-10-17T11:59:55Z", precision=None, precision_reason="NOT_CAPTURED"),
        visibility=ConditionReceiptVisibility(
            received_at=Timestamp(value="2026-10-17T11:59:56Z", precision="COLLECTION_ONLY"),
            assembled_at=assembled,
        ),
        integrity_ok=True,
        quote_asset="USD",
        quote_valuation=QuoteUsdValuation(
            rate=Decimal(1), as_of=None, basis="USD_UNIT", unavailable_reason=None
        ),
        mid_quote=Decimal(100),
        allocation_depth_p1_usd=Decimal(10000),
        allocation_depth_reason=None,
        asks=(BookLevel.from_wire("101", "100"),),
    )
    order = ConditionImpactOrder(notional_usd=Decimal(100), reason=None)
    return ConditionSplitExecution(
        token_id="T",
        cutoff=AT,
        assembled_at=assembled,
        books=(book,),
        orders=ConditionImpactOrders(T1=order, T2=order, T3=order),
    )


def test_split_execution_ignores_known_zero_allocations_without_inventing_a_fill() -> None:
    from tank.analysis.condition_graph_contracts import ObservedConditionSlippage

    split = split_execution()
    empty = type(split.books[0]).model_validate(
        split.books[0].model_copy(
            update={
                "venue": "b",
                "instrument_id": "b:spot:T",
                "allocation_depth_p1_usd": Decimal(0),
                "asks": (),
            }
        )
    )
    with_empty = type(split).model_validate(split.model_copy(update={"books": (*split.books, empty)}))
    value = with_empty.split_slippage("T1")
    assert isinstance(value, ObservedConditionSlippage) and value.value == pytest.approx(100)
    assert with_empty.books[1].asks == ()
    assert value == split.split_slippage("T1")


def test_split_execution_preserves_missing_depth_fx_and_zero_order_causes() -> None:
    from tank.analysis.condition_graph_contracts import ConditionImpactOrder, UnavailableConditionSlippage

    split = split_execution()
    missing = type(split.books[0]).model_validate(
        split.books[0].model_copy(
            update={
                "allocation_depth_p1_usd": None,
                "allocation_depth_reason": "INCOMPLETE",
            }
        )
    )
    no_depth = type(split).model_validate(split.model_copy(update={"books": (missing,)})).split_slippage("T1")
    assert isinstance(no_depth, UnavailableConditionSlippage) and no_depth.reason == "INCOMPLETE"
    zero_orders = type(split.orders).model_validate(
        split.orders.model_copy(update={"T1": ConditionImpactOrder(notional_usd=Decimal(0), reason=None)})
    )
    zero = type(split).model_validate(split.model_copy(update={"orders": zero_orders})).split_slippage("T1")
    assert isinstance(zero, UnavailableConditionSlippage) and zero.reason == "ZERO_NOTIONAL"
    no_fx = type(split.books[0]).model_validate(
        split.books[0].model_copy(
            update={
                "quote_asset": "USDT",
                "quote_valuation": QuoteUsdValuation(
                    rate=None,
                    as_of=None,
                    basis="UNAVAILABLE",
                    unavailable_reason="QUOTE_USD_RATE_UNAVAILABLE",
                ),
            }
        )
    )
    result = type(split).model_validate(split.model_copy(update={"books": (no_fx,)})).split_slippage("T1")
    assert (
        isinstance(result, UnavailableConditionSlippage) and result.reason == "QUOTE_CONVERSION_UNAVAILABLE"
    )


def test_enriched_liquidity_binds_selected_book_orders_and_actual_family_scope() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionFragmentationGraph,
        ConditionImpactOrder,
        ConditionImpactOrders,
        ConditionVolatilityCompression,
        EnrichedConditionLiquidity,
        ObservedConditionSlippage,
        UnavailableConditionVolatility,
    )

    basic = single_venue_liquidity()
    source = split_execution()
    venue = basic.per_venue[0]
    book = type(source.books[0]).model_validate(
        source.books[0].model_copy(
            update={
                "instrument_id": venue.instrument_id,
                "observed_at": venue.observed_at,
                "allocation_depth_p1_usd": Decimal(100),
            }
        )
    )
    orders = ConditionImpactOrders(
        T1=ConditionImpactOrder(notional_usd=venue.impact_tiers.T1.notional_usd, reason=None),
        T2=ConditionImpactOrder(notional_usd=venue.impact_tiers.T2.notional_usd, reason=None),
        T3=ConditionImpactOrder(notional_usd=venue.impact_tiers.T3.notional_usd, reason=None),
    )
    execution = type(source).model_validate(source.model_copy(update={"orders": orders, "books": (book,)}))
    fragment_interval = ConditionInterval(window="W15M", start=AT - timedelta(minutes=15), cutoff=AT)
    funding = ConditionFundingConfirmationGraph(
        token_id="T",
        assembled_at=source.assembled_at,
        cutoff=AT,
        primary=(),
        independent=(),
        normal_range_8h=DecimalRange(bounds=None, reason="INCOMPLETE"),
    )
    fragment = ConditionFragmentationGraph(
        token_id="T",
        assembled_at=source.assembled_at,
        interval=fragment_interval,
        price_pairs=(),
        price_confirmations=(),
        funding=funding,
        flow_capture=(),
        top_sign_history=ConditionTopVenueSignHistory(
            token_id="T", interval=basic.interval, selected_venues=(), windows=()
        ),
    )
    graph = EnrichedConditionLiquidity(
        kind="ENRICHED",
        token_id="T",
        assembled_at=source.assembled_at,
        interval=basic.interval,
        measurement_band_percent=basic.measurement_band_percent,
        per_venue=basic.per_venue,
        current_depth=basic.current_depth,
        previous_depth=basic.previous_depth,
        elasticity=basic.elasticity,
        replenishment=basic.replenishment,
        fragmentation=fragment,
        execution=execution,
        vol_compression=ConditionVolatilityCompression(
            token_id="T",
            reference_venue=None,
            cutoff=AT,
            window="W24H",
            realized=UnavailableConditionVolatility(status="UNAVAILABLE", value=None, reason="INCOMPLETE"),
            normal_range=FloatRange(bounds=None, reason="INCOMPLETE"),
        ),
    )
    value = graph.execution.split_slippage("T1")
    assert isinstance(value, ObservedConditionSlippage) and value.value == pytest.approx(100)
    best = graph.best_single_venue_slippage("T1")
    assert best.value is None
    with pytest.raises(ValidationError, match="selected book cohort"):
        type(graph).model_validate(
            graph.model_copy(update={"execution": execution.model_copy(update={"books": ()})})
        )


def test_missing_volatility_reference_does_not_invent_a_venue_for_an_unavailable_measurement() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionVolatilityCompression,
        ObservedConditionVolatility,
        UnavailableConditionVolatility,
    )

    graph = ConditionVolatilityCompression(
        token_id="T",
        reference_venue=None,
        cutoff=AT,
        window="W24H",
        realized=UnavailableConditionVolatility(status="UNAVAILABLE", value=None, reason="INCOMPLETE"),
        normal_range=FloatRange(bounds=None, reason="INCOMPLETE"),
    )
    assert graph.classification().value is None
    with pytest.raises(ValidationError, match="actual reference venue"):
        type(graph).model_validate(
            graph.model_copy(
                update={
                    "realized": ObservedConditionVolatility(
                        status="OBSERVED",
                        value=0.0,
                        completed_hour=Timestamp(
                            value="2026-10-17T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
                        ),
                        return_count=24,
                        resolution_s=3600,
                    )
                }
            )
        )


def holders_revenue() -> CapturedConditionHoldersRevenue:
    from tank.analysis.condition_graph_contracts import (
        ConditionHoldersRevenuePeriod,
        ConditionHoldersRevenueProtocol,
        SupplyCadenceScope,
        UnavailableConditionCadence,
    )

    protocol = ConditionHoldersRevenueProtocol(
        protocol="example",
        provider="DefiLlama",
        unit="USD",
        source_url=None,
        periods=(
            ConditionHoldersRevenuePeriod(provider_period="24h", value_usd=Decimal(0), status="OBSERVED"),
            ConditionHoldersRevenuePeriod(provider_period="30d", value_usd=None, status="UNAVAILABLE"),
        ),
        chart_available=None,
        latest_chart_point=None,
        reason=None,
    )
    return CapturedConditionHoldersRevenue(
        token_id="T",
        status="OBSERVED",
        protocols=(protocol,),
        as_of=Timestamp(value="2026-10-17T11:59:00Z", precision=None, precision_reason="NOT_CAPTURED"),
        visibility=ConditionReceiptVisibility(
            received_at=Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
            assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
        ),
        cutoff=AT,
        cadence=UnavailableConditionCadence(
            status="UNAVAILABLE",
            seconds=None,
            scope=SupplyCadenceScope(
                kind="SUPPLY", collection="supply", token_id="T", observation_kind="FUNDAMENTALS", chain=None
            ),
            origin=None,
            reason="CADENCE_NOT_CAPTURED",
        ),
        identity_evidence=None,
        identity_reason="IDENTITY_EVIDENCE_NOT_CAPTURED",
        source_observation_sha256="a" * 64,
        reason=None,
    )


def test_holders_revenue_preserves_zero_unknown_period_and_capture_provenance_separately() -> None:
    from tank.analysis.condition_graph_contracts import ConditionUnavailablePredicate

    graph = holders_revenue()
    assert graph.coverage("24h").complete is True
    assert graph.coverage("30d").observed == 0
    assert graph.availability() == "PARTIAL"
    assert graph.protocols[0].periods[0].value_usd == 0
    assert graph.protocols[0].chart_available is None
    assert graph.as_of.precision is None
    assert isinstance(graph.visibility.eligible(), ConditionUnavailablePredicate)
    with pytest.raises(ValidationError, match="source cadence"):
        type(graph).model_validate(graph.model_copy(update={"status": "STALE"}))


def test_holders_revenue_provider_seconds_do_not_invent_a_day_period_or_accept_future_points() -> None:
    from tank.analysis.condition_graph_contracts import ConditionHoldersRevenueChartPoint

    graph = holders_revenue()
    point = ConditionHoldersRevenueChartPoint(
        provider_timestamp=str(int((AT - timedelta(days=1)).timestamp())),
        value_usd=Decimal("1.25"),
        provider_period="UNSPECIFIED",
    )
    protocol = type(graph.protocols[0]).model_validate(
        graph.protocols[0].model_copy(update={"chart_available": True, "latest_chart_point": point})
    )
    actual = type(graph).model_validate(graph.model_copy(update={"protocols": (protocol,)}))
    assert actual.protocols[0].latest_chart_point == point
    assert point.observed_at() == AT - timedelta(days=1)
    future = type(point).model_validate(
        point.model_copy(update={"provider_timestamp": str(int((AT + timedelta(seconds=1)).timestamp()))})
    )
    with pytest.raises(ValidationError, match="future provider"):
        type(graph).model_validate(
            graph.model_copy(
                update={"protocols": (protocol.model_copy(update={"latest_chart_point": future}),)}
            )
        )
    with pytest.raises(ValidationError):
        ConditionHoldersRevenueChartPoint(
            provider_timestamp="NaN", value_usd=Decimal(0), provider_period="UNSPECIFIED"
        )


def inventory_comparison(current: Decimal, previous: Decimal) -> ComparableConditionExchangeInventory:
    from tank.analysis.condition_graph_contracts import (
        ConditionInventoryBalanceChange,
        ConditionInventoryCoverage,
    )

    visibility = ConditionReceiptVisibility(
        received_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
        assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
    )
    return ComparableConditionExchangeInventory(
        status="COMPARABLE_BALANCES",
        token_id="T",
        requested_window="W24H",
        cutoff=AT,
        current_observed_at=Timestamp(
            value="2026-10-17T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
        ),
        previous_observed_at=Timestamp(
            value="2026-10-16T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
        ),
        current_visibility=visibility,
        previous_visibility=visibility,
        balances=(
            ConditionInventoryBalanceChange(
                venue="a",
                chain_id=1,
                contract="token-contract",
                wallet_list_commit="captured-list-1",
                current_qty_native=current,
                previous_qty_native=previous,
            ),
        ),
        coverage=ConditionInventoryCoverage(
            complete_balances=1,
            observed_balances=1,
            comparable_balances=1,
            coverage_errors=(),
            unsupported_venues=(),
        ),
        normal_range=FloatRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
    )


def test_inventory_comparison_preserves_exact_native_change_and_actual_cohort_identity() -> None:
    graph = inventory_comparison(Decimal("1" + "0" * 100), Decimal("9" * 100))
    assert graph.native_change().value == Decimal(1)
    assert graph.balances[0].change() == Decimal(1)
    assert graph.balances[0].wallet_list_commit == "captured-list-1"
    with pytest.raises(ValidationError, match="distinct actual wallet-list"):
        type(graph).model_validate(
            graph.model_copy(update={"balances": (graph.balances[0], graph.balances[0])})
        )
    with pytest.raises(ValidationError, match="prior source clock"):
        type(graph).model_validate(
            graph.model_copy(
                update={
                    "previous_observed_at": Timestamp(
                        value=None, precision=None, precision_reason="NOT_CAPTURED"
                    )
                }
            )
        )


def test_inventory_zero_baseline_and_empty_comparable_cohort_are_distinct() -> None:
    graph = inventory_comparison(Decimal(0), Decimal(0))
    assert graph.native_change().value == Decimal(0)
    assert graph.fractional_change().reason == "ZERO_BASELINE"
    assert graph.fractional_change().value is None
    empty_coverage = type(graph.coverage).model_validate(
        graph.coverage.model_copy(update={"comparable_balances": 0})
    )
    missing = type(graph).model_validate(
        graph.model_copy(
            update={
                "balances": (),
                "coverage": empty_coverage,
                "previous_observed_at": Timestamp(
                    value=None, precision=None, precision_reason="NOT_CAPTURED"
                ),
            }
        )
    )
    assert missing.native_change().reason == "INCOMPLETE"
    assert missing.fractional_change().reason == "INCOMPLETE"
    assert inventory_comparison(Decimal(110), Decimal(100)).fractional_change().value == pytest.approx(0.1)


def wallet_flows() -> CapturedConditionWalletFlows:
    from tank.analysis.condition_graph_contracts import (
        ConditionUnprovenWalletCoverage,
        ConditionWalletFlow,
        ConditionWalletFlowEvidenceRef,
    )

    assembled = Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY")
    visibility = ConditionReceiptVisibility(
        received_at=Timestamp(value="2026-10-17T11:59:59Z", precision="COLLECTION_ONLY"),
        assembled_at=assembled,
    )
    flow = ConditionWalletFlow(
        token_id="T",
        observed_at=Timestamp(value="2026-10-17T11:59:00Z", precision="EXACT"),
        visibility=visibility,
        classification="AMBIGUOUS",
        classification_reason=None,
        quantity_native=Decimal(10),
        quantity_reason=None,
        reaches_exchange=True,
        destination_reason=None,
        evidence=ConditionWalletFlowEvidenceRef(source_observation_sha256="b" * 64, flow_index=0),
    )
    return CapturedConditionWalletFlows(
        status="CAPTURED_RECORDS",
        token_id="T",
        interval=ConditionInterval(window="W15M", start=AT - timedelta(minutes=15), cutoff=AT),
        assembled_at=assembled,
        flows=(flow,),
        successful_queries=1,
        query_visibility=visibility,
        coverage=ConditionUnprovenWalletCoverage(status="UNAVAILABLE", reason="QUERY_SCOPE_NOT_CAPTURED"),
        coverage_errors=(),
    )


def test_wallet_flow_observation_does_not_turn_partial_no_positive_into_proven_absence() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionNoPositiveSupplyObservation,
        ConditionPositiveSupplyObservation,
        ConditionUnavailablePredicate,
    )

    graph = wallet_flows()
    assert isinstance(graph.flows[0].positive_supply_witness(), ConditionUnavailablePredicate)
    result = graph.observed_evidence()
    assert isinstance(result, ConditionNoPositiveSupplyObservation) and result.observation_count == 1
    assert graph.coverage.status == "UNAVAILABLE"
    positive = type(graph).model_validate(
        graph.model_copy(
            update={"flows": (graph.flows[0].model_copy(update={"classification": "TREASURY_INTERNAL"}),)}
        )
    )
    observed = positive.observed_evidence()
    assert isinstance(observed, ConditionPositiveSupplyObservation) and observed.evidence == (
        graph.flows[0].evidence,
    )
    zero = type(graph.flows[0]).model_validate(
        positive.flows[0].model_copy(update={"quantity_native": Decimal(0)})
    )
    from tank.analysis.condition_graph_contracts import ConditionObservedPredicate

    no_positive = zero.positive_supply_witness()
    assert isinstance(no_positive, ConditionObservedPredicate) and no_positive.value is False


def test_wallet_query_counter_does_not_prove_visibility_or_expand_the_occurrence_window() -> None:
    from tank.analysis.condition_graph_contracts import UnavailableConditionSupplyObservation

    graph = wallet_flows()
    unknown_visibility = ConditionReceiptVisibility(
        received_at=Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
        assembled_at=graph.assembled_at,
    )
    invisible = type(graph).model_validate(
        graph.model_copy(
            update={
                "query_visibility": unknown_visibility,
                "flows": (graph.flows[0].model_copy(update={"visibility": unknown_visibility}),),
            }
        )
    )
    assert invisible.successful_queries == 1
    assert isinstance(invisible.observed_evidence(), UnavailableConditionSupplyObservation)
    with pytest.raises(ValidationError, match="half-open"):
        type(graph).model_validate(
            graph.model_copy(
                update={
                    "flows": (
                        graph.flows[0].model_copy(
                            update={
                                "observed_at": Timestamp(value="2026-10-17T12:00:00Z", precision="EXACT"),
                            }
                        ),
                    )
                }
            )
        )


def lending_pool() -> ConditionLendingPoolCalculation:
    from tank.analysis.condition_graph_contracts import UnrecordedConditionLendingIdentity

    return ConditionLendingPoolCalculation(
        pool_id="fixture-pool",
        identity=UnrecordedConditionLendingIdentity(
            status="UNRECORDED", reason="REGISTRY_POOL_IDENTITY_NOT_CAPTURED"
        ),
        total_supply_usd=Decimal(100),
        total_borrow_usd=Decimal(0),
        previous_utilization=Decimal(0),
        borrow_apy=7.0,
    )


def test_lending_known_zero_and_previous_zero_are_not_missing_or_an_apr() -> None:
    pool = lending_pool()
    assert pool.utilization().value == 0
    assert pool.change().value == 0
    assert pool.borrow_apr().value is None
    assert pool.borrow_apr().reason == "APY_IS_NOT_APR"
    over = type(pool).model_validate(pool.model_copy(update={"total_borrow_usd": Decimal(150)}))
    assert over.utilization().value == Decimal("1.5")
    assert over.change().value == 1.5
    no_supply = type(pool).model_validate(pool.model_copy(update={"total_supply_usd": Decimal(0)}))
    assert no_supply.utilization().reason == "ZERO_SUPPLY"
    missing = type(pool).model_validate(pool.model_copy(update={"total_borrow_usd": None}))
    assert missing.utilization().reason == "INCOMPLETE"
    no_previous = type(pool).model_validate(pool.model_copy(update={"previous_utilization": None}))
    assert no_previous.change().reason == "INCOMPLETE"


def test_lending_pool_cannot_borrow_another_registered_identity() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionLendingCatalogue,
        RecordedConditionCaptureIdentity,
        RegisteredConditionLendingIdentity,
    )
    from tank.contracts.provenance import ResolvedVersionIdentity
    from tank.contracts.registry import LendingPool

    pool = lending_pool()
    identity = RegisteredConditionLendingIdentity(
        status="REGISTERED",
        pool=LendingPool(
            pool_id="other-pool",
            project="fixture-project",
            chain="fixture-chain",
            underlying_tokens=["fixture-contract"],
        ),
        catalogue=ConditionLendingCatalogue(
            source_url="https://example.test/catalogue",
            captured_at_label="fixture-catalogue-v1",
            source_sha256="c" * 64,
        ),
        capture_identity=RecordedConditionCaptureIdentity(
            status="RECORDED",
            versions=ResolvedVersionIdentity(
                config_version="fixture-config-v1", code_version="fixture-code-v1"
            ),
        ),
    )
    with pytest.raises(ValidationError, match="another registered pool"):
        type(pool).model_validate(pool.model_copy(update={"identity": identity}))


def test_lending_observed_change_requires_actual_prior_timestamp_and_same_assembly() -> None:
    from tank.analysis.condition_graph_contracts import CapturedConditionLending

    visibility = wallet_flows().query_visibility
    with pytest.raises(ValidationError, match="actual prior capture"):
        CapturedConditionLending(
            status="CAPTURED_POOLS",
            token_id="T",
            requested_window="W24H",
            cutoff=AT,
            observed_at=Timestamp(
                value="2026-10-17T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
            ),
            previous_observed_at=Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
            current_visibility=visibility,
            previous_visibility=visibility,
            pools=(lending_pool(),),
            unmapped_pool_ids=(),
        )
    recorded = CapturedConditionLending(
        status="CAPTURED_POOLS",
        token_id="T",
        requested_window="W24H",
        cutoff=AT,
        observed_at=Timestamp(value="2026-10-17T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"),
        previous_observed_at=Timestamp(
            value="2026-10-16T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
        ),
        current_visibility=visibility,
        previous_visibility=visibility,
        pools=(lending_pool(),),
        unmapped_pool_ids=(),
    )
    assert recorded.pools[0].change().value == 0
    with pytest.raises(ValidationError, match="distinct from unmapped"):
        type(recorded).model_validate(
            recorded.model_copy(update={"unmapped_pool_ids": (recorded.pools[0].pool_id,)})
        )


def test_boundary_proofs_cannot_cross_actual_token_venue_or_market_scope() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionBoundaryGroup,
        ConditionSpotVenue,
        IncompleteConditionTradeCounts,
    )

    spot = basic_spot_graph()
    base = legacy_boundary_calculation()
    aggregate = type(base).model_validate(
        base.model_copy(
            update={
                "venue": "A",
                "buy_qty": Decimal(15),
                "sell_qty": Decimal(0),
                "sum_sq_qty": Decimal(15),
                "counts": IncompleteConditionTradeCounts(
                    status="UNAVAILABLE", n_buy=None, n_sell=0, reason="INCOMPLETE"
                ),
            }
        )
    )
    group = ConditionBoundaryGroup(start=aggregate.start, cutoff=aggregate.end, aggregates=(aggregate,))
    venue = ConditionSpotVenue.model_validate(
        spot.venues[0].model_copy(update={"boundary_segments": (group,)})
    )
    checked = BasicConditionSpot.model_validate(spot.model_copy(update={"venues": (venue, spot.venues[1])}))
    assert checked.token_id == "T"
    for field, value in (("venue", "OTHER"), ("market_type", "PERP")):
        wrong = type(group).model_validate(
            group.model_copy(update={"aggregates": (aggregate.model_copy(update={field: value}),)})
        )
        with pytest.raises(ValidationError, match="actual venue and spot"):
            ConditionSpotVenue.model_validate(venue.model_copy(update={"boundary_segments": (wrong,)}))
    other_token = type(group).model_validate(
        group.model_copy(update={"aggregates": (aggregate.model_copy(update={"token_id": "OTHER"}),)})
    )
    bad_venue = ConditionSpotVenue.model_validate(
        venue.model_copy(update={"boundary_segments": (other_token,)})
    )
    with pytest.raises(ValidationError, match="another token"):
        BasicConditionSpot.model_validate(spot.model_copy(update={"venues": (bad_venue, spot.venues[1])}))
    with pytest.raises(ValidationError, match="cannot mix"):
        ConditionBoundaryGroup(
            start=aggregate.start,
            cutoff=aggregate.end,
            aggregates=(aggregate, aggregate.model_copy(update={"token_id": "OTHER"})),
        )


def test_flow_capture_proof_cannot_borrow_another_actual_token_venue_or_market() -> None:
    window = complete_perp_flow_window("a:perp:T", "a")
    for field, value in (("token_id", "OTHER"), ("venue", "OTHER"), ("market_type", "SPOT")):
        piece = window.pieces[0].model_copy(
            update={"aggregate": window.pieces[0].aggregate.model_copy(update={field: value})}
        )
        with pytest.raises(ValidationError, match="another token/venue/market"):
            type(window).model_validate(window.model_copy(update={"pieces": (piece, *window.pieces[1:])}))


def test_observed_zero_spot_volume_has_no_majority_or_arbitrary_contributor() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionSpotBreadth,
        ConditionSpotConcentration,
        ConditionSpotCoverage,
        ConditionSpotMajority,
        ConditionSpotTopN,
        ConditionWeightedBreadth,
        ObservedConditionComposite,
    )

    source = basic_spot_graph()
    venues = tuple(
        type(row).model_validate(
            row.model_copy(
                update={
                    "volume_native": Decimal(0),
                    "buy_qty": Decimal(0),
                    "sell_qty": Decimal(0),
                    "cvd": Decimal(0),
                    "sum_sq_qty": Decimal(0),
                    "sign": 0,
                    "buy_share": None,
                    "bs_ratio": None,
                    "volume_share": None,
                    "null_reasons": {
                        "buy_share": "ZERO_TOTAL_VOLUME",
                        "bs_ratio": "ZERO_SELL_QUANTITY",
                        "volume_share": "ZERO_TOTAL_VOLUME",
                    },
                }
            )
        )
        for row in source.venues
    )
    empty_top = ConditionSpotTopN(n=0, venues=(), agreement=None, agreement_reason="ZERO_TOTAL_VOLUME")
    concentration = ConditionSpotConcentration(
        hhi=None,
        top_volume_venue=None,
        top_volume_share=None,
        top_contrib_venue=None,
        top_contrib_share=None,
        others_cwb_dir=None,
        null_reasons={
            "hhi": "ZERO_TOTAL_VOLUME",
            "top_volume_venue": "ZERO_TOTAL_VOLUME",
            "top_volume_share": "ZERO_TOTAL_VOLUME",
            "top_contrib_venue": "ZERO_DIRECTIONAL_CONTRIBUTION",
            "top_contrib_share": "ZERO_DIRECTIONAL_CONTRIBUTION",
            "others_cwb_dir": "ZERO_DIRECTIONAL_CONTRIBUTION",
        },
    )
    graph = BasicConditionSpot(
        token_id="T",
        kind="BASIC",
        venues=venues,
        composite_max=ObservedConditionComposite(
            status="OBSERVED",
            cvd=Decimal(0),
            volume=Decimal(0),
            buy_share=None,
            bs_ratio=None,
            venues=("A", "B"),
            sign=0,
            null_reasons={"buy_share": "ZERO_TOTAL_VOLUME", "bs_ratio": "ZERO_SELL_QUANTITY"},
        ),
        breadth=ConditionSpotBreadth(
            n_pos=0, n_neg=0, n_neutral=2, pct_pos=0.0, pct_neg=0.0, null_reasons={}
        ),
        cw_breadth=ConditionWeightedBreadth(
            buy=None,
            sell=None,
            neutral=None,
            null_reasons={
                "buy": "ZERO_TOTAL_VOLUME",
                "sell": "ZERO_TOTAL_VOLUME",
                "neutral": "ZERO_TOTAL_VOLUME",
            },
        ),
        exact_majority=ConditionSpotMajority(
            buy=None,
            sell=None,
            neutral=None,
            covered=None,
            null_reasons={
                "buy": "ZERO_TOTAL_VOLUME",
                "sell": "ZERO_TOTAL_VOLUME",
                "neutral": "ZERO_TOTAL_VOLUME",
                "covered": "ZERO_TOTAL_VOLUME",
            },
        ),
        topn=empty_top,
        concentration=concentration,
        direction="NEUTRAL",
        direction_reason=None,
        classification="INSUFFICIENT_DATA",
        coverage=ConditionSpotCoverage(
            covered_share_est=None,
            exact_majority=None,
            n_complete=2,
            excluded=(),
            null_reasons={"covered_share_est": "ZERO_TOTAL_VOLUME", "exact_majority": "ZERO_TOTAL_VOLUME"},
        ),
    )
    assert graph.topn.n == 0 and graph.direction == "NEUTRAL"
    assert isinstance(graph.composite_max, ObservedConditionComposite)
    assert graph.composite_max.volume == 0
    with pytest.raises(ValidationError, match="majority set or top contributor"):
        type(graph).model_validate(graph.model_copy(update={"topn": source.topn}))


def test_unrepresentable_display_ratios_preserve_known_native_values_and_other_sides() -> None:
    from tank.analysis.condition_graph_contracts import ConditionWeightedBreadth, ObservedConditionComposite

    composite = ObservedConditionComposite(
        status="OBSERVED",
        cvd=Decimal("-0." + "9" * 400),
        volume=Decimal("1." + "0" * 399 + "1"),
        buy_share=None,
        bs_ratio=None,
        venues=("a",),
        sign=0,
        null_reasons={"buy_share": "NOT_ESTIMABLE", "bs_ratio": "NOT_ESTIMABLE"},
    )
    assert composite.volume > 1 and composite.cvd > -1
    breadth = ConditionWeightedBreadth(buy=None, sell=1.0, neutral=0.0, null_reasons={"buy": "NOT_ESTIMABLE"})
    assert breadth.sell == 1.0 and breadth.neutral == 0.0 and breadth.buy is None
    with pytest.raises(ValidationError, match="same actual volume denominator"):
        type(breadth).model_validate(
            breadth.model_copy(update={"null_reasons": {"buy": "ZERO_TOTAL_VOLUME"}})
        )


def unlock_schedule() -> CapturedConditionUnlocks:
    from tank.analysis.condition_graph_contracts import (
        ConditionScheduledUnlock,
        UnavailableConditionCirculatingShare,
    )
    from tank.util import iso

    visibility = wallet_flows().query_visibility
    unlock = ConditionScheduledUnlock(
        token_id="T",
        scheduled_at=Timestamp(
            value=iso(AT + timedelta(days=14)), precision=None, precision_reason="NOT_CAPTURED"
        ),
        quantity_native=Decimal(100),
        quantity_reason=None,
        label=None,
        circulating_share=UnavailableConditionCirculatingShare(
            status="UNAVAILABLE", value=None, reason="NOT_CAPTURED"
        ),
        recipient_class=None,
        transferable=True,
        transferability_reason=None,
        source_observation_sha256="d" * 64,
        schedule_index=0,
    )
    return CapturedConditionUnlocks(
        status="CAPTURED_SCHEDULE",
        token_id="T",
        cutoff=AT,
        observed_at=Timestamp(value="2026-10-17T11:59:00Z", precision=None, precision_reason="NOT_CAPTURED"),
        visibility=visibility,
        upcoming=(unlock,),
    )


def test_unlock_window_preserves_actual_optional_metadata_and_inclusive_fourteen_day_endpoint() -> None:
    from tank.util import iso

    schedule = unlock_schedule()
    assert schedule.upcoming[0].transferable is True
    assert schedule.upcoming[0].label is None
    assert schedule.upcoming[0].scheduled_at.precision is None
    assert schedule.upcoming[0].circulating_share.value is None
    with pytest.raises(ValidationError, match="fourteen-day"):
        type(schedule).model_validate(
            schedule.model_copy(
                update={
                    "upcoming": (
                        schedule.upcoming[0].model_copy(
                            update={
                                "scheduled_at": Timestamp(
                                    value=iso(AT + timedelta(days=14, seconds=1)),
                                    precision=None,
                                    precision_reason="NOT_CAPTURED",
                                ),
                            }
                        ),
                    )
                }
            )
        )
    unknown = type(schedule.upcoming[0]).model_validate(
        schedule.upcoming[0].model_copy(
            update={
                "quantity_native": None,
                "quantity_reason": "QUANTITY_NOT_CAPTURED",
                "transferable": None,
                "transferability_reason": "TRANSFERABILITY_NOT_CAPTURED",
            }
        )
    )
    assert unknown.quantity_native is None and unknown.transferable is None


def test_inventory_capture_errors_preserve_actual_alternative_failure_shapes() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionInventoryCoverage,
        ConditionInventoryDecimalsError,
        ConditionInventoryWalletCaptureError,
        ConditionUnsupportedInventoryChain,
    )

    coverage = ConditionInventoryCoverage(
        complete_balances=0,
        observed_balances=0,
        comparable_balances=0,
        coverage_errors=(
            ConditionUnsupportedInventoryChain(
                kind="CHAIN_NOT_SUPPORTED", chain="unsupported-chain", reason="CHAIN_NOT_SUPPORTED"
            ),
            ConditionInventoryDecimalsError(
                kind="DECIMALS_CAPTURE_FAILED",
                chain="chain",
                chain_id=1,
                contract="contract",
                operation="decimals",
                reason="",
            ),
            ConditionInventoryWalletCaptureError(
                kind="WALLET_CAPTURE_FAILED", wallet="wallet", chain_id=1, reason="provider refusal"
            ),
        ),
        unsupported_venues=(),
    )
    assert len(coverage.coverage_errors) == 3
    assert isinstance(coverage.coverage_errors[0], ConditionUnsupportedInventoryChain)
    assert isinstance(coverage.coverage_errors[1], ConditionInventoryDecimalsError)
    assert coverage.coverage_errors[1].reason == ""


def chain_liquidity() -> CapturedConditionChainLiquidity:
    from tank.analysis.condition_graph_contracts import (
        SelectedConditionStablecoinPoint,
        UnavailableConditionStablecoinPoint,
    )

    return CapturedConditionChainLiquidity(
        status="CAPTURED_CHAIN_CONTEXT",
        token_id="T",
        chain="chain",
        cutoff=AT,
        observed_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
        visibility=wallet_flows().query_visibility,
        current=SelectedConditionStablecoinPoint(
            status="SELECTED_SOURCE_POINT",
            observed_at=Timestamp(
                value="2026-10-17T00:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
            ),
            value_usd=Decimal(0),
            value_reason=None,
            series_index=2,
        ),
        day_ago=SelectedConditionStablecoinPoint(
            status="SELECTED_SOURCE_POINT",
            observed_at=Timestamp(
                value="2026-10-16T00:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
            ),
            value_usd=Decimal(10),
            value_reason=None,
            series_index=1,
        ),
        week_ago=UnavailableConditionStablecoinPoint(status="UNAVAILABLE", reason="NO_ASOF_POINT"),
        source_observation_sha256="e" * 64,
    )


def test_chain_supply_change_preserves_actual_usd_zero_and_missing_historical_point() -> None:
    graph = chain_liquidity()
    assert graph.change_24h().value == Decimal(-10)
    assert graph.change_24h().unit == "USD"
    assert graph.change_7d().value is None
    assert graph.change_7d().reason == "INCOMPLETE"
    from tank.analysis.condition_graph_contracts import SelectedConditionStablecoinPoint

    assert isinstance(graph.current, SelectedConditionStablecoinPoint)
    assert graph.current.observed_at.precision is None


def test_chain_supply_selection_cannot_borrow_a_point_after_the_requested_target() -> None:
    graph = chain_liquidity()
    with pytest.raises(ValidationError, match="as-of target"):
        type(graph).model_validate(graph.model_copy(update={"day_ago": graph.current}))


def test_documented_non_supply_wallet_labels_are_not_promoted_to_exchange_supply() -> None:
    from tank.analysis.condition_graph_contracts import ConditionObservedPredicate

    flow = wallet_flows().flows[0]
    for label in ("CEX_CUSTODY_MIGRATION", "FRESH_ACCUMULATION", "OTC_SETTLEMENT"):
        actual = type(flow).model_validate(flow.model_copy(update={"classification": label}))
        result = actual.positive_supply_witness()
        assert isinstance(result, ConditionObservedPredicate) and result.value is False


def empty_supply_graph() -> ConditionSupplyGraph:
    from tank.analysis.condition_graph_contracts import (
        ConfiguredConditionSettlementChain,
        UnavailableConditionExchangeInventory,
        UnavailableConditionHoldersRevenue,
        UnavailableConditionLending,
        UnavailableConditionUnlocks,
        UnavailableConditionWalletFlows,
    )
    from tank.contracts.provenance import ResolvedVersionIdentity

    return ConditionSupplyGraph(
        token_id="T",
        interval=ConditionInterval(window="W15M", start=AT - timedelta(minutes=15), cutoff=AT),
        assembled_at=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY"),
        settlement_chain=ConfiguredConditionSettlementChain(
            token_id="T",
            chain=None,
            origin="CURRENT_CONFIGURED_SETTLEMENT_CHAIN",
            versions=ResolvedVersionIdentity(
                config_version="fixture-config-v1", code_version="fixture-code-v1"
            ),
        ),
        exchange_inventory=UnavailableConditionExchangeInventory(
            status="UNAVAILABLE", reason="NO_CAPTURED_OBSERVATION"
        ),
        wallet_flows=UnavailableConditionWalletFlows(status="UNAVAILABLE", reason="NO_CAPTURED_OBSERVATION"),
        unlocks=UnavailableConditionUnlocks(status="UNAVAILABLE", reason="NO_CAPTURED_OBSERVATION"),
        lending=UnavailableConditionLending(status="UNAVAILABLE", reason="NO_CAPTURED_OBSERVATION"),
        chain_contexts=(),
        holders_revenue=UnavailableConditionHoldersRevenue(
            status="UNAVAILABLE", reason="NO_CAPTURED_FUNDAMENTALS"
        ),
    )


def test_supply_presence_distinguishes_no_inputs_from_no_positive_observation_and_a_witness() -> None:
    from tank.analysis.condition_graph_contracts import (
        ConditionNoPositiveSupplyEvidence,
        ConditionPositiveSupplyEvidence,
        UnavailableConditionSupplyEvidence,
    )

    graph = empty_supply_graph()
    assert isinstance(graph.evidence(), UnavailableConditionSupplyEvidence)
    potential = type(graph).model_validate(graph.model_copy(update={"unlocks": unlock_schedule()}))
    assert isinstance(potential.unlocks, CapturedConditionUnlocks)
    assert potential.unlocks.upcoming[0].transferable is True
    observed = potential.evidence()
    assert isinstance(observed, ConditionNoPositiveSupplyEvidence) and observed.available_inputs == (
        "UNLOCKS",
    )
    inventory = inventory_comparison(Decimal(110), Decimal(100))
    inventory = type(inventory).model_validate(
        inventory.model_copy(
            update={"normal_range": FloatRange(bounds=FloatBounds(lower=-0.01, upper=0.05), reason=None)}
        )
    )
    increased = type(graph).model_validate(graph.model_copy(update={"exchange_inventory": inventory}))
    evidence = increased.evidence()
    assert isinstance(evidence, ConditionPositiveSupplyEvidence)
    assert evidence.inventory is not None and evidence.inventory.native_change == 10
    assert evidence.wallet_flows == ()


def test_supply_primary_chain_uses_captured_configuration_instead_of_first_context() -> None:
    from tank.analysis.condition_graph_contracts import (
        CapturedConditionChainLiquidity,
        UnavailableConditionChainLiquidity,
        UnavailableConditionSupplyEvidence,
    )

    graph = empty_supply_graph()
    policy = graph.settlement_chain.model_copy(update={"chain": "other-chain"})
    mismatched = type(graph).model_validate(
        graph.model_copy(update={"settlement_chain": policy, "chain_contexts": (chain_liquidity(),)})
    )
    primary = mismatched.primary_chain_context()
    assert isinstance(primary, UnavailableConditionChainLiquidity) and primary.chain == "other-chain"
    assert isinstance(mismatched.evidence(), UnavailableConditionSupplyEvidence)
    matched = type(graph).model_validate(
        mismatched.model_copy(update={"settlement_chain": policy.model_copy(update={"chain": "chain"})})
    )
    assert isinstance(matched.primary_chain_context(), CapturedConditionChainLiquidity)


def test_unrepresentable_inventory_and_lending_changes_are_not_fabricated_zero() -> None:
    inventory = inventory_comparison(Decimal("1" + "0" * 1000), Decimal("9" * 1000))
    assert inventory.native_change().value == 1
    assert inventory.fractional_change().reason == "NOT_ESTIMABLE"
    pool = lending_pool()
    tiny = type(pool).model_validate(
        pool.model_copy(update={"total_supply_usd": Decimal("1e1000"), "total_borrow_usd": Decimal(1)})
    )
    assert tiny.utilization().value == Decimal("1e-1000")
    assert tiny.change().reason == "NOT_ESTIMABLE"
