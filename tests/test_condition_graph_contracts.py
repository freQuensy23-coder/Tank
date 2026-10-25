from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_graph_contracts import (
    AvailableConditionFunding,
    BasicConditionSpot,
    ConditionCadenceScope,
    ConditionCollection,
    ConditionDepthValues,
    ConditionDerivativeSupportFeatures,
    ConditionFieldObservation,
    ConditionFlowAggregate,
    ConditionFundingObservation,
    ConditionInterval,
    ConditionRelativeGraph,
    ConditionRelativeWindow,
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
        evidence=FlowAggregateEvidenceRef(projection_sha256="e" * 64),
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
        sign=0,
        reason="INCOMPLETE",
    )
    assert observed.volume == 0 and unavailable.volume is None
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
