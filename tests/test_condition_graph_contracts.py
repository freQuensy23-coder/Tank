from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_graph_contracts import (
    BasicConditionSpot,
    ConditionCadenceScope,
    ConditionCollection,
    ConditionDepthValues,
    ConditionFieldObservation,
    ConditionFlowAggregate,
    ConditionInterval,
    DeclaredConditionCadence,
    NonForwardConditionInterval,
    ObservedConditionClose,
    UnavailableConditionClose,
    WindowedConditionSpot,
)
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
