from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_graph_contracts import (
    ConditionCadenceScope,
    ConditionCollection,
    ConditionDepthValues,
    ConditionFieldObservation,
    ConditionInterval,
    DeclaredConditionCadence,
    NonForwardConditionInterval,
    ObservedConditionClose,
    UnavailableConditionClose,
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
