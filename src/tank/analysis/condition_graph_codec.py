"""Explicit condition-graph serialization and consumed-evidence binding."""

from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal
from typing import Literal, Protocol

from pydantic import JsonValue

from tank.analysis.condition_graph_contracts import (
    AssessedConditionProofFreshness,
    AvailableConditionFunding,
    BasicConditionSpot,
    CapturedConditionChainLiquidity,
    CapturedConditionHoldersRevenue,
    CapturedConditionLending,
    CapturedConditionUnlocks,
    CapturedConditionWalletFlows,
    ComparableConditionExchangeInventory,
    ComputedConditionPreDepth,
    ComputedConditionReplenishmentSummary,
    ConditionAggregateLiquidations,
    ConditionBookDepthBounds,
    ConditionBoundaryGroup,
    ConditionCadence,
    ConditionChainLiquidity,
    ConditionClose,
    ConditionComputedOutcomeRanges,
    ConditionConfidence,
    ConditionConfidenceFailure,
    ConditionCoverageFailure,
    ConditionDeclaredEventAnchor,
    ConditionDepthNode,
    ConditionDerivatives,
    ConditionDerivativeSupportFeatures,
    ConditionDerivativeVenue,
    ConditionEvaluationProof,
    ConditionFieldObservation,
    ConditionFlowAggregate,
    ConditionFlowAggregateProjection,
    ConditionFlowCaptureProjection,
    ConditionFlowCaptureWitness,
    ConditionFragmentationGraph,
    ConditionFunding,
    ConditionFundingObservation,
    ConditionGeneratedEmptyFlowProjection,
    ConditionGeneratedEmptyFlowVisibility,
    ConditionHoldersRevenue,
    ConditionIncrementalVolumes,
    ConditionInterval,
    ConditionLiquidations,
    ConditionLiquidity,
    ConditionLiquidityVenue,
    ConditionNativeVolumeSupport,
    ConditionNoPositiveSupplyEvidence,
    ConditionObservedFieldFailure,
    ConditionObservedFundingDispersion,
    ConditionObservedIncrementalVolume,
    ConditionObservedPredicate,
    ConditionOpenInterest,
    ConditionPositiveSupplyEvidence,
    ConditionPreDepthWindow,
    ConditionPredicate,
    ConditionReceiptVisibility,
    ConditionReferenceTradeEvidenceRef,
    ConditionReferenceTradeProjection,
    ConditionRelativeGraph,
    ConditionRelativeNormalRange,
    ConditionRelativeWindow,
    ConditionRequestedDerivativeView,
    ConditionScheduledUnlock,
    ConditionSlippage,
    ConditionSpotValues,
    ConditionSupplyGraph,
    ConditionSynchronizedOpenInterest,
    ConditionWalletFlow,
    ConditionWalletFlowEvidenceRef,
    ConditionWireClocks,
    DeclaredConditionCadence,
    DeclaredConditionDirection,
    EnrichedConditionDerivatives,
    EnrichedConditionDerivativeVenue,
    EnrichedConditionLiquidity,
    EnrichedConditionSpot,
    FlowAggregateEvidenceRef,
    FlowCaptureEvidenceRef,
    FullConditionMarketState,
    KnownConditionTradeCount,
    MetricSemanticsCadenceOrigin,
    MissingConditionProofObservation,
    NonForwardConditionMarketState,
    ObservedConditionCohort,
    ObservedConditionCohortComparison,
    ObservedConditionDepthCohort,
    ObservedConditionMarketContext,
    ObservedConditionReferencePrice,
    ObservedConditionSizeProfile,
    ObservedConditionVolatility,
    SelectedConditionStablecoinPoint,
    SelectiveConditionConfidence,
    UnavailableConditionCirculatingShare,
    UnavailableConditionLeadLag,
    UnavailableConditionOpenInterest,
    UnavailableConditionSizeProfile,
    UnavailableConditionSupplyObservation,
    UnrequestedConditionFunding,
    UnrequestedConditionOpenInterest,
    WindowedConditionSpot,
    sum_condition_native,
)
from tank.analysis.spot_enrichment import CohortEnrichedConditionSpot
from tank.analysis.statistics_contracts import (
    DecimalRange,
    FloatRange,
    HistoryDiagnostic,
    UnavailableBeta,
    UnavailableHistoryDiagnostics,
)
from tank.contracts import DomainModel
from tank.contracts.json import JsonDocument
from tank.time_precision import Timestamp, offset_schedule
from tank.util import iso, parse_iso_time, sha256


def _checked_projection(value: ConditionFlowAggregateProjection) -> ConditionFlowAggregateProjection:
    if type(value) not in (ConditionFlowAggregateProjection, ConditionFlowAggregate):
        raise TypeError("A declared flow aggregate projection is required")
    validated = type(value).model_validate(value)
    return ConditionFlowAggregateProjection(
        token_id=validated.token_id,
        venue=validated.venue,
        market_type=validated.market_type,
        instrument_id=validated.instrument_id,
        start=validated.start,
        end=validated.end,
        complete=validated.complete,
        buy_qty=validated.buy_qty,
        sell_qty=validated.sell_qty,
        sum_sq_qty=validated.sum_sq_qty,
        buy_notional=validated.buy_notional,
        sell_notional=validated.sell_notional,
        notional_reason=validated.notional_reason,
        low=validated.low,
        low_reason=validated.low_reason,
        counts=validated.counts,
    )


def flow_aggregate_projection_sha256(value: ConditionFlowAggregateProjection) -> str:
    projection = _checked_projection(value)
    serialized = JsonDocument.model_validate(projection.model_dump(mode="json"), strict=True).root
    envelope: dict[str, JsonValue] = {"schema": "tank.condition.flow-aggregate.v1", "projection": serialized}
    return sha256(envelope)


def validate_flow_aggregate_evidence(value: ConditionFlowAggregate) -> None:
    if type(value) is not ConditionFlowAggregate:
        raise TypeError("A bound flow aggregate is required")
    validated = ConditionFlowAggregate.model_validate(value)
    expected = flow_aggregate_projection_sha256(validated)
    if validated.evidence.projection_sha256 != expected:
        raise ValueError("Flow aggregate evidence does not match its declared projection")


def bind_flow_aggregate_evidence(
    value: ConditionFlowAggregateProjection, reference: FlowAggregateEvidenceRef
) -> ConditionFlowAggregate:
    projection = _checked_projection(value)
    if isinstance(value, ConditionFlowAggregate):
        validate_flow_aggregate_evidence(value)
    digest = flow_aggregate_projection_sha256(projection)
    validated_reference = FlowAggregateEvidenceRef.model_validate(reference)
    if validated_reference.projection_sha256 != digest:
        raise ValueError("Flow aggregate evidence does not match its declared projection")
    return ConditionFlowAggregate(
        token_id=projection.token_id,
        venue=projection.venue,
        market_type=projection.market_type,
        instrument_id=projection.instrument_id,
        start=projection.start,
        end=projection.end,
        complete=projection.complete,
        buy_qty=projection.buy_qty,
        sell_qty=projection.sell_qty,
        sum_sq_qty=projection.sum_sq_qty,
        buy_notional=projection.buy_notional,
        sell_notional=projection.sell_notional,
        notional_reason=projection.notional_reason,
        low=projection.low,
        low_reason=projection.low_reason,
        counts=projection.counts,
        evidence=validated_reference,
    )


class ConditionFlowEvidenceLookup(Protocol):
    """Resolve immutable effective wire evidence after reprojecting and binding the expected data.

    The read adapter preserves original evidence separately, refuses ambiguous projection
    identities, and never substitutes expected financial values for missing source fields.
    """

    def resolve_flow_aggregate(
        self, reference: FlowAggregateEvidenceRef, *, expected: ConditionFlowAggregateProjection
    ) -> Mapping[str, JsonValue]: ...


def flow_aggregate_evidence_wire(
    value: ConditionFlowAggregate, evidence: ConditionFlowEvidenceLookup
) -> dict[str, JsonValue]:
    validate_flow_aggregate_evidence(value)
    projection = _checked_projection(value)
    resolved = evidence.resolve_flow_aggregate(value.evidence, expected=projection)
    checked = JsonDocument.validate_value(dict(resolved))
    if not isinstance(checked, dict):
        raise ValueError("Flow aggregate proof serialization requires an object")
    return deepcopy(checked)


def _checked_capture_projection(value: ConditionFlowCaptureProjection) -> ConditionFlowCaptureProjection:
    if type(value) not in (
        ConditionFlowCaptureProjection,
        ConditionFlowCaptureWitness,
        ConditionFlowAggregateProjection,
        ConditionEvaluationProof,
        ConditionFlowAggregate,
    ):
        raise TypeError("A declared flow capture projection is required")
    validated = type(value).model_validate(value)
    if isinstance(validated, ConditionFlowAggregate):
        validate_flow_aggregate_evidence(validated)
    return ConditionFlowCaptureProjection(
        token_id=validated.token_id,
        venue=validated.venue,
        market_type=validated.market_type,
        instrument_id=validated.instrument_id,
        start=validated.start,
        end=validated.end,
        complete=validated.complete,
    )


def flow_capture_projection_sha256(value: ConditionFlowCaptureProjection) -> str:
    projection = _checked_capture_projection(value)
    serialized = JsonDocument.model_validate(projection.model_dump(mode="json"), strict=True).root
    envelope: dict[str, JsonValue] = {"schema": "tank.condition.flow-capture.v1", "projection": serialized}
    return sha256(envelope)


def validate_flow_capture_evidence(value: ConditionFlowCaptureWitness) -> None:
    if type(value) is not ConditionFlowCaptureWitness:
        raise TypeError("A bound flow capture witness is required")
    validated = ConditionFlowCaptureWitness.model_validate(value)
    if validated.evidence.projection_sha256 != flow_capture_projection_sha256(validated):
        raise ValueError("Flow capture evidence does not match its declared projection")


def bind_flow_capture_evidence(
    value: ConditionFlowCaptureProjection, reference: FlowCaptureEvidenceRef
) -> ConditionFlowCaptureWitness:
    projection = _checked_capture_projection(value)
    if isinstance(value, ConditionFlowCaptureWitness):
        validate_flow_capture_evidence(value)
    checked_reference = FlowCaptureEvidenceRef.model_validate(reference)
    if checked_reference.projection_sha256 != flow_capture_projection_sha256(projection):
        raise ValueError("Flow capture evidence does not match its declared projection")
    return ConditionFlowCaptureWitness(
        token_id=projection.token_id,
        venue=projection.venue,
        market_type=projection.market_type,
        instrument_id=projection.instrument_id,
        start=projection.start,
        end=projection.end,
        complete=projection.complete,
        evidence=checked_reference,
    )


def _spot_values_wire(
    value: ConditionSpotValues, evidence: ConditionFlowEvidenceLookup
) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    result = JsonDocument.validate_value(
        validated.model_dump(
            mode="json",
            warnings="error",
            include={
                "venues",
                "composite_max",
                "breadth",
                "cw_breadth",
                "exact_majority",
                "topn",
                "concentration",
                "direction",
                "direction_reason",
                "classification",
                "coverage",
            },
        )
    )
    if not isinstance(result, dict):
        raise TypeError("A basic spot result must serialize as an object")
    venues: list[JsonValue] = []
    for venue in validated.venues:
        serialized_venue = JsonDocument.validate_value(
            venue.model_dump(mode="json", exclude={"boundary_segments"}, warnings="error")
        )
        if not isinstance(serialized_venue, dict):
            raise TypeError("A spot venue must serialize as an object")
        boundaries: list[JsonValue] = []
        for boundary in venue.boundary_segments:
            count = boundary.trade_count()
            serialized_boundary: dict[str, JsonValue] = {
                "start": iso(boundary.start),
                "cutoff": iso(boundary.cutoff),
                "trade_count": count.value,
                "aggregates": [
                    flow_aggregate_evidence_wire(aggregate, evidence) for aggregate in boundary.aggregates
                ],
            }
            if not isinstance(count, KnownConditionTradeCount):
                serialized_boundary["trade_count_reason"] = count.reason
            boundaries.append(serialized_boundary)
        serialized_venue["boundary_segments"] = boundaries
        venues.append(serialized_venue)
    result["venues"] = venues
    composite = JsonDocument.validate_value(
        validated.composite_max.model_dump(mode="json", exclude={"status"}, warnings="error")
    )
    result["composite_max"] = composite
    return result


def encode_basic_condition_spot(
    value: BasicConditionSpot, evidence: ConditionFlowEvidenceLookup
) -> dict[str, JsonValue]:
    validated = BasicConditionSpot.model_validate(value)
    result = _spot_values_wire(validated, evidence)
    result["composite_cohort"] = None
    result["cohort_consistent"] = {}
    result["size_profile"] = None
    return result


def encode_condition_reference_close(value: ConditionClose) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    result: dict[str, JsonValue] = {
        "ref_venue": validated.reference_instrument_id,
        "close_1h_price": str(validated.price) if validated.price is not None else None,
    }
    clock_fields: dict[str, JsonValue] = {
        name: value for name, value in validated.observed_at.fields("close_1h_observed_at").items()
    }
    clock = JsonDocument.validate_value(clock_fields)
    if not isinstance(clock, dict):
        raise TypeError("Reference close timestamp fields must serialize as an object")
    result.update(clock)
    return result


def _model_wire(value: DomainModel) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    result = JsonDocument.validate_value(validated.model_dump(mode="json", warnings="error"))
    if not isinstance(result, dict):
        raise TypeError("A declared domain object must serialize as an object")
    return result


def _clock_wire(value: Timestamp, prefix: str) -> dict[str, JsonValue]:
    validated = Timestamp.model_validate(value)
    return {name: scalar for name, scalar in validated.fields(prefix).items()}


def _decimal_wire(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _range_wire(value: DecimalRange | FloatRange | ConditionRelativeNormalRange) -> list[JsonValue] | None:
    validated = type(value).model_validate(value)
    bounds = validated.bounds
    if bounds is None:
        return None
    if isinstance(value, DecimalRange):
        return [str(bounds.lower), str(bounds.upper)]
    return [float(bounds.lower), float(bounds.upper)]


def _predicate_wire(value: ConditionPredicate) -> bool | None:
    validated = type(value).model_validate(value)
    return validated.value if isinstance(validated, ConditionObservedPredicate) else None


def encode_condition_open_interest(value: ConditionOpenInterest) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    if isinstance(validated, UnrequestedConditionOpenInterest):
        return {}
    if isinstance(validated, UnavailableConditionOpenInterest):
        return {
            "oi_class": None,
            "oi_native_start": None,
            "oi_native_end": None,
            "oi_native_chg_pct": None,
            "oi_usd_start": None,
            "oi_usd_end": None,
            "oi_usd_chg_pct": None,
            "reason": validated.reason,
            "detail": validated.detail,
        }
    result: dict[str, JsonValue] = {
        "requested_window_duration_s": validated.requested_window_duration_s,
        "observed_window_duration_s": validated.observed_window_duration_s,
        "precision": validated.end_observed_at.precision,
        "oi_native_start": str(validated.oi_native_start),
        "oi_native_end": str(validated.oi_native_end),
        "oi_native_chg_pct": validated.oi_native_chg_pct,
        "oi_usd_start": _decimal_wire(validated.oi_usd_start),
        "oi_usd_end": _decimal_wire(validated.oi_usd_end),
        "oi_usd_chg_pct": validated.oi_usd_chg_pct,
        "oi_usd_reason": validated.oi_usd_reason,
        "oi_usd_detail": validated.oi_usd_detail,
        "decomposition": _model_wire(validated.decomposition),
        "native_normal_range": _range_wire(validated.native_normal_range),
        "native_normal_range_reason": validated.native_normal_range.reason,
        "usd_normal_range": _range_wire(validated.usd_normal_range),
        "usd_normal_range_reason": validated.usd_normal_range.reason,
        "native_oi_vs_normal": validated.native_oi_vs_normal,
        "native_beyond": validated.native_beyond,
        "usd_beyond": validated.usd_beyond,
        "oi_class": validated.oi_class,
        "classification_ready": validated.classification_ready,
    }
    result.update(_clock_wire(validated.start_observed_at, "start_observed_at"))
    result.update(_clock_wire(validated.end_observed_at, "end_observed_at"))
    return result


def _funding_observed_clock(
    observation: ConditionFundingObservation | None, prefix: str
) -> dict[str, JsonValue]:
    return {prefix: None} if observation is None else _clock_wire(observation.observed_at, prefix)


def encode_condition_funding(value: ConditionFunding) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    if isinstance(validated, UnrequestedConditionFunding):
        return {}
    settled, estimate, start = validated.settled, validated.estimate, validated.start
    selected: ConditionFundingObservation | None = None
    if isinstance(validated, AvailableConditionFunding):
        selected = estimate if validated.selected_kind == "CURRENT_ESTIMATE" else settled
        if selected is None:
            raise ValueError("Observed funding requires its actual selected observation")
    result: dict[str, JsonValue] = {
        "funding_settled_last": None if settled is None else str(settled.rate),
        "funding_settled_interval_h": None if settled is None else _decimal_wire(settled.interval_h),
        "funding_settled_8h_eq": None
        if settled is None
        else _decimal_wire(settled.equivalent_eight_hour_rate),
        "funding_estimate": None if estimate is None else str(estimate.rate),
        "funding_estimate_interval_h": None if estimate is None else _decimal_wire(estimate.interval_h),
        "funding_estimate_8h_eq": None
        if estimate is None
        else _decimal_wire(estimate.equivalent_eight_hour_rate),
        "funding_start": None if start is None else str(start.rate),
        "funding_start_interval_h": None if start is None else _decimal_wire(start.interval_h),
        "interval_h": None if selected is None else _decimal_wire(selected.interval_h),
        "funding_8h_eq": None if selected is None else _decimal_wire(selected.equivalent_eight_hour_rate),
        "funding_8h_kind": validated.selected_kind
        if isinstance(validated, AvailableConditionFunding)
        else None,
        "funding_vs_normal": validated.funding_vs_normal
        if isinstance(validated, AvailableConditionFunding)
        else None,
        "funding_normal_range_8h": _range_wire(validated.normal_range),
        "funding_normal_range_reason": validated.normal_range.reason,
        "funding_reason": None if isinstance(validated, AvailableConditionFunding) else validated.reason,
    }
    result.update(_funding_observed_clock(settled, "funding_settled_last_observed_at"))
    result.update(_funding_observed_clock(estimate, "funding_estimate_observed_at"))
    result.update(_funding_observed_clock(start, "funding_start_observed_at"))
    return result


def _liquidations_wire(value: ConditionLiquidations) -> dict[str, JsonValue]:
    validated = ConditionLiquidations.model_validate(value)
    result: dict[str, JsonValue] = {
        "liq_coverage": validated.coverage,
        "liq_source_coverage": validated.source_coverage,
        "liq_source_origin": _model_wire(validated.source_origin),
    }
    for name, side in (("long", validated.long), ("short", validated.short)):
        result["liq_" + name + "_count"] = side.observed_record_count
        result["liq_" + name + "_native"] = _decimal_wire(side.native)
        result["liq_" + name + "_usd"] = _decimal_wire(side.usd)
        result["liq_" + name + "_usd_reason"] = side.usd_reason
    window: dict[str, JsonValue] = {
        "complete": validated.capture_window.complete,
        "reason": validated.capture_window.reason,
    }
    window.update(_clock_wire(validated.capture_window.start, "start"))
    window.update(_clock_wire(validated.capture_window.observed_through, "observed_through"))
    result["liq_capture_window"] = window
    return result


def _derivative_venue_wire(value: ConditionDerivativeVenue) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    result: dict[str, JsonValue] = {
        "venue": validated.venue,
        "instrument_id": validated.instrument_id,
        "coverage": {"venue": validated.venue},
        "basis_bps": _decimal_wire(validated.basis_bps),
        "basis_reason": validated.basis_reason,
    }
    result.update(encode_condition_open_interest(validated.open_interest))
    result.update(encode_condition_funding(validated.funding))
    if isinstance(validated.liquidations, ConditionLiquidations):
        result.update(_liquidations_wire(validated.liquidations))
    exclusions = validated.oi_temporal_exclusions
    if exclusions is not None:
        observation: dict[str, JsonValue] = {
            "oi_native": _decimal_wire(exclusions.latest_observation.oi_native),
            "oi_usd": _decimal_wire(exclusions.latest_observation.oi_usd),
            "scope": exclusions.latest_observation.scope,
        }
        observation.update(_clock_wire(exclusions.latest_observation.observed_at, "ts"))
        result["oi_temporal_exclusions"] = {
            "count": exclusions.count,
            "reason": exclusions.reason,
            "detail": exclusions.detail,
            "latest_observation": observation,
        }
    if isinstance(validated, EnrichedConditionDerivativeVenue):
        support = validated.support
        result.update(
            {
                "perp_taker_buy_share": support.perp_taker_buy_share,
                "perp_taker_buy_share_reason": support.perp_taker_buy_share_reason,
                "perp_volume_usd": _decimal_wire(support.perp_volume_usd),
                "perp_volume_reason": support.perp_volume_reason,
                "basis_normal_range": _range_wire(support.basis_normal_range),
                "basis_normal_range_reason": support.basis_normal_range.reason,
                "basis_vs_normal": support.basis_vs_normal,
                "basis_widening": _predicate_wire(support.basis_widening),
                "funding_rising": _predicate_wire(support.funding_rising),
            }
        )
    return result


def _native_volume_wire(value: ConditionNativeVolumeSupport) -> dict[str, JsonValue]:
    validated = ConditionNativeVolumeSupport.model_validate(value)
    return {
        "value": _decimal_wire(validated.value),
        "previous": _decimal_wire(validated.previous),
        "normal_range": _range_wire(validated.normal_range),
        "unit": validated.unit,
        "window": validated.window,
        "reason": validated.reason,
    }


def _derivative_support_wire(value: ConditionDerivativeSupportFeatures) -> dict[str, JsonValue]:
    validated = ConditionDerivativeSupportFeatures.model_validate(value)
    predicates: tuple[tuple[str, ConditionPredicate], ...] = (
        ("spot_volume_up_beyond_normal", validated.spot_volume_up_beyond_normal),
        ("spot_volume_within_normal", validated.spot_volume_within_normal),
        ("perp_volume_up_beyond_normal", validated.perp_volume_up_beyond_normal),
        ("perp_volume_within_normal", validated.perp_volume_within_normal),
        ("perp_taker_flow_buy", validated.perp_taker_flow_buy),
        ("basis_within_normal", validated.basis_within_normal),
        ("basis_widening", validated.basis_widening),
        ("funding_rising", validated.funding_rising),
        ("liquidations_first_half_majority", validated.liquidations_first_half_majority),
        ("large_ticket_buy_majority", validated.large_ticket_buy_majority),
        ("large_tickets_minority", validated.large_tickets_minority),
        ("large_ticket_sell_share_rising", validated.large_ticket_sell_share_rising),
        ("exchange_inflows", validated.exchange_inflows),
        ("localized_inventory_transfer", validated.localized_inventory_transfer),
        ("ask_depth_building", validated.ask_depth_building),
        ("ask_replenishment", validated.ask_replenishment),
    )
    result: dict[str, JsonValue] = {name: _predicate_wire(predicate) for name, predicate in predicates}
    reasons: dict[str, JsonValue] = {
        name: predicate.reason
        for name, predicate in predicates
        if not isinstance(predicate, ConditionObservedPredicate)
    }
    result["null_reasons"] = reasons
    result["spot_volume"] = _native_volume_wire(validated.spot_volume)
    result["perp_volume"] = _native_volume_wire(validated.perp_volume)
    return result


def encode_condition_derivatives(value: ConditionDerivatives) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    return _derivative_window_wire(
        validated, [_derivative_venue_wire(venue) for venue in validated.per_venue]
    )


def _derivative_window_wire(
    validated: ConditionDerivatives, per_venue: list[JsonValue]
) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {"per_venue": per_venue}
    synchronized = validated.synchronized
    if isinstance(synchronized, ConditionSynchronizedOpenInterest):
        sync = encode_condition_open_interest(synchronized.values)
        sync["venues"] = list(synchronized.venues)
        result["synchronized"] = sync
    else:
        result["synchronized"] = None
    result["synchronized_status"] = synchronized.status
    dispersion = validated.funding_dispersion
    result["funding_dispersion_8h"] = (
        str(dispersion.value) if isinstance(dispersion, ConditionObservedFundingDispersion) else None
    )
    result["funding_dispersion_status"] = dispersion.status
    if dispersion.status == "UNAVAILABLE":
        result["funding_dispersion_reason"] = dispersion.reason
    if isinstance(validated.liquidations, ConditionAggregateLiquidations):
        totals: dict[str, JsonValue] = {}
        for name, sides in (
            ("complete_venues_total", validated.liquidations.complete_venues_total),
            ("sampled_venues_min", validated.liquidations.sampled_venues_min),
        ):
            totals[name] = {"long": _decimal_wire(sides.long), "short": _decimal_wire(sides.short)}
            totals[name + "_reasons"] = {"long": sides.long_reason, "short": sides.short_reason}
        result["liquidations"] = totals
    if isinstance(validated.incremental_volume, ConditionIncrementalVolumes):
        incremental = validated.incremental_volume
        result["incremental_volume_coverage"] = {
            "spot": _model_wire(incremental.spot_coverage),
            "perp": _model_wire(incremental.perp_coverage),
        }
        values = incremental.values
        result["incremental_volume_usd"] = {
            "spot": _decimal_wire(values.spot_usd),
            "perp": _decimal_wire(values.perp_usd),
        }
        result["incremental_share"] = (
            {
                "spot": values.spot_share,
                "perp": values.perp_share,
                "increment_usd": {"spot": str(values.spot_usd), "perp": str(values.perp_usd)},
            }
            if isinstance(values, ConditionObservedIncrementalVolume)
            else None
        )
        if not isinstance(values, ConditionObservedIncrementalVolume):
            result["incremental_share_reason"] = values.reason
    if isinstance(validated, EnrichedConditionDerivatives):
        result["funding_dispersion_normal_range_8h"] = _range_wire(validated.funding_dispersion_normal_range)
        result["funding_dispersion_beyond_normal"] = _predicate_wire(
            validated.funding_dispersion_beyond_normal
        )
        lead = validated.lead_lag
        sequence: list[JsonValue] = []
        for point in lead.sequence:
            item: dict[str, JsonValue] = {"venue": point.venue, "market": point.market}
            item.update(_clock_wire(point.first_observed_at, "t_first"))
            sequence.append(item)
        lead_wire: dict[str, JsonValue] = {
            "window": lead.interval.window,
            "start": iso(lead.interval.start),
            "cutoff": iso(lead.interval.cutoff),
            "first_mover": lead.first_mover,
            "spot_confirmed": _predicate_wire(lead.spot_confirmation),
            "sequence": sequence,
            "evaluated_markets": list(lead.evaluated_markets),
        }
        if isinstance(lead, UnavailableConditionLeadLag):
            lead_wire["reason"] = lead.reason
        result["lead_lag"] = lead_wire
        result["support_features"] = _derivative_support_wire(validated.support_features)
    else:
        result["lead_lag"] = {
            "first_mover": "UNKNOWN",
            "spot_confirmed": None,
            "sequence": [],
            "status": "NOT_REQUESTED",
        }
    return result


def encode_scoped_condition_spot(
    value: WindowedConditionSpot | CohortEnrichedConditionSpot | EnrichedConditionSpot,
    evidence: ConditionFlowEvidenceLookup,
    *,
    clocks: ConditionWireClocks,
) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    clock_context = ConditionWireClocks.model_validate(clocks)
    if validated.token_id != clock_context.token_id:
        raise ValueError("Spot serialization cannot borrow another subject's clock context")
    bounds = clock_context.bounds(validated.interval)
    result = _spot_values_wire(validated, evidence)
    result.update(_clock_wire(bounds.start, "start"))
    result.update(_clock_wire(bounds.cutoff, "cutoff"))
    result["start_origin"] = bounds.start_origin
    result["window"] = validated.interval.window
    result.update(_clock_wire(validated.coverage_through, "observed_at"))
    result["precision"] = validated.coverage_through.precision
    cohort = validated.composite_cohort
    if isinstance(cohort, ObservedConditionCohort):
        cohort_wire = _model_wire(cohort)
        del cohort_wire["status"]
        cohort_wire["reason"] = None
        result["composite_cohort"] = cohort_wire
    else:
        result["composite_cohort"] = None
        result["composite_cohort_reason"] = cohort.reason
    result["cohort_consistent"] = {
        name: consistent for name, consistent in validated.cohort_consistent.items()
    }
    if isinstance(validated, (CohortEnrichedConditionSpot, EnrichedConditionSpot)):
        result["enrichment_phase"] = (
            "COHORT_COMPARISONS" if isinstance(validated, CohortEnrichedConditionSpot) else "FULL_ENRICHMENT"
        )
        comparisons: dict[str, JsonValue] = {}
        for name, comparison in validated.cohort_comparisons.items():
            if isinstance(comparison, ObservedConditionCohortComparison):
                current = _model_wire(comparison.current)
                other = _model_wire(comparison.other)
                del current["status"]
                del other["status"]
                comparisons[name] = {
                    "cohort": list(comparison.cohort),
                    "current": current,
                    "other": other,
                    "share_of_max": comparison.share_of_max,
                    "share_majority": comparison.share_majority,
                    "reason": None,
                }
            else:
                comparisons[name] = {
                    "cohort": list(comparison.cohort),
                    "current": None,
                    "other": None,
                    "share_of_max": comparison.share_of_max,
                    "share_majority": comparison.share_majority,
                    "reason": comparison.reason,
                }
        result["cohort_comparisons"] = comparisons
        profile = validated.size_profile
        result["size_profile_status"] = profile.status
        if isinstance(profile, ObservedConditionSizeProfile):
            profile_wire = _model_wire(profile)
            del profile_wire["status"]
            result["size_profile"] = profile_wire
        else:
            result["size_profile"] = None
            if isinstance(profile, UnavailableConditionSizeProfile):
                result["size_profile_reason"] = profile.reason
    else:
        result["size_profile"] = None
        result["size_profile_status"] = "NOT_REQUESTED"
    return result


def reference_trade_projection_sha256(value: ConditionReferenceTradeProjection) -> str:
    if type(value) is not ConditionReferenceTradeProjection:
        raise TypeError("A declared reference trade projection is required")
    projection = ConditionReferenceTradeProjection.model_validate(value)
    serialized: JsonValue = _model_wire(projection)
    envelope: dict[str, JsonValue] = {
        "schema": "tank.condition.reference-trade.v1",
        "projection": serialized,
    }
    return sha256(envelope)


class ConditionReferenceTradeLookup(Protocol):
    """Resolve original/effective storage-read trade evidence with qualified SPOT identity.

    The adapter verifies its source descriptor and reprojects the expected token,
    instrument, market, price and actual clock. Canonical storage-read projections
    must not be described as original provider HTTP bytes.
    """

    def resolve_reference_trade(
        self, reference: ConditionReferenceTradeEvidenceRef, *, expected: ConditionReferenceTradeProjection
    ) -> Mapping[str, JsonValue]: ...


class ConditionRelativeEvidenceLookup(ConditionFlowEvidenceLookup, ConditionReferenceTradeLookup, Protocol):
    pass


def reference_trade_evidence_wire(
    value: ConditionReferenceTradeProjection,
    reference: ConditionReferenceTradeEvidenceRef,
    evidence: ConditionReferenceTradeLookup,
) -> dict[str, JsonValue]:
    digest = reference_trade_projection_sha256(value)
    projection = ConditionReferenceTradeProjection.model_validate(value)
    validated = ConditionReferenceTradeEvidenceRef.model_validate(reference)
    if validated.projection_sha256 != digest:
        raise ValueError("Reference trade evidence does not match its declared projection")
    resolved = evidence.resolve_reference_trade(validated, expected=projection)
    checked = JsonDocument.validate_value(dict(resolved))
    if not isinstance(checked, dict):
        raise TypeError("Reference trade evidence must serialize as an object")
    return deepcopy(checked)


def _boundary_wire(
    value: ConditionBoundaryGroup, evidence: ConditionFlowEvidenceLookup
) -> dict[str, JsonValue]:
    boundary = ConditionBoundaryGroup.model_validate(value)
    count = boundary.trade_count()
    result: dict[str, JsonValue] = {
        "start": iso(boundary.start),
        "cutoff": iso(boundary.cutoff),
        "trade_count": count.value,
        "aggregates": [
            flow_aggregate_evidence_wire(aggregate, evidence) for aggregate in boundary.aggregates
        ],
    }
    if not isinstance(count, KnownConditionTradeCount):
        result["trade_count_reason"] = count.reason
    return result


def _history_wire(value: HistoryDiagnostic) -> dict[str, JsonValue]:
    validated = type(value).model_validate(value)
    if isinstance(validated, UnavailableHistoryDiagnostics):
        return {
            "exact": validated.exact,
            "reason": validated.reason,
            "target_window_duration_s": validated.target_window_duration_s,
            "resolution_s": validated.resolution_s,
            "sample_count": validated.sample_count,
        }
    duration, staleness = validated.observed_duration_bounds_s, validated.endpoint_staleness_bounds_s
    return {
        "target_window_duration_s": validated.target_window_duration_s,
        "observed_duration_bounds_s": None if duration is None else [duration.lower, duration.upper],
        "endpoint_staleness_bounds_s": None if staleness is None else [staleness.lower, staleness.upper],
        "exact": validated.exact,
        "resolution_s": validated.resolution_s,
        "alignment": validated.alignment,
        "sample_count": validated.sample_count,
        "latest_observed_at": None
        if validated.latest_observed_at is None
        else iso(validated.latest_observed_at),
    }


def _relative_window_wire(
    value: ConditionRelativeWindow, root: ConditionRelativeGraph, clocks: ConditionWireClocks
) -> dict[str, JsonValue]:
    window = ConditionRelativeWindow.model_validate(value)
    bounds = clocks.bounds(window.interval)
    reference = root.reference_instrument_id
    covered = False if reference is None else root.reference_coverage[reference].complete
    result: dict[str, JsonValue] = {
        "window": window.interval.window,
        "start_origin": bounds.start_origin,
        "precision": window.observed_at.precision,
        "coverage": {"reference_venue": reference, "complete_24h": covered},
        "r_raw": window.raw.value,
        "r_raw_reason": window.raw.reason,
        "r_btc_adj": window.btc_adjusted.value,
        "r_btc_adj_reason": window.btc_adjusted.reason,
        "r_eth_adj": window.eth_adjusted.value,
        "r_eth_adj_reason": window.eth_adjusted.reason,
        "r_sector_adj": window.sector_adjusted.value,
        "sector_reason": window.sector_adjusted.reason,
        "relative_measure": window.measure,
        "relative_return": window.relative_return(),
        "btc_reference_freshness": _model_wire(window.btc_reference.freshness()),
        "eth_reference_freshness": _model_wire(window.eth_reference.freshness()),
        "pctile": window.percentile,
        "pctile_reason": window.percentile_reason,
        "normal_range": _range_wire(window.normal_range),
        "normal_range_reason": window.normal_range.reason,
        "beyond_normal": window.beyond_normal(),
        "historical_window_resolution_s": window.history.resolution_s,
        "historical_window_duration_s": window.history.target_window_duration_s,
        "requested_window_duration_s": (window.interval.cutoff - window.interval.start).total_seconds(),
        "historical_alignment": "AS_OF_AT_EXACT_TARGET_BOUNDS",
        "historical_measurement": _history_wire(window.history),
    }
    result.update(_clock_wire(bounds.start, "start"))
    result.update(_clock_wire(bounds.cutoff, "cutoff"))
    result.update(_clock_wire(window.observed_at, "observed_at"))
    return result


def encode_condition_relative(
    value: ConditionRelativeGraph, evidence: ConditionRelativeEvidenceLookup, *, clocks: ConditionWireClocks
) -> dict[str, JsonValue]:
    validated = ConditionRelativeGraph.model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if validated.token_id != context.token_id:
        raise ValueError("Relative serialization cannot borrow another subject's clock context")
    event_window = validated.windows["EVENT"]
    for window in validated.windows.values():
        context.bounds(window.interval)
    anchor = validated.anchor
    anchor_type: Literal["T0", "T1", "KNOWN_TO_US_AT", "ANALYSIS_WINDOW_START"]
    if isinstance(anchor.origin, ConditionDeclaredEventAnchor):
        if context.event_anchor != anchor.origin.at:
            raise ValueError("Relative event anchor metadata must preserve its captured clock")
        anchor_clock = anchor.origin.at
        anchor_type = anchor.origin.source
        anchor_origin = "CAPTURED_EVENT_ANCHOR"
    else:
        if context.event_anchor.value is not None:
            raise ValueError("An analysis-window start cannot be labeled as a captured event anchor")
        if context.cutoff.value is None:
            raise ValueError("Relative serialization requires its actual cutoff")
        anchor_clock = offset_schedule(context.cutoff, anchor.origin.at - event_window.interval.cutoff)
        anchor_type = "ANALYSIS_WINDOW_START"
        anchor_origin = "CUTOFF_OFFSET"
    coverage: dict[str, JsonValue] = {}
    for iid, item in validated.reference_coverage.items():
        coverage[iid] = {
            "complete": item.complete,
            "expected_duration_s": item.expected_duration_s,
            "boundary_segments": [_boundary_wire(group, evidence) for group in item.boundary_segments],
            "excluded": [_model_wire(exclusion) for exclusion in item.excluded],
            "volume_unit": item.volume_unit,
            "covered_volume_native": _decimal_wire(item.covered_volume_native),
        }
    anchor_wire: dict[str, JsonValue] = {
        "type": anchor_type,
        "start_origin": anchor_origin,
        "ref_price": _decimal_wire(anchor.price.value),
        "price_source": anchor.price.source,
    }
    anchor_wire.update(_clock_wire(anchor_clock, "ts"))
    anchor_wire.update(_clock_wire(anchor.price.observed_at, "price_observed_at"))
    if isinstance(anchor.price, ObservedConditionReferencePrice):
        reference = anchor.trade_evidence_ref
        if reference is None or anchor.price.source != "LAST_TRADE":
            raise ValueError("An observed anchor requires its actual reference trade evidence")
        projection = ConditionReferenceTradeProjection(
            token_id=validated.token_id,
            instrument_id=anchor.price.instrument_id,
            market_type="SPOT",
            source=anchor.price.source,
            price=anchor.price.value,
            observed_at=anchor.price.observed_at,
        )
        anchor_wire["evidence"] = reference_trade_evidence_wire(projection, reference, evidence)
        anchor_wire["price_reason"] = None
    else:
        anchor_wire["evidence"] = None
        anchor_wire["price_reason"] = anchor.price.reason
    declaration = validated.direction_declaration
    beta_btc, beta_eth = event_window.btc_beta, event_window.eth_beta
    repricing = validated.repricing.band()
    market = validated.context.classification()
    inputs = validated.context
    context_wire: dict[str, JsonValue] = {
        "btc_return_24h": inputs.btc_return_24h,
        "rv24": inputs.rv24,
        "rv24_bound": iso(inputs.rv24_bound),
        "rv24_resolution_s": inputs.rv24_resolution_s,
        "return_normal_range": _range_wire(inputs.return_normal_range),
        "return_normal_range_reason": inputs.return_normal_range.reason,
        "return_lower_quartile": inputs.return_lower_quartile,
        "rv_normal_range": _range_wire(inputs.rv_normal_range),
        "rv_normal_range_reason": inputs.rv_normal_range.reason,
        "rule": inputs.rule,
    }
    context_wire.update(_clock_wire(inputs.rv24_observed_at, "rv24_observed_cutoff"))
    runup = validated.runup
    runup_wire: dict[str, JsonValue] = {
        "window": runup.window,
        "anchor_kind": anchor_type,
        "anchor_origin": anchor_origin,
        "start_origin": "CUTOFF_OFFSET",
        "ar": runup.value.value,
        "ar_reason": runup.value.reason,
        "pctile": runup.percentile,
        "pctile_reason": runup.percentile_reason,
        "flag": _predicate_wire(runup.beyond_normal),
    }
    runup_wire.update(_clock_wire(offset_schedule(anchor_clock, runup.start - runup.cutoff), "start"))
    runup_wire.update(_clock_wire(anchor_clock, "cutoff"))
    if not isinstance(runup.beyond_normal, ConditionObservedPredicate):
        runup_wire["flag_reason"] = runup.beyond_normal.reason
    outcome_reference = validated.outcome_btc_reference
    btc_wire: dict[str, JsonValue] = {
        "instrument_id": outcome_reference.instrument_id,
        "entry_ref_price": _decimal_wire(outcome_reference.value),
        "price_source": outcome_reference.source,
        "reason": None
        if isinstance(outcome_reference, ObservedConditionReferencePrice)
        else outcome_reference.reason,
    }
    btc_wire.update(_clock_wire(outcome_reference.observed_at, "observed_at"))
    ranges: dict[str, JsonValue] = {}
    if isinstance(validated.outcome_ranges, ConditionComputedOutcomeRanges):
        for name, baseline in validated.outcome_ranges.by_horizon.items():
            serialized_range: dict[str, JsonValue] = {
                "value": _range_wire(baseline.normal_range),
                "unit": baseline.unit,
                "relative_measure": baseline.measure,
                "window": baseline.window,
                "precision": baseline.observed_at.precision,
                "coverage": {"sample_count": baseline.history.sample_count},
                "reason": baseline.normal_range.reason,
                "historical_measurement": _history_wire(baseline.history),
            }
            serialized_range.update(
                _clock_wire(offset_schedule(anchor_clock, baseline.start - baseline.cutoff), "start")
            )
            serialized_range.update(_clock_wire(anchor_clock, "cutoff"))
            serialized_range.update(_clock_wire(baseline.observed_at, "observed_at"))
            ranges[name] = serialized_range
    result: dict[str, JsonValue] = {
        "expected_direction": declaration.value,
        "repricing_sign": (1 if declaration.normalized() == "BUY" else -1)
        if isinstance(declaration, DeclaredConditionDirection)
        else None,
        "outcome_btc_reference": btc_wire,
        "normal_ranges": ranges,
        "normal_ranges_status": validated.outcome_ranges.status,
        "entry_ref_price": _decimal_wire(validated.entry.value),
        "entry_price_source": validated.entry.source,
        "ref_venue": validated.reference_instrument_id,
        "reference_coverage": coverage,
        "reference_reason": validated.reference_reason,
        "anchor": anchor_wire,
        "windows": {
            name: _relative_window_wire(window, validated, context)
            for name, window in validated.windows.items()
        },
        "beta_btc": beta_btc.value,
        "beta_eth": beta_eth.value,
        "beta_flags": {
            "btc": beta_btc.reason if isinstance(beta_btc, UnavailableBeta) else None,
            "eth": beta_eth.reason if isinstance(beta_eth, UnavailableBeta) else None,
        },
        "beta_estimates": {"btc": _model_wire(beta_btc), "eth": _model_wire(beta_eth)},
        "pre_event_runup": runup_wire,
        "repricing_band": repricing.value,
        "repricing_reason": None if repricing.status == "OBSERVED" else repricing.reason,
        "market_context": market.value,
        "market_context_reason": None
        if isinstance(market, ObservedConditionMarketContext)
        else market.reason,
        "context_inputs": context_wire,
    }
    result.update(_clock_wire(validated.entry.observed_at, "entry_price_observed_at"))
    result.update(encode_condition_reference_close(validated.close_1h))
    return result


def generated_empty_flow_projection_sha256(value: ConditionGeneratedEmptyFlowProjection) -> str:
    if type(value) not in (ConditionGeneratedEmptyFlowProjection, ConditionGeneratedEmptyFlowVisibility):
        raise TypeError("A declared generated empty flow projection is required")
    checked = type(value).model_validate(value)
    projection = ConditionGeneratedEmptyFlowProjection(
        token_id=checked.token_id,
        venue=checked.venue,
        market_type=checked.market_type,
        segment=checked.segment,
    )
    serialized = JsonDocument.model_validate(projection.model_dump(mode="json"), strict=True).root
    return sha256({"schema": "tank.condition.generated-empty-flow.v1", "projection": serialized})


def validate_generated_empty_flow_evidence(value: ConditionGeneratedEmptyFlowVisibility) -> None:
    if type(value) is not ConditionGeneratedEmptyFlowVisibility:
        raise TypeError("A bound generated empty flow visibility is required")
    validated = ConditionGeneratedEmptyFlowVisibility.model_validate(value)
    if validated.evidence.projection_sha256 != generated_empty_flow_projection_sha256(validated):
        raise ValueError("Generated empty flow evidence does not match its declared projection")


def _cadence_wire(value: ConditionCadence) -> dict[str, JsonValue]:
    cadence = type(value).model_validate(value)
    known_label = isinstance(cadence.origin, MetricSemanticsCadenceOrigin)
    return {
        "cadence_s": cadence.seconds,
        "cadence_source": "SCOPED_METRIC_SEMANTICS" if known_label else None,
        "cadence_source_reason": None if known_label else "RULE_LABEL_NOT_CAPTURED",
        "cadence_origin": None if cadence.origin is None else _model_wire(cadence.origin),
        "cadence_scope": _model_wire(cadence.scope),
        "cadence_reason": None if isinstance(cadence, DeclaredConditionCadence) else cadence.reason,
    }


def _field_observation_wire(value: ConditionFieldObservation) -> dict[str, JsonValue]:
    observation = ConditionFieldObservation.model_validate(value)
    result: dict[str, JsonValue] = {
        "collection": observation.collection,
        "token_id": observation.token_id,
        "instrument_id": observation.instrument_id,
        "metric_key": observation.metric_key,
        "observation_kind": observation.observation_kind,
        "chain": observation.chain,
        "age_s": observation.age_s,
        "fresh": observation.fresh,
        "reason": observation.reason,
    }
    result.update(_clock_wire(observation.observed_at, "observed_at"))
    result.update(_cadence_wire(observation.cadence))
    if observation.stream is not None:
        result["basis"] = observation.stream.basis
        result["coverage_status"] = observation.stream.coverage_status
        result["detail"] = observation.stream.detail
    return result


def _confidence_failure_wire(value: ConditionConfidenceFailure) -> dict[str, JsonValue]:
    failure = type(value).model_validate(value)
    if isinstance(failure, ConditionCoverageFailure):
        return {"condition": failure.condition, "reason": failure.reason}
    if isinstance(failure, ConditionObservedFieldFailure):
        observed = _field_observation_wire(failure.observation)
        del observed["fresh"]
        observed["condition"] = failure.condition
        return observed
    result: dict[str, JsonValue] = {
        "condition": failure.condition,
        "reason": failure.reason,
        "token_id": failure.token_id,
        "collection": failure.collection,
        "instrument_id": failure.instrument_id,
        "metric_key": failure.metric_key,
        "metric_family": failure.metric_family,
    }
    result.update(_cadence_wire(failure.cadence))
    return result


def encode_condition_confidence(
    value: ConditionConfidence, *, clocks: ConditionWireClocks
) -> dict[str, JsonValue]:
    confidence = type(value).model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if confidence.subjects() - {context.token_id}:
        raise ValueError("Confidence serialization cannot borrow another subject's observations or failures")
    if context.cutoff.value is None:
        raise ValueError("Confidence serialization requires its actual cutoff")
    cutoff = parse_iso_time(context.cutoff.value)
    for observation in confidence.freshness:
        stamp = observation.observed_at.value
        if stamp is None or observation.age_s != (cutoff - parse_iso_time(stamp)).total_seconds():
            raise ValueError("Confidence serialization must preserve its actual observation age and cutoff")
    result: dict[str, JsonValue] = {
        "class": confidence.classification,
        "failed": [_confidence_failure_wire(failure) for failure in confidence.failed],
        "limiting": None if confidence.limiting is None else _confidence_failure_wire(confidence.limiting),
        "derivatives_native_coverage_ready": confidence.derivatives_native_coverage_ready,
        "derivatives_classification_ready": confidence.derivatives_classification_ready,
        "freshness": [_field_observation_wire(observation) for observation in confidence.freshness],
    }
    if isinstance(confidence, SelectiveConditionConfidence):
        bounds = context.bounds(confidence.analysis_window)
        window: dict[str, JsonValue] = {
            "window": confidence.analysis_window.window,
            "start_origin": bounds.start_origin,
        }
        window.update(_clock_wire(bounds.start, "start"))
        window.update(_clock_wire(bounds.cutoff, "cutoff"))
        result["analysis_window"] = window
        result["derivatives_classification_ready_reason"] = confidence.derivatives_classification_ready_reason
    return result


def encode_condition_depth(value: ConditionDepthNode, *, clocks: ConditionWireClocks) -> dict[str, JsonValue]:
    depth = ConditionDepthNode.model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if context.cutoff.value is None or depth.cutoff != parse_iso_time(context.cutoff.value):
        raise ValueError("Depth serialization requires its actual selected cutoff")
    observations: list[JsonValue] = []
    for observation in depth.observations:
        if observation.token_id != context.token_id:
            raise ValueError("Depth serialization cannot borrow another subject's selected book")
        item: dict[str, JsonValue] = {
            "token_id": observation.token_id,
            "venue": observation.venue,
            "instrument_id": observation.instrument_id,
            "metric_key": observation.metric_key,
            "fresh": observation.fresh,
        }
        item.update(_clock_wire(observation.observed_at, "observed_at"))
        item.update(_cadence_wire(observation.cadence))
        observations.append(item)
    return {
        "aggregate_depth_p1_usd": _decimal_wire(depth.aggregate_depth_p1_usd),
        "pre_event_depth_p1_usd": _decimal_wire(depth.pre_event_depth_p1_usd),
        "spread_ratio": depth.spread_ratio,
        "observations": observations,
    }


class ConditionWalletFlowEvidenceLookup(Protocol):
    def resolve_wallet_flow(
        self, reference: ConditionWalletFlowEvidenceRef, *, expected: ConditionWalletFlow
    ) -> Mapping[str, JsonValue]: ...


class ConditionUnlockEvidenceLookup(Protocol):
    def resolve_unlock(self, *, expected: ConditionScheduledUnlock) -> Mapping[str, JsonValue]: ...


class ConditionLendingEvidenceLookup(Protocol):
    def resolve_lending_observation(
        self, *, expected: CapturedConditionLending
    ) -> Mapping[str, JsonValue]: ...


class ConditionSupplyEvidenceLookup(
    ConditionWalletFlowEvidenceLookup, ConditionUnlockEvidenceLookup, ConditionLendingEvidenceLookup, Protocol
):
    """Reproject verified captured-observation facts without replacing absent evidence."""


def _verified_supply_wire(value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    checked = JsonDocument.validate_value(dict(value))
    if not isinstance(checked, dict):
        raise ValueError("Supply source evidence must resolve to a verified object")
    return deepcopy(checked)


def encode_condition_wallet_flows(
    value: CapturedConditionWalletFlows, evidence: ConditionWalletFlowEvidenceLookup
) -> dict[str, JsonValue]:
    captured = CapturedConditionWalletFlows.model_validate(value)
    flows: list[JsonValue] = []
    for flow in captured.flows:
        visibility = flow.visibility.eligible()
        if not isinstance(visibility, ConditionObservedPredicate) or not visibility.value:
            continue
        resolved = evidence.resolve_wallet_flow(flow.evidence, expected=flow)
        wire = _verified_supply_wire(resolved)
        wire.update(_clock_wire(flow.observed_at, "ts"))
        wire.update(
            {
                "class": flow.classification,
                "class_reason": flow.classification_reason,
                "qty": _decimal_wire(flow.quantity_native),
                "qty_reason": flow.quantity_reason,
                "reaches_exchange": flow.reaches_exchange,
                "destination_reason": flow.destination_reason,
                "evidence": _model_wire(flow.evidence),
            }
        )
        flows.append(wire)
    return {
        "flows": flows,
        "observation": _model_wire(captured.observed_evidence()),
        "successful_queries": captured.successful_queries,
        "query_visibility": _model_wire(captured.query_visibility),
        "coverage": _model_wire(captured.coverage),
        "coverage_errors": [_model_wire(error) for error in captured.coverage_errors],
    }


def encode_condition_unlocks(
    value: CapturedConditionUnlocks, evidence: ConditionUnlockEvidenceLookup
) -> dict[str, JsonValue]:
    captured = CapturedConditionUnlocks.model_validate(value)
    visibility = captured.visibility.eligible()
    available = isinstance(visibility, ConditionObservedPredicate) and visibility.value
    schedules: list[JsonValue] = []
    if available:
        for unlock in captured.upcoming:
            resolved = evidence.resolve_unlock(expected=unlock)
            wire = _verified_supply_wire(resolved)
            share = unlock.circulating_share
            wire.update(_clock_wire(unlock.scheduled_at, "date"))
            wire.update(
                {
                    "token_id": unlock.token_id,
                    "qty": _decimal_wire(unlock.quantity_native),
                    "qty_reason": unlock.quantity_reason,
                    "label": unlock.label,
                    "pct_circulating": _decimal_wire(share.value),
                    "pct_circulating_unit": None
                    if isinstance(share, UnavailableConditionCirculatingShare)
                    else share.unit,
                    "circulating_share": _model_wire(share),
                    "recipient_class": unlock.recipient_class,
                    "transferable": unlock.transferable,
                    "transferability_reason": unlock.transferability_reason,
                    "inference": "POTENTIAL_SUPPLY_IS_NOT_A_SALE",
                    "source_observation_sha256": unlock.source_observation_sha256,
                    "schedule_index": unlock.schedule_index,
                }
            )
            schedules.append(wire)
    result: dict[str, JsonValue] = {
        "unlocks_next_14d": schedules,
        "schedule_available": available,
        "schedule_reason": None
        if available
        else "RECEIVED_AFTER_ASSEMBLY"
        if isinstance(visibility, ConditionObservedPredicate)
        else "RECEIPT_VISIBILITY_UNPROVEN",
        "capture_visibility": _model_wire(captured.visibility),
    }
    result.update(_clock_wire(captured.observed_at, "observed_at"))
    return result


def encode_condition_lending(
    value: CapturedConditionLending, evidence: ConditionLendingEvidenceLookup
) -> dict[str, JsonValue]:
    captured = CapturedConditionLending.model_validate(value)
    visibility = captured.current_visibility.eligible()
    if not isinstance(visibility, ConditionObservedPredicate) or not visibility.value:
        return {
            "status": "UNAVAILABLE",
            "window": captured.requested_window,
            "reason": "RECEIVED_AFTER_ASSEMBLY"
            if isinstance(visibility, ConditionObservedPredicate)
            else "RECEIPT_VISIBILITY_UNPROVEN",
        }
    wire = _verified_supply_wire(evidence.resolve_lending_observation(expected=captured))
    raw_pools = wire["pools"]
    if not isinstance(raw_pools, list):
        raise ValueError("Verified lending observation requires its actual pool array")
    pool_records: list[dict[str, JsonValue]] = []
    actual_ids: set[str] = set()
    expected_pools = {pool.pool_id: pool for pool in captured.pools}
    for raw in raw_pools:
        if not isinstance(raw, dict):
            raise ValueError("Verified lending pool must be an object")
        pool_id = raw["pool_id"]
        if not isinstance(pool_id, str) or not pool_id.strip() or pool_id in actual_ids:
            raise ValueError("Verified lending pools require distinct actual identities")
        actual_ids.add(pool_id)
        pool_records.append(raw)
    if actual_ids != set(expected_pools):
        raise ValueError("Verified lending pool identities differ from the requested captured observation")
    prior_visibility = captured.previous_visibility.eligible()
    prior_visible = isinstance(prior_visibility, ConditionObservedPredicate) and prior_visibility.value
    for raw in pool_records:
        pool_id = raw["pool_id"]
        if not isinstance(pool_id, str):
            raise ValueError("Verified lending pool requires its actual identity")
        pool = expected_pools[pool_id]
        utilization = pool.utilization()
        change = pool.change()
        raw.update(
            {
                "utilization": _decimal_wire(utilization.value),
                "utilization_reason": utilization.reason,
                "chg_24h": change.value if prior_visible else None,
                "chg_24h_reason": "PRIOR_OBSERVATION_NOT_CAPTURED"
                if captured.previous_source_observation_sha256 is None
                else change.reason
                if prior_visible
                else "PRIOR_RECEIVED_AFTER_ASSEMBLY"
                if isinstance(prior_visibility, ConditionObservedPredicate)
                else "PRIOR_RECEIPT_VISIBILITY_UNPROVEN",
                "borrow_apr": None,
                "borrow_apr_reason": "NOT_ESTIMABLE",
                "borrow_apr_policy_reason": pool.borrow_apr().reason,
                "identity_qualification": _model_wire(pool.identity),
            }
        )
    serialized_pools: list[JsonValue] = [pool for pool in pool_records]
    wire.update(
        {
            "window": captured.requested_window,
            "pools": serialized_pools,
            "borrow_apr": None,
            "reason": "APY_IS_NOT_APR",
            "unmapped_pool_ids": list(captured.unmapped_pool_ids),
            "source_observation_sha256": captured.source_observation_sha256,
            "previous_source_observation_sha256": captured.previous_source_observation_sha256,
            "capture_visibility": _model_wire(captured.current_visibility),
            "previous_capture_visibility": _model_wire(captured.previous_visibility),
        }
    )
    wire.update(_clock_wire(captured.observed_at, "observed_at"))
    wire.update(_clock_wire(captured.previous_observed_at, "previous_observed_at"))
    return wire


def encode_condition_exchange_inventory(
    value: ComparableConditionExchangeInventory, *, clocks: ConditionWireClocks
) -> dict[str, JsonValue]:
    inventory = ComparableConditionExchangeInventory.model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if (
        inventory.token_id != context.token_id
        or context.cutoff.value is None
        or inventory.cutoff != parse_iso_time(context.cutoff.value)
    ):
        raise ValueError("Inventory serialization requires its actual token and cutoff context")
    current_visibility = inventory.current_visibility.eligible()
    if not isinstance(current_visibility, ConditionObservedPredicate) or not current_visibility.value:
        return {
            "value": None,
            "reason": "RECEIVED_AFTER_ASSEMBLY"
            if isinstance(current_visibility, ConditionObservedPredicate)
            else "RECEIPT_VISIBILITY_UNPROVEN",
            "window": inventory.requested_window,
        }
    prior_visibility = inventory.previous_visibility.eligible()
    prior_visible = (
        inventory.previous_observed_at.value is not None
        and isinstance(prior_visibility, ConditionObservedPredicate)
        and prior_visibility.value
    )
    prior_reason = (
        "PRIOR_OBSERVATION_NOT_CAPTURED"
        if inventory.previous_observed_at.value is None
        else "PRIOR_RECEIVED_AFTER_ASSEMBLY"
        if isinstance(prior_visibility, ConditionObservedPredicate)
        else "PRIOR_RECEIPT_VISIBILITY_UNPROVEN"
    )
    native, fractional = inventory.native_change(), inventory.fractional_change()
    balances: list[JsonValue] = []
    for balance in inventory.balances:
        share = balance.fractional_change()
        balances.append(
            {
                "venue": balance.venue,
                "chainid": balance.chain_id,
                "contract": balance.contract,
                "wallet_list_commit": balance.wallet_list_commit,
                "qty_native": str(balance.current_qty_native),
                "chg_native_24h": str(balance.change()) if prior_visible else None,
                "chg_pct": share.value if prior_visible else None,
                "chg_pct_reason": share.reason if prior_visible else prior_reason,
            }
        )
    result: dict[str, JsonValue] = {
        "window": inventory.requested_window,
        "chg_native_24h": _decimal_wire(native.value) if prior_visible else None,
        "chg_pct": fractional.value if prior_visible else None,
        "chg_pct_reason": fractional.reason if prior_visible else prior_reason,
        "normal_range": _range_wire(inventory.normal_range),
        "normal_range_reason": inventory.normal_range.reason,
        "per_venue": balances,
        "coverage": _model_wire(inventory.coverage),
        "reason": native.reason if prior_visible else prior_reason,
        "capture_visibility": _model_wire(inventory.current_visibility),
        "previous_capture_visibility": _model_wire(inventory.previous_visibility),
    }
    result.update(_clock_wire(inventory.current_observed_at, "observed_at"))
    result.update(_clock_wire(inventory.previous_observed_at, "previous_observed_at"))
    return result


def encode_condition_holders_revenue(
    value: ConditionHoldersRevenue, *, clocks: ConditionWireClocks
) -> dict[str, JsonValue]:
    holders = type(value).model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if not isinstance(holders, CapturedConditionHoldersRevenue):
        return {"status": holders.status, "protocols": [], "reason": holders.reason}
    if (
        holders.token_id != context.token_id
        or context.cutoff.value is None
        or holders.cutoff != parse_iso_time(context.cutoff.value)
    ):
        raise ValueError("Holders revenue serialization requires its actual token and cutoff context")
    visibility = holders.visibility.eligible()
    if not isinstance(visibility, ConditionObservedPredicate) or not visibility.value:
        result: dict[str, JsonValue] = {
            "status": "UNAVAILABLE",
            "protocols": [],
            "reason": "RECEIVED_AFTER_ASSEMBLY"
            if isinstance(visibility, ConditionObservedPredicate)
            else "RECEIPT_VISIBILITY_UNPROVEN",
        }
        result.update(_clock_wire(holders.visibility.received_at, "capture_received_at"))
        return result
    protocols: list[JsonValue] = []
    for protocol in holders.protocols:
        point = protocol.latest_chart_point
        protocols.append(
            {
                "protocol": protocol.protocol,
                "provider": protocol.provider,
                "unit": protocol.unit,
                "source_url": protocol.source_url,
                "periods": [
                    {
                        "provider_period": period.provider_period,
                        "value": _decimal_wire(period.value_usd),
                        "status": period.status,
                    }
                    for period in protocol.periods
                ],
                "chart_available": protocol.chart_available,
                "latest_chart_point": None
                if point is None
                else {
                    "provider_timestamp": point.provider_timestamp,
                    "value": str(point.value_usd),
                    "provider_period": point.provider_period,
                },
                "reason": protocol.reason,
            }
        )
    source_at = holders.as_of.value
    if source_at is None:
        raise ValueError("Captured holders revenue requires its actual source clock")
    periods: tuple[Literal["24h", "30d"], ...] = ("24h", "30d")
    output: dict[str, JsonValue] = {
        "status": holders.status,
        "availability": holders.availability(),
        "protocols": protocols,
        "coverage": {period: _model_wire(holders.coverage(period)) for period in periods},
        "capture_availability": "KNOWN",
        "age_s": (holders.cutoff - parse_iso_time(source_at)).total_seconds(),
        "identity_evidence": None
        if holders.identity_evidence is None
        else _model_wire(holders.identity_evidence),
        "identity_reason": holders.identity_reason,
        "source_observation_sha256": holders.source_observation_sha256,
        "reason": holders.reason,
        "inference_limits": [
            "PROVIDER_REVENUE_IS_NOT_PROOF_OF_TOKEN_CAPTURE",
            "PER_PROTOCOL_VALUES_NOT_SUMMED_PARENT_CHILD_SCOPE_MAY_OVERLAP",
        ],
    }
    output.update(_clock_wire(holders.as_of, "as_of"))
    output.update(_clock_wire(holders.visibility.received_at, "capture_received_at"))
    output.update(_cadence_wire(holders.cadence))
    output["cadence_basis"] = output["cadence_source"]
    output["cadence_basis_reason"] = output["cadence_source_reason"]
    return output


def encode_condition_chain_liquidity(
    value: ConditionChainLiquidity, *, clocks: ConditionWireClocks
) -> dict[str, JsonValue]:
    chain = type(value).model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if not isinstance(chain, CapturedConditionChainLiquidity):
        return {"chain": chain.chain, "value": None, "reason": chain.reason}
    if (
        chain.token_id != context.token_id
        or context.cutoff.value is None
        or chain.cutoff != parse_iso_time(context.cutoff.value)
    ):
        raise ValueError("Chain context serialization requires its actual token and cutoff")
    visibility = chain.visibility.eligible()
    if not isinstance(visibility, ConditionObservedPredicate) or not visibility.value:
        return {
            "chain": chain.chain,
            "value": None,
            "reason": "RECEIVED_AFTER_ASSEMBLY"
            if isinstance(visibility, ConditionObservedPredicate)
            else "RECEIPT_VISIBILITY_UNPROVEN",
        }
    current = chain.current
    day_change, week_change = chain.change_24h(), chain.change_7d()
    result: dict[str, JsonValue] = {
        "chain": chain.chain,
        "supply_usd": _decimal_wire(current.value_usd)
        if isinstance(current, SelectedConditionStablecoinPoint)
        else None,
        "supply_chg_24h": _decimal_wire(day_change.value),
        "supply_chg_24h_reason": day_change.reason,
        "supply_chg_7d": _decimal_wire(week_change.value),
        "supply_chg_7d_reason": week_change.reason,
        "reason": current.value_reason
        if isinstance(current, SelectedConditionStablecoinPoint)
        else current.reason,
        "asof_points": {
            "current": _model_wire(current),
            "day_ago": _model_wire(chain.day_ago),
            "week_ago": _model_wire(chain.week_ago),
        },
        "source_observation_sha256": chain.source_observation_sha256,
        "capture_visibility": _model_wire(chain.visibility),
    }
    result.update(_clock_wire(chain.observed_at, "observed_at"))
    if isinstance(current, SelectedConditionStablecoinPoint):
        result.update(_clock_wire(current.observed_at, "supply_usd_observed_at"))
    return result


def _supply_visibility_failure(value: ConditionReceiptVisibility) -> str | None:
    visibility = value.eligible()
    if isinstance(visibility, ConditionObservedPredicate):
        return None if visibility.value else "RECEIVED_AFTER_ASSEMBLY"
    return "RECEIPT_VISIBILITY_UNPROVEN"


def encode_condition_supply(
    value: ConditionSupplyGraph,
    evidence: ConditionSupplyEvidenceLookup,
    *,
    clocks: ConditionWireClocks,
) -> dict[str, JsonValue]:
    supply = ConditionSupplyGraph.model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if supply.token_id != context.token_id:
        raise ValueError("Supply serialization cannot borrow another token's clock context")
    bounds = context.bounds(supply.interval)
    unavailable: dict[str, JsonValue] = {}
    if isinstance(supply.exchange_inventory, ComparableConditionExchangeInventory):
        inventory = encode_condition_exchange_inventory(supply.exchange_inventory, clocks=context)
        failure = _supply_visibility_failure(supply.exchange_inventory.current_visibility)
        if failure is not None:
            unavailable["exchange_inventory"] = failure
    else:
        inventory = {"value": None, "reason": supply.exchange_inventory.reason}
        unavailable["exchange_inventory"] = supply.exchange_inventory.reason
    wallet = supply.wallet_flows
    if isinstance(wallet, CapturedConditionWalletFlows):
        wallet_wire = encode_condition_wallet_flows(wallet, evidence)
        observation = wallet.observed_evidence()
        if isinstance(observation, UnavailableConditionSupplyObservation):
            unavailable["flows"] = observation.reason
        flows = wallet_wire["flows"]
    else:
        flows = []
        wallet_wire = _model_wire(wallet)
        unavailable["flows"] = wallet.reason
    unlocks = supply.unlocks
    if isinstance(unlocks, CapturedConditionUnlocks):
        unlock_wire = encode_condition_unlocks(unlocks, evidence)
        schedules = unlock_wire["unlocks_next_14d"]
        failure = _supply_visibility_failure(unlocks.visibility)
        if failure is not None:
            unavailable["unlocks"] = failure
    else:
        schedules = []
        unlock_wire = _model_wire(unlocks)
        unavailable["unlocks"] = unlocks.reason
    lending = supply.lending
    if isinstance(lending, CapturedConditionLending):
        lending_wire = encode_condition_lending(lending, evidence)
        failure = _supply_visibility_failure(lending.current_visibility)
        if failure is not None:
            unavailable["lending"] = failure
    else:
        lending_wire = {"value": None, "reason": lending.reason}
        unavailable["lending"] = lending.reason
    primary_chain = supply.primary_chain_context()
    if isinstance(primary_chain, CapturedConditionChainLiquidity):
        failure = _supply_visibility_failure(primary_chain.visibility)
        if failure is not None:
            unavailable["stablecoin_chain"] = failure
    else:
        unavailable["stablecoin_chain"] = primary_chain.reason
    presence = supply.evidence()
    result: dict[str, JsonValue] = {
        "token_id": supply.token_id,
        "window": supply.interval.window,
        "holders_revenue": encode_condition_holders_revenue(supply.holders_revenue, clocks=context),
        "exchange_inventory": inventory,
        "flows": flows,
        "wallet_flow_observation": wallet_wire,
        "unlocks_next_14d": schedules,
        "unlock_schedule": unlock_wire,
        "lending": lending_wire,
        "stablecoin_chain": encode_condition_chain_liquidity(primary_chain, clocks=context),
        "chain_contexts": [
            encode_condition_chain_liquidity(chain, clocks=context) for chain in supply.chain_contexts
        ],
        "settlement_chain": _model_wire(supply.settlement_chain),
        "unavailable": unavailable,
        "supply_in_evidence": "TRUE"
        if isinstance(presence, ConditionPositiveSupplyEvidence)
        else "FALSE"
        if isinstance(presence, ConditionNoPositiveSupplyEvidence)
        else "UNKNOWN",
        "supply_evidence": _model_wire(presence),
    }
    result.update(_clock_wire(bounds.start, "start"))
    result.update(_clock_wire(bounds.cutoff, "cutoff"))
    result.update(_clock_wire(supply.assembled_at, "assembled_at"))
    return result


def encode_condition_evaluation_proof(value: ConditionEvaluationProof) -> dict[str, JsonValue]:
    proof = ConditionEvaluationProof.model_validate(value)
    observations: list[JsonValue] = []
    freshness = proof.freshness
    if isinstance(freshness, AssessedConditionProofFreshness):
        for observation in freshness.observations:
            item: dict[str, JsonValue] = {
                "token_id": observation.token_id,
                "instrument_id": observation.instrument_id,
            }
            item.update(_clock_wire(observation.observed_at, "observed_at"))
            item.update(_cadence_wire(observation.cadence))
            if isinstance(observation, MissingConditionProofObservation):
                item.update(
                    {
                        "status": observation.status,
                        "collection": observation.collection,
                        "reason": observation.reason,
                    }
                )
            else:
                item["fresh"] = observation.fresh
                if isinstance(observation, ConditionFieldObservation):
                    item.update(
                        {
                            "collection": observation.collection,
                            "age_s": observation.age_s,
                            "reason": observation.reason,
                        }
                    )
            observations.append(item)
    key = proof.cache_key
    result: dict[str, JsonValue] = {
        "metric": proof.requirement.metric,
        "window": proof.requirement.window,
        "dependency_fingerprint": key.dependency_identity,
        "content_sha256": key.content_sha256,
        "visibility_sha256": key.visibility_sha256,
        "reused": proof.reused,
        "reason": proof.reason,
        "observations": observations,
        "available": proof.available,
        "coverage": None if proof.coverage is None else _model_wire(proof.coverage),
        "freshness": _model_wire(freshness),
    }
    result.update(_clock_wire(proof.assembled_at, "assembled_at"))
    return result


def _liquidity_venue_wire(value: ConditionLiquidityVenue) -> dict[str, JsonValue]:
    venue = ConditionLiquidityVenue.model_validate(value)
    bid, ask = venue.bid.usd.value, venue.ask.usd.value
    total = None if bid is None or ask is None else sum_condition_native((bid, ask))
    tiers = {"T1": venue.impact_tiers.T1, "T2": venue.impact_tiers.T2, "T3": venue.impact_tiers.T3}
    ranges: dict[str, JsonValue] = {"bid": venue.bid_range.percent, "ask": venue.ask_range.percent}
    result: dict[str, JsonValue] = {
        "venue": venue.venue,
        "instrument_id": venue.instrument_id,
        "coverage": {"venue": venue.venue, "range_covered_pct": ranges, "integrity_ok": venue.integrity_ok},
        "spread_bps": venue.spread_bps,
        "spread_reason": venue.spread_reason,
        "depth_p1_usd": _decimal_wire(total),
        "bid_depth_p1_usd": _decimal_wire(bid),
        "ask_depth_p1_usd": _decimal_wire(ask),
        "depth_sides": {"bid": _model_wire(venue.bid), "ask": _model_wire(venue.ask)},
        "range_covered_pct": dict(ranges),
        "range_reasons": {"bid": venue.bid_range.reason, "ask": venue.ask_range.reason},
        "tier_notional": {name: _decimal_wire(tier.notional_usd) for name, tier in tiers.items()},
        "slippage_bps": {name: tier.slippage_bps() for name, tier in tiers.items()},
        "impact_tiers": _model_wire(venue.impact_tiers),
        "quote_asset": venue.quote_asset,
        "quote_valuation": _model_wire(venue.quote_valuation),
        "precision": venue.observed_at.precision,
    }
    result.update(_clock_wire(venue.observed_at, "observed_at"))
    return result


def encode_condition_fragmentation(value: ConditionFragmentationGraph) -> dict[str, JsonValue]:
    graph = ConditionFragmentationGraph.model_validate(value)
    predicates = {
        "persistent_gap": graph.persistent_gap(),
        "validated": graph.validated(),
        "price_path_validated": graph.price_path_validated(),
        "funding_path_validated": graph.funding_path_validated(),
        "funding_beyond_normal": graph.funding.primary_beyond_normal(),
        "top_sign_flips": graph.top_sign_history.multiple_flips(),
        "gap_closed_quickly": graph.gap_closed_quickly(),
    }
    pair_observations: list[JsonValue] = []
    persistent_pairs: list[JsonValue] = []
    unconverted_pairs: list[JsonValue] = []
    for pair in graph.price_pairs:
        pair_wire = _model_wire(pair)
        if not isinstance(pair_wire, dict):
            raise ValueError("A fragmentation pair must serialize as an object")
        gap_series = pair.gap_series()
        pair_wire["gap_series"] = [_model_wire(point) for point in gap_series]
        persistent = pair.persistent_gap()
        pair_wire["persistent"] = _predicate_wire(persistent)
        pair_observations.append(pair_wire)
        if isinstance(persistent, ConditionObservedPredicate) and persistent.value:
            persistent_pairs.append([pair.left_venue, pair.right_venue])
        if any(
            not isinstance(point.gap, ConditionObservedPredicate)
            and point.gap.reason == "QUOTE_CONVERSION_UNAVAILABLE"
            for point in gap_series
        ):
            unconverted_pairs.append([pair.left_venue, pair.right_venue])
    costs = {pair.quote_conversion_uncertainty_bps for pair in graph.price_pairs}
    complete_venues: list[JsonValue] = []
    for capture in graph.spot_capture:
        complete = capture.complete()
        if isinstance(complete, ConditionObservedPredicate) and complete.value:
            complete_venues.append(capture.venue)
    result: dict[str, JsonValue] = {
        "token_id": graph.token_id,
        "window": graph.interval.window,
        "start": iso(graph.interval.start),
        "cutoff": iso(graph.interval.cutoff),
        "funding_normal_range_8h": _range_wire(graph.funding.normal_range_8h),
        "venues": persistent_pairs,
        "pair_observations": pair_observations,
        "cross_quote_not_evaluated": unconverted_pairs,
        "quote_conversion_uncertainty_bps": str(next(iter(costs))) if len(costs) == 1 else None,
        "quote_conversion_uncertainty_reason": None
        if len(costs) == 1
        else "NO_EVALUATED_PAIRS"
        if not costs
        else "MULTIPLE_CAPTURED_POLICIES",
        "complete_venues": complete_venues,
        "reason": None
        if any(isinstance(value, ConditionObservedPredicate) for value in predicates.values())
        else "INCOMPLETE",
        "funding_confirmations": _model_wire(graph.funding),
        "price_confirmations": [_model_wire(confirmation) for confirmation in graph.price_confirmations],
        "spot_capture": [_model_wire(capture) for capture in graph.spot_capture],
        "perp_capture": [_model_wire(capture) for capture in graph.perp_capture],
        "top_sign_history": _model_wire(graph.top_sign_history),
        "predicate_proofs": {name: _model_wire(predicate) for name, predicate in predicates.items()},
    }
    result.update({name: _predicate_wire(predicate) for name, predicate in predicates.items()})
    result.update(_clock_wire(graph.assembled_at, "assembled_at"))
    return result


def encode_condition_pre_depth_window(
    value: ConditionPreDepthWindow, *, interval: ConditionInterval, clocks: ConditionWireClocks
) -> dict[str, JsonValue]:
    window = ConditionPreDepthWindow.model_validate(value)
    scope = ConditionInterval.model_validate(interval)
    context = ConditionWireClocks.model_validate(clocks)
    context.bounds(scope)
    anchor = window.origin
    if isinstance(anchor, ConditionDeclaredEventAnchor):
        if anchor.at != context.event_anchor:
            raise ValueError("Pre-depth serialization requires the actual captured event anchor metadata")
        at = anchor.at
    else:
        if anchor.at != scope.start:
            raise ValueError("An analysis pre-depth boundary must retain the actual analysis window start")
        at = offset_schedule(context.cutoff, anchor.at - scope.cutoff)
    result: dict[str, JsonValue] = {"anchor_type": anchor.kind}
    result.update(_clock_wire(offset_schedule(at, window.start - window.cutoff), "start"))
    result.update(_clock_wire(at, "cutoff"))
    return result


def encode_condition_liquidity(
    value: ConditionLiquidity, *, clocks: ConditionWireClocks
) -> dict[str, JsonValue]:
    liquidity = type(value).model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if liquidity.token_id != context.token_id:
        raise ValueError("Liquidity serialization cannot borrow another token's clock context")
    bounds = context.bounds(liquidity.interval)
    spread = liquidity.spread.ratio()
    drop, lost = liquidity.depth_drop(), liquidity.depth_majority_lost()
    state, vacuum, thin = liquidity.state(), liquidity.vacuum(), liquidity.thinness()
    elasticity = liquidity.elasticity
    response = elasticity.bps_per_depth_unit()
    flow = elasticity.flow.comparison_flow()
    hit_side = flow.hit_side() if flow.status == "OBSERVED" else None
    replenishment = liquidity.replenishment.summary()
    previous = liquidity.previous_depth
    per: list[JsonValue] = [_liquidity_venue_wire(venue) for venue in liquidity.per_venue]
    previous_wire: dict[str, JsonValue]
    side_drops: dict[str, JsonValue] = {}
    cohorts: dict[str, JsonValue] = {}
    if isinstance(previous, ComputedConditionPreDepth):
        previous_wire = encode_condition_pre_depth_window(
            previous.window, interval=liquidity.interval, clocks=context
        )
        pre_total = previous.totals.combined.value
        pre_sides: dict[str, JsonValue] = {
            "bid": _decimal_wire(previous.totals.bid.value),
            "ask": _decimal_wire(previous.totals.ask.value),
        }
        for name, cohort in (("bid", previous.bid_cohort), ("ask", previous.ask_cohort)):
            side_drops[name] = (
                cohort.drop().value if isinstance(cohort, ObservedConditionDepthCohort) else None
            )
            cohorts[name] = list(cohort.venues) if isinstance(cohort, ObservedConditionDepthCohort) else []
    else:
        previous_wire = {"start": None, "cutoff": None, "reason": previous.reason}
        pre_total = None
        pre_sides = {"bid": None, "ask": None}
        side_drops = {"bid": None, "ask": None}
        cohorts = {"bid": [], "ask": []}
    observations: list[JsonValue] = []
    for burst in liquidity.replenishment.bursts:
        row = _model_wire(burst)
        if not isinstance(row, dict):
            raise ValueError("A replenishment observation must serialize as an object")
        row.update(_clock_wire(burst.flow_start, "ts"))
        row.update(
            {
                "replenished": _predicate_wire(burst.replenished(liquidity.replenishment.scope)),
                "resilient": _predicate_wire(burst.resilient(liquidity.replenishment.scope)),
                "recovery_s": burst.flow_start_to_recovery_s,
            }
        )
        observations.append(row)
    replenish_wire: dict[str, JsonValue] = {
        "available": isinstance(replenishment, ComputedConditionReplenishmentSummary),
        "summary": _model_wire(replenishment),
        "scan_scope": _model_wire(liquidity.replenishment.scope),
        "bursts": replenishment.observed_bursts
        if isinstance(replenishment, ComputedConditionReplenishmentSummary)
        else None,
        "replenished": replenishment.observed_replenished
        if isinstance(replenishment, ComputedConditionReplenishmentSummary)
        else None,
        "resilient_bursts": replenishment.observed_resilient_replenished
        if isinstance(replenishment, ComputedConditionReplenishmentSummary)
        else None,
        "median_recovery_s": replenishment.median_flow_start_to_recovery_s
        if isinstance(replenishment, ComputedConditionReplenishmentSummary)
        else None,
        "observations": observations,
    }
    current_side = (
        None if hit_side is None else elasticity.current.ask if hit_side == "ask" else elasticity.current.bid
    )
    bound = (
        None
        if current_side is None
        else current_side.native
        if flow.status == "OBSERVED" and flow.unit == "NATIVE"
        else current_side.usd
    )
    previous_side = (
        None
        if hit_side is None or not isinstance(elasticity.previous, ConditionBookDepthBounds)
        else elasticity.previous.ask
        if hit_side == "ask"
        else elasticity.previous.bid
    )
    pre_bound = (
        None
        if previous_side is None
        else previous_side.native
        if flow.status == "OBSERVED" and flow.unit == "NATIVE"
        else previous_side.usd
    )
    current_hit_depth = (
        None
        if hit_side is None
        else liquidity.current_depth.ask.value
        if hit_side == "ask"
        else liquidity.current_depth.bid.value
    )
    prior_hit_depth = (
        None
        if hit_side is None or not isinstance(previous, ComputedConditionPreDepth)
        else previous.totals.ask.value
        if hit_side == "ask"
        else previous.totals.bid.value
    )
    result: dict[str, JsonValue] = {
        "token_id": liquidity.token_id,
        "window": liquidity.interval.window,
        "enrichment_phase": liquidity.kind,
        "depth_measurement_band_pct": liquidity.measurement_band_percent,
        "per_venue": per,
        "aggregate_depth_p1_usd": _decimal_wire(liquidity.current_depth.combined.value),
        "aggregate_bid_depth_p1_usd": _decimal_wire(liquidity.current_depth.bid.value),
        "aggregate_ask_depth_p1_usd": _decimal_wire(liquidity.current_depth.ask.value),
        "depth_totals": _model_wire(liquidity.current_depth),
        "pre_event_depth_p1_usd": _decimal_wire(pre_total),
        "pre_event_depth_window": previous_wire,
        "pre_event_side_depth_p1_usd": pre_sides,
        "pre_depth": _model_wire(previous),
        "depth_drop": drop.value,
        "depth_drop_reason": None if drop.status == "OBSERVED" else drop.reason,
        "depth_majority_lost": _predicate_wire(lost),
        "depth_drop_by_side": side_drops,
        "depth_drop_cohort": cohorts,
        "spread_ratio": spread.value,
        "spread_ratio_reason": spread.reason,
        "spread_comparison": _model_wire(liquidity.spread),
        "replenishment": replenish_wire,
        "elasticity": {
            "bps_per_depth_unit": response.bps_per_depth_unit,
            "proof": _model_wire(response),
            "weak_response": _predicate_wire(elasticity.weak_response()),
            "excess_response": _predicate_wire(elasticity.excess_response()),
            "hit_side": hit_side,
            "hit_side_reason": None
            if hit_side is not None
            else "ZERO_NET_FLOW"
            if flow.status == "OBSERVED"
            else "INCOMPLETE",
            "depth_unit": flow.unit if flow.status == "OBSERVED" else None,
            "net_flow": _decimal_wire(flow.value),
            "hit_side_depth_lower_bound": None if bound is None else _decimal_wire(bound.value),
            "hit_side_covered_venues": [] if bound is None else list(bound.covered_venues),
            "hit_side_coverage_complete": None if bound is None else bound.fully_covered(),
            "pre_event_hit_side_depth_lower_bound": None
            if pre_bound is None
            else _decimal_wire(pre_bound.value),
            "pre_event_hit_side_covered_venues": [] if pre_bound is None else list(pre_bound.covered_venues),
            "hit_side_depth_p1_usd": _decimal_wire(current_hit_depth),
            "pre_event_hit_side_depth_p1_usd": _decimal_wire(prior_hit_depth),
            "avg_hit_side_depth_p1_usd": _decimal_wire(elasticity.average_hit_side_depth_usd.value),
            "inputs": _model_wire(elasticity),
        },
        "liquidity_state": state.value,
        "liquidity_state_proof": _model_wire(state),
        "vacuum": _predicate_wire(vacuum),
        "vacuum_proof": _model_wire(vacuum),
        "thin": _predicate_wire(thin),
        "thin_proof": _model_wire(thin),
    }
    result.update(_clock_wire(bounds.start, "start"))
    result.update(_clock_wire(bounds.cutoff, "cutoff"))
    if isinstance(liquidity, EnrichedConditionLiquidity):
        volatility = liquidity.vol_compression
        classification = volatility.classification()
        volatility_wire: dict[str, JsonValue] = {
            "rv24": volatility.realized.value,
            "normal_range": _range_wire(volatility.normal_range),
            "state": classification.value,
            "classification": _model_wire(classification),
            "measurement": _model_wire(volatility.realized),
            "reference_venue": volatility.reference_venue,
        }
        if isinstance(volatility.realized, ObservedConditionVolatility):
            volatility_wire.update(_clock_wire(volatility.realized.completed_hour, "rv24_observed_at"))
        result["vol_compression"] = volatility_wire
        result["fragmentation"] = encode_condition_fragmentation(liquidity.fragmentation)
        tier_names: tuple[Literal["T1", "T2", "T3"], ...] = ("T1", "T2", "T3")
        best: dict[Literal["T1", "T2", "T3"], ConditionSlippage] = {
            tier: liquidity.best_single_venue_slippage(tier) for tier in tier_names
        }
        split: dict[Literal["T1", "T2", "T3"], ConditionSlippage] = {
            tier: liquidity.execution.split_slippage(tier) for tier in tier_names
        }
        result["best_single_venue_slippage_bps"] = {tier: value.value for tier, value in best.items()}
        result["split_slippage_bps"] = {tier: value.value for tier, value in split.items()}
        result["best_single_venue_slippage_proofs"] = {
            tier: _model_wire(value) for tier, value in best.items()
        }
        result["split_slippage_proofs"] = {tier: _model_wire(value) for tier, value in split.items()}
        result["execution"] = _model_wire(liquidity.execution)
        result.update(_clock_wire(liquidity.assembled_at, "assembled_at"))
    else:
        result["vol_compression"] = {"status": "NOT_REQUESTED"}
        result["fragmentation"] = {"status": "NOT_REQUESTED"}
    return result


class ConditionStateEvidenceLookup(ConditionRelativeEvidenceLookup, ConditionSupplyEvidenceLookup, Protocol):
    """Rejoin only the source witnesses actually consumed by a full state graph."""


def encode_nonforward_condition_market_state(
    value: NonForwardConditionMarketState, *, clocks: ConditionWireClocks
) -> dict[str, JsonValue]:
    state = NonForwardConditionMarketState.model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if (
        state.token_id != context.token_id
        or context.cutoff.value is None
        or parse_iso_time(context.cutoff.value) != state.interval.cutoff
    ):
        raise ValueError("A nonforward result requires its actual subject and cutoff metadata")
    result: dict[str, JsonValue] = {
        "token_id": state.token_id,
        "state_phase": state.kind,
        "move_class": state.move_class,
        "divergence": {"state": state.divergence_state},
        "data_confidence": {"class": state.confidence},
        "analysis_window": {
            "start": iso(state.interval.start),
            "cutoff": iso(state.interval.cutoff),
            "reason": state.reason,
        },
    }
    result.update(_clock_wire(context.cutoff, "cutoff"))
    return result


def encode_full_condition_market_state(
    value: FullConditionMarketState,
    evidence: ConditionStateEvidenceLookup,
    *,
    clocks: ConditionWireClocks,
) -> dict[str, JsonValue]:
    state = FullConditionMarketState.model_validate(value)
    context = ConditionWireClocks.model_validate(clocks)
    if (
        state.token_id != context.token_id
        or context.cutoff.value is None
        or parse_iso_time(context.cutoff.value) != state.cutoff
    ):
        raise ValueError("Full state serialization requires its actual subject and cutoff metadata")
    evaluation = state.candidates.divergence
    proposal = state.candidates.proposal(fragmentation=state.liquidity.fragmentation.validated())
    candidates: list[JsonValue] = []
    for candidate in evaluation.candidates:
        candidates.append(
            {
                "state": candidate.state,
                "eligible": candidate.eligible(),
                "refuted": _predicate_wire(candidate.refutation),
                "testable": candidate.testable(),
                "support": candidate.support,
                "support_reason": candidate.support_reason,
                "evaluation": _model_wire(candidate),
            }
        )
    support_reason = (
        "LOW_DATA_CONFIDENCE"
        if state.data_confidence.classification == "LOW"
        else proposal.reason
        if proposal.status == "UNAVAILABLE"
        else None
    )
    result: dict[str, JsonValue] = {
        "token_id": state.token_id,
        "state_phase": state.kind,
        "spot": {
            name: encode_scoped_condition_spot(spot, evidence, clocks=context)
            for name, spot in state.spot.items()
        },
        "derivatives": {"windows": {state.primary_window: encode_condition_derivatives(state.derivatives)}},
        "liquidity": encode_condition_liquidity(state.liquidity, clocks=context),
        "supply": encode_condition_supply(state.supply, evidence, clocks=context),
        "relative": encode_condition_relative(state.relative, evidence, clocks=context),
        "data_confidence": encode_condition_confidence(state.data_confidence, clocks=context),
        "primary_window": state.primary_window,
        "eligible_before_divergence": [name for name in sorted(state.candidates.eligible_before_divergence)],
        "eligible_support_before_divergence": {
            name: count for name, count in state.candidates.eligible_before_divergence.items()
        },
        "move_class": state.move_class(),
        "support": state.selected_support(),
        "support_reason": support_reason,
        "alternatives": list(state.alternatives()),
        "divergence": {
            "state": state.divergence_state(),
            "triggered": _predicate_wire(evaluation.trigger),
            "trigger_proof": _model_wire(evaluation.trigger),
            "candidates": candidates,
            "unresolved": list(evaluation.unresolved()),
            "confirmed": state.absorption_confirmed(),
            "confirmation_scope": "SELECTED_STATE_LABEL",
        },
        "class_confidence": state.class_confidence(),
        "flags": list(state.candidates.flags),
        "selection_proof": _model_wire(proposal),
    }
    result.update(_clock_wire(context.cutoff, "cutoff"))
    return result


def encode_requested_condition_derivatives(value: ConditionRequestedDerivativeView) -> dict[str, JsonValue]:
    view = ConditionRequestedDerivativeView.model_validate(value)
    source = view.requested
    state = view.state
    direct = {venue.instrument_id: venue for venue in source.per_venue}
    state_venues = {} if state is None else {venue.instrument_id: venue for venue in state.per_venue}
    identities = list(direct) if state is None else list(state_venues)
    funding_requested = view.requested_domain("funding")
    if funding_requested:
        funding_fresh = view.freshness_passes("funding")
    rows: list[JsonValue] = []
    for identity in identities:
        full = None if state is None else state_venues[identity]
        requested = direct[identity]
        row = _derivative_venue_wire(requested if full is None else full)
        row["state_membership"] = full is not None
        if funding_requested:
            if full is not None:
                row["state_funding_facts"] = _model_wire(full.funding)
            row.update(encode_condition_funding(requested.funding))
            row["funding_source_facts"] = _model_wire(requested.funding)
            row["funding_requested"] = True
            available = (
                isinstance(requested.funding, AvailableConditionFunding)
                and requested.funding.funding_vs_normal is not None
            )
            row["funding_evaluation_available"] = available and funding_fresh
            row["funding_evaluation_reason"] = (
                None
                if available and funding_fresh
                else "FRESHNESS_GATE_FAILED"
                if not funding_fresh
                else "INCOMPLETE"
            )
            if not funding_fresh:
                row["funding_vs_normal"] = None
        rows.append(row)
    result = _derivative_window_wire(source if state is None else state, rows)
    if view.requested_domain("oi"):
        oi_fresh = view.freshness_passes("oi")
        raw = _derivative_window_wire(source, [])
        result["synchronized"] = raw["synchronized"] if oi_fresh else None
        result["synchronized_status"] = source.synchronized.status if oi_fresh else "UNAVAILABLE"
        result["oi_source_facts"] = _model_wire(source.synchronized)
        if state is not None:
            result["state_oi_source_facts"] = _model_wire(state.synchronized)
        if not oi_fresh:
            result["synchronized_reason"] = "FRESHNESS_GATE_FAILED"
    result["requested_evaluation"] = [encode_condition_evaluation_proof(proof) for proof in view.proofs]
    return result
