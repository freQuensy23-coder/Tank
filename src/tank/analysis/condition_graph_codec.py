"""Explicit condition-graph serialization and consumed-evidence binding."""

from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal
from typing import Protocol

from pydantic import JsonValue

from tank.analysis.condition_graph_contracts import (
    AvailableConditionFunding,
    BasicConditionSpot,
    ConditionAggregateLiquidations,
    ConditionClose,
    ConditionDerivatives,
    ConditionDerivativeSupportFeatures,
    ConditionDerivativeVenue,
    ConditionFlowAggregate,
    ConditionFlowAggregateProjection,
    ConditionFlowCaptureProjection,
    ConditionFlowCaptureWitness,
    ConditionFunding,
    ConditionFundingObservation,
    ConditionIncrementalVolumes,
    ConditionLiquidations,
    ConditionNativeVolumeSupport,
    ConditionObservedFundingDispersion,
    ConditionObservedIncrementalVolume,
    ConditionObservedPredicate,
    ConditionOpenInterest,
    ConditionPredicate,
    ConditionSynchronizedOpenInterest,
    EnrichedConditionDerivatives,
    EnrichedConditionDerivativeVenue,
    FlowAggregateEvidenceRef,
    FlowCaptureEvidenceRef,
    KnownConditionTradeCount,
    UnavailableConditionLeadLag,
    UnavailableConditionOpenInterest,
    UnrequestedConditionFunding,
    UnrequestedConditionOpenInterest,
)
from tank.analysis.statistics_contracts import DecimalRange, FloatRange
from tank.contracts import DomainModel
from tank.contracts.json import JsonDocument
from tank.time_precision import Timestamp
from tank.util import iso, sha256


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


def encode_basic_condition_spot(
    value: BasicConditionSpot, evidence: ConditionFlowEvidenceLookup
) -> dict[str, JsonValue]:
    validated = BasicConditionSpot.model_validate(value)
    result = JsonDocument.validate_value(
        validated.model_dump(mode="json", exclude={"kind", "token_id"}, warnings="error")
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


def _range_wire(value: DecimalRange | FloatRange) -> list[JsonValue] | None:
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
    result: dict[str, JsonValue] = {
        "per_venue": [_derivative_venue_wire(venue) for venue in validated.per_venue]
    }
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
