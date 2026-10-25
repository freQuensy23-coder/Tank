from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.condition_graph_codec import (
    bind_flow_aggregate_evidence,
    flow_aggregate_evidence_wire,
    flow_aggregate_projection_sha256,
    validate_flow_aggregate_evidence,
)
from tank.analysis.condition_graph_contracts import (
    BasicConditionDerivatives,
    ConditionFlowAggregateProjection,
    ConditionSizeProfile,
    ConditionWireClocks,
    EnrichedConditionSpot,
    FlowAggregateEvidenceRef,
    IncompleteConditionTradeCounts,
)
from tank.util import sha256


def projection() -> ConditionFlowAggregateProjection:
    end = datetime(2026, 10, 17, 12, tzinfo=UTC)
    return ConditionFlowAggregateProjection(
        token_id="T",
        venue="a",
        market_type="SPOT",
        instrument_id="fixture-instrument",
        start=end - timedelta(seconds=30),
        end=end,
        complete=True,
        buy_qty=Decimal(10),
        sell_qty=Decimal(0),
        sum_sq_qty=Decimal(100),
        buy_notional=None,
        sell_notional=None,
        notional_reason="INCOMPLETE",
        low=None,
        low_reason="INCOMPLETE",
        counts=IncompleteConditionTradeCounts(
            status="UNAVAILABLE", n_buy=None, n_sell=0, reason="INCOMPLETE"
        ),
    )


class EvidenceLookup:
    def __init__(self, expected: ConditionFlowAggregateProjection) -> None:
        self.expected = expected
        self.calls = 0
        self.wire: dict[str, JsonValue] = {
            "token_id": expected.token_id,
            "venue": expected.venue,
            "market_type": expected.market_type,
            "instrument_id": expected.instrument_id,
            "start": "2026-10-17T11:59:30Z",
            "end": "2026-10-17T12:00:00Z",
            "complete": True,
            "buy_qty": "10",
            "sell_qty": "0",
            "sum_sq_qty": "100",
            "buy_notional": None,
            "sell_notional": None,
            "low": None,
            "provider_note": {"unchanged": True},
            "n_sell": 0,
        }
        self.source_identity = sha256(
            {
                "schema": "fixture-storage-read-evidence.v1",
                "original": self.wire,
                "effective": self.wire,
                "parent": None,
            }
        )

    def resolve_flow_aggregate(
        self, reference: FlowAggregateEvidenceRef, *, expected: ConditionFlowAggregateProjection
    ) -> Mapping[str, JsonValue]:
        self.calls += 1
        assert expected == self.expected
        assert reference.source_evidence_sha256 == self.source_identity
        assert reference.projection_sha256 == flow_aggregate_projection_sha256(self.expected)
        return self.wire


def test_projection_hash_binds_actual_token_venue_and_market_without_id_inference() -> None:
    source = projection()
    digest = flow_aggregate_projection_sha256(source)
    assert len(digest) == 64
    for field, value in (("token_id", "OTHER"), ("venue", "other"), ("market_type", "PERP")):
        changed = type(source).model_validate(source.model_copy(update={field: value}))
        assert flow_aggregate_projection_sha256(changed) != digest
    bound = bind_flow_aggregate_evidence(
        source,
        FlowAggregateEvidenceRef(
            projection_sha256=flow_aggregate_projection_sha256(source),
            source_evidence_sha256=EvidenceLookup(source).source_identity,
        ),
    )
    assert bound.evidence.projection_sha256 == digest
    assert bound.counts.n_buy is None and bound.counts.n_sell == 0
    assert bind_flow_aggregate_evidence(bound, bound.evidence) == bound


def test_invalid_or_stale_projection_refuses_before_evidence_lookup() -> None:
    source = projection()
    bound = bind_flow_aggregate_evidence(
        source,
        FlowAggregateEvidenceRef(
            projection_sha256=flow_aggregate_projection_sha256(source),
            source_evidence_sha256=EvidenceLookup(source).source_identity,
        ),
    )
    lookup = EvidenceLookup(source)
    changed = bound.model_copy(update={"venue": "different"})
    with pytest.raises(ValueError, match="does not match"):
        flow_aggregate_evidence_wire(changed, lookup)
    assert lookup.calls == 0
    with pytest.raises(ValueError, match="does not match"):
        bind_flow_aggregate_evidence(changed, changed.evidence)
    with pytest.raises(ValidationError, match="boolean"):
        flow_aggregate_projection_sha256(source.model_copy(update={"complete": 1}))

    class ExtendedProjection(ConditionFlowAggregateProjection):
        extra: str

    with pytest.raises(TypeError, match="declared flow aggregate"):
        flow_aggregate_projection_sha256(
            ExtendedProjection(
                token_id=source.token_id,
                venue=source.venue,
                market_type=source.market_type,
                instrument_id=source.instrument_id,
                start=source.start,
                end=source.end,
                complete=source.complete,
                buy_qty=source.buy_qty,
                sell_qty=source.sell_qty,
                sum_sq_qty=source.sum_sq_qty,
                buy_notional=source.buy_notional,
                sell_notional=source.sell_notional,
                notional_reason=source.notional_reason,
                low=source.low,
                low_reason=source.low_reason,
                counts=source.counts,
                extra="not admitted",
            )
        )


def test_proof_serialization_preserves_effective_wire_and_does_not_mutate_evidence() -> None:
    source = projection()
    bound = bind_flow_aggregate_evidence(
        source,
        FlowAggregateEvidenceRef(
            projection_sha256=flow_aggregate_projection_sha256(source),
            source_evidence_sha256=EvidenceLookup(source).source_identity,
        ),
    )
    validate_flow_aggregate_evidence(bound)
    lookup = EvidenceLookup(source)
    wire = flow_aggregate_evidence_wire(bound, lookup)
    assert lookup.calls == 1 and wire == lookup.wire
    wire["provider_note"] = {"unchanged": False}
    assert lookup.wire["provider_note"] == {"unchanged": True}


def test_capture_geometry_binds_actual_scope_without_dummy_financial_totals() -> None:
    from tank.analysis.condition_graph_codec import (
        bind_flow_capture_evidence,
        flow_capture_projection_sha256,
        validate_flow_capture_evidence,
    )
    from tank.analysis.condition_graph_contracts import ConditionFlowCaptureProjection, FlowCaptureEvidenceRef

    source = projection()
    geometry = ConditionFlowCaptureProjection(
        token_id=source.token_id,
        venue=source.venue,
        market_type=source.market_type,
        instrument_id=source.instrument_id,
        start=source.start,
        end=source.end,
        complete=True,
    )
    digest = flow_capture_projection_sha256(geometry)
    assert digest != flow_aggregate_projection_sha256(source)
    reference = FlowCaptureEvidenceRef(
        projection_sha256=digest, source_evidence_sha256=EvidenceLookup(source).source_identity
    )
    witness = bind_flow_capture_evidence(geometry, reference)
    validate_flow_capture_evidence(witness)
    assert witness.start == geometry.start and witness.complete is True
    with pytest.raises(ValueError, match="does not match"):
        bind_flow_capture_evidence(
            witness.model_copy(update={"end": witness.end + timedelta(seconds=1)}), reference
        )


def test_basic_spot_wire_rejoins_verified_boundary_and_keeps_unknown_count() -> None:
    from test_condition_graph_contracts import basic_spot_graph

    from tank.analysis.condition_graph_codec import encode_basic_condition_spot
    from tank.analysis.condition_graph_contracts import BasicConditionSpot, ConditionBoundaryGroup

    source = ConditionFlowAggregateProjection.model_validate(projection().model_copy(update={"venue": "A"}))
    lookup = EvidenceLookup(source)
    aggregate = bind_flow_aggregate_evidence(
        source,
        FlowAggregateEvidenceRef(
            projection_sha256=flow_aggregate_projection_sha256(source),
            source_evidence_sha256=lookup.source_identity,
        ),
    )
    base = basic_spot_graph()
    boundary = ConditionBoundaryGroup(start=aggregate.start, cutoff=aggregate.end, aggregates=(aggregate,))
    first = base.venues[0].model_copy(update={"boundary_segments": (boundary,)})
    model = BasicConditionSpot.model_validate(base.model_copy(update={"venues": (first, base.venues[1])}))
    wire = encode_basic_condition_spot(model, lookup)
    assert lookup.calls == 1
    venues = wire["venues"]
    assert isinstance(venues, list) and isinstance(venues[0], dict)
    segments = venues[0]["boundary_segments"]
    assert isinstance(segments, list) and isinstance(segments[0], dict)
    assert segments[0]["trade_count"] is None
    assert segments[0]["trade_count_reason"] == "INCOMPLETE"
    assert segments[0]["aggregates"] == [lookup.wire]
    assert wire["cw_breadth"] == {"buy": 1.0, "sell": 0.0, "neutral": 0.0, "null_reasons": {}}
    assert wire["direction"] == "BUY" and wire["composite_cohort"] is None
    assert wire["cohort_consistent"] == {} and wire["size_profile"] is None
    assert "kind" not in wire and "token_id" not in wire
    segments[0]["aggregates"] = []
    assert lookup.wire["provider_note"] == {"unchanged": True}
    assert model.venues[0].boundary_segments[0].aggregates[0] == aggregate


def test_basic_spot_wire_refuses_stale_aggregate_before_lookup() -> None:
    from test_condition_graph_contracts import basic_spot_graph

    from tank.analysis.condition_graph_codec import encode_basic_condition_spot
    from tank.analysis.condition_graph_contracts import ConditionBoundaryGroup

    source = ConditionFlowAggregateProjection.model_validate(projection().model_copy(update={"venue": "A"}))
    lookup = EvidenceLookup(source)
    bound = bind_flow_aggregate_evidence(
        source,
        FlowAggregateEvidenceRef(
            projection_sha256=flow_aggregate_projection_sha256(source),
            source_evidence_sha256=lookup.source_identity,
        ),
    )
    changed = bound.model_copy(update={"buy_qty": Decimal(11)})
    group = ConditionBoundaryGroup(start=changed.start, cutoff=changed.end, aggregates=(changed,))
    base = basic_spot_graph()
    first = base.venues[0].model_copy(update={"boundary_segments": (group,)})
    with pytest.raises(ValueError, match="does not match"):
        encode_basic_condition_spot(base.model_copy(update={"venues": (first, base.venues[1])}), lookup)
    assert lookup.calls == 0


def test_reference_close_wire_preserves_actual_unknown_precision_and_unknown_clock() -> None:
    from tank.analysis.condition_graph_codec import encode_condition_reference_close
    from tank.analysis.condition_graph_contracts import ObservedConditionClose, UnavailableConditionClose
    from tank.time_precision import Timestamp

    known = ObservedConditionClose(
        status="OBSERVED",
        reference_instrument_id="opaque-reference",
        price=Decimal("12.50"),
        observed_at=Timestamp(
            value="2026-10-17T12:00:00Z",
            precision=None,
            precision_reason="NOT_CAPTURED",
        ),
    )
    wire = encode_condition_reference_close(known)
    assert wire == {
        "ref_venue": "opaque-reference",
        "close_1h_price": "12.50",
        "close_1h_observed_at": "2026-10-17T12:00:00Z",
        "close_1h_observed_at_prec": None,
        "close_1h_observed_at_prec_reason": "NOT_CAPTURED",
    }
    unavailable = UnavailableConditionClose(
        status="UNAVAILABLE",
        reference_instrument_id=None,
        price=None,
        observed_at=Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
        reason="INCOMPLETE",
    )
    missing = encode_condition_reference_close(unavailable)
    assert missing["ref_venue"] is None and missing["close_1h_price"] is None
    assert missing["close_1h_observed_at"] is None
    assert missing["close_1h_observed_at_prec"] is None


def test_oi_wire_preserves_zero_native_end_unknown_usd_and_actual_precision() -> None:
    from test_condition_graph_contracts import observed_oi_without_usd

    from tank.analysis.condition_graph_codec import encode_condition_open_interest

    observed = observed_oi_without_usd()
    wire = encode_condition_open_interest(observed)
    assert wire["oi_native_start"] == "10" and wire["oi_native_end"] == "0"
    assert wire["oi_native_chg_pct"] == -1.0
    assert wire["oi_usd_start"] == "0" and wire["oi_usd_chg_pct"] is None
    assert wire["oi_usd_reason"] == "INCOMPLETE"
    assert wire["precision"] is None and wire["classification_ready"] is False
    assert wire["start_observed_at"] == "2026-10-17T11:00:00Z"
    assert wire["end_observed_at_prec_reason"] == "PRECISION_NOT_CAPTURED"
    assert wire["native_normal_range"] == [-0.1, 0.1]
    assert wire["usd_normal_range"] is None
    with pytest.raises(ValidationError):
        encode_condition_open_interest(observed.model_copy(update={"classification_ready": True}))


def test_funding_wire_retains_known_zero_estimate_and_does_not_invent_a_missing_kind() -> None:
    from test_condition_graph_contracts import funding_observation

    from tank.analysis.condition_graph_codec import encode_condition_funding
    from tank.analysis.condition_graph_contracts import AvailableConditionFunding, UnavailableConditionFunding
    from tank.analysis.statistics_contracts import DecimalRange

    baseline = DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY")
    known = AvailableConditionFunding(
        status="OBSERVED",
        settled=funding_observation("SETTLED", Decimal(8)),
        estimate=funding_observation("CURRENT_ESTIMATE", Decimal(1)),
        start=None,
        selected_kind="CURRENT_ESTIMATE",
        normal_range=baseline,
        funding_vs_normal=None,
    )
    wire = encode_condition_funding(known)
    assert wire["funding_8h_eq"] == "0" and wire["funding_8h_kind"] == "CURRENT_ESTIMATE"
    assert wire["funding_settled_last"] == "0" and wire["funding_estimate"] == "0"
    assert wire["funding_vs_normal"] is None and wire["funding_normal_range_reason"] == "INSUFFICIENT_HISTORY"
    assert wire["funding_estimate_observed_at_prec"] is None
    assert wire["funding_estimate_observed_at_prec_reason"] == "PRECISION_NOT_CAPTURED"
    missing = UnavailableConditionFunding(
        status="UNAVAILABLE",
        reason="INCOMPLETE",
        settled=funding_observation("SETTLED", None),
        estimate=None,
        start=None,
        normal_range=baseline,
    )
    unavailable = encode_condition_funding(missing)
    assert unavailable["funding_settled_last"] == "0"
    assert unavailable["funding_8h_kind"] is None and unavailable["funding_8h_eq"] is None
    assert unavailable["interval_h"] is None and unavailable["funding_reason"] == "INCOMPLETE"


def basic_derivatives() -> BasicConditionDerivatives:
    from test_condition_graph_contracts import AT, funding_observation, observed_oi_without_usd

    from tank.analysis.condition_graph_contracts import (
        AvailableConditionFunding,
        BasicConditionDerivatives,
        ConditionDerivativeVenue,
        ConditionInterval,
        ConditionUnavailableFundingDispersion,
        UnavailableConditionSynchronizedOpenInterest,
        UnrequestedConditionIncrementalVolumes,
        UnrequestedConditionLiquidations,
    )
    from tank.analysis.statistics_contracts import DecimalRange

    return BasicConditionDerivatives(
        token_id="T",
        kind="BASIC",
        interval=ConditionInterval(window="W1H", start=AT - timedelta(hours=1), cutoff=AT),
        per_venue=(
            ConditionDerivativeVenue(
                venue="bybit",
                instrument_id="opaque-iid",
                open_interest=observed_oi_without_usd(),
                funding=AvailableConditionFunding(
                    status="OBSERVED",
                    settled=funding_observation("SETTLED", Decimal(8)),
                    estimate=None,
                    start=None,
                    selected_kind="SETTLED",
                    normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
                    funding_vs_normal=None,
                ),
                liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
                oi_temporal_exclusions=None,
                basis_bps=None,
                basis_reason="INCOMPLETE",
            ),
        ),
        synchronized=UnavailableConditionSynchronizedOpenInterest(status="UNAVAILABLE", reason="INCOMPLETE"),
        funding_dispersion=ConditionUnavailableFundingDispersion(
            status="UNAVAILABLE", reason="INSUFFICIENT_OBSERVATIONS", observation_count=1
        ),
        liquidations=UnrequestedConditionLiquidations(status="NOT_REQUESTED"),
        incremental_volume=UnrequestedConditionIncrementalVolumes(status="NOT_REQUESTED"),
    )


def test_basic_derivative_wire_retains_flat_consumed_fields_and_explicit_unrequested_phase() -> None:
    from tank.analysis.condition_graph_codec import encode_condition_derivatives

    model = basic_derivatives()
    wire = encode_condition_derivatives(model)
    per = wire["per_venue"]
    assert isinstance(per, list) and isinstance(per[0], dict)
    assert per[0]["oi_native_end"] == "0" and per[0]["funding_8h_eq"] == "0"
    assert per[0]["coverage"] == {"venue": "bybit"}
    assert "open_interest" not in per[0] and "funding" not in per[0]
    assert "liq_long_usd" not in per[0] and "oi_temporal_exclusions" not in per[0]
    assert wire["synchronized"] is None and wire["synchronized_status"] == "UNAVAILABLE"
    assert wire["funding_dispersion_8h"] is None
    assert wire["funding_dispersion_reason"] == "INSUFFICIENT_OBSERVATIONS"
    assert wire["lead_lag"] == {
        "first_mover": "UNKNOWN",
        "spot_confirmed": None,
        "sequence": [],
        "status": "NOT_REQUESTED",
    }
    assert "liquidations" not in wire and "incremental_share" not in wire
    per[0]["oi_native_end"] = "1"
    assert model.per_venue[0].open_interest == basic_derivatives().per_venue[0].open_interest


def test_derivative_wire_does_not_erase_native_liquidations_or_invent_usd_conversion() -> None:
    from tank.analysis.condition_graph_codec import encode_condition_derivatives
    from tank.analysis.condition_graph_contracts import (
        BasicConditionDerivatives,
        ConditionAggregateLiquidations,
        ConditionCurrentLiquidationCapability,
        ConditionLiquidationCaptureWindow,
        ConditionLiquidations,
        ConditionLiquidationSide,
        ConditionLiquidationTotals,
    )
    from tank.contracts.provenance import ResolvedVersionIdentity
    from tank.time_precision import Timestamp

    base = basic_derivatives()
    missing = ConditionLiquidationTotals(
        long=None, short=None, long_reason="INCOMPLETE", short_reason="INCOMPLETE"
    )
    sampled = ConditionLiquidationTotals(
        long=None, short=Decimal(0), long_reason="INCOMPLETE", short_reason=None
    )
    capture = ConditionLiquidationCaptureWindow(
        requested=base.interval,
        start=Timestamp(value="2026-10-17T11:00:00Z", precision=None, precision_reason="NOT_CAPTURED"),
        observed_through=Timestamp(
            value="2026-10-17T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
        ),
        complete=True,
        reason=None,
    )
    rows = ConditionLiquidations(
        status="REQUESTED",
        long=ConditionLiquidationSide(
            observed_record_count=1, native=Decimal(10), usd=None, usd_reason="INCOMPLETE"
        ),
        short=ConditionLiquidationSide(
            observed_record_count=0, native=Decimal(0), usd=Decimal(0), usd_reason=None
        ),
        source_coverage="SAMPLED",
        coverage="SAMPLED",
        capture_window=capture,
        source_origin=ConditionCurrentLiquidationCapability(
            kind="CURRENT_SOURCE_CAPABILITY",
            versions=ResolvedVersionIdentity(config_version="fixture-config", code_version="fixture-code"),
        ),
    )
    venue = base.per_venue[0].model_copy(update={"liquidations": rows})
    model = BasicConditionDerivatives.model_validate(
        base.model_copy(
            update={
                "per_venue": (venue,),
                "liquidations": ConditionAggregateLiquidations(
                    status="REQUESTED", complete_venues_total=missing, sampled_venues_min=sampled
                ),
            }
        )
    )
    wire = encode_condition_derivatives(model)
    per = wire["per_venue"]
    assert isinstance(per, list) and isinstance(per[0], dict)
    assert per[0]["liq_long_native"] == "10" and per[0]["liq_long_usd"] is None
    assert per[0]["liq_long_usd_reason"] == "INCOMPLETE"
    assert per[0]["liq_short_count"] == 0 and per[0]["liq_short_usd"] == "0"
    assert per[0]["liq_coverage"] == "SAMPLED"
    totals = wire["liquidations"]
    assert isinstance(totals, dict)
    assert totals["sampled_venues_min"] == {"long": None, "short": "0"}
    capture_wire = per[0]["liq_capture_window"]
    assert isinstance(capture_wire, dict) and capture_wire["complete"] is True
    assert capture_wire["start_prec"] is None


def test_enriched_derivative_wire_preserves_unavailable_predicates_and_lead_lag_scope() -> None:
    from test_condition_graph_contracts import unknown_derivative_support

    from tank.analysis.condition_graph_codec import encode_condition_derivatives
    from tank.analysis.condition_graph_contracts import (
        ConditionUnavailableFundingDispersion,
        ConditionUnavailablePredicate,
        EnrichedConditionDerivatives,
        UnavailableConditionLeadLag,
    )
    from tank.analysis.statistics_contracts import DecimalRange

    base = basic_derivatives()
    unavailable = ConditionUnavailablePredicate(status="UNAVAILABLE", reason="INCOMPLETE")
    model = EnrichedConditionDerivatives(
        token_id=base.token_id,
        kind="ENRICHED",
        interval=base.interval,
        per_venue=(),
        synchronized=base.synchronized,
        funding_dispersion=ConditionUnavailableFundingDispersion(
            status="UNAVAILABLE", reason="INSUFFICIENT_OBSERVATIONS", observation_count=0
        ),
        liquidations=base.liquidations,
        incremental_volume=base.incremental_volume,
        funding_dispersion_normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        funding_dispersion_beyond_normal=ConditionUnavailablePredicate(
            status="UNAVAILABLE", reason="INSUFFICIENT_HISTORY"
        ),
        lead_lag=UnavailableConditionLeadLag(
            status="UNAVAILABLE",
            first_mover="UNKNOWN",
            reason="INCOMPLETE",
            interval=base.interval,
            evaluated_markets=(),
            sequence=(),
            spot_confirmation=unavailable,
        ),
        support_features=unknown_derivative_support(),
    )
    wire = encode_condition_derivatives(model)
    support = wire["support_features"]
    assert isinstance(support, dict) and support["basis_within_normal"] is None
    assert support["perp_taker_flow_buy"] is None
    reasons = support["null_reasons"]
    assert isinstance(reasons, dict) and reasons["basis_within_normal"] == "INCOMPLETE"
    lead = wire["lead_lag"]
    assert isinstance(lead, dict) and lead["window"] == "W1H"
    assert lead["start"] == "2026-10-17T11:00:00Z" and lead["cutoff"] == "2026-10-17T12:00:00Z"
    assert lead["reason"] == "INCOMPLETE" and lead["spot_confirmed"] is None
    assert wire["funding_dispersion_beyond_normal"] is None

    from tank.analysis.condition_graph_contracts import (
        ConditionLeadLagPoint,
        ConditionObservedPredicate,
        ObservedConditionLeadLag,
    )
    from tank.time_precision import Timestamp

    observed = ObservedConditionLeadLag(
        status="OBSERVED",
        first_mover="SPOT",
        interval=base.interval,
        evaluated_markets=("SPOT", "PERP"),
        spot_confirmation=ConditionObservedPredicate(status="OBSERVED", value=False),
        sequence=(
            ConditionLeadLagPoint(
                venue="a",
                market="SPOT",
                first_observed_at=Timestamp(
                    value="2026-10-17T11:10:00Z", precision=None, precision_reason="NOT_CAPTURED"
                ),
            ),
            ConditionLeadLagPoint(
                venue="b",
                market="PERP",
                first_observed_at=Timestamp(
                    value="2026-10-17T11:12:00Z", precision=None, precision_reason="NOT_CAPTURED"
                ),
            ),
        ),
    )
    detected = EnrichedConditionDerivatives.model_validate(model.model_copy(update={"lead_lag": observed}))
    known = encode_condition_derivatives(detected)["lead_lag"]
    assert isinstance(known, dict) and known["first_mover"] == "SPOT" and known["spot_confirmed"] is False
    sequence = known["sequence"]
    assert isinstance(sequence, list) and isinstance(sequence[0], dict)
    assert sequence[0]["t_first"] == "2026-10-17T11:10:00Z"
    assert sequence[0]["t_first_prec"] is None and sequence[0]["t_first_prec_reason"] == "NOT_CAPTURED"
    assert "first_observed_at" not in sequence[0]


def test_incremental_wire_preserves_observed_zero_side_and_unknown_total_separately() -> None:
    from tank.analysis.condition_graph_codec import encode_condition_derivatives
    from tank.analysis.condition_graph_contracts import (
        BasicConditionDerivatives,
        ConditionIncrementalVolumeCoverage,
        ConditionIncrementalVolumes,
        ConditionObservedIncrementalVolume,
        ConditionUnavailableIncrementalVolume,
    )

    base = basic_derivatives()
    spot = ConditionIncrementalVolumeCoverage(
        instruments=("spot-iid",), complete=True, unit="USD", reason=None
    )
    perp = ConditionIncrementalVolumeCoverage(
        instruments=("perp-iid",), complete=True, unit="USD", reason=None
    )
    observed = ConditionIncrementalVolumes(
        status="REQUESTED",
        spot_coverage=spot,
        perp_coverage=perp,
        values=ConditionObservedIncrementalVolume(
            status="OBSERVED", spot_usd=Decimal(0), perp_usd=Decimal(3), spot_share=0.0, perp_share=1.0
        ),
    )
    model = BasicConditionDerivatives.model_validate(base.model_copy(update={"incremental_volume": observed}))
    wire = encode_condition_derivatives(model)
    assert wire["incremental_share"] == {
        "spot": 0.0,
        "perp": 1.0,
        "increment_usd": {"spot": "0", "perp": "3"},
    }
    no_increment = ConditionUnavailableIncrementalVolume(
        status="UNAVAILABLE", reason="ZERO_TOTAL_VOLUME", spot_usd=Decimal(0), perp_usd=Decimal(0)
    )
    zero = ConditionIncrementalVolumes.model_validate(observed.model_copy(update={"values": no_increment}))
    zero_model = BasicConditionDerivatives.model_validate(
        base.model_copy(update={"incremental_volume": zero})
    )
    zero_wire = encode_condition_derivatives(zero_model)
    assert (
        zero_wire["incremental_share"] is None
        and zero_wire["incremental_share_reason"] == "ZERO_TOTAL_VOLUME"
    )
    assert zero_wire["incremental_volume_usd"] == {"spot": "0", "perp": "0"}


def condition_wire_clocks() -> ConditionWireClocks:
    from tank.analysis.condition_graph_contracts import ConditionWireClocks
    from tank.time_precision import Timestamp

    return ConditionWireClocks(
        token_id="T",
        cutoff=Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY", precision_reason=None),
        event_anchor=Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
    )


def test_windowed_spot_wire_preserves_actual_clock_precision_without_inventing_a_bucket() -> None:
    from test_condition_graph_contracts import windowed_spot_graph

    from tank.analysis.condition_graph_codec import encode_scoped_condition_spot
    from tank.analysis.condition_graph_contracts import WindowedConditionSpot
    from tank.time_precision import Timestamp

    base = windowed_spot_graph()
    unknown = Timestamp(value="2026-10-17T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED")
    model = WindowedConditionSpot.model_validate(base.model_copy(update={"coverage_through": unknown}))
    lookup = EvidenceLookup(projection())
    wire = encode_scoped_condition_spot(model, lookup, clocks=condition_wire_clocks())
    assert lookup.calls == 0
    assert wire["window"] == "W1H" and wire["start"] == "2026-10-17T11:00:00Z"
    assert wire["start_prec"] == "COLLECTION_ONLY" and wire["start_origin"] == "CUTOFF_OFFSET"
    assert wire["cutoff_prec"] == "COLLECTION_ONLY"
    assert wire["observed_at"] == "2026-10-17T12:00:00Z" and wire["observed_at_prec"] is None
    assert wire["observed_at_prec_reason"] == "NOT_CAPTURED" and wire["precision"] is None
    assert wire["composite_cohort"] is None and wire["composite_cohort_reason"] == "NOT_COHORT_CONSISTENT"
    assert wire["size_profile_status"] == "NOT_REQUESTED"
    with pytest.raises(ValueError, match="subject"):
        encode_scoped_condition_spot(
            model, lookup, clocks=condition_wire_clocks().model_copy(update={"token_id": "OTHER"})
        )
    wrong = condition_wire_clocks().model_copy(
        update={
            "cutoff": Timestamp(value="2026-10-17T13:00:00Z", precision=None, precision_reason="NOT_CAPTURED")
        }
    )
    with pytest.raises(ValueError, match="cutoff metadata"):
        encode_scoped_condition_spot(model, lookup, clocks=wrong)


def test_event_wire_clock_uses_actual_anchor_or_explicit_derived_origin() -> None:
    from test_condition_graph_contracts import AT

    from tank.analysis.condition_graph_contracts import ConditionInterval, ConditionWireClocks
    from tank.time_precision import Timestamp

    interval = ConditionInterval(window="EVENT", start=AT - timedelta(hours=1), cutoff=AT)
    context = condition_wire_clocks()
    derived = context.bounds(interval)
    assert derived.start_origin == "CUTOFF_OFFSET"
    anchor = Timestamp(
        value="2026-10-17T11:00:00Z", precision=None, precision_reason="ANCHOR_PRECISION_NOT_CAPTURED"
    )
    captured = ConditionWireClocks.model_validate(context.model_copy(update={"event_anchor": anchor}))
    bounds = captured.bounds(interval)
    assert bounds.start_origin == "CAPTURED_EVENT_ANCHOR" and bounds.start == anchor
    assert bounds.start.precision is None and bounds.start.precision_reason == "ANCHOR_PRECISION_NOT_CAPTURED"
    with pytest.raises(ValueError, match="captured event anchor"):
        captured.bounds(ConditionInterval(window="EVENT", start=AT - timedelta(hours=2), cutoff=AT))


def enriched_spot(
    profile: ConditionSizeProfile, role: Literal["PRIMARY", "OTHER_WINDOW"]
) -> EnrichedConditionSpot:
    from test_condition_graph_contracts import windowed_spot_graph

    from tank.analysis.condition_graph_contracts import (
        ConditionCohortComparison,
        ConditionWindow,
        EnrichedConditionSpot,
        UnavailableConditionCohortComparison,
    )

    base = windowed_spot_graph()
    comparisons: dict[ConditionWindow, ConditionCohortComparison] = {
        name: UnavailableConditionCohortComparison(
            status="UNAVAILABLE",
            cohort=(),
            share_of_max=None,
            share_majority=None,
            reason="NOT_COHORT_CONSISTENT",
        )
        for name in base.cohort_consistent
    }
    return EnrichedConditionSpot(
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
        enrichment_role=role,
        cohort_comparisons=comparisons,
        size_profile=profile,
    )


def test_enriched_spot_wire_distinguishes_observed_zero_unavailable_and_unrequested_profile() -> None:
    from tank.analysis.condition_graph_codec import encode_scoped_condition_spot
    from tank.analysis.condition_graph_contracts import (
        ObservedConditionSizeProfile,
        UnavailableConditionSizeProfile,
        UnrequestedConditionSizeProfile,
    )

    lookup = EvidenceLookup(projection())
    profile = ObservedConditionSizeProfile(
        status="OBSERVED",
        large_ticket_buy_share=1.0,
        large_ticket_sell_share=0.0,
        large_ticket_buy_majority=True,
        large_tickets_minority=True,
        large_ticket_buy_notional=Decimal(10),
        large_ticket_sell_notional=Decimal(0),
        sell_share_rising=None,
        compatible_venues=("A", "B"),
        null_reasons={"sell_share_rising": "INCOMPLETE"},
    )
    wire = encode_scoped_condition_spot(
        enriched_spot(profile, "PRIMARY"), lookup, clocks=condition_wire_clocks()
    )
    observed = wire["size_profile"]
    assert isinstance(observed, dict) and observed["large_ticket_sell_share"] == 0.0
    assert observed["large_ticket_sell_notional"] == "0" and observed["sell_share_rising"] is None
    assert observed["null_reasons"] == {"sell_share_rising": "INCOMPLETE"}
    assert wire["size_profile_status"] == "OBSERVED"
    unavailable = encode_scoped_condition_spot(
        enriched_spot(
            UnavailableConditionSizeProfile(status="UNAVAILABLE", reason="NOT_SUPPORTED"), "PRIMARY"
        ),
        lookup,
        clocks=condition_wire_clocks(),
    )
    assert unavailable["size_profile"] is None and unavailable["size_profile_reason"] == "NOT_SUPPORTED"
    unrequested = encode_scoped_condition_spot(
        enriched_spot(UnrequestedConditionSizeProfile(status="NOT_REQUESTED"), "OTHER_WINDOW"),
        lookup,
        clocks=condition_wire_clocks(),
    )
    assert unrequested["size_profile"] is None and unrequested["size_profile_status"] == "NOT_REQUESTED"
    assert "size_profile_reason" not in unrequested


def test_enriched_spot_wire_preserves_actual_shared_cohorts_and_unknown_prior_velocity() -> None:
    from tank.analysis.condition_graph_codec import encode_scoped_condition_spot
    from tank.analysis.condition_graph_contracts import (
        EnrichedConditionSpot,
        ObservedConditionCohort,
        ObservedConditionCohortComparison,
        ObservedConditionComposite,
        UnavailableConditionSizeProfile,
    )

    base = enriched_spot(
        UnavailableConditionSizeProfile(status="UNAVAILABLE", reason="INCOMPLETE"), "PRIMARY"
    )
    composite = base.composite_max
    assert isinstance(composite, ObservedConditionComposite)
    cohort = ObservedConditionCohort(
        status="OBSERVED",
        cvd=composite.cvd,
        volume=composite.volume,
        buy_share=composite.buy_share,
        bs_ratio=composite.bs_ratio,
        venues=composite.venues,
        sign=composite.sign,
        null_reasons=composite.null_reasons,
        cohort=composite.venues,
        share_of_max=1.0,
        share_majority=True,
        velocity=None,
        velocity_reason="INCOMPLETE",
    )
    comparison = ObservedConditionCohortComparison(
        status="OBSERVED",
        cohort=composite.venues,
        current=composite,
        other=composite,
        share_of_max=1.0,
        share_majority=True,
    )
    model = EnrichedConditionSpot.model_validate(
        base.model_copy(
            update={
                "composite_cohort": cohort,
                "cohort_consistent": {name: True for name in base.cohort_consistent},
                "cohort_comparisons": {name: comparison for name in base.cohort_consistent},
            }
        )
    )
    wire = encode_scoped_condition_spot(model, EvidenceLookup(projection()), clocks=condition_wire_clocks())
    common = wire["composite_cohort"]
    assert isinstance(common, dict) and common["volume"] == "30" and common["cohort"] == ["A", "B"]
    assert common["velocity"] is None and common["velocity_reason"] == "INCOMPLETE"
    assert common["reason"] is None and "status" not in common
    comparisons = wire["cohort_comparisons"]
    assert isinstance(comparisons, dict) and isinstance(comparisons["W15M"], dict)
    other = comparisons["W15M"]["other"]
    assert isinstance(other, dict) and other["volume"] == "30" and "status" not in other
    assert comparisons["W15M"]["reason"] is None
