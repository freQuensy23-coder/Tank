from collections.abc import Mapping
from copy import deepcopy
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
    CapturedConditionLending,
    ConditionFlowAggregateProjection,
    ConditionGeneratedEmptyFlowProjection,
    ConditionReferenceTradeEvidenceRef,
    ConditionReferenceTradeProjection,
    ConditionScheduledUnlock,
    ConditionSizeProfile,
    ConditionWalletFlow,
    ConditionWalletFlowEvidenceRef,
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


class ReferenceTradeEvidence:
    def __init__(self) -> None:
        self.calls = 0
        self.wire: dict[str, JsonValue] = {
            "token_id": "T",
            "instrument_id": "opaque-spot-iid",
            "market_type": "SPOT",
            "p": "12.50",
            "ts": "2026-10-17T10:59:00Z",
            "provider_note": {"exact": "retained"},
        }
        self.original = deepcopy(self.wire)

    def source_digest(self) -> str:
        return sha256(
            {"schema": "fixture.storage-read-reference.v1", "original": self.original, "effective": self.wire}
        )

    def projection(self) -> ConditionReferenceTradeProjection:
        from tank.time_precision import Timestamp

        token, instrument, price, stamp = (
            self.wire[name] for name in ("token_id", "instrument_id", "p", "ts")
        )
        if (
            not isinstance(token, str)
            or not isinstance(instrument, str)
            or not isinstance(price, str)
            or not isinstance(stamp, str)
            or self.wire["market_type"] != "SPOT"
        ):
            raise ValueError("Invalid fixture storage-read trade")
        return ConditionReferenceTradeProjection(
            token_id=token,
            instrument_id=instrument,
            market_type="SPOT",
            source="LAST_TRADE",
            price=Decimal(price),
            observed_at=Timestamp(value=stamp, precision=None, precision_reason="NOT_CAPTURED"),
        )

    def resolve_reference_trade(
        self, reference: ConditionReferenceTradeEvidenceRef, *, expected: ConditionReferenceTradeProjection
    ) -> Mapping[str, JsonValue]:
        from tank.analysis.condition_graph_codec import reference_trade_projection_sha256

        self.calls += 1
        if (
            reference.source_evidence_sha256 != self.source_digest()
            or self.projection() != expected
            or reference.projection_sha256 != reference_trade_projection_sha256(expected)
        ):
            raise ValueError("Reference source evidence mismatch")
        return self.wire


def test_reference_trade_wire_binds_source_descriptor_and_actual_projection_before_rejoin() -> None:
    from tank.analysis.condition_graph_codec import (
        reference_trade_evidence_wire,
        reference_trade_projection_sha256,
    )

    source = ReferenceTradeEvidence()
    projection = source.projection()
    reference = ConditionReferenceTradeEvidenceRef(
        projection_sha256=reference_trade_projection_sha256(projection),
        source_evidence_sha256=source.source_digest(),
    )
    wire = reference_trade_evidence_wire(projection, reference, source)
    assert source.calls == 1 and wire == source.wire
    wire["provider_note"] = {"exact": "changed"}
    assert source.wire["provider_note"] == {"exact": "retained"}
    with pytest.raises(ValueError, match="declared projection"):
        reference_trade_evidence_wire(projection.model_copy(update={"token_id": "OTHER"}), reference, source)
    assert source.calls == 1
    source.wire["p"] = "13.50"
    with pytest.raises(ValueError, match="source evidence mismatch"):
        reference_trade_evidence_wire(projection, reference, source)
    assert source.original["p"] == "12.50"


class UnrequestedRelativeEvidence:
    def resolve_flow_aggregate(
        self, reference: FlowAggregateEvidenceRef, *, expected: ConditionFlowAggregateProjection
    ) -> Mapping[str, JsonValue]:
        raise AssertionError("No flow evidence is declared by this unavailable reference fixture")

    def resolve_reference_trade(
        self, reference: ConditionReferenceTradeEvidenceRef, *, expected: ConditionReferenceTradeProjection
    ) -> Mapping[str, JsonValue]:
        raise AssertionError("No anchor trade is declared by this unavailable reference fixture")


def test_relative_wire_preserves_analysis_origin_unknown_precision_and_unavailable_beta() -> None:
    from test_condition_graph_contracts import relative_graph_without_event

    from tank.analysis.condition_graph_codec import encode_condition_relative

    model = relative_graph_without_event()
    wire = encode_condition_relative(model, UnrequestedRelativeEvidence(), clocks=condition_wire_clocks())
    assert wire["expected_direction"] is None and wire["repricing_sign"] is None
    assert wire["beta_btc"] is None and wire["beta_eth"] is None
    assert wire["repricing_band"] is None and wire["repricing_reason"] == "NO_EVENT_ANCHOR"
    anchor = wire["anchor"]
    assert isinstance(anchor, dict)
    assert anchor["type"] == "ANALYSIS_WINDOW_START" and anchor["start_origin"] == "CUTOFF_OFFSET"
    assert anchor["ts"] == "2026-10-17T08:00:00Z"
    assert anchor["ts_prec"] == "COLLECTION_ONLY" and anchor["evidence"] is None
    runup = wire["pre_event_runup"]
    assert isinstance(runup, dict)
    assert runup["window"] == "PRE_ANALYSIS_W24H" and runup["anchor_kind"] == "ANALYSIS_WINDOW_START"
    assert runup["start"] == "2026-10-16T08:00:00Z" and runup["cutoff"] == anchor["ts"]
    assert wire["entry_price_observed_at_prec"] is None
    assert wire["entry_price_observed_at_prec_reason"] == "PRECISION_NOT_CAPTURED"
    context = wire["context_inputs"]
    assert isinstance(context, dict)
    assert context["rv24_observed_cutoff"] is None
    assert context["rv24_observed_cutoff_prec_reason"] == "INCOMPLETE"
    assert context["rv24_bound"] == "2026-10-17T12:00:00Z"


def test_relative_wire_refuses_borrowed_subject_or_event_clock_context() -> None:
    from test_condition_graph_contracts import relative_graph_without_event

    from tank.analysis.condition_graph_codec import encode_condition_relative
    from tank.time_precision import Timestamp

    model = relative_graph_without_event()
    context = condition_wire_clocks()
    with pytest.raises(ValueError, match="another subject"):
        encode_condition_relative(
            model, UnrequestedRelativeEvidence(), clocks=context.model_copy(update={"token_id": "OTHER"})
        )
    with pytest.raises(ValueError):
        encode_condition_relative(
            model,
            UnrequestedRelativeEvidence(),
            clocks=context.model_copy(
                update={"event_anchor": Timestamp(value="2026-10-17T08:00:00Z", precision="EXACT")}
            ),
        )


def generated_empty_projection() -> ConditionGeneratedEmptyFlowProjection:
    from tank.analysis.condition_graph_contracts import ConditionGeneratedEmptyFlowProjection
    from tank.market.trade_contracts import EmptyBoundarySegment

    end = datetime(2026, 10, 17, 12, tzinfo=UTC)
    return ConditionGeneratedEmptyFlowProjection(
        token_id="T",
        venue="a",
        market_type="SPOT",
        segment=EmptyBoundarySegment(
            instrument_id="fixture-instrument",
            start=end - timedelta(seconds=30),
            start_prec="EXACT",
            start_prec_reason=None,
            end=end,
            end_prec="EXACT",
            end_prec_reason=None,
            complete=False,
            observed_at=end + timedelta(seconds=1),
            observed_at_prec="COLLECTION_ONLY",
            observed_at_prec_reason=None,
            coverage_reason="INCOMPLETE",
            buy_qty=Decimal(0),
            sell_qty=Decimal(0),
            buy_notional=None,
            sell_notional=None,
            n_buy=0,
            n_sell=0,
            sum_sq_qty=Decimal(0),
            open=None,
            high=None,
            low=None,
            close=None,
            vwap=None,
            size_buckets=None,
            first_trade=None,
            last_trade=None,
        ),
    )


def test_generated_empty_visibility_is_independent_of_capture_completeness_and_receipt() -> None:
    from tank.analysis.condition_graph_codec import (
        generated_empty_flow_projection_sha256,
        validate_generated_empty_flow_evidence,
    )
    from tank.analysis.condition_graph_contracts import (
        ConditionGeneratedEmptyFlowVisibility,
        FlowCaptureEvidenceRef,
    )
    from tank.time_precision import Timestamp

    projection = generated_empty_projection()
    observed = ConditionGeneratedEmptyFlowVisibility(
        token_id=projection.token_id,
        venue=projection.venue,
        market_type=projection.market_type,
        segment=projection.segment,
        basis="GENERATED_EMPTY_PIECE",
        assembled_at=Timestamp(value="2026-10-17T12:00:02Z", precision="COLLECTION_ONLY"),
        evidence=FlowCaptureEvidenceRef(
            projection_sha256=generated_empty_flow_projection_sha256(projection),
            source_evidence_sha256=sha256({"fixture": "actual-generated-empty-source"}),
        ),
    )
    validate_generated_empty_flow_evidence(observed)
    result = observed.eligible()
    assert result.status == "OBSERVED" and result.value is True
    assert not observed.segment.complete
    future = observed.model_copy(
        update={"assembled_at": Timestamp(value="2026-10-17T12:00:00Z", precision="COLLECTION_ONLY")}
    )
    result = future.eligible()
    assert result.status == "OBSERVED" and result.value is False
    modified = observed.model_copy(update={"venue": "OTHER"})
    with pytest.raises(ValueError, match="does not match"):
        validate_generated_empty_flow_evidence(modified)
    with pytest.raises(ValidationError):
        type(projection).model_validate(
            projection.model_copy(
                update={"segment": projection.segment.model_copy(update={"observed_at_prec": "EXACT"})}
            )
        )


def test_generated_visibility_cannot_authorize_an_incomplete_or_different_capture_piece() -> None:
    from test_condition_graph_contracts import capture_witness

    from tank.analysis.condition_graph_codec import generated_empty_flow_projection_sha256
    from tank.analysis.condition_graph_contracts import (
        ConditionFlowCapturePiece,
        ConditionGeneratedEmptyFlowVisibility,
        FlowCaptureEvidenceRef,
    )
    from tank.time_precision import Timestamp

    projection = generated_empty_projection()
    witness = capture_witness(
        projection.segment.instrument_id,
        projection.venue,
        projection.market_type,
        projection.segment.start,
        projection.segment.end,
    )
    visibility = ConditionGeneratedEmptyFlowVisibility(
        token_id=projection.token_id,
        venue=projection.venue,
        market_type=projection.market_type,
        segment=projection.segment,
        basis="GENERATED_EMPTY_PIECE",
        assembled_at=Timestamp(value="2026-10-17T12:00:02Z", precision="COLLECTION_ONLY"),
        evidence=FlowCaptureEvidenceRef(
            projection_sha256=generated_empty_flow_projection_sha256(projection),
            source_evidence_sha256=witness.evidence.source_evidence_sha256,
        ),
    )
    with pytest.raises(ValidationError, match="exact complete scoped source piece"):
        ConditionFlowCapturePiece(basis="BOUNDARY", aggregate=witness, visibility=visibility)
    complete_segment = type(projection.segment).model_validate(
        projection.segment.model_copy(update={"complete": True, "coverage_reason": None})
    )
    complete_projection = type(projection).model_validate(
        projection.model_copy(update={"segment": complete_segment})
    )
    visibility = type(visibility).model_validate(
        visibility.model_copy(
            update={
                "segment": complete_segment,
                "evidence": FlowCaptureEvidenceRef(
                    projection_sha256=generated_empty_flow_projection_sha256(complete_projection),
                    source_evidence_sha256=witness.evidence.source_evidence_sha256,
                ),
            }
        )
    )
    piece = ConditionFlowCapturePiece(basis="BOUNDARY", aggregate=witness, visibility=visibility)
    result = piece.visibility.eligible()
    assert result.status == "OBSERVED" and result.value is True
    for invalid in (
        visibility.model_copy(update={"venue": "OTHER"}),
        visibility.model_copy(
            update={"evidence": visibility.evidence.model_copy(update={"source_evidence_sha256": "0" * 64})}
        ),
    ):
        with pytest.raises(ValidationError, match="exact complete scoped source piece"):
            ConditionFlowCapturePiece(basis="BOUNDARY", aggregate=witness, visibility=invalid)


def test_confidence_wire_preserves_required_failure_and_qualified_cadence_without_rule_guess() -> None:
    from test_condition_graph_contracts import selective_confidence_graph

    from tank.analysis.condition_graph_codec import encode_condition_confidence
    from tank.analysis.condition_graph_contracts import SelectiveConditionConfidence

    graph = selective_confidence_graph("T")
    confidence = graph.confidence
    assert isinstance(confidence, SelectiveConditionConfidence)
    wire = encode_condition_confidence(confidence, clocks=condition_wire_clocks())
    assert wire["class"] == "MEDIUM"
    assert wire["derivatives_classification_ready"] is None
    assert wire["derivatives_classification_ready_reason"] == "NOT_ESTIMABLE"
    failures = wire["failed"]
    assert isinstance(failures, list) and len(failures) == 1
    failure = failures[0]
    assert isinstance(failure, dict)
    assert failure["instrument_id"] == "venue:spot:T" and failure["metric_family"] == "depth"
    assert failure["cadence_s"] == 10.0 and failure["cadence_source"] is None
    assert failure["cadence_source_reason"] == "RULE_LABEL_NOT_CAPTURED"
    origin = failure["cadence_origin"]
    assert isinstance(origin, dict)
    assert origin["kind"] == "DECLARED_METADATA"
    assert origin["declaration_identity"] == {
        "status": "UNRECORDED",
        "reason": "DECLARATION_IDENTITY_NOT_CAPTURED",
    }
    assert wire["limiting"] == failure
    window = wire["analysis_window"]
    assert isinstance(window, dict)
    assert window["start"] == "2026-10-17T08:00:00Z" and window["start_prec"] == "COLLECTION_ONLY"
    with pytest.raises(ValueError, match="another subject"):
        encode_condition_confidence(
            confidence, clocks=condition_wire_clocks().model_copy(update={"token_id": "OTHER"})
        )


def test_confidence_wire_refuses_an_age_borrowed_from_another_cutoff() -> None:
    from test_condition_graph_contracts import scoped_field_observation

    from tank.analysis.condition_graph_codec import encode_condition_confidence
    from tank.analysis.condition_graph_contracts import FullConditionConfidence
    from tank.time_precision import Timestamp

    confidence = FullConditionConfidence(
        kind="FULL",
        classification="HIGH",
        failed=(),
        limiting=None,
        derivatives_native_coverage_ready=True,
        derivatives_classification_ready=True,
        freshness=(scoped_field_observation("INSTRUMENT"),),
    )
    wire = encode_condition_confidence(confidence, clocks=condition_wire_clocks())
    assert wire["limiting"] is None and wire["failed"] == []
    observations = wire["freshness"]
    assert isinstance(observations, list) and len(observations) == 1
    observation = observations[0]
    assert isinstance(observation, dict)
    assert observation["age_s"] == 5.0 and observation["observed_at_prec"] == "EXACT"
    with pytest.raises(ValueError, match="observation age and cutoff"):
        encode_condition_confidence(
            confidence,
            clocks=condition_wire_clocks().model_copy(
                update={"cutoff": Timestamp(value="2026-10-17T12:01:00Z", precision="COLLECTION_ONLY")}
            ),
        )


def test_depth_wire_preserves_source_precision_and_exact_usd_amounts() -> None:
    from test_condition_graph_contracts import AT, declared_book_cadence

    from tank.analysis.condition_graph_codec import encode_condition_depth
    from tank.analysis.condition_graph_contracts import ConditionDepthNode, ConditionDepthObservation
    from tank.time_precision import Timestamp

    depth = ConditionDepthNode(
        aggregate_depth_p1_usd=Decimal("10000000000000000000000000000000001"),
        pre_event_depth_p1_usd=Decimal(0),
        spread_ratio=None,
        cutoff=AT,
        observations=(
            ConditionDepthObservation(
                token_id="T",
                instrument_id="venue:spot:T",
                metric_key=None,
                venue="venue",
                observed_at=Timestamp(
                    value="2026-10-17T11:59:55Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
                ),
                cadence=declared_book_cadence(),
                fresh=True,
            ),
        ),
    )
    wire = encode_condition_depth(depth, clocks=condition_wire_clocks())
    assert wire["aggregate_depth_p1_usd"] == "10000000000000000000000000000000001"
    assert wire["pre_event_depth_p1_usd"] == "0" and wire["spread_ratio"] is None
    observations = wire["observations"]
    assert isinstance(observations, list) and len(observations) == 1
    observation = observations[0]
    assert isinstance(observation, dict)
    assert observation["observed_at_prec"] is None
    assert observation["observed_at_prec_reason"] == "PRECISION_NOT_CAPTURED"
    with pytest.raises(ValueError, match="another subject"):
        encode_condition_depth(depth, clocks=condition_wire_clocks().model_copy(update={"token_id": "OTHER"}))
    with pytest.raises(ValueError, match="selected cutoff"):
        encode_condition_depth(
            depth,
            clocks=condition_wire_clocks().model_copy(
                update={"cutoff": Timestamp(value="2026-10-17T12:01:00Z", precision="COLLECTION_ONLY")}
            ),
        )


class SupplyWireEvidence:
    def __init__(self, flow: ConditionWalletFlow, unlock: ConditionScheduledUnlock) -> None:
        self.flow = flow
        self.unlock = unlock
        self.flow_calls = 0
        self.unlock_calls = 0
        self.flow_wire: dict[str, JsonValue] = {
            "token_id": flow.token_id,
            "classification": flow.classification,
            "qty_native": str(flow.quantity_native),
            "reaches_exchange": flow.reaches_exchange,
            "source_note": {"exact_extension": ["retained"]},
        }
        self.unlock_wire: dict[str, JsonValue] = {
            "token_id": unlock.token_id,
            "source_note": {"exact_extension": ["retained"]},
        }

    def resolve_wallet_flow(
        self, reference: ConditionWalletFlowEvidenceRef, *, expected: ConditionWalletFlow
    ) -> Mapping[str, JsonValue]:
        self.flow_calls += 1
        if reference != self.flow.evidence or expected != self.flow:
            raise ValueError("Captured wallet source does not reproject to the requested facts")
        return self.flow_wire

    def resolve_unlock(self, *, expected: ConditionScheduledUnlock) -> Mapping[str, JsonValue]:
        self.unlock_calls += 1
        if expected != self.unlock:
            raise ValueError("Captured unlock source does not reproject to the requested facts")
        return self.unlock_wire


def test_supply_wire_preserves_extensions_explicit_absence_and_potential_supply_semantics() -> None:
    from test_condition_graph_contracts import unlock_schedule, wallet_flows

    from tank.analysis.condition_graph_codec import encode_condition_unlocks, encode_condition_wallet_flows

    flows, unlocks = wallet_flows(), unlock_schedule()
    evidence = SupplyWireEvidence(flows.flows[0], unlocks.upcoming[0])
    flow_source_before, unlock_source_before = deepcopy(evidence.flow_wire), deepcopy(evidence.unlock_wire)
    flow_wire = encode_condition_wallet_flows(flows, evidence)
    assert flow_wire["coverage"] == {"status": "UNAVAILABLE", "reason": "QUERY_SCOPE_NOT_CAPTURED"}
    observation = flow_wire["observation"]
    assert isinstance(observation, dict)
    assert observation["status"] == "NO_POSITIVE_OBSERVATION" and observation["observation_count"] == 1
    records = flow_wire["flows"]
    assert isinstance(records, list) and len(records) == 1
    flow = records[0]
    assert isinstance(flow, dict)
    assert flow["class"] == "AMBIGUOUS" and flow["qty"] == "10"
    assert flow["source_note"] == evidence.flow_wire["source_note"]
    assert flow["ts"] == flows.flows[0].observed_at.value and flow["ts_prec"] == "EXACT"
    unlock_wire = encode_condition_unlocks(unlocks, evidence)
    assert unlock_wire["schedule_available"] is True and unlock_wire["schedule_reason"] is None
    schedules = unlock_wire["unlocks_next_14d"]
    assert isinstance(schedules, list) and len(schedules) == 1
    unlock = schedules[0]
    assert isinstance(unlock, dict)
    assert unlock["date"] == unlocks.upcoming[0].scheduled_at.value
    assert unlock["date_prec"] is None and unlock["date_prec_reason"] == "NOT_CAPTURED"
    assert unlock["pct_circulating"] is None and unlock["pct_circulating_unit"] is None
    assert unlock["label"] is None and unlock["transferable"] is True
    assert unlock["inference"] == "POTENTIAL_SUPPLY_IS_NOT_A_SALE"
    assert unlock["source_note"] == evidence.unlock_wire["source_note"]
    for record in (flow, unlock):
        note = record["source_note"]
        assert isinstance(note, dict)
        extension = note["exact_extension"]
        assert isinstance(extension, list)
        extension.append("changed")
    assert evidence.flow_wire == flow_source_before and evidence.unlock_wire == unlock_source_before
    assert evidence.flow_calls == evidence.unlock_calls == 1


def test_future_received_supply_records_do_not_rejoin_original_evidence_as_available() -> None:
    from test_condition_graph_contracts import unlock_schedule, wallet_flows

    from tank.analysis.condition_graph_codec import encode_condition_unlocks, encode_condition_wallet_flows
    from tank.time_precision import Timestamp

    flows, unlocks = wallet_flows(), unlock_schedule()
    evidence = SupplyWireEvidence(flows.flows[0], unlocks.upcoming[0])
    future_visibility = flows.query_visibility.model_copy(
        update={"received_at": Timestamp(value="2026-10-17T12:00:01Z", precision="COLLECTION_ONLY")}
    )
    later_flows = type(flows).model_validate(
        flows.model_copy(
            update={"flows": (flows.flows[0].model_copy(update={"visibility": future_visibility}),)}
        )
    )
    later_unlocks = type(unlocks).model_validate(unlocks.model_copy(update={"visibility": future_visibility}))
    assert encode_condition_wallet_flows(later_flows, evidence)["flows"] == []
    result = encode_condition_unlocks(later_unlocks, evidence)
    assert result["unlocks_next_14d"] == [] and result["schedule_available"] is False
    assert result["schedule_reason"] == "RECEIVED_AFTER_ASSEMBLY"
    assert evidence.flow_calls == evidence.unlock_calls == 0


def test_supply_wire_propagates_source_reprojection_refusal_without_filling_expected_facts() -> None:
    from test_condition_graph_contracts import unlock_schedule, wallet_flows

    from tank.analysis.condition_graph_codec import encode_condition_unlocks, encode_condition_wallet_flows

    flows, unlocks = wallet_flows(), unlock_schedule()
    evidence = SupplyWireEvidence(flows.flows[0], unlocks.upcoming[0])
    changed_flows = type(flows).model_validate(
        flows.model_copy(
            update={"flows": (flows.flows[0].model_copy(update={"quantity_native": Decimal(11)}),)}
        )
    )
    with pytest.raises(ValueError, match="does not reproject"):
        encode_condition_wallet_flows(changed_flows, evidence)
    changed_unlocks = type(unlocks).model_validate(
        unlocks.model_copy(
            update={"upcoming": (unlocks.upcoming[0].model_copy(update={"schedule_index": 1}),)}
        )
    )
    with pytest.raises(ValueError, match="does not reproject"):
        encode_condition_unlocks(changed_unlocks, evidence)


def captured_lending() -> CapturedConditionLending:
    from test_condition_graph_contracts import AT, lending_pool, lending_storage_observation, wallet_flows

    from tank.time_precision import Timestamp

    visibility = wallet_flows().query_visibility
    return CapturedConditionLending(
        status="CAPTURED_POOLS",
        token_id="T",
        requested_window="W24H",
        cutoff=AT,
        observed_at=Timestamp(value="2026-10-17T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"),
        previous_observed_at=Timestamp(
            value="2026-10-16T12:00:00Z", precision=None, precision_reason="NOT_CAPTURED"
        ),
        source_observation_sha256=sha256(lending_storage_observation("2026-10-17T12:00:00Z")),
        previous_source_observation_sha256=sha256(lending_storage_observation("2026-10-16T12:00:00Z")),
        current_visibility=visibility,
        previous_visibility=visibility,
        pools=(lending_pool(),),
        unmapped_pool_ids=(),
    )


class LendingWireEvidence:
    def __init__(self, expected: CapturedConditionLending) -> None:
        from test_condition_graph_contracts import lending_storage_observation

        self.expected = expected
        self.wire = lending_storage_observation("2026-10-17T12:00:00Z")
        self.calls = 0

    def resolve_lending_observation(self, *, expected: CapturedConditionLending) -> Mapping[str, JsonValue]:
        self.calls += 1
        if expected != self.expected or expected.source_observation_sha256 != sha256(self.wire):
            raise ValueError("Captured lending observation does not match its source witness")
        return self.wire


def test_lending_wire_retains_provider_apy_lexical_form_and_never_infers_apr() -> None:
    from tank.analysis.condition_graph_codec import encode_condition_lending

    model = captured_lending()
    evidence = LendingWireEvidence(model)
    original = deepcopy(evidence.wire)
    wire = encode_condition_lending(model, evidence)
    pools = wire["pools"]
    assert isinstance(pools, list) and len(pools) == 1
    pool = pools[0]
    assert isinstance(pool, dict)
    assert pool["borrow_apy"] == "7.00" and pool["total_supply_usd"] == "100"
    assert pool["utilization"] == "0" and pool["chg_24h"] == 0.0
    assert pool["borrow_apr"] is None and pool["borrow_apr_reason"] == "NOT_ESTIMABLE"
    assert pool["borrow_apr_policy_reason"] == "APY_IS_NOT_APR"
    assert wire["observed_at_prec"] is None and wire["observed_at_prec_reason"] == "NOT_CAPTURED"
    assert wire["source_observation_sha256"] == sha256(original)
    pool["borrow_apy"] = "changed"
    assert evidence.wire == original and evidence.calls == 1


def test_lending_wire_does_not_expose_a_future_received_prior_comparison() -> None:
    from tank.analysis.condition_graph_codec import encode_condition_lending
    from tank.time_precision import Timestamp

    model = captured_lending()
    prior_future = type(model).model_validate(
        model.model_copy(
            update={
                "previous_visibility": model.previous_visibility.model_copy(
                    update={
                        "received_at": Timestamp(value="2026-10-17T12:00:01Z", precision="COLLECTION_ONLY")
                    }
                )
            }
        )
    )
    wire = encode_condition_lending(prior_future, LendingWireEvidence(prior_future))
    pools = wire["pools"]
    assert isinstance(pools, list)
    pool = pools[0]
    assert isinstance(pool, dict)
    assert pool["utilization"] == "0" and pool["chg_24h"] is None
    assert pool["chg_24h_reason"] == "PRIOR_RECEIVED_AFTER_ASSEMBLY"
    current_future = type(model).model_validate(
        model.model_copy(update={"current_visibility": prior_future.previous_visibility})
    )
    evidence = LendingWireEvidence(current_future)
    assert encode_condition_lending(current_future, evidence) == {
        "status": "UNAVAILABLE",
        "window": "W24H",
        "reason": "RECEIVED_AFTER_ASSEMBLY",
    }
    assert evidence.calls == 0


def test_lending_wire_requires_the_exact_current_and_prior_source_witnesses() -> None:
    from tank.analysis.condition_graph_codec import encode_condition_lending

    model = captured_lending()
    evidence = LendingWireEvidence(model)
    wrong_source = type(model).model_validate(
        model.model_copy(update={"source_observation_sha256": "0" * 64})
    )
    with pytest.raises(ValueError, match="source witness"):
        encode_condition_lending(wrong_source, evidence)
    with pytest.raises(ValidationError, match="actual prior observation presence"):
        type(model).model_validate(model.model_copy(update={"previous_source_observation_sha256": None}))


def test_current_lending_observation_does_not_invent_a_missing_prior_row_or_receipt() -> None:
    from tank.analysis.condition_graph_codec import encode_condition_lending
    from tank.time_precision import Timestamp

    model = captured_lending()
    no_prior_clock = Timestamp(value=None, precision=None, precision_reason="PRIOR_OBSERVATION_NOT_CAPTURED")
    no_prior = type(model).model_validate(
        model.model_copy(
            update={
                "previous_observed_at": no_prior_clock,
                "previous_source_observation_sha256": None,
                "previous_visibility": model.previous_visibility.model_copy(
                    update={"received_at": no_prior_clock}
                ),
                "pools": (model.pools[0].model_copy(update={"previous_utilization": None}),),
            }
        )
    )
    wire = encode_condition_lending(no_prior, LendingWireEvidence(no_prior))
    pools = wire["pools"]
    assert isinstance(pools, list)
    pool = pools[0]
    assert isinstance(pool, dict)
    assert pool["utilization"] == "0" and pool["chg_24h"] is None
    assert pool["chg_24h_reason"] == "PRIOR_OBSERVATION_NOT_CAPTURED"
    assert wire["previous_source_observation_sha256"] is None
    assert wire["previous_observed_at"] is None
    with pytest.raises(ValidationError, match="absent prior.*source receipt"):
        type(no_prior).model_validate(
            no_prior.model_copy(update={"previous_visibility": model.previous_visibility})
        )


def test_lending_wire_does_not_match_a_pool_by_position_or_duplicate_identity() -> None:
    from tank.analysis.condition_graph_codec import encode_condition_lending

    model = captured_lending()
    evidence = LendingWireEvidence(model)
    pools = evidence.wire["pools"]
    assert isinstance(pools, list)
    pools.append(deepcopy(pools[0]))
    duplicated = type(model).model_validate(
        model.model_copy(update={"source_observation_sha256": sha256(evidence.wire)})
    )
    evidence.expected = duplicated
    with pytest.raises(ValueError, match="distinct actual identities"):
        encode_condition_lending(duplicated, evidence)


def test_inventory_wire_keeps_exact_native_changes_and_does_not_invent_a_float_zero() -> None:
    from test_condition_graph_contracts import inventory_comparison

    from tank.analysis.condition_graph_codec import encode_condition_exchange_inventory

    previous = Decimal("1" + "0" * 1001)
    current = Decimal("1" + "0" * 1000 + "1")
    model = inventory_comparison(current, previous)
    wire = encode_condition_exchange_inventory(model, clocks=condition_wire_clocks())
    assert wire["chg_native_24h"] == "1"
    assert wire["chg_pct"] is None and wire["chg_pct_reason"] == "NOT_ESTIMABLE"
    assert wire["observed_at_prec"] is None and wire["observed_at_prec_reason"] == "NOT_CAPTURED"
    balances = wire["per_venue"]
    assert isinstance(balances, list) and len(balances) == 1
    balance = balances[0]
    assert isinstance(balance, dict)
    assert balance["qty_native"] == str(current) and balance["chg_native_24h"] == "1"
    assert balance["chg_pct"] is None and balance["chg_pct_reason"] == "NOT_ESTIMABLE"
    assert balance["contract"] == "token-contract" and balance["wallet_list_commit"] == "captured-list-1"
    zero = encode_condition_exchange_inventory(
        inventory_comparison(Decimal(0), Decimal(0)), clocks=condition_wire_clocks()
    )
    assert zero["chg_native_24h"] == "0" and zero["chg_pct"] is None
    assert zero["chg_pct_reason"] == "ZERO_BASELINE"


def test_inventory_wire_keeps_current_quantity_without_a_future_prior_comparison() -> None:
    from test_condition_graph_contracts import inventory_comparison

    from tank.analysis.condition_graph_codec import encode_condition_exchange_inventory
    from tank.time_precision import Timestamp

    model = inventory_comparison(Decimal(12), Decimal(10))
    prior_future = type(model).model_validate(
        model.model_copy(
            update={
                "previous_visibility": model.previous_visibility.model_copy(
                    update={
                        "received_at": Timestamp(value="2026-10-17T12:00:01Z", precision="COLLECTION_ONLY")
                    }
                )
            }
        )
    )
    wire = encode_condition_exchange_inventory(prior_future, clocks=condition_wire_clocks())
    assert wire["chg_native_24h"] is None and wire["chg_pct"] is None
    assert wire["reason"] == "PRIOR_RECEIVED_AFTER_ASSEMBLY"
    balances = wire["per_venue"]
    assert isinstance(balances, list)
    balance = balances[0]
    assert isinstance(balance, dict)
    assert balance["qty_native"] == "12" and balance["chg_native_24h"] is None
    current_future = type(model).model_validate(
        model.model_copy(update={"current_visibility": prior_future.previous_visibility})
    )
    assert encode_condition_exchange_inventory(current_future, clocks=condition_wire_clocks()) == {
        "value": None,
        "reason": "RECEIVED_AFTER_ASSEMBLY",
        "window": "W24H",
    }
    with pytest.raises(ValueError, match="actual token and cutoff"):
        encode_condition_exchange_inventory(
            model, clocks=condition_wire_clocks().model_copy(update={"token_id": "OTHER"})
        )
