from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_derivative_input_codec import decode_oi_inputs, resolve_oi_observation
from tank.analysis.condition_derivative_inputs import OIReadSnapshot
from tank.util import JsonInput


def instrument() -> dict[str, JsonInput]:
    return {
        "instrument_id": "venue:perp:T",
        "token_id": "T",
        "venue": "venue",
        "market_type": "PERP_LINEAR",
        "quote_asset": "USDT",
    }


def observation() -> dict[str, JsonInput]:
    return {
        "instrument_id": "venue:perp:T",
        "ts": "2026-10-16T12:00:00Z",
        "received_ts": "2026-10-16T12:00:01Z",
        "oi_native": "0",
        "oi_usd": None,
        "mark_price": "2",
    }


def test_oi_read_preserves_native_zero_and_unknown_usd_mark_clock() -> None:
    original = observation()
    snapshot = decode_oi_inputs(token_id="T", instruments=[instrument()], observations=[original])
    point = snapshot.observations[0]
    assert point.native_quantity.value == Decimal(0)
    assert point.reported_usd.status == "NOT_CAPTURED"
    assert point.mark_price.value == Decimal(2)
    assert point.mark_observed_at.value is None
    assert point.observed_at.precision is None
    assert point.edge_declaration == "ABSENT"
    assert resolve_oi_observation(snapshot, point.evidence, expected=point) == original
    original["oi_native"] = "100"
    assert snapshot.observations[0].native_quantity.value == Decimal(0)
    resolved = resolve_oi_observation(snapshot, point.evidence, expected=point)
    assert resolved["oi_native"] == "0"


@pytest.mark.parametrize("value", [True, "NaN", "Infinity", "nonnumeric", []])
def test_oi_read_retains_invalid_number_as_unestimable(value: JsonInput) -> None:
    row = observation()
    row["oi_native"] = value
    point = decode_oi_inputs(token_id="T", instruments=[instrument()], observations=[row]).observations[0]
    assert point.native_quantity.status == "NOT_ESTIMABLE"
    assert point.native_quantity.value is None


def test_each_unregistered_oi_row_needs_its_own_scope_evidence() -> None:
    first = observation() | instrument()
    second = observation()
    with pytest.raises(ValueError, match="actual derivative market type"):
        decode_oi_inputs(token_id="T", instruments=[], observations=[first, second])
    snapshot = decode_oi_inputs(token_id="T", instruments=[], observations=[first, first.copy()])
    assert all(source.descriptor.scope_basis == "SOURCE_ROW" for source in snapshot.source_projections)
    assert all(source.original == source.scope_original for source in snapshot.source_projections)
    assert snapshot.observations[0].evidence != snapshot.observations[1].evidence


@pytest.mark.parametrize("field,value", [("token_id", "OTHER"), ("venue", "other"), ("market_type", "SPOT")])
def test_oi_registry_scope_cannot_be_overridden(field: str, value: str) -> None:
    row = observation()
    row[field] = value
    with pytest.raises(ValueError, match="contradict"):
        decode_oi_inputs(token_id="T", instruments=[instrument()], observations=[row])


def test_oi_source_projection_cannot_be_mutated_under_its_reference() -> None:
    snapshot = decode_oi_inputs(token_id="T", instruments=[instrument()], observations=[observation()])
    changed = snapshot.source_projections[0].model_copy(update={"original": b"{}"})
    with pytest.raises(ValidationError):
        OIReadSnapshot.model_validate(snapshot.model_copy(update={"source_projections": (changed,)}))


def test_endpoint_selection_retains_nearest_source_clocks_and_exact_native_change() -> None:
    from tank.analysis.condition_derivative_inputs import OISelectionPolicy
    from tank.analysis.open_interest import OIWindowRequest, OpenInterestCalculator, SelectedOIEndpoint
    from tank.analysis.statistics_contracts import FloatBounds, FloatRange
    from tank.time_precision import chosen_boundary, collection_clock

    first = observation() | {"ts": "2026-10-16T11:00:10Z", "oi_native": "1"}
    last = observation() | {"ts": "2026-10-16T11:59:50Z", "oi_native": "1" + "0" * 400}
    source = decode_oi_inputs(token_id="T", instruments=[instrument()], observations=[first, last])
    calculator = OpenInterestCalculator(
        source,
        OISelectionPolicy(tolerance_s=60, minimum_history_days=7, lookback_days=28, quantiles=(0.025, 0.975)),
    )
    selected = calculator.select(
        OIWindowRequest(
            start=chosen_boundary("2026-10-16T11:00:00Z", precision="EXACT"),
            cutoff=chosen_boundary("2026-10-16T12:00:00Z", precision="EXACT"),
            assembled_at=collection_clock("2026-10-16T12:00:02Z"),
        )
    )[0]
    assert isinstance(selected.start, SelectedOIEndpoint)
    assert isinstance(selected.end, SelectedOIEndpoint)
    assert selected.start.observation.observed_at.value == first["ts"]
    assert selected.end.observation.observed_at.value == last["ts"]
    assert selected.end.observation.observed_at.precision is None
    change = selected.native_change(
        normal_range=FloatRange(bounds=FloatBounds(lower=-0.1, upper=0.1), reason=None)
    )
    assert change is not None
    assert change.value is None and change.reason == "NOT_ESTIMABLE"
    assert change.position == "ABOVE" and change.beyond is True
    assert selected.end.observation.reported_usd.value is None


@pytest.mark.parametrize("late", [False, True])
def test_selection_reevaluates_actual_receipt_visibility_without_changing_sources(late: bool) -> None:
    from tank.analysis.condition_derivative_inputs import OISelectionPolicy
    from tank.analysis.open_interest import (
        OIWindowRequest,
        OpenInterestCalculator,
        SelectedOIEndpoint,
        UnavailableOIEndpoint,
    )
    from tank.time_precision import chosen_boundary, collection_clock

    first = observation() | {"ts": "2026-10-16T11:00:00Z", "oi_native": "10"}
    last = observation() | {"oi_native": "12", "received_ts": "2026-10-16T12:00:03Z"}
    source = decode_oi_inputs(token_id="T", instruments=[instrument()], observations=[first, last])
    calculator = OpenInterestCalculator(
        source,
        OISelectionPolicy(tolerance_s=60, minimum_history_days=7, lookback_days=28, quantiles=(0.025, 0.975)),
    )
    selected = calculator.select(
        OIWindowRequest(
            start=chosen_boundary("2026-10-16T11:00:00Z", precision="EXACT"),
            cutoff=chosen_boundary("2026-10-16T12:00:00Z", precision="EXACT"),
            assembled_at=collection_clock("2026-10-16T12:00:04Z" if late else "2026-10-16T12:00:02Z"),
        )
    )[0]
    if late:
        assert isinstance(selected.end, SelectedOIEndpoint)
        assert selected.end.observation.evidence == source.observations[1].evidence
    else:
        assert isinstance(selected.end, UnavailableOIEndpoint)
        assert selected.end.reason == "NO_ENDPOINT_SAMPLE"
        assert selected.excluded[-1].reason == "RECEIPT_AFTER_ASSEMBLY"


def test_equal_distance_distinct_evidence_is_explicitly_ambiguous() -> None:
    from tank.analysis.condition_derivative_inputs import OISelectionPolicy
    from tank.analysis.open_interest import OIWindowRequest, OpenInterestCalculator, UnavailableOIEndpoint
    from tank.time_precision import chosen_boundary, collection_clock

    row = observation() | {"oi_native": "10"}
    source = decode_oi_inputs(token_id="T", instruments=[instrument()], observations=[row, row.copy()])
    selected = OpenInterestCalculator(
        source,
        OISelectionPolicy(tolerance_s=60, minimum_history_days=7, lookback_days=28, quantiles=(0.025, 0.975)),
    ).select(
        OIWindowRequest(
            start=chosen_boundary("2026-10-16T11:00:00Z", precision="EXACT"),
            cutoff=chosen_boundary("2026-10-16T12:00:00Z", precision="EXACT"),
            assembled_at=collection_clock("2026-10-16T12:00:02Z"),
        )
    )[0]
    assert isinstance(selected.end, UnavailableOIEndpoint)
    assert selected.end.reason == "AMBIGUOUS_ENDPOINT_SAMPLE"


def test_actual_derivative_producer_and_typed_selection_preserve_native_endpoints() -> None:
    from truth_support import fixture_truth_config

    import tank.analysis.truth as truth
    from tank.analysis.condition_derivative_inputs import OISelectionPolicy
    from tank.analysis.open_interest import OIWindowRequest, OpenInterestCalculator, SelectedOIEndpoint
    from tank.time_precision import chosen_boundary, collection_clock

    first = observation() | {"ts": "2026-10-16T11:00:10Z", "oi_native": "10", "mark_price": None}
    last = observation() | {"ts": "2026-10-16T11:59:50Z", "oi_native": "12", "mark_price": None}
    registry = instrument()
    source = decode_oi_inputs(token_id="T", instruments=[registry], observations=[first, last])
    request = OIWindowRequest(
        start=chosen_boundary("2026-10-16T11:00:00Z", precision="EXACT"),
        cutoff=chosen_boundary("2026-10-16T12:00:00Z", precision="EXACT"),
        assembled_at=collection_clock("2026-10-16T12:00:02Z"),
    )
    selected = OpenInterestCalculator(
        source,
        OISelectionPolicy(tolerance_s=60, minimum_history_days=7, lookback_days=28, quantiles=(0.025, 0.975)),
    ).select(request)[0]
    actual = truth.compute_condition_values(
        {
            "token_id": "T",
            "cutoff": "2026-10-16T12:00:00Z",
            "assembled_at": "2026-10-16T12:00:02Z",
            "instruments": [registry],
            "oi": [first, last],
        },
        [{"metric": "deriv.native_oi_chg_pct", "window": "W1H"}],
        config=fixture_truth_config(),
    )["derivatives"]["windows"]["W1H"]["per_venue"][0]
    assert isinstance(selected.start, SelectedOIEndpoint)
    assert isinstance(selected.end, SelectedOIEndpoint)
    assert selected.start.observation.native_quantity.value == actual["oi_native_start"] == Decimal(10)
    assert selected.end.observation.native_quantity.value == actual["oi_native_end"] == Decimal(12)
    assert actual["oi_usd_start"] is None and actual["oi_usd_end"] is None
