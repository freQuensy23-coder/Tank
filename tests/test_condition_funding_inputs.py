"""Authored normalized funding reads retain clocks, missing fields and immutable evidence."""

from decimal import Decimal

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.condition_funding_input_codec import decode_funding_inputs
from tank.analysis.condition_funding_inputs import FundingInputObservation, FundingKind, FundingReadSnapshot
from tank.contracts.json import decode_unique_json
from tank.time_precision import Timestamp


def instrument() -> dict[str, JsonValue]:
    return {"instrument_id": "venue:perp:T", "token_id": "T", "venue": "venue", "market_type": "PERP_LINEAR"}


def funding_record(kind: FundingKind) -> dict[str, JsonValue]:
    """The authored batch was received at 12:01; source observations remain separate."""
    row: dict[str, JsonValue] = {
        "instrument_id": "venue:perp:T",
        "kind": kind,
        "rate": "-0.001",
        "interval_h": "4",
        "rate_8h_eq": "-0.002",
        "as_of": "2026-10-16T12:00:00Z",
        "as_of_prec": "COLLECTION_ONLY",
        "received_ts": "2026-10-16T12:01:00Z",
        "received_ts_prec": "COLLECTION_ONLY",
        "provider_extension": {"retained": True},
    }
    if kind == "SETTLED":
        row.update(
            settlement_time="2026-10-16T08:00:00Z",
            settlement_time_prec="EXACT",
            funding_time="2026-10-16T08:00:00Z",
            funding_time_prec="EXACT",
        )
    return row


@pytest.mark.parametrize("kind", ["SETTLED", "CURRENT_ESTIMATE"])
def test_funding_clock_preserves_settlement_versus_actual_estimate_observation(kind: FundingKind) -> None:
    source = decode_funding_inputs(token_id="T", instruments=[instrument()], records=[funding_record(kind)])
    point = source.observations[0]
    assert point.rate.value == Decimal("-0.001")
    assert point.interval_h.value == Decimal(4)
    assert point.recorded_eight_hour_rate.value == Decimal("-0.002")
    assert point.clock.field == ("settlement_time" if kind == "SETTLED" else "as_of")
    assert point.clock.timestamp.value == (
        "2026-10-16T08:00:00Z" if kind == "SETTLED" else "2026-10-16T12:00:00Z"
    )
    assert point.clock.timestamp.precision == ("EXACT" if kind == "SETTLED" else "COLLECTION_ONLY")
    assert point.received_at.value == "2026-10-16T12:01:00Z"


def test_funding_settlement_alias_is_used_without_borrowing_its_missing_precision() -> None:
    row = funding_record("SETTLED")
    row.pop("settlement_time")
    row.pop("settlement_time_prec")
    row.pop("funding_time_prec")
    point = decode_funding_inputs(token_id="T", instruments=[instrument()], records=[row]).observations[0]
    assert point.clock.field == "funding_time"
    assert point.clock.timestamp.value == "2026-10-16T08:00:00Z"
    assert point.clock.timestamp.precision is None


def test_settled_funding_does_not_substitute_collection_observation_for_missing_settlement() -> None:
    row = funding_record("SETTLED")
    for key in ("settlement_time", "settlement_time_prec", "funding_time", "funding_time_prec"):
        row.pop(key)
    point = decode_funding_inputs(token_id="T", instruments=[instrument()], records=[row]).observations[0]
    assert point.clock.timestamp.value is None
    assert point.clock.field is None
    assert point.rate.value == Decimal("-0.001")
    assert point.recorded_eight_hour_rate.value == Decimal("-0.002")


def test_funding_read_refuses_conflicting_actual_settlement_aliases() -> None:
    row = funding_record("SETTLED")
    row["funding_time"] = "2026-10-16T09:00:00Z"
    with pytest.raises(ValueError, match="clock aliases contradict"):
        decode_funding_inputs(token_id="T", instruments=[instrument()], records=[row])


@pytest.mark.parametrize("field", ["rate", "interval_h", "rate_8h_eq"])
def test_missing_funding_component_does_not_acquire_a_protocol_default(field: str) -> None:
    row = funding_record("SETTLED")
    row.pop(field)
    point = decode_funding_inputs(token_id="T", instruments=[instrument()], records=[row]).observations[0]
    component = {
        "rate": point.rate,
        "interval_h": point.interval_h,
        "rate_8h_eq": point.recorded_eight_hour_rate,
    }
    assert component[field].status == "NOT_CAPTURED"
    assert component[field].value is None
    assert all(value.value is not None for key, value in component.items() if key != field)


@pytest.mark.parametrize("value", ["NaN", "Infinity", True, {"value": "0.01"}])
def test_unusable_funding_rate_keeps_actual_interval_and_source_evidence(value: JsonValue) -> None:
    row = funding_record("CURRENT_ESTIMATE")
    row["rate"] = value
    source = decode_funding_inputs(token_id="T", instruments=[instrument()], records=[row])
    assert source.observations[0].rate.status == "NOT_ESTIMABLE"
    assert source.observations[0].rate.value is None
    assert source.observations[0].interval_h.value == 4
    assert decode_unique_json(source.source_projections[0].original) == row


def test_funding_missing_receipt_stays_unknown_and_future_receipt_is_not_visible() -> None:
    row = funding_record("CURRENT_ESTIMATE")
    before = Timestamp(value="2026-10-16T12:00:30Z", precision="EXACT", precision_reason=None)
    point = decode_funding_inputs(token_id="T", instruments=[instrument()], records=[row]).observations[0]
    assert point.visible_at(before) is False
    row.pop("received_ts")
    row.pop("received_ts_prec")
    missing = decode_funding_inputs(token_id="T", instruments=[instrument()], records=[row]).observations[0]
    assert missing.received_at.value is None
    assert missing.visible_at(before) is None


def test_caller_mutation_cannot_change_funding_read_or_its_exact_source_digest() -> None:
    scope, row = instrument(), funding_record("SETTLED")
    source = decode_funding_inputs(token_id="T", instruments=[scope], records=[row])
    original = source.source_projections[0].original
    reference = source.observations[0].evidence
    scope["venue"] = "other"
    row["rate"] = "0.7"
    assert source.observations[0].rate.value == Decimal("-0.001")
    assert source.observations[0].instrument.venue == "venue"
    assert source.source_projections[0].original == original
    assert source.observations[0].evidence == reference


def test_public_funding_read_refuses_copied_fact_disagreeing_with_captured_record() -> None:
    source = decode_funding_inputs(
        token_id="T", instruments=[instrument()], records=[funding_record("SETTLED")]
    )
    point = source.observations[0]
    changed = point.model_copy(update={"kind": "CURRENT_ESTIMATE"})
    with pytest.raises(ValidationError, match="clock field"):
        FundingInputObservation.model_validate(changed)
    forged = source.model_copy(
        update={
            "observations": (
                point.model_copy(
                    update={
                        "clock": source.observations[0].clock.model_copy(
                            update={
                                "timestamp": Timestamp(
                                    value="2026-10-16T07:00:00Z", precision="EXACT", precision_reason=None
                                )
                            }
                        )
                    }
                ),
            )
        }
    )
    with pytest.raises(ValidationError, match="consumed facts disagree"):
        FundingReadSnapshot.model_validate(forged)


def test_explicit_empty_funding_read_contains_no_numeric_or_instrument_defaults() -> None:
    source = decode_funding_inputs(token_id="T", instruments=[], records=[])
    assert source.instruments == ()
    assert source.observations == ()
    assert source.source_projections == ()


def test_self_scoped_funding_record_does_not_authorize_a_later_unscoped_record() -> None:
    first = dict(funding_record("SETTLED"), **instrument())
    second = funding_record("CURRENT_ESTIMATE")
    with pytest.raises(ValueError, match="actual derivative market type"):
        decode_funding_inputs(token_id="T", instruments=[], records=[first, second])
