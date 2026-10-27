"""Recorded normalized funding survives independently absent or unusable raw components."""

from decimal import Decimal
from typing import Literal

import pytest
from foundation_helpers import array_field, object_field, present
from funding_input_fixtures import funding_record_fixture, funding_scope_fixture
from pydantic import ValidationError
from test_condition_derivative_selection import derivative_view
from test_storage import repo as repo

from tank.analysis.condition_funding_input_codec import decode_funding_inputs
from tank.analysis.condition_graph_codec import (
    encode_condition_funding,
    encode_requested_condition_derivatives,
)
from tank.analysis.condition_graph_contracts import (
    AvailableConditionFunding,
    ConditionFundingObservation,
    ConditionRequestedDerivativeView,
    RecordedConditionFundingObservation,
)
from tank.analysis.statistics_contracts import DecimalBounds, DecimalRange
from tank.contracts.derivative_values import ReportedDerivativeNumber
from tank.storage import StorageBackend
from tank.time_precision import Timestamp


def recorded_observation(
    *,
    rate: ReportedDerivativeNumber,
    interval: ReportedDerivativeNumber,
    equivalent: Decimal,
    kind: Literal["SETTLED", "CURRENT_ESTIMATE"] = "SETTLED",
) -> RecordedConditionFundingObservation:
    return RecordedConditionFundingObservation(
        basis="RECORDED_EIGHT_HOUR_EQUIVALENT",
        kind=kind,
        raw_rate=rate,
        raw_interval_h=interval,
        equivalent_eight_hour_rate=equivalent,
        observed_at=Timestamp(
            value="2026-10-17T12:00:00Z", precision=None, precision_reason="PRECISION_NOT_CAPTURED"
        ),
    )


def observed_funding(observation: RecordedConditionFundingObservation) -> AvailableConditionFunding:
    return AvailableConditionFunding(
        status="OBSERVED",
        settled=observation if observation.kind == "SETTLED" else None,
        estimate=observation if observation.kind == "CURRENT_ESTIMATE" else None,
        start=None,
        selected_kind=observation.kind,
        normal_range=DecimalRange(
            bounds=DecimalBounds(lower=Decimal("-0.01"), upper=Decimal("0.01")), reason=None
        ),
        funding_vs_normal="WITHIN",
    )


@pytest.mark.parametrize("value", [None, "bad", "0", "-4", "4"])
def test_recorded_rate_preserves_independent_duration_phase(value: str | None) -> None:
    interval = ReportedDerivativeNumber.from_value(value)
    observation = recorded_observation(
        rate=ReportedDerivativeNumber.from_value(None), interval=interval, equivalent=Decimal("-0.002")
    )
    wire = encode_condition_funding(observed_funding(observation))
    assert wire["funding_settled_last"] is None
    assert wire["funding_8h_eq"] == "-0.002"
    assert wire["funding_8h_basis"] == "RECORDED_EIGHT_HOUR_EQUIVALENT"
    assert object_field(wire, "funding_settled_raw_interval_h") == {
        "status": interval.status,
        "value": None if interval.value is None else str(interval.value),
    }
    assert observation.interval_h == (Decimal(4) if value == "4" else None)
    assert wire["interval_h"] == ("4" if value == "4" else None)
    assert wire["funding_settled_interval_reason"] == (
        "NOT_CAPTURED"
        if value is None
        else "NOT_ESTIMABLE"
        if value == "bad"
        else None
        if value == "4"
        else "NON_POSITIVE"
    )
    assert observation.equivalent_rate_reason is None


@pytest.mark.parametrize("raw", [None, "bad", "0", "-0.7"])
def test_recorded_known_zero_does_not_recompute_from_raw_rate(raw: str | None) -> None:
    observation = recorded_observation(
        rate=ReportedDerivativeNumber.from_value(raw),
        interval=ReportedDerivativeNumber.from_value("4"),
        equivalent=Decimal(0),
    )
    wire = encode_condition_funding(observed_funding(observation))
    assert wire["funding_8h_eq"] == "0"
    assert object_field(wire, "funding_settled_raw_rate")["status"] == observation.raw_rate.status
    assert wire["funding_settled_last"] == (None if observation.rate is None else str(observation.rate))


def test_recorded_start_and_estimate_keep_normalized_values_and_actual_clocks() -> None:
    observation = recorded_observation(
        rate=ReportedDerivativeNumber.from_value(None),
        interval=ReportedDerivativeNumber.from_value(None),
        equivalent=Decimal(0),
        kind="CURRENT_ESTIMATE",
    )
    start = RecordedConditionFundingObservation.model_validate(
        observation.model_copy(update={"kind": "SETTLED", "equivalent_eight_hour_rate": Decimal("-0.001")})
    )
    funding = observed_funding(observation)
    funding = type(funding).model_validate(funding.model_copy(update={"start": start}))
    wire = encode_condition_funding(funding)
    assert wire["funding_start"] is None and wire["funding_start_8h_eq"] == "-0.001"
    assert wire["funding_start_observed_at"] == start.observed_at.value
    assert wire["funding_8h_kind"] == "CURRENT_ESTIMATE"
    with pytest.raises(ValidationError, match="takes precedence"):
        AvailableConditionFunding.model_validate(
            funding.model_copy(update={"settled": start, "selected_kind": "SETTLED"})
        )


@pytest.mark.parametrize("field", ["basis", "equivalent_eight_hour_rate", "observed_at", "raw_rate"])
def test_recorded_observation_revalidates_copied_invalid_facts(field: str) -> None:
    observation = recorded_observation(
        rate=ReportedDerivativeNumber.from_value(None),
        interval=ReportedDerivativeNumber.from_value(None),
        equivalent=Decimal(0),
    )
    invalid = {
        "basis": "CURRENT_ANALYSIS_RATE_INTERVAL",
        "equivalent_eight_hour_rate": Decimal("NaN"),
        "observed_at": Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
        "raw_rate": ReportedDerivativeNumber.model_construct(status="OBSERVED", value=None),
    }
    with pytest.raises(ValidationError):
        RecordedConditionFundingObservation.model_validate(
            observation.model_copy(update={field: invalid[field]})
        )


async def test_actual_projected_funding_record_reaches_recorded_output_without_unused_raw_fields(
    repo: StorageBackend,
) -> None:
    row = funding_record_fixture("CURRENT_ESTIMATE")
    row.update(token_id="T", metric_key="venue.funding")
    await repo.put("mkt.funding_obs", "funding", row)
    points = [
        point
        async for point in repo.iter_market_projection(
            "mkt.funding_obs",
            ("instrument_id", "kind", "as_of", "as_of_prec", "rate_8h_eq", "received_ts", "received_ts_prec"),
            "as_of",
            instrument_ids=("venue:perp:T",),
        )
    ]
    source = decode_funding_inputs(token_id="T", instruments=[funding_scope_fixture()], records=points)
    selected = source.observations[0]
    observation = RecordedConditionFundingObservation(
        basis="RECORDED_EIGHT_HOUR_EQUIVALENT",
        kind=selected.kind,
        raw_rate=selected.rate,
        raw_interval_h=selected.interval_h,
        equivalent_eight_hour_rate=present(selected.recorded_eight_hour_rate.value),
        observed_at=selected.clock.timestamp,
    )
    wire = encode_condition_funding(observed_funding(observation))
    assert wire["funding_8h_eq"] == "-0.002"
    assert wire["funding_estimate"] is None and wire["funding_estimate_interval_h"] is None
    assert object_field(wire, "funding_estimate_raw_rate")["status"] == "NOT_CAPTURED"
    assert wire["funding_estimate_observed_at"] == "2026-10-16T12:00:00Z"


def recorded_requested_view() -> ConditionRequestedDerivativeView:
    view = derivative_view("funding", 0)
    venue = view.requested.per_venue[0]
    assert isinstance(venue.funding, AvailableConditionFunding)
    actual = venue.funding.settled
    assert actual is not None
    recorded = RecordedConditionFundingObservation(
        basis="RECORDED_EIGHT_HOUR_EQUIVALENT",
        kind=actual.kind,
        raw_rate=ReportedDerivativeNumber.from_value(None),
        raw_interval_h=ReportedDerivativeNumber.from_value(None),
        equivalent_eight_hour_rate=present(actual.equivalent_eight_hour_rate),
        observed_at=actual.observed_at,
    )
    funding = type(venue.funding).model_validate(venue.funding.model_copy(update={"settled": recorded}))
    venue = type(venue).model_validate(venue.model_copy(update={"funding": funding}))
    requested = type(view.requested).model_validate(view.requested.model_copy(update={"per_venue": (venue,)}))
    return type(view).model_validate(view.model_copy(update={"requested": requested}))


def test_recorded_requested_view_does_not_borrow_richer_raw_state_facts() -> None:
    view = recorded_requested_view()
    wire = encode_requested_condition_derivatives(view)
    row = array_field(wire, "per_venue")[0]
    assert isinstance(row, dict)
    assert row["funding_settled_last"] is None and row["funding_8h_eq"] == "0"
    assert row["funding_8h_basis"] == "RECORDED_EIGHT_HOUR_EQUIVALENT"
    assert object_field(object_field(row, "state_funding_facts"), "settled")["rate"] == "0"


def test_raw_requested_view_does_not_inherit_recorded_state_basis() -> None:
    original = derivative_view("funding", 0)
    recorded = recorded_requested_view()
    state = present(original.state)
    venue = state.per_venue[0]
    venue = type(venue).model_validate(
        venue.model_copy(update={"funding": recorded.requested.per_venue[0].funding})
    )
    state = type(state).model_validate(state.model_copy(update={"per_venue": (venue,)}))
    view = type(original).model_validate(original.model_copy(update={"state": state}))
    row = array_field(encode_requested_condition_derivatives(view), "per_venue")[0]
    assert isinstance(row, dict)
    assert row["funding_settled_last"] == "0" and "funding_8h_basis" not in row
    assert "funding_settled_raw_rate" not in row


@pytest.mark.parametrize("field", ["raw_rate", "raw_interval_h", "equivalent_eight_hour_rate"])
def test_recorded_requested_view_refuses_conflicting_known_state_facts(field: str) -> None:
    view = recorded_requested_view()
    venue = view.requested.per_venue[0]
    assert isinstance(venue.funding, AvailableConditionFunding)
    actual = venue.funding.settled
    assert actual is not None
    assert isinstance(actual, RecordedConditionFundingObservation)
    changed = {
        "raw_rate": ReportedDerivativeNumber.from_value("1"),
        "raw_interval_h": ReportedDerivativeNumber.from_value("4"),
        "equivalent_eight_hour_rate": Decimal("0.001"),
    }
    actual = type(actual).model_validate(actual.model_copy(update={field: changed[field]}))
    funding = type(venue.funding).model_validate(venue.funding.model_copy(update={"settled": actual}))
    venue = type(venue).model_validate(venue.model_copy(update={"funding": funding}))
    requested = type(view.requested).model_validate(view.requested.model_copy(update={"per_venue": (venue,)}))
    with pytest.raises(ValidationError, match="Shared funding observations|settlement duration"):
        type(view).model_validate(view.model_copy(update={"requested": requested}))


def test_legacy_raw_observation_keeps_its_original_wire_shape() -> None:
    view = derivative_view("funding", 0)
    funding = view.requested.per_venue[0].funding
    assert isinstance(funding, AvailableConditionFunding)
    assert isinstance(funding.settled, ConditionFundingObservation)
    wire = encode_condition_funding(funding)
    assert wire["funding_settled_last"] == "0" and wire["funding_settled_interval_h"] == "8"
    assert "funding_8h_basis" not in wire and "funding_settled_raw_rate" not in wire


@pytest.mark.parametrize("value", [None, True, Decimal("NaN"), Decimal("Infinity")])
def test_recorded_normalized_value_requires_a_finite_reported_number(value: object) -> None:
    observation = recorded_observation(
        rate=ReportedDerivativeNumber.from_value(None),
        interval=ReportedDerivativeNumber.from_value(None),
        equivalent=Decimal(0),
    )
    with pytest.raises(ValidationError):
        type(observation).model_validate(observation.model_copy(update={"equivalent_eight_hour_rate": value}))
