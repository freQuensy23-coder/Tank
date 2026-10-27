"""Current normalization has an exact authority and an identity distinct from recorded funding."""

from decimal import MAX_EMAX, MIN_EMIN, Decimal

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
    ComputedConditionFundingObservation,
    RecordedConditionFundingObservation,
    UnavailableConditionFunding,
)
from tank.analysis.funding_normalization_math import (
    FundingRateCalculation,
    FundingRateOperands,
    normalize_funding_rate,
)
from tank.analysis.funding_rate import (
    CalculatedFundingEquivalent,
    funding_equivalent,
)
from tank.analysis.statistics_contracts import DecimalRange
from tank.contracts.derivative_values import ReportedDerivativeNumber
from tank.contracts.funding import FundingNormalizationRequest
from tank.storage import StorageBackend
from tank.time_precision import Timestamp


def computed_observation(rate: str, interval: str) -> ComputedConditionFundingObservation:
    return ComputedConditionFundingObservation(
        basis="CURRENT_ANALYSIS_RATE_INTERVAL",
        kind="CURRENT_ESTIMATE",
        calculation=normalize_funding_rate(
            FundingRateOperands(rate=Decimal(rate), interval_h=Decimal(interval))
        ),
        observed_at=Timestamp(
            value="2026-10-16T12:00:00Z", precision=None, precision_reason="SOURCE_PRECISION_NOT_CAPTURED"
        ),
    )


def observed_funding(observation: ComputedConditionFundingObservation) -> AvailableConditionFunding:
    return AvailableConditionFunding(
        status="OBSERVED",
        settled=None,
        estimate=observation,
        start=None,
        selected_kind="CURRENT_ESTIMATE",
        normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        funding_vs_normal=None,
    )


@pytest.mark.parametrize(
    "rate,interval,expected",
    [
        ("0.001", "4", "0.002"),
        ("-0.001", "8", "-0.001"),
        ("0", "1", "0"),
        ("1E1000000000", "1E1000000000", "8"),
    ],
)
def test_computed_funding_retains_actual_operands_and_current_basis(
    rate: str, interval: str, expected: str
) -> None:
    observation = computed_observation(rate, interval)
    assert observation.equivalent_eight_hour_rate == Decimal(expected)
    wire = encode_condition_funding(observed_funding(observation))
    assert wire["funding_8h_basis"] == "CURRENT_ANALYSIS_RATE_INTERVAL"
    normalized_wire = wire["funding_8h_eq"]
    assert isinstance(normalized_wire, str) and Decimal(normalized_wire) == Decimal(expected)
    calculation = object_field(wire, "funding_estimate_calculation")
    assert calculation["calculation_precision_digits"] == 128
    assert object_field(calculation, "operands") == {
        "rate": str(Decimal(rate)),
        "interval_h": str(Decimal(interval)),
    }
    assert "funding_estimate_raw_rate" not in wire


def test_unrepresentable_current_calculation_preserves_known_operands_and_own_cause() -> None:
    observation = ComputedConditionFundingObservation(
        basis="CURRENT_ANALYSIS_RATE_INTERVAL",
        kind="CURRENT_ESTIMATE",
        calculation=normalize_funding_rate(
            FundingRateOperands(rate=Decimal((0, (1,), MAX_EMAX)), interval_h=Decimal((0, (1,), MIN_EMIN)))
        ),
        observed_at=computed_observation("0", "1").observed_at,
    )
    assert observation.equivalent_eight_hour_rate is None
    assert observation.equivalent_rate_reason == "NOT_ESTIMABLE"
    unavailable = UnavailableConditionFunding(
        status="UNAVAILABLE",
        reason="INCOMPLETE",
        settled=None,
        estimate=observation,
        start=None,
        normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
    )
    wire = encode_condition_funding(unavailable)
    assert wire["funding_estimate_8h_eq"] is None
    assert wire["funding_estimate_8h_eq_reason"] == "NOT_ESTIMABLE"
    assert wire["funding_estimate"] == str(observation.rate)
    assert wire["funding_estimate_interval_h"] == str(observation.interval_h)
    with pytest.raises(ValidationError, match="actual normalized observation"):
        observed_funding(observation)


@pytest.mark.parametrize("field", ["basis", "calculation", "observed_at"])
def test_computed_observation_revalidates_actual_nested_arithmetic(field: str) -> None:
    observation = computed_observation("0.001", "4")
    changes = {
        "basis": "RECORDED_EIGHT_HOUR_EQUIVALENT",
        "calculation": observation.calculation.model_copy(update={"value": Decimal(1)}),
        "observed_at": Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
    }
    with pytest.raises(ValidationError):
        type(observation).model_validate(observation.model_copy(update={field: changes[field]}))


async def test_actual_narrow_funding_read_builds_current_calculation_without_claiming_recorded_origin(
    repo: StorageBackend,
) -> None:
    row = funding_record_fixture("CURRENT_ESTIMATE")
    row.update(token_id="T", metric_key="venue.funding")
    await repo.put("mkt.funding_obs", "point", row)
    projected = [
        point
        async for point in repo.iter_market_projection(
            "mkt.funding_obs",
            (
                "instrument_id",
                "kind",
                "rate",
                "interval_h",
                "as_of",
                "as_of_prec",
                "received_ts",
                "received_ts_prec",
            ),
            "as_of",
            instrument_ids=("venue:perp:T",),
        )
    ]
    source = decode_funding_inputs(token_id="T", instruments=[funding_scope_fixture()], records=projected)
    selected = funding_equivalent(
        source.observations[0], request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW")
    )
    assert isinstance(selected, CalculatedFundingEquivalent)
    assert selected.source.recorded_eight_hour_rate.status == "NOT_CAPTURED"
    observation = ComputedConditionFundingObservation(
        basis="CURRENT_ANALYSIS_RATE_INTERVAL",
        kind=selected.source.kind,
        calculation=normalize_funding_rate(
            FundingRateOperands(
                rate=present(selected.source.rate.value), interval_h=present(selected.source.interval_h.value)
            )
        ),
        observed_at=selected.source.clock.timestamp,
    )
    wire = encode_condition_funding(observed_funding(observation))
    assert wire["funding_8h_eq"] == "-0.002"
    assert wire["funding_8h_basis"] == "CURRENT_ANALYSIS_RATE_INTERVAL"
    assert observation.equivalent_eight_hour_rate == selected.value


def test_distinct_recorded_and_current_normalizations_do_not_imply_agreement() -> None:
    view = derivative_view("funding", 0)
    requested_venue = view.requested.per_venue[0]
    assert isinstance(requested_venue.funding, AvailableConditionFunding)
    old_point = requested_venue.funding.settled
    assert old_point is not None
    recorded = RecordedConditionFundingObservation(
        basis="RECORDED_EIGHT_HOUR_EQUIVALENT",
        kind="SETTLED",
        raw_rate=ReportedDerivativeNumber.from_value("1"),
        raw_interval_h=ReportedDerivativeNumber.from_value("4"),
        equivalent_eight_hour_rate=Decimal(0),
        observed_at=old_point.observed_at,
    )
    current = ComputedConditionFundingObservation(
        basis="CURRENT_ANALYSIS_RATE_INTERVAL",
        kind="SETTLED",
        calculation=normalize_funding_rate(FundingRateOperands(rate=Decimal(1), interval_h=Decimal(4))),
        observed_at=old_point.observed_at,
    )
    requested_funding = type(requested_venue.funding).model_validate(
        requested_venue.funding.model_copy(update={"settled": current, "funding_vs_normal": "ABOVE"})
    )
    requested_venue = type(requested_venue).model_validate(
        requested_venue.model_copy(update={"funding": requested_funding})
    )
    requested = type(view.requested).model_validate(
        view.requested.model_copy(update={"per_venue": (requested_venue,)})
    )
    state = present(view.state)
    state_venue = state.per_venue[0]
    assert isinstance(state_venue.funding, AvailableConditionFunding)
    state_funding = type(state_venue.funding).model_validate(
        state_venue.funding.model_copy(update={"settled": recorded})
    )
    state_venue = type(state_venue).model_validate(state_venue.model_copy(update={"funding": state_funding}))
    state = type(state).model_validate(state.model_copy(update={"per_venue": (state_venue,)}))
    combined = type(view).model_validate(view.model_copy(update={"requested": requested, "state": state}))
    wire = encode_requested_condition_derivatives(combined)
    point = array_field(wire, "per_venue")[0]
    assert isinstance(point, dict)
    assert point["funding_8h_eq"] == "2" and point["funding_8h_basis"] == "CURRENT_ANALYSIS_RATE_INTERVAL"
    assert (
        object_field(object_field(point, "state_funding_facts"), "settled")["equivalent_eight_hour_rate"]
        == "0"
    )
    forged = current.calculation.model_copy(update={"value": Decimal(0)})
    with pytest.raises(ValidationError):
        FundingRateCalculation.model_validate(forged)
