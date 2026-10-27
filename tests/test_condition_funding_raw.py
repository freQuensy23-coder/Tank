"""Unavailable normalization retains independent reported funding components and clocks."""

from decimal import MAX_EMAX, MIN_EMIN, Decimal

import pytest
from foundation_helpers import object_field
from funding_input_fixtures import funding_record_fixture, funding_scope_fixture
from pydantic import ValidationError
from test_condition_graph_contracts import funding_observation
from test_storage import repo as repo

from tank.analysis.condition_funding_input_codec import decode_funding_inputs
from tank.analysis.condition_graph_codec import encode_condition_funding
from tank.analysis.condition_graph_contracts import (
    AvailableConditionFunding,
    UnavailableConditionFunding,
    UnnormalizedConditionFundingObservation,
)
from tank.analysis.funding_normalization_math import normalize_funding_components
from tank.analysis.statistics_contracts import DecimalRange
from tank.contracts.derivative_values import ReportedDerivativeNumber
from tank.contracts.funding import FundingNormalizationMode, FundingNormalizationRequest
from tank.storage import StorageBackend
from tank.time_precision import Timestamp


def raw_observation(
    rate: str | None, interval: str | None, mode: FundingNormalizationMode
) -> UnnormalizedConditionFundingObservation:
    return UnnormalizedConditionFundingObservation(
        phase="RAW_COMPONENTS_ONLY",
        kind="CURRENT_ESTIMATE",
        normalization=normalize_funding_components(
            rate=ReportedDerivativeNumber.from_value(rate),
            interval=ReportedDerivativeNumber.from_value(interval),
            request=FundingNormalizationRequest(mode=mode),
        ),
        recorded_eight_hour_rate=ReportedDerivativeNumber.from_value(None),
        observed_at=Timestamp(
            value="2026-10-16T12:00:00Z", precision=None, precision_reason="SOURCE_PRECISION_NOT_CAPTURED"
        ),
    )


def unavailable(observation: UnnormalizedConditionFundingObservation) -> UnavailableConditionFunding:
    return UnavailableConditionFunding(
        status="UNAVAILABLE",
        reason="INCOMPLETE",
        settled=None,
        estimate=observation,
        start=None,
        normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
    )


@pytest.mark.parametrize(
    "rate,interval,mode,reason",
    [
        (None, "-4", "CALCULATE_FROM_RAW", "RATE_NOT_CAPTURED"),
        ("bad", "8", "CALCULATE_FROM_RAW", "RATE_NOT_ESTIMABLE"),
        ("0", None, "CALCULATE_FROM_RAW", "INTERVAL_NOT_CAPTURED"),
        ("0", "bad", "CALCULATE_FROM_RAW", "INTERVAL_NOT_ESTIMABLE"),
        ("0", "0", "CALCULATE_FROM_RAW", "NONPOSITIVE_INTERVAL"),
        ("0", "4", "RECORDED_ONLY", "RAW_NORMALIZATION_NOT_REQUESTED"),
    ],
)
def test_raw_component_cause_and_request_survive_public_funding_wire(
    rate: str | None, interval: str | None, mode: FundingNormalizationMode, reason: str
) -> None:
    observation = raw_observation(rate, interval, mode)
    wire = encode_condition_funding(unavailable(observation))
    assert wire["funding_estimate_normalization_reason"] == reason
    assert object_field(wire, "funding_estimate_normalization_request") == {"mode": mode}
    assert object_field(wire, "funding_estimate_raw_rate") == {
        "status": observation.raw_rate.status,
        "value": None if observation.rate is None else str(observation.rate),
    }
    assert object_field(wire, "funding_estimate_raw_interval_h") == {
        "status": observation.raw_interval_h.status,
        "value": None if observation.raw_interval_h.value is None else str(observation.raw_interval_h.value),
    }
    assert wire["funding_estimate_8h_eq"] is None and wire["funding_8h_eq"] is None
    assert wire["funding_estimate_observed_at"] == observation.observed_at.value
    assert object_field(wire, "funding_estimate_recorded_8h_eq")["status"] == "NOT_CAPTURED"


def test_generic_selected_settled_keeps_own_clock_and_unavailable_estimate_report() -> None:
    observation = raw_observation(None, "-4", "CALCULATE_FROM_RAW")
    settled = funding_observation("SETTLED", Decimal(8))
    settled = type(settled).model_validate(
        settled.model_copy(
            update={
                "observed_at": Timestamp(
                    value="2026-10-16T08:00:00Z",
                    precision=None,
                    precision_reason="SOURCE_PRECISION_NOT_CAPTURED",
                )
            }
        )
    )
    funding = AvailableConditionFunding(
        status="OBSERVED",
        settled=settled,
        estimate=observation,
        start=None,
        selected_kind="SETTLED",
        normal_range=DecimalRange(bounds=None, reason="INSUFFICIENT_HISTORY"),
        funding_vs_normal=None,
    )
    wire = encode_condition_funding(funding)
    assert wire["funding_8h_eq"] == "0" and wire["funding_8h_kind"] == "SETTLED"
    assert wire["funding_settled_last_observed_at"] == "2026-10-16T08:00:00Z"
    assert wire["funding_estimate_observed_at"] == "2026-10-16T12:00:00Z"
    assert wire["funding_estimate_interval_h"] is None
    assert object_field(wire, "funding_estimate_raw_interval_h") == {"status": "OBSERVED", "value": "-4"}
    assert wire["funding_estimate_interval_reason"] == "NON_POSITIVE"
    with pytest.raises(ValidationError, match="actual normalized observation"):
        AvailableConditionFunding.model_validate(
            funding.model_copy(update={"selected_kind": "CURRENT_ESTIMATE"})
        )


@pytest.mark.parametrize("field", ["recorded_eight_hour_rate", "normalization", "observed_at", "phase"])
def test_raw_observation_revalidates_phase_cause_and_actual_source_clock(field: str) -> None:
    observation = raw_observation(None, "-4", "CALCULATE_FROM_RAW")
    changes = {
        "recorded_eight_hour_rate": ReportedDerivativeNumber.from_value("0"),
        "normalization": observation.normalization.model_copy(update={"reason": "NONPOSITIVE_INTERVAL"}),
        "observed_at": Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED"),
        "phase": "OBSERVED_NORMALIZED",
    }
    with pytest.raises(ValidationError):
        type(observation).model_validate(observation.model_copy(update={field: changes[field]}))


@pytest.mark.parametrize("failed", [False, True])
def test_attempted_calculation_requires_computed_observation_even_without_a_value(failed: bool) -> None:
    if failed:
        rate = ReportedDerivativeNumber(status="OBSERVED", value=Decimal((0, (1,), MAX_EMAX)))
        interval = ReportedDerivativeNumber(status="OBSERVED", value=Decimal((0, (1,), MIN_EMIN)))
    else:
        rate = ReportedDerivativeNumber.from_value("0")
        interval = ReportedDerivativeNumber.from_value("4")
    result = normalize_funding_components(
        rate=rate, interval=interval, request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW")
    )
    assert result.calculation is not None
    observation = raw_observation(None, "-4", "CALCULATE_FROM_RAW")
    with pytest.raises(ValidationError, match="funding components|computed observation"):
        type(observation).model_validate(observation.model_copy(update={"normalization": result}))


async def test_actual_retained_projection_preserves_invalid_reported_interval_without_padding_rate(
    repo: StorageBackend,
) -> None:
    row = funding_record_fixture("CURRENT_ESTIMATE")
    row.update(token_id="T", metric_key="venue.funding", interval_h="-4")
    await repo.put("mkt.funding_obs", "retained-report", row)
    points = [
        point
        async for point in repo.iter_market_projection(
            "mkt.funding_obs",
            ("instrument_id", "kind", "interval_h", "as_of", "as_of_prec", "received_ts", "received_ts_prec"),
            "as_of",
            instrument_ids=("venue:perp:T",),
        )
    ]
    source = decode_funding_inputs(token_id="T", instruments=[funding_scope_fixture()], records=points)
    point = source.observations[0]
    observation = UnnormalizedConditionFundingObservation(
        phase="RAW_COMPONENTS_ONLY",
        kind=point.kind,
        normalization=normalize_funding_components(
            rate=point.rate,
            interval=point.interval_h,
            request=FundingNormalizationRequest(mode="CALCULATE_FROM_RAW"),
        ),
        recorded_eight_hour_rate=point.recorded_eight_hour_rate,
        observed_at=point.clock.timestamp,
    )
    wire = encode_condition_funding(unavailable(observation))
    assert object_field(wire, "funding_estimate_raw_rate")["status"] == "NOT_CAPTURED"
    assert object_field(wire, "funding_estimate_raw_interval_h") == {"status": "OBSERVED", "value": "-4"}
    assert wire["funding_estimate_normalization_reason"] == "RATE_NOT_CAPTURED"
    assert wire["funding_estimate_observed_at"] == "2026-10-16T12:00:00Z"
