"""Funding clock roles keep reported scalars distinct from declared uncertainty bounds."""

from datetime import datetime, timedelta
from math import isfinite
from typing import Literal, Self

from pydantic import model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp, interval_bounds
from tank.util import parse_iso_time

type FundingClockRole = Literal["OPERATIONAL", "ESTIMATE_SAMPLE", "SETTLEMENT_EVENT", "CALCULATION_BOUNDARY"]
type FundingClockAbsence = Literal[
    "CLOCK_NOT_CAPTURED",
    "PAID_TIME_NOT_CAPTURED",
    "BUCKET_ORIENTATION_NOT_CAPTURED",
    "CLOCK_BOUNDS_NOT_ESTIMABLE",
]
type FundingClockBasis = Literal[
    "REPORTED_SCALAR", "COLLECTION_OBSERVATION", "REPORTED_BOUNDARY", "DECLARED_EXACT", "DECLARED_BUCKET"
]


class _ClockValue(DomainModel):
    lower: datetime | None
    upper: datetime | None
    basis: FundingClockBasis | None
    reason: FundingClockAbsence | None


def _bounds(clock: Timestamp, role: FundingClockRole) -> _ClockValue:
    if clock.value is None:
        return _ClockValue(lower=None, upper=None, basis=None, reason="CLOCK_NOT_CAPTURED")
    if role == "SETTLEMENT_EVENT" and clock.precision == "COLLECTION_ONLY":
        return _ClockValue(lower=None, upper=None, basis=None, reason="PAID_TIME_NOT_CAPTURED")
    if clock.precision is not None and clock.precision.startswith("BUCKET("):
        if clock.bucket_anchor not in ("START", "END"):
            return _ClockValue(lower=None, upper=None, basis=None, reason="BUCKET_ORIENTATION_NOT_CAPTURED")
        try:
            lower, upper = interval_bounds(clock)
        except (OverflowError, ValueError):
            return _ClockValue(lower=None, upper=None, basis=None, reason="CLOCK_BOUNDS_NOT_ESTIMABLE")
        if lower is None or upper is None:
            return _ClockValue(lower=None, upper=None, basis=None, reason="CLOCK_BOUNDS_NOT_ESTIMABLE")
        return _ClockValue(lower=lower, upper=upper, basis="DECLARED_BUCKET", reason=None)
    point = parse_iso_time(clock.value)
    basis: FundingClockBasis = (
        "DECLARED_EXACT"
        if clock.precision == "EXACT"
        else "REPORTED_BOUNDARY"
        if clock.precision == "COLLECTION_ONLY" and role == "CALCULATION_BOUNDARY"
        else "COLLECTION_OBSERVATION"
        if clock.precision == "COLLECTION_ONLY"
        else "REPORTED_SCALAR"
    )
    return _ClockValue(lower=point, upper=point, basis=basis, reason=None)


class FundingClockBounds(_ClockValue):
    clock: Timestamp
    role: FundingClockRole

    @model_validator(mode="after")
    def actual_reported_clock(self) -> Self:
        expected = _bounds(self.clock, self.role)
        if (
            self.lower != expected.lower
            or self.upper != expected.upper
            or self.basis != expected.basis
            or self.reason != expected.reason
        ):
            raise ValueError("Funding clock bounds must preserve their actual role and reported metadata")
        return self


def funding_clock_bounds(clock: Timestamp, *, role: FundingClockRole) -> FundingClockBounds:
    clock = Timestamp.model_validate(clock)
    value = _bounds(clock, role)
    return FundingClockBounds(
        clock=clock, role=role, lower=value.lower, upper=value.upper, basis=value.basis, reason=value.reason
    )


def funding_receipt_visible(received_at: Timestamp, assembled_at: Timestamp) -> bool | None:
    received = funding_clock_bounds(received_at, role="OPERATIONAL")
    assembly = funding_clock_bounds(assembled_at, role="OPERATIONAL")
    if received.lower is None or received.upper is None or assembly.lower is None or assembly.upper is None:
        return None
    if received.upper <= assembly.lower:
        return True
    if received.lower > assembly.upper:
        return False
    return None


def funding_source_before(
    observed_at: Timestamp,
    *,
    kind: Literal["SETTLED", "CURRENT_ESTIMATE"],
    boundary: Timestamp,
    strict: bool,
) -> bool | None:
    if kind not in ("SETTLED", "CURRENT_ESTIMATE") or not isinstance(strict, bool):
        raise ValueError("Funding temporal comparison requires its actual kind and strictness")
    source = funding_clock_bounds(
        observed_at, role="SETTLEMENT_EVENT" if kind == "SETTLED" else "ESTIMATE_SAMPLE"
    )
    bound = funding_clock_bounds(boundary, role="CALCULATION_BOUNDARY")
    if source.lower is None or source.upper is None or bound.lower is None or bound.upper is None:
        return None
    if source.upper < bound.lower or not strict and source.upper == bound.lower:
        return True
    if source.lower > bound.upper or strict and source.lower == bound.upper:
        return False
    return None


def funding_history_time_eligible(
    observed_at: Timestamp,
    *,
    before: Timestamp,
    lookback_days: int,
) -> bool | None:
    if isinstance(lookback_days, bool) or not isinstance(lookback_days, int) or lookback_days <= 0:
        raise ValueError("Funding retention requires an actual positive day count")
    source = funding_clock_bounds(observed_at, role="SETTLEMENT_EVENT")
    bound = funding_clock_bounds(before, role="CALCULATION_BOUNDARY")
    if source.lower is None or source.upper is None or bound.lower is None or bound.upper is None:
        return None
    try:
        latest_lower = bound.upper - timedelta(days=lookback_days)
    except OverflowError:
        raise ValueError("Funding historical boundary cannot be represented") from None
    if source.lower >= latest_lower and source.upper < bound.lower:
        return True
    if source.upper < latest_lower or source.lower >= bound.upper:
        return False
    return None


def funding_observation_fresh(
    observed_at: Timestamp,
    *,
    kind: Literal["SETTLED", "CURRENT_ESTIMATE"],
    cutoff: Timestamp,
    cadence_s: float,
) -> bool | None:
    if (
        isinstance(cadence_s, bool)
        or not isinstance(cadence_s, (int, float))
        or not isfinite(cadence_s)
        or cadence_s <= 0
    ):
        raise ValueError("Funding freshness requires an actual positive finite cadence")
    before = funding_source_before(observed_at, kind=kind, boundary=cutoff, strict=False)
    if before is not True:
        return before
    source = funding_clock_bounds(
        observed_at, role="SETTLEMENT_EVENT" if kind == "SETTLED" else "ESTIMATE_SAMPLE"
    )
    bound = funding_clock_bounds(cutoff, role="CALCULATION_BOUNDARY")
    if source.lower is None or bound.upper is None:
        return None
    return (bound.upper - source.lower).total_seconds() <= cadence_s
