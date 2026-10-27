"""Role-scoped clock policy preserves reported scalars and declared uncertainty bounds."""

from datetime import datetime
from math import isfinite
from typing import Literal, Self

from pydantic import model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp, interval_bounds
from tank.util import parse_iso_time

type ObservationClockRole = Literal[
    "OPERATIONAL", "SAMPLE_OBSERVATION", "SOURCE_ENDPOINT", "CALCULATION_BOUNDARY"
]
type ObservationClockAbsence = Literal[
    "CLOCK_NOT_CAPTURED",
    "SOURCE_ENDPOINT_NOT_CAPTURED",
    "BUCKET_ORIENTATION_NOT_CAPTURED",
    "CLOCK_BOUNDS_NOT_ESTIMABLE",
]
type ObservationClockBasis = Literal[
    "REPORTED_SCALAR", "COLLECTION_OBSERVATION", "REPORTED_BOUNDARY", "DECLARED_EXACT", "DECLARED_BUCKET"
]


class _ClockValue(DomainModel):
    lower: datetime | None
    upper: datetime | None
    basis: ObservationClockBasis | None
    reason: ObservationClockAbsence | None


def _bounds(clock: Timestamp, role: ObservationClockRole) -> _ClockValue:
    if clock.value is None:
        return _ClockValue(lower=None, upper=None, basis=None, reason="CLOCK_NOT_CAPTURED")
    if role == "SOURCE_ENDPOINT" and clock.precision == "COLLECTION_ONLY":
        return _ClockValue(lower=None, upper=None, basis=None, reason="SOURCE_ENDPOINT_NOT_CAPTURED")
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
    basis: ObservationClockBasis = (
        "DECLARED_EXACT"
        if clock.precision == "EXACT"
        else "REPORTED_BOUNDARY"
        if clock.precision == "COLLECTION_ONLY" and role == "CALCULATION_BOUNDARY"
        else "COLLECTION_OBSERVATION"
        if clock.precision == "COLLECTION_ONLY"
        else "REPORTED_SCALAR"
    )
    return _ClockValue(lower=point, upper=point, basis=basis, reason=None)


class ObservationClockBounds(_ClockValue):
    clock: Timestamp
    role: ObservationClockRole

    @model_validator(mode="after")
    def actual_reported_clock(self) -> Self:
        expected = _bounds(self.clock, self.role)
        if (
            self.lower != expected.lower
            or self.upper != expected.upper
            or self.basis != expected.basis
            or self.reason != expected.reason
        ):
            raise ValueError("Observation clock bounds must preserve their actual role and reported metadata")
        return self


def clock_bounds(clock: Timestamp, *, role: ObservationClockRole) -> ObservationClockBounds:
    clock = Timestamp.model_validate(clock)
    value = _bounds(clock, role)
    return ObservationClockBounds(
        clock=clock, role=role, lower=value.lower, upper=value.upper, basis=value.basis, reason=value.reason
    )


def receipt_visible(received_at: Timestamp, assembled_at: Timestamp) -> bool | None:
    received = clock_bounds(received_at, role="OPERATIONAL")
    assembly = clock_bounds(assembled_at, role="OPERATIONAL")
    if received.lower is None or received.upper is None or assembly.lower is None or assembly.upper is None:
        return None
    if received.upper <= assembly.lower:
        return True
    if received.lower > assembly.upper:
        return False
    return None


def clock_precedes(
    observed_at: Timestamp,
    *,
    role: ObservationClockRole,
    boundary: Timestamp,
    strict: bool,
) -> bool | None:
    if not isinstance(strict, bool):
        raise ValueError("Clock comparison requires actual boolean strictness")
    source = clock_bounds(observed_at, role=role)
    bound = clock_bounds(boundary, role="CALCULATION_BOUNDARY")
    if source.lower is None or source.upper is None or bound.lower is None or bound.upper is None:
        return None
    if source.upper < bound.lower or not strict and source.upper == bound.lower:
        return True
    if source.lower > bound.upper or strict and source.lower == bound.upper:
        return False
    return None


def clock_fresh(
    observed_at: Timestamp,
    *,
    role: ObservationClockRole,
    cutoff: Timestamp,
    cadence_s: float,
) -> bool | None:
    if (
        isinstance(cadence_s, bool)
        or not isinstance(cadence_s, (int, float))
        or not isfinite(cadence_s)
        or cadence_s <= 0
    ):
        raise ValueError("Observation freshness requires an actual positive finite cadence")
    before = clock_precedes(observed_at, role=role, boundary=cutoff, strict=False)
    if before is not True:
        return before
    source = clock_bounds(observed_at, role=role)
    bound = clock_bounds(cutoff, role="CALCULATION_BOUNDARY")
    if source.lower is None or bound.upper is None:
        return None
    return (bound.upper - source.lower).total_seconds() <= cadence_s
