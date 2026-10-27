"""Funding-specific temporal requests delegate to the shared role-scoped clock authority."""

from datetime import timedelta
from typing import Literal

from tank.analysis.observation_clock import clock_bounds, clock_fresh, clock_precedes
from tank.time_precision import Timestamp


def _source_role(
    kind: Literal["SETTLED", "CURRENT_ESTIMATE"],
) -> Literal["SOURCE_ENDPOINT", "SAMPLE_OBSERVATION"]:
    if kind not in ("SETTLED", "CURRENT_ESTIMATE"):
        raise ValueError("Funding clock policy requires its actual source kind")
    return "SOURCE_ENDPOINT" if kind == "SETTLED" else "SAMPLE_OBSERVATION"


def funding_source_before(
    observed_at: Timestamp,
    *,
    kind: Literal["SETTLED", "CURRENT_ESTIMATE"],
    boundary: Timestamp,
    strict: bool,
) -> bool | None:
    return clock_precedes(observed_at, role=_source_role(kind), boundary=boundary, strict=strict)


def funding_history_time_eligible(
    observed_at: Timestamp,
    *,
    before: Timestamp,
    lookback_days: int,
) -> bool | None:
    if isinstance(lookback_days, bool) or not isinstance(lookback_days, int) or lookback_days <= 0:
        raise ValueError("Funding retention requires an actual positive day count")
    source = clock_bounds(observed_at, role="SOURCE_ENDPOINT")
    bound = clock_bounds(before, role="CALCULATION_BOUNDARY")
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
    return clock_fresh(observed_at, role=_source_role(kind), cutoff=cutoff, cadence_s=cadence_s)
