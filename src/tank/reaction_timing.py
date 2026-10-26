"""Existing operational timing for late reaction reconsideration work."""

from datetime import timedelta

from tank.contracts import DomainModel
from tank.time_precision import Timestamp, offset_schedule

CAPTURE_EXPIRY_S = 6 * 3600
LATE_REVISION_DEFER_S = 1800


class LateReactionSchedule(DomainModel):
    recorded_at: Timestamp
    run_after: Timestamp
    expires_at: Timestamp


def late_reaction_schedule(recorded_at: Timestamp) -> LateReactionSchedule:
    """Use the current operation's actual clock, never the historical source clock."""
    recorded_at = Timestamp.model_validate(recorded_at)
    if recorded_at.value is None or recorded_at.precision != "COLLECTION_ONLY":
        raise ValueError("Late reaction scheduling requires the actual operation collection clock")
    return LateReactionSchedule(
        recorded_at=recorded_at,
        run_after=offset_schedule(recorded_at, timedelta(seconds=LATE_REVISION_DEFER_S)),
        expires_at=offset_schedule(recorded_at, timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
