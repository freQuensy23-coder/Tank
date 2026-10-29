"""Capture policies shared by ingestion, repair and operator reports."""

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from tank.time_precision import Timestamp, propagate_timestamp
from tank.util import parse_time


def _verified_t0(event: Mapping[str, Any]) -> Timestamp | None:
    value = event.get("event_time")
    precision = event.get("event_time_prec", event.get("event_time_precision"))
    if isinstance(value, dict):
        precision = value.get("precision", value.get("prec", precision))
    if value and precision == "EXACT" and event.get("state_verified"):
        selected = propagate_timestamp(event, "event_time")
        if selected.value and selected.precision == "EXACT":
            return selected
    return None


def t0_pending(event: Mapping[str, Any], at: Any) -> bool:
    """Whether `event` has a verified EXACT T0 that has not happened before `at`."""
    selected = _verified_t0(event)
    return selected is not None and parse_time(selected.value) >= parse_time(at)


def event_anchor(event: Mapping[str, Any], at: Any = None) -> dict[str, Any]:
    """The market anchor of `event`: verified EXACT T0, else T1, else known_to_us_at.

    With `at`, a T0 that is not strictly before `at` has not happened yet at that
    moment, so the anchor falls back to T1 or known_to_us_at; a scheduled event is
    anchored on its own time only once that time has passed.
    """
    selected = _verified_t0(event)
    if selected is not None and (at is None or not t0_pending(event, at)):
        return {"type": "T0", **selected.fields("ts")}
    for field, kind in (("public_at", "T1"), ("known_to_us_at", "KNOWN_TO_US_AT")):
        if event.get(field):
            selected = propagate_timestamp(event, field)
            if selected.value:
                return {"type": kind, **selected.fields("ts")}
    raise ValueError("Event has no verified exactT0,publicT1 orknown_to_us_at anchor")


def capture_boundary_points(cutoff: Any, anchor: Any = None) -> set[datetime]:
    """The exact window bounds a snapshot at `cutoff` (and its anchor) splits bars at:
    the cutoff, each analysis window's start and the window before it, and the anchor with
    its mirrored pre-anchor bound."""
    from datetime import timedelta

    from tank.util import parse_time

    end = parse_time(cutoff)
    points = {end}
    for minutes in (15, 60, 240, 1440):
        points.update((end - timedelta(minutes=minutes), end - timedelta(minutes=2 * minutes)))
    if anchor is not None:
        at = parse_time(anchor.get("ts") if isinstance(anchor, dict) else anchor)
        points.update((at, at - (end - at)))
    return points
