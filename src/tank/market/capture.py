"""Capture policies shared by ingestion, repair and operator reports."""

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Literal, Self

from pydantic import JsonValue, TypeAdapter, model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp, propagate_timestamp
from tank.util import parse_iso_time

type TimeInput = datetime | str

_JSON = TypeAdapter(dict[str, JsonValue])


class EventAnchorCandidates(DomainModel):
    event_time: Timestamp | None
    state_verified: bool | None
    public_at: Timestamp | None
    known_to_us_at: Timestamp | None


class EventAnchor(DomainModel):
    type: Literal["T0", "T1", "KNOWN_TO_US_AT"]
    timestamp: Timestamp

    @model_validator(mode="after")
    def has_value(self) -> Self:
        if self.timestamp.value is None:
            raise ValueError("Event anchor requires a timestamp value")
        return self

    @property
    def value(self) -> str:
        value = self.timestamp.value
        if value is None:
            raise ValueError("Event anchor requires a timestamp value")
        return value

    def to_wire(self) -> dict[str, JsonValue]:
        return _JSON.validate_python({"type": self.type, **self.timestamp.fields("ts")})


def choose_event_anchor(candidates: EventAnchorCandidates) -> EventAnchor:
    """Prefer verified exact T0, then public T1, then our first observation."""
    event_time = candidates.event_time
    if (
        candidates.state_verified is True
        and event_time is not None
        and event_time.value is not None
        and event_time.precision == "EXACT"
    ):
        return EventAnchor(type="T0", timestamp=event_time)
    for timestamp, kind in (
        (candidates.public_at, "T1"),
        (candidates.known_to_us_at, "KNOWN_TO_US_AT"),
    ):
        if timestamp is not None and timestamp.value is not None:
            if kind == "T1":
                return EventAnchor(type="T1", timestamp=timestamp)
            return EventAnchor(type="KNOWN_TO_US_AT", timestamp=timestamp)
    raise ValueError("Event has no verified exactT0,publicT1 orknown_to_us_at anchor")


def event_anchor(event: Mapping[str, JsonValue]) -> EventAnchor:
    """Decode timestamp provenance at the event repository boundary."""
    candidates = EventAnchorCandidates.model_validate(
        {
            "event_time": propagate_timestamp(event, "event_time")
            if event.get("event_time") is not None
            else None,
            "state_verified": event.get("state_verified"),
            "public_at": propagate_timestamp(event, "public_at")
            if event.get("public_at") is not None
            else None,
            "known_to_us_at": (
                propagate_timestamp(event, "known_to_us_at")
                if event.get("known_to_us_at") is not None
                else None
            ),
        }
    )
    return choose_event_anchor(candidates)


def capture_boundary_points(
    cutoff: TimeInput,
    anchor: Mapping[str, JsonValue] | EventAnchor | TimeInput | None = None,
) -> set[datetime]:
    """Exact cutoff/window splits plus the anchor and its mirrored pre-anchor bound."""
    end = parse_iso_time(cutoff)
    points = {end}
    for minutes in (15, 60, 240, 1440):
        points.update((end - timedelta(minutes=minutes), end - timedelta(minutes=2 * minutes)))
    if anchor is not None:
        value: TimeInput | JsonValue
        if isinstance(anchor, EventAnchor):
            value = anchor.timestamp.value
        elif isinstance(anchor, Mapping):
            value = anchor["ts"]
        else:
            value = anchor
        if not isinstance(value, (datetime, str)):
            raise ValueError("Capture anchor requires a timestamp value")
        at = parse_iso_time(value)
        points.update((at, at - (end - at)))
    return points
