"""Capture policies shared by ingestion, repair and operator reports."""

from tank.time_precision import propagate_timestamp


def event_anchor(event):
    value = event.get("event_time")
    precision = event.get("event_time_prec", event.get("event_time_precision"))
    if isinstance(value, dict):
        precision = value.get("precision", value.get("prec", precision))
    if value and precision == "EXACT" and event.get("state_verified"):
        selected = propagate_timestamp(event, "event_time")
        if selected.value and selected.precision == "EXACT":
            return {"type": "T0", **selected.fields("ts")}
    for field, kind in (("public_at", "T1"), ("known_to_us_at", "KNOWN_TO_US_AT")):
        if event.get(field):
            selected = propagate_timestamp(event, field)
            if selected.value:
                return {"type": kind, **selected.fields("ts")}
    raise ValueError("Event has no verified exactT0,publicT1 orknown_to_us_at anchor")
