"""Capture policies shared by ingestion, repair and operator reports."""

from tank.util import iso


def event_anchor(event):
    value = event.get("event_time")
    precision = event.get("event_time_prec")
    if isinstance(value, dict):
        precision = value.get("precision", precision)
        value = value.get("value", value.get("ts"))
    if value and precision == "EXACT" and event.get("state_verified"):
        return {"type": "T0", "ts": iso(value)}
    if event.get("public_at"):
        return {"type": "T1", "ts": iso(event["public_at"])}
    if event.get("known_to_us_at"):
        return {"type": "KNOWN_TO_US_AT", "ts": iso(event["known_to_us_at"])}
    raise ValueError("Event has no verified exactT0,publicT1 orknown_to_us_at anchor")
