"""Move the typed statement records a test snapshot carries (the `*_sql` keys) in time with the rest of it."""

import dataclasses
import json
import re
from datetime import datetime

from tank.util import iso, parse_time


def shifted_evidence(value, delta):
    if dataclasses.is_dataclass(value):
        return dataclasses.replace(
            value,
            **{f.name: shifted_evidence(getattr(value, f.name), delta) for f in dataclasses.fields(value)},
        )
    if isinstance(value, tuple):
        return tuple(shifted_evidence(item, delta) for item in value)
    if isinstance(value, dict):
        return {key: shifted_evidence(item, delta) for key, item in value.items()}
    if isinstance(value, datetime):
        return value + delta
    if isinstance(value, str) and re.match(r"^\d{4}-\d{2}-\d{2}T", value):
        return iso(parse_time(value) + delta)
    if isinstance(value, str) and value[:1] == "{":
        return json.dumps(shifted_evidence(json.loads(value), delta))
    if isinstance(value, list):
        return [shifted_evidence(item, delta) for item in value]
    return value
