from __future__ import annotations
import dataclasses
import hashlib
import json
import secrets
import time
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

UTC = timezone.utc

def utcnow() -> datetime:
    return datetime.now(UTC)

def parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, (int, float, Decimal)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
        number = float(value)
        if abs(number) > 1e14:
            number /= 1e6
        elif abs(number) > 1e11:
            number /= 1e3
        return datetime.fromtimestamp(number, UTC)
    return parse_time(datetime.fromisoformat(str(value).replace("Z", "+00:00")))

def iso(value: Any = None) -> str:
    return parse_time(value if value is not None else utcnow()).isoformat().replace("+00:00", "Z")

def new_id(prefix: str) -> str:
    value = (int(time.time() * 1000) << 80) | secrets.randbits(80)
    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    chars = []
    for _ in range(26):
        chars.append(alphabet[value & 31]); value >>= 5
    return prefix.rstrip("-") + "-" + "".join(reversed(chars))

def jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value): return jsonable(dataclasses.asdict(value))
    if hasattr(value, "model_dump"): return jsonable(value.model_dump(mode="python"))
    if isinstance(value, dict): return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)): return [jsonable(v) for v in value]
    if isinstance(value, datetime): return iso(value)
    if isinstance(value, Decimal): return str(value)
    if isinstance(value, Enum): return value.value
    if isinstance(value, Path): return str(value)
    return value

def canonical_json(value: Any) -> str:
    return json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)

def sha256(value: Any) -> str:
    payload = value if isinstance(value, bytes) else value.encode() if isinstance(value, str) else canonical_json(value).encode()
    return hashlib.sha256(payload).hexdigest()
