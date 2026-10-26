import hashlib
import json
import math
import secrets
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Literal, overload

from pydantic import BaseModel, JsonValue

from tank.contracts.json import JsonDocument

UTC = UTC

type JsonInput = (
    JsonValue
    | str
    | int
    | float
    | bool
    | None
    | datetime
    | Decimal
    | Enum
    | Path
    | BaseModel
    | Mapping[str, JsonInput]
    | list[JsonInput]
    | tuple[JsonInput, ...]
    | set[JsonInput]
)

type TimeInput = datetime | str


type EpochInput = str | int | float | Decimal
type EpochUnit = Literal["s", "ms", "us"]
_EPOCH_DIVISORS: dict[EpochUnit, int] = {"s": 1, "ms": 1000, "us": 1000000}
_ISO_PARSE_CACHE_MAX_ENTRIES = 4096
_ISO_PARSE_CACHE_MAX_KEY_CHARS = 128


def utcnow() -> datetime:
    return datetime.now(UTC)


def clock_iso() -> str:
    """Observe the local processing clock explicitly, without formatting an absent value."""
    return utcnow().isoformat().replace("+00:00", "Z")


def parse_epoch(value: EpochInput, *, unit: EpochUnit) -> datetime:
    """Decode a declared epoch unit with the existing float/fromtimestamp rounding."""
    if isinstance(value, bool):
        raise ValueError("Epoch timestamp must not be a boolean")
    if unit not in _EPOCH_DIVISORS:
        raise ValueError("Unsupported epoch timestamp unit")
    try:
        number = float(value)
        if not math.isfinite(number):
            raise ValueError
        return datetime.fromtimestamp(number / _EPOCH_DIVISORS[unit], UTC)
    except (OSError, OverflowError, TypeError, ValueError):
        raise ValueError("Invalid or out-of-range epoch timestamp") from None


def _iso_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise ValueError("Timestamp requires an ISO date-time")
    if value.replace(".", "", 1).lstrip("+-").isdigit():
        raise ValueError("Numeric timestamps require an explicit epoch unit")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("Invalid ISO timestamp") from None


def _aware_utc(parsed: datetime) -> datetime:
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Timestamp requires an explicit timezone")
    return parsed.astimezone(UTC)


@lru_cache(maxsize=_ISO_PARSE_CACHE_MAX_ENTRIES)
def _plain_iso_time(value: str) -> datetime:
    return _aware_utc(_iso_datetime(value))


def parse_iso_time(value: datetime | str) -> datetime:
    """Decode an ISO timestamp with a declared timezone; never infer epoch units or zone."""
    if type(value) is str and len(value) <= _ISO_PARSE_CACHE_MAX_KEY_CHARS:
        return _plain_iso_time(value)
    return _aware_utc(_iso_datetime(value))


def parse_utc_time(value: datetime | str) -> datetime:
    """Decode a format whose boundary explicitly declares that zone-less values mean UTC."""
    parsed = _iso_datetime(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_time(value: TimeInput) -> datetime:
    """Decode an explicitly zoned ISO timestamp; provider epochs require declared units."""
    return parse_iso_time(value)


def iso(value: TimeInput) -> str:
    return parse_time(value).isoformat().replace("+00:00", "Z")


def new_id(prefix: str, *, timestamp: TimeInput | None = None, entropy: int | None = None) -> str:
    aliases = {
        "ITEM": "ITM",
        "EVENT": "EVT",
        "OBSERVATION": "OBS",
        "CLAIM": "CLM",
        "EDGE": "EDG",
        "OUTLOOK": "OUT",
        "DECISION": "DEC",
        "COND": "CND",
        "CONDITION": "CND",
        "CALL": "CAL",
        "ALERT": "ALR",
        "PROPOSAL": "PRP",
        "FIXTURE": "FIX",
        "DELIVERY": "DLV",
        "REVIEW": "REV",
        "STATE": "STC",
        "TRUTH-REQUEST": "REQ",
    }
    prefix = prefix.rstrip("-").upper()
    prefix = aliases.get(prefix, prefix)
    milliseconds = (
        int(time.time() * 1000) if timestamp is None else int(parse_time(timestamp).timestamp() * 1000)
    )
    randomness = secrets.randbits(80) if entropy is None else entropy
    if not 0 <= milliseconds < 2**48 or not 0 <= randomness < 2**80:
        raise ValueError("ULID timestamp/entropy is outside its encoded range")
    value = (milliseconds << 80) | randomness
    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    chars = []
    for _ in range(26):
        chars.append(alphabet[value & 31])
        value >>= 5
    return prefix + "-" + "".join(reversed(chars))


@overload
def jsonable(value: dict[str, JsonValue]) -> dict[str, JsonValue]: ...


@overload
def jsonable(value: Mapping[str, JsonInput]) -> dict[str, JsonValue]: ...


@overload
def jsonable(value: JsonInput) -> JsonValue: ...


def jsonable(value: JsonInput) -> JsonValue:
    if isinstance(value, BaseModel):
        return JsonDocument.validate_value(value.model_dump(mode="json", warnings="error"))
    if isinstance(value, Mapping):
        return {key: jsonable(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(child) for child in value]
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return JsonDocument.validate_value(value.value)
    if isinstance(value, Path):
        return str(value)
    return value


def canonical_json(value: JsonInput) -> str:
    return json.dumps(
        jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def sha256(value: JsonInput | bytes) -> str:
    payload = (
        value
        if isinstance(value, bytes)
        else value.encode()
        if isinstance(value, str)
        else canonical_json(value).encode()
    )
    return hashlib.sha256(payload).hexdigest()
