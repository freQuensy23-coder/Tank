import hashlib
import json
import secrets
import time
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import overload

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
    | dict[str, JsonInput]
    | list[JsonInput]
    | tuple[JsonInput, ...]
    | set[JsonInput]
)

type TimeInput = datetime | str | int | float | Decimal


def utcnow() -> datetime:
    return datetime.now(UTC)


def parse_time(value: TimeInput) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, (int, float, Decimal)) or (
        isinstance(value, str) and value.replace(".", "", 1).isdigit()
    ):
        number = float(value)
        if abs(number) > 1e14:
            number /= 1e6
        elif abs(number) > 1e11:
            number /= 1e3
        return datetime.fromtimestamp(number, UTC)
    return parse_time(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def iso(value: TimeInput | None = None) -> str:
    return parse_time(value if value is not None else utcnow()).isoformat().replace("+00:00", "Z")


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
def jsonable(value: dict[str, JsonInput]) -> dict[str, JsonValue]: ...


@overload
def jsonable(value: JsonInput) -> JsonValue: ...


def jsonable(value: JsonInput) -> JsonValue:
    if isinstance(value, BaseModel):
        return JsonDocument.validate_value(value.model_dump(mode="json", warnings="error"))
    if isinstance(value, dict):
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
