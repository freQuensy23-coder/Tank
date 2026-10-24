"""JSON values validated only where bytes cross a wire or persistence boundary."""

from math import isfinite

from pydantic import JsonValue, TypeAdapter

JSON_VALUE = TypeAdapter[JsonValue](JsonValue)


def _require_finite(value: JsonValue) -> None:
    if isinstance(value, float) and not isfinite(value):
        raise ValueError("JSON numbers must be finite")
    if isinstance(value, dict):
        for child in value.values():
            _require_finite(child)
    elif isinstance(value, list):
        for child in value:
            _require_finite(child)


def decode_json(payload: str | bytes | bytearray) -> JsonValue:
    value = JSON_VALUE.validate_json(payload, strict=True)
    _require_finite(value)
    return value


def encode_json(value: JsonValue) -> bytes:
    validated = JSON_VALUE.validate_python(value, strict=True)
    _require_finite(validated)
    return JSON_VALUE.dump_json(validated, warnings="error")
