"""JSON values validated only where bytes cross a wire or persistence boundary."""

from pydantic import JsonValue, TypeAdapter

JSON_VALUE = TypeAdapter[JsonValue](JsonValue)


def decode_json(payload: str | bytes | bytearray) -> JsonValue:
    return JSON_VALUE.validate_json(payload, strict=True)


def encode_json(value: JsonValue) -> bytes:
    return JSON_VALUE.dump_json(value, warnings="error")
