"""JSON values validated only where bytes cross a wire or persistence boundary."""

from math import isfinite

from pydantic import ConfigDict, JsonValue, RootModel


class JsonDocument(RootModel[JsonValue]):
    model_config = ConfigDict(strict=True)

    @classmethod
    def validate_value(cls, value: JsonValue) -> JsonValue:
        return cls.model_validate(value, strict=True).root


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
    value = JsonDocument.model_validate_json(payload, strict=True).root
    _require_finite(value)
    return value


def encode_json(value: JsonValue) -> bytes:
    validated = JsonDocument.validate_value(value)
    _require_finite(validated)
    return JsonDocument(validated).model_dump_json(warnings="error").encode()
