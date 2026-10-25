"""JSON values validated only where bytes cross a wire or persistence boundary."""

import json
from math import isfinite

from pydantic import ConfigDict, JsonValue, RootModel


class JsonDocument(RootModel[JsonValue]):
    model_config = ConfigDict(strict=True, hide_input_in_errors=True)

    @classmethod
    def validate_value(cls, value: JsonValue) -> JsonValue:
        validated = cls.model_validate(value, strict=True).root
        _require_finite(validated)
        return validated


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
    return JsonDocument(validated).model_dump_json(warnings="error").encode()


class DuplicateJsonKeyError(ValueError):
    """A raw JSON object repeats a decoded field name."""


def _require_unique_keys(pairs: list[tuple[str, JsonValue]]) -> None:
    names: set[str] = set()
    for name, _value in pairs:
        if name in names:
            raise DuplicateJsonKeyError("JSON object keys must be unique")
        names.add(name)


def decode_unique_json(payload: str | bytes | bytearray) -> JsonValue:
    """Refuse ambiguous raw objects without replacing the authoritative JSON decoder."""
    try:
        json.loads(payload, object_pairs_hook=_require_unique_keys)
    except DuplicateJsonKeyError:
        raise
    except (ValueError, RecursionError):
        uniqueness_checked = False
    else:
        uniqueness_checked = True
    value = decode_json(payload)
    if not uniqueness_checked:
        raise ValueError("JSON object-key uniqueness could not be validated") from None
    return value
