from collections.abc import Mapping

from pydantic import JsonValue


def present[T](value: T | None) -> T:
    """Require the persisted fixture or successful claim that this test expects."""
    assert value is not None
    return value


def text_field(record: Mapping[str, JsonValue], field: str) -> str:
    value = record[field]
    assert isinstance(value, str)
    return value


def object_field(record: Mapping[str, JsonValue], field: str) -> dict[str, JsonValue]:
    """Require the actual JSON object at a persisted associative-index field."""
    value = record[field]
    assert isinstance(value, dict)
    return value


def array_field(record: Mapping[str, JsonValue], field: str) -> list[JsonValue]:
    """Require the actual JSON array in a stored or presentation fixture."""
    value = record[field]
    assert isinstance(value, list)
    return value
