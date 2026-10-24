from pydantic import JsonValue

from tank.models import Record


def present[T](value: T | None) -> T:
    """Require the persisted fixture or successful claim that this test expects."""
    assert value is not None
    return value


def text_field(record: Record, field: str) -> str:
    value = record[field]
    assert isinstance(value, str)
    return value


def object_field(record: Record, field: str) -> dict[str, JsonValue]:
    """Require the actual JSON object at a persisted associative-index field."""
    value = record[field]
    assert isinstance(value, dict)
    return value
