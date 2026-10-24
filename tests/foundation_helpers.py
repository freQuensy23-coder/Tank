from tank.models import Record


def present[T](value: T | None) -> T:
    """Require the persisted fixture or successful claim that this test expects."""
    assert value is not None
    return value


def text_field(record: Record, field: str) -> str:
    value = record[field]
    assert isinstance(value, str)
    return value
