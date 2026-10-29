from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tank import util


def test_repeated_plain_timestamp_text_is_parsed_once(monkeypatch):
    calls = []
    parser = getattr(util, "_parse_text_time", None)
    if parser is not None:
        parser.cache_clear()

    class ObservedDatetime(datetime):
        @classmethod
        def fromisoformat(cls, value):
            calls.append(value)
            return super().fromisoformat(value)

    monkeypatch.setattr(util, "datetime", ObservedDatetime)
    try:
        for _ in range(12):
            assert util.parse_time("2026-10-01T12:34:56.123456Z") == datetime(
                2026, 10, 1, 12, 34, 56, 123456, UTC
            )
        assert len(calls) <= 1
    finally:
        if parser is not None:
            parser.cache_clear()


@pytest.mark.parametrize(
    "value",
    ["0", "86400.125", "1728000000000", "1728000000000000", "2026-10-01T12:34:56.123456+02:00"],
)
def test_timestamp_text_reuse_preserves_epoch_units_and_timezone(value):
    if "T" in value:
        expected = datetime.fromisoformat(value).astimezone(UTC)
    else:
        expected = util.parse_time(Decimal(value))
    assert util.parse_time(value) == expected
    assert util.parse_time(value) == expected


def test_timestamp_text_subclass_keeps_its_conversion_hooks():
    calls = []

    class ObservedText(str):
        def replace(self, *args, **kwargs):
            calls.append(args)
            return super().replace(*args, **kwargs)

    value = ObservedText("2026-10-01T12:34:56Z")
    assert util.parse_time(value) == util.parse_time(str(value))
    assert util.parse_time(value) == util.parse_time(str(value))
    assert len(calls) == 2


def test_invalid_timestamp_text_still_raises_on_each_call():
    for _ in range(2):
        with pytest.raises(ValueError):
            util.parse_time("not-a-market-timestamp")
