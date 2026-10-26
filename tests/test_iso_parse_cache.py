"""Only immutable plain ISO strings share successful UTC parse results."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, tzinfo
from typing import SupportsIndex

import pytest

from tank.util import _plain_iso_time, iso, parse_epoch, parse_iso_time, parse_time, parse_utc_time


@pytest.fixture(autouse=True)
def isolated_cache() -> Iterator[None]:
    _plain_iso_time.cache_clear()
    yield
    _plain_iso_time.cache_clear()


@pytest.mark.parametrize(
    "value",
    [
        "2026-10-26T10:20:30Z",
        "2026-10-26T10:20:30.123456+03:00",
        "2026-10-26t10:20:30-07:30",
        "1970-01-01T00:00:00+00:00",
    ],
)
def test_plain_string_cache_preserves_utc_values_and_actual_callers(value: str) -> None:
    expected = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    first = parse_iso_time(value)
    assert first == expected and first.tzinfo is UTC
    assert parse_iso_time(value) is first
    assert parse_time(value) == expected
    assert iso(value) == expected.isoformat().replace("+00:00", "Z")
    assert _plain_iso_time.cache_info().misses == 1
    assert _plain_iso_time.cache_info().hits == 3


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("2026-10-26T10:20:30", "Timestamp requires an explicit timezone"),
        ("2026-10-26", "Timestamp requires an explicit timezone"),
        ("0", "Numeric timestamps require an explicit epoch unit"),
        ("-1.5", "Numeric timestamps require an explicit epoch unit"),
        ("not a timestamp", "Invalid ISO timestamp"),
        ("10000-01-01T00:00:00Z", "Invalid ISO timestamp"),
    ],
)
def test_refusals_keep_their_original_error_and_are_not_cached(value: str, message: str) -> None:
    for _ in range(2):
        with pytest.raises(ValueError, match="^" + message + "$"):
            parse_iso_time(value)
    assert _plain_iso_time.cache_info().currsize == 0
    assert _plain_iso_time.cache_info().misses == 2


class MutableZone(tzinfo):
    def __init__(self, hours: int) -> None:
        self.hours = hours

    def utcoffset(self, dt: datetime | None) -> timedelta:
        return timedelta(hours=self.hours)

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(0)

    def tzname(self, dt: datetime | None) -> str:
        return "MUTABLE"


def test_datetime_with_mutable_timezone_is_revalidated_on_every_call() -> None:
    zone = MutableZone(1)
    value = datetime(2026, 10, 26, 12, tzinfo=zone)
    first = parse_iso_time(value)
    zone.hours = 2
    second = parse_iso_time(value)
    assert first == datetime(2026, 10, 26, 11, tzinfo=UTC)
    assert second == datetime(2026, 10, 26, 10, tzinfo=UTC)
    zone.hours = 24
    with pytest.raises(ValueError):
        parse_iso_time(value)
    assert _plain_iso_time.cache_info().currsize == 0


class MutableIso(str):
    replacement: str

    def replace(self, old: str, new: str, count: SupportsIndex = -1) -> str:
        if old == "Z":
            return self.replacement.replace(old, new, count)
        return super().replace(old, new, count)


def test_string_subclass_behavior_and_errors_are_never_reused() -> None:
    value = MutableIso("2026-10-26T12:00:00Z")
    value.replacement = "2026-10-26T12:00:00Z"
    assert parse_iso_time(value) == datetime(2026, 10, 26, 12, tzinfo=UTC)
    value.replacement = "2026-10-26T13:00:00Z"
    assert parse_iso_time(value) == datetime(2026, 10, 26, 13, tzinfo=UTC)
    value.replacement = "invalid"
    with pytest.raises(ValueError, match="Invalid ISO timestamp"):
        parse_iso_time(value)
    assert _plain_iso_time.cache_info().currsize == 0


def test_cache_is_bounded_and_does_not_change_explicit_epoch_or_utc_policy() -> None:
    start = datetime(2026, 10, 26, tzinfo=UTC)
    first = start.isoformat()
    for second in range(4100):
        parse_iso_time((start + timedelta(seconds=second)).isoformat())
    before = _plain_iso_time.cache_info()
    assert before.maxsize == 4096 and before.currsize == 4096 and before.misses == 4100
    assert parse_iso_time(first) == start
    assert _plain_iso_time.cache_info().misses == before.misses + 1
    assert parse_epoch(0, unit="s") == datetime(1970, 1, 1, tzinfo=UTC)
    assert parse_utc_time("2026-10-26T00:00:00") == start
    with pytest.raises(ValueError, match="explicit timezone"):
        parse_iso_time(datetime(2026, 10, 26))


def test_long_valid_fraction_is_accepted_without_retaining_its_large_key() -> None:
    value = "2026-10-26T00:00:00." + "123456" * 1000 + "Z"
    expected = datetime(2026, 10, 26, microsecond=123456, tzinfo=UTC)
    assert parse_iso_time(value) == expected
    assert parse_iso_time(value) == expected
    assert _plain_iso_time.cache_info().currsize == 0
