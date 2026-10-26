"""Operational cache metadata; market payloads remain at their storage wire boundary."""

from datetime import datetime
from typing import Annotated

from pydantic import Field, field_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import OneInt
from tank.models import Record
from tank.util import parse_iso_time

type DayCountKey = tuple[str, datetime]
type DependencyHeadKey = tuple[str, str, tuple[tuple[str, str], ...], str, str]
type NonnegativeCount = Annotated[int, Field(ge=0)]
type InstrumentID = Annotated[str, Field(min_length=1)]
type DayCount = Annotated[int, Field(ge=0, le=1440)]


class RequestCounts(DomainModel):
    succeeded: NonnegativeCount
    failed: NonnegativeCount

    def success(self) -> "RequestCounts":
        return RequestCounts(succeeded=self.succeeded + 1, failed=self.failed)

    def failure(self) -> "RequestCounts":
        return RequestCounts(succeeded=self.succeeded, failed=self.failed + 1)


class SavedDayCounts(DomainModel):
    counts: dict[DayCountKey, DayCount]
    monotonic_at: float = Field(ge=0, allow_inf_nan=False)

    @field_validator("counts")
    @classmethod
    def aligned_keys(cls, value: dict[DayCountKey, int]) -> dict[DayCountKey, int]:
        for instrument_id, day in value:
            if not instrument_id:
                raise ValueError("Day counts require instrument identity")
            _utc_day(day)
        return value


def _utc_day(value: datetime | str) -> datetime:
    instant = parse_iso_time(value)
    if instant.hour or instant.minute or instant.second or instant.microsecond:
        raise ValueError("Day counts require an aligned UTC day")
    return instant


class SavedKlineDayCounts(DomainModel):
    version: OneInt
    saved_at: str
    counts: list[tuple[InstrumentID, str, DayCount]]

    @field_validator("saved_at")
    @classmethod
    def aware_clock(cls, value: str) -> str:
        parse_iso_time(value)
        return value

    @field_validator("counts")
    @classmethod
    def aligned_days(cls, value: list[tuple[str, str, int]]) -> list[tuple[str, str, int]]:
        seen: set[DayCountKey] = set()
        for instrument_id, day, _ in value:
            key = (instrument_id, _utc_day(day))
            if key in seen:
                raise ValueError("Day counts require unique instrument and UTC day")
            seen.add(key)
        return value


class DependencyHeadMemo(DomainModel):
    at: datetime
    heads: tuple[Record | None, Record | None, Record | None]
    computed: float = Field(ge=0, allow_inf_nan=False)

    @field_validator("at")
    @classmethod
    def aware_clock(cls, value: datetime) -> datetime:
        return parse_iso_time(value)
