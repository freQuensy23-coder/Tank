from array import array
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from math import isfinite
from typing import Literal, Self, overload

from pydantic import Field, GetCoreSchemaHandler, field_validator, model_validator
from pydantic_core import CoreSchema, core_schema

from tank.contracts import DomainModel
from tank.util import parse_iso_time


class StatisticalPopulation(Sequence[float]):
    """Compact finite statistical samples whose arrays never escape the collection."""

    __slots__ = ("__values", "__ordered", "__negative")

    def __init__(self, values: Iterable[float]) -> None:
        samples = array("d")
        for value in values:
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
                raise ValueError("Statistical samples must be finite real numbers")
            samples.append(value)
        self.__values = samples
        self.__ordered = array("d", sorted(samples))
        self.__negative: Self | None = None

    def __len__(self) -> int:
        return len(self.__values)

    def __iter__(self) -> Iterator[float]:
        return iter(self.__values)

    @overload
    def __getitem__(self, index: int) -> float: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[float, ...]: ...

    def __getitem__(self, index: int | slice) -> float | tuple[float, ...]:
        if isinstance(index, slice):
            return tuple(self.__values[index])
        return self.__values[index]

    def ordered_value(self, index: int) -> float:
        return self.__ordered[index]

    def scaled(self, sign: Literal[-1, 1]) -> Self:
        if isinstance(sign, bool) or sign not in (-1, 1):
            raise ValueError("Statistical population scale must be -1 or 1")
        if sign == 1:
            return self
        if self.__negative is None:
            self.__negative = type(self)(-value for value in self.__values)
            self.__negative.__negative = self
        return self.__negative

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type[Self], handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(
            cls.revalidated, core_schema.is_instance_schema(cls)
        )

    @classmethod
    def revalidated(cls, value: Self) -> Self:
        return cls(value)


class FloatBounds(DomainModel):
    lower: float
    upper: float

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.lower > self.upper:
            raise ValueError("Statistical bounds are reversed")
        return self


class DecimalBounds(DomainModel):
    lower: Decimal
    upper: Decimal

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.lower > self.upper:
            raise ValueError("Statistical bounds are reversed")
        return self


RangeUnavailableReason = Literal["INCOMPLETE", "INSUFFICIENT_HISTORY", "DEGENERATE_HISTORY"]


class FloatRange(DomainModel):
    bounds: FloatBounds | None
    reason: RangeUnavailableReason | None

    @model_validator(mode="after")
    def availability(self) -> Self:
        if (self.bounds is None) != (self.reason is not None):
            raise ValueError("Unavailable statistical ranges require a reason")
        return self


class DecimalRange(DomainModel):
    bounds: DecimalBounds | None
    reason: Literal["INCOMPLETE", "INSUFFICIENT_HISTORY"] | None

    @model_validator(mode="after")
    def availability(self) -> Self:
        if (self.bounds is None) != (self.reason is not None):
            raise ValueError("Unavailable statistical ranges require a reason")
        return self


class OpenInterestBaseline(DomainModel):
    native: FloatRange
    usd: FloatRange


class FundingBaseline(DomainModel):
    equivalent_eight_hour_rate: DecimalRange


class EstimatedBeta(DomainModel):
    kind: Literal["ESTIMATED"]
    value: float
    paired_sample_count: int = Field(ge=1)


class AssumedBeta(DomainModel):
    kind: Literal["ASSUMED"]
    value: float
    reason: Literal["INSUFFICIENT_HISTORY", "NOT_ESTIMABLE"]
    paired_sample_count: int = Field(ge=0)

    @field_validator("value")
    @classmethod
    def declared_assumption(cls, value: float) -> float:
        if value != 1.0:
            raise ValueError("Unavailable regression uses the explicit unit-beta assumption")
        return value


class DurationBounds(DomainModel):
    lower: float = Field(ge=0)
    upper: float = Field(ge=0)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.lower > self.upper:
            raise ValueError("Duration bounds are reversed")
        return self


class HistoryDiagnostics(DomainModel):
    target_window_duration_s: float = Field(gt=0)
    observed_duration_bounds_s: DurationBounds | None
    endpoint_staleness_bounds_s: DurationBounds | None
    exact: bool
    resolution_s: float = Field(gt=0)
    alignment: Literal["AS_OF_AT_EXACT_TARGET_BOUNDS"]
    sample_count: int = Field(ge=0)
    latest_observed_at: datetime | None

    @field_validator("latest_observed_at")
    @classmethod
    def aware_latest(cls, value: datetime | None) -> datetime | None:
        return None if value is None else parse_iso_time(value)

    @model_validator(mode="after")
    def observations(self) -> Self:
        facets = (
            self.observed_duration_bounds_s,
            self.endpoint_staleness_bounds_s,
            self.latest_observed_at,
        )
        if self.sample_count == 0:
            if any(value is not None for value in facets) or self.exact:
                raise ValueError("Empty history cannot declare measured endpoints")
        elif any(value is None for value in facets):
            raise ValueError("Observed history requires measured endpoints")
        staleness = self.endpoint_staleness_bounds_s
        if staleness is not None and self.exact != (staleness.upper == 0):
            raise ValueError("History exactness must match measured endpoint staleness")
        return self


class RelativeHistory(DomainModel):
    samples: StatisticalPopulation
    diagnostics: HistoryDiagnostics

    @model_validator(mode="after")
    def count(self) -> Self:
        if len(self.samples) != self.diagnostics.sample_count:
            raise ValueError("History sample count disagrees with its population")
        return self


class BitcoinContextHistory(DomainModel):
    returns: StatisticalPopulation
    volatility: StatisticalPopulation


class HourlyReturns(DomainModel):
    by_time: dict[datetime, float]

    @field_validator("by_time")
    @classmethod
    def aware_keys(cls, values: dict[datetime, float]) -> dict[datetime, float]:
        for value in values:
            parse_iso_time(value)
        return values


class NativeVolumeBoundary(DomainModel):
    cumulative: Decimal = Field(ge=0)
    segment: int = Field(ge=0)


class NativeVolumeIndex(DomainModel):
    bounds: dict[datetime, NativeVolumeBoundary]
    running: Decimal = Field(ge=0)
    segment: int = Field(ge=0)
    last: datetime | None
    last_open_us: int | None

    @field_validator("last")
    @classmethod
    def aware_last(cls, value: datetime | None) -> datetime | None:
        return None if value is None else parse_iso_time(value)

    @field_validator("bounds")
    @classmethod
    def aware_bounds(
        cls, values: dict[datetime, NativeVolumeBoundary]
    ) -> dict[datetime, NativeVolumeBoundary]:
        for value in values:
            parse_iso_time(value)
        return values


class DepthBaseline(DomainModel):
    mean_usd: Decimal = Field(ge=0)


@dataclass(slots=True)
class BaselineCache[T]:
    capacity: int
    _entries: dict[str, T] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.capacity, bool) or self.capacity < 1:
            raise ValueError("Baseline cache capacity must be positive")

    def get(self, key: str) -> T | None:
        return self._entries.get(key)

    def put(self, key: str, value: T) -> None:
        if key not in self._entries and len(self._entries) == self.capacity:
            del self._entries[next(iter(self._entries))]
        self._entries[key] = value

    def remember(self, key: str, build: Callable[[], T]) -> T:
        if key not in self._entries:
            self.put(key, build())
        return self._entries[key]


@dataclass(slots=True)
class BaselineCaches:
    open_interest: BaselineCache[OpenInterestBaseline]
    funding: BaselineCache[FundingBaseline]
    hourly_returns: BaselineCache[HourlyReturns]
    beta: BaselineCache[EstimatedBeta | AssumedBeta]
    relative_history: BaselineCache[RelativeHistory]
    bitcoin_context: BaselineCache[BitcoinContextHistory]
    native_volume: BaselineCache[NativeVolumeIndex]
    depth: BaselineCache[DepthBaseline]
