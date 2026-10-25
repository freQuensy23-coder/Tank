"""Fresh statistical reference prices bound only by declared source cadence."""

from bisect import bisect_right
from collections.abc import Mapping
from datetime import datetime
from math import isfinite
from typing import Literal, Self

from pydantic import Field, JsonValue, field_validator, model_validator

from tank.contracts import DomainModel
from tank.util import parse_iso_time

type CadenceSource = Literal["SERIES_CADENCE", "BAR_INTERVAL", "CAPTURE_METADATA", "METRIC_SEMANTICS"]


class PriceCadence(DomainModel):
    seconds: float = Field(gt=0)
    source: CadenceSource


def decode_price_cadence(value: JsonValue, source: CadenceSource) -> PriceCadence | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Declared price cadence requires numeric seconds")
    return PriceCadence(seconds=float(value), source=source)


class ObservedPriceFreshness(DomainModel):
    status: Literal["OBSERVED"]
    observed_at: datetime
    age_s: float = Field(ge=0)
    cadence: PriceCadence

    @field_validator("observed_at")
    @classmethod
    def aware_time(cls, value: datetime) -> datetime:
        return parse_iso_time(value)

    @model_validator(mode="after")
    def fresh(self) -> Self:
        if self.age_s > self.cadence.seconds:
            raise ValueError("Observed reference exceeds its declared cadence")
        return self


class ObservedReferencePrice(ObservedPriceFreshness):
    price: float = Field(gt=0)

    def freshness(self) -> ObservedPriceFreshness:
        return ObservedPriceFreshness(
            status="OBSERVED", observed_at=self.observed_at, age_s=self.age_s, cadence=self.cadence
        )


class UnavailableReferencePrice(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["NO_OBSERVATION", "CADENCE_NOT_CAPTURED", "STALE"]
    observed_at: datetime | None
    age_s: float | None = Field(ge=0)
    cadence: PriceCadence | None

    @field_validator("observed_at")
    @classmethod
    def aware_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else parse_iso_time(value)

    @model_validator(mode="after")
    def known_facets(self) -> Self:
        if self.reason == "NO_OBSERVATION":
            if any(value is not None for value in (self.observed_at, self.age_s, self.cadence)):
                raise ValueError("Missing reference has no measured time or cadence")
        elif self.observed_at is None or self.age_s is None:
            raise ValueError("Unavailable observed reference requires its measured age")
        elif self.reason == "CADENCE_NOT_CAPTURED":
            if self.cadence is not None:
                raise ValueError("Uncaptured cadence cannot declare a freshness allowance")
        elif self.cadence is None or self.age_s <= self.cadence.seconds:
            raise ValueError("Stale reference must exceed its declared cadence")
        return self


type ReferencePrice = ObservedReferencePrice | UnavailableReferencePrice


class ReferencePriceReturn(DomainModel):
    value: float | None
    start: ReferencePrice
    end: ReferencePrice
    reason: Literal["NO_FRESH_ENDPOINTS", "NOT_ESTIMABLE"] | None

    @model_validator(mode="after")
    def availability(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("Unavailable reference return requires its actual reason")
        if self.value is not None and (
            not isinstance(self.start, ObservedReferencePrice)
            or not isinstance(self.end, ObservedReferencePrice)
        ):
            raise ValueError("Reference return requires two fresh observed endpoints")
        return self

    def freshness(self) -> "ReferenceFreshness":
        return ReferenceFreshness(
            start_observation=self.start.freshness()
            if isinstance(self.start, ObservedReferencePrice)
            else self.start,
            end_observation=self.end.freshness()
            if isinstance(self.end, ObservedReferencePrice)
            else self.end,
            reason=self.reason,
        )


class ReferenceFreshness(DomainModel):
    start_observation: ObservedPriceFreshness | UnavailableReferencePrice
    end_observation: ObservedPriceFreshness | UnavailableReferencePrice
    reason: Literal["NO_FRESH_ENDPOINTS", "NOT_ESTIMABLE"] | None


class ReferencePriceSeries:
    def __init__(self, prices: Mapping[datetime, float], cadences: Mapping[datetime, PriceCadence]) -> None:
        self._prices = dict(prices)
        self._cadences = {at: PriceCadence.model_validate(value) for at, value in cadences.items()}
        if not self._cadences.keys() <= self._prices.keys():
            raise ValueError("Reference cadence requires a corresponding observation")
        if any(not isinstance(at, datetime) or at.tzinfo is None or at.utcoffset() is None for at in prices):
            raise ValueError("Reference prices require aware observation clocks")
        if any(isinstance(value, bool) or not isfinite(value) or value <= 0 for value in prices.values()):
            raise ValueError("Reference prices require positive finite values")
        self._times = tuple(sorted(prices))

    def at(self, bound: datetime) -> ReferencePrice:
        target = parse_iso_time(bound)
        index = bisect_right(self._times, target) - 1
        if index < 0:
            return UnavailableReferencePrice(
                status="UNAVAILABLE", reason="NO_OBSERVATION", observed_at=None, age_s=None, cadence=None
            )
        observed = self._times[index]
        age = (target - observed).total_seconds()
        cadence = self._cadences.get(observed)
        if cadence is None:
            return UnavailableReferencePrice(
                status="UNAVAILABLE",
                reason="CADENCE_NOT_CAPTURED",
                observed_at=observed,
                age_s=age,
                cadence=None,
            )
        if age > cadence.seconds:
            return UnavailableReferencePrice(
                status="UNAVAILABLE",
                reason="STALE",
                observed_at=observed,
                age_s=age,
                cadence=cadence,
            )
        return ObservedReferencePrice(
            status="OBSERVED", price=self._prices[observed], observed_at=observed, age_s=age, cadence=cadence
        )

    def return_between(self, start: datetime, end: datetime) -> ReferencePriceReturn:
        if parse_iso_time(start) >= parse_iso_time(end):
            raise ValueError("Reference return requires ordered boundaries")
        before, after = self.at(start), self.at(end)
        if not isinstance(before, ObservedReferencePrice) or not isinstance(after, ObservedReferencePrice):
            return ReferencePriceReturn(value=None, start=before, end=after, reason="NO_FRESH_ENDPOINTS")
        value = after.price / before.price - 1
        return ReferencePriceReturn(
            value=value if isfinite(value) else None,
            start=before,
            end=after,
            reason=None if isfinite(value) else "NOT_ESTIMABLE",
        )
