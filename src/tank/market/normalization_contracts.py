"""Consumed registry scope for the retained exchange normalization adapters."""

from collections.abc import Mapping
from typing import Self

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.registry import RegistryMarketType, RegistryVenue


class NormalizationMarket(DomainModel):
    venue: RegistryVenue
    market_type: RegistryMarketType

    @classmethod
    def from_wire(cls, instrument: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({key: instrument[key] for key in cls.model_fields if key in instrument})


class NormalizationInstrument(NormalizationMarket):
    instrument_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
