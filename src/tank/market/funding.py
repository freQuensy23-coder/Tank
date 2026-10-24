"""Pure funding calculations over validated observations and explicit clock provenance."""

from decimal import Decimal
from typing import Literal

from pydantic import Field, field_validator

from tank.contracts import DomainModel


class FundingObservation(DomainModel):
    """A provider rate per declared funding interval; zero is an observed rate."""

    instrument_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    kind: Literal["SETTLED", "CURRENT_ESTIMATE"]
    rate: Decimal
    interval_h: Decimal = Field(gt=0)
    timestamp: str = Field(min_length=1)
    source_clock: bool
    metric_key: str = Field(min_length=1)

    @field_validator("rate", "interval_h")
    @classmethod
    def finite(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("funding values must be finite")
        return value

    @property
    def rate_8h_eq(self) -> Decimal:
        """Normalize a per-interval fractional rate to eight hours, without rounding."""
        from decimal import localcontext

        with localcontext() as context:
            context.prec = 100
            return self.rate * 8 / self.interval_h
