"""Reported derivative numeric components independently of later semantic qualification."""

from decimal import Decimal, InvalidOperation
from typing import Literal, Self

from pydantic import JsonValue, model_validator

from tank.contracts import DomainModel


class ReportedDerivativeNumber(DomainModel):
    status: Literal["OBSERVED", "NOT_CAPTURED", "NOT_ESTIMABLE"]
    value: Decimal | None

    @model_validator(mode="after")
    def captured_value(self) -> Self:
        if (self.status == "OBSERVED") != (self.value is not None):
            raise ValueError("A reported derivative number must preserve its actual availability")
        return self

    @classmethod
    def from_value(cls, value: JsonValue) -> Self:
        if value is None:
            return cls(status="NOT_CAPTURED", value=None)
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            return cls(status="NOT_ESTIMABLE", value=None)
        try:
            parsed = Decimal(str(value))
        except InvalidOperation:
            return cls(status="NOT_ESTIMABLE", value=None)
        return cls(
            status="OBSERVED" if parsed.is_finite() else "NOT_ESTIMABLE",
            value=parsed if parsed.is_finite() else None,
        )
