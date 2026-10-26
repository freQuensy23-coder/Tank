"""Recorded USD components independently of native OI and new FX derivations."""

from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_derivative_inputs import OIObservation
from tank.contracts import DomainModel


class RecordedOIUsd(DomainModel):
    status: Literal["RECORDED_USD"]
    value: Decimal = Field(ge=0)
    source: OIObservation

    @model_validator(mode="after")
    def actual_recorded_component(self) -> Self:
        if self.source.reported_usd.status != "OBSERVED" or self.source.reported_usd.value != self.value:
            raise ValueError("Recorded OI USD must match its actual stored component")
        return self


class UnavailableRecordedOIUsd(DomainModel):
    status: Literal["UNAVAILABLE"]
    reason: Literal["RECORDED_USD_NOT_CAPTURED", "REPORTED_USD_INVALID"]
    source: OIObservation

    @model_validator(mode="after")
    def actual_component_absence(self) -> Self:
        component = self.source.reported_usd
        if component.status == "OBSERVED" and component.value is not None and component.value >= 0:
            raise ValueError("A known recorded OI USD component cannot become unavailable")
        expected = (
            "RECORDED_USD_NOT_CAPTURED" if component.status == "NOT_CAPTURED" else "REPORTED_USD_INVALID"
        )
        if self.reason != expected:
            raise ValueError("Recorded OI USD absence must retain its actual component cause")
        return self


type RecordedOIUsdComponent = RecordedOIUsd | UnavailableRecordedOIUsd


def recorded_oi_usd(source: OIObservation) -> RecordedOIUsdComponent:
    """Admit a stored amount without asserting provider or historical FX origin.

    Source and receipt clocks remain unchanged. Request visibility is a separate
    gate; a mark price cannot replace a missing or invalid stored USD component.
    """
    source = OIObservation.model_validate(source)
    component = source.reported_usd
    if component.status == "OBSERVED" and component.value is not None and component.value >= 0:
        return RecordedOIUsd(status="RECORDED_USD", value=component.value, source=source)
    return UnavailableRecordedOIUsd(
        status="UNAVAILABLE",
        reason=(
            "RECORDED_USD_NOT_CAPTURED" if component.status == "NOT_CAPTURED" else "REPORTED_USD_INVALID"
        ),
        source=source,
    )
