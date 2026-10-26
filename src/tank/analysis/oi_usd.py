"""Recorded USD components independently of native OI and new FX derivations."""

from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_derivative_inputs import OIObservation
from tank.analysis.condition_graph_contracts import (
    ConditionOISynchronization,
    ConditionOISynchronizationContribution,
    ConditionOISynchronizationEndpoint,
)
from tank.analysis.oi_synchronization import ObservedOISynchronizedSelection
from tank.contracts import DomainModel
from tank.time_precision import Timestamp


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


def _recorded_endpoint(
    at: Timestamp, sources: tuple[OIObservation, ...]
) -> ConditionOISynchronizationEndpoint:
    contributions: list[ConditionOISynchronizationContribution] = []
    for source in sources:
        native = source.native_quantity.value
        if native is None or native < 0:
            raise ValueError("Recorded OI valuation requires the actual selected native component")
        usd = recorded_oi_usd(source)
        contributions.append(
            ConditionOISynchronizationContribution(
                instrument_id=source.instrument.instrument_id,
                venue=source.instrument.venue,
                observed_at=source.observed_at,
                oi_native=native,
                oi_usd=usd.value if isinstance(usd, RecordedOIUsd) else None,
            )
        )
    return ConditionOISynchronizationEndpoint(at=at, contributions=tuple(contributions))


def recorded_oi_synchronization(
    selection: ObservedOISynchronizedSelection,
) -> ConditionOISynchronization:
    """Attach stored USD components to the exact selected native source cohort.

    Missing or invalid USD does not remove an instrument or its native amount.
    Endpoint totals remain unavailable when any USD contribution is missing.
    This does not derive a new amount from mark prices or assert FX provenance.
    """
    selection = ObservedOISynchronizedSelection.model_validate(selection)
    return ConditionOISynchronization(
        aggregation_class=selection.native_witness.aggregation_class,
        tolerance_s=selection.native_witness.tolerance_s,
        start=_recorded_endpoint(
            selection.native_witness.start.at, tuple(source.start for source in selection.sources)
        ),
        end=_recorded_endpoint(
            selection.native_witness.end.at, tuple(source.end for source in selection.sources)
        ),
    )
