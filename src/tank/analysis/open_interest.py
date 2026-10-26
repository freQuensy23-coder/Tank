"""Pure OI endpoint selection over owned, validated source observations."""

from datetime import datetime
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_derivative_inputs import (
    DerivativeInstrument,
    OIInputEvidenceRef,
    OIObservation,
    OIReadSnapshot,
    OISelectionPolicy,
)
from tank.analysis.oi_ratio import NativeOiChange, native_oi_change
from tank.analysis.statistics_contracts import FloatRange
from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import parse_iso_time

type OIObservationExclusionReason = Literal[
    "SOURCE_CLOCK_NOT_CAPTURED",
    "SOURCE_AFTER_CUTOFF",
    "OI_BUCKET_EDGE_UNSPECIFIED",
    "RECEIPT_NOT_CAPTURED",
    "RECEIPT_AFTER_ASSEMBLY",
]


class OIWindowRequest(DomainModel):
    start: Timestamp
    cutoff: Timestamp
    assembled_at: Timestamp

    @model_validator(mode="after")
    def actual_window(self) -> Self:
        if self.start.value is None or self.cutoff.value is None:
            raise ValueError("OI endpoint selection requires actual logical window bounds")
        if parse_iso_time(self.start.value) >= parse_iso_time(self.cutoff.value):
            raise ValueError("OI endpoint selection requires a forward logical window")
        return self


class ExcludedOIObservation(DomainModel):
    evidence: OIInputEvidenceRef
    reason: OIObservationExclusionReason


class SelectedOIEndpoint(DomainModel):
    status: Literal["OBSERVED"]
    at: Timestamp
    observation: OIObservation
    tolerance_s: int = Field(ge=0)

    @model_validator(mode="after")
    def actual_source_endpoint(self) -> Self:
        source = self.observation.reported_time()
        if self.at.value is None or source is None:
            raise ValueError("Selected OI endpoint requires actual grid and source clocks")
        if abs((source - parse_iso_time(self.at.value)).total_seconds()) > self.tolerance_s:
            raise ValueError("Selected OI endpoint must satisfy the declared tolerance")
        if self.observation.explicitly_unresolved_edge():
            raise ValueError("An unresolved OI bucket edge cannot establish a selected endpoint")
        native = self.observation.native_quantity.value
        if native is None or native < 0:
            raise ValueError("Selected OI endpoint requires observed nonnegative native quantity")
        return self


class UnavailableOIEndpoint(DomainModel):
    status: Literal["UNAVAILABLE"]
    at: Timestamp
    reason: Literal[
        "ASSEMBLY_CLOCK_NOT_CAPTURED",
        "NO_ENDPOINT_SAMPLE",
        "AMBIGUOUS_ENDPOINT_SAMPLE",
        "NATIVE_QUANTITY_NOT_CAPTURED",
        "NATIVE_QUANTITY_INVALID",
    ]


type OIEndpoint = SelectedOIEndpoint | UnavailableOIEndpoint


class OIInstrumentWindow(DomainModel):
    instrument: DerivativeInstrument
    start: OIEndpoint
    end: OIEndpoint
    excluded: tuple[ExcludedOIObservation, ...]

    @model_validator(mode="after")
    def selected_instrument(self) -> Self:
        for endpoint in (self.start, self.end):
            if (
                isinstance(endpoint, SelectedOIEndpoint)
                and endpoint.observation.instrument != self.instrument
            ):
                raise ValueError("OI endpoints must retain their actual instrument scope")
        if self.start.at.value is None or self.end.at.value is None:
            raise ValueError("An OI window requires both actual logical bounds")
        if parse_iso_time(self.start.at.value) >= parse_iso_time(self.end.at.value):
            raise ValueError("An OI window requires forward logical bounds")
        if isinstance(self.start, SelectedOIEndpoint) and isinstance(self.end, SelectedOIEndpoint):
            initial = self.start.observation.reported_time()
            final = self.end.observation.reported_time()
            if (
                initial is None
                or final is None
                or initial > final
                or final > parse_iso_time(self.end.at.value)
            ):
                raise ValueError("Selected OI source endpoints cannot run backwards")
        return self

    def native_change(self, *, normal_range: FloatRange) -> NativeOiChange | None:
        if not isinstance(self.start, SelectedOIEndpoint) or not isinstance(self.end, SelectedOIEndpoint):
            return None
        initial = self.start.observation.native_quantity.value
        final = self.end.observation.native_quantity.value
        if initial is None or final is None:
            raise ValueError("Observed OI endpoints must retain their actual native quantities")
        return native_oi_change(initial, final, normal_range=normal_range)


def _exclusion(point: OIObservation, request: OIWindowRequest) -> OIObservationExclusionReason | None:
    source = point.reported_time()
    if source is None:
        return "SOURCE_CLOCK_NOT_CAPTURED"
    if request.cutoff.value is None:
        raise ValueError("Validated OI request lost its actual cutoff")
    if source > parse_iso_time(request.cutoff.value):
        return "SOURCE_AFTER_CUTOFF"
    if point.explicitly_unresolved_edge():
        return "OI_BUCKET_EDGE_UNSPECIFIED"
    if point.received_at.value is None:
        return "RECEIPT_NOT_CAPTURED"
    if point.visible_at(request.assembled_at) is False:
        return "RECEIPT_AFTER_ASSEMBLY"
    return None


def _endpoint(
    points: tuple[OIObservation, ...], at: Timestamp, *, tolerance_s: int, assembled_at: Timestamp
) -> OIEndpoint:
    if assembled_at.value is None:
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="ASSEMBLY_CLOCK_NOT_CAPTURED")
    if at.value is None:
        raise ValueError("OI endpoint target must retain its actual logical clock")
    target = parse_iso_time(at.value)
    candidates: list[tuple[float, OIObservation]] = []
    for point in points:
        source: datetime | None = point.reported_time()
        if source is not None:
            distance = abs((source - target).total_seconds())
            if distance <= tolerance_s:
                candidates.append((distance, point))
    if not candidates:
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="NO_ENDPOINT_SAMPLE")
    distance = min(value[0] for value in candidates)
    nearest = [point for skew, point in candidates if skew == distance]
    if len(nearest) != 1:
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="AMBIGUOUS_ENDPOINT_SAMPLE")
    selected = nearest[0]
    quantity = selected.native_quantity
    if quantity.status == "NOT_CAPTURED":
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="NATIVE_QUANTITY_NOT_CAPTURED")
    if quantity.value is None or quantity.value < Decimal(0):
        return UnavailableOIEndpoint(status="UNAVAILABLE", at=at, reason="NATIVE_QUANTITY_INVALID")
    return SelectedOIEndpoint(status="OBSERVED", at=at, observation=selected, tolerance_s=tolerance_s)


class OpenInterestCalculator:
    """Own immutable admitted reads once; keep actual assembly in each request."""

    __slots__ = ("_source", "_policy")

    def __init__(self, source: OIReadSnapshot, policy: OISelectionPolicy) -> None:
        self._source = OIReadSnapshot.model_validate(source)
        self._policy = OISelectionPolicy.model_validate(policy)

    def select(self, request: OIWindowRequest) -> tuple[OIInstrumentWindow, ...]:
        actual = OIWindowRequest.model_validate(request)
        result: list[OIInstrumentWindow] = []
        for instrument in self._source.instruments:
            points: list[OIObservation] = []
            excluded: list[ExcludedOIObservation] = []
            for point in self._source.observations:
                if point.instrument != instrument:
                    continue
                reason = _exclusion(point, actual)
                if reason is None:
                    points.append(point)
                else:
                    excluded.append(ExcludedOIObservation(evidence=point.evidence, reason=reason))
            admitted = tuple(points)
            result.append(
                OIInstrumentWindow(
                    instrument=instrument,
                    start=_endpoint(
                        admitted,
                        actual.start,
                        tolerance_s=self._policy.tolerance_s,
                        assembled_at=actual.assembled_at,
                    ),
                    end=_endpoint(
                        admitted,
                        actual.cutoff,
                        tolerance_s=self._policy.tolerance_s,
                        assembled_at=actual.assembled_at,
                    ),
                    excluded=tuple(excluded),
                )
            )
        return tuple(result)
