"""Funding selection over one owned immutable read, with exact selected source identities."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Literal, Self

from pydantic import model_validator

from tank.analysis.condition_derivative_inputs import DerivativeInstrument
from tank.analysis.condition_funding_input_codec import decode_funding_inputs
from tank.analysis.condition_funding_inputs import (
    FundingInputEvidenceRef,
    FundingInputObservation,
    FundingReadSnapshot,
)
from tank.analysis.funding_time import funding_clock_bounds, funding_source_before
from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import JsonInput, parse_iso_time

type FundingExclusionReason = Literal[
    "ASSEMBLY_CLOCK_NOT_CAPTURED",
    "SOURCE_CLOCK_NOT_CAPTURED",
    "SOURCE_AFTER_CUTOFF",
    "RECEIPT_NOT_CAPTURED",
    "RECEIPT_AFTER_ASSEMBLY",
    "RECEIPT_ORDER_UNCERTAIN",
    "SOURCE_CLOCK_NOT_ORDERABLE",
]
type FundingSelectionRole = Literal["LATEST_SETTLED", "LATEST_ESTIMATE", "SETTLED_START"]


class FundingWindowRequest(DomainModel):
    start: Timestamp
    cutoff: Timestamp
    assembled_at: Timestamp

    @model_validator(mode="after")
    def actual_bounds(self) -> Self:
        if self.start.value is None or self.cutoff.value is None:
            raise ValueError("Funding windows require actual start and cutoff clocks")
        if parse_iso_time(self.start.value) >= parse_iso_time(self.cutoff.value):
            raise ValueError("Funding calculation requires a forward window")
        return self


class ExcludedFundingObservation(DomainModel):
    source: FundingInputEvidenceRef
    reason: FundingExclusionReason


class SelectedFundingObservation(DomainModel):
    status: Literal["OBSERVED"]
    role: FundingSelectionRole
    observation: FundingInputObservation

    @model_validator(mode="after")
    def actual_kind(self) -> Self:
        expected = "CURRENT_ESTIMATE" if self.role == "LATEST_ESTIMATE" else "SETTLED"
        if self.observation.kind != expected or self.observation.clock.timestamp.value is None:
            raise ValueError("Selected funding requires its actual kind and observation clock")
        return self


class UnavailableFundingSelection(DomainModel):
    status: Literal["UNAVAILABLE"]
    role: FundingSelectionRole
    reason: Literal["NO_ELIGIBLE_OBSERVATION", "AMBIGUOUS_LATEST_SOURCE"]
    candidates: tuple[FundingInputEvidenceRef, ...]

    @model_validator(mode="after")
    def actual_candidates(self) -> Self:
        if len(set(self.candidates)) != len(self.candidates):
            raise ValueError("Funding selection cannot repeat an evidence candidate")
        if (self.reason == "AMBIGUOUS_LATEST_SOURCE") != (len(self.candidates) > 1):
            raise ValueError("Ambiguous funding requires distinct actual latest candidates")
        if self.reason == "NO_ELIGIBLE_OBSERVATION" and self.candidates:
            raise ValueError("Missing funding cannot invent candidate observations")
        return self


type FundingObservationSelection = SelectedFundingObservation | UnavailableFundingSelection


class FundingInstrumentSelection(DomainModel):
    instrument: DerivativeInstrument
    request: FundingWindowRequest
    settled: FundingObservationSelection
    estimate: FundingObservationSelection
    start: FundingObservationSelection
    excluded: tuple[ExcludedFundingObservation, ...]

    @model_validator(mode="after")
    def actual_scope(self) -> Self:
        for selection, role in (
            (self.settled, "LATEST_SETTLED"),
            (self.estimate, "LATEST_ESTIMATE"),
            (self.start, "SETTLED_START"),
        ):
            if selection.role != role:
                raise ValueError("Funding selection must preserve its calculation role")
            if not isinstance(selection, SelectedFundingObservation):
                continue
            point = selection.observation
            at = point.clock.timestamp.value
            bound = self.request.start if role == "SETTLED_START" else self.request.cutoff
            if (
                point.instrument != self.instrument
                or at is None
                or funding_source_before(point.clock.timestamp, kind=point.kind, boundary=bound, strict=False)
                is not True
                or point.visible_at(self.request.assembled_at) is not True
            ):
                raise ValueError(
                    "Selected funding must be bound to its actual instrument, window and receipt"
                )
        selected = {
            point.observation.evidence
            for point in (self.settled, self.estimate, self.start)
            if isinstance(point, SelectedFundingObservation)
        }
        if selected.intersection(point.source for point in self.excluded):
            raise ValueError("Excluded funding cannot provide an active selected observation")
        return self


def _latest(
    points: Sequence[tuple[datetime, FundingInputObservation]],
    *,
    role: FundingSelectionRole,
    scopes: Mapping[FundingInputEvidenceRef, str],
) -> FundingObservationSelection:
    if not points:
        return UnavailableFundingSelection(
            status="UNAVAILABLE", role=role, reason="NO_ELIGIBLE_OBSERVATION", candidates=()
        )
    bounded: list[tuple[datetime, datetime, FundingInputObservation]] = []
    for _, point in points:
        bounds = funding_clock_bounds(
            point.clock.timestamp, role="SETTLEMENT_EVENT" if point.kind == "SETTLED" else "ESTIMATE_SAMPLE"
        )
        if bounds.lower is None or bounds.upper is None:
            raise ValueError("Eligible funding selection requires orderable source bounds")
        bounded.append((bounds.lower, bounds.upper, point))
    latest_lower = max(lower for lower, _, _ in bounded)
    latest = tuple(point for _, upper, point in bounded if upper >= latest_lower)
    identities = {(point.evidence.source_projection_sha256, scopes[point.evidence]) for point in latest}
    if len(identities) > 1:
        return UnavailableFundingSelection(
            status="UNAVAILABLE",
            role=role,
            reason="AMBIGUOUS_LATEST_SOURCE",
            candidates=tuple(point.evidence for point in latest),
        )
    return SelectedFundingObservation(
        status="OBSERVED", role=role, observation=min(latest, key=lambda point: point.evidence.row_index)
    )


class FundingCalculator:
    """Own raw admission and digest indexes once; windows reuse immutable source facts."""

    __slots__ = ("_source", "_observations", "_index", "_scopes")

    def __init__(
        self,
        *,
        token_id: str,
        instruments: Sequence[Mapping[str, JsonInput]],
        records: Sequence[Mapping[str, JsonInput]],
    ) -> None:
        self._source = decode_funding_inputs(token_id=token_id, instruments=instruments, records=records)
        self._observations = {point.evidence: point for point in self._source.observations}
        self._scopes = {
            point.reference: point.descriptor.scope_projection_sha256
            for point in self._source.source_projections
        }
        indexed: dict[str, list[FundingInputObservation]] = {
            instrument.instrument_id: [] for instrument in self._source.instruments
        }
        for point in self._source.observations:
            indexed[point.instrument.instrument_id].append(point)
        self._index = {instrument: tuple(points) for instrument, points in indexed.items()}

    @property
    def source(self) -> FundingReadSnapshot:
        return self._source

    def observation(self, reference: FundingInputEvidenceRef) -> FundingInputObservation:
        try:
            return self._observations[reference]
        except KeyError:
            raise ValueError("Selected funding reference is not in this admitted read") from None

    def select(self, request: FundingWindowRequest) -> tuple[FundingInstrumentSelection, ...]:
        request = FundingWindowRequest.model_validate(request)
        if request.start.value is None or request.cutoff.value is None:
            raise ValueError("Funding selection requires actual request bounds")
        selected: list[FundingInstrumentSelection] = []
        for instrument in self._source.instruments:
            settled: list[tuple[datetime, FundingInputObservation]] = []
            estimates: list[tuple[datetime, FundingInputObservation]] = []
            excluded: list[ExcludedFundingObservation] = []
            for point in self._index[instrument.instrument_id]:
                reason: FundingExclusionReason | None
                if request.assembled_at.value is None:
                    reason = "ASSEMBLY_CLOCK_NOT_CAPTURED"
                elif point.clock.timestamp.value is None:
                    reason = "SOURCE_CLOCK_NOT_CAPTURED"
                elif (
                    funding_source_before(
                        point.clock.timestamp, kind=point.kind, boundary=request.cutoff, strict=False
                    )
                    is False
                ):
                    reason = "SOURCE_AFTER_CUTOFF"
                elif (
                    funding_source_before(
                        point.clock.timestamp, kind=point.kind, boundary=request.cutoff, strict=False
                    )
                    is not True
                ):
                    reason = "SOURCE_CLOCK_NOT_ORDERABLE"
                elif point.received_at.value is None:
                    reason = "RECEIPT_NOT_CAPTURED"
                elif point.visible_at(request.assembled_at) is False:
                    reason = "RECEIPT_AFTER_ASSEMBLY"
                elif point.visible_at(request.assembled_at) is not True:
                    reason = "RECEIPT_ORDER_UNCERTAIN"
                else:
                    reason = None
                if reason is not None:
                    excluded.append(ExcludedFundingObservation(source=point.evidence, reason=reason))
                    continue
                if point.clock.timestamp.value is None:
                    raise ValueError("Admitted funding requires its actual observation clock")
                at = parse_iso_time(point.clock.timestamp.value)
                (settled if point.kind == "SETTLED" else estimates).append((at, point))
            selected.append(
                FundingInstrumentSelection(
                    instrument=instrument,
                    request=request,
                    settled=_latest(settled, role="LATEST_SETTLED", scopes=self._scopes),
                    estimate=_latest(estimates, role="LATEST_ESTIMATE", scopes=self._scopes),
                    start=_latest(
                        tuple(
                            point
                            for point in settled
                            if funding_source_before(
                                point[1].clock.timestamp, kind="SETTLED", boundary=request.start, strict=False
                            )
                            is True
                        ),
                        role="SETTLED_START",
                        scopes=self._scopes,
                    ),
                    excluded=tuple(excluded),
                )
            )
        return tuple(selected)
