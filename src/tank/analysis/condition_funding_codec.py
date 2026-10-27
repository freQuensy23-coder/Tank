"""Owned funding read admission and exact cached source/visibility binding."""

from collections.abc import Mapping, Sequence
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_funding_inputs import FundingInputEvidenceRef, FundingInputObservation
from tank.analysis.condition_graph_contracts import ConditionWindow
from tank.analysis.funding_family import FundingConditionCalculation, condition_funding_family
from tank.analysis.funding_history import FundingHistoryPolicy
from tank.analysis.funding_history_cache import FundingHistoryCache
from tank.analysis.funding_rate import FundingNormalizationRequest
from tank.analysis.funding_selection import FundingCalculator, FundingWindowRequest
from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import JsonInput, sha256


def captured_funding_history_policy(
    config: Mapping[str, JsonInput], *, normalization: FundingNormalizationRequest
) -> FundingHistoryPolicy:
    if "history_min_days" not in config or "normal_range_quantiles" not in config:
        raise ValueError("Funding calculation requires captured history policy")
    quantiles = config["normal_range_quantiles"]
    if not isinstance(quantiles, (list, tuple)) or len(quantiles) != 2:
        raise ValueError("Funding history requires exactly two captured quantile probabilities")
    if "historical_lookback_days" in config:
        lookback = config["historical_lookback_days"]
    elif "base_retention_days" in config:
        lookback = config["base_retention_days"]
    else:
        raise ValueError("Funding calculation requires captured historical retention")
    return FundingHistoryPolicy.model_validate(
        {
            "minimum_history_days": config["history_min_days"],
            "lookback_days": lookback,
            "quantiles": tuple(quantiles),
            "normalization": normalization,
        }
    )


class FundingReadIdentity(DomainModel):
    format: Literal["CANONICAL_FUNDING_READ_MANIFEST_V1"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FundingVisibilityWitness(DomainModel):
    source: FundingInputEvidenceRef
    visible: bool | None


class FundingConditionFacts(FundingConditionCalculation):
    source_identity: FundingReadIdentity
    visibility_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def actual_subject(self) -> Self:
        if self.result.token_id != self.source_identity.token_id:
            raise ValueError("Funding cache facts must preserve their actual source subject")
        return self


class FundingConditionLookup:
    """Own strict immutable input/facts and operational indexes without cached assembly."""

    __slots__ = ("_calculator", "_facts")

    def __init__(
        self,
        *,
        token_id: str,
        instruments: Sequence[Mapping[str, JsonInput]],
        observations: Sequence[Mapping[str, JsonInput]],
        request: FundingWindowRequest,
        policy: FundingHistoryPolicy,
        history_cache: FundingHistoryCache,
        window: ConditionWindow,
    ) -> None:
        request = FundingWindowRequest.model_validate(request)
        self._calculator = FundingCalculator(token_id=token_id, instruments=instruments, records=observations)
        source = self._calculator.source
        identity = FundingReadIdentity(
            format="CANONICAL_FUNDING_READ_MANIFEST_V1",
            token_id=source.token_id,
            manifest_sha256=sha256(
                (source.token_id, source.instruments, tuple(point.evidence for point in source.observations))
            ),
        )
        calculation = condition_funding_family(
            self._calculator, request, history_policy=policy, history_cache=history_cache, window=window
        )
        self._facts = FundingConditionFacts(
            result=calculation.result,
            selected=calculation.selected,
            source_identity=identity,
            visibility_sha256=self.visibility_fingerprint(request.assembled_at),
        )

    def visibility_fingerprint(self, assembled_at: Timestamp) -> str:
        assembled = Timestamp.model_validate(assembled_at)
        return sha256(
            tuple(
                FundingVisibilityWitness(source=point.evidence, visible=point.visible_at(assembled))
                for point in self._calculator.source.observations
            )
        )

    def matches_visibility(self, assembled_at: Timestamp) -> bool:
        return self.visibility_fingerprint(assembled_at) == self._facts.visibility_sha256

    @property
    def facts(self) -> FundingConditionFacts:
        return self._facts

    def selected_observations(self) -> tuple[FundingInputObservation, ...]:
        result: list[FundingInputObservation] = []
        for venue in self._facts.result.per_venue:
            reference = self._facts.selected_reference(venue.instrument_id)
            if reference is None:
                continue
            source = self._calculator.observation(reference)
            if source.instrument.instrument_id != venue.instrument_id:
                raise ValueError("Funding freshness cannot borrow another selected instrument witness")
            result.append(source)
        return tuple(result)
