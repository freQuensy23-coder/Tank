"""Owned OI read admission and exact cached fact/evidence identity."""

from collections.abc import Mapping, Sequence
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_derivative_input_codec import decode_oi_inputs, decode_oi_synchronization_rules
from tank.analysis.condition_derivative_inputs import (
    CapturedOISynchronizationRules,
    OIInputEvidenceRef,
    OIObservation,
    OIRuleReadScope,
    OISelectionPolicy,
    UncapturedOISynchronizationRules,
)
from tank.analysis.condition_graph_contracts import (
    BasicConditionDerivatives,
    ConditionWindow,
)
from tank.analysis.oi_family import OIConditionCalculation, calculate_oi_family
from tank.analysis.oi_history_cache import OIHistoryCache
from tank.analysis.oi_synchronization import OISynchronizationPolicy
from tank.analysis.open_interest import OIWindowRequest, OpenInterestCalculator
from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import JsonInput, sha256


def captured_oi_selection_policy(config: Mapping[str, JsonInput]) -> OISelectionPolicy:
    """Read only required captured policy facts, preserving the authored history override."""
    required = ("oi_sync_tolerance_s", "history_min_days", "normal_range_quantiles")
    if any(name not in config for name in required):
        raise ValueError("OI calculation requires captured selection and history policy")
    quantiles = config["normal_range_quantiles"]
    if not isinstance(quantiles, (list, tuple)) or len(quantiles) != 2:
        raise ValueError("OI calculation requires two actual configured quantiles")
    if "historical_lookback_days" in config:
        horizon = config["historical_lookback_days"]
    elif "base_retention_days" in config:
        horizon = config["base_retention_days"]
    else:
        raise ValueError("OI calculation requires its captured historical lookback")
    return OISelectionPolicy.model_validate(
        {
            "tolerance_s": config["oi_sync_tolerance_s"],
            "minimum_history_days": config["history_min_days"],
            "lookback_days": horizon,
            "quantiles": tuple(quantiles),
        }
    )


class OIReadIdentity(DomainModel):
    """Canonical storage-read manifest, never provider-byte provenance."""

    format: Literal["CANONICAL_OI_READ_MANIFEST_V1"]
    token_id: str = Field(min_length=1, pattern=r"\S")
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class OIVisibilityWitness(DomainModel):
    source: OIInputEvidenceRef
    visible: bool | None


class OIConditionFacts(OIConditionCalculation):
    source_identity: OIReadIdentity
    visibility_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def actual_subject(self) -> Self:
        if self.result.token_id != self.source_identity.token_id:
            raise ValueError("Cached OI facts must preserve their actual source subject")
        return self


class OIConditionLookup:
    """Own admitted immutable reads and their calculated facts outside the domain graph."""

    __slots__ = ("_source", "_calculator", "_facts")

    def __init__(
        self,
        *,
        token_id: str,
        instruments: Sequence[Mapping[str, JsonInput]],
        observations: Sequence[Mapping[str, JsonInput]],
        semantics: Sequence[Mapping[str, JsonInput]] | None,
        request: OIWindowRequest,
        policy: OISelectionPolicy,
        synchronization_policy: OISynchronizationPolicy,
        history_cache: OIHistoryCache,
        window: ConditionWindow,
    ) -> None:
        request = OIWindowRequest.model_validate(request)
        self._source = decode_oi_inputs(token_id=token_id, instruments=instruments, observations=observations)
        self._calculator = OpenInterestCalculator(self._source, policy)
        rules: CapturedOISynchronizationRules | UncapturedOISynchronizationRules
        if semantics is None:
            rules = UncapturedOISynchronizationRules(
                status="NOT_CAPTURED", token_id=token_id, reason="OI_SEMANTICS_NOT_CAPTURED"
            )
        else:
            rules = decode_oi_synchronization_rules(
                scope=OIRuleReadScope.from_snapshot(self._source), records=semantics
            )
        identity = OIReadIdentity(
            format="CANONICAL_OI_READ_MANIFEST_V1",
            token_id=token_id,
            manifest_sha256=sha256(
                (
                    self._source.token_id,
                    self._source.instruments,
                    tuple(point.evidence for point in self._source.observations),
                    rules,
                )
            ),
        )
        calculation = calculate_oi_family(
            self._calculator,
            request,
            rules=rules,
            synchronization_policy=synchronization_policy,
            history_cache=history_cache,
            window=window,
        )
        self._facts = OIConditionFacts(
            result=calculation.result,
            closing=calculation.closing,
            source_identity=identity,
            visibility_sha256=self.visibility_fingerprint(request.assembled_at),
        )

    def visibility_fingerprint(self, assembled_at: Timestamp) -> str:
        assembly = Timestamp.model_validate(assembled_at)
        witnesses = tuple(
            OIVisibilityWitness(source=point.evidence, visible=point.visible_at(assembly))
            for point in self._source.observations
        )
        return sha256(witnesses)

    def matches_visibility(self, assembled_at: Timestamp) -> bool:
        return self.visibility_fingerprint(assembled_at) == self._facts.visibility_sha256

    @property
    def facts(self) -> OIConditionFacts:
        return OIConditionFacts.model_validate(self._facts)

    @property
    def result(self) -> BasicConditionDerivatives:
        return BasicConditionDerivatives.model_validate(self._facts.result)

    def closing_observations(self) -> tuple[OIObservation, ...]:
        selected: list[OIObservation] = []
        for closing in self._facts.closing:
            point = self._calculator.observation(closing.reference)
            if point.instrument.instrument_id != closing.instrument_id:
                raise ValueError("Selected OI freshness evidence cannot borrow another instrument")
            selected.append(point)
        return tuple(selected)
