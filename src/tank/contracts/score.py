"""Shared event score domain contracts."""

from typing import Literal, Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel


class ScoreDimensionView(DomainModel):
    value: int | float | None = Field(ge=0, le=10, allow_inf_nan=False)
    rationale: str | None = None
    reason: str | None = None

    def explanation(self, missing: str) -> str:
        if self.rationale:
            return self.rationale
        return self.reason.replace("_", " ").lower() if self.reason else missing


class ScoreDimension(ScoreDimensionView):
    rationale: str = Field(min_length=1)
    evidence: list[str]
    proposed_by: Literal["LLM"]
    caps_applied: list[str]

    @model_validator(mode="after")
    def require_missing_reason(self) -> Self:
        if self.value is None and not self.reason:
            raise ValueError("Missing score requires reason")
        return self

    def presentation(self) -> ScoreDimensionView:
        return ScoreDimensionView(value=self.value, rationale=self.rationale, reason=self.reason)


class ScoreDimensions(DomainModel):
    economic_directness: ScoreDimension
    magnitude: ScoreDimension
    permanence: ScoreDimension
    distribution_expansion: ScoreDimension
    mechanical_flows: ScoreDimension
    surprise: ScoreDimension
    implementation: ScoreDimension

    def ordered(self) -> tuple[ScoreDimension, ...]:
        return (
            self.economic_directness,
            self.magnitude,
            self.permanence,
            self.distribution_expansion,
            self.mechanical_flows,
            self.surprise,
            self.implementation,
        )


class ScoreAssessment(DomainModel):
    dims: ScoreDimensions


class ScoreDimensionsView(DomainModel):
    economic_directness: ScoreDimensionView
    magnitude: ScoreDimensionView
    permanence: ScoreDimensionView
    distribution_expansion: ScoreDimensionView
    mechanical_flows: ScoreDimensionView
    surprise: ScoreDimensionView
    implementation: ScoreDimensionView

    def ordered(self) -> tuple[ScoreDimensionView, ...]:
        return (
            self.economic_directness,
            self.magnitude,
            self.permanence,
            self.distribution_expansion,
            self.mechanical_flows,
            self.surprise,
            self.implementation,
        )


class ScoreAssessmentView(DomainModel):
    dims: ScoreDimensionsView

    @classmethod
    def from_domain(cls, assessment: ScoreAssessment) -> Self:
        dims = assessment.dims
        return cls(
            dims=ScoreDimensionsView(
                economic_directness=dims.economic_directness.presentation(),
                magnitude=dims.magnitude.presentation(),
                permanence=dims.permanence.presentation(),
                distribution_expansion=dims.distribution_expansion.presentation(),
                mechanical_flows=dims.mechanical_flows.presentation(),
                surprise=dims.surprise.presentation(),
                implementation=dims.implementation.presentation(),
            )
        )
