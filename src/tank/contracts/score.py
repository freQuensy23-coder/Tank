"""Shared event score domain contracts."""

from pydantic import Field

from tank.contracts import DomainModel


class ScoreDimension(DomainModel):
    value: int | float | None = Field(ge=0, le=10, allow_inf_nan=False)
    rationale: str | None = None
    reason: str | None = None

    def explanation(self, missing: str) -> str:
        if self.rationale:
            return self.rationale
        return self.reason.replace("_", " ").lower() if self.reason else missing


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
