"""Explicit unavailable scores for tests that do not exercise score assessment."""

from pydantic import JsonValue, RootModel
from tank.contracts.score import ScoreDimension, ScoreDimensions


class ScoreDocument(RootModel[dict[str, JsonValue]]):
    pass


def unassessed_dimension() -> ScoreDimension:
    return ScoreDimension(
        value=None,
        reason="NOT_ASSESSED",
        rationale="This fixture does not perform a score assessment.",
        evidence=[],
        proposed_by="LLM",
        caps_applied=[],
    )


def unassessed_score_model() -> ScoreDimensions:
    return ScoreDimensions(
        economic_directness=unassessed_dimension(),
        magnitude=unassessed_dimension(),
        permanence=unassessed_dimension(),
        distribution_expansion=unassessed_dimension(),
        mechanical_flows=unassessed_dimension(),
        surprise=unassessed_dimension(),
        implementation=unassessed_dimension(),
    )


def unassessed_score_dimensions() -> dict[str, JsonValue]:
    """Serialize complete unknown dimensions at the raw test-document boundary."""
    return ScoreDocument.model_validate_json(unassessed_score_model().model_dump_json()).root
