"""Known exposure outranks uncaptured exposure at the beneficiary storage boundary."""

from collections.abc import Mapping

from pydantic import ConfigDict, Field, JsonValue, RootModel


class PublicationExposureScore(RootModel[int | None]):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        validate_default=True,
        revalidate_instances="always",
        allow_inf_nan=False,
    )
    root: int | None = Field(ge=0, le=10)


def _priority(score: int | None) -> tuple[bool, int | None]:
    return score is None, None if score is None else -score


def exposure_priority_from_record(record: Mapping[str, JsonValue]) -> tuple[bool, int | None]:
    if "exposure_score" not in record:
        raise ValueError("Publication ordering requires the actual exposure score or explicit unknown")
    return _priority(PublicationExposureScore.model_validate(record["exposure_score"], strict=True).root)
