"""Persisted assessment identity passed between application jobs and evidence consumers."""

from collections.abc import Mapping
from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp


class MarketTruthAssessmentRef(DomainModel):
    mta_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    cutoff: str
    cutoff_prec: str | None
    cutoff_prec_reason: str | None
    cutoff_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @model_validator(mode="after")
    def declared_cutoff(self) -> Self:
        Timestamp(self.cutoff, self.cutoff_prec, self.cutoff_prec_reason, self.cutoff_bucket_anchor)
        return self

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        """Select a stable identity from a persisted full assessment or a job reference."""
        return cls.model_validate({field: record[field] for field in cls.model_fields if field in record})
