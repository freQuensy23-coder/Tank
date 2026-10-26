"""Durable build submission phases preserve uncertain provider acceptance."""

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import ConfigDict, Field, RootModel, model_validator

from tank.contracts import DomainModel
from tank.deployment.api_contracts import BuildObservation
from tank.deployment.identities import SHA256, CommitSHA
from tank.time_precision import Timestamp
from tank.util import parse_utc_time


class SubmissionFacts(DomainModel):
    release_sha: CommitSHA
    source_digest: SHA256
    requested_at: Timestamp
    budget_s: Annotated[float, Field(gt=0, allow_inf_nan=False)]

    @model_validator(mode="after")
    def collection_origin(self) -> Self:
        if self.requested_at.value is None or self.requested_at.precision != "COLLECTION_ONLY":
            raise ValueError("Build submission requires its actual collection clock")
        parse_utc_time(self.requested_at.value)
        return self

    def remaining(self, observed: Timestamp) -> float:
        if observed.value is None or observed.precision != "COLLECTION_ONLY":
            raise ValueError("Build observation requires its actual collection clock")
        requested_value = self.requested_at.value
        if requested_value is None:
            raise ValueError("Build submission clock is unavailable")
        requested: datetime = parse_utc_time(requested_value)
        elapsed = (parse_utc_time(observed.value) - requested).total_seconds()
        if elapsed < 0:
            raise ValueError("Build observation clock precedes its submission")
        return self.budget_s - elapsed


class UnknownBuildSubmission(SubmissionFacts):
    phase: Literal["ACCEPTANCE_UNKNOWN"]


class ReceivedBuildSubmission(SubmissionFacts):
    phase: Literal["BUILD_RECEIVED"]
    observation: BuildObservation


type BuildSubmission = Annotated[
    UnknownBuildSubmission | ReceivedBuildSubmission, Field(discriminator="phase")
]


class SubmissionDocument(RootModel[BuildSubmission]):
    model_config = ConfigDict(strict=True, hide_input_in_errors=True)
