"""Reaction commands and actual anchor facts, independent of queue orchestration."""

from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import TrueFlag
from tank.time_precision import Timestamp, propagate_timestamp

type ReactionIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]
type ReactionRevision = Annotated[int, Field(ge=1)]
type ReactionBucketAnchor = Literal["START", "END", "UNSPECIFIED"]
type ReactionCheckpoint = Literal[
    "PRE_24H", "PRE_4H", "PRE_1H", "POST_15M", "POST_1H", "POST_4H", "POST_24H", "POST_72H"
]


class ReactionAnchorFacts(DomainModel):
    type: Literal["T0", "T1", "KNOWN_TO_US_AT"]
    ts: str
    ts_prec: str | None = None
    ts_prec_reason: str | None = None
    precision: str | None = None
    ts_bucket_anchor: ReactionBucketAnchor | None = None

    @model_validator(mode="after")
    def actual_occurrence(self) -> Self:
        if self.timestamp.value is None:
            raise ValueError("Reaction anchor requires its actual occurrence")
        return self

    @property
    def timestamp(self) -> Timestamp:
        return propagate_timestamp(self.source_fields(), "ts", precision_aliases=("precision",))

    def source_fields(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", exclude_unset=True)

    def with_propagated_clock(self) -> Self:
        return type(self).model_validate(self.source_fields() | self.timestamp.fields("ts"))


class ReactionSubject(DomainModel):
    event_id: ReactionIdentity
    token_id: ReactionIdentity


class ReactionTarget(ReactionSubject):
    checkpoint: ReactionCheckpoint


class ReactionLookupCommand(ReactionTarget):
    revision: ReactionRevision = 1


class ReactionSchedulingFacts(ReactionTarget):
    cutoff: str | None = None
    cutoff_prec: str | None = None
    cutoff_prec_reason: str | None = None
    cutoff_bucket_anchor: ReactionBucketAnchor | None = None
    retro: bool | None = None
    revision: ReactionRevision = 1
    reason: ReactionIdentity | None = None

    @model_validator(mode="after")
    def actual_optional_scheduling_facts(self) -> Self:
        if self.cutoff is None:
            if any(
                value is not None
                for value in (self.cutoff_prec, self.cutoff_prec_reason, self.cutoff_bucket_anchor)
            ):
                raise ValueError("Reaction scheduling precision requires its actual cutoff")
        else:
            propagate_timestamp(self.storage_fields(), "cutoff")
        if self.revision > 1 and (self.reason is None or not self.reason.strip()):
            raise ValueError("Reaction revision requires its actual reason")
        return self

    def storage_fields(self) -> dict[str, JsonValue]:
        return self.model_dump(
            mode="json", exclude_unset=True, include=set(ReactionSchedulingFacts.model_fields) | {"anchor"}
        )


class InitialReactionCommand(ReactionSchedulingFacts):
    anchor: ReactionAnchorFacts

    def with_propagated_anchor(self) -> Self:
        return type(self).model_validate(
            self.model_copy(update={"anchor": self.anchor.with_propagated_clock()})
        )


class ManualReactionRevisionCommand(ReactionSchedulingFacts):
    revision: Annotated[int, Field(ge=2)]
    reason: ReactionIdentity
    anchor: ReactionAnchorFacts | None = None


class RevisedReactionCommand(ReactionTarget):
    automatic_revision: TrueFlag
    change_id: ReactionIdentity
    reason: ReactionIdentity

    def storage_fields(self) -> dict[str, JsonValue]:
        return self.model_dump(
            mode="json", exclude_unset=True, include=set(RevisedReactionCommand.model_fields)
        )


type ReactionCaptureCommand = (
    InitialReactionCommand | ManualReactionRevisionCommand | RevisedReactionCommand | ReactionLookupCommand
)


def reaction_command_from_wire(
    record: Mapping[str, JsonValue],
) -> InitialReactionCommand | ManualReactionRevisionCommand | RevisedReactionCommand:
    if "automatic_revision" in record:
        return RevisedReactionCommand.model_validate(record)
    revision = record.get("revision")
    if isinstance(revision, int) and not isinstance(revision, bool) and revision > 1:
        return ManualReactionRevisionCommand.model_validate(record)
    return InitialReactionCommand.model_validate(record)
