"""Consumed milestone verification facts, separate from complete event records."""

from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import parse_time

type MilestoneIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]


class MilestoneKind(DomainModel):
    kind: MilestoneIdentity

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        if "kind" in record and "milestone" in record and record["kind"] != record["milestone"]:
            raise ValueError("Milestone identity declarations conflict")
        projected = {"kind": record["kind"]} if "kind" in record else {}
        if "kind" not in record and "milestone" in record:
            projected["kind"] = record["milestone"]
        return cls.model_validate(projected)


class MilestoneCheckCommand(DomainModel):
    event_id: MilestoneIdentity
    milestone: MilestoneIdentity
    due_at: str | None = None

    @model_validator(mode="after")
    def reported_due_at(self) -> Self:
        if self.due_at is not None:
            parse_time(self.due_at)
        return self

    @classmethod
    def from_notification(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({field: record[field] for field in cls.model_fields if field in record})


class ScheduledMilestoneCheck(MilestoneCheckCommand):
    due_at: str
    due_at_prec: str | None
    due_at_prec_reason: str | None
    source_id: MilestoneIdentity | None
    due_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @model_validator(mode="after")
    def actual_schedule(self) -> Self:
        Timestamp(self.due_at, self.due_at_prec, self.due_at_prec_reason, self.due_at_bucket_anchor)
        return self

    def command(self) -> MilestoneCheckCommand:
        return MilestoneCheckCommand(event_id=self.event_id, milestone=self.milestone, due_at=self.due_at)


class MissedMilestoneIdentity(DomainModel):
    kind: MilestoneIdentity
    due_at: str
    status: Literal["MISSED"]

    @model_validator(mode="after")
    def actual_due_at(self) -> Self:
        parse_time(self.due_at)
        return self

    @classmethod
    def from_notification(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({field: record[field] for field in cls.model_fields if field in record})


class AcceptedMilestoneResolution(DomainModel):
    kind: MilestoneIdentity
    due_at: str
    outcome: str
    quotes: list[str]
    evidence: list[str]

    @model_validator(mode="after")
    def actual_due_at(self) -> Self:
        parse_time(self.due_at)
        return self

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        projected = {field: record[field] for field in cls.model_fields if field in record}
        projected["kind"] = MilestoneKind.from_record(record).kind
        return cls.model_validate(projected)


class MilestoneEventWire(DomainModel):
    event_id: MilestoneIdentity
    milestone_resolutions: list[dict[str, JsonValue]] = Field(default_factory=list)


class MilestoneResolutionHistory(DomainModel):
    event_id: MilestoneIdentity
    resolutions: list[AcceptedMilestoneResolution]

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue], command: MilestoneCheckCommand) -> Self:
        wire = MilestoneEventWire.model_validate(
            {field: record[field] for field in MilestoneEventWire.model_fields if field in record}
        )
        if wire.event_id != command.event_id:
            raise ValueError("Milestone event conflicts with its requested identity")
        return cls(
            event_id=wire.event_id,
            resolutions=[AcceptedMilestoneResolution.from_record(row) for row in wire.milestone_resolutions],
        )

    def latest(self, command: MilestoneCheckCommand) -> AcceptedMilestoneResolution | None:
        matches = [
            resolution
            for resolution in self.resolutions
            if resolution.kind == command.milestone
            and (command.due_at is None or resolution.due_at == command.due_at)
        ]
        return matches[-1] if matches else None
