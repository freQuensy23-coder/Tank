"""Consumed milestone verification facts, separate from complete event records."""

from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp, propagate_timestamp, timestamp_owned_fields
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


class MilestoneExpiryCommand(MilestoneCheckCommand):
    due_at: str = Field(...)


class MilestoneEventOwner(DomainModel):
    event_id: MilestoneIdentity

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue], expected_event_id: str) -> Self:
        owner = cls.model_validate({"event_id": record["event_id"]} if "event_id" in record else {})
        if owner.event_id != expected_event_id:
            raise ValueError("Milestone event conflicts with its requested identity")
        return owner


class MilestoneTimerState(DomainModel):
    kind: MilestoneIdentity
    status: Literal["PENDING", "CONFIRMED", "VERIFIED", "FAILED", "MISSED"] | None
    due: Timestamp
    expires: Timestamp | None

    @model_validator(mode="after")
    def actual_clock(self) -> Self:
        if self.due.value is None:
            raise ValueError("Milestone timer requires its actual deadline")
        if self.expires is not None and self.expires.value is None:
            raise ValueError("Milestone expiry requires its actual occurrence")
        return self

    @property
    def due_at(self) -> str:
        if self.due.value is None:
            raise ValueError("Milestone timer requires its actual deadline")
        return self.due.value

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        due = propagate_timestamp(record, "due_at")
        expiry_fields = {
            field: record[field] for field in timestamp_owned_fields("expires_at") if field in record
        }
        expiry = propagate_timestamp(record, "expires_at") if expiry_fields else None
        if expiry is not None and expiry.value is None:
            expiry = None
        return cls.model_validate(
            {
                "kind": MilestoneKind.from_record(record).kind,
                "status": record.get("status"),
                "due": due,
                "expires": expiry,
            }
        )


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


class MilestoneEventWire(MilestoneEventOwner):
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
