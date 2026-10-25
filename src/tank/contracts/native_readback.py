"""Native session identity and observed lifecycle without adapter dependencies."""

from typing import Literal

from pydantic import Field, field_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import TrueFlag
from tank.util import parse_time

type NativeLifecycle = Literal["rescheduling", "running", "idle", "terminated"]


class NativeSessionReference(DomainModel):
    session_id: str = Field(min_length=1)


class NativeSessionState(NativeSessionReference):
    kind: Literal["FOUND"]
    status: NativeLifecycle
    archived_at: str | None

    @field_validator("archived_at")
    @classmethod
    def actual_archive_clock(cls, value: str | None) -> str | None:
        if value is not None:
            parse_time(value)
        return value


class NativeSessionMissing(NativeSessionReference):
    kind: Literal["MISSING"]
    missing: TrueFlag


type NativeSessionLookup = NativeSessionState | NativeSessionMissing
