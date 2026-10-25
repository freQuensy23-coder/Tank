"""Native session identity and observed lifecycle without adapter dependencies."""

from typing import Literal

from pydantic import Field

from tank.contracts import DomainModel
from tank.contracts.scalars import TrueFlag

type NativeLifecycle = Literal["rescheduling", "running", "idle", "terminated"]


class NativeSessionReference(DomainModel):
    session_id: str = Field(min_length=1)


class NativeSessionState(NativeSessionReference):
    kind: Literal["FOUND"]
    status: NativeLifecycle
    archived_at: str | None


class NativeSessionMissing(NativeSessionReference):
    kind: Literal["MISSING"]
    missing: TrueFlag


type NativeSessionLookup = NativeSessionState | NativeSessionMissing
