"""Persisted queue envelope; payload is a dispatcher-owned JSON wire contract."""

from typing import Literal

from pydantic import Field, JsonValue

from tank.contracts import DomainModel


class JobEnvelope(DomainModel):
    job_id: str
    type: str
    priority: int = Field(ge=0, le=4)
    idem_key: str
    payload: dict[str, JsonValue]
    status: Literal["QUEUED", "RUNNING", "DONE", "FAILED", "EXPIRED"]
    attempts: int = Field(ge=0)
    max_attempts: int = Field(ge=1)
    last_error: str | None
    created_at: str
    created_at_prec: str | None
    created_at_prec_reason: str | None
    run_after: str
    run_after_prec: str | None
    run_after_prec_reason: str | None
    expires_at: str | None
    expires_at_prec: str | None
    expires_at_prec_reason: str | None
    finished_at: str | None
    finished_at_prec: str | None
    finished_at_prec_reason: str | None
    created_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    run_after_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    expires_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    finished_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
