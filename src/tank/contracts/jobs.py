"""Persisted queue envelope; payload is a dispatcher-owned JSON wire contract."""

from collections.abc import Mapping
from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.question_retirement import (
    QuestionFailureRefusalLedger,
    QuestionFailureRefusalReceipt,
    QuestionRefusalDigest,
)
from tank.time_precision import Timestamp
from tank.util import parse_time


class JobMetadata(DomainModel):
    job_id: str
    type: str
    priority: int = Field(ge=0, le=4)
    idem_key: str
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
    recovery_reclaim: bool | None = None
    recovery_count: int | None = Field(default=None, ge=0)
    recovery_at: str | None = None
    recovery_at_prec: str | None = None
    recovery_at_prec_reason: str | None = None
    slot_waiting_since: str | None = None
    slot_admitted: bool | None = None
    interrupted_at: str | None = None
    interrupted_at_prec: str | None = None
    interrupted_at_prec_reason: str | None = None
    interrupted_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    interruption_reason: Literal["CONTROLLED_GENERATION_STOP"] | None = None
    ops_progress_recorded_at: str | None = None
    ops_progress_recorded_at_prec: str | None = None
    ops_progress_recorded_at_prec_reason: str | None = None
    ops_progress_recorded_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    delivery_failure_recorded_at: str | None = None
    delivery_failure_recorded_at_prec: str | None = None
    delivery_failure_recorded_at_prec_reason: str | None = None
    delivery_failure_recorded_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    followup_failure_recorded_at: str | None = None
    followup_failure_recorded_at_prec: str | None = None
    followup_failure_recorded_at_prec_reason: str | None = None
    followup_failure_recorded_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    recovery_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @model_validator(mode="after")
    def validate_lifecycle_clocks(self) -> Self:
        for value, precision, reason, anchor in (
            (
                self.created_at,
                self.created_at_prec,
                self.created_at_prec_reason,
                self.created_at_bucket_anchor,
            ),
            (self.run_after, self.run_after_prec, self.run_after_prec_reason, self.run_after_bucket_anchor),
            (
                self.expires_at,
                self.expires_at_prec,
                self.expires_at_prec_reason,
                self.expires_at_bucket_anchor,
            ),
            (
                self.finished_at,
                self.finished_at_prec,
                self.finished_at_prec_reason,
                self.finished_at_bucket_anchor,
            ),
        ):
            Timestamp(value, precision, reason, anchor)
        for value, precision, reason, anchor in (
            (
                self.recovery_at,
                self.recovery_at_prec,
                self.recovery_at_prec_reason,
                self.recovery_at_bucket_anchor,
            ),
            (
                self.interrupted_at,
                self.interrupted_at_prec,
                self.interrupted_at_prec_reason,
                self.interrupted_at_bucket_anchor,
            ),
            (
                self.ops_progress_recorded_at,
                self.ops_progress_recorded_at_prec,
                self.ops_progress_recorded_at_prec_reason,
                self.ops_progress_recorded_at_bucket_anchor,
            ),
            (
                self.delivery_failure_recorded_at,
                self.delivery_failure_recorded_at_prec,
                self.delivery_failure_recorded_at_prec_reason,
                self.delivery_failure_recorded_at_bucket_anchor,
            ),
            (
                self.followup_failure_recorded_at,
                self.followup_failure_recorded_at_prec,
                self.followup_failure_recorded_at_prec_reason,
                self.followup_failure_recorded_at_bucket_anchor,
            ),
        ):
            if any(item is not None for item in (value, precision, reason, anchor)):
                Timestamp(value, precision, reason, anchor)
        if self.slot_waiting_since is not None:
            parse_time(self.slot_waiting_since)
        terminal = self.status in {"DONE", "FAILED", "EXPIRED"}
        if terminal != (self.finished_at is not None):
            raise ValueError("Job completion clock must agree with terminal status")
        return self


type HostQuestionRefusals = dict[QuestionRefusalDigest, QuestionFailureRefusalReceipt]


def _unreported_question_refusals(value: HostQuestionRefusals | None) -> bool:
    return value is None


class JobEnvelope(JobMetadata):
    """Persisted queue wire envelope; dispatchers decode concrete command payloads."""

    payload: dict[str, JsonValue]
    result: JsonValue = None
    host_question_failure_refusals: HostQuestionRefusals | None = Field(
        default=None, exclude_if=_unreported_question_refusals
    )

    @model_validator(mode="after")
    def actual_host_refusal_ledger(self) -> Self:
        if "host_question_failure_refusals" in self.model_fields_set:
            if self.host_question_failure_refusals is None:
                raise ValueError("A reported host question refusal ledger cannot be null")
            QuestionFailureRefusalLedger(job_id=self.job_id, entries=self.host_question_failure_refusals)
        return self


class JobRetryState(DomainModel):
    """Retry only needs execution fields, including for direct SQL jobs lacking provenance."""

    status: Literal["QUEUED", "RUNNING", "DONE", "FAILED", "EXPIRED"]
    attempts: int = Field(ge=0)
    max_attempts: int = Field(ge=1)
    expires_at: str | None

    @classmethod
    def from_document(cls, document: Mapping[str, JsonValue]) -> "JobRetryState":
        return cls.model_validate(
            {
                "status": document["status"],
                "attempts": document["attempts"],
                "max_attempts": document["max_attempts"],
                "expires_at": document.get("expires_at"),
            }
        )


class JobIdentity(DomainModel):
    """Identity projection independent of legacy payload/provenance completeness."""

    job_id: str
    type: str
    idem_key: str

    @classmethod
    def from_document(cls, document: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate(
            {"job_id": document["job_id"], "type": document["type"], "idem_key": document["idem_key"]}
        )


class JobClaimState(JobRetryState):
    """Only fields that claiming/recovery actually reads from a persisted job."""

    job_id: str
    type: str
    priority: int = Field(ge=0, le=4)
    run_after: str
    recovery_reclaim: bool | None
    recovery_count: int | None = Field(ge=0)

    @classmethod
    def from_document(cls, document: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "job_id": document["job_id"],
                "type": document["type"],
                "status": document["status"],
                "priority": document["priority"],
                "run_after": document["run_after"],
                "expires_at": document.get("expires_at"),
                "attempts": document["attempts"],
                "max_attempts": document["max_attempts"],
                "recovery_reclaim": document.get("recovery_reclaim"),
                "recovery_count": document.get("recovery_count"),
            }
        )


class JobClaimRevision(DomainModel):
    """Only the identity/state needed for conditional rejection of a claimed wire row."""

    job_id: str
    type: str
    status: Literal["QUEUED", "RUNNING", "DONE", "FAILED", "EXPIRED"]
    attempts: int = Field(ge=0)

    @classmethod
    def from_document(cls, document: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "job_id": document["job_id"],
                "type": document["type"],
                "status": document["status"],
                "attempts": document["attempts"],
            }
        )


class InvalidJobClaim(DomainModel):
    job_id: str
    kind: str
    attempts: int = Field(ge=0)
    revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    error_codes: tuple[str, ...]
    error_fields: tuple[str, ...]
