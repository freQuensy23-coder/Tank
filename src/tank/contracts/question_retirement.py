"""Pure question-retirement and durable host-refusal facts."""

from typing import Annotated, Literal, Protocol, Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.time_precision import Timestamp
from tank.util import sha256

type RetirementIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]
type UnprovedRetirementReason = Literal["INITIAL_ACCEPTANCE_NOT_RETAINED", "INPUT_JOURNAL_NOT_RETAINED"]


class RetiredQuestionMessages(DomainModel):
    known_ids: list[RetirementIdentity]


class QueuedQuestionSession(DomainModel):
    session_id: RetirementIdentity | None
    status: Literal["QUEUED"]


class ActiveQuestionIntent(DomainModel):
    message_id: RetirementIdentity | None


class UnknownQuestionAcceptance(DomainModel):
    target: FollowupRefusalTarget
    message_id: RetirementIdentity
    turn_status: Literal["PENDING", "RUNNING"] | None
    reason: Literal["INITIAL_ACCEPTANCE_NOT_RETAINED"]


class UnretainedQuestionJournal(DomainModel):
    target: FollowupRefusalTarget
    reason: Literal["INPUT_JOURNAL_NOT_RETAINED"]


type QuestionRetirementWitness = Annotated[
    UnknownQuestionAcceptance | UnretainedQuestionJournal, Field(discriminator="reason")
]


class UnprovedQuestionRetirement(ValueError):
    def __init__(self, witness: QuestionRetirementWitness) -> None:
        super().__init__("Question retirement refused: " + witness.reason)
        self.witness = witness
        self.reason = witness.reason


class QuestionFailureRefusalRequest(DomainModel):
    job_id: RetirementIdentity
    job_status: Literal["FAILED", "EXPIRED"]
    witness: QuestionRetirementWitness

    def key(self) -> str:
        return sha256(self)


class QuestionFailureRefusalReceipt(DomainModel):
    request: QuestionFailureRefusalRequest
    controller: ResolvedVersionIdentity
    recorded_at: Timestamp

    @model_validator(mode="after")
    def actual_controller_receipt_clock(self) -> Self:
        if self.recorded_at.value is None:
            raise ValueError("A host question refusal requires its actual recording clock")
        return self


class QuestionFailureRefusalSink(Protocol):
    async def record(self, request: QuestionFailureRefusalRequest) -> QuestionFailureRefusalReceipt: ...


type QuestionRefusalDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class QuestionFailureRefusalLedger(DomainModel):
    job_id: RetirementIdentity
    entries: dict[QuestionRefusalDigest, QuestionFailureRefusalReceipt]

    @model_validator(mode="after")
    def exact_request_keys(self) -> Self:
        for key, receipt in self.entries.items():
            if receipt.request.job_id != self.job_id or receipt.request.key() != key:
                raise ValueError("Host question refusal differs from its actual job or request key")
        return self
