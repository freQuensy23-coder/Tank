"""Native polling state and required scout/session identity projections."""

from pydantic import Field

from tank.contracts import DomainModel


class ExecutionPoll(DomainModel):
    deadline: float
    delay: float = Field(gt=0)
    signature: tuple[str, ...]


class HistoryPoll(DomainModel):
    deadline: float
    delay: float = Field(gt=0)


class ScoutProposalIdentity(DomainModel):
    prp_id: str = Field(min_length=1)


class ScoutSessionReference(DomainModel):
    session_id: str = Field(min_length=1)
    proposal_id: str = Field(min_length=1)
