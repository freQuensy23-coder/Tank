"""In-memory native polling state; neither class supplies execution or source facts."""

from pydantic import Field

from tank.contracts import DomainModel


class ExecutionPoll(DomainModel):
    deadline: float
    delay: float = Field(gt=0)
    signature: tuple[str, ...]


class HistoryPoll(DomainModel):
    deadline: float
    delay: float = Field(gt=0)
