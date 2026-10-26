"""Known local question-retirement facts without inferred native acceptance."""

from typing import Annotated, Literal

from pydantic import Field

from tank.contracts import DomainModel

type RetirementIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]
type UnprovedRetirementReason = Literal["INITIAL_ACCEPTANCE_NOT_RETAINED"]


class UnprovedQuestionRetirement(ValueError):
    def __init__(self, reason: UnprovedRetirementReason) -> None:
        super().__init__("Question retirement refused: " + reason)
        self.reason = reason


class RetiredQuestionMessages(DomainModel):
    known_ids: list[RetirementIdentity]


class QueuedQuestionSession(DomainModel):
    session_id: RetirementIdentity | None
    status: Literal["QUEUED"]


class ActiveQuestionIntent(DomainModel):
    message_id: RetirementIdentity | None
