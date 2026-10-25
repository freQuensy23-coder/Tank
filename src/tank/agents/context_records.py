"""Closed readable reference records at the model-context projection boundary."""

from typing import Literal

from pydantic import Field, JsonValue

from tank.analysis.watch_contracts import WatchExpression
from tank.contracts import DomainModel


class DocumentReference(DomainModel):
    url: str | None
    title: str | None
    observed_at: str | None
    excerpt: str | None


class EventReference(DomainModel):
    statement: str | None
    public_at: str | None


class ClaimReference(DomainModel):
    quote: str | None
    subject: str | None
    predicate: str | None
    object: str | None
    sources: list[DocumentReference] | None
    verified: bool | None
    stage: str | None


class ConditionReference(DomainModel):
    text: str | None
    conditions: WatchExpression | None
    token: str | None
    role: Literal["UPGRADE", "DOWNGRADE", "INVALIDATION", "AGENT"] | None
    created_at: str | None


class ProposalReference(DomainModel):
    subject: str | None


type RecordReference = (
    DocumentReference | EventReference | ClaimReference | ConditionReference | ProposalReference
)


class ClaimEvidenceOwner(DomainModel):
    item_id: str | None = Field(min_length=1)


def reference_wire(reference: RecordReference) -> dict[str, JsonValue]:
    """Serialize only actual known descriptive facts at the model-facing JSON boundary."""
    result = reference.model_dump(mode="json", exclude_none=True)
    if isinstance(reference, ClaimReference) and not reference.sources:
        del result["sources"]
    return result


class EventOwnership(DomainModel):
    event_id: str = Field(min_length=1)


class EventMergeState(DomainModel):
    lifecycle: str = Field(min_length=1)


class ProposalAvailability(DomainModel):
    human_action: str | None = Field(min_length=1)


class ContextMessageProjection(DomainModel):
    message: JsonValue
    item: JsonValue
    current_event: JsonValue
    proposal: JsonValue
    open_proposals: list[JsonValue] | None

    def to_wire(self, *, event_scope: bool, proposal_scope: bool) -> dict[str, JsonValue]:
        result = self.model_dump(mode="json")
        if not event_scope:
            del result["current_event"]
        if not proposal_scope:
            del result["proposal"]
            del result["open_proposals"]
        return result
