"""Concrete malformed-source-entry evidence and stable identity contracts."""

import re
from collections.abc import Mapping
from typing import Literal

from pydantic import Field, JsonValue

from tank.contracts import DomainModel


class EntryIdentity(DomainModel):
    id: str | None = None
    title: str | None = None
    link: str | None = None
    excerpt: str | None = None


class ErrorEvidence(DomainModel):
    reason: str = Field(min_length=1)
    entry_index: int | None = Field(default=None, ge=0)
    entry: EntryIdentity | None = None
    error: str | None = None
    payload_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")


class DigestIdentity(DomainModel):
    entry_sha256: str


class ProposalIdentity(DomainModel):
    proposal_id: str


class AccountIdentity(DomainModel):
    account: str


class ExceptionIdentity(DomainModel):
    error: str


class DigestEntryError(ErrorEvidence):
    entry_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    def identity(self) -> DigestIdentity:
        return DigestIdentity(entry_sha256=self.entry_sha256)


class ProposalEntryError(ErrorEvidence):
    proposal_id: str = Field(min_length=1)

    def identity(self) -> ProposalIdentity:
        return ProposalIdentity(proposal_id=self.proposal_id)


class AccountEntryError(ErrorEvidence):
    account: str | None

    def identity(self) -> AccountIdentity:
        return AccountIdentity(account=self.account.lower() if self.account is not None else "")


class DocumentEntryError(ErrorEvidence):
    def identity(self) -> ExceptionIdentity:
        return ExceptionIdentity(error=re.sub(r"\d+", "#", self.error if self.error is not None else ""))


type SourceEntryError = DigestEntryError | ProposalEntryError | AccountEntryError | DocumentEntryError
type SourceErrorIdentity = DigestIdentity | ProposalIdentity | AccountIdentity | ExceptionIdentity


def parse_entry_error(value: SourceEntryError | Mapping[str, JsonValue]) -> SourceEntryError:
    """Identify a concrete wire variant without losing missing-account semantics."""
    if isinstance(value, (DigestEntryError, ProposalEntryError, AccountEntryError, DocumentEntryError)):
        return value
    if "entry_sha256" in value:
        return DigestEntryError.model_validate(value)
    if "proposal_id" in value:
        return ProposalEntryError.model_validate(value)
    if "account" in value:
        return AccountEntryError.model_validate(value)
    return DocumentEntryError.model_validate(value)


class EntryErrorMarker(DomainModel):
    notice_id: str
    kind: Literal["ENTRY_ERROR"] = "ENTRY_ERROR"
    subject_id: str
    text: str
    group_notice_id: str
    feed_url: str
    entry_error: SourceEntryError
    created_at: str
    created_at_prec: Literal["COLLECTION_ONLY"] = "COLLECTION_ONLY"
    created_at_prec_reason: None = None
    created_by: Literal["source-poller"] = "source-poller"

    def storage_record(self) -> dict[str, JsonValue]:
        record: dict[str, JsonValue] = self.model_dump(mode="json")
        record["entry_error"] = self.entry_error.model_dump(mode="json", exclude_unset=True)
        return record


class EntryErrorNotice(DomainModel):
    notice_id: str
    kind: Literal["ENTRY_ERRORS"] = "ENTRY_ERRORS"
    subject_id: str
    source_id: str
    source_name: str
    feed_url: str
    seen_at: str
    entry_error_keys: list[str]
    entry_errors: list[SourceEntryError]
    created_at: str
    created_at_prec: Literal["COLLECTION_ONLY"] = "COLLECTION_ONLY"
    created_at_prec_reason: None = None
    created_by: Literal["source-poller"] = "source-poller"
    text: str

    def storage_record(self) -> dict[str, JsonValue]:
        record: dict[str, JsonValue] = self.model_dump(mode="json")
        record["entry_errors"] = [
            error.model_dump(mode="json", exclude_unset=True) for error in self.entry_errors
        ]
        return record
