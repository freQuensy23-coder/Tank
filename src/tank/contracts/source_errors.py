"""Concrete malformed-source-entry evidence and stable identity contracts."""

import base64
import binascii
import re
from collections.abc import Mapping
from typing import Literal, Self

from pydantic import Field, JsonValue, field_validator, model_validator

from tank.contracts import DomainModel
from tank.util import parse_iso_time, sha256


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


class XDiagnosticRequest(DomainModel):
    method: Literal["GET"]
    url: str = Field(min_length=1)
    body: None


class XDiagnosticResponse(DomainModel):
    status: int = Field(ge=100, le=599)
    headers: dict[str, str]
    body: str
    bodyEncoding: Literal["utf-8", "base64"]
    bodyBytes: int = Field(ge=0)
    bodySha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    bodyTruncated: bool
    omittedWireBytes: int | None = Field(default=None, ge=0)
    wireBytes: int | None = Field(default=None, ge=0)
    captureLimitBytes: int | None = Field(default=None, ge=0)
    serializedBodyLimitBytes: int | None = Field(default=None, ge=0)
    readError: str | None = None
    decodeError: str | None = None
    redacted: bool

    @model_validator(mode="after")
    def actual_body_evidence(self) -> Self:
        try:
            raw = (
                base64.b64decode(self.body, validate=True)
                if self.bodyEncoding == "base64"
                else self.body.encode()
            )
        except (ValueError, binascii.Error) as exc:
            raise ValueError("X diagnostic body encoding is invalid") from exc
        if self.bodyBytes != len(raw) or self.bodySha256 != sha256(raw):
            raise ValueError("X diagnostic body metrics conflict with its actual recorded bytes")
        return self


class XDiagnosticAttempt(DomainModel):
    attempt: int = Field(ge=1)
    at: str
    status: int | None = Field(default=None, ge=100, le=599)
    retryable: bool
    error: str
    response: XDiagnosticResponse | None = None

    @field_validator("at")
    @classmethod
    def declared_clock(cls, value: str) -> str:
        parse_iso_time(value)
        return value


class XDiagnostics(DomainModel):
    request: XDiagnosticRequest
    outcome: str = Field(min_length=1)
    attemptCount: int = Field(ge=0, le=3)
    attempts: list[XDiagnosticAttempt] = Field(max_length=3)

    @model_validator(mode="after")
    def actual_attempt_count(self) -> Self:
        if self.attemptCount != len(self.attempts):
            raise ValueError("X diagnostic count conflicts with its recorded attempts")
        return self


class DiagnosticResponseIdentity(DomainModel):
    status: int
    bodyEncoding: Literal["utf-8", "base64"]
    bodyBytes: int = Field(ge=0)
    bodySha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    bodyTruncated: bool
    omittedWireBytes: int | None
    wireBytes: int | None
    captureLimitBytes: int | None
    serializedBodyLimitBytes: int | None
    readError: str | None
    decodeError: str | None
    redacted: bool


class NoDiagnosticResponseIdentity(DomainModel):
    pass


class DiagnosticAttemptIdentity(DomainModel):
    attempt: int
    error: str
    status: int | None
    response: DiagnosticResponseIdentity | NoDiagnosticResponseIdentity
    headers: dict[str, str]


class DiagnosticEvidenceIdentity(DomainModel):
    request: XDiagnosticRequest
    outcome: str
    attempts: list[DiagnosticAttemptIdentity]


class AccountDiagnosticIdentity(DomainModel):
    account: str
    evidence: DiagnosticEvidenceIdentity


class DiagnosticDocumentReference(DomainModel):
    filename: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    bytes: int = Field(ge=0)


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
    diagnostics: XDiagnostics | None = None

    def identity(self) -> AccountIdentity | AccountDiagnosticIdentity:
        account = self.account.lower() if self.account is not None else ""
        if self.diagnostics is None:
            return AccountIdentity(account=account)
        diagnostics = self.diagnostics
        attempts: list[DiagnosticAttemptIdentity] = []
        for attempt in diagnostics.attempts:
            response = attempt.response
            attempts.append(
                DiagnosticAttemptIdentity(
                    attempt=attempt.attempt,
                    error=attempt.error,
                    status=attempt.status,
                    response=NoDiagnosticResponseIdentity()
                    if response is None
                    else DiagnosticResponseIdentity(
                        status=response.status,
                        bodyEncoding=response.bodyEncoding,
                        bodyBytes=response.bodyBytes,
                        bodySha256=response.bodySha256,
                        bodyTruncated=response.bodyTruncated,
                        omittedWireBytes=response.omittedWireBytes,
                        wireBytes=response.wireBytes,
                        captureLimitBytes=response.captureLimitBytes,
                        serializedBodyLimitBytes=response.serializedBodyLimitBytes,
                        readError=response.readError,
                        decodeError=response.decodeError,
                        redacted=response.redacted,
                    ),
                    headers={}
                    if response is None
                    else {
                        name: value
                        for name, value in response.headers.items()
                        if name not in {"date", "x-request-id", "x-transaction-id", "cf-ray"}
                    },
                )
            )
        return AccountDiagnosticIdentity(
            account=account,
            evidence=DiagnosticEvidenceIdentity(
                request=diagnostics.request, outcome=diagnostics.outcome, attempts=attempts
            ),
        )


class DocumentEntryError(ErrorEvidence):
    def identity(self) -> ExceptionIdentity:
        return ExceptionIdentity(error=re.sub(r"\d+", "#", self.error if self.error is not None else ""))


type SourceEntryError = DigestEntryError | ProposalEntryError | AccountEntryError | DocumentEntryError
type SourceErrorIdentity = (
    DigestIdentity | ProposalIdentity | AccountIdentity | AccountDiagnosticIdentity | ExceptionIdentity
)


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


class EntryErrorRenderContent(DomainModel):
    """Actual report content consumed before the final notice text exists."""

    notice_id: str
    source_id: str
    source_name: str
    feed_url: str
    seen_at: str
    entry_errors: list[SourceEntryError]


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
    document_manifest: list[DiagnosticDocumentReference] | None = None
    created_at: str
    created_at_prec: Literal["COLLECTION_ONLY"] = "COLLECTION_ONLY"
    created_at_prec_reason: None = None
    created_by: Literal["source-poller"] = "source-poller"
    text: str

    def storage_record(self) -> dict[str, JsonValue]:
        notice = EntryErrorNotice.model_validate(self)
        record: dict[str, JsonValue] = notice.model_dump(mode="json")
        record["entry_errors"] = [
            error.model_dump(mode="json", exclude_unset=True) for error in notice.entry_errors
        ]
        if notice.document_manifest is None:
            record.pop("document_manifest")
        return record
