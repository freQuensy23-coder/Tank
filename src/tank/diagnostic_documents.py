"""Typed byte-preserving attachment presentation of admitted source diagnostics."""

import base64
import re
from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.source_errors import (
    AccountEntryError,
    DiagnosticDocumentReference,
    SourceEntryError,
    XDiagnosticRequest,
    parse_entry_error,
)
from tank.util import sha256


class DiagnosticAttachment(DomainModel):
    filename: str = Field(min_length=1)
    content: bytes

    @property
    def sha256(self) -> str:
        return sha256(self.content)

    def reference(self) -> DiagnosticDocumentReference:
        return DiagnosticDocumentReference(
            filename=self.filename, sha256=sha256(self.content), bytes=len(self.content)
        )


def absent_body(value: str | None) -> bool:
    return value is None


class DiagnosticDocumentResponse(DomainModel):
    status: int
    headers: dict[str, str]
    bodyEncoding: Literal["utf-8", "base64"]
    bodyBytes: int
    bodySha256: str
    bodyTruncated: bool
    omittedWireBytes: int | None = Field(default=None, ge=0)
    wireBytes: int | None = Field(default=None, ge=0)
    captureLimitBytes: int | None = Field(default=None, ge=0)
    serializedBodyLimitBytes: int | None = Field(default=None, ge=0)
    readError: str | None = None
    decodeError: str | None = None
    redacted: bool
    body: str | None = Field(default=None, exclude_if=absent_body)
    bodyDocument: str | None


class DiagnosticDocumentAttempt(DomainModel):
    attempt: int
    at: str
    status: int | None = None
    retryable: bool
    error: str
    response: DiagnosticDocumentResponse | None = None


class DiagnosticDocumentMetadata(DomainModel):
    request: XDiagnosticRequest
    outcome: str
    attemptCount: int
    attempts: list[DiagnosticDocumentAttempt]


def documents(
    errors: Sequence[SourceEntryError | Mapping[str, JsonValue]],
) -> list[DiagnosticAttachment]:
    result: list[DiagnosticAttachment] = []
    for index, error in enumerate(errors, 1):
        parsed = parse_entry_error(error)
        if not isinstance(parsed, AccountEntryError) or parsed.diagnostics is None:
            continue
        account = re.sub(r"[^a-z0-9_]", "_", (parsed.account if parsed.account else "run").lower())[:30]
        diagnostics = parsed.diagnostics
        attempts: list[DiagnosticDocumentAttempt] = []
        for attempt in diagnostics.attempts:
            response = attempt.response
            projected: DiagnosticDocumentResponse | None = None
            if response is not None:
                content = (
                    base64.b64decode(response.body, validate=True)
                    if response.bodyEncoding == "base64"
                    else response.body.encode()
                )
                filename: str | None = None
                if content:
                    suffix = "bin" if response.bodyEncoding == "base64" else "txt"
                    filename = (
                        f"x-{account}-{index}-attempt-{attempt.attempt}-{sha256(content)[:12]}.{suffix}"
                    )
                    result.append(DiagnosticAttachment(filename=filename, content=content))
                projected = DiagnosticDocumentResponse.model_validate(
                    {
                        **response.model_dump(mode="json", exclude_unset=True, exclude={"body"}),
                        "body": None if content else "",
                        "bodyDocument": filename,
                    }
                )
            attempts.append(
                DiagnosticDocumentAttempt.model_validate(
                    {
                        **attempt.model_dump(mode="json", exclude_unset=True, exclude={"response"}),
                        **({"response": projected} if "response" in attempt.model_fields_set else {}),
                    }
                )
            )
        metadata = DiagnosticDocumentMetadata(
            request=diagnostics.request,
            outcome=diagnostics.outcome,
            attemptCount=diagnostics.attemptCount,
            attempts=attempts,
        )
        result.append(
            DiagnosticAttachment(
                filename=f"x-{account}-{index}-diagnostics.json",
                content=metadata.model_dump_json(exclude_unset=True, indent=2).encode(),
            )
        )
    return result
