import base64
import re
from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.logging import redact_text
from tank.sources.errors import (
    AccountEntryError,
    DiagnosticDocumentReference,
    SourceEntryError,
    XDiagnosticAttempt,
    XDiagnosticRequest,
    XDiagnosticResponse,
    XDiagnostics,
    parse_entry_error,
)
from tank.util import sha256

BODY_LIMIT = 2_000_000
HEADERS = {
    "content-type",
    "content-encoding",
    "content-length",
    "retry-after",
    "server",
    "date",
    "x-request-id",
    "x-transaction-id",
    "cf-ray",
    "x-rate-limit-limit",
    "x-rate-limit-remaining",
    "x-rate-limit-reset",
}


def redact(value: str, token: str | None = None) -> str:
    text = redact_text(value, (token,) if token is not None else ())
    return re.sub(
        r"(?i)((?<![A-Za-z0-9_])(?:authorization|set-cookie|cookie|auth_token|ct0)[\"']?[ \t]*[:=][ \t]*)"
        r"(\"(?:\\[^\r\n]|[^\"\\\r\n])*\"?|'(?:\\[^\r\n]|[^'\\\r\n])*'?|"
        r"(?:(?:Basic|Bearer)[ \t]+)?[^\s,;&<>{}\[\]\"']+)",
        lambda match: (
            match[1]
            + (match[2][0] if match[2][0] in "\"'" else "")
            + "[REDACTED]"
            + (match[2][0] if match[2][0] in "\"'" and match[2][-1] == match[2][0] else "")
        ),
        text,
    )


def sanitize(value: XDiagnostics | Mapping[str, JsonValue], token: str | None = None) -> XDiagnostics:
    diagnostics = XDiagnostics.model_validate(value)
    attempts: list[XDiagnosticAttempt] = []
    for original in diagnostics.attempts:
        response = original.response
        safe_response: XDiagnosticResponse | None = None
        if response is not None:
            raw = (
                base64.b64decode(response.body, validate=True)
                if response.bodyEncoding == "base64"
                else response.body.encode()
            )
            clipped = len(raw) > BODY_LIMIT
            original_raw = raw[:BODY_LIMIT]
            safe_raw = redact(original_raw.decode("latin-1"), token).encode("latin-1")
            safe_body = (
                base64.b64encode(safe_raw).decode()
                if response.bodyEncoding == "base64"
                else safe_raw.decode("utf-8", errors="replace")
            )
            if response.bodyEncoding == "utf-8":
                safe_raw = safe_body.encode()
            safe_response = XDiagnosticResponse.model_validate(
                response.model_copy(
                    update={
                        "headers": {
                            key.lower(): redact(val, token)
                            for key, val in response.headers.items()
                            if key.lower() in HEADERS
                        },
                        "body": safe_body,
                        "bodyBytes": len(safe_raw),
                        "bodySha256": sha256(safe_raw),
                        "bodyTruncated": response.bodyTruncated or clipped,
                        "readError": None
                        if response.readError is None
                        else redact(response.readError, token),
                        "decodeError": None
                        if response.decodeError is None
                        else redact(response.decodeError, token),
                        "redacted": response.redacted or safe_raw != original_raw,
                    }
                )
            )
        attempts.append(
            XDiagnosticAttempt.model_validate(
                original.model_copy(
                    update={"error": redact(original.error, token), "response": safe_response}
                )
            )
        )
    return XDiagnostics.model_validate(
        diagnostics.model_copy(
            update={
                "request": XDiagnosticRequest.model_validate(
                    diagnostics.request.model_copy(update={"url": redact(diagnostics.request.url, token)})
                ),
                "outcome": redact(diagnostics.outcome, token),
                "attempts": attempts,
            }
        )
    )


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
