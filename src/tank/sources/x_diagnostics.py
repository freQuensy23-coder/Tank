import base64
import re
from collections.abc import Mapping, Sequence

from pydantic import JsonValue

from tank.contracts.source_errors import (
    AccountEntryError,
    SourceEntryError,
    XDiagnosticAttempt,
    XDiagnosticRequest,
    XDiagnosticResponse,
    XDiagnostics,
    parse_entry_error,
)
from tank.logging import redact_text
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


def admit_source_errors(
    errors: Sequence[SourceEntryError | Mapping[str, JsonValue]],
    token: str | None,
) -> list[SourceEntryError]:
    admitted: list[SourceEntryError] = []
    for error in errors:
        parsed = parse_entry_error(error)
        if isinstance(parsed, AccountEntryError) and parsed.diagnostics is not None:
            parsed = AccountEntryError.model_validate(
                parsed.model_copy(update={"diagnostics": sanitize(parsed.diagnostics, token)})
            )
        admitted.append(parsed)
    return admitted
