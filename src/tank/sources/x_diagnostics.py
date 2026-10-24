import base64
import binascii
import json
import re

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


def redact(value, token=None):
    text = redact_text(str(value), (token,) if token else ())
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


def sanitize(value, token=None):
    if not isinstance(value, dict):
        return None
    request = value.get("request") or {}
    result = {
        "request": {
            "method": "GET",
            "url": redact(request.get("url", ""), token),
            "body": None,
        },
        "outcome": redact(value.get("outcome", "unknown"), token),
        "attempts": [],
    }
    for original in (value.get("attempts") or [])[:3]:
        attempt = {key: original[key] for key in ("attempt", "at", "status", "retryable") if key in original}
        attempt["error"] = redact(original.get("error", ""), token)
        if response := original.get("response"):
            encoding = response.get("bodyEncoding", "utf-8")
            body = str(response.get("body", ""))
            try:
                raw = base64.b64decode(body, validate=True) if encoding == "base64" else body.encode()
            except (ValueError, binascii.Error):
                raw = b"INVALID_DIAGNOSTIC_BODY_ENCODING"
                encoding = "utf-8"
            clipped = len(raw) > BODY_LIMIT
            original_raw = raw[:BODY_LIMIT]
            raw = redact(original_raw.decode("latin-1"), token).encode("latin-1")
            safe_body = (
                base64.b64encode(raw).decode()
                if encoding == "base64"
                else raw.decode("utf-8", errors="replace")
            )
            if encoding != "base64":
                raw = safe_body.encode()
            attempt["response"] = {
                "status": response.get("status"),
                "headers": {
                    str(key).lower(): redact(val, token)
                    for key, val in (response.get("headers") or {}).items()
                    if str(key).lower() in HEADERS
                },
                "body": safe_body,
                "bodyEncoding": encoding,
                "bodyBytes": len(raw),
                "bodySha256": sha256(raw),
                "bodyTruncated": bool(response.get("bodyTruncated")) or clipped,
                "omittedWireBytes": response.get("omittedWireBytes"),
                "wireBytes": response.get("wireBytes"),
                "captureLimitBytes": response.get("captureLimitBytes"),
                "serializedBodyLimitBytes": response.get("serializedBodyLimitBytes"),
                "readError": redact(response.get("readError"), token) if response.get("readError") else None,
                "decodeError": redact(response.get("decodeError"), token)
                if response.get("decodeError")
                else None,
                "redacted": bool(response.get("redacted")) or raw != original_raw,
            }
        result["attempts"].append(attempt)
    result["attemptCount"] = len(result["attempts"])
    return result


def evidence_identity(diagnostics):
    return {
        "request": diagnostics["request"],
        "outcome": diagnostics["outcome"],
        "attempts": [
            {
                "attempt": attempt.get("attempt"),
                "error": attempt.get("error"),
                "status": attempt.get("status"),
                "response": {
                    key: value
                    for key, value in (attempt.get("response") or {}).items()
                    if key not in {"headers", "body"}
                },
                "headers": {
                    key: value
                    for key, value in (attempt.get("response") or {}).get("headers", {}).items()
                    if key not in {"date", "x-request-id", "x-transaction-id", "cf-ray"}
                },
            }
            for attempt in diagnostics["attempts"]
        ],
    }


def documents(errors):
    result = []
    for index, error in enumerate(errors, 1):
        diagnostics = error.get("diagnostics")
        if not diagnostics:
            continue
        account = re.sub(r"[^a-z0-9_]", "_", str(error.get("account") or "run").lower())[:30]
        metadata = json.loads(json.dumps(diagnostics))
        for attempt in metadata["attempts"]:
            response = attempt.get("response")
            if response is None:
                continue
            content = (
                base64.b64decode(response.pop("body"))
                if response["bodyEncoding"] == "base64"
                else response.pop("body").encode()
            )
            if not content:
                response["body"] = ""
                response["bodyDocument"] = None
                continue
            suffix = "bin" if response["bodyEncoding"] == "base64" else "txt"
            filename = f"x-{account}-{index}-attempt-{attempt['attempt']}-{sha256(content)[:12]}.{suffix}"
            response["bodyDocument"] = filename
            result.append({"filename": filename, "content": content, "sha256": sha256(content)})
        content = json.dumps(metadata, ensure_ascii=False, indent=2).encode()
        result.append(
            {
                "filename": f"x-{account}-{index}-diagnostics.json",
                "content": content,
                "sha256": sha256(content),
            }
        )
    return result
