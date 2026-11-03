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


def redact(value, token=None, limit=8192):
    value = value[:limit] if isinstance(value, str) else "INVALID_DIAGNOSTIC_TEXT"
    text = redact_text(value, (token,) if token else ())
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


def sanitize(value, token=None, budget=None):
    if value is None:
        return None
    invalid = []
    if not isinstance(value, dict):
        invalid.append("diagnostics")
        value = {}
    request = value.get("request") or {}
    if not isinstance(request, dict):
        invalid.append("request")
        request = {}
    attempts = value.get("attempts") or []
    if not isinstance(attempts, list):
        invalid.append("attempts")
        attempts = []
    if budget is None:
        budget = [12_000_000]
    result = {
        "request": {
            "method": "GET",
            "url": redact(request.get("url", ""), token),
            "body": None,
        },
        "outcome": redact(value.get("outcome", "unknown"), token),
        "attempts": [],
    }
    for index, original in enumerate(attempts[:3], 1):
        if not isinstance(original, dict):
            invalid.append(f"attempts[{index}]")
            original = {}
        attempt = {
            key: original[key][:8192] if isinstance(original[key], str) else original[key]
            for key in ("at", "status", "retryable")
            if key in original and isinstance(original[key], (str, int, bool, type(None)))
        }
        attempt["attempt"] = index
        attempt["error"] = redact(original.get("error", ""), token)
        response = original.get("response")
        if response is not None:
            if not isinstance(response, dict):
                invalid.append(f"response[{index}]")
                response = {}
            headers = response.get("headers") or {}
            if not isinstance(headers, dict):
                invalid.append(f"headers[{index}]")
                headers = {}
            encoding = response.get("bodyEncoding", "utf-8")
            body = response.get("body", "")
            if not isinstance(body, str):
                invalid.append(f"body[{index}]")
                body = "INVALID_DIAGNOSTIC_BODY"
                encoding = "utf-8"
            limit = min(BODY_LIMIT, budget[0])
            encoded_limit = ((limit + 2) // 3) * 4
            clipped = len(body) > (encoded_limit if encoding == "base64" else limit)
            try:
                raw = (
                    base64.b64decode(body[:encoded_limit], validate=True)
                    if encoding == "base64"
                    else body[:limit].encode()
                )
            except (ValueError, binascii.Error):
                raw = b"INVALID_DIAGNOSTIC_BODY_ENCODING"
                encoding = "utf-8"
            clipped = clipped or len(raw) > limit
            original_raw = raw[:limit]
            budget[0] -= len(original_raw)
            raw = redact(original_raw.decode("latin-1"), token, BODY_LIMIT).encode("latin-1")
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
                    for key, val in headers.items()
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
    inherited = value.get("invalidEvidence")
    if isinstance(inherited, list):
        invalid.extend(item for item in inherited[:20] if isinstance(item, str))
    if invalid:
        result["invalidEvidence"] = sorted(set(invalid))
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
        content = json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True).encode()
        result.append(
            {
                "filename": f"x-{account}-{index}-diagnostics.json",
                "content": content,
                "sha256": sha256(content),
            }
        )
    return result
