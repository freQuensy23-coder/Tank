import base64
import binascii
import json
import re
from datetime import datetime

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
    value = re.sub(
        r"(?i)([?&](?:api[-_]?(?:key|token)|access_token|token|auth|password|secret)=)[^&\s'\"<>]*",
        r"\1[REDACTED]",
        value,
    )
    text = redact_text(value, (token,) if token else ())
    return re.sub(
        r"(?i)((?<![A-Za-z0-9_])(?:authorization|set-cookie|cookie|auth_token|api[-_]?token|auth|ct0)[\"']?[ \t]*[:=][ \t]*)"
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


def text_prefix(body, limit):
    parts = []
    size = 0
    offset = 0
    while offset < len(body) and size < limit:
        count = min(4096, limit - size)
        chunk = body[offset : offset + count].encode("utf-8", "surrogatepass")
        parts.append(chunk[: limit - size])
        size += len(parts[-1])
        offset += count
        if len(parts[-1]) != len(chunk):
            return b"".join(parts), True
    return b"".join(parts), offset < len(body)


def sanitize(value, token=None, budget=None):
    if value is None:
        return None
    invalid = []

    def container(parent, key, kind, default, path):
        candidate = parent.get(key, default)
        if not isinstance(candidate, kind):
            invalid.append(path)
            return default
        return candidate

    def text(candidate, path):
        if not isinstance(candidate, str):
            invalid.append(path)
            return "INVALID_DIAGNOSTIC_TEXT"
        if "\x00" in candidate[:8192] or any(0xD800 <= ord(c) <= 0xDFFF for c in candidate[:8192]):
            invalid.append(path)
        safe = redact(candidate, token).replace("\x00", "[NUL]")
        return safe.encode("utf-8", "replace").decode()[:8192]

    def scalar(parent, key, kind, path, minimum=0, maximum=2**63 - 1):
        candidate = parent.get(key)
        if candidate is None:
            return None
        if type(candidate) is kind and (kind is bool or minimum <= candidate <= maximum):
            return candidate
        invalid.append(path)
        return None

    if not isinstance(value, dict):
        invalid.append("diagnostics")
        value = {}
    request = container(value, "request", dict, {}, "request")
    attempts = container(value, "attempts", list, [], "attempts")
    if budget is None:
        budget = [12_000_000]
    outcome = value.get("outcome", "unknown")
    if not isinstance(outcome, str) or outcome not in {
        "unknown",
        "exhausted",
        "deadline_exhausted",
        "permanent_failure",
    }:
        invalid.append("outcome")
        outcome = "unknown"
    result = {
        "request": {"method": "GET", "url": text(request.get("url", ""), "request.url"), "body": None},
        "outcome": outcome,
        "attempts": [],
    }
    for index, original in enumerate(attempts[:3], 1):
        if not isinstance(original, dict):
            invalid.append(f"attempts[{index}]")
            original = {}
        attempt = {"attempt": index, "error": text(original.get("error", ""), f"error[{index}]")}
        if "at" in original:
            timestamp = original["at"]
            try:
                if not isinstance(timestamp, str) or len(timestamp) > 64:
                    raise ValueError
                parsed = datetime.fromisoformat(timestamp)
                if parsed.tzinfo is None:
                    raise ValueError
                attempt["at"] = parsed.isoformat()
            except ValueError:
                invalid.append(f"at[{index}]")
        for key, kind, maximum in (("status", int, 599), ("retryable", bool, 1)):
            if key in original:
                attempt[key] = scalar(original, key, kind, f"{key}[{index}]", 100, maximum)
        response = original.get("response")
        if response is not None:
            if not isinstance(response, dict):
                invalid.append(f"response[{index}]")
                response = {}
            headers = container(response, "headers", dict, {}, f"headers[{index}]")
            encoding = response.get("bodyEncoding", "utf-8")
            if not isinstance(encoding, str) or encoding not in {"utf-8", "base64"}:
                invalid.append(f"bodyEncoding[{index}]")
                encoding = "utf-8"
            body = response.get("body", "")
            if not isinstance(body, str):
                invalid.append(f"body[{index}]")
                body = ""
            limit = min(BODY_LIMIT, budget[0])
            if encoding == "base64":
                encoded_limit = ((limit + 2) // 3) * 4
                try:
                    decoded = base64.b64decode(body[:encoded_limit], validate=True)
                    clipped = len(body) > encoded_limit or len(decoded) > limit
                    original_raw = decoded[:limit]
                except (ValueError, binascii.Error):
                    invalid.append(f"bodyEncoding[{index}]")
                    original_raw, clipped = b"", bool(body)
            else:
                original_raw, clipped = text_prefix(body, limit)
            byte_token = token.encode("utf-8").decode("latin-1") if token else None
            transformed = redact(original_raw.decode("latin-1"), byte_token, BODY_LIMIT).encode("latin-1")
            raw = transformed[:limit]
            transformed_clipped = len(transformed) > limit
            budget[0] -= max(len(original_raw), len(raw))
            try:
                safe_body = raw.decode("utf-8")
                if "\x00" in safe_body:
                    invalid.append(f"bodyText[{index}]")
                    raise ValueError
                if encoding == "base64":
                    raise ValueError
                encoding = "utf-8"
            except (UnicodeDecodeError, ValueError):
                safe_body = base64.b64encode(raw).decode("ascii")
                encoding = "base64"
            clean_response = {
                "status": scalar(response, "status", int, f"response.status[{index}]", 100, 599),
                "headers": {
                    key.lower(): text(val, f"headers[{index}].{key.lower()}")
                    for key, val in headers.items()
                    if isinstance(key, str) and key.lower() in HEADERS
                },
                "body": safe_body,
                "bodyEncoding": encoding,
                "bodyBytes": len(raw),
                "bodySha256": sha256(raw),
                "bodyTruncated": bool(scalar(response, "bodyTruncated", bool, f"bodyTruncated[{index}]"))
                or clipped
                or transformed_clipped,
                "consumerCapturedBytes": len(original_raw),
                "consumerCaptureTruncated": clipped,
                "consumerTransformTruncated": transformed_clipped,
                "consumerBodyLimitBytes": limit,
                "redacted": bool(scalar(response, "redacted", bool, f"redacted[{index}]"))
                or transformed != original_raw,
            }
            for key in ("omittedWireBytes", "wireBytes", "captureLimitBytes", "serializedBodyLimitBytes"):
                clean_response[key] = scalar(response, key, int, f"{key}[{index}]")
            for key in ("readError", "decodeError"):
                candidate = response.get(key)
                clean_response[key] = text(candidate, f"{key}[{index}]") if candidate is not None else None
            for key in ("finalUrl", "title"):
                if key in response:
                    clean_response[key] = text(response[key], f"{key}[{index}]")
            for key in ("articleCount", "statusLinkCount", "textCount", "scriptCount"):
                if key in response:
                    clean_response[key] = scalar(response, key, int, f"{key}[{index}]", 0, BODY_LIMIT)
            attempt["response"] = clean_response
        result["attempts"].append(attempt)
    result["attemptCount"] = len(result["attempts"])
    if "invalidEvidence" in value:
        invalid.append("invalidEvidence")
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
        metadata = {
            **diagnostics,
            "attempts": [
                {
                    **attempt,
                    **({"response": dict(attempt["response"])} if "response" in attempt else {}),
                }
                for attempt in diagnostics["attempts"]
            ],
        }
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
