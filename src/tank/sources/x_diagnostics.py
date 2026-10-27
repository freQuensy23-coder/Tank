import base64
import binascii
import json
import re
from itertools import permutations, product
from math import factorial

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


DOCUMENT_SERIALIZATION_VERSION = 2
LEGACY_CANDIDATE_LIMIT = 720
LEGACY_NOTICE_CANDIDATE_LIMIT = 4096
RESPONSE_FIELDS = (
    "status",
    "headers",
    "body",
    "bodyEncoding",
    "bodyBytes",
    "bodySha256",
    "bodyTruncated",
    "omittedWireBytes",
    "wireBytes",
    "captureLimitBytes",
    "serializedBodyLimitBytes",
    "readError",
    "decodeError",
    "redacted",
)


def _ordered_fields(value, fields, required=None):
    if (
        not isinstance(value, dict)
        or set(value) - set(fields)
        or set(fields if required is None else required) - set(value)
    ):
        raise ValueError("Unsupported diagnostic manifest schema")
    return {key: value[key] for key in fields if key in value}


def _integer(value, minimum=0):
    return type(value) is int and value >= minimum


def _validated_metadata(value):
    metadata = _ordered_fields(value, ("request", "outcome", "attempts", "attemptCount"))
    request = _ordered_fields(metadata["request"], ("method", "url", "body"))
    if request["method"] != "GET" or request["body"] is not None or not isinstance(request["url"], str):
        raise ValueError("Unsupported diagnostic manifest request")
    metadata["request"] = request
    attempts = metadata["attempts"]
    if (
        not isinstance(metadata["outcome"], str)
        or not isinstance(attempts, list)
        or len(attempts) > 3
        or not _integer(metadata["attemptCount"])
        or metadata["attemptCount"] != len(attempts)
    ):
        raise ValueError("Invalid diagnostic manifest attempts")
    metadata["attempts"] = []
    seen = set()
    for original in attempts:
        attempt = _ordered_fields(
            original,
            ("attempt", "at", "status", "retryable", "error", "response"),
            ("attempt", "error"),
        )
        if (
            not _integer(attempt["attempt"], 1)
            or attempt["attempt"] in seen
            or not isinstance(attempt["error"], str)
            or ("at" in attempt and not isinstance(attempt["at"], str))
            or ("status" in attempt and not _integer(attempt["status"]))
            or ("retryable" in attempt and type(attempt["retryable"]) is not bool)
        ):
            raise ValueError("Invalid diagnostic manifest attempt")
        seen.add(attempt["attempt"])
        if "response" in attempt:
            response = _ordered_fields(attempt["response"], RESPONSE_FIELDS)
            headers = response["headers"]
            if (
                not isinstance(headers, dict)
                or any(key not in HEADERS or not isinstance(val, str) for key, val in headers.items())
                or not isinstance(response["bodyEncoding"], str)
                or response["bodyEncoding"] not in {"utf-8", "base64"}
                or not isinstance(response["body"], str)
                or not _integer(response["bodyBytes"])
                or not isinstance(response["bodySha256"], str)
                or type(response["bodyTruncated"]) is not bool
                or type(response["redacted"]) is not bool
                or (response["status"] is not None and not _integer(response["status"]))
                or any(
                    response[key] is not None and not _integer(response[key])
                    for key in (
                        "omittedWireBytes",
                        "wireBytes",
                        "captureLimitBytes",
                        "serializedBodyLimitBytes",
                    )
                )
                or any(
                    response[key] is not None and not isinstance(response[key], str)
                    for key in ("readError", "decodeError")
                )
            ):
                raise ValueError("Invalid diagnostic manifest response")
            raw = (
                base64.b64decode(response["body"], validate=True)
                if response["bodyEncoding"] == "base64"
                else response["body"].encode("utf-8")
            )
            if len(raw) != response["bodyBytes"] or sha256(raw) != response["bodySha256"]:
                raise ValueError("Diagnostic manifest body integrity mismatch")
            response["headers"] = dict(headers)
            attempt["response"] = response
        metadata["attempts"].append(attempt)
    return metadata


def _metadata_bytes(metadata, version):
    return json.dumps(
        metadata, ensure_ascii=False, indent=2, sort_keys=version == DOCUMENT_SERIALIZATION_VERSION
    ).encode("utf-8")


def documents(errors, version=DOCUMENT_SERIALIZATION_VERSION):
    if type(version) is not int or version not in {1, DOCUMENT_SERIALIZATION_VERSION}:
        raise ValueError("Unsupported diagnostic manifest serialization version")
    if not isinstance(errors, list):
        raise ValueError("Invalid diagnostic manifest entry errors")
    result = []
    for index, error in enumerate(errors, 1):
        if not isinstance(error, dict):
            raise ValueError("Invalid diagnostic manifest entry error")
        diagnostics = error.get("diagnostics")
        if diagnostics is None:
            continue
        account = re.sub(r"[^a-z0-9_]", "_", str(error.get("account") or "run").lower())[:30]
        metadata = _validated_metadata(diagnostics)
        for attempt in metadata["attempts"]:
            response = attempt.get("response")
            if response is None:
                continue
            content = (
                base64.b64decode(response.pop("body"), validate=True)
                if response["bodyEncoding"] == "base64"
                else response.pop("body").encode("utf-8")
            )
            if not content:
                response["body"] = ""
                response["bodyDocument"] = None
                continue
            suffix = "bin" if response["bodyEncoding"] == "base64" else "txt"
            filename = f"x-{account}-{index}-attempt-{attempt['attempt']}-{sha256(content)[:12]}.{suffix}"
            response["bodyDocument"] = filename
            result.append({"filename": filename, "content": content, "sha256": sha256(content)})
        content = _metadata_bytes(metadata, version)
        result.append(
            {
                "filename": f"x-{account}-{index}-diagnostics.json",
                "content": content,
                "sha256": sha256(content),
            }
        )
    return result


def _matches_manifest(document, saved):
    return (
        document["filename"] == saved["filename"]
        and len(document["content"]) == saved["bytes"]
        and sha256(document["content"]) == saved["sha256"]
    )


def verified_documents(notice):
    if not isinstance(notice, dict) or notice.get("kind") != "ENTRY_ERRORS":
        raise ValueError("Unsupported diagnostic manifest notice")
    version = notice.get("document_serialization_version", 1)
    attachments = documents(notice.get("entry_errors"), version=version)
    manifest = notice.get("document_manifest")
    if not isinstance(manifest, list) or not manifest or len(attachments) != len(manifest):
        raise ValueError("Diagnostic document manifest count mismatch")
    filenames = set()
    for saved in manifest:
        _ordered_fields(saved, ("filename", "sha256", "bytes"))
        if (
            not isinstance(saved["filename"], str)
            or saved["filename"] in filenames
            or not isinstance(saved["sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", saved["sha256"]) is None
            or not _integer(saved["bytes"], 1)
        ):
            raise ValueError("Invalid diagnostic document manifest entry")
        filenames.add(saved["filename"])
    candidate_budget = LEGACY_NOTICE_CANDIDATE_LIMIT
    for document, saved in zip(attachments, manifest, strict=True):
        if version == DOCUMENT_SERIALIZATION_VERSION or not document["filename"].endswith(".json"):
            if not _matches_manifest(document, saved):
                raise ValueError("Diagnostic document manifest mismatch")
            continue
        if document["filename"] != saved["filename"] or len(document["content"]) != saved["bytes"]:
            raise ValueError("Legacy diagnostic document manifest mismatch")
        metadata = json.loads(document["content"])
        responses = [attempt["response"] for attempt in metadata["attempts"] if "response" in attempt]
        candidates = 1
        for response in responses:
            candidates *= factorial(len(response["headers"]))
        if candidates > LEGACY_CANDIDATE_LIMIT or candidates > candidate_budget:
            raise ValueError("Legacy diagnostic manifest header order exceeds reconstruction bound")
        candidate_budget -= candidates
        orders = [permutations(response["headers"].items()) for response in responses]
        for combination in product(*orders):
            for response, headers in zip(responses, combination, strict=True):
                response["headers"] = dict(headers)
            content = _metadata_bytes(metadata, 1)
            if len(content) == saved["bytes"] and sha256(content) == saved["sha256"]:
                document["content"] = content
                document["sha256"] = saved["sha256"]
                break
        else:
            raise ValueError("Legacy diagnostic document manifest mismatch")
    return attachments
