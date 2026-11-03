import base64
import json
import re
from datetime import datetime
from itertools import permutations, product
from math import factorial, prod

from tank.sources.x_diagnostics import BODY_LIMIT, HEADERS, documents, redact, sanitize
from tank.util import sha256

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


def _ordered(value, fields, required=None):
    if (
        not isinstance(value, dict)
        or len(value) > len(fields)
        or set(value) - set(fields)
        or set(fields if required is None else required) - set(value)
    ):
        raise ValueError("Unsupported legacy diagnostic schema")
    return {key: value[key] for key in fields if key in value}


def _bounded_text(value, limit=8192):
    if not isinstance(value, str) or len(value) > limit:
        raise ValueError("Invalid legacy diagnostic text")


def _bounded_scalar(value, kind, minimum=0, maximum=2**63 - 1):
    if value is not None and (type(value) is not kind or not minimum <= value <= maximum):
        raise ValueError("Invalid legacy diagnostic scalar")


def _safe_text(value, limit=8192):
    _bounded_text(value, limit)
    if "\x00" in value or any(0xD800 <= ord(char) <= 0xDFFF for char in value) or redact(value) != value:
        raise ValueError("Unsafe diagnostic text")


def _preflight(errors, secrets):
    budget = 12_000_000
    consumer_fields = (
        "consumerCapturedBytes",
        "consumerCaptureTruncated",
        "consumerTransformTruncated",
        "consumerBodyLimitBytes",
    )
    text_fields = ("finalUrl", "title")
    count_fields = ("articleCount", "statusLinkCount", "textCount", "scriptCount")
    invalid_paths = (
        {"diagnostics", "request", "request.url", "attempts", "outcome", "invalidEvidence"}
        | {
            f"{field}[{index}]"
            for index in range(1, 4)
            for field in (
                "attempts",
                "error",
                "at",
                "status",
                "retryable",
                "response",
                "headers",
                "bodyEncoding",
                "body",
                "bodyText",
                "response.status",
                "bodyTruncated",
                "redacted",
                "omittedWireBytes",
                "wireBytes",
                "captureLimitBytes",
                "serializedBodyLimitBytes",
                "readError",
                "decodeError",
            )
            + text_fields
            + count_fields
        }
        | {f"headers[{index}].{header}" for index in range(1, 4) for header in HEADERS}
    )
    for error in errors:
        account = error.get("account")
        if account is not None:
            _bounded_text(account)
        value = error.get("diagnostics")
        if value is None:
            continue
        metadata = _ordered(
            value,
            ("request", "outcome", "attempts", "attemptCount", "invalidEvidence"),
            ("request", "outcome", "attempts", "attemptCount"),
        )
        if "invalidEvidence" in metadata:
            paths = metadata["invalidEvidence"]
            if not isinstance(paths, list) or len(paths) > len(invalid_paths):
                raise ValueError("Invalid diagnostic evidence paths")
            for path in paths:
                _bounded_text(path, 64)
                if path not in invalid_paths:
                    raise ValueError("Invalid diagnostic evidence path")
            if paths != sorted(set(paths)):
                raise ValueError("Invalid diagnostic evidence paths")
        request = _ordered(metadata["request"], ("method", "url", "body"))
        _safe_text(request["method"])
        _safe_text(request["url"])
        if request["method"] != "GET" or request["body"] is not None:
            raise ValueError("Invalid legacy diagnostic request")
        _bounded_text(metadata["outcome"])
        if metadata["outcome"] not in {"unknown", "exhausted", "deadline_exhausted", "permanent_failure"}:
            raise ValueError("Invalid legacy diagnostic outcome")
        attempts = metadata["attempts"]
        if (
            not isinstance(attempts, list)
            or len(attempts) > 3
            or type(metadata["attemptCount"]) is not int
            or metadata["attemptCount"] != len(attempts)
        ):
            raise ValueError("Invalid legacy diagnostic attempts")
        for index, original in enumerate(attempts, 1):
            attempt = _ordered(
                original, ("attempt", "at", "status", "retryable", "error", "response"), ("attempt", "error")
            )
            if type(attempt["attempt"]) is not int or attempt["attempt"] != index:
                raise ValueError("Invalid legacy diagnostic attempt number")
            _safe_text(attempt["error"])
            if "at" in attempt:
                _safe_text(attempt["at"], 64)
                if datetime.fromisoformat(attempt["at"]).tzinfo is None:
                    raise ValueError("Invalid diagnostic timestamp")
            _bounded_scalar(attempt.get("status"), int, 100, 599)
            _bounded_scalar(attempt.get("retryable"), bool)
            if "response" not in attempt:
                continue
            response = _ordered(
                attempt["response"],
                RESPONSE_FIELDS + consumer_fields + text_fields + count_fields,
                ("status", "headers", "body", "bodyEncoding", "bodyTruncated"),
            )
            headers = response["headers"]
            if not isinstance(headers, dict) or len(headers) > len(HEADERS):
                raise ValueError("Invalid legacy diagnostic headers")
            for key, item in headers.items():
                if not isinstance(key, str) or len(key) > 8192 or key not in HEADERS:
                    raise ValueError("Invalid legacy diagnostic header")
                _safe_text(item)
            _bounded_scalar(response["status"], int, 100, 599)
            for key in (
                "bodyBytes",
                "omittedWireBytes",
                "wireBytes",
                "captureLimitBytes",
                "serializedBodyLimitBytes",
            ):
                _bounded_scalar(response.get(key), int)
            for key in ("bodyTruncated", "redacted"):
                if key in response and type(response[key]) is not bool:
                    raise ValueError("Invalid legacy diagnostic flag")
            for key in ("readError", "decodeError"):
                if response.get(key) is not None:
                    _safe_text(response[key])
            for key in text_fields:
                if key in response:
                    _safe_text(response[key])
            for key in count_fields:
                _bounded_scalar(response.get(key), int, 0, BODY_LIMIT)
            for key in consumer_fields:
                if key in response:
                    _bounded_scalar(response[key], bool if "Truncated" in key else int)
            if "bodySha256" in response:
                _bounded_text(response["bodySha256"], 64)
                if re.fullmatch(r"[0-9a-f]{64}", response["bodySha256"]) is None:
                    raise ValueError("Invalid legacy diagnostic body hash")
            encoding = response["bodyEncoding"]
            _bounded_text(encoding)
            body = response["body"]
            limit = min(BODY_LIMIT, budget)
            _bounded_text(body, ((limit + 2) // 3) * 4 if encoding == "base64" else limit)
            if encoding == "base64":
                padding = 2 if body.endswith("==") else int(body.endswith("="))
                size = len(body) // 4 * 3 - padding
                if len(body) % 4 or not body.isascii():
                    raise ValueError("Invalid legacy diagnostic body encoding")
            elif encoding == "utf-8":
                size = 0
                for offset in range(0, len(body), 4096):
                    size += len(body[offset : offset + 4096].encode())
                    if size > limit:
                        raise ValueError("Legacy diagnostic body budget exhausted")
            else:
                raise ValueError("Invalid legacy diagnostic body encoding")
            if size > limit:
                raise ValueError("Legacy diagnostic body budget exhausted")
            budget -= size
            raw = base64.b64decode(body, validate=True) if encoding == "base64" else body.encode()
            if encoding == "base64" and base64.b64encode(raw).decode() != body:
                raise ValueError("Invalid diagnostic body encoding")
            if encoding == "utf-8" and "\x00" in body:
                raise ValueError("Invalid diagnostic body text")
            if "bodyBytes" in response and response["bodyBytes"] != len(raw):
                raise ValueError("Invalid diagnostic body length")
            if "bodySha256" in response and response["bodySha256"] != sha256(raw):
                raise ValueError("Invalid diagnostic body hash")
            if any(secret.encode() in raw for secret in secrets if secret):
                raise ValueError("Unsafe diagnostic body")
            text = raw.decode("latin-1")
            if redact(text, limit=BODY_LIMIT) != text:
                raise ValueError("Unsafe diagnostic body")
        if not _secret_free(metadata, secrets):
            raise ValueError("Unsafe diagnostic evidence")


def _secret_free(value, secrets):
    if isinstance(value, dict):
        return all(_secret_free(key, secrets) and _secret_free(item, secrets) for key, item in value.items())
    if isinstance(value, list):
        return all(_secret_free(item, secrets) for item in value)
    return not isinstance(value, str) or not any(secret in value for secret in secrets if secret)


def _legacy_metadata(value, budget, secrets):
    metadata = _ordered(value, ("request", "outcome", "attempts", "attemptCount"))
    metadata["request"] = _ordered(metadata["request"], ("method", "url", "body"))
    attempts = metadata["attempts"]
    if not isinstance(attempts, list) or len(attempts) > 3:
        raise ValueError("Invalid legacy diagnostic attempts")
    metadata["attempts"] = []
    for index, original in enumerate(attempts, 1):
        attempt = _ordered(
            original, ("attempt", "at", "status", "retryable", "error", "response"), ("attempt", "error")
        )
        if type(attempt["attempt"]) is not int or attempt["attempt"] != index:
            raise ValueError("Invalid legacy diagnostic attempt number")
        if "response" in attempt:
            response = _ordered(attempt["response"], RESPONSE_FIELDS)
            if not isinstance(response["body"], str) or len(response["body"]) > ((BODY_LIMIT + 2) // 3) * 4:
                raise ValueError("Invalid legacy diagnostic body")
            attempt["response"] = response
        metadata["attempts"].append(attempt)
    clean = sanitize(metadata, budget=budget)
    if "invalidEvidence" in clean or not _secret_free(metadata, secrets):
        raise ValueError("Unsafe legacy diagnostic evidence")
    for original, projected in zip(metadata["attempts"], clean["attempts"], strict=True):
        if "at" in original:
            projected["at"] = original["at"]
        if "response" in projected:
            projected["response"] = {key: projected["response"][key] for key in RESPONSE_FIELDS}
            response = original["response"]
            raw = (
                base64.b64decode(response["body"], validate=True)
                if response["bodyEncoding"] == "base64"
                else response["body"].encode()
            )
            if any(secret.encode() in raw for secret in secrets if secret):
                raise ValueError("Unsafe legacy diagnostic body")
    if json.dumps(clean, sort_keys=True) != json.dumps(metadata, sort_keys=True):
        raise ValueError("Legacy diagnostic evidence fails current validation")
    return metadata


def _matches(document, saved):
    return saved == {
        "filename": document["filename"],
        "sha256": document["sha256"],
        "bytes": len(document["content"]),
    }


def verified_documents(notice, secrets=()):
    version = notice.get("document_serialization_version", 1)
    if type(version) is not int or version not in {1, 2}:
        raise ValueError("Unsupported diagnostic serialization version")
    manifest = notice.get("document_manifest")
    if not isinstance(manifest, list) or not manifest or len(manifest) > 16_384:
        raise ValueError("Invalid diagnostic manifest")
    for saved in manifest:
        _ordered(saved, ("filename", "sha256", "bytes"))
        if (
            not isinstance(saved["filename"], str)
            or len(saved["filename"]) > 8192
            or not isinstance(saved["sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", saved["sha256"]) is None
            or type(saved["bytes"]) is not int
            or saved["bytes"] < 1
        ):
            raise ValueError("Invalid diagnostic manifest entry")
    if len({row["filename"] for row in manifest}) != len(manifest):
        raise ValueError("Duplicate diagnostic manifest filename")
    errors = notice.get("entry_errors")
    if (
        not isinstance(errors, list)
        or len(errors) > 4096
        or any(not isinstance(error, dict) for error in errors)
    ):
        raise ValueError("Invalid diagnostic entries")
    if version == 1 and notice.get("kind") == "ENTRY_ERRORS":
        _preflight(errors, secrets)
    attachments = documents(errors)
    if len(attachments) != len(manifest):
        raise ValueError("Diagnostic manifest count mismatch")
    pairs = list(zip(attachments, manifest, strict=True))
    if all(_matches(document, saved) for document, saved in pairs):
        return attachments
    if version != 1 or notice.get("kind") != "ENTRY_ERRORS":
        raise ValueError("Diagnostic manifest mismatch")
    budget = [12_000_000]
    metadata = [
        _legacy_metadata(error["diagnostics"], budget, secrets)
        for error in errors
        if error.get("diagnostics")
    ]
    pending = []
    for document, saved in pairs:
        if not document["filename"].endswith("-diagnostics.json"):
            if not _matches(document, saved):
                raise ValueError("Legacy diagnostic body manifest mismatch")
            continue
        value = metadata[len(pending)]
        canonical = json.loads(document["content"])
        for attempt, rendered in zip(value["attempts"], canonical["attempts"], strict=True):
            if "response" in attempt:
                response = attempt["response"]
                response.pop("body")
                if rendered["response"]["bodyDocument"] is None:
                    response["body"] = ""
                response["bodyDocument"] = rendered["response"]["bodyDocument"]
        responses = [attempt["response"] for attempt in value["attempts"] if "response" in attempt]
        candidates = prod(factorial(len(response["headers"])) for response in responses)
        if (
            candidates > 720
            or document["filename"] != saved["filename"]
            or len(document["content"]) != saved["bytes"]
        ):
            raise ValueError("Legacy diagnostic reconstruction refused")
        pending.append((document, saved, value, responses, candidates))
    if sum(row[4] for row in pending) > 4096:
        raise ValueError("Legacy diagnostic notice candidate budget exhausted")
    for document, saved, value, responses, _ in pending:
        for combination in product(*(permutations(response["headers"].items()) for response in responses)):
            for response, headers in zip(responses, combination, strict=True):
                response["headers"] = dict(headers)
            content = json.dumps(value, ensure_ascii=False, indent=2).encode()
            candidate = {"filename": document["filename"], "content": content, "sha256": sha256(content)}
            if _matches(candidate, saved):
                document.update(candidate)
                break
        else:
            raise ValueError("Legacy diagnostic manifest mismatch")
    return attachments
