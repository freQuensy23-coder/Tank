import contextlib
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from tank.clickhouse_market import ClickHouse

_SAFE_PHRASES = (
    "Unknown identifier", "Unknown expression or function identifier", "Unknown function",
    "Syntax error", "Expected one of", "Illegal type", "Nested type", "inside Nullable",
    "There is no supertype", "Cannot convert", "Cannot parse", "Division by zero",
    "Decimal math overflow", "Too large", "Too deep", "AST", "scale", "precision",
    "native financial range", "UNKNOWN_IDENTIFIER", "UNKNOWN_FUNCTION", "SYNTAX_ERROR",
    "ILLEGAL_TYPE_OF_ARGUMENT", "NO_COMMON_TYPE", "NUMBER_OF_ARGUMENTS_DOESNT_MATCH",
    "BAD_ARGUMENTS", "TYPE_MISMATCH", "DECIMAL_OVERFLOW", "TOO_LARGE_QUERY",
    "TOO_DEEP_AST", "TOO_BIG_AST", "CANNOT_PARSE_TEXT", "CANNOT_PARSE_NUMBER",
    "ATTEMPT_TO_READ_AFTER_EOF", "UNKNOWN_SETTING", "UNSUPPORTED_METHOD",
    "URI", "Request", "Too Long", "Bad Gateway", "Service Unavailable", "Server Error",
)


def failure_receipt(error, nodeid, query):
    message = str(error)
    status = re.match(r"ClickHouse (\d{3}):", message)
    code = re.search(r"\bCode: (\d+)\b", message)
    message = re.split(r"(?i)\b(?:while processing|in scope|stack trace)\b", message)[0]
    diagnostics = [phrase for phrase in _SAFE_PHRASES if phrase in message]
    identifiers = set(re.findall(r"\b(?:spot|deriv|rel|lq)_[A-Za-z0-9_]+\b", query))
    diagnostics.extend(sorted(identifiers.intersection(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", message))))
    diagnostics.extend(re.findall(r"\bposition [0-9]{1,8}\b", message))
    safe = " | ".join(diagnostics)[:512] or "[redacted]"
    return {
        "exception_class": type(error).__name__[:128],
        "http_status": int(status[1]) if status else None,
        "native_error_code": int(code[1]) if code else None,
        "server_message": safe,
        "nodeid": nodeid[:512],
        "query_digest": hashlib.sha256(query.encode()).hexdigest(),
    }


def retain_first_failure(directory, receipt):
    directory = Path(directory)
    if directory.is_symlink():
        raise ValueError("SQL failure evidence directory must not be a symlink")
    directory.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(receipt["nodeid"].encode()).hexdigest()
    destination = directory / f"truth-sql-failure-{digest}.json"
    if destination.is_symlink():
        raise ValueError("SQL failure evidence destination must not be a symlink")
    descriptor, temporary = tempfile.mkstemp(prefix=".truth-sql-failure-", dir=directory)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(receipt, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        with contextlib.suppress(FileExistsError):
            os.link(temporary, destination)
    finally:
        os.unlink(temporary)


class EvidenceClickHouse(ClickHouse):
    def __init__(self, url, nodeid):
        if urlparse(url).hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("native SQL qualification requires a loopback service")
        super().__init__(url)
        self.nodeid = nodeid

    async def rows(self, query, params=None):
        try:
            return await super().rows(query, params)
        except Exception as error:
            with contextlib.suppress(OSError, ValueError):
                retain_first_failure(
                    Path(__file__).resolve().parents[1] / ".test-results",
                    failure_receipt(error, self.nodeid, query),
                )
            raise
