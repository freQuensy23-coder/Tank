import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from tank.clickhouse_market import ClickHouse

_SAFE_WORDS = frozenset(
    "DB Exception Syntax error failed at position Expected one of end query expression "
    "Unknown identifier function aggregate Missing columns column not found in scope "
    "Illegal type types argument arguments for function There is no supertype because some "
    "of them are String FixedString and other not Nullable Array Tuple Decimal Decimal256 "
    "Float64 Float32 Int64 UInt64 UInt8 Nothing Cannot convert parse value from to "
    "Nested inside Nullable argument must be constant Number does match Overflow "
    "Division by zero division max maximum size depth exceeded Too large AST expanded "
    "Unexpected character token before after Type mismatch incompatible scale precision "
    "First second third fourth return result has different sizes JOIN ON AS WITH SELECT "
    "IN IN_SCOPE UNKNOWN_IDENTIFIER UNKNOWN_FUNCTION SYNTAX_ERROR ILLEGAL_TYPE_OF_ARGUMENT "
    "NO_COMMON_TYPE NUMBER_OF_ARGUMENTS_DOESNT_MATCH BAD_ARGUMENTS TYPE_MISMATCH "
    "DECIMAL_OVERFLOW TOO_LARGE_QUERY TOO_DEEP_AST TOO_BIG_AST CANNOT_PARSE_TEXT "
    "CANNOT_PARSE_NUMBER ATTEMPT_TO_READ_AFTER_EOF UNKNOWN_SETTING UNSUPPORTED_METHOD "
    "native financial range Input scientific notation refused ClickHouse HTTP URI Request "
    "Too Long Bad Gateway Service Unavailable Internal Server Error".split()
)


def failure_receipt(error, nodeid, query):
    message = str(error)
    status = re.match(r"ClickHouse (\d{3}):", message)
    code = re.search(r"\bCode: (\d+)\b", message)
    message = re.split(r"(?i)\b(?:while processing|in scope|stack trace)\b", message)[0]
    message = re.sub(r"(['\"`]).*?\1", " [redacted] ", message, flags=re.DOTALL)
    words = re.findall(r"[A-Za-z_]+", message)
    safe = " ".join(word if word in _SAFE_WORDS else "[redacted]" for word in words)
    safe = re.sub(r"(?:\[redacted\] ?)+", "[redacted] ", safe).strip()[:512]
    return {
        "exception_class": type(error).__name__,
        "http_status": int(status[1]) if status else None,
        "native_error_code": int(code[1]) if code else None,
        "server_message": safe,
        "nodeid": nodeid,
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
        try:
            os.link(temporary, destination)
        except FileExistsError:
            pass
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
            try:
                retain_first_failure(
                    Path(__file__).resolve().parents[1] / ".test-results",
                    failure_receipt(error, self.nodeid, query),
                )
            except (OSError, ValueError):
                pass
            raise
