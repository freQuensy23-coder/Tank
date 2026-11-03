import re
from contextlib import suppress
from uuid import UUID

import httpx
import psycopg

from tank.util import iso

MAX_CHAIN = 8
MAX_RECEIPT_BYTES = 4096
MAX_HISTORY_BYTES = 131072


def _typed_text(value, pattern, secrets):
    if type(value) is not str or len(value) > 128 or not re.fullmatch(pattern, value):
        return None
    return None if any(secret and secret in value for secret in secrets) else value


def failure_receipt(job, exc, *, settings, phase, started_at, finished_at, deadline_at):
    configured = getattr(settings, "configured_secrets", None)
    secrets = configured() if configured else ()
    chain = []
    seen = set()
    error = exc
    while error is not None and id(error) not in seen and len(chain) < MAX_CHAIN:
        seen.add(id(error))
        cls = type(error)
        module = _typed_text(cls.__module__, r"[A-Za-z_][A-Za-z0-9_.]{0,63}", secrets)
        name = _typed_text(cls.__name__, r"[A-Za-z_][A-Za-z0-9_]{0,63}", secrets)
        entry = {"class": f"{module}.{name}" if module and name else "unknown"}
        response = (
            error.response
            if isinstance(error, httpx.HTTPStatusError)
            else vars(error).get("_tank_http_response")
        )
        if isinstance(response, httpx.Response):
            status = response.status_code
            if type(status) is int and 100 <= status <= 599:
                entry["http_status"] = status
            for header, key in (
                ("x-request-id", "request_id"),
                ("x-clickhouse-query-id", "query_id"),
            ):
                value = _typed_text(response.headers.get(header), r"[0-9a-fA-F-]{36}", secrets)
                if value is not None:
                    with suppress(ValueError):
                        normalized = _typed_text(str(UUID(value)), r"[0-9a-f-]{36}", secrets)
                        if normalized is not None:
                            entry[key] = normalized
        if isinstance(error, psycopg.Error):
            constraint = _typed_text(error.diag.constraint_name, r"[A-Za-z_][A-Za-z0-9_]{0,62}", secrets)
            sqlstate = _typed_text(error.sqlstate, r"[0-9A-Z]{5}", secrets)
            if constraint:
                entry["db_constraint"] = constraint
            if sqlstate:
                entry["sqlstate"] = sqlstate
        chain.append(entry)
        error = error.__cause__ or (None if error.__suppress_context__ else error.__context__)
    identity = {}
    for key, pattern in (
        ("code_version", r"[0-9a-f]{64}"),
        ("config_version", r"(?:[0-9a-f]{40}|dev\.[0-9a-f]{64})"),
        ("build_sha", r"[0-9a-f]{40}"),
    ):
        identity[key] = _typed_text(getattr(settings, key, None), pattern, secrets)
    return {
        "job_id": job["job_id"],
        "stage": job["type"],
        "attempt": job["attempts"],
        "phase": phase,
        "started_at": iso(started_at),
        "finished_at": iso(finished_at),
        "handler_deadline_at": iso(deadline_at) if deadline_at is not None else None,
        "expires_at": job.get("expires_at"),
        "runtime": identity,
        "exception_chain": chain,
        "chain_truncated": error is not None,
    }
