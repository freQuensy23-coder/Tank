"""Application logging with credential-safe URL and exception rendering."""

import asyncio
import hashlib
import inspect
import json
import logging
import math
import os
import re
import time
import traceback
from contextlib import asynccontextmanager, contextmanager, suppress
from contextvars import ContextVar
from functools import wraps
from urllib.parse import unquote, urlsplit
from uuid import uuid4

_KNOWN_SECRETS: set[str] = set()
_TIMING: ContextVar[dict | None] = ContextVar("market_timing", default=None)
_TIMING_LOG = logging.getLogger("tank.market_timing")
_TIMING_NUMBERS = frozenset({"rows", "bytes", "cache_hit", "durable_age_ms", "attempt", "range_ms"})
_TIMING_IDS = frozenset(
    {
        "job_id",
        "call_id",
        "session_id",
        "event_id",
        "token_id",
        "request_id",
        "query_id",
        "mta_id",
    }
)
_TIMING_LABELS = {
    "collection": frozenset(
        {
            "flow_bars",
            "klines",
            "books",
            "oi",
            "funding",
            "liquidations",
            "tickers",
            "supply",
        }
    ),
    "window": frozenset({"W15M", "W1H", "W4H", "W24H", "EVENT"}),
    "kind": frozenset({"price", "volume"}),
}


def timing_identity(value):
    return hashlib.sha256(str(value).encode()).hexdigest()[:32]


def timing_query_id():
    return (_TIMING.get() or {}).get("query_id") or uuid4().hex


def _timing_fields(fields):
    safe = {}
    for key, value in fields.items():
        if (
            key in _TIMING_NUMBERS
            and isinstance(value, (int, float))
            and math.isfinite(value)
            or key in _TIMING_IDS
            and isinstance(value, str)
            and re.fullmatch(r"[0-9a-f]{32}", value)
            or key in _TIMING_LABELS
            and isinstance(value, str)
            and value in _TIMING_LABELS[key]
        ):
            safe[key] = value
    return safe


def _timing_emit(record):
    with suppress(Exception):
        _TIMING_LOG.info(json.dumps(record, separators=(",", ":"), sort_keys=True))


@contextmanager
def timing_span(stage, *, root=False, cpu=False, started_ns=None, **fields):
    parent = _TIMING.get()
    values = dict(fields)
    if parent is None and not root:
        yield values
        return
    record = {
        "trace_id": parent["trace_id"] if parent else uuid4().hex,
        "span_id": uuid4().hex,
        "parent_span_id": parent["span_id"] if parent else None,
        "stage": stage,
        **{key: value for key, value in (parent or {}).items() if key in _TIMING_IDS},
        **_timing_fields(values),
    }
    begin = time.monotonic_ns() if started_ns is None else started_ns
    record["started_mono_ns"] = begin
    record["process_id"] = os.getpid()
    cpu_begin = time.thread_time_ns() if cpu else None
    token = _TIMING.set(record)
    _timing_emit({**record, "event": "stage_start"})
    status = "ok"
    try:
        yield values
    except asyncio.CancelledError:
        status = "cancelled"
        raise
    except BaseException:
        status = "error"
        raise
    finally:
        ended = time.monotonic_ns()
        wall = max(0, ended - begin) / 1_000_000
        used_cpu = max(0, time.thread_time_ns() - cpu_begin) / 1_000_000 if cpu_begin is not None else None
        _TIMING.reset(token)
        _timing_emit(
            {
                **record,
                **_timing_fields(values),
                "event": "stage_end",
                "status": status,
                "wall_ms": wall,
                "ended_mono_ns": ended,
                **({"thread_cpu_ms": used_cpu} if used_cpu is not None else {}),
            }
        )


def market_stage(stage, *, root=False, cpu=False):
    def decorate(function):
        if inspect.iscoroutinefunction(function):

            @wraps(function)
            async def asynchronous(*args, **kwargs):
                with timing_span(stage, root=root):
                    return await function(*args, **kwargs)

            return asynchronous

        @wraps(function)
        def synchronous(*args, **kwargs):
            with timing_span(stage, root=root, cpu=cpu):
                return function(*args, **kwargs)

        return synchronous

    return decorate


@asynccontextmanager
async def timing_lock(lock, stage):
    with timing_span(stage):
        await lock.acquire()
    try:
        yield
    finally:
        lock.release()


async def timing_thread(function, *args, **kwargs):
    queued = time.monotonic_ns()

    def run():
        with timing_span("threadpool.wait", started_ns=queued):
            pass
        with timing_span("threadpool.work", cpu=True):
            return function(*args, **kwargs)

    return await asyncio.to_thread(run)


def redact_text(value: str, secrets: tuple[str, ...] = ()) -> str:
    text = str(value)
    for secret in sorted((s for s in secrets if s), key=len, reverse=True):
        text = text.replace(secret, "[REDACTED]")
    text = re.sub(r"(https?://api\.telegram\.org/bot)[^/\s]+", r"\1[REDACTED]", text)
    text = re.sub(
        r"(?i)([?&](?:api[-_]?key|access_token|token|password|secret)=)[^&\s'\"<>]*",
        r"\1[REDACTED]",
        text,
    )
    text = re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/-]+", r"\1[REDACTED]", text)
    text = re.sub(r"\b(\w+://[^/:@\s]+:)[^/@\s]+(@)", r"\1[REDACTED]\2", text)
    return text


class CredentialFilter(logging.Filter):
    def __init__(self, secrets: tuple[str, ...] = ()) -> None:
        super().__init__()
        self.secrets = secrets

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_text(record.getMessage(), self.secrets)
        record.args = ()
        if record.exc_info:
            record.exc_text = redact_text("".join(traceback.format_exception(*record.exc_info)), self.secrets)
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = redact_text(record.exc_text, self.secrets)
        return True


def configure_logging(settings=None) -> None:
    """Install filters before clients are constructed, without logging settings."""
    values = []
    if settings is not None:
        for name in (
            "openma_api_key",
            "foundry_api_key",
            "telegram_token",
            "etherscan_api_key",
            "codex_gateway_key",
            "apify_api_token",
        ):
            value = getattr(settings, name, None)
            if value:
                values.append(str(value))
        for name in ("database_url", "openma_database_url"):
            value = getattr(settings, name, None)
            if value:
                password = urlsplit(str(value)).password
                if password:
                    values.extend((password, unquote(password)))

    _KNOWN_SECRETS.update(values)
    safe_filter = CredentialFilter(tuple(_KNOWN_SECRETS))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    for handler in logging.getLogger().handlers:
        for old in list(handler.filters):
            if isinstance(old, CredentialFilter):
                handler.removeFilter(old)
        handler.addFilter(safe_filter)

    for name in ("httpx", "httpcore", "tank"):
        logger = logging.getLogger(name)
        for old in list(logger.filters):
            if isinstance(old, CredentialFilter):
                logger.removeFilter(old)
        logger.addFilter(safe_filter)
        if name in {"httpx", "httpcore"}:
            logger.setLevel(logging.WARNING)
