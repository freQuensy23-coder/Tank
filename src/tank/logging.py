"""Application logging with credential-safe URL and exception rendering."""

import asyncio
import inspect
import json
import logging
import re
import time
import traceback
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from urllib.parse import unquote, urlsplit
from uuid import uuid4

_KNOWN_SECRETS: set[str] = set()
_MARKET_STAGE: ContextVar[tuple[str, str] | None] = ContextVar("market_stage", default=None)


@contextmanager
def market_stage(stage, *, trace_id=None, cpu=False, **fields):
    parent = _MARKET_STAGE.get()
    trace = trace_id or (parent[0] if parent else uuid4().hex)
    span = uuid4().hex
    record = {
        "stage": stage,
        "trace_id": trace,
        "span_id": span,
        "parent_id": parent[1] if parent and parent[0] == trace else None,
    }
    for key in ("window", "job_id", "call_id", "query_id", "rows", "bytes", "queue_s", "queued_wall_s"):
        value = fields.get(key)
        if isinstance(value, (int, float)) or isinstance(value, str) and re.fullmatch(r"[\w.:-]{1,128}", value):
            record[key] = value
    logger = logging.getLogger("tank.market_timing")
    token = _MARKET_STAGE.set((trace, span))
    status = "ok"
    wall = time.perf_counter()
    thread = time.thread_time() if cpu else None
    try:
        logger.info(json.dumps(dict(record, status="started")))
        yield record
    except BaseException as error:
        status = "cancelled" if isinstance(error, asyncio.CancelledError) else "error"
        raise
    finally:
        elapsed = time.perf_counter() - wall
        used = time.thread_time() - thread if thread is not None else None
        _MARKET_STAGE.reset(token)
        logger.info(json.dumps(dict(record, status=status, wall_s=elapsed, thread_cpu_s=used)))


def market_timed(stage):
    def decorate(function):
        if inspect.iscoroutinefunction(function):

            @wraps(function)
            async def asynchronous(*args, **kwargs):
                with market_stage(stage):
                    return await function(*args, **kwargs)

            return asynchronous

        @wraps(function)
        def synchronous(*args, **kwargs):
            with market_stage(stage, cpu=True):
                return function(*args, **kwargs)

        return synchronous

    return decorate


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
