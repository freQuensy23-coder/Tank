"""Application logging with credential-safe URL and exception rendering."""

import logging
import re
import traceback
from urllib.parse import unquote, urlsplit

from tank.settings import Settings

_KNOWN_SECRETS: set[str] = set()


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
    text = re.sub(r"(\w+://[^/:@\s]+:)[^/@\s]+(@)", r"\1[REDACTED]\2", text)
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


def configure_logging(settings: Settings | None = None) -> None:
    """Install filters before clients are constructed, without logging settings."""
    values = []
    if settings is not None:
        for name in (
            "openma_api_key",
            "opencode_api_key",
            "telegram_token",
            "etherscan_api_key",
            "codex_gateway_key",
            "apify_api_token",
        ):
            value = getattr(settings, name)
            if value:
                values.append(str(value))
        for name in ("database_url", "openma_database_url"):
            value = getattr(settings, name)
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
