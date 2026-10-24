import io
import logging

import httpx

from tank.logging import CredentialFilter, configure_logging
from tank.settings import Settings


async def test_credential_urls_are_redacted_even_when_httpx_info_enabled() -> None:
    configure_logging(Settings(etherscan_api_key="fake-query-secret", telegram_token="fake-bot-secret"))
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger = logging.getLogger("httpx")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200))) as client:
            await client.get("https://api.etherscan.io/v2/api?apikey=fake-query-secret&chainid=1")
            await client.post("https://api.telegram.org/botfake-bot-secret/sendMessage")
        text = stream.getvalue()
        assert "fake-query-secret" not in text
        assert "fake-bot-secret" not in text
        assert "[REDACTED]" in text
        assert "api.etherscan.io" in text
    finally:
        logger.removeHandler(handler)
        logger.setLevel(logging.WARNING)


def test_exception_trace_and_database_password_redacted() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(CredentialFilter(("fake-secret",)))
    logger = logging.getLogger("tank.test.safe-errors")
    logger.addHandler(handler)
    try:
        try:
            raise RuntimeError("postgresql://reader:db-password@host/tank; Bearer fake-secret")
        except RuntimeError:
            logger.exception("Request failed api_key=fake-secret")
        text = stream.getvalue()
        assert "fake-secret" not in text
        assert "db-password" not in text
        assert "RuntimeError" in text
        assert "[REDACTED]" in text
    finally:
        logger.removeHandler(handler)


def test_reload_preserves_old_and_new_credential_redaction() -> None:
    configure_logging(Settings(openma_api_key="old-generation-secret"))
    configure_logging(Settings(openma_api_key="new-generation-secret"))
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger = logging.getLogger("tank")
    logger.addHandler(handler)
    try:
        logger.warning("Old %s and new %s", "old-generation-secret", "new-generation-secret")
        assert "generation-secret" not in stream.getvalue()
        assert stream.getvalue().count("[REDACTED]") == 2
    finally:
        logger.removeHandler(handler)


def test_opencode_and_gateway_keys_redacted_from_logs() -> None:
    configure_logging(
        Settings(opencode_api_key="fake-router-log-key", codex_gateway_key="fake-gateway-log-key")
    )
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger = logging.getLogger("tank")
    logger.addHandler(handler)
    try:
        logger.warning("Provider %s gateway %s", "fake-router-log-key", "fake-gateway-log-key")
        assert "fake-router-log-key" not in stream.getvalue()
        assert "fake-gateway-log-key" not in stream.getvalue()
        assert stream.getvalue().count("[REDACTED]") == 2
    finally:
        logger.removeHandler(handler)
