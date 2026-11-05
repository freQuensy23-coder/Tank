import io
import logging

import httpx
import pytest

from tank.logging import CredentialFilter, configure_logging, redact_text
from tank.settings import Settings


async def test_credential_urls_are_redacted_even_when_httpx_info_enabled():
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


def test_exception_trace_and_database_password_redacted():
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


@pytest.mark.parametrize("scheme", ["postgresql", "postgresql+psycopg", "git+ssh", "x.custom-proto"])
@pytest.mark.parametrize("prefix", ["", "+", "-", ".", "( ", "1.", "12-", "9+", "_1.", "1._"])
def test_punctuated_scheme_credentials_are_redacted(scheme, prefix):
    value = f"{prefix}{scheme}://reader:db-password@host/path"
    assert redact_text(value) == f"{prefix}{scheme}://reader:[REDACTED]@host/path"


def test_reload_preserves_old_and_new_credential_redaction():
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


@pytest.mark.parametrize("scheme", ["", "Basic ", "Bearer\t", "Bearer\n"])
@pytest.mark.parametrize("leading", ["?", "?detail=before&"])
@pytest.mark.parametrize(
    "key",
    [
        "apikey",
        "api_key",
        "api-key",
        "access_token",
        "token",
        "password",
        "secret",
        "authorization",
        "set-cookie",
        "cookie",
        "auth_token",
        "ct0",
        "AUTH_TOKEN",
    ],
)
def test_query_redaction_removes_each_credential_piece_and_preserves_safe_neighbors(key, scheme, leading):
    pieces = [f"logging-{key}-{part}" for part in ("prefix", "suffix", "tail")]
    value = (
        f'url="https://x.com/error{leading}{key}={scheme}'
        + pieces[0]
        + ","
        + pieces[1]
        + ";"
        + pieces[2]
        + '&detail=after+%2C%3B"\nbody'
    )
    safe = redact_text(value)
    assert safe == f'url="https://x.com/error{leading}{key}=[REDACTED]&detail=after+%2C%3B"\nbody'
    assert all(piece not in safe for piece in pieces)


@pytest.mark.parametrize("leading", ["?", "?detail=before&"])
@pytest.mark.parametrize("key", ["auth_token", "ct0", "authorization", "api_key"])
@pytest.mark.parametrize(
    "value,replacement,boundary",
    [
        (
            '"query-prefix,query-suffix;query-tail"',
            '"[REDACTED]"',
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            "'query-prefix,query-suffix;query-tail'",
            "'[REDACTED]'",
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            '"query-prefix\\",query-suffix;query-tail"',
            '"[REDACTED]"',
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            "'query-prefix\\',query-suffix;query-tail'",
            "'[REDACTED]'",
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            '"query-prefix&inner=query-suffix;query-tail"',
            '"[REDACTED]"',
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            'Basic "query-prefix,query-suffix;query-tail"',
            '"[REDACTED]"',
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            "Bearer\t'query-prefix,query-suffix;query-tail'",
            "'[REDACTED]'",
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            'Bearer\n"query-prefix,query-suffix;query-tail"',
            '"[REDACTED]"',
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            ' \t"query-prefix,query-suffix;query-tail"',
            '"[REDACTED]"',
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            '"query-prefix,query-suffix;query-tail',
            '"[REDACTED]',
            "\r\nsafe body",
        ),
        (
            '""',
            '"[REDACTED]"',
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            "''",
            "'[REDACTED]'",
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            'Basic ""',
            '"[REDACTED]"',
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            "",
            "[REDACTED]",
            "&detail=after+%2C%3B\r\nsafe body",
        ),
        (
            "",
            "[REDACTED]",
            "",
        ),
        (
            "",
            "[REDACTED]",
            "\r\nsafe body",
        ),
        (
            " \t",
            "[REDACTED]",
            "\r\nsafe body",
        ),
        (
            "query-prefix,query-suffix;query-tail",
            "[REDACTED]",
            " safe body",
        ),
        (
            "query-prefix,query-suffix;query-tail",
            "[REDACTED]",
            "<safe>\r\nsafe body",
        ),
    ],
)
def test_query_value_grammar_consumes_quoted_and_empty_credentials(
    key, leading, value, replacement, boundary
):
    raw = f"safe π https://x.com/error{leading}{key}={value}{boundary}"
    expected = f"safe π https://x.com/error{leading}{key}={replacement}{boundary}"
    safe = redact_text(raw)
    assert safe == expected
    assert all(piece not in safe for piece in ("query-prefix", "query-suffix", "query-tail"))


def test_foundry_and_gateway_keys_redacted_from_logs():
    configure_logging(
        Settings(foundry_api_key="fake-router-log-key", codex_gateway_key="fake-gateway-log-key")
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
