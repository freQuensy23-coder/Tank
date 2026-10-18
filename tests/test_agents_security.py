from types import SimpleNamespace

import pytest

from tank.agents.security import redact_tool_arguments


@pytest.mark.parametrize("key", ["api_key", "apikey", "access_token", "token", "password", "secret", "auth"])
def test_evidence_url_credentials_are_redacted_and_refused(key):
    original = {"url": f"https://example.test/evidence?{key}=fake-sensitive&other=ok"}
    clean, reason = redact_tool_arguments(original, SimpleNamespace())
    assert "fake-sensitive" not in str(clean)
    assert reason and "fake-sensitive" not in reason
    assert "fake-sensitive" in original["url"]
    assert "other=ok" in clean["url"]


def test_domain_token_is_preserved_and_copy_is_detached():
    original = {"token": "aave:AAVE", "nested": [{"symbol": "AAVE"}]}
    clean, reason = redact_tool_arguments(original, SimpleNamespace())
    assert clean == original and reason is None
    clean["nested"][0]["symbol"] = "changed"
    assert original["nested"][0]["symbol"] == "AAVE"


def test_nested_explicit_auth_and_configured_secret_values_are_scrubbed():
    clean, reason = redact_tool_arguments(
        {"nested": [{"Authorization": "Bearer fake-one"}], "text": "prefix fake-two suffix"},
        SimpleNamespace(typesafe_api_key="fake-two", openma_url="https://example.test"),
    )
    assert reason and "fake-one" not in str(clean) and "fake-two" not in str(clean)
    assert "fake" not in reason


def test_public_url_and_empty_credentials_are_allowed():
    args = {"url": "https://example.test/token/AAVE?symbol=AAVE", "api_key": None}
    clean, reason = redact_tool_arguments(args, SimpleNamespace())
    assert clean == args and reason is None


def test_known_secret_is_scrubbed_when_url_encoded_under_unrecognized_key():
    clean, reason = redact_tool_arguments(
        {"url": "https://example.test?custom=fake%2Fvalue%2Bsecret"},
        SimpleNamespace(openma_api_key="fake/value+secret"),
    )
    assert reason and "fake" not in clean["url"]


@pytest.mark.parametrize(
    "setting,value",
    [
        ("telegram_token", "fake-telegram-bot-credential"),
        ("codex_gateway_key", "fake-gateway-credential"),
        ("openrouter_api_key", "fake-router-credential"),
    ],
)
def test_all_settings_token_and_key_credentials_are_scrubbed(setting, value):
    clean, reason = redact_tool_arguments(
        {"url": f"https://api.telegram.org/bot{value}/getMe", "query": f"look up {value}"},
        SimpleNamespace(**{setting: value}),
    )
    assert reason and value not in str(clean) and value not in reason


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://tank_agent:FAKEPASS@postgres/tank",
        "https://user:FAKEPASS@example.test/evidence",
        "Evidence from postgresql://tank_agent:FAKEPASS@postgres/tank please",
    ],
)
def test_url_and_dsn_userinfo_is_redacted_without_configured_secret(url):
    clean, reason = redact_tool_arguments({"query": url, "token": "aave:AAVE"}, SimpleNamespace())
    assert reason and "FAKEPASS" not in str(clean) and "tank_agent" not in str(clean)
    assert clean["token"] == "aave:AAVE"


def test_configured_database_password_is_scrubbed_from_plain_text():
    clean, reason = redact_tool_arguments(
        {"query": "password is FAKE/PASS"},
        SimpleNamespace(database_url="postgresql://tank_agent:FAKE%2FPASS@postgres/tank"),
    )
    assert reason and "FAKE" not in str(clean)
