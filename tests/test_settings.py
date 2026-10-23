import base64
import hashlib
import json

import pytest
import yaml

from tank.settings import Settings, resolve_versions


def test_runtime_generation_freezes_config_and_preserves_exact_recovery_inputs(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "thresholds.yaml").write_text("filter_cutoff: 0.71\n")
    (config / "schema.json").write_text('{"enum":["OLD"]}')
    settings = Settings(config_dir=config, data_dir=tmp_path / "data", telegram_token="fake-env-secret")
    old = resolve_versions(settings)
    (config / "thresholds.yaml").write_text("filter_cutoff: 0.81\n")
    (config / "schema.json").write_text('{"enum":["NEW"]}')
    (config / "new.json").write_text("{}")
    new = resolve_versions(settings)
    assert old.load_config("thresholds")["filter_cutoff"] == 0.71
    assert old.read_config_json("schema.json") == {"enum": ["OLD"]}
    assert not old.has_config("new.json")
    assert new.load_config("thresholds")["filter_cutoff"] == 0.81
    assert new.config_version != old.config_version
    assert new.code_version == old.code_version
    modified = old.load_config("thresholds")
    modified["filter_cutoff"] = 1
    assert old.load_config("thresholds")["filter_cutoff"] == 0.71
    artifact = (settings.data_dir / "config_versions" / f"{old.config_version}.json").read_bytes()
    assert b"fake-env-secret" not in artifact
    frozen = json.loads(artifact)
    digest = hashlib.sha256()
    for name, encoded in sorted(frozen["files_base64"].items()):
        digest.update(name.encode() + b"\0" + base64.b64decode(encoded) + b"\0")
    assert digest.hexdigest() == frozen["content_sha256"]
    assert base64.b64decode(frozen["files_base64"]["thresholds.yaml"]) == b"filter_cutoff: 0.71\n"
    with pytest.raises(ValueError, match="relative"):
        old.read_config_bytes("../outside")


def test_invalid_candidate_cannot_replace_valid_snapshot(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "thresholds.yaml").write_text("filter_cutoff: 0.71\n")
    settings = Settings(config_dir=config, data_dir=tmp_path / "data")
    old = resolve_versions(settings)
    (config / "thresholds.yaml").write_text("filter_cutoff: [\n")
    with pytest.raises(yaml.YAMLError):
        resolve_versions(settings)
    assert old.load_config("thresholds")["filter_cutoff"] == 0.71
    assert len(list((settings.data_dir / "config_versions").glob("*.json"))) == 1


def test_secrets_come_from_the_environment_names_of_the_env_file(monkeypatch):
    values = {
        "TELEGRAM_BOT_TOKEN": "FAKE-telegram",
        "CODEX_GATEWAY_KEY": "FAKE-internal-key",
        "OMA_API_KEY": "FAKE-oma-key",
        "ETHERSCAN_API_KEY": "FAKE-etherscan",
        "TANK_DB_PASSWORD": "FAKE tank/pass",
        "OMA_DB_PASSWORD": "FAKE-oma-db",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    settings = Settings(_env_file=None)
    assert settings.telegram_token == "FAKE-telegram"
    assert settings.codex_gateway_key == "FAKE-internal-key"
    assert settings.openma_api_key == "FAKE-oma-key"
    assert settings.etherscan_api_key == "FAKE-etherscan"
    assert settings.database_url == "postgresql://tank:FAKE%20tank%2Fpass@postgres:5432/tank"
    assert settings.openma_database_url == "postgresql://oma:FAKE-oma-db@postgres:5432/oma"
    assert not any(value in repr(settings) for value in values.values())
    assert not hasattr(settings, "secrets_dir")
    assert (
        Settings(telegram_token="FAKE-explicit-secret", _env_file=None).telegram_token
        == "FAKE-explicit-secret"
    )
    assert (
        Settings(database_url="postgresql:///explicit", _env_file=None).database_url
        == "postgresql:///explicit"
    )


def test_config_snapshots_refuse_auth_files_inside_config(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "thresholds.yaml").write_text("filter_cutoff: 0.71\n")
    auth = config / "synthetic-auth.json"
    auth.write_text('{"tokens":{"access_token":"FAKE-secret-must-not-be-archived"}}')
    settings = Settings(config_dir=config, data_dir=tmp_path / "data", codex_auth_path=auth)
    with pytest.raises(ValueError, match="outside the versioned configuration") as caught:
        resolve_versions(settings)
    assert "FAKE-secret" not in str(caught.value)
    assert not settings.data_dir.exists()


def test_deployment_values_come_from_frozen_config_with_explicit_override(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "delivery.yaml").write_text("alerts_chat_id: '-1000000000001'\nops_chat_id: '-1000000000002'\n")
    (config / "sources.yaml").write_text("contact_email: test@example.invalid\n")
    settings = Settings(
        config_dir=config,
        data_dir=tmp_path / "data",
        telegram_alerts_chat="explicit-test-chat",
        telegram_ops_chat=None,
    )
    frozen = resolve_versions(settings)
    assert frozen.telegram_alerts_chat == "explicit-test-chat"
    assert frozen.telegram_ops_chat == "-1000000000002"
    assert frozen.user_agent == "TankV0/1.0 (test@example.invalid)"
    (config / "delivery.yaml").write_text("alerts_chat_id: other\nops_chat_id: other\n")
    assert frozen.telegram_ops_chat == "-1000000000002"


def test_opencode_alias_and_secret(tmp_path, monkeypatch):
    monkeypatch.delenv("TANK_OPENCODE_API_KEY", raising=False)
    monkeypatch.delenv("OPENCODE_API_KEY", raising=False)
    assert Settings(_env_file=None).opencode_api_key is None
    monkeypatch.setenv("OPENCODE_API_KEY", "FAKE-raw-router")
    assert Settings(_env_file=None).opencode_api_key == "FAKE-raw-router"
    monkeypatch.setenv("TANK_OPENCODE_API_KEY", "FAKE-prefixed-router")
    settings = Settings(_env_file=None)
    assert settings.opencode_api_key == "FAKE-prefixed-router"
    assert "FAKE-prefixed-router" not in repr(settings)
    assert Settings(_env_file=None, opencode_api_key="FAKE-explicit").opencode_api_key == "FAKE-explicit"
    assert "jev_provider" not in Settings.model_fields


def test_codex_auth_default_only_resolves_path(tmp_path, monkeypatch):
    from pathlib import Path

    monkeypatch.delenv("TANK_CODEX_AUTH_PATH", raising=False)
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "custom-codex"))
    assert Settings(_env_file=None).codex_auth_path == tmp_path / "custom-codex/auth.json"
    monkeypatch.delenv("CODEX_HOME")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "home"))
    assert Settings(_env_file=None).codex_auth_path == tmp_path / "home/.codex/auth.json"
    assert (
        Settings(_env_file=None, codex_auth_path=tmp_path / "explicit").codex_auth_path
        == tmp_path / "explicit"
    )


def test_readiness_checks_opencode_key(tmp_path):
    from tank.cli import readiness_checks

    settings = Settings(_env_file=None, opencode_api_key="FAKE-ready-key")
    checks = {row["check"]: row for row in readiness_checks(settings)}
    assert checks["opencode_api_key"]["ok"]
    assert "jev_provider" not in checks
    assert "typesafe_api_key" not in checks
    assert "FAKE-ready-key" not in str(checks)


def test_x_disabled_is_explicit_readiness_scope_not_missing_credentials(tmp_path):
    from tank.cli import readiness_checks

    settings = Settings(_env_file=None, x_enabled=False)
    checks = {row["check"]: row for row in readiness_checks(settings)}
    assert checks["x_disabled_by_operator"] == {
        "check": "x_disabled_by_operator",
        "ok": True,
        "enabled": False,
    }
    assert "x_accounts" not in checks


@pytest.mark.parametrize("name", ["TANK_USER_AGENT", "TANK_TELEGRAM_ALERTS_CHAT", "TANK_TELEGRAM_OPS_CHAT"])
def test_configuration_values_never_come_from_environment(monkeypatch, caplog, name):
    monkeypatch.setenv(name, "FAKE-environment-value")
    settings = Settings(_env_file=None)
    assert getattr(settings, name.removeprefix("TANK_").lower()) != "FAKE-environment-value"
    assert name in caplog.text and "FAKE-environment-value" not in caplog.text


def test_user_agent_and_chats_from_configuration(tmp_path):
    from tank.cli import readiness_checks

    config = tmp_path / "config"
    config.mkdir()
    (config / "delivery.yaml").write_text("alerts_chat_id: '-1001'\nops_chat_id: '-1002'\n")
    (config / "sources.yaml").write_text("contact_email: ops@example.org\nsources:\n- source_id: S29\n")
    (config / "registry.yaml").write_text("x_accounts:\n- handle: ethereum\n")
    frozen = resolve_versions(Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data"))
    assert frozen.user_agent == "TankV0/1.0 (ops@example.org)"
    assert (frozen.telegram_alerts_chat, frozen.telegram_ops_chat) == ("-1001", "-1002")
    checks = {row["check"]: row for row in readiness_checks(frozen)}
    assert checks["user_agent_contact"]["ok"] and checks["x_accounts"] == {
        "check": "x_accounts",
        "ok": True,
        "count": 1,
    }
    assert "x_list_id" not in checks
    legacy = frozen.model_copy(update={"user_agent": "TankV0/1.0 contact=ops@example.org"})
    assert not {row["check"]: row for row in readiness_checks(legacy)}["user_agent_contact"]["ok"]


def test_database_urls_from_the_role_passwords(monkeypatch):
    for name in ("TANK_DATABASE_URL", "TANK_OPENMA_DATABASE_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TANK_DB_PASSWORD", "FAKE p@ss/word")
    monkeypatch.setenv("OMA_DB_PASSWORD", "FAKE-oma")
    settings = Settings(_env_file=None)
    assert settings.database_url == "postgresql://tank:FAKE%20p%40ss%2Fword@postgres:5432/tank"
    assert settings.openma_database_url == "postgresql://oma:FAKE-oma@postgres:5432/oma"
    monkeypatch.setenv("TANK_DATABASE_URL", "postgresql://explicit/tank_test_x")
    assert Settings(_env_file=None).database_url == "postgresql://explicit/tank_test_x"
    monkeypatch.delenv("TANK_DB_PASSWORD")
    assert Settings(_env_file=None).database_url == "postgresql://explicit/tank_test_x"


def test_frozen_config_is_parsed_once_and_callers_get_copies(tmp_path, monkeypatch):
    import yaml

    import tank.settings as module

    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_text("tokens:\n- token_id: T\n")
    frozen = resolve_versions(Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data"))
    calls = []
    original = yaml.load
    monkeypatch.setattr(module.yaml, "load", lambda *a, **k: calls.append(1) or original(*a, **k))
    first = frozen.load_config("registry")
    first["tokens"].append({"token_id": "MUTATED"})
    second = frozen.load_config("registry.yaml")
    assert second == {"tokens": [{"token_id": "T"}]} and len(calls) == 1
    (config / "registry.yaml").write_text("tokens: []\n")
    assert frozen.load_config("registry") == {"tokens": [{"token_id": "T"}]}
    unfrozen = Settings(_env_file=None, config_dir=config)
    assert unfrozen.load_config("registry") == {"tokens": []}
