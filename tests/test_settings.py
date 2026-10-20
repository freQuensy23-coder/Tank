import base64
import hashlib
import json
import subprocess

import pytest

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


def test_clean_deployment_uses_git_commit_and_dirty_edit_is_identified(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "thresholds.yaml").write_text("filter_cutoff: 0.71\n")
    subprocess.run(["git", "init", "-q", str(config)], check=True)
    subprocess.run(["git", "-C", str(config), "add", "thresholds.yaml"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(config),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "Config",
        ],
        check=True,
    )
    commit = subprocess.check_output(["git", "-C", str(config), "rev-parse", "HEAD"], text=True).strip()
    settings = Settings(config_dir=config, data_dir=tmp_path / "data")
    assert resolve_versions(settings).config_version == commit
    (config / "thresholds.yaml").write_text("filter_cutoff: 0.81\n")
    assert resolve_versions(settings).config_version.startswith(commit + "-dirty.")


def test_invalid_candidate_cannot_replace_valid_snapshot(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "thresholds.yaml").write_text("filter_cutoff: 0.71\n")
    settings = Settings(config_dir=config, data_dir=tmp_path / "data")
    old = resolve_versions(settings)
    (config / "thresholds.yaml").write_text("filter_cutoff: [\n")
    with pytest.raises(Exception):
        resolve_versions(settings)
    assert old.load_config("thresholds")["filter_cutoff"] == 0.71
    assert len(list((settings.data_dir / "config_versions").glob("*.json"))) == 1


def test_docker_file_secrets_and_explicit_precedence(tmp_path, monkeypatch):
    for field in ("TANK_TELEGRAM_TOKEN", "TANK_CODEX_GATEWAY_KEY", "TANK_OPENMA_API_KEY"):
        monkeypatch.delenv(field, raising=False)
    (tmp_path / "telegram_bot_token").write_text("FAKE-telegram-file-secret\n")
    (tmp_path / "codex_gateway_key").write_text("FAKE-internal-key\n")
    (tmp_path / "oma_api_key").write_text("FAKE-oma-key\n")
    (tmp_path / "x_session").write_text("synthetic storage state, not loaded by Settings")
    settings = Settings(secrets_dir=tmp_path, _env_file=None)
    assert settings.telegram_token == "FAKE-telegram-file-secret"
    assert settings.codex_gateway_key == "FAKE-internal-key"
    assert settings.openma_api_key == "FAKE-oma-key"
    assert settings.x_storage_state_path == tmp_path / "x_session"
    assert not hasattr(settings, "daily_budget_usd")
    monkeypatch.setenv("TANK_TELEGRAM_TOKEN", "FAKE-environment-secret")
    assert Settings(secrets_dir=tmp_path, _env_file=None).telegram_token == "FAKE-environment-secret"
    assert (
        Settings(secrets_dir=tmp_path, telegram_token="FAKE-explicit-secret", _env_file=None).telegram_token
        == "FAKE-explicit-secret"
    )


def test_config_mount_manifest_cannot_hide_modified_contents(tmp_path):
    from tank.settings import _CONFIG_SUFFIXES, _capture_tree, _digest

    config = tmp_path / "config"
    config.mkdir()
    (config / "thresholds.yaml").write_text("filter_cutoff: 0.71\n")
    commit = "a" * 40
    manifest = {
        "git_commit": commit,
        "git_dirty": False,
        "content_sha256": _digest(_capture_tree(config, _CONFIG_SUFFIXES)),
    }
    (config / ".tank-config-version.json").write_text(json.dumps(manifest))
    settings = Settings(config_dir=config, data_dir=tmp_path / "data")
    assert resolve_versions(settings).config_version == commit
    (config / "thresholds.yaml").write_text("filter_cutoff: 0.81\n")
    modified = resolve_versions(settings)
    assert modified.config_version.startswith(commit + "-dirty.")
    assert modified.load_config("thresholds")["filter_cutoff"] == 0.81


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


def test_openrouter_alias_secret_and_explicit_provider(tmp_path, monkeypatch):
    monkeypatch.delenv("TANK_OPENROUTER_API_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    (tmp_path / "openrouter_api_key").write_text("FAKE-file-router\n")
    assert Settings(_env_file=None, secrets_dir=tmp_path).openrouter_api_key == "FAKE-file-router"
    monkeypatch.setenv("OPENROUTER_API_KEY", "FAKE-raw-router")
    assert Settings(_env_file=None, secrets_dir=tmp_path).openrouter_api_key == "FAKE-raw-router"
    monkeypatch.setenv("TANK_OPENROUTER_API_KEY", "FAKE-prefixed-router")
    settings = Settings(_env_file=None, secrets_dir=tmp_path)
    assert settings.openrouter_api_key == "FAKE-prefixed-router"
    assert "FAKE-prefixed-router" not in repr(settings)
    assert Settings(_env_file=None, openrouter_api_key="FAKE-explicit").openrouter_api_key == "FAKE-explicit"
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


def test_readiness_requires_only_openrouter_key(tmp_path):
    from tank.cli import readiness_checks

    settings = Settings(_env_file=None, secrets_dir=tmp_path, openrouter_api_key="FAKE-ready-key")
    checks = {row["check"]: row for row in readiness_checks(settings)}
    assert checks["openrouter_api_key"]["ok"]
    assert "jev_provider" not in checks
    assert "typesafe_api_key" not in checks
    assert "FAKE-ready-key" not in str(checks)


def test_x_disabled_is_explicit_readiness_scope_not_missing_credentials(tmp_path):
    from tank.cli import readiness_checks

    settings = Settings(_env_file=None, x_enabled=False, x_profile_dir=tmp_path / "absent")
    checks = {row["check"]: row for row in readiness_checks(settings)}
    assert checks["x_disabled_by_operator"] == {
        "check": "x_disabled_by_operator",
        "ok": True,
        "enabled": False,
    }
    assert "s29_list_id" not in checks and "x_profile" not in checks
    assert settings.x_profile_dir.exists() is False


@pytest.mark.parametrize(
    "name", ["TANK_USER_AGENT", "TANK_TELEGRAM_ALERTS_CHAT", "TANK_TELEGRAM_OPS_CHAT"]
)
def test_configuration_values_never_come_from_environment(monkeypatch, name):
    monkeypatch.setenv(name, "FAKE-environment-value")
    with pytest.raises(ValueError, match=name):
        Settings(_env_file=None)
    monkeypatch.setenv(name, "")
    assert getattr(Settings(_env_file=None), name.removeprefix("TANK_").lower()) != ""


def test_user_agent_and_chats_from_configuration(tmp_path):
    from tank.cli import readiness_checks

    config = tmp_path / "config"
    config.mkdir()
    (config / "delivery.yaml").write_text("alerts_chat_id: '-1001'\nops_chat_id: '-1002'\n")
    (config / "sources.yaml").write_text(
        "contact_email: ops@example.org\nsources:\n- source_id: S29\n  list_id: '12345'\n"
    )
    frozen = resolve_versions(Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data"))
    assert frozen.user_agent == "TankV0/1.0 (ops@example.org)"
    assert (frozen.telegram_alerts_chat, frozen.telegram_ops_chat) == ("-1001", "-1002")
    checks = {row["check"]: row for row in readiness_checks(frozen)}
    assert checks["user_agent_contact"]["ok"] and checks["s29_list_id"]["ok"]
    assert "x_list_id" not in checks
    legacy = frozen.model_copy(update={"user_agent": "TankV0/1.0 contact=ops@example.org"})
    assert not {row["check"]: row for row in readiness_checks(legacy)}["user_agent_contact"]["ok"]
