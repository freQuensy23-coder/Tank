import base64
import hashlib
import json
from pathlib import Path

import pytest
import yaml
from pydantic import JsonValue

from tank.contracts import DomainModel
from tank.contracts.configuration import DeliveryConfiguration
from tank.contracts.json import JsonDocument
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.sources import SourceConfiguration
from tank.settings import Settings, resolve_versions
from tank.util import canonical_json


def test_runtime_generation_freezes_config_and_preserves_exact_recovery_inputs(tmp_path: Path) -> None:
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


def test_invalid_candidate_cannot_replace_valid_snapshot(tmp_path: Path) -> None:
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


def test_secrets_come_from_the_environment_names_of_the_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "TELEGRAM_BOT_TOKEN": "FAKE-telegram",
        "CODEX_GATEWAY_KEY": "FAKE-internal-key",
        "OMA_API_KEY": "FAKE-oma-key",
        "ETHERSCAN_API_KEY": "FAKE-etherscan",
        "APIFY_API_TOKEN": "FAKE-apify-token",
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
    assert settings.apify_api_token == "FAKE-apify-token"
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


def test_config_snapshots_refuse_auth_files_inside_config(tmp_path: Path) -> None:
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


def test_deployment_values_come_from_frozen_config_with_explicit_override(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    delivery = DeliveryConfiguration(
        alerts_chat_id="-1000000000001",
        ops_chat_id="-1000000000002",
        parse_mode="HTML",
        max_attempts=5,
        secrets="TELEGRAM_BOT_TOKEN",
    )
    sources = SourceConfiguration(version="fixture", contact_email="test@example.invalid", sources=[])
    (config / "delivery.yaml").write_text(yaml.safe_dump(delivery.model_dump(mode="json")))
    (config / "sources.yaml").write_text(yaml.safe_dump(sources.model_dump(mode="json")))
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


def test_opencode_alias_and_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_codex_auth_default_only_resolves_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_readiness_checks_opencode_key(tmp_path: Path) -> None:
    from tank.cli import readiness_checks

    settings = Settings(_env_file=None, opencode_api_key="FAKE-ready-key")
    checks = {row.check: row for row in readiness_checks(settings)}
    assert checks["opencode_api_key"].ok
    assert "jev_provider" not in checks
    assert "typesafe_api_key" not in checks
    assert "FAKE-ready-key" not in str(checks)


@pytest.mark.parametrize("name", ["TANK_USER_AGENT", "TANK_TELEGRAM_ALERTS_CHAT", "TANK_TELEGRAM_OPS_CHAT"])
def test_configuration_values_never_come_from_environment(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, name: str
) -> None:
    monkeypatch.setenv(name, "FAKE-environment-value")
    settings = Settings(_env_file=None)
    assert getattr(settings, name.removeprefix("TANK_").lower()) != "FAKE-environment-value"
    assert name in caplog.text and "FAKE-environment-value" not in caplog.text


def test_user_agent_and_chats_from_configuration(tmp_path: Path) -> None:
    from tank.cli import readiness_checks

    config = tmp_path / "config"
    config.mkdir()
    delivery = DeliveryConfiguration(
        alerts_chat_id="-1001",
        ops_chat_id="-1002",
        parse_mode="HTML",
        max_attempts=5,
        secrets="TELEGRAM_BOT_TOKEN",
    )
    baseline = Settings(config_dir=Path("config"), _env_file=None).load_config_as(
        "sources", SourceConfiguration
    )
    sources = SourceConfiguration(
        version="fixture",
        contact_email="ops@example.org",
        sources=[source for source in baseline.sources if source.source_id == "S29"],
    )
    (config / "delivery.yaml").write_text(yaml.safe_dump(delivery.model_dump(mode="json")))
    (config / "sources.yaml").write_text(yaml.safe_dump(sources.model_dump(mode="json")))
    frozen = resolve_versions(
        Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data", apify_api_token="FAKE-apify")
    )
    assert frozen.user_agent == "TankV0/1.0 (ops@example.org)"
    assert (frozen.telegram_alerts_chat, frozen.telegram_ops_chat) == ("-1001", "-1002")
    checks = {row.check: row for row in readiness_checks(frozen)}
    assert checks["user_agent_contact"].ok and checks["apify_api_token"].ok
    assert "x_list_id" not in checks
    legacy = frozen.model_copy(update={"user_agent": "TankV0/1.0 contact=ops@example.org"})
    assert not {row.check: row for row in readiness_checks(legacy)}["user_agent_contact"].ok


def test_database_urls_from_the_role_passwords(monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_frozen_config_is_parsed_once_and_callers_get_copies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import yaml

    import tank.settings as module

    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_text("tokens:\n- token_id: T\n")
    frozen = resolve_versions(Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data"))
    calls: list[int] = []

    class CountingLoader(yaml.SafeLoader):
        def __init__(self, stream: bytes | str) -> None:
            calls.append(1)
            super().__init__(stream)

    monkeypatch.setattr(module, "_YAML_LOADER", CountingLoader)
    first = frozen.load_config("registry")
    tokens = first["tokens"]
    assert isinstance(tokens, list)
    tokens.append({"token_id": "MUTATED"})
    second = frozen.load_config("registry.yaml")
    assert second == {"tokens": [{"token_id": "T"}]} and len(calls) == 1
    (config / "registry.yaml").write_text("tokens: []\n")
    assert frozen.load_config("registry") == {"tokens": [{"token_id": "T"}]}
    unfrozen = Settings(_env_file=None, config_dir=config)
    assert unfrozen.load_config("registry") == {"tokens": []}


def test_explicit_empty_codex_home_is_not_replaced_with_a_personal_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CODEX_HOME", "")
    monkeypatch.delenv("TANK_CODEX_AUTH_PATH", raising=False)
    with pytest.raises(ValueError, match="nonempty directory"):
        Settings(_env_file=None)


def test_declared_empty_role_password_keeps_its_declared_host_instead_of_local_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TANK_DATABASE_URL", raising=False)
    monkeypatch.delenv("TANK_OPENMA_DATABASE_URL", raising=False)
    settings = Settings(_env_file=None, tank_db_password="", oma_db_password="")
    assert settings.database_url == "postgresql://tank:@postgres:5432/tank"
    assert settings.openma_database_url == "postgresql://oma:@postgres:5432/oma"
    explicit = Settings(_env_file=None, tank_db_password="", database_url="postgresql:///explicit")
    assert explicit.database_url == "postgresql:///explicit"


def test_version_identity_requires_capture_and_detects_post_capture_relabelling(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "rules.yaml").write_text("value: 1\n")
    intake = Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    with pytest.raises(ValueError, match="requires resolve_versions"):
        intake.require_versions()
    declared_intake = Settings(
        config_dir=config, config_version="declared-release", code_version="0.1.0", _env_file=None
    )
    with pytest.raises(ValueError, match="requires resolve_versions"):
        declared_intake.require_versions()
    resolved = resolve_versions(intake)
    identity = resolved.require_versions()
    assert identity.config_version == resolved.config_version
    assert identity.code_version == resolved.code_version
    (config / "rules.yaml").write_text("value: 2\n")
    assert resolved.require_versions() == identity
    with pytest.raises(ValueError, match="no longer matches"):
        resolved.model_copy(update={"config_version": "different-label"}).require_versions()
    assert (
        ResolvedVersionIdentity(config_version="declared-release", code_version="0.1.0").code_version
        == "0.1.0"
    )


@pytest.mark.parametrize("config_version", ["", " ", "unversioned", "UNKNOWN"])
def test_version_identity_refuses_empty_or_unknown_provenance(config_version: str) -> None:
    with pytest.raises(ValueError):
        ResolvedVersionIdentity(config_version=config_version, code_version="declared-code")


def test_captured_config_inventory_requires_resolution_and_stays_frozen(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "z.yaml").write_text("value: 1\n")
    (config / "a.yaml").write_text("value: 2\n")
    intake = Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    with pytest.raises(ValueError, match="requires resolve_versions"):
        intake.captured_config_paths()
    resolved = resolve_versions(intake)
    assert resolved.captured_config_paths() == ("a.yaml", "z.yaml")
    (config / "later.yaml").write_text("value: 3\n")
    assert resolved.captured_config_paths() == ("a.yaml", "z.yaml")
    with pytest.raises(ValueError, match="no longer matches"):
        resolved.model_copy(update={"code_version": "different"}).captured_config_paths()


def test_openma_accessor_reads_the_captured_configuration(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    original = Settings(_env_file=None).read_config_bytes("openma.yaml")
    (config / "openma.yaml").write_bytes(original)
    settings = resolve_versions(Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None))
    configuration = settings.openma()
    assert configuration.model_id in configuration.model_prices
    assert configuration.max_running_sessions > 0
    (config / "openma.yaml").write_text("invalid: replacement\n")
    assert configuration == settings.openma()


@pytest.mark.parametrize("number", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_json_candidate_cannot_replace_captured_configuration(tmp_path: Path, number: str) -> None:
    config = tmp_path / "config"
    config.mkdir()
    payload = config / "schema.json"
    payload.write_text('{"limit":1}')
    settings = Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    captured = resolve_versions(settings)
    payload.write_text('{"limit":' + number + "}")
    with pytest.raises(ValueError, match="finite"):
        resolve_versions(settings)
    assert captured.read_config_json("schema.json") == {"limit": 1}
    assert len(list((settings.data_dir / "config_versions").glob("*.json"))) == 1


class CapturedRuleFixture(DomainModel):
    values: list[str]


def test_typed_captured_config_reuses_parsing_and_keeps_model_data_independent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "config"
    config.mkdir()
    file = config / "rules.yaml"
    file.write_text("values: [first, second]\n")
    intake = Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    captured = resolve_versions(intake)
    original_load = yaml.load
    parses = 0

    def counted_load(stream: str | bytes, Loader: type[yaml.SafeLoader]) -> JsonValue:
        nonlocal parses
        parses += 1
        return JsonDocument.validate_value(original_load(stream, Loader=Loader))

    monkeypatch.setattr(yaml, "load", counted_load)
    first = captured.load_config_as("rules", CapturedRuleFixture)
    first.values.append("caller-only")
    assert captured.load_config_as("rules", CapturedRuleFixture).values == ["first", "second"]
    assert parses == 1
    file.write_text("values: [changed]\n")
    assert captured.load_config_as("rules", CapturedRuleFixture).values == ["first", "second"]
    assert intake.load_config_as("rules", CapturedRuleFixture).values == ["changed"]
    file.write_text("values: [later]\n")
    assert intake.load_config_as("rules", CapturedRuleFixture).values == ["later"]
    updated = resolve_versions(intake)
    assert updated.load_config_as("rules", CapturedRuleFixture).values == ["later"]
    assert updated.require_versions() != captured.require_versions()
    file.write_text("values: [\n")
    with pytest.raises(yaml.YAMLError):
        resolve_versions(intake)
    assert captured.load_config_as("rules", CapturedRuleFixture).values == ["first", "second"]


def test_cached_typed_actual_registry_preserves_canonical_config_and_nested_isolation(
    tmp_path: Path,
) -> None:
    captured = resolve_versions(Settings(data_dir=tmp_path / "data", _env_file=None))
    expected = hashlib.sha256(canonical_json(Settings(_env_file=None).registry()).encode()).hexdigest()
    registry = captured.registry()
    assert hashlib.sha256(canonical_json(registry).encode()).hexdigest() == expected
    count = len(registry.tokens)
    assert count > 0
    registry.tokens.clear()
    assert len(captured.registry().tokens) == count
    assert hashlib.sha256(canonical_json(captured.registry()).encode()).hexdigest() == expected
