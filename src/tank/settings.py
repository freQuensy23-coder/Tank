import base64
import copy
import hashlib
import json
import logging
import os
import re
import tempfile
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from urllib.parse import quote, unquote, urlsplit

import yaml
from pydantic import AliasChoices, BaseModel, Field, JsonValue, PrivateAttr
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from tank.contracts.configuration import DeliveryConfiguration, Thresholds
from tank.contracts.json import JsonDocument, decode_json
from tank.contracts.registry import (
    DependencyConfiguration,
    ExchangeAddressesConfiguration,
    RegistryConfiguration,
    SemanticsConfiguration,
)
from tank.contracts.sources import SourceConfiguration
from tank.util import JsonInput

_PROCESS_CODE_VERSION: str | None = None
_CONFIG_SUFFIXES = {".yaml", ".yml", ".json", ".md"}
_YAML_LOADER = yaml.SafeLoader


CONFIG_OWNED_FIELDS = ("user_agent", "telegram_alerts_chat", "telegram_ops_chat")


def _capture_tree(root: Path, suffixes: set[str]) -> dict[str, bytes]:
    captured = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if any(part.startswith(".") or part == "__pycache__" for part in relative.parts):
            continue
        if path.is_file() and path.suffix in suffixes:
            if path.is_symlink():
                raise ValueError(f"Versioned input must not be a symlink: {path}")
            captured[relative.as_posix()] = path.read_bytes()
    if not captured:
        raise ValueError(f"No versioned inputs in {root}")
    return captured


def _digest(captured: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    for path, content in sorted(captured.items()):
        digest.update(path.encode() + b"\0" + content + b"\0")
    return digest.hexdigest()


def _config_version(build_sha: str | None, digest: str) -> str:
    """The commit the image was built from; a development checkout is identified by content."""
    if build_sha is None or build_sha == "dev":
        return f"dev.{digest}"
    if re.fullmatch(r"[0-9a-f]{40}", build_sha) is None:
        raise ValueError("Build identity requires dev or 40 lowercase hexadecimal characters")
    return build_sha


def _default_codex_auth_path() -> Path:
    root = os.environ.get("CODEX_HOME")
    if root is None:
        return Path.home() / ".codex" / "auth.json"
    if root == "":
        raise ValueError("CODEX_HOME must name a nonempty directory")
    return Path(root).expanduser() / "auth.json"


class ConfigOwnedSource(PydanticBaseSettingsSource):
    """Exclude configuration-owned fields before Pydantic merges environment sources."""

    def __init__(self, source: PydanticBaseSettingsSource) -> None:
        super().__init__(source.settings_cls)
        self.source = source

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[JsonInput, str, bool]:
        return self.source.get_field_value(field, field_name)

    def __call__(self) -> dict[str, JsonInput]:
        values = self.source()
        present = sorted(name for name in CONFIG_OWNED_FIELDS if name in values)
        if present:
            logging.getLogger("tank.settings").warning(
                "Ignoring %s from the environment: set sources.yaml contact_email and "
                "delivery.yaml chat IDs instead",
                ", ".join("TANK_" + name.upper() for name in present),
            )
        return {key: value for key, value in values.items() if key not in CONFIG_OWNED_FIELDS}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TANK_", env_file=".env", extra="ignore", hide_input_in_errors=True
    )
    data_dir: Path = Path("data")
    config_dir: Path = Path("config")
    replay_private_mask_path: Path | None = Field(default=None, repr=False, exclude=True)
    database_url: str = Field(default="postgresql:///tank", repr=False)

    clickhouse_url: str | None = None
    openma_url: str = "http://127.0.0.1:8787"
    openma_database_url: str = Field(default="postgresql:///oma", repr=False)
    openma_api_key: str | None = Field(
        default=None,
        repr=False,
        validation_alias=AliasChoices("TANK_OPENMA_API_KEY", "OMA_API_KEY", "openma_api_key"),
    )
    openma_model: str = "gpt-6.1-sol"
    tank_db_password: str | None = Field(
        default=None,
        repr=False,
        exclude=True,
        validation_alias=AliasChoices("TANK_DB_PASSWORD", "tank_db_password"),
    )
    oma_db_password: str | None = Field(
        default=None,
        repr=False,
        exclude=True,
        validation_alias=AliasChoices("OMA_DB_PASSWORD", "oma_db_password"),
    )
    codex_gateway_host: str = "0.0.0.0"
    codex_gateway_port: int = 8790
    codex_gateway_key: str | None = Field(
        default=None,
        repr=False,
        validation_alias=AliasChoices("TANK_CODEX_GATEWAY_KEY", "CODEX_GATEWAY_KEY", "codex_gateway_key"),
    )
    codex_auth_path: Path = Field(default_factory=_default_codex_auth_path)

    opencode_api_key: str | None = Field(
        default=None,
        repr=False,
        validation_alias=AliasChoices("TANK_OPENCODE_API_KEY", "OPENCODE_API_KEY", "opencode_api_key"),
    )
    telegram_token: str | None = Field(
        default=None,
        repr=False,
        validation_alias=AliasChoices("TANK_TELEGRAM_TOKEN", "TELEGRAM_BOT_TOKEN", "telegram_token"),
    )
    telegram_alerts_chat: str | None = None
    telegram_ops_chat: str | None = None
    ops_progress_enabled: bool = True
    ops_progress_delay_s: float = Field(default=15, ge=0, le=60)
    etherscan_api_key: str | None = Field(
        default=None,
        repr=False,
        validation_alias=AliasChoices("TANK_ETHERSCAN_API_KEY", "ETHERSCAN_API_KEY", "etherscan_api_key"),
    )
    user_agent: str = "TankV0/1.0 (operator contact not configured)"
    apify_api_token: str | None = Field(
        default=None,
        repr=False,
        exclude=True,
        validation_alias=AliasChoices("TANK_APIFY_API_TOKEN", "APIFY_API_TOKEN", "apify_api_token"),
    )
    config_version: str = "unversioned"
    build_sha: str | None = Field(default=None, pattern=r"^(?:dev|[0-9a-f]{40})$")
    code_version: str = "0.1.0"
    workers: int = 8
    replay: bool = False
    capture_only: bool = False
    backup_dir: Path | None = None
    logs_dir: Path | None = None
    backup_hour_utc: int = 2
    market_retention_days: int = 90
    shutdown_grace_s: int = 10
    _config_snapshot: Mapping[str, bytes] | None = PrivateAttr(default=None)

    _parsed_config: dict[str, dict[str, JsonValue]] | None = PrivateAttr(default=None)

    def model_post_init(self, context: JsonValue) -> None:
        if self.tank_db_password and "database_url" not in self.model_fields_set:
            self.database_url = (
                f"postgresql://tank:{quote(self.tank_db_password, safe='')}@postgres:5432/tank"
            )
        if self.oma_db_password and "openma_database_url" not in self.model_fields_set:
            self.openma_database_url = (
                f"postgresql://oma:{quote(self.oma_db_password, safe='')}@postgres:5432/oma"
            )

    def configured_secrets(self) -> frozenset[str]:
        """Return configured credentials without reading any credential files."""
        secrets = {
            value
            for value in (
                self.openma_api_key,
                self.opencode_api_key,
                self.telegram_token,
                self.etherscan_api_key,
                self.codex_gateway_key,
                self.apify_api_token,
                self.tank_db_password,
                self.oma_db_password,
            )
            if value is not None and value != ""
        }
        for url in (self.database_url, self.openma_database_url, self.clickhouse_url):
            if url is not None:
                password = urlsplit(url).password
                if password is not None and password != "":
                    secrets.update((password, unquote(password)))
        return frozenset(secrets)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            ConfigOwnedSource(env_settings),
            ConfigOwnedSource(dotenv_settings),
        )

    @staticmethod
    def _config_name(relative: str) -> str:
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("Configuration path must be relative to config_dir")
        return path.as_posix()

    def has_config(self, relative: str) -> bool:
        name = self._config_name(relative)
        if self._config_snapshot is not None:
            return name in self._config_snapshot
        return (self.config_dir / name).is_file()

    def read_config_bytes(self, relative: str) -> bytes:
        name = self._config_name(relative)
        if self._config_snapshot is not None:
            try:
                return self._config_snapshot[name]
            except KeyError:
                raise FileNotFoundError(f"Configuration snapshot has no {name}") from None
        return (self.config_dir / name).read_bytes()

    def read_config_text(self, relative: str) -> str:
        return self.read_config_bytes(relative).decode("utf-8")

    def read_config_json(self, relative: str) -> JsonValue:
        return decode_json(self.read_config_bytes(relative))

    def thresholds(self) -> Thresholds:
        return self.load_config_as("thresholds", Thresholds)

    def load_config_as[T: BaseModel](self, name: str, schema: type[T]) -> T:
        relative = name if name.endswith((".yaml", ".yml")) else name + ".yaml"
        payload = yaml.load(self.read_config_bytes(relative), Loader=_YAML_LOADER)
        return schema.model_validate(payload, strict=True)

    def registry(self) -> RegistryConfiguration:
        return self.load_config_as("registry", RegistryConfiguration)

    def semantics(self) -> SemanticsConfiguration:
        return self.load_config_as("semantics", SemanticsConfiguration)

    def dependencies(self) -> DependencyConfiguration:
        return self.load_config_as("dependency_map", DependencyConfiguration)

    def exchange_addresses(self) -> ExchangeAddressesConfiguration:
        return self.load_config_as("exchange_addresses", ExchangeAddressesConfiguration)

    def load_config(self, name: str) -> dict[str, JsonValue]:
        relative = name if name.endswith((".yaml", ".yml")) else name + ".yaml"
        cache = self._parsed_config if self._config_snapshot is not None else None
        if cache is not None and relative in cache:
            return copy.deepcopy(cache[relative])
        value = JsonDocument.validate_value(yaml.load(self.read_config_bytes(relative), Loader=_YAML_LOADER))
        if not isinstance(value, dict):
            raise ValueError(f"Configuration must be a mapping: {relative}")
        if cache is not None:
            cache[relative] = value
            return copy.deepcopy(value)
        return value


def configured_values(settings: Settings) -> dict[str, str]:
    """Read declared contact/chat values from validated versioned configuration."""
    values: dict[str, str] = {}
    if settings.has_config("delivery.yaml"):
        delivery = settings.load_config_as("delivery", DeliveryConfiguration)
        values["telegram_alerts_chat"] = delivery.alerts_chat_id
        values["telegram_ops_chat"] = delivery.ops_chat_id
    if settings.has_config("sources.yaml"):
        sources = settings.load_config_as("sources", SourceConfiguration)
        values["user_agent"] = f"TankV0/1.0 ({sources.contact_email})"
    return values


def resolve_versions(settings: Settings) -> Settings:
    """Fingerprint the exact configuration and code loaded by this process.

    Hash paths and contents in stable order. Credentials belong in the environment,
    never in the versioned configuration. The environment is never included in these hashes.
    """

    global _PROCESS_CODE_VERSION
    config_root = settings.config_dir.resolve()
    for sensitive in (
        settings.codex_auth_path,
        settings.data_dir,
    ):
        if Path(sensitive).resolve().is_relative_to(config_root):
            raise ValueError(
                "Credentials and runtime data must be outside the versioned configuration directory"
            )
    captured = _capture_tree(settings.config_dir, _CONFIG_SUFFIXES)
    digest = _digest(captured)
    version = _config_version(settings.build_sha, digest)
    if captured != _capture_tree(settings.config_dir, _CONFIG_SUFFIXES):
        raise ValueError("Configuration changed during capture; retry after completing the edit")
    for name, content in captured.items():
        if name.endswith((".yaml", ".yml")):
            if not isinstance(yaml.safe_load(content), dict):
                raise ValueError(f"Configuration must be a mapping: {name}")
        elif name.endswith(".json"):
            json.loads(content)
    if _PROCESS_CODE_VERSION is None:
        _PROCESS_CODE_VERSION = _digest(_capture_tree(Path(__file__).parent, {".py", ".sql", ".json"}))
    resolved = settings.model_copy(update={"config_version": version, "code_version": _PROCESS_CODE_VERSION})
    resolved._config_snapshot = MappingProxyType(captured)
    resolved._parsed_config = {}

    deployment_values = {
        field: value
        for field, value in configured_values(resolved).items()
        if field not in settings.model_fields_set or getattr(settings, field) is None
    }
    if deployment_values:
        resolved = resolved.model_copy(update=deployment_values)

    directory = settings.data_dir / "config_versions"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{version}.json"
    payload = json.dumps(
        {
            "config_version": version,
            "content_sha256": digest,
            "files_base64": {
                name: base64.b64encode(content).decode("ascii") for name, content in captured.items()
            },
        },
        sort_keys=True,
    ).encode()
    if target.exists():
        if target.read_bytes() != payload:
            raise ValueError(f"Versioned configuration collision: {version}")
    else:
        with tempfile.NamedTemporaryFile(dir=directory, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    return resolved
