import base64
import copy
import hashlib
import io
import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from types import MappingProxyType
from typing import Any
from urllib.parse import quote

import yaml
from pydantic import AliasChoices, Field, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROCESS_CODE_VERSION: str | None = None
_CONFIG_SUFFIXES = {".yaml", ".yml", ".json", ".md"}
_YAML_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
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


def _git_config_identity(root: Path, content_digest: str) -> tuple[str, bool]:
    """A clean Git label requires exact captured payload bytes at pinned HEAD.

    Status alone misses ignored/untracked roots, ignored extra files and tracked
    files marked assume-unchanged/skip-worktree. Read objects directly, without
    checkout filters, and compare the same versioned-file policy as runtime.
    """
    root = root.resolve()

    def git(*arguments, data=None, directory=None):
        return subprocess.run(
            ["git", "--literal-pathspecs", "-C", str(directory or root), *arguments],
            input=data,
            check=True,
            capture_output=True,
            timeout=5,
        ).stdout

    commit = git("rev-parse", "HEAD").decode().strip()
    repository = Path(git("rev-parse", "--show-toplevel").decode().strip()).resolve()
    prefix = root.relative_to(repository)
    tree = git("ls-tree", "-r", "-z", "--full-tree", commit, "--", prefix.as_posix(), directory=repository)
    objects = []
    for row in tree.split(b"\0"):
        if not row:
            continue
        metadata, name = row.split(b"\t", 1)
        mode, kind, oid = metadata.split()
        relative = Path(os.fsdecode(name)).relative_to(prefix)
        if relative.suffix not in _CONFIG_SUFFIXES or any(
            part.startswith(".") or part == "__pycache__" for part in relative.parts
        ):
            continue
        if kind != b"blob" or mode == b"120000":
            return commit, True
        objects.append((relative.as_posix(), oid))
    head_payload = {}
    if objects:
        output = io.BytesIO(git("cat-file", "--batch", data=b"".join(oid + b"\n" for _, oid in objects)))
        for name, expected_oid in objects:
            header = output.readline().strip().split()
            if len(header) != 3 or header[0] != expected_oid or header[1] != b"blob":
                return commit, True
            size = int(header[2])
            content = output.read(size)
            if len(content) != size or output.read(1) != b"\n":
                return commit, True
            head_payload[name] = content
    dirty_status = bool(git("status", "--porcelain", "--", ".").strip())
    return commit, dirty_status or _digest(head_payload) != content_digest


def _config_version(root: Path, digest: str) -> str:
    """Use the deployment commit; identify uncommitted development precisely."""
    manifest_path = root / ".tank-config-version.json"
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text())
        commit = manifest["git_commit"]
        if (
            not isinstance(commit, str)
            or len(commit) != 40
            or any(c not in "0123456789abcdef" for c in commit)
        ):
            raise ValueError("Configuration manifest requires a full Git commit")
        clean = manifest.get("content_sha256") == digest and manifest.get("git_dirty") is False
        return commit if clean else f"{commit}-dirty.{digest}"
    try:
        commit, dirty = _git_config_identity(root, digest)
        return f"{commit}-dirty.{digest}" if dirty else commit
    except (OSError, subprocess.SubprocessError):
        return f"uncommitted.{digest}"


def _default_codex_auth_path() -> Path:
    root = os.environ.get("CODEX_HOME")
    return Path(root).expanduser() / "auth.json" if root else Path.home() / ".codex" / "auth.json"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TANK_", env_file=".env", extra="ignore")
    data_dir: Path = Path("data")
    config_dir: Path = Path("config")
    replay_private_mask_path: Path | None = Field(default=None, repr=False, exclude=True)
    database_url: str = "postgresql:///tank"
    openma_url: str = "http://127.0.0.1:8787"
    openma_database_url: str = "postgresql:///oma"
    openma_api_key: str | None = None
    openma_model: str = "gpt-6.1-sol"
    secrets_dir: Path = Path("/run/secrets")
    codex_gateway_host: str = "0.0.0.0"
    codex_gateway_port: int = 8790
    codex_gateway_key: str | None = None
    codex_auth_path: Path = Field(default_factory=_default_codex_auth_path)
    opencode_api_key: str | None = Field(
        default=None,
        repr=False,
        validation_alias=AliasChoices("TANK_OPENCODE_API_KEY", "OPENCODE_API_KEY", "opencode_api_key"),
    )
    telegram_token: str | None = None
    telegram_alerts_chat: str | None = None
    telegram_ops_chat: str | None = None
    etherscan_api_key: str | None = None
    user_agent: str = "TankV0/1.0 (operator contact not configured)"
    x_enabled: bool = True
    x_profile_dir: Path = Path("data/chrome-profile")
    x_storage_state_path: Path = Path("/run/secrets/x_session")
    x_headless: bool = True
    config_version: str = "unversioned"
    code_version: str = "0.1.0"
    workers: int = 8
    replay: bool = False
    capture_only: bool = False
    backup_dir: Path | None = None
    logs_dir: Path | None = None
    backup_hour_utc: int = 2
    market_retention_days: int = 90
    shutdown_grace_s: int = 30
    _config_snapshot: Any = PrivateAttr(default=None)
    _parsed_config: Any = PrivateAttr(default=None)

    @classmethod
    def settings_customise_sources(
        cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings
    ):
        def docker_secrets():
            root = Path(
                init_settings().get("secrets_dir")
                or env_settings().get("secrets_dir")
                or dotenv_settings().get("secrets_dir")
                or "/run/secrets"
            )
            aliases = {
                "openma_api_key": "oma_api_key",
                "opencode_api_key": "opencode_api_key",
                "telegram_token": "telegram_bot_token",
                "etherscan_api_key": "etherscan_api_key",
                "codex_gateway_key": "codex_gateway_key",
            }
            values = {}
            for field, name in aliases.items():
                path = root / name
                if path.is_file():
                    value = path.read_text().strip()
                    if not value:
                        raise ValueError(f"Docker secret {name} is empty")
                    values[field] = value
            if (root / "x_session").is_file():
                values["x_storage_state_path"] = root / "x_session"
            for field, name, role in (
                ("database_url", "tank_db_password", "tank"),
                ("openma_database_url", "oma_db_password", "oma"),
            ):
                path = root / name
                if path.is_file():
                    password = path.read_text().strip()
                    if not password:
                        raise ValueError(f"Docker secret {name} is empty")
                    values[field] = f"postgresql://{role}:{quote(password, safe='')}@postgres:5432/{role}"
            return values

        def without_config_owned(source):
            def read():
                values = source()
                present = sorted(name for name in CONFIG_OWNED_FIELDS if values.get(name))
                if present:
                    logging.getLogger("tank.settings").warning(
                        "Ignoring %s from the environment: set sources.yaml contact_email and "
                        "delivery.yaml chat IDs instead",
                        ", ".join("TANK_" + name.upper() for name in present),
                    )
                return {k: v for k, v in values.items() if k not in CONFIG_OWNED_FIELDS}

            return read

        return (
            init_settings,
            without_config_owned(env_settings),
            without_config_owned(dotenv_settings),
            docker_secrets,
            file_secret_settings,
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

    def read_config_json(self, relative: str) -> Any:
        return json.loads(self.read_config_bytes(relative))

    def load_config(self, name: str) -> dict[str, Any]:
        relative = name if name.endswith((".yaml", ".yml")) else name + ".yaml"
        cache = self._parsed_config if self._config_snapshot is not None else None
        if cache is not None and relative in cache:
            return copy.deepcopy(cache[relative])
        value = yaml.load(self.read_config_bytes(relative), Loader=_YAML_LOADER)
        if not isinstance(value, dict):
            raise ValueError(f"Configuration must be a mapping: {relative}")
        if cache is not None:
            cache[relative] = value
            return copy.deepcopy(value)
        return value


def configured_values(settings: Settings) -> dict[str, str]:
    """User-Agent and Telegram chats from the configuration (TDD §13, HLD §3)."""
    values = {}
    if settings.has_config("delivery.yaml"):
        delivery = settings.load_config("delivery")
        for field, key in (("telegram_alerts_chat", "alerts_chat_id"), ("telegram_ops_chat", "ops_chat_id")):
            if delivery.get(key) is not None:
                values[field] = str(delivery[key])
    if settings.has_config("sources.yaml"):
        contact = settings.load_config("sources").get("contact_email")
        if contact:
            values["user_agent"] = f"TankV0/1.0 ({contact})"
    return values


def resolve_versions(settings: Settings) -> Settings:
    """Fingerprint the exact configuration and code loaded by this process.

    Hash paths and contents in stable order. Credentials belong in Docker secrets
    or the environment, never in the versioned configuration. The environment is never
    included in these hashes.
    """

    global _PROCESS_CODE_VERSION
    config_root = settings.config_dir.resolve()
    for sensitive in (
        settings.secrets_dir,
        settings.codex_auth_path,
        settings.x_storage_state_path,
        settings.x_profile_dir,
        settings.data_dir,
    ):
        if Path(sensitive).resolve().is_relative_to(config_root):
            raise ValueError(
                "Credentials, browser state and runtime data must be outside the versioned configuration directory"
            )
    captured = _capture_tree(settings.config_dir, _CONFIG_SUFFIXES)
    digest = _digest(captured)
    version = _config_version(settings.config_dir, digest)
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
