"""Declared synthetic settings used by native-agent component fixtures."""

from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

from pydantic import JsonValue

from tank.contracts.configuration import Thresholds

type FixtureConfigurations = dict[str, dict[str, JsonValue]]


@dataclass
class AgentFixtureSettings:
    documents: FixtureConfigurations
    tool_reader: Callable[[str], JsonValue]
    openrouter_api_key: str | None
    opencode_api_key: str | None
    replay: bool
    ops_progress_enabled: bool
    openma_api_key: str
    openma_url: str
    openma_model: str
    config_version: str
    code_version: str
    telegram_token: str | None
    codex_gateway_key: str | None
    database_url: str | None

    def load_config(self, name: str) -> dict[str, JsonValue]:
        return self.documents[name]

    def read_config_json(self, name: str) -> JsonValue:
        return self.tool_reader(name)

    def thresholds(self) -> Thresholds:
        return Thresholds.model_validate(self.documents["thresholds"])

    def configured_secrets(self) -> frozenset[str]:
        values = {
            value
            for value in (
                self.openrouter_api_key,
                self.opencode_api_key,
                self.openma_api_key,
                self.telegram_token,
                self.codex_gateway_key,
            )
            if value is not None
        }
        if self.database_url is not None:
            password = urlsplit(self.database_url).password
            if password is not None:
                values.update((password, unquote(password)))
        return frozenset(values)
