from decimal import Decimal
from pathlib import Path
from typing import Any
import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TANK_", env_file=".env", extra="ignore")
    data_dir: Path = Path("data")
    config_dir: Path = Path("config")
    database_url: str = "postgresql:///tank"
    openma_url: str = "http://127.0.0.1:8787"
    openma_api_key: str | None = None
    openma_model: str = "gpt-6.1-sol"
    tavily_api_key: str | None = None
    typesafe_api_key: str | None = None
    telegram_token: str | None = None
    telegram_alerts_chat: str | None = None
    telegram_ops_chat: str | None = None
    etherscan_api_key: str | None = None
    user_agent: str = "TankV0/0.1 (operator contact not configured)"
    x_profile_dir: Path = Path("data/chrome-profile")
    x_list_id: str | None = None
    daily_budget_usd: Decimal = Decimal("30")
    config_version: str = "unversioned"
    code_version: str = "0.1.0"
    workers: int = 8
    replay: bool = False
    backup_dir: Path | None = None
    backup_hour_utc: int = 2
    market_retention_days: int = 90

    def load_config(self, name: str) -> dict[str, Any]:
        path = self.config_dir / (name if name.endswith((".yaml", ".yml")) else name + ".yaml")
        with path.open() as handle:
            value = yaml.safe_load(handle)
        if not isinstance(value, dict):
            raise ValueError(f"Configuration must be a mapping: {path}")
        return value
