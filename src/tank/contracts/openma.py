"""Closed OpenMA configuration and synchronization boundary contracts."""

from typing import Literal, Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel


class OpenMAModelPrices(DomainModel):
    input_per_million: int | float = Field(ge=0)
    output_per_million: int | float = Field(ge=0)
    cache_read_per_million: int | float = Field(ge=0)
    cache_creation_per_million: int | float = Field(ge=0)


class OpenMAConfiguration(DomainModel):
    version: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    scout_agent_id: str = Field(min_length=1)
    environment_id: str = Field(min_length=1)
    replay_environment_id: str = Field(min_length=1)
    release: str = Field(pattern=r"^[a-f0-9]{40}$")
    prompt_version: str = Field(min_length=1)
    model_id: str = Field(min_length=1)
    reasoning_effort: str = Field(min_length=1)
    model_prices: dict[str, OpenMAModelPrices]
    pricing_basis: str = Field(min_length=1)
    model_access: Literal["CHATGPT_SUBSCRIPTION_CODEX_OAUTH"]
    gateway_base_url: str = Field(min_length=1)
    model_card_provider: str = Field(min_length=1)
    openma_overlay: str = Field(min_length=1)
    max_running_sessions: int = Field(ge=1)

    @model_validator(mode="after")
    def declared_model_prices(self) -> Self:
        if self.model_id not in self.model_prices:
            raise ValueError("OpenMA model requires explicitly configured prices")
        return self

    def agent_id_for(self, name: str) -> str:
        match name:
            case "tank-event-analyst":
                return self.agent_id
            case "tank-scout-reviewer":
                return self.scout_agent_id
            case _:
                raise ValueError("Unknown managed OpenMA agent")

    def environment_id_for(self, name: str) -> str:
        match name:
            case "tank-analysis":
                return self.environment_id
            case "tank-replay":
                return self.replay_environment_id
            case _:
                raise ValueError("Unknown managed OpenMA environment")
