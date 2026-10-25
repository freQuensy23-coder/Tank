"""Read-only registry projections used to label model context."""

from typing import Literal, Protocol, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel


class ContextConfiguration(Protocol):
    def load_config(self, name: str) -> dict[str, JsonValue]: ...
    def read_config_json(self, name: str) -> JsonValue: ...


class TokenLabel(DomainModel):
    token_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    name: str | None
    project: str | None

    def display_name(self) -> str:
        if self.project:
            return self.project
        if self.name:
            return self.name
        return self.symbol


class EntityLabel(DomainModel):
    entity_id: str = Field(min_length=1)
    name: str | None
    names: list[str]
    aliases: list[str]

    def display_name(self) -> str:
        if self.name and self.name != self.entity_id:
            return self.name
        if self.aliases:
            return self.aliases[0]
        if self.names:
            return self.names[0]
        return self.entity_id


class InstrumentLabel(DomainModel):
    instrument_id: str = Field(min_length=1)
    venue: str = Field(min_length=1)
    market_type: str = Field(min_length=1)
    venue_symbol: str = Field(min_length=1)


class SourceLabel(DomainModel):
    source_id: str = Field(min_length=1)
    name: str = Field(min_length=1)


type LabelNamespace = Literal["tokens", "entities", "instruments", "sources"]


def label_rows(configuration: dict[str, JsonValue], namespace: LabelNamespace) -> list[dict[str, JsonValue]]:
    rows = configuration[namespace]
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError(f"Context {namespace} must be an array of objects")
    return [row for row in rows if isinstance(row, dict)]


class ContextRegistry(DomainModel):
    tokens: list[TokenLabel]
    entities: list[EntityLabel]
    instruments: list[InstrumentLabel]
    sources: list[SourceLabel]

    @model_validator(mode="after")
    def unique_identities(self) -> Self:
        for identifiers in (
            [row.token_id for row in self.tokens],
            [row.entity_id for row in self.entities],
            [row.instrument_id for row in self.instruments],
            [row.source_id for row in self.sources],
        ):
            if len(identifiers) != len(set(identifiers)):
                raise ValueError("Context registry identities must be unique within their namespace")
        return self

    @classmethod
    def from_configuration(cls, settings: ContextConfiguration) -> Self:
        registry = settings.load_config("registry")
        sources = settings.load_config("sources")
        return cls(
            tokens=[
                TokenLabel.model_validate(
                    {
                        "token_id": row["token_id"],
                        "symbol": row["symbol"],
                        "name": row.get("name"),
                        "project": row.get("project"),
                    }
                )
                for row in label_rows(registry, "tokens")
            ],
            entities=[
                EntityLabel.model_validate(
                    {
                        "entity_id": row["entity_id"],
                        "name": row.get("name"),
                        "names": row["names"],
                        "aliases": row["aliases"],
                    }
                )
                for row in label_rows(registry, "entities")
            ],
            instruments=[
                InstrumentLabel.model_validate(
                    {key: row[key] for key in ("instrument_id", "venue", "market_type", "venue_symbol")}
                )
                for row in label_rows(registry, "instruments")
            ],
            sources=[
                SourceLabel.model_validate({key: row[key] for key in ("source_id", "name")})
                for row in label_rows(sources, "sources")
            ],
        )
