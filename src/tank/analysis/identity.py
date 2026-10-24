"""Registry identity resolution with explicit names and qualified-alias semantics."""

import re
from collections.abc import Mapping, Sequence
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue


class IdentityModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class QualifiedAlias(IdentityModel):
    alias: str = Field(min_length=1)
    requires_any: list[str] = Field(min_length=1)


class EntityIdentity(IdentityModel):
    entity_id: str = Field(min_length=1)
    name: str | None = None
    names: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    qualified_aliases: list[QualifiedAlias] = Field(default_factory=list)


class TokenIdentity(IdentityModel):
    token_id: str = Field(min_length=1)
    symbol: str | None = None
    entity_id: str | None = None


class IdentityRegistry(IdentityModel):
    entities: list[EntityIdentity] = Field(default_factory=list)
    tokens: list[TokenIdentity] = Field(default_factory=list)

    @classmethod
    def from_wire(cls, registry: Mapping[str, JsonValue]) -> Self:
        """Project the identity namespace from the larger capture/configuration registry."""
        entities: list[dict[str, JsonValue]] = []
        tokens: list[dict[str, JsonValue]] = []
        for collection, fields, destination in (
            (
                "entities",
                ("entity_id", "name", "names", "aliases", "qualified_aliases"),
                entities,
            ),
            ("tokens", ("token_id", "symbol", "entity_id"), tokens),
        ):
            rows = registry.get(collection, [])
            if not isinstance(rows, list):
                raise ValueError(f"Registry {collection} must be an array")
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError(f"Registry {collection} must contain objects")
                destination.append({key: row[key] for key in fields if key in row})
        return cls.model_validate({"entities": entities, "tokens": tokens})


def _registry(registry: IdentityRegistry | Mapping[str, JsonValue]) -> IdentityRegistry:
    return registry if isinstance(registry, IdentityRegistry) else IdentityRegistry.from_wire(registry)


def _word(term: str, text: str) -> bool:
    return bool(term) and re.search(r"(?<!\w)" + re.escape(term) + r"(?!\w)", text, re.IGNORECASE) is not None


def resolve_entity(
    registry: IdentityRegistry | Mapping[str, JsonValue],
    name: str | None,
    entity_id: str | None = None,
    context: str = "",
) -> str:
    """A generic alias resolves only with its explicit qualifier; unknown names stay unresolved."""
    domain = _registry(registry)
    if entity_id is not None and any(row.entity_id == entity_id for row in domain.entities):
        return entity_id
    text = "" if name is None else name.strip()
    for row in domain.entities:
        qualified = {rule.alias.casefold() for rule in row.qualified_aliases}
        known = [*row.names, *row.aliases]
        if row.name is not None:
            known.append(row.name)
        if text and any(
            text.casefold() == value.casefold() and value.casefold() not in qualified for value in known
        ):
            return row.entity_id
        for rule in row.qualified_aliases:
            if (text.casefold() == rule.alias.casefold() or _word(rule.alias, text)) and any(
                _word(qualifier, text + " " + context) for qualifier in rule.requires_any
            ):
                return row.entity_id
    return "UNRESOLVED:" + text


def resolve_token(registry: IdentityRegistry | Mapping[str, JsonValue], value: str, context: str = "") -> str:
    """Resolve ids/tickers/projects; explicit external chain ids remain unchanged."""
    domain = _registry(registry)
    text = value.strip()
    if text.startswith("UNRESOLVED:") or any(row.token_id == text for row in domain.tokens) or ":" in text:
        return text
    ticker = text.removeprefix("$")
    for row in domain.tokens:
        if row.symbol is not None and ticker.casefold() == row.symbol.casefold():
            return row.token_id
    entity = resolve_entity(domain, text, context=context)
    for row in domain.tokens:
        if row.entity_id == entity:
            return row.token_id
    return "UNRESOLVED:" + text


def resolve_tokens(
    registry: IdentityRegistry | Mapping[str, JsonValue], values: Sequence[str], context: str = ""
) -> list[str]:
    domain = _registry(registry)
    return list(dict.fromkeys(resolve_token(domain, value, context) for value in values))


def entity_identity(
    registry: IdentityRegistry | Mapping[str, JsonValue],
    entity_id: str | None,
    name: str | None,
    context: str = "",
) -> str:
    """Require registry ids for known entities, retaining names only outside the registry."""
    if entity_id is not None:
        return entity_id
    if name is None or not name.strip():
        raise ValueError("Name each entity by its registry entity_id or, outside the registry, by its name")
    resolved = resolve_entity(registry, name, context=context)
    if not resolved.startswith("UNRESOLVED:"):
        raise ValueError(f"{name!r} is the registry entity {resolved}: pass its entity_id")
    return "UNRESOLVED:" + name.strip()


def token_identities(
    registry: IdentityRegistry | Mapping[str, JsonValue],
    token_ids: Sequence[str],
    names: Sequence[str],
    context: str = "",
) -> list[str]:
    """Registered tokens use ids, and unknown tokens retain explicit unresolved names."""
    domain = _registry(registry)
    unresolved = []
    for name in names:
        if not name.strip():
            raise ValueError("A token outside the registry needs its name")
        resolved = resolve_token(domain, name, context)
        if any(row.token_id == resolved for row in domain.tokens):
            raise ValueError(f"{name!r} is the registry token {resolved}: pass its token_id")
        unresolved.append("UNRESOLVED:" + name.strip())
    return list(dict.fromkeys([*token_ids, *unresolved]))
