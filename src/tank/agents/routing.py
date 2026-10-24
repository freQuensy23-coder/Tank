"""Pure routing views and read-only candidate selection."""

import re
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Protocol, Self

from pydantic import JsonValue

from tank.agents.jev import JevEvent, JevItem, JevState
from tank.agents.pipeline_contracts import RoutingCandidate, RoutingRegistry, RoutingSession
from tank.contracts import DomainModel
from tank.contracts.source_items import (
    FeedSourceItem,
    SocialSourceItem,
    SourceEntityHint,
    SourceIdentityHints,
)
from tank.util import TimeInput, iso

TEXT_LIMIT = 12000


class RoutingItem(DomainModel):
    item_id: str
    source: str
    author: str
    title: str
    text: str
    quoted: str | None
    author_organization: str | None
    identity_hints: SourceIdentityHints | None
    monitored_author: str | None

    @classmethod
    def from_source(cls, item: FeedSourceItem) -> Self:
        return cls(
            item_id=item.item_id,
            source=item.source,
            author=item.author,
            title=item.title,
            text=item.text,
            quoted=item.quoted,
            author_organization=item.author_organization,
            identity_hints=item.identity_hints,
            monitored_author=item.monitored_author if isinstance(item, SocialSourceItem) else None,
        )


class RoutingReadStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    def iter_range(
        self,
        collection: str,
        time_field: str,
        *,
        start: TimeInput | None = None,
        end: TimeInput | None = None,
        filters: dict[str, JsonValue] | None = None,
        batch_size: int = 10000,
    ) -> AsyncIterator[dict[str, JsonValue]]: ...


def _clean(value: str) -> str:
    return " ".join(value.split())


def _words(value: str) -> list[str]:
    return re.findall(r"\w+", value.casefold())


def _truncate(text: str, limit: int = TEXT_LIMIT) -> str:
    if len(text) <= limit:
        return text
    cut = text[:limit]
    end = max(cut.rfind(". "), cut.rfind("? "), cut.rfind("! "))
    return (cut[: end + 1] if end > limit // 2 else cut[: cut.rfind(" ")]) + " …"


def item_view(item: RoutingItem) -> JevItem:
    source = _clean(item.source)
    author = _clean(item.author)
    title = _clean(item.title)
    body = _clean(item.text)
    if title and source:
        for separator in (" - ", " | ", " — "):
            if title.casefold().endswith((separator + source).casefold()):
                title = title[: -len(separator + source)].rstrip()
    if body.casefold().startswith(title.casefold()):
        body = body[len(title) :].lstrip(" -|:—")
    if set(_words(body)) <= set(_words(title)) | set(_words(source)):
        body = ""
    quoted = _clean(item.quoted) if item.quoted is not None else ""
    return JevItem(
        source=source,
        author=author if author and author.casefold() != source.casefold() else None,
        title=title if title else None,
        text=_truncate(body) if body else None,
        quoted=_truncate(quoted) if quoted else None,
    )


def item_state(item: RoutingItem) -> JevState:
    return JevState(item=item_view(item))


class RoutingSelector:
    def __init__(self, repo: RoutingReadStore, registry: RoutingRegistry) -> None:
        self.repo = repo
        self.registry = registry

    def identities(self, item: RoutingItem) -> set[str]:
        registry = self.registry
        text = " ".join(
            value
            for value in (item.title, item.text, item.quoted, item.author_organization)
            if value is not None
        )
        hints = item.identity_hints
        raw_tokens = hints.tokens_mentioned if hints is not None else []
        result = set(raw_tokens)
        if hints is not None:
            result.update(
                entity.entity_id if isinstance(entity, SourceEntityHint) else entity
                for entity in hints.entities
            )
        symbols = {
            token.symbol.casefold(): token.token_id for token in registry.tokens if token.symbol is not None
        }
        for value in raw_tokens:
            symbol = value.removeprefix("$").casefold()
            if symbol in symbols:
                result.add(symbols[symbol])

        def matches(name: str) -> bool:
            return (
                bool(name)
                and re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", text, re.IGNORECASE) is not None
            )

        for entity in registry.entities:
            hit = any(matches(name) for name in entity.aliases) or any(
                matches(rule.alias) and any(matches(qualifier) for qualifier in rule.requires_any)
                for rule in entity.qualified_aliases
            )
            if hit:
                result.update((entity.entity_id, *entity.tokens))
        for token in registry.tokens:
            if token.symbol is not None and matches("$" + token.symbol):
                result.add(token.token_id)
                if token.entity_id is not None:
                    result.add(token.entity_id)
        authors = {item.author.lstrip("@").lower()}
        if item.monitored_author is not None:
            authors.add(item.monitored_author.lstrip("@").lower())
        for account in registry.accounts:
            if account.handle.lstrip("@").lower() in authors and account.entity_id is not None:
                result.add(account.entity_id)
        for token in registry.tokens:
            if token.token_id in result or (token.entity_id is not None and token.entity_id in result):
                result.add(token.token_id)
                if token.entity_id is not None:
                    result.add(token.entity_id)
        return result

    def event_view(self, event: RoutingCandidate) -> JevEvent:
        registry = self.registry
        names: dict[str, str] = {}
        for entity in registry.entities:
            if entity.aliases:
                names[entity.entity_id] = entity.aliases[0]
            elif entity.names:
                names[entity.entity_id] = entity.names[0]
            else:
                names[entity.entity_id] = entity.entity_id
        symbols = {token.token_id: token.symbol for token in registry.tokens if token.symbol is not None}

        def label(key: str, table: dict[str, str]) -> str:
            key = key.removeprefix("UNRESOLVED:")
            return table[key] if key in table else key.split(":")[-1]

        return JevEvent(
            statement=_clean(event.statement),
            entities=list(dict.fromkeys(label(entity, names) for entity in event.entities)),
            tokens=list(dict.fromkeys(label(token, symbols) for token in event.tokens_mentioned)),
            stage=event.event_state.lower(),
        )

    def routing_state(self, item: RoutingItem, event: RoutingCandidate) -> JevState:
        return JevState(item=item_view(item), event=self.event_view(event))

    async def candidate_events(
        self, identities: set[str], since: str, now: datetime
    ) -> list[RoutingCandidate]:
        recent: set[str] = set()
        async for row in self.repo.iter_range(
            "ops.agent_sessions",
            "last_item_at",
            start=since,
            end=iso(now + timedelta(microseconds=1)),
        ):
            session = RoutingSession.from_wire(row)
            if session.event_id is not None and session.status != "FAILED":
                recent.add(session.event_id)
        events: list[RoutingCandidate] = []
        for event_id in sorted(recent):
            candidate_record = await self.repo.get("evt.events", event_id)
            if candidate_record is None:
                continue
            event = RoutingCandidate.from_wire(candidate_record)
            if event.event_id != event_id:
                raise ValueError("Routing event identity does not match its repository key")
            if event.lifecycle in {"CLOSED", "MERGED", "INVALIDATED"}:
                continue
            if identities.intersection((*event.entities, *event.tokens_mentioned)):
                events.append(event)
        return events
