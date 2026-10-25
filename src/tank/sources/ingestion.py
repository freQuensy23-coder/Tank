"""Typed capture ingestion independent of polling, transport and presentation formatting."""

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol, Self

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.progress import SourceProgressSink
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.repository import DocumentStore, Repository
from tank.contracts.source_items import (
    SOURCE_ITEMS,
    ArticleDiscovery,
    CapturedItem,
    FederalRegisterDiscovery,
    SocialDiscovery,
    SourceItem,
    SourceItemDocument,
    SourceLifecycle,
    UnprocessedSource,
    captured_source_item,
)
from tank.contracts.sources import SourceAccount, SourceDefinition
from tank.sources.organization import PublisherContext, publisher_organization
from tank.sources.policy import initial_relevance, is_filtered_source
from tank.util import sha256


class IngestionStore(DocumentStore, Protocol):
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
    async def enqueue(
        self,
        kind: str,
        payload: dict[str, JsonValue],
        idem_key: str,
        *,
        priority: int,
        max_attempts: int,
    ) -> dict[str, JsonValue]: ...


class RawCaptureStore(Protocol):
    def discard(self, item: CapturedItem) -> None: ...


class EmptyContentIdentity(DomainModel):
    source_id: str
    external_id: str
    media: list[str] = Field(max_length=0)


class SourceIngestion:
    def __init__(
        self,
        store: IngestionStore,
        versions: ResolvedVersionIdentity,
        accounts: Mapping[str, SourceAccount],
        raw: RawCaptureStore,
        progress: SourceProgressSink,
    ) -> None:
        self.store = store
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        self.accounts = accounts
        self.raw = raw
        self.progress = progress
        self.items = Repository(store, SOURCE_ITEMS)

    async def captured(self, source_id: str, external_id: str) -> SourceItem | None:
        matches = await self.items.list(filters={"source_id": source_id, "external_id": external_id}, limit=1)
        return matches[0].root if matches else None

    async def same_content(self, content_hash: str) -> SourceItem | None:
        matches = await self.items.list(filters={"content_hash": content_hash}, limit=1)
        return matches[0].root if matches else None

    async def observe(self, item_id: str, feed: str) -> SourceItem:
        document = await self.items.get(item_id)
        if document is None:
            raise ValueError("Observed source item no longer exists")
        item = document.root
        updated = item.observe(feed)
        if updated != item:
            await self.items.put(item_id, SourceItemDocument(updated))
        return updated

    async def observe_known(self, source_id: str, external_id: str, feed: str) -> bool:
        item = await self.captured(source_id, external_id)
        if item is None:
            return False
        async with self.store.lock("ingest-dedup"), self.store.transaction():
            await self.observe(item.item_id, feed)
        return True

    async def ingest(self, source: SourceDefinition, captured: CapturedItem) -> SourceItem:
        material, evidence = captured.discovery, captured.capture
        if material.source_id != source.source_id:
            raise ValueError("Captured source identity conflicts with the configured publisher")
        quote = material.quoted if material.quoted is not None else ""
        body = " ".join((material.title + " " + material.text + " " + quote).split())
        digest = (
            sha256(body)
            if body
            else sha256(
                EmptyContentIdentity(
                    source_id=source.source_id, external_id=material.external_id, media=[]
                ).model_dump(mode="json")
            )
        )
        async with self.store.lock("ingest-dedup"), self.store.transaction():
            existing = await self.captured(source.source_id, material.external_id)
            if existing is None:
                existing = await self.same_content(digest)
            if existing is not None:
                if existing.item_id == evidence.item_id:
                    return existing
                self.raw.discard(captured)
                return await self.observe(existing.item_id, evidence.feed)
            if not body and not isinstance(material, SocialDiscovery):
                raise ValueError("Source entry has no normalized content")
            monitored = (
                material.monitored_author if isinstance(material, SocialDiscovery) else material.author
            )
            account = self.accounts.get(monitored.lower().lstrip("@")) if monitored is not None else None
            original = (
                self.accounts.get(material.author.lower().lstrip("@"))
                if material.author is not None
                else None
            )
            role = account.role if account is not None else None
            subtype = material.subtype if isinstance(material, ArticleDiscovery) else None
            filtered = is_filtered_source(source.source_id, subtype, role)
            deferrable = (
                source.source_id in {"S30", "S31", "S32", "S33", "S34"}
                or (source.source_id == "S25" and subtype == "commits")
                or (source.source_id == "S29" and role == "OTHER")
            )
            declared_organization = (
                material.author_organization
                if isinstance(material, (ArticleDiscovery, FederalRegisterDiscovery))
                else None
            )
            organization = publisher_organization(
                source,
                PublisherContext(
                    url=material.url,
                    feed=evidence.feed,
                    author_organization=declared_organization,
                    monitored_author=material.monitored_author
                    if isinstance(material, SocialDiscovery)
                    else None,
                    author=material.author,
                    agencies=material.agencies if isinstance(material, FederalRegisterDiscovery) else None,
                ),
                self.accounts,
            )
            lifecycle = SourceLifecycle(
                item_id=evidence.item_id,
                feed=evidence.feed,
                source=source.name,
                source_name=source.name,
                author=material.author if material.author is not None else source.name,
                author_role=role,
                original_author_role=original.role if original is not None else None,
                author_organization=organization,
                monitored_accounts=[material.monitored_author]
                if isinstance(material, SocialDiscovery)
                else [],
                deferrable=deferrable,
                raw_ref=evidence.raw_ref,
                raw_sha256=evidence.raw_sha256,
                content_hash=digest,
                observed_at=evidence.observed_at,
                observed_at_prec="COLLECTION_ONLY",
                observed_at_prec_reason=None,
                observed_at_precision="COLLECTION_ONLY",
                source_class=source.source_class,
                relevance=initial_relevance(source.source_id, subtype, role),
                extraction_status="PENDING",
                observation_count=1,
                discovery_config_version=evidence.discovery_config_version,
                discovery_code_version=evidence.discovery_code_version,
                config_version=self.versions.config_version,
                code_version=self.versions.code_version,
                processing=UnprocessedSource(),
            )
            item = captured_source_item(captured, lifecycle)
            await self.items.put(item.item_id, SourceItemDocument(item))
            await self.progress.record(
                item.item_id,
                "RECEIVED",
                "Ожидает проверки Jev" if filtered else "Jev не требуется; ожидает маршрутизации к агенту",
            )
            if not filtered:
                await self.progress.record(
                    item.item_id, "JEV_BYPASSED", "Источник проходит напрямую к агенту"
                )
            await self.store.enqueue(
                "FILTER_ITEM" if filtered else "ROUTE_ITEM",
                {"item_id": item.item_id},
                item.item_id,
                priority=2 if deferrable else 1,
                max_attempts=2,
            )
            return item
