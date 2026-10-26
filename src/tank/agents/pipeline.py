"""Typed source classification and event routing; wire JSON remains at adapters."""

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Literal, Protocol

import httpx
from pydantic import JsonValue, ValidationError

from tank.agents.classification import DECISION_PROMPT_VERSION as DECISION_PROMPT_VERSION
from tank.agents.classification import FILTER as FILTER
from tank.agents.classification import FILTER_CRITERIA as FILTER_CRITERIA
from tank.agents.classification import ROUTE as ROUTE
from tank.agents.classification import ROUTE_CRITERIA as ROUTE_CRITERIA
from tank.agents.classification import DecisionClassifier
from tank.agents.decision import DecisionEvent, DecisionItem, DecisionState
from tank.agents.pipeline_contracts import (
    ClassificationIdentity,
    ClassificationName,
    DecisionScoreRecord,
    RoutingCandidate,
    RoutingRegistry,
)
from tank.agents.routing import RoutingItem, RoutingSelector, item_state, item_view
from tank.contracts.configuration import Thresholds
from tank.contracts.job_repository import QueueStore
from tank.contracts.progress import SourceProgressSink
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.repository import Collection, DocumentStore, Repository
from tank.contracts.source_items import (
    SOURCE_ITEMS,
    AlwaysRelevance,
    ClassifiedRelevance,
    FeedSourceItem,
    PendingRelevance,
    RoutedSource,
    SourceItemDocument,
    SourceRoute,
    SourceRoutingMessage,
    WebEvidenceItem,
)
from tank.job_repository import JobRepository
from tank.runtime_contracts import ItemJob
from tank.time_precision import collection_clock, offset_schedule
from tank.util import TimeInput, iso, utcnow

TEXT_LIMIT = 12000
type CutoffName = Literal["filter_cutoff", "routing_cutoff"]


class PipelineSettings(Protocol):
    @property
    def foundry_api_key(self) -> str | None: ...
    @property
    def foundry_endpoint(self) -> str: ...
    @property
    def foundry_deployment(self) -> str: ...
    @property
    def replay(self) -> bool: ...

    def load_config(self, name: str) -> dict[str, JsonValue]: ...
    def thresholds(self) -> Thresholds: ...


class PipelineStore(QueueStore, DocumentStore, Protocol):
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


def priority(item: FeedSourceItem) -> Literal[1, 2]:
    return 2 if item.deferrable else 1


class ConfigurationError(ValueError):
    pass


class Pipeline:
    def __init__(
        self,
        repo: PipelineStore,
        settings: PipelineSettings,
        *,
        progress: SourceProgressSink,
        versions: ResolvedVersionIdentity,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        versions = ResolvedVersionIdentity.model_validate(versions)
        self.repo, self.settings = repo, settings
        self.progress = progress
        self.http = httpx.AsyncClient(timeout=15) if http is None else http
        self.owns_http = http is None
        self.items = Repository(repo, SOURCE_ITEMS)
        self.scores = Repository(repo, Collection("ops.llm_usage", DecisionScoreRecord))
        self.jobs = JobRepository(repo)
        self.classifier = DecisionClassifier(self.scores, settings, self.http, versions=versions)

    def cutoff(self, name: CutoffName) -> float:
        try:
            thresholds = self.settings.thresholds()
        except ValidationError:
            raise ConfigurationError(f"{name} requires a calibrated probability") from None
        match name:
            case "filter_cutoff":
                return thresholds.filter_cutoff
            case "routing_cutoff":
                return thresholds.routing_cutoff
            case _:
                raise ConfigurationError("Unregistered classification threshold")

    async def source_item(self, item_id: str) -> FeedSourceItem:
        document = await self.items.get(item_id)
        if document is None:
            raise KeyError(item_id)
        item = document.root
        if isinstance(item, WebEvidenceItem):
            raise ValueError("Publisher classification jobs cannot process agent-requested WEB evidence")
        if item.item_id != item_id:
            raise ValueError("Source item identity does not match its repository key")
        return item

    async def save_source(self, item: FeedSourceItem) -> FeedSourceItem:
        stored = await self.items.put(item.item_id, SourceItemDocument(item))
        if isinstance(stored.root, WebEvidenceItem):
            raise ValueError("Publisher source storage unexpectedly returned WEB evidence")
        return stored.root

    @staticmethod
    def item_view(item: FeedSourceItem) -> DecisionItem:
        return item_view(RoutingItem.from_source(item))

    @staticmethod
    def state(item: FeedSourceItem) -> DecisionState:
        return item_state(RoutingItem.from_source(item))

    async def score(self, item: FeedSourceItem, name: ClassificationName, state: DecisionState) -> float:
        return await self.classifier.score(item, name, state)

    def score_identity(
        self, item: FeedSourceItem, name: ClassificationName, state: DecisionState
    ) -> ClassificationIdentity:
        return self.classifier.score_identity(item, name, state)

    async def _score_record(
        self, item: FeedSourceItem, name: ClassificationName, state: DecisionState
    ) -> DecisionScoreRecord:
        return await self.classifier._score_record(item, name, state)

    async def filter_item_wire(self, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        item = await self.filter_item(ItemJob.model_validate(payload))
        return item.storage_record()

    async def filter_item(self, payload: ItemJob) -> FeedSourceItem:
        async with self.repo.lock("filter:" + payload.item_id):
            item = await self.source_item(payload.item_id)
            if isinstance(item.processing, RoutedSource):
                return item
            scored: DecisionScoreRecord | None = None
            cutoff: float | None = None
            if not isinstance(item.relevance, AlwaysRelevance):
                cutoff = self.cutoff("filter_cutoff")
                scored = await self._score_record(item, "relevant", self.state(item))
            async with self.repo.lock("ingest-dedup"), self.repo.transaction():
                item = await self.source_item(payload.item_id)
                promoted = isinstance(item.relevance, AlwaysRelevance)
                if isinstance(item.processing, RoutedSource):
                    return item
                if not promoted:
                    if scored is None or cutoff is None:
                        raise ValueError("Unconditional source relevance changed during classification")
                    relevance = ClassifiedRelevance(
                        decision="SEND" if scored.score >= cutoff else "SKIP",
                        p_relevant=scored.score,
                        cutoff=cutoff,
                        model=scored.resolved_model_id,
                        provider=scored.classifier_provider,
                        requested_model=scored.requested_model_id,
                        semantic_model_id=scored.semantic_model_id,
                        model_pin_evidence=scored.model_pin_evidence,
                        usage_id=scored.usage_id,
                    )
                    item = await self.save_source(item.classified(relevance))
                accepted = promoted or (
                    isinstance(item.relevance, ClassifiedRelevance) and item.relevance.decision == "SEND"
                )
                detail = "Источник проходит напрямую к агенту"
                if not promoted:
                    if scored is None or cutoff is None:
                        raise RuntimeError("Conditional classification lost its score")
                    detail = f"score={scored.score:.4f}; порог={cutoff:.4f}"
                await self.progress.record(
                    item.item_id,
                    "JEV_BYPASSED" if promoted else "JEV_PASSED" if accepted else "JEV_REJECTED",
                    detail,
                )
                if accepted:
                    await self.jobs.enqueue(
                        "ROUTE_ITEM",
                        ItemJob(item_id=item.item_id).model_dump(
                            mode="json", by_alias=True, exclude_unset=True
                        ),
                        item.item_id,
                        priority=priority(item),
                        max_attempts=2,
                        expires_at=offset_schedule(collection_clock(item.observed_at), timedelta(hours=6)),
                    )
            return item

    def registry(self) -> RoutingRegistry:
        return RoutingRegistry.from_wire(self.settings.load_config("registry"))

    def selector(self) -> RoutingSelector:
        return RoutingSelector(self.repo, self.registry())

    def identities(self, item: FeedSourceItem) -> set[str]:
        return self.selector().identities(RoutingItem.from_source(item))

    def event_view(self, event: RoutingCandidate) -> DecisionEvent:
        return self.selector().event_view(event)

    def routing_state(self, item: FeedSourceItem, event: RoutingCandidate) -> DecisionState:
        return self.selector().routing_state(RoutingItem.from_source(item), event)

    async def _candidate_events(
        self, identities: set[str], since: str, now: datetime
    ) -> list[RoutingCandidate]:
        return await self.selector().candidate_events(identities, since, now)

    async def candidate_events(self, item: FeedSourceItem) -> list[RoutingCandidate]:
        now = utcnow()
        since = iso(now - timedelta(days=self.settings.thresholds().routing_window_days))
        return await self._candidate_events(self.identities(item), since, now)

    async def route_item_wire(self, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        item = await self.route_item(ItemJob.model_validate(payload))
        return item.storage_record()

    async def route_item(self, payload: ItemJob) -> FeedSourceItem:
        async with self.repo.lock("routing:" + payload.item_id):
            item = await self.source_item(payload.item_id)
            if isinstance(item.processing, RoutedSource):
                return item
            if isinstance(item.relevance, PendingRelevance) or item.relevance.decision == "SKIP":
                raise ValueError("Source relevance does not permit routing to an agent")
            identities = self.identities(item)
            now = utcnow()
            since = iso(now - timedelta(days=self.settings.thresholds().routing_window_days))
            best: RoutingCandidate | None = None
            best_score = -1.0
            scores: dict[str, float] = {}
            candidates = await self._candidate_events(identities, since, now)
            if candidates:
                cutoff = self.cutoff("routing_cutoff")
                requests = [
                    asyncio.create_task(self.score(item, "same_event", self.routing_state(item, event)))
                    for event in candidates
                ]
                try:
                    results = await asyncio.gather(*requests, return_exceptions=True)
                except BaseException:
                    for request in requests:
                        request.cancel()
                    await asyncio.gather(*requests, return_exceptions=True)
                    raise
                for event, score in zip(candidates, results, strict=True):
                    if isinstance(score, BaseException):
                        raise score
                    scores[event.event_id] = score
                    if score >= cutoff and score > best_score:
                        best, best_score = event, score
            if best is not None:
                message = SourceRoutingMessage(
                    kind="ITEM_ADDED", item_id=item.item_id, event_id=best.event_id
                )
                route = SourceRoute(decision="ADDED", event_id=best.event_id, p_same=best_score)
            else:
                message = SourceRoutingMessage(kind="NEW_ITEM", item_id=item.item_id)
                route = SourceRoute(decision="NEW_SESSION", event_id=None, p_same=None)
            async with self.repo.lock("ingest-dedup"), self.repo.transaction():
                item = await self.source_item(payload.item_id)
                item = await self.save_source(item.routed(route, message, scores))
                await self.progress.record(
                    item.item_id,
                    "AGENT_QUEUED",
                    "Маршрутизация: существующее событие"
                    if best is not None
                    else "Маршрутизация: новая сессия",
                    event_id=route.event_id,
                )
                await self.jobs.enqueue(
                    "AGENT_MESSAGE",
                    message.model_dump(mode="json", exclude_unset=True),
                    item.item_id + ":" + message.kind,
                    priority=priority(item),
                )
            return item

    async def close(self) -> None:
        if self.owns_http:
            await self.http.aclose()
