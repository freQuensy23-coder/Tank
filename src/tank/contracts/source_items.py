"""Canonical source discovery and immutable capture evidence contracts."""

from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import (
    ConfigDict,
    Field,
    JsonValue,
    RootModel,
    field_validator,
    model_serializer,
    model_validator,
)

from tank.contracts import DomainModel
from tank.contracts.page_evidence import PageReadProvenance, RecordedBodyProvenance
from tank.contracts.provenance import GenerationProvenance
from tank.contracts.repository import Collection
from tank.contracts.sources import SourceAuthorRole
from tank.time_precision import Timestamp, collection_clock, propagate_timestamp
from tank.util import parse_iso_time

type BucketAnchor = Literal["START", "END", "UNSPECIFIED"]
type SourceContextId = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]


class PublisherCMSIdentity(DomainModel):
    entry_id: str = Field(min_length=1)
    slug: str = Field(pattern=r"^[A-Za-z0-9-]+$")
    title: str = Field(min_length=1)
    content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class PublisherCMSRecovery(DomainModel):
    status: Literal["VERIFIED_SAME_CMS_ENTRY"]
    canonical_url: str
    resolved_url: str
    original_http_status: Literal[404]
    original_response_url: str
    original_payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    observed_at: str
    publisher_cms_identity: PublisherCMSIdentity
    observed_at_prec: Literal["COLLECTION_ONLY"] | None = None
    observed_at_prec_reason: None = None


class IssuingAgency(DomainModel):
    name: str | None = None
    raw_name: str | None = None
    id: int | None = Field(default=None, ge=0)
    parent_id: int | None = None
    url: str | None = None
    json_url: str | None = None
    slug: str | None = None

    @model_validator(mode="after")
    def declared_publisher(self) -> Self:
        if self.id is None and not (
            (self.name is not None and self.name.strip())
            or (self.raw_name is not None and self.raw_name.strip())
        ):
            raise ValueError("Issuing agency requires a declared name or identifier")
        return self


class AnnouncementLabel(DomainModel):
    key: str | None = None
    title: str | None = None
    name: str | None = None

    @model_validator(mode="after")
    def named_label(self) -> Self:
        if not any(value is not None and value.strip() for value in (self.key, self.title, self.name)):
            raise ValueError("Announcement label requires its declared key or name")
        return self


class SourceText(DomainModel):
    title: str
    text: str
    quoted: str | None = None


class DiscoveryBase(SourceText):
    author: str | None = None
    source_id: str
    external_id: str
    url: str
    published_at: str | None
    published_prec: str | None = None
    published_at_precision: str | None = None
    published_prec_reason: str | None = None
    published_at_reason: str | None = None
    published_at_missing_reason: str | None = None
    published_at_bucket_size_s: int | float | str | None = None
    published_at_bucket_anchor: BucketAnchor | None = None
    published_at_basis: str | None = None
    source_updated_at: str | None = None
    source_updated_at_prec: str | None = None
    source_updated_at_prec_reason: str | None = None
    source_updated_at_precision: str | None = None

    @field_validator("external_id")
    @classmethod
    def nonempty_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Source discovery requires an external identity")
        return value

    @model_validator(mode="after")
    def publication_provenance(self) -> Self:
        _ = self.publication
        return self

    @property
    def publication(self) -> Timestamp:
        return propagate_timestamp(
            {
                "published_at": self.published_at,
                "published_prec": self.published_prec,
                "published_at_precision": self.published_at_precision,
                "published_prec_reason": self.published_prec_reason,
                "published_at_reason": self.published_at_reason,
                "published_at_bucket_size_s": self.published_at_bucket_size_s,
                "published_at_bucket_anchor": self.published_at_bucket_anchor,
            },
            "published_at",
        )


class ArticleDiscovery(DiscoveryBase):
    source_id: Literal[
        "S01",
        "S02",
        "S04",
        "S05",
        "S06",
        "S07",
        "S08",
        "S12",
        "S13",
        "S15",
        "S16",
        "S17",
        "S18",
        "S19",
        "S20",
        "S21",
        "S22",
        "S23",
        "S25",
        "S26",
        "S27",
        "S28",
        "S30",
        "S31",
        "S32",
        "S33",
        "S34",
    ]
    categories: list[str | None] = Field(default_factory=list)
    subtype: Literal["releases", "commits"] | None = None
    publisher_cms_identity: PublisherCMSIdentity | None = None
    publisher_cms_slug_url: str | None = None
    publisher_cms_recovery: PublisherCMSRecovery | None = None
    publisher_url_aliases: list[str] = Field(default_factory=list)
    author_organization: str | None = None


class FederalRegisterDiscovery(DiscoveryBase):
    source_id: Literal["S03"]
    document_type: str | None = None
    pdf_url: str | None = None
    agencies: list[IssuingAgency] | None = None
    author_organization: str | None = None


class ExchangeAnnouncementDiscovery(DiscoveryBase):
    source_id: Literal["S09", "S10", "S11"]
    announcement_type: int | str | AnnouncementLabel | None = None
    tags: list[str | AnnouncementLabel] | None = None
    business_published_at: str | None = None
    business_published_at_prec: str | None = None
    business_published_at_prec_reason: str | None = None
    business_published_at_precision: str | None = None


class InstrumentDiscovery(DiscoveryBase):
    source_id: Literal["S14"]
    change: Literal["ADDED", "REMOVED", "STATE_UPDATE"]


class GovernanceDiscovery(DiscoveryBase):
    source_id: Literal["S24"]
    proposal_id: str = Field(min_length=1)
    state: str = Field(min_length=1)
    end: str | None
    end_prec: str | None = None
    end_prec_reason: str | None = None
    end_precision: str | None = None
    scores: list[int | float]
    scores_total: int | float


class SocialDiscovery(DiscoveryBase):
    source_id: Literal["S29"]
    author: str = Field(min_length=1)
    monitored_author: str = Field(min_length=1)


type DiscoveredItem = (
    ArticleDiscovery
    | FederalRegisterDiscovery
    | ExchangeAnnouncementDiscovery
    | InstrumentDiscovery
    | GovernanceDiscovery
    | SocialDiscovery
)


class DiscoveryObservation(DomainModel):
    observed_at: str | None = None
    observed_at_prec: Literal["COLLECTION_ONLY"] | None = None
    observed_at_prec_reason: None = None
    observed_at_precision: Literal["COLLECTION_ONLY"] | None = None


class DiscoveryEnvelope(DomainModel):
    discovery: DiscoveredItem
    observation: DiscoveryObservation
    payload: bytes | None


def discovered_item(source_id: str, wire: Mapping[str, JsonValue | bytes]) -> DiscoveryEnvelope:
    """Decode a publisher material separately from local clock and raw transport bytes."""
    if "source_id" in wire and wire["source_id"] != source_id:
        raise ValueError("Discovery source identity conflicts with its configured source")
    context = {"observed_at", "observed_at_prec", "observed_at_prec_reason", "observed_at_precision"}
    record = {key: value for key, value in wire.items() if key not in context and key != "_payload"}
    record["source_id"] = source_id
    external_id = record["external_id"]
    if (
        isinstance(external_id, bool)
        or not isinstance(external_id, (str, int))
        or not str(external_id).strip()
    ):
        raise ValueError("Publisher entry has no valid external identity")
    record["external_id"] = str(external_id)
    material: DiscoveredItem
    if source_id == "S03":
        material = FederalRegisterDiscovery.model_validate(record)
    elif source_id in {"S09", "S10", "S11"}:
        material = ExchangeAnnouncementDiscovery.model_validate(record)
    elif source_id == "S14":
        material = InstrumentDiscovery.model_validate(record)
    elif source_id == "S24":
        material = GovernanceDiscovery.model_validate(record)
    elif source_id == "S29":
        material = SocialDiscovery.model_validate(record)
    else:
        material = ArticleDiscovery.model_validate(record)
    return DiscoveryEnvelope.model_validate(
        {
            "discovery": material,
            "observation": {field: wire[field] for field in context if field in wire},
            "payload": wire.get("_payload"),
        }
    )


class SourceObservedClock(DomainModel):
    observed_at: str

    @field_validator("observed_at")
    @classmethod
    def aware_collection_clock(cls, value: str) -> str:
        parse_iso_time(value)
        return value


class CaptureEvidence(SourceObservedClock):
    item_id: str = Field(min_length=1)
    feed: str = Field(min_length=1)
    observed_at: str
    raw_ref: str = Field(min_length=1)
    raw_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    discovery_config_version: str
    discovery_code_version: str


class CapturedItem(DomainModel):
    discovery: DiscoveredItem
    capture: CaptureEvidence

    @model_validator(mode="after")
    def publication_provenance(self) -> Self:
        _ = self.discovery.publication
        return self

    def storage_record(self) -> dict[str, JsonValue]:
        return {
            **self.discovery.model_dump(mode="json", by_alias=True, exclude_unset=True),
            **self.capture.model_dump(mode="json"),
            **collection_clock(self.capture.observed_at).fields("observed_at"),
        }


def captured_item(source_id: str, wire: Mapping[str, JsonValue]) -> CapturedItem:
    """Decode the flat capture/checkpoint wire without weakening required capture evidence."""
    capture_fields = {
        "item_id",
        "feed",
        "raw_ref",
        "raw_sha256",
        "discovery_config_version",
        "discovery_code_version",
    }
    material = discovered_item(
        source_id, {name: value for name, value in wire.items() if name not in capture_fields}
    )
    evidence = CaptureEvidence.model_validate(
        {name: wire[name] for name in (*capture_fields, "observed_at") if name in wire}
    )
    return CapturedItem(discovery=material.discovery, capture=evidence)


class RawSourceReference(DomainModel):
    raw_ref: str = Field(min_length=1)
    raw_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class PendingSourceItem(SourceObservedClock):
    discovery: DiscoveredItem
    item_id: str = Field(min_length=1)
    feed: str = Field(min_length=1)
    observed_at: str
    discovery_config_version: str
    discovery_code_version: str
    raw: RawSourceReference | None

    def storage_record(self) -> dict[str, JsonValue]:
        record: dict[str, JsonValue] = {
            **self.discovery.model_dump(mode="json", by_alias=True, exclude_unset=True),
            "item_id": self.item_id,
            "feed": self.feed,
            **collection_clock(self.observed_at).fields("observed_at"),
            "discovery_config_version": self.discovery_config_version,
            "discovery_code_version": self.discovery_code_version,
        }
        if self.raw is not None:
            record.update(self.raw.model_dump(mode="json"))
        return record

    def captured(self) -> CapturedItem:
        if self.raw is None:
            raise ValueError("Source item has no captured raw evidence")
        return CapturedItem(
            discovery=self.discovery,
            capture=CaptureEvidence(
                item_id=self.item_id,
                feed=self.feed,
                observed_at=self.observed_at,
                raw_ref=self.raw.raw_ref,
                raw_sha256=self.raw.raw_sha256,
                discovery_config_version=self.discovery_config_version,
                discovery_code_version=self.discovery_code_version,
            ),
        )


class PendingRelevance(DomainModel):
    decision: None
    pending_reason: Literal["AWAITING_FILTER", "SOURCE_INGESTION_POLICY_NOT_CAPTURED"]


class AlwaysRelevance(DomainModel):
    decision: Literal["ALWAYS"]
    reason: Literal["AGENT_REQUESTED_EVIDENCE"] | None = None


class ClassifiedRelevance(DomainModel):
    decision: Literal["SEND", "SKIP"]
    p_relevant: float = Field(ge=0, le=1, allow_inf_nan=False)
    cutoff: float = Field(ge=0, le=1, allow_inf_nan=False)
    model: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    requested_model: str = Field(min_length=1)
    semantic_model_id: str = Field(min_length=1)
    model_pin_evidence: str = Field(min_length=1)
    usage_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def cutoff_decision(self) -> Self:
        if (self.decision == "SEND") != (self.p_relevant >= self.cutoff):
            raise ValueError("Relevance decision conflicts with its score and cutoff")
        return self


type SourceRelevance = PendingRelevance | AlwaysRelevance | ClassifiedRelevance


class SourceRoutingMessage(DomainModel):
    kind: Literal["NEW_ITEM", "ITEM_ADDED"]
    item_id: SourceContextId
    event_id: SourceContextId | None = None
    session_id: SourceContextId | None = None


class SourceRoute(DomainModel):
    decision: Literal["ADDED", "NEW_SESSION"]
    event_id: SourceContextId | None
    p_same: float | None = Field(ge=0, le=1, allow_inf_nan=False)

    @model_validator(mode="after")
    def resolved_target(self) -> Self:
        if self.decision == "ADDED" and (self.event_id is None or self.p_same is None):
            raise ValueError("Added source route requires an event and matching probability")
        if self.decision == "NEW_SESSION" and (self.event_id is not None or self.p_same is not None):
            raise ValueError("New source route cannot claim an existing event match")
        return self


class UnprocessedSource(DomainModel):
    state: Literal["UNPROCESSED"] = "UNPROCESSED"

    def storage_fields(self) -> dict[str, JsonValue]:
        return {}


class ClassifiedSource(DomainModel):
    state: Literal["CLASSIFIED"] = "CLASSIFIED"
    status: Literal["RELEVANT", "SKIPPED"]
    filter_score: float = Field(ge=0, le=1, allow_inf_nan=False)

    def storage_fields(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", exclude={"state"})


class RoutedSource(DomainModel):
    state: Literal["ROUTED"] = "ROUTED"
    status: Literal["ROUTED"]
    route: SourceRoute
    routing_message: SourceRoutingMessage
    routing_scores: dict[str, float]
    route_event_id: str | None
    filter_score: float | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)

    @model_validator(mode="after")
    def consistent_target(self) -> Self:
        if self.route_event_id != self.route.event_id:
            raise ValueError("Source route event identity conflicts with its route")
        expected = "ITEM_ADDED" if self.route.decision == "ADDED" else "NEW_ITEM"
        if self.routing_message.kind != expected or self.routing_message.event_id != self.route.event_id:
            raise ValueError("Source routing message conflicts with its route")
        if any(not 0 <= score <= 1 for score in self.routing_scores.values()):
            raise ValueError("Source routing probability is outside 0..1")
        return self

    def storage_fields(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", exclude={"state"}, exclude_unset=True)


type SourceProcessing = UnprocessedSource | ClassifiedSource | RoutedSource
type ExtractionStatus = Literal["PENDING", "RUNNING", "DONE", "SKIPPED", "FAILED"]
type SourceClass = Literal["PRIMARY", "SECONDARY", "SOCIAL_LEAD", "TECHNICAL_EVIDENCE"]


class PendingExtraction(DomainModel):
    status: Literal["PENDING"]


class RunningExtraction(DomainModel):
    status: Literal["RUNNING"]


class CompletedExtraction(DomainModel):
    status: Literal["DONE"]


class SkippedExtraction(DomainModel):
    status: Literal["SKIPPED"]
    decision: Literal["NO_EVENT"] | None
    reason: str | None


class FailedExtraction(DomainModel):
    status: Literal["FAILED"]
    reason: str | None


type SourceExtractionOutcome = (
    PendingExtraction | RunningExtraction | CompletedExtraction | SkippedExtraction | FailedExtraction
)


class SourceExtractionFields(DomainModel):
    extraction_status: ExtractionStatus
    extraction_decision: Literal["NO_EVENT"] | None = None
    extraction_reason: str | None = None

    def with_extraction(self, outcome: SourceExtractionOutcome) -> Self:
        """Replace the current outcome; retry and success explicitly clear prior rejection/failure metadata."""
        decision = outcome.decision if isinstance(outcome, SkippedExtraction) else None
        reason = outcome.reason if isinstance(outcome, (SkippedExtraction, FailedExtraction)) else None
        return type(self).model_validate(
            self.model_copy(
                update={
                    "extraction_status": outcome.status,
                    "extraction_decision": decision,
                    "extraction_reason": reason,
                }
            )
        )


class SourceEntityHint(DomainModel):
    entity_id: str = Field(min_length=1)
    name: str | None = None


class SourceIdentityHints(DomainModel):
    tokens_mentioned: list[str] = Field(default_factory=list)
    entities: list[str | SourceEntityHint] = Field(default_factory=list)


class SourceLifecycle(SourceExtractionFields, SourceObservedClock):
    item_id: str = Field(min_length=1)
    feed: str = Field(min_length=1)
    source: str = Field(min_length=1)
    source_name: str = Field(min_length=1)
    author: str = Field(min_length=1)
    author_role: SourceAuthorRole | None
    original_author_role: SourceAuthorRole | None
    author_organization: str | None
    monitored_accounts: list[str]
    deferrable: bool
    raw_ref: str = Field(min_length=1)
    raw_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    content_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    observed_at: str
    observed_at_prec: Literal["COLLECTION_ONLY"]
    observed_at_prec_reason: None
    observed_at_precision: Literal["COLLECTION_ONLY"] | None = None
    source_class: SourceClass
    relevance: SourceRelevance
    observation_count: int = Field(ge=1)
    observed_feeds: list[str] | None = None
    discovery_config_version: str | None = None
    discovery_code_version: str | None = None
    config_version: str
    code_version: str
    processing: SourceProcessing = Field(discriminator="state")
    identity_hints: SourceIdentityHints | None = None

    @model_validator(mode="after")
    def consistent_processing(self) -> Self:
        processing = self.processing
        if isinstance(processing, ClassifiedSource):
            if not isinstance(self.relevance, ClassifiedRelevance):
                raise ValueError("Classified source requires its classification provenance")
            if processing.filter_score != self.relevance.p_relevant:
                raise ValueError("Source filter score conflicts with relevance probability")
            expected = "RELEVANT" if self.relevance.decision == "SEND" else "SKIPPED"
            if processing.status != expected:
                raise ValueError("Source classification status conflicts with relevance decision")
        if isinstance(processing, RoutedSource):
            if isinstance(self.relevance, PendingRelevance) or self.relevance.decision == "SKIP":
                raise ValueError("Unapproved source relevance cannot route to an agent")
            if (
                isinstance(self.relevance, ClassifiedRelevance)
                and processing.filter_score != self.relevance.p_relevant
            ):
                raise ValueError("Routed source lost its classification probability")
            if processing.routing_message.item_id != self.item_id:
                raise ValueError("Source routing message item identity conflicts with the captured item")
        if self.observed_feeds is not None:
            if len(self.observed_feeds) != self.observation_count or len(set(self.observed_feeds)) != len(
                self.observed_feeds
            ):
                raise ValueError("Source observation count conflicts with its known feeds")
            if self.feed not in self.observed_feeds:
                raise ValueError("Source observations omit the captured feed")
        return self

    @property
    def status(self) -> Literal["RELEVANT", "SKIPPED", "ROUTED"] | None:
        return None if isinstance(self.processing, UnprocessedSource) else self.processing.status

    def classified(self, relevance: ClassifiedRelevance) -> Self:
        if isinstance(self.relevance, AlwaysRelevance):
            raise ValueError("Unconditional ingestion policy cannot be replaced by a classifier")
        updated = self.model_copy(
            update={
                "relevance": relevance,
                "processing": ClassifiedSource(
                    status="RELEVANT" if relevance.decision == "SEND" else "SKIPPED",
                    filter_score=relevance.p_relevant,
                ),
                "extraction_status": "SKIPPED" if relevance.decision == "SKIP" else self.extraction_status,
            }
        )
        return type(self).model_validate(updated)

    def routed(self, route: SourceRoute, message: SourceRoutingMessage, scores: dict[str, float]) -> Self:
        previous = self.processing
        if isinstance(previous, (ClassifiedSource, RoutedSource)):
            processing = RoutedSource(
                status="ROUTED",
                route=route,
                routing_message=message,
                routing_scores=scores,
                route_event_id=route.event_id,
                filter_score=previous.filter_score,
            )
        else:
            processing = RoutedSource(
                status="ROUTED",
                route=route,
                routing_message=message,
                routing_scores=scores,
                route_event_id=route.event_id,
            )
        return type(self).model_validate(self.model_copy(update={"processing": processing}))

    def observe(self, feed: str) -> Self:
        known = self.observed_feeds
        if known is None:
            if self.observation_count != 1:
                raise ValueError("Source observation feeds were not captured for multiple sightings")
            known = [self.feed]
        if feed in known:
            return self
        updated = self.model_copy(
            update={
                "observed_feeds": [*known, feed],
                "observation_count": self.observation_count + 1,
            }
        )
        return type(self).model_validate(updated)

    def storage_record(self) -> dict[str, JsonValue]:
        record: dict[str, JsonValue] = self.model_dump(
            mode="json", by_alias=True, exclude={"processing", "identity_hints"}, exclude_unset=True
        )
        record.update(self.processing.storage_fields())
        if self.identity_hints is not None:
            record.update(self.identity_hints.model_dump(mode="json", exclude_unset=True))
        return record


class ArticleSourceItem(SourceLifecycle, ArticleDiscovery):
    author: str = Field(min_length=1)


class FederalRegisterSourceItem(SourceLifecycle, FederalRegisterDiscovery):
    author: str = Field(min_length=1)


class ExchangeAnnouncementSourceItem(SourceLifecycle, ExchangeAnnouncementDiscovery):
    author: str = Field(min_length=1)


class InstrumentSourceItem(SourceLifecycle, InstrumentDiscovery):
    author: str = Field(min_length=1)


class GovernanceSourceItem(SourceLifecycle, GovernanceDiscovery):
    author: str = Field(min_length=1)


class SocialSourceItem(SourceLifecycle, SocialDiscovery):
    author: str = Field(min_length=1)


type FeedSourceItem = (
    ArticleSourceItem
    | FederalRegisterSourceItem
    | ExchangeAnnouncementSourceItem
    | InstrumentSourceItem
    | GovernanceSourceItem
    | SocialSourceItem
)


def captured_source_item(captured: CapturedItem, lifecycle: SourceLifecycle) -> FeedSourceItem:
    """Assemble the inherited aggregate from validated material and workflow facts."""
    material = captured.discovery
    evidence = captured.capture
    if (
        lifecycle.item_id != evidence.item_id
        or lifecycle.feed != evidence.feed
        or lifecycle.raw_ref != evidence.raw_ref
        or lifecycle.raw_sha256 != evidence.raw_sha256
        or lifecycle.observed_at != evidence.observed_at
    ):
        raise ValueError("Source lifecycle conflicts with its captured evidence")
    document: dict[str, JsonValue | SourceProcessing] = {
        **material.model_dump(mode="json", by_alias=True, exclude_unset=True),
        **lifecycle.model_dump(mode="json", exclude={"processing"}, exclude_unset=True),
        "processing": lifecycle.processing,
    }
    if isinstance(material, FederalRegisterDiscovery):
        return FederalRegisterSourceItem.model_validate(document)
    if isinstance(material, ExchangeAnnouncementDiscovery):
        return ExchangeAnnouncementSourceItem.model_validate(document)
    if isinstance(material, InstrumentDiscovery):
        return InstrumentSourceItem.model_validate(document)
    if isinstance(material, GovernanceDiscovery):
        return GovernanceSourceItem.model_validate(document)
    if isinstance(material, SocialDiscovery):
        return SocialSourceItem.model_validate(document)
    return ArticleSourceItem.model_validate(document)


def _source_processing(record: Mapping[str, JsonValue]) -> SourceProcessing:
    routing_fields = {"route", "routing_message", "routing_scores", "route_event_id"}
    if "status" not in record:
        if "filter_score" in record or any(field in record for field in routing_fields):
            raise ValueError("Source processing metadata requires its persisted status")
        return UnprocessedSource()
    status = record["status"]
    if status == "RELEVANT" or status == "SKIPPED":
        if any(field in record for field in routing_fields):
            raise ValueError("Classified source cannot contain unresolved routed metadata")
        return ClassifiedSource.model_validate(
            {field: record[field] for field in ("status", "filter_score") if field in record}
        )
    if status == "ROUTED":
        fields: dict[str, JsonValue] = {
            name: record[name]
            for name in ("status", "route", "routing_message", "routing_scores", "route_event_id")
            if name in record
        }
        if "filter_score" in record:
            fields["filter_score"] = record["filter_score"]
        return RoutedSource.model_validate(fields)
    raise ValueError("Unknown persisted source processing status")


def parse_feed_source_item(record: Mapping[str, JsonValue]) -> FeedSourceItem:
    """Validate the flat durable source document at its repository boundary."""
    processing_fields = {
        "tokens_mentioned",
        "entities",
        "status",
        "filter_score",
        "route",
        "routing_message",
        "routing_scores",
        "route_event_id",
    }
    document: dict[str, JsonValue | SourceProcessing | SourceIdentityHints] = {
        key: value for key, value in record.items() if key not in processing_fields
    }
    document["processing"] = _source_processing(record)
    if "tokens_mentioned" in record or "entities" in record:
        document["identity_hints"] = SourceIdentityHints.model_validate(
            {field: record[field] for field in ("tokens_mentioned", "entities") if field in record}
        )
    source_id = record["source_id"]
    if not isinstance(source_id, str):
        raise ValueError("Persisted source item requires its declared source identity")
    if source_id == "S03":
        return FederalRegisterSourceItem.model_validate(document)
    if source_id in {"S09", "S10", "S11"}:
        return ExchangeAnnouncementSourceItem.model_validate(document)
    if source_id == "S14":
        return InstrumentSourceItem.model_validate(document)
    if source_id == "S24":
        return GovernanceSourceItem.model_validate(document)
    if source_id == "S29":
        return SocialSourceItem.model_validate(document)
    return ArticleSourceItem.model_validate(document)


class WebEvidenceItem(GenerationProvenance, SourceText, SourceExtractionFields, SourceObservedClock):
    item_id: str = Field(min_length=1)
    source_id: Literal["WEB"]
    feed: Literal["web:search", "web:page"]
    published_prec: Literal["COLLECTION_ONLY"]
    published_at: None
    published_at_precision: Literal["COLLECTION_ONLY"]
    published_prec_reason: str | None = None
    relevance: AlwaysRelevance
    external_id: str
    raw_ref: str = Field(min_length=1)
    raw_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    raw_media_type: str = Field(min_length=1)
    evidence_kind: Literal["FETCHED_PAGE", "SEARCH_RESULT"]
    raw_size_bytes: int = Field(ge=0)
    readable_text_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    content_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    url: str = Field(min_length=1)
    observed_at: str
    observed_at_prec: Literal["COLLECTION_ONLY"]
    observed_at_prec_reason: None
    observed_at_precision: Literal["COLLECTION_ONLY"] | None = None
    source_class: SourceClass
    page_read_provenance: PageReadProvenance | None = None
    observation_count: int | None = Field(default=None, ge=1)
    observed_feeds: list[str] | None = None

    @model_validator(mode="after")
    def consistent_observations(self) -> Self:
        if (self.observation_count is None) != (self.observed_feeds is None):
            raise ValueError("Web source observation count and feeds must be declared together")
        if self.observed_feeds is not None and (
            len(self.observed_feeds) != self.observation_count
            or len(set(self.observed_feeds)) != len(self.observed_feeds)
            or self.feed not in self.observed_feeds
        ):
            raise ValueError("Web source observations conflict with their captured origin")
        return self

    def observe(self, feed: str) -> Self:
        known = self.observed_feeds if self.observed_feeds is not None else [self.feed]
        if feed in known:
            return self
        return type(self).model_validate(
            self.model_copy(update={"observation_count": len(known) + 1, "observed_feeds": [*known, feed]})
        )

    @model_validator(mode="after")
    def evidence_origin(self) -> Self:
        if (
            isinstance(self.page_read_provenance, RecordedBodyProvenance)
            and self.page_read_provenance.captured_body_sha256 != self.raw_sha256
        ):
            raise ValueError("Recorded page provenance conflicts with the stored raw digest")
        expected = "web:search" if self.evidence_kind == "SEARCH_RESULT" else "web:page"
        if self.feed != expected or self.external_id != self.item_id:
            raise ValueError("Web evidence origin conflicts with its capture identity")
        if self.relevance.reason != "AGENT_REQUESTED_EVIDENCE":
            raise ValueError("Web evidence requires its agent-requested evidence provenance")
        return self

    def storage_record(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", exclude_unset=True)


type SourceItem = FeedSourceItem | WebEvidenceItem


def parse_source_item(record: Mapping[str, JsonValue]) -> SourceItem:
    if record["source_id"] == "WEB":
        return WebEvidenceItem.model_validate(record)
    return parse_feed_source_item(record)


class SourceItemDocument(RootModel[SourceItem]):
    """Closed source-item wire codec for the shared typed repository port."""

    model_config = ConfigDict(
        strict=True,
        frozen=True,
        validate_default=True,
        allow_inf_nan=False,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    @model_validator(mode="before")
    @classmethod
    def decode_document(cls, data: SourceItem | JsonValue | Self) -> SourceItem:
        if isinstance(data, cls):
            return data.root
        if isinstance(
            data,
            (
                ArticleSourceItem,
                FederalRegisterSourceItem,
                ExchangeAnnouncementSourceItem,
                InstrumentSourceItem,
                GovernanceSourceItem,
                SocialSourceItem,
                WebEvidenceItem,
            ),
        ):
            return data
        if isinstance(data, dict):
            return parse_source_item(data)
        raise ValueError("Source item document must be a typed source item or a stored JSON object")

    @model_serializer(mode="plain")
    def encode_document(self) -> dict[str, JsonValue]:
        return self.root.storage_record()


SOURCE_ITEMS = Collection("ingest.source_items", SourceItemDocument)
