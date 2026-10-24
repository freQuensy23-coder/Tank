"""Typed reporting data and a side-effect port independent of workflow decisions."""

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Literal, Protocol, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.source_items import SourceContextId
from tank.time_precision import Timestamp

type ProgressStage = Literal[
    "ANALYSIS_UPDATED",
    "RECEIVED",
    "EVIDENCE_FETCHED",
    "JEV_PASSED",
    "JEV_REJECTED",
    "JEV_BYPASSED",
    "AGENT_QUEUED",
    "AGENT_ACCEPTED",
    "EVENT_RECORDED",
    "OBSERVATION_ADDED",
    "AGENT_REJECTED",
    "ASSESSED",
    "PUBLICATION_BLOCKED",
    "ALERT_QUEUED",
    "ALERT_SENT",
    "ALERT_FAILED",
    "AGENT_COMPLETED",
    "PROCESSING_FAILED",
    "PROCESSING_EXPIRED",
]


class ProgressSettings(Protocol):
    @property
    def ops_progress_enabled(self) -> bool: ...
    @property
    def replay(self) -> bool: ...


class ProgressPreferences(DomainModel):
    ops_progress_enabled: bool
    replay: bool


class ProgressRepository(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(
        self, collection: str, key: str, record: dict[str, JsonValue], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[None]: ...
    def transaction(self) -> AbstractAsyncContextManager[None]: ...
    async def enqueue(
        self,
        kind: str,
        payload: dict[str, JsonValue],
        idem_key: str,
        *,
        priority: int,
        run_after: Timestamp,
        max_attempts: int,
        expires_at: Timestamp,
    ) -> JsonValue: ...


class EventProgressRepository(ProgressRepository, Protocol):
    def iter_metadata(
        self,
        collection: str,
        fields: list[str],
        *,
        filters: dict[str, JsonValue] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[dict[str, JsonValue]]: ...


class SourceProgressSink(Protocol):
    async def record(
        self,
        item_id: str,
        stage: ProgressStage,
        reason: str = "",
        *,
        event_id: str | None = None,
        key: str | None = None,
    ) -> None: ...


class PublicationReceipt(DomainModel):
    contract_version: Literal[1]
    event_id: SourceContextId
    kind: Literal["FULL", "PARTIAL", "UPDATE"] | None
    ems_id: SourceContextId
    ems_class: str = Field(min_length=1)
    lifecycle: str = Field(min_length=1)
    freshness: str | None
    certainty: str | None
    map_version: int | None = Field(ge=1)
    analysis_set: list[SourceContextId]
    decision_ids: list[SourceContextId]
    market_truth_ids: list[SourceContextId]
    previous_alert_id: SourceContextId | None
    previous_alert_version: int | None = Field(ge=1)
    disposition: Literal["PUBLISHED", "SUPPRESSED", "REFUSED"]
    reason: str = Field(min_length=1)
    alert_id: SourceContextId | None
    alert_version: int | None = Field(ge=1)
    newest_generated_alert_id: SourceContextId | None = None
    newest_generated_alert_version: int | None = Field(default=None, ge=1)
    trigger_item_id: SourceContextId | None = None
    trigger_freshness: str | None = None
    gate_reason: str | None = None

    @model_validator(mode="after")
    def complete_references(self) -> Self:
        for identity, version in (
            (self.previous_alert_id, self.previous_alert_version),
            (self.newest_generated_alert_id, self.newest_generated_alert_version),
            (self.alert_id, self.alert_version),
        ):
            if (identity is None) != (version is None):
                raise ValueError("Publication receipt alert identity and version must be declared together")
        if self.disposition == "PUBLISHED" and (self.alert_id is None or self.kind is None):
            raise ValueError("Published receipt requires its accepted card identity and kind")
        return self


class ProgressDetails(DomainModel):
    missing_market_truth: list[SourceContextId] | None = None
    missing_decisions: list[SourceContextId] | None = None
    publication_receipt: PublicationReceipt | None = None

    def storage_fields(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", exclude_unset=True)


class EventObservationSubject(DomainModel):
    item_id: SourceContextId


class ProgressSubject(DomainModel):
    item_id: SourceContextId
    source_id: str = Field(min_length=1)
    evidence_kind: Literal["FETCHED_PAGE", "SEARCH_RESULT"] | None = None


class ProgressIdentity(DomainModel):
    item_id: SourceContextId
    stage: ProgressStage
    event_id: SourceContextId | None
    key: str


class ProgressNotice(DomainModel):
    notice_id: str
    kind: Literal["ITEM_PROGRESS"] = "ITEM_PROGRESS"
    subject_id: SourceContextId
    stage: ProgressStage
    event_id: SourceContextId | None
    reason: str
    text: str
    format_version: Literal[2] = 2
    details: ProgressDetails
    created_at: str
    created_at_prec: Literal["COLLECTION_ONLY"] = "COLLECTION_ONLY"
    created_at_prec_reason: None = None
    created_by: Literal["ops-progress"] = "ops-progress"

    def storage_record(self) -> dict[str, JsonValue]:
        record: dict[str, JsonValue] = self.model_dump(mode="json")
        record["details"] = self.details.storage_fields()
        return record


class ToolProgressContext(DomainModel):
    event_id: SourceContextId | None = None
    call_id: SourceContextId | None = None


class PublicationProgressResult(DomainModel):
    published: bool
    reason: str = Field(min_length=1)
    alert_id: SourceContextId | None = None
    version: int | None = Field(default=None, ge=1)
    delivery_not_before: str | None = None
    details: ProgressDetails

    @model_validator(mode="after")
    def publication_identity(self) -> Self:
        if self.published and (self.alert_id is None or self.version is None):
            raise ValueError("Published progress requires its card identity and version")
        return self
