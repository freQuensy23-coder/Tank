"""Consumed delivery activation facts independent of transport and persistence."""

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal, Protocol, Self

from pydantic import ConfigDict, Field, JsonValue, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.progress import ProgressStage
from tank.contracts.scalars import FalseFlag
from tank.contracts.source_errors import DiagnosticDocumentReference, SourceEntryError, parse_entry_error
from tank.contracts.telegram_state import MessageReference
from tank.contracts.telegram_wire import PositiveId, TelegramRetryParameters, TelegramSentMessageReceipt
from tank.time_precision import Timestamp, propagate_timestamp
from tank.util import JsonInput, TimeInput, parse_iso_time, sha256

type DeliveryIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]
type DeliveredPrice = str | int | float


class DeliverySettings(Protocol):
    @property
    def telegram_token(self) -> str | None: ...
    @property
    def telegram_alerts_chat(self) -> str | None: ...
    @property
    def telegram_ops_chat(self) -> str | None: ...
    @property
    def ops_progress_enabled(self) -> bool: ...
    @property
    def replay(self) -> bool: ...
    @property
    def ops_progress_delay_s(self) -> float: ...


class DeliveryStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(
        self, collection: str, key: str, record: dict[str, JsonValue], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
    def iter_all(
        self, collection: str, *, filters: dict[str, JsonValue] | None = None, batch_size: int = 1000
    ) -> AsyncIterator[dict[str, JsonValue]]: ...
    def iter_metadata(
        self,
        collection: str,
        fields: list[str],
        *,
        filters: dict[str, JsonValue] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[dict[str, JsonValue]]: ...
    async def list_latest(
        self,
        collection: str,
        time_field: str,
        *,
        filters: dict[str, JsonValue] | None = None,
        limit: int = 1000,
    ) -> list[dict[str, JsonValue]]: ...
    async def enqueue(
        self,
        kind: str,
        payload: Mapping[str, JsonInput],
        idem_key: str,
        *,
        priority: int = 1,
        run_after: TimeInput | Timestamp | None = None,
        expires_at: TimeInput | Timestamp | None = None,
        max_attempts: int = 3,
    ) -> dict[str, JsonValue]: ...


class OpsCardOutcome(DomainModel):
    code: DeliveryIdentity
    status: str
    fields: list[tuple[str, str]]
    reason: str
    link: str | None = None


class RenderedOpsCard(OpsCardOutcome):
    html: str
    text: str


class OpsPresentationClocks(DomainModel):
    created_at: str | None = None
    at: str | None = None
    cutoff: str | None = None
    generated_at: str | None = None
    observed_at: str | None = None

    @field_validator("created_at", "at", "cutoff", "generated_at", "observed_at")
    @classmethod
    def recorded_clock(cls, value: str | None) -> str | None:
        if value is not None:
            parse_iso_time(value)
        return value


class OpsRelevance(DomainModel):
    decision: Literal["ALWAYS", "SEND", "SKIP"] | None = None
    p_relevant: float | None = None
    cutoff: float | None = None


type OpsInstrumentValue = str | int | float | bool | None


class OpsInstrumentView(DomainModel):
    symbol: OpsInstrumentValue = None
    venue: OpsInstrumentValue = None
    market_type: OpsInstrumentValue = None
    status: OpsInstrumentValue = None
    base: OpsInstrumentValue = None
    quote: OpsInstrumentValue = None
    contract_type: OpsInstrumentValue = None

    def displayed_fields(self) -> dict[str, OpsInstrumentValue]:
        return {
            "symbol": self.symbol,
            "venue": self.venue,
            "market_type": self.market_type,
            "status": self.status,
            "base": self.base,
            "quote": self.quote,
            "contract_type": self.contract_type,
        }


class OpsSourceItem(OpsPresentationClocks):
    item_id: DeliveryIdentity
    title: str | None = None
    text: str | None = None
    body: str | None = None
    quoted: str | None = None
    source_id: str | None = None
    source_name: str | None = None
    source: str | None = None
    url: str | None = None
    evidence_kind: str | None = None
    route_event_id: DeliveryIdentity | None = None
    extraction_status: str | None = None
    extraction_reason: str | None = None
    extraction_decision: str | None = None
    relevance: OpsRelevance | None = None


class OpsProgressDetails(DomainModel):
    missing_market_truth: list[str] | None = None
    missing_decisions: list[str] | None = None


class OpsProgressNotice(OpsPresentationClocks):
    notice_id: DeliveryIdentity
    stage: ProgressStage
    event_id: DeliveryIdentity | None = None
    reason: str | None = None
    format_version: int | None = None
    details: OpsProgressDetails | None = None
    subject_id: DeliveryIdentity | None = None
    created_precision: str | None = None
    created_precision_reason: str | None = None
    created_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    def creation_clock(self) -> Timestamp | None:
        if self.created_at is None and all(
            value is None
            for value in (self.created_precision, self.created_precision_reason, self.created_bucket_anchor)
        ):
            return None
        reason = self.created_precision_reason
        if reason is None and (self.created_at is None or self.created_precision is None):
            reason = "INCOMPLETE"
        return Timestamp(self.created_at, self.created_precision, reason, self.created_bucket_anchor)


class OpsEventView(DomainModel):
    statement: str | None = None
    current_ems_id: DeliveryIdentity | None = None


class OpsAssessmentView(OpsPresentationClocks):
    model_config = ConfigDict(**DomainModel.model_config, validate_by_name=True)

    classification: str | None = Field(default=None, alias="class")
    class_reasons: list[str] | None = None


class OpsMapView(OpsPresentationClocks):
    map_version: int | None = Field(default=None, ge=1)
    analysis_set: list[DeliveryIdentity] | None = None


class SelectedOpsMap(DomainModel):
    map_version: int = Field(ge=1)
    analysis_set: tuple[DeliveryIdentity, ...]


class UnknownOpsMap(DomainModel):
    reason: Literal["MAP_VERSION_UNREPORTED", "ANALYSIS_SET_UNREPORTED"]


def select_ops_map(maps: Sequence[OpsMapView]) -> SelectedOpsMap | UnknownOpsMap:
    candidates: list[tuple[int, OpsMapView]] = []
    for record in maps:
        if record.map_version is None:
            return UnknownOpsMap(reason="MAP_VERSION_UNREPORTED")
        candidates.append((record.map_version, record))
    if not candidates:
        raise ValueError("Ops map selection requires a recorded map")
    version = max(candidate[0] for candidate in candidates)
    selected_sets: list[frozenset[str]] = []
    unreported = False
    for candidate_version, selected in candidates:
        if candidate_version != version:
            continue
        if selected.analysis_set is None:
            unreported = True
            continue
        members = frozenset(selected.analysis_set)
        if selected_sets and members != selected_sets[0]:
            raise ValueError("Ops map version has contradictory analysis sets")
        selected_sets.append(members)
    if unreported:
        return UnknownOpsMap(reason="ANALYSIS_SET_UNREPORTED")
    return SelectedOpsMap(map_version=version, analysis_set=tuple(sorted(selected_sets[0])))


class OpsTokenView(OpsPresentationClocks):
    token_id: DeliveryIdentity


class OpsAlertView(OpsPresentationClocks):
    alert_id: DeliveryIdentity
    version: int = Field(gt=0)
    delivery_not_before: str | None = None


class OpsDeliveryView(OpsPresentationClocks):
    status: Literal["GENERATED", "FAILED", "SENT"]
    terminal: bool | None = None
    attempt: int | None = Field(default=None, ge=0)
    suppressed: bool | None = None
    http_status: int | None = Field(default=None, ge=100, le=599)
    error: str | None = None
    chat_id: str | None = None
    message_ref: MessageReference | None = None

    @model_validator(mode="after")
    def actual_receipt_phase(self) -> Self:
        if self.attempt == 0 and not (
            self.status == "GENERATED"
            or self.status == "FAILED"
            and (self.suppressed is True or self.terminal is True)
        ):
            raise ValueError("Zero delivery attempts require a recorded pre-send phase")
        if self.status == "SENT" and self.message_ref is None:
            raise ValueError("Sent Ops publication requires its actual message receipt")
        return self


class OpsObservationView(DomainModel):
    event_id: DeliveryIdentity | None = None
    link_type: str | None = None


class OpsCardSnapshot(DomainModel):
    item: OpsSourceItem
    history: list[OpsProgressNotice] = Field(min_length=1)
    stages: dict[ProgressStage, OpsProgressNotice]
    event: OpsEventView | None
    assessment: OpsAssessmentView | None
    maps: list[OpsMapView]
    truths: list[OpsTokenView]
    decisions: list[OpsTokenView]
    alert: OpsAlertView | None
    deliveries: list[OpsDeliveryView]
    prior_receipt: OpsDeliveryView | None
    observations: list[OpsObservationView]


class OpsAdoptedClockBasis(DomainModel):
    kind: Literal["ADOPTED_PARENT_PENDING"]
    item_id: DeliveryIdentity
    chat_id: DeliveryIdentity
    notice_id: DeliveryIdentity
    message_ref: MessageReference
    parent_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    at: Timestamp

    @model_validator(mode="after")
    def actual_parent_clock(self) -> Self:
        if self.at.value is None:
            raise ValueError("Adopted progress timing requires its observed parent clock")
        return self


def absent_adopted_basis(value: OpsAdoptedClockBasis | None) -> bool:
    return value is None


class DeliveredCardActivation(DomainModel):
    event_id: DeliveryIdentity
    alert_id: DeliveryIdentity
    version: int = Field(gt=0)
    decisions: tuple[DeliveryIdentity, ...]

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        values = {name: record[name] for name in ("event_id", "alert_id", "version") if name in record}
        decisions = record.get("decisions")
        if not isinstance(decisions, list):
            raise ValueError("Delivered card decisions require an explicit identity list")
        return cls.model_validate({**values, "decisions": tuple(decisions)})


class DeliveredCorrection(DomainModel):
    event_id: DeliveryIdentity
    alert_id: DeliveryIdentity
    version: int = Field(gt=0)

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate(
            {name: record[name] for name in ("event_id", "alert_id", "version") if name in record}
        )


class CardDeliveryHeader(DeliveredCorrection):
    kind: Literal["FULL", "PARTIAL", "UPDATE", "PRELIMINARY"] | None = None
    delivery_immediate: bool | None = None
    terminal_correction: bool | None = None
    milestone_resolution_id: DeliveryIdentity | None = None
    delivery_not_before: str | None = None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({name: record[name] for name in cls.model_fields if name in record})


class DeliveryText(DomainModel):
    text: str


class DeliveryEventBinding(DomainModel):
    event_id: DeliveryIdentity | None = None


class CardDeliveryText(DomainModel):
    card_text: str


class SentDeliveryReceipt(DomainModel):
    status: Literal["SENT"]
    message_ref: MessageReference
    at: Timestamp

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        stamp = propagate_timestamp(record, "at")
        if stamp.value is None:
            raise ValueError("Sent delivery requires its recorded clock")
        return cls.model_validate(
            {
                **{name: record[name] for name in ("status", "message_ref") if name in record},
                "at": stamp,
            }
        )


class DeliveryLedgerPhase(DomainModel):
    status: Literal["GENERATED", "FAILED", "SENT"]
    terminal: bool | None = None


class DeliveredEventView(DomainModel):
    event_id: DeliveryIdentity
    lifecycle: str | None = None
    known_to_us_at: str | None = None
    first_card_delivered_at: str | None = None
    first_card_deadline: str | None = None
    milestone_resolution_id: DeliveryIdentity | None = None
    call_expired_ids: list[DeliveryIdentity] | None = None
    expiry_notices_sent: list[DeliveryIdentity] | None = None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue], *, event_id: str) -> Self:
        result = cls.model_validate({name: record[name] for name in cls.model_fields if name in record})
        if result.event_id != event_id:
            raise ValueError("Delivered event differs from the publication's actual event")
        return result


def delivery_snapshot_object(
    record: Mapping[str, JsonValue], path: tuple[str, ...]
) -> Mapping[str, JsonValue] | None:
    current: Mapping[str, JsonValue] = record
    for name in path:
        value = current.get(name)
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError("Reported delivery snapshot requires an object")
        current = value
    return current


class UnreportedDeliveryInterval(DomainModel):
    kind: Literal["UNREPORTED"]


class FrozenDeliveryInterval(DomainModel):
    update_rate_limit_s: float = Field(ge=0)

    @classmethod
    def from_card(cls, record: Mapping[str, JsonValue]) -> Self | UnreportedDeliveryInterval:
        raw = delivery_snapshot_object(record, ("card_struct", "event", "thresholds"))
        if raw is None:
            return UnreportedDeliveryInterval(kind="UNREPORTED")
        return cls.model_validate({name: raw[name] for name in cls.model_fields if name in raw})


class FrozenDeliveryAssessment(DomainModel):
    classification: str | None = Field(default=None, alias="class")

    @classmethod
    def from_card(cls, record: Mapping[str, JsonValue]) -> Self:
        raw = delivery_snapshot_object(record, ("card_struct", "assessment"))
        return cls.model_validate({"class": raw["class"]} if raw is not None and "class" in raw else {})


class DeliveryExpiryReferences(DomainModel):
    expired_call_ids: list[DeliveryIdentity] | None = None
    call_expired_ids: list[DeliveryIdentity] | None = None
    snapshot_call_expired_ids: list[DeliveryIdentity] | None = None

    @classmethod
    def from_card(cls, record: Mapping[str, JsonValue]) -> Self:
        raw = delivery_snapshot_object(record, ("card_struct", "event"))
        return cls.model_validate(
            {
                **{name: record[name] for name in ("expired_call_ids", "call_expired_ids") if name in record},
                **(
                    {"snapshot_call_expired_ids": raw["call_expired_ids"]}
                    if raw is not None and "call_expired_ids" in raw
                    else {}
                ),
            }
        )

    def delivered_ids(self) -> list[str] | None:
        for values in (self.expired_call_ids, self.call_expired_ids, self.snapshot_call_expired_ids):
            if values:
                return values
        return None


class InvalidatedDeliveryEvent(DomainModel):
    event_id: DeliveryIdentity
    lifecycle: Literal["INVALIDATED"]


class DocumentDeliveryNotice(DomainModel):
    notice_id: DeliveryIdentity
    entry_errors: tuple[SourceEntryError, ...]
    document_manifest: tuple[DiagnosticDocumentReference, ...]

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        errors = record.get("entry_errors")
        manifest = record.get("document_manifest")
        if not isinstance(errors, list) or not isinstance(manifest, list):
            raise ValueError("Document delivery requires its recorded errors and manifest")
        admitted: list[SourceEntryError] = []
        for error in errors:
            if not isinstance(error, dict):
                raise ValueError("Document delivery error requires an object")
            admitted.append(parse_entry_error(error))
        return cls.model_validate(
            {
                **{name: record[name] for name in ("notice_id",) if name in record},
                "entry_errors": tuple(admitted),
                "document_manifest": tuple(
                    DiagnosticDocumentReference.model_validate(row) for row in manifest
                ),
            }
        )


class DocumentLedgerEntry(DomainModel):
    notice_id: DeliveryIdentity
    channel: Literal["telegram:ops"]
    status: Literal["GENERATED", "FAILED", "SENT"]
    document_id: DeliveryIdentity | None = None
    method: str | None = None
    message_ref: MessageReference | None = None
    chat_id: DeliveryIdentity | None = None

    @model_validator(mode="after")
    def document_identity(self) -> Self:
        if self.method == "sendDocument" and self.document_id is None:
            raise ValueError("Document delivery requires its recorded document identity")
        if self.document_id is not None and self.method not in (None, "sendDocument"):
            raise ValueError("Document identity conflicts with its reported delivery method")
        return self

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue], notice_id: str) -> Self:
        result = cls.model_validate({name: record[name] for name in cls.model_fields if name in record})
        if result.notice_id != notice_id:
            raise ValueError("Document history differs from its actual notice")
        return result


class DocumentRetryAttempt(DomainModel):
    method: Literal["sendDocument"]
    attempt: int = Field(ge=1)
    terminal: bool | None = None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({name: record[name] for name in cls.model_fields if name in record})


class DocumentParentReceipt(DomainModel):
    notice_id: DeliveryIdentity
    channel: Literal["telegram:ops"]
    message_ref: MessageReference
    chat_id: DeliveryIdentity


class DeliveryTelegramRejection(DomainModel):
    ok: FalseFlag
    parameters: TelegramRetryParameters | None = None
    description: str | None = None


class TelegramReplyReceipt(DomainModel):
    message_id: PositiveId


class DeliveryTelegramReceipt(TelegramSentMessageReceipt):
    reply_to_message: TelegramReplyReceipt | None = None

    @classmethod
    def from_result(cls, result: JsonValue) -> Self:
        if not isinstance(result, dict):
            raise ValueError("Telegram sent receipt requires an object")
        values = {name: result[name] for name in ("message_id", "date") if name in result}
        reply = result.get("reply_to_message")
        if reply is not None:
            if not isinstance(reply, dict):
                raise ValueError("Telegram reported reply requires an object")
            return cls.model_validate(
                {
                    **values,
                    "reply_to_message": TelegramReplyReceipt.model_validate(
                        {name: reply[name] for name in ("message_id",) if name in reply}
                    ),
                }
            )
        return cls.model_validate(values)


class ProgressDeliveryNotice(DomainModel):
    notice_id: DeliveryIdentity
    kind: Literal["ITEM_PROGRESS"]
    subject_id: DeliveryIdentity

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({name: record[name] for name in cls.model_fields if name in record})


class OpsCardDeliveryState(DomainModel):
    item_id: DeliveryIdentity
    chat_id: str
    message_ref: MessageReference | None = None
    covered_notice_ids: tuple[DeliveryIdentity, ...] | None = None
    render_digest: str | None = None
    blocked: bool | None = None
    updated_at: str | None = None
    adopted_notice_id: DeliveryIdentity | None = None
    adopted_clock_basis: OpsAdoptedClockBasis | None = Field(default=None, exclude_if=absent_adopted_basis)

    @model_validator(mode="after")
    def reported_adoption(self) -> Self:
        if "adopted_clock_basis" in self.model_fields_set and self.adopted_clock_basis is None:
            raise ValueError("Reported adopted clock basis requires its observed receipt")
        if self.adopted_clock_basis is not None and (
            self.adopted_clock_basis.item_id != self.item_id
            or self.adopted_clock_basis.chat_id != self.chat_id
            or str(self.adopted_clock_basis.message_ref) != str(self.message_ref)
        ):
            raise ValueError("Adopted clock basis differs from its actual card target")
        return self

    @field_validator("updated_at")
    @classmethod
    def actual_update_clock(cls, value: str | None) -> str | None:
        if value is not None:
            parse_iso_time(value)
        return value

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue], *, item_id: str, chat_id: str) -> Self:
        values = {name: record[name] for name in cls.model_fields if name in record}
        if "covered_notice_ids" in values:
            notices = values["covered_notice_ids"]
            if not isinstance(notices, list):
                raise ValueError("Covered progress notices require their actual identity list")
            values.pop("covered_notice_ids")
            result = cls.model_validate({**values, "covered_notice_ids": tuple(notices)})
        else:
            result = cls.model_validate(values)
        if result.item_id != item_id or result.chat_id != chat_id:
            raise ValueError("Stored Ops card does not belong to its delivery recipient and item")
        return result


class OpsSpacingPolicy(DomainModel):
    delay_s: float = Field(ge=0, le=60)


class OpsImmediateSchedule(DomainModel):
    kind: Literal["IMMEDIATE"]


class OpsDueSchedule(DomainModel):
    kind: Literal["NOT_BEFORE"]
    basis: Literal["FIRST_NOTICE", "CARD_UPDATE", "ADOPTED_PARENT", "RECORDED_PROGRESS_SENT"]
    not_before: Timestamp

    @model_validator(mode="after")
    def actual_due(self) -> Self:
        if self.not_before.value is None:
            raise ValueError("Progress scheduling requires its actual clock value")
        return self


class OpsSentReceipt(DomainModel):
    notice_id: DeliveryIdentity
    channel: Literal["telegram:ops"]
    status: Literal["SENT"]
    chat_id: DeliveryIdentity
    message_ref: MessageReference
    progress_card: str | None = None
    method: str | None = None
    document_id: str | None = None
    receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    at: Timestamp

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                **{
                    name: record[name]
                    for name in cls.model_fields
                    if name not in {"at", "receipt_sha256"} and name in record
                },
                "at": propagate_timestamp(record, "at"),
                "receipt_sha256": sha256(dict(record)),
            }
        )


def select_ops_receipt(receipts: Sequence[OpsSentReceipt], *, clock_required: bool) -> OpsSentReceipt | None:
    if not receipts:
        return None
    if not clock_required and len({str(receipt.message_ref) for receipt in receipts}) == 1:
        return min(receipts, key=lambda receipt: receipt.receipt_sha256)
    timed: list[tuple[datetime, OpsSentReceipt]] = []
    for receipt in receipts:
        if receipt.at.value is None:
            raise ValueError("Ops parent receipt timing is unreported")
        timed.append((parse_iso_time(receipt.at.value), receipt))
    latest = max(instant for instant, _ in timed)
    candidates = [receipt for instant, receipt in timed if instant == latest]
    if len({str(receipt.message_ref) for receipt in candidates}) != 1:
        raise ValueError("Ops parent receipts have ambiguous same-time targets")
    return min(candidates, key=lambda receipt: receipt.receipt_sha256)


class DeliveryAttemptOrdinal(DomainModel):
    attempt: int = Field(ge=0)
    status: Literal["GENERATED", "FAILED", "SENT"] | None = None
    suppressed: bool | None = None
    terminal: bool | None = None

    @model_validator(mode="after")
    def declared_zero_phase(self) -> Self:
        if self.attempt == 0 and not (
            self.status == "GENERATED"
            or self.status == "FAILED"
            and (self.suppressed is True or self.terminal is True)
        ):
            raise ValueError("Zero delivery attempts require a recorded pre-send phase")
        return self

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({name: record[name] for name in cls.model_fields if name in record})


class OpsBackoffState(DomainModel):
    until: str

    @field_validator("until")
    @classmethod
    def actual_backoff_clock(cls, value: str) -> str:
        parse_iso_time(value)
        return value


class OpenDeliveryCall(DomainModel):
    call_id: DeliveryIdentity
    event_id: DeliveryIdentity
    token_id: DeliveryIdentity
    status: Literal["OPEN"]


class DeliveryDecisionPhase(DomainModel):
    tracking_ended: bool | None = None
    actionable: bool | None = None


class StartCallDecision(DomainModel):
    token_id: DeliveryIdentity
    action: Literal["START"]


class StartCallPrices(DomainModel):
    entry_ref_price: DeliveredPrice
    invalidation_price: DeliveredPrice

    @field_validator("entry_ref_price", "invalidation_price", mode="before")
    @classmethod
    def observed_price(cls, value: JsonValue) -> DeliveredPrice:
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise ValueError("Call activation requires a reported price")
        try:
            price = Decimal(str(value))
        except InvalidOperation:
            raise ValueError("Call activation requires a finite positive price") from None
        if not price.is_finite() or price <= 0:
            raise ValueError("Call activation requires a finite positive price")
        return value


class ExitCallDecision(DomainModel):
    token_id: DeliveryIdentity
    action: Literal["EXIT"]


class OtherCallDecision(DomainModel):
    token_id: DeliveryIdentity
    action: Literal["WATCH_TO_START", "ADD", "HOLD", "WAIT", "DONT_CHASE", "REDUCE"]


type CallActivationDecision = StartCallDecision | ExitCallDecision | OtherCallDecision


def call_activation_decision(record: Mapping[str, JsonValue]) -> CallActivationDecision | None:
    """Admit only facts consumed after the stored decision's inactive phase."""
    phase = DeliveryDecisionPhase.model_validate(
        {name: record[name] for name in ("tracking_ended", "actionable") if name in record}
    )
    if phase.tracking_ended is True or phase.actionable is False:
        return None
    fields = ("token_id", "action")
    if record.get("action") == "START":
        return StartCallDecision.model_validate({name: record[name] for name in fields if name in record})
    values = {name: record[name] for name in fields if name in record}
    if record.get("action") == "EXIT":
        return ExitCallDecision.model_validate(values)
    return OtherCallDecision.model_validate(values)
