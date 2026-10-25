"""Operation-owned source checkpoint and instrument observation records."""

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, ConfigDict, Field, JsonValue, RootModel, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.repository import Collection
from tank.contracts.scalars import require_integer
from tank.contracts.source_items import (
    CapturedItem,
    PendingSourceItem,
    SourceContextId,
    SourceItem,
    SourceLifecycle,
)
from tank.contracts.sources import FeedKind, SourceFeed
from tank.util import iso, parse_iso_time, sha256


class ReadCounter(DomainModel):
    ok: int = Field(ge=0)
    failed: int = Field(ge=0)


type RestrictionFlag = Literal[
    "trading_disabled",
    "cancel_only",
    "post_only",
    "limit_only",
    "auction_mode",
]


class CoinbaseRestrictionFlags(DomainModel):
    trading_disabled: bool | None = None
    cancel_only: bool | None = None
    post_only: bool | None = None
    limit_only: bool | None = None
    auction_mode: bool | None = None

    def known_changes(self, current: "CoinbaseRestrictionFlags") -> tuple[RestrictionFlag, ...]:
        observations: tuple[tuple[RestrictionFlag, bool | None, bool | None], ...] = (
            ("trading_disabled", self.trading_disabled, current.trading_disabled),
            ("cancel_only", self.cancel_only, current.cancel_only),
            ("post_only", self.post_only, current.post_only),
            ("limit_only", self.limit_only, current.limit_only),
            ("auction_mode", self.auction_mode, current.auction_mode),
        )
        return tuple(
            name
            for name, previous, observed in observations
            if previous is not None and observed is not None and previous != observed
        )


type ReportedContractKind = Literal["PERPETUAL", "DATED_FUTURE"]
type BinanceContractType = Literal[
    "PERPETUAL",
    "CURRENT_MONTH",
    "NEXT_MONTH",
    "CURRENT_QUARTER",
    "NEXT_QUARTER",
    "PERPETUAL_DELIVERING",
]
type BybitContractType = Literal[
    "InversePerpetual",
    "LinearPerpetual",
    "LinearFutures",
    "InverseFutures",
]


def binance_contract_kind(code: BinanceContractType) -> ReportedContractKind:
    match code:
        case "PERPETUAL" | "PERPETUAL_DELIVERING":
            return "PERPETUAL"
        case "CURRENT_MONTH" | "NEXT_MONTH" | "CURRENT_QUARTER" | "NEXT_QUARTER":
            return "DATED_FUTURE"
    raise ValueError("Unsupported Binance reported contract type")


def bybit_contract_kind(
    code: BybitContractType,
) -> tuple[ReportedContractKind, Literal["LINEAR", "INVERSE"]]:
    match code:
        case "LinearPerpetual":
            return "PERPETUAL", "LINEAR"
        case "InversePerpetual":
            return "PERPETUAL", "INVERSE"
        case "LinearFutures":
            return "DATED_FUTURE", "LINEAR"
        case "InverseFutures":
            return "DATED_FUTURE", "INVERSE"
    raise ValueError("Unsupported Bybit reported contract type")


class BinanceContractObservation(DomainModel):
    provider: Literal["binance"]
    kind: ReportedContractKind
    contract_type: BinanceContractType
    margin_asset: str = Field(min_length=1, pattern=r"^\S+$")

    @model_validator(mode="after")
    def actual_contract_family(self) -> Self:
        if self.kind != binance_contract_kind(self.contract_type):
            raise ValueError("Binance reported contract type conflicts with its observed family")
        return self


class BybitContractObservation(DomainModel):
    provider: Literal["bybit"]
    kind: ReportedContractKind
    contract_type: BybitContractType
    settlement_mode: Literal["LINEAR", "INVERSE"]
    settle_coin: str = Field(min_length=1, pattern=r"^\S+$")

    @model_validator(mode="after")
    def actual_contract_family(self) -> Self:
        kind, mode = bybit_contract_kind(self.contract_type)
        if self.kind != kind or self.settlement_mode != mode:
            raise ValueError("Bybit reported contract type conflicts with its observed family/mode")
        return self


type ReportedDerivativeContract = Annotated[
    BinanceContractObservation | BybitContractObservation,
    Field(discriminator="provider"),
]


class InstrumentObservation(DomainModel):
    model_config = ConfigDict(validate_by_name=True, serialize_by_alias=True)

    acquisition_market_type: Literal["SPOT", "PERP_LINEAR"] = Field(
        validation_alias="market_type", serialization_alias="market_type"
    )
    symbol: str = Field(min_length=1)
    base: str | None
    quote: str | None
    status: str = Field(min_length=1)
    contract_type: str | None


class CoinbaseInstrument(InstrumentObservation):
    venue: Literal["coinbase"]
    status_flags: CoinbaseRestrictionFlags | None = None


class VenueInstrument(InstrumentObservation):
    venue: Literal["binance", "kraken", "okx", "bybit", "hyperliquid"]
    reported_contract: ReportedDerivativeContract | None = None

    def with_reported_contract(self, observation: ReportedDerivativeContract) -> Self:
        return type(self).model_validate(self.model_copy(update={"reported_contract": observation}))

    @model_validator(mode="after")
    def reported_contract_owner(self) -> Self:
        if self.reported_contract is not None and (
            self.venue != self.reported_contract.provider
            or self.contract_type != self.reported_contract.contract_type
            or self.acquisition_market_type != "PERP_LINEAR"
        ):
            raise ValueError("Reported contract observation conflicts with its capture owner/code")
        return self


type InstrumentRecord = CoinbaseInstrument | VenueInstrument


class InstrumentSnapshot(RootModel[dict[str, InstrumentRecord]]):
    model_config = ConfigDict(
        strict=True, frozen=True, hide_input_in_errors=True, revalidate_instances="always"
    )

    @model_validator(mode="after")
    def declared_symbol_keys(self) -> Self:
        if any(symbol != record.symbol for symbol, record in self.root.items()):
            raise ValueError("Instrument snapshot key conflicts with its declared symbol")
        return self

    def storage_record(self) -> dict[str, JsonValue]:
        return {
            symbol: record.model_dump(mode="json", exclude_unset=True, by_alias=True)
            for symbol, record in self.root.items()
        }


class InstrumentAcquisitionScope(DomainModel):
    venue: Literal["binance", "coinbase", "kraken", "okx", "bybit", "hyperliquid"]
    market_type: Literal["SPOT", "PERP_LINEAR"]
    method: Literal["GET", "POST"]
    url: str = Field(min_length=1)
    body_sha256: str | None = Field(pattern=r"^[a-f0-9]{64}$")
    form_sha256: str | None = Field(pattern=r"^[a-f0-9]{64}$")
    headers_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @classmethod
    def from_feed(cls, feed: SourceFeed) -> "InstrumentAcquisitionScope":
        if feed.venue is None or feed.market_type is None:
            raise ValueError("Instrument acquisition requires a venue and market type")
        return cls(
            venue=feed.venue,
            market_type=feed.market_type,
            method=feed.method,
            url=feed.url,
            body_sha256=None if feed.body is None else sha256(feed.body),
            form_sha256=None if feed.form is None else sha256(feed.form),
            headers_sha256=sha256(feed.headers),
        )

    @property
    def digest(self) -> str:
        return sha256(self.model_dump(mode="json"))


class ScopedInstrumentBaseline(DomainModel):
    source_id: Literal["S14"]
    acquisition_scope: InstrumentAcquisitionScope
    instruments: InstrumentSnapshot
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    config_version: str = Field(min_length=1)
    code_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def actual_observations(self) -> Self:
        ResolvedVersionIdentity(config_version=self.config_version, code_version=self.code_version)
        if self.sha256 != sha256(self.instruments.storage_record()):
            raise ValueError("Instrument baseline digest does not match its observations")
        return self

    @classmethod
    def from_document(cls, value: dict[str, JsonValue]) -> "ScopedInstrumentBaseline":
        return cls.model_validate({name: value[name] for name in cls.model_fields if name in value})


class InstrumentBaselineArchive(DomainModel):
    checkpoint_kind: Literal["INSTRUMENT_BASELINE_ARCHIVE"]
    source_id: Literal["S14"]
    original_checkpoint_key: str = Field(min_length=1)
    original_record: dict[str, JsonValue]
    original_record_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    original_scope: InstrumentAcquisitionScope | None
    scope_declaration: Literal["RECORDED", "NOT_RECORDED"]
    captured_at: str
    captured_at_prec: Literal["COLLECTION_ONLY"]
    capture_versions: ResolvedVersionIdentity

    @model_validator(mode="after")
    def preserve_actual_scope_declaration(self) -> Self:
        parse_iso_time(self.captured_at)
        if self.original_record_sha256 != sha256(self.original_record):
            raise ValueError("Archived baseline digest does not match its original record")
        if "acquisition_scope" in self.original_record:
            declared = InstrumentAcquisitionScope.model_validate(self.original_record["acquisition_scope"])
            if self.scope_declaration != "RECORDED" or self.original_scope != declared:
                raise ValueError("Archived baseline scope conflicts with its actual declaration")
        elif self.scope_declaration != "NOT_RECORDED" or self.original_scope is not None:
            raise ValueError("Historical baseline has no declared acquisition scope")
        return self

    @property
    def storage_key(self) -> str:
        return "instrument-baseline-archive:" + sha256(
            {
                "checkpoint_key": self.original_checkpoint_key,
                "record_sha256": self.original_record_sha256,
            }
        )

    @classmethod
    def capture(
        cls,
        checkpoint_key: str,
        record: dict[str, JsonValue],
        *,
        captured_at: str,
        versions: ResolvedVersionIdentity,
    ) -> "InstrumentBaselineArchive":
        known = "acquisition_scope" in record
        return cls(
            checkpoint_kind="INSTRUMENT_BASELINE_ARCHIVE",
            source_id="S14",
            original_checkpoint_key=checkpoint_key,
            original_record=record,
            original_record_sha256=sha256(record),
            original_scope=InstrumentAcquisitionScope.model_validate(record["acquisition_scope"])
            if known
            else None,
            scope_declaration="RECORDED" if known else "NOT_RECORDED",
            captured_at=captured_at,
            captured_at_prec="COLLECTION_ONLY",
            capture_versions=versions,
        )


INSTRUMENT_BASELINE_ARCHIVES = Collection("ops.source_checkpoints", InstrumentBaselineArchive)


class InstrumentSnapshotUpdate(DomainModel):
    source_id: Literal["S14"]
    checkpoint_key: str = Field(min_length=1)
    snapshot: InstrumentSnapshot
    acquisition_scope: InstrumentAcquisitionScope
    archives: list[InstrumentBaselineArchive]
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    versions: ResolvedVersionIdentity

    @model_validator(mode="after")
    def actual_snapshot_digest(self) -> Self:
        if sha256(self.snapshot.storage_record()) != self.sha256:
            raise ValueError("Prepared instrument snapshot digest does not match its observations")
        if any(
            isinstance(record, VenueInstrument)
            and record.venue in {"binance", "bybit"}
            and record.acquisition_market_type == "PERP_LINEAR"
            and record.reported_contract is None
            for record in self.snapshot.root.values()
        ):
            raise ValueError("New derivative capture requires reported contract observations")
        return self

    def storage_record(self) -> dict[str, JsonValue]:
        return {
            "source_id": self.source_id,
            "sha256": self.sha256,
            "instruments": self.snapshot.storage_record(),
            "acquisition_scope": self.acquisition_scope.model_dump(mode="json"),
            **self.versions.model_dump(mode="json"),
        }

    @property
    def scoped_checkpoint_key(self) -> str:
        return self.checkpoint_key + ":scope:" + self.acquisition_scope.digest


class NoCommittedCoverage(DomainModel):
    pass


class HttpCoverage(DomainModel):
    etag: str | None
    last_modified: str | None
    discovery_checkpoint: str | None = None


class GovernanceCoverage(DomainModel):
    pending_proposals: list[str]
    etag: str | None = None
    last_modified: str | None = None
    discovery_checkpoint: str | None = None


class InstrumentCoverage(HttpCoverage):
    instrument_snapshot_schema_version: Annotated[Literal[2], BeforeValidator(require_integer)]
    cache_scope: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")


class CapturedInstrumentCoverage(DomainModel):
    instruments: InstrumentSnapshot
    etag: str | None = None
    last_modified: str | None = None
    instrument_snapshot_schema_version: Annotated[Literal[1, 2], BeforeValidator(require_integer)] | None = (
        None
    )


class SocialCoverage(DomainModel):
    account_cursors: dict[str, str]


type PendingCoverage = (
    NoCommittedCoverage
    | HttpCoverage
    | GovernanceCoverage
    | InstrumentCoverage
    | CapturedInstrumentCoverage
    | SocialCoverage
)


type DeclarationStatus = Literal["DECLARED", "ABSENT", "INVALID"]
type RowDeclarationStatus = DeclarationStatus | Literal["NOT_APPLICABLE"]


def observed_identifier(
    wire: Mapping[str, JsonValue], field: Literal["source_id", "batch_id", "item_id"]
) -> tuple[str | None, DeclarationStatus]:
    if field not in wire:
        return None, "ABSENT"
    value = wire[field]
    if not isinstance(value, str) or not value.strip() or any(character.isspace() for character in value):
        return None, "INVALID"
    return value, "DECLARED"


def observed_external_id(wire: JsonValue) -> tuple[str | None, DeclarationStatus]:
    if not isinstance(wire, dict) or "external_id" not in wire:
        return None, "ABSENT"
    value = wire["external_id"]
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).strip():
        return None, "INVALID"
    return str(value), "DECLARED"


class QuarantineOrigin(DomainModel):
    operation_source_id: SourceContextId
    feed_key: str = Field(min_length=1)
    batch_position: int = Field(ge=0)
    declared_checkpoint_source_id: SourceContextId | None
    checkpoint_source_declaration: DeclarationStatus
    declared_batch_source_id: SourceContextId | None
    batch_source_declaration: DeclarationStatus
    declared_batch_id: SourceContextId | None
    batch_declaration: DeclarationStatus

    @model_validator(mode="after")
    def consistent_declarations(self) -> Self:
        for value, declaration in (
            (self.declared_checkpoint_source_id, self.checkpoint_source_declaration),
            (self.declared_batch_source_id, self.batch_source_declaration),
            (self.declared_batch_id, self.batch_declaration),
        ):
            if (declaration == "DECLARED") != (value is not None and bool(value.strip())):
                raise ValueError("Quarantine declaration value conflicts with its evidence status")
        return self


def quarantine_identity(origin: QuarantineOrigin, raw_sha256: str, versions: ResolvedVersionIdentity) -> str:
    wire: dict[str, JsonValue] = {
        "origin": origin.model_dump(mode="json"),
        "raw_sha256": raw_sha256,
        "detected_versions": versions.model_dump(mode="json"),
    }
    return sha256(wire)


class PendingRecovery(DomainModel):
    acquisition_batch_id: SourceContextId
    disposition: Literal["NEW_CAPTURED_OBSERVATION"]
    item_id: SourceContextId
    source_id: SourceContextId
    external_id: str = Field(min_length=1)
    feed: str = Field(min_length=1)
    observed_at: str
    raw_ref: str = Field(min_length=1)
    raw_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    linked_at: str
    linked_at_prec: Literal["COLLECTION_ONLY"]
    linked_at_prec_reason: None
    discovery_versions: ResolvedVersionIdentity
    link_versions: ResolvedVersionIdentity

    @model_validator(mode="after")
    def actual_clocks(self) -> Self:
        observed, linked = parse_iso_time(self.observed_at), parse_iso_time(self.linked_at)
        if linked < observed:
            raise ValueError("Recovery linkage cannot precede its actual observation")
        return self


class PendingQuarantineIssue(DomainModel):
    row_position: int | None = Field(ge=0)
    reason: Literal[
        "MISSING_BATCH_METADATA",
        "INVALID_BATCH_METADATA",
        "INVALID_PENDING_ROW",
        "MISSING_PREASSIGNED_ITEM_ID",
    ]
    validation_errors: int = Field(ge=1)
    declared_source_id: SourceContextId | None
    source_declaration: RowDeclarationStatus
    declared_item_id: SourceContextId | None
    item_declaration: RowDeclarationStatus
    declared_external_id: str | None
    external_declaration: RowDeclarationStatus
    recovery: PendingRecovery | None = None

    @model_validator(mode="after")
    def actual_row_declarations(self) -> Self:
        for value, status in (
            (self.declared_source_id, self.source_declaration),
            (self.declared_item_id, self.item_declaration),
            (self.declared_external_id, self.external_declaration),
        ):
            if self.row_position is None:
                if status != "NOT_APPLICABLE" or value is not None:
                    raise ValueError("Whole-batch quarantine cannot invent a selected row identity")
            elif status == "NOT_APPLICABLE" or (status == "DECLARED") != (
                value is not None and bool(value.strip())
            ):
                raise ValueError("Pending row identity conflicts with its actual declaration")
        return self


class PendingBatchQuarantine(DomainModel):
    evidence_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    origin: QuarantineOrigin
    raw_batch: JsonValue
    detected_versions: ResolvedVersionIdentity
    raw_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    quarantined_at: str
    quarantined_at_prec: Literal["COLLECTION_ONLY"]
    quarantined_at_prec_reason: None
    issues: list[PendingQuarantineIssue] = Field(min_length=1)

    @model_validator(mode="after")
    def preserved_evidence(self) -> Self:
        if sha256(self.raw_batch) != self.raw_sha256:
            raise ValueError("Quarantine raw evidence digest does not match its captured record")
        if quarantine_identity(self.origin, self.raw_sha256, self.detected_versions) != self.evidence_id:
            raise ValueError("Quarantine evidence identity does not match its actual origin")
        batch_wire = self.raw_batch if isinstance(self.raw_batch, dict) else {}
        if observed_identifier(batch_wire, "batch_id") != (
            self.origin.declared_batch_id,
            self.origin.batch_declaration,
        ) or observed_identifier(batch_wire, "source_id") != (
            self.origin.declared_batch_source_id,
            self.origin.batch_source_declaration,
        ):
            raise ValueError("Quarantine origin contradicts its preserved batch declaration")
        raw_rows = batch_wire.get("rows")
        for issue in self.issues:
            if issue.row_position is None:
                continue
            if not isinstance(raw_rows, list) or issue.row_position >= len(raw_rows):
                raise ValueError("Quarantine selected row does not exist in preserved evidence")
            raw_row = raw_rows[issue.row_position]
            row_wire = raw_row if isinstance(raw_row, dict) else {}
            if (
                observed_identifier(row_wire, "source_id")
                != (issue.declared_source_id, issue.source_declaration)
                or observed_identifier(row_wire, "item_id")
                != (issue.declared_item_id, issue.item_declaration)
                or observed_external_id(raw_row) != (issue.declared_external_id, issue.external_declaration)
            ):
                raise ValueError("Quarantine row declaration contradicts its preserved evidence")
            if issue.reason == "MISSING_PREASSIGNED_ITEM_ID" and (
                not isinstance(raw_row, dict) or "item_id" in raw_row
            ):
                raise ValueError("Missing-identity quarantine requires the actual absent field")
        instant = parse_iso_time(self.quarantined_at)
        for issue in self.issues:
            if issue.recovery is not None:
                recovery = issue.recovery
                if (
                    recovery.source_id != self.origin.operation_source_id
                    or recovery.acquisition_batch_id == self.origin.declared_batch_id
                    or (
                        issue.declared_source_id is not None
                        and issue.declared_source_id != recovery.source_id
                    )
                    or (issue.declared_item_id is not None and issue.declared_item_id == recovery.item_id)
                    or issue.declared_external_id is None
                    or recovery.external_id != issue.declared_external_id
                    or parse_iso_time(recovery.observed_at) < instant
                    or parse_iso_time(recovery.linked_at) < instant
                ):
                    raise ValueError(
                        "Pending recovery requires a new observation of the same actual source identity"
                    )
        return self

    @property
    def unresolved(self) -> bool:
        return any(issue.recovery is None for issue in self.issues)

    def link_new_capture(
        self,
        captured: CapturedItem,
        stored: SourceItem,
        *,
        linked_at: datetime,
        versions: ResolvedVersionIdentity,
        acquisition_batch_id: str,
    ) -> Self:
        material, receipt = captured.discovery, captured.capture
        if (
            not acquisition_batch_id.strip()
            or acquisition_batch_id == self.origin.declared_batch_id
            or material.source_id != self.origin.operation_source_id
            or self.origin.checkpoint_source_declaration == "INVALID"
            or self.origin.batch_source_declaration == "INVALID"
            or (
                self.origin.declared_checkpoint_source_id is not None
                and self.origin.declared_checkpoint_source_id != material.source_id
            )
            or (
                self.origin.declared_batch_source_id is not None
                and self.origin.declared_batch_source_id != material.source_id
            )
            or not isinstance(stored, SourceLifecycle)
            or stored.discovery_config_version != receipt.discovery_config_version
            or stored.discovery_code_version != receipt.discovery_code_version
            or stored.item_id != receipt.item_id
            or stored.source_id != material.source_id
            or stored.external_id != material.external_id
            or stored.raw_ref != receipt.raw_ref
            or stored.raw_sha256 != receipt.raw_sha256
            or stored.observed_at != receipt.observed_at
            or parse_iso_time(receipt.observed_at) < parse_iso_time(self.quarantined_at)
        ):
            return self
        eligible = [
            issue
            for issue in self.issues
            if issue.row_position is not None
            and issue.recovery is None
            and issue.declared_external_id == material.external_id
            and (issue.declared_source_id is None or issue.declared_source_id == material.source_id)
            and issue.declared_item_id != receipt.item_id
        ]
        if not eligible:
            return self
        recovery = PendingRecovery(
            acquisition_batch_id=acquisition_batch_id,
            disposition="NEW_CAPTURED_OBSERVATION",
            item_id=stored.item_id,
            source_id=material.source_id,
            external_id=material.external_id,
            feed=receipt.feed,
            observed_at=receipt.observed_at,
            raw_ref=receipt.raw_ref,
            raw_sha256=receipt.raw_sha256,
            linked_at=iso(linked_at),
            linked_at_prec="COLLECTION_ONLY",
            linked_at_prec_reason=None,
            discovery_versions=ResolvedVersionIdentity(
                config_version=receipt.discovery_config_version,
                code_version=receipt.discovery_code_version,
            ),
            link_versions=versions,
        )
        issues = [
            PendingQuarantineIssue.model_validate(issue.model_copy(update={"recovery": recovery}))
            if issue in eligible
            else issue
            for issue in self.issues
        ]
        return type(self).model_validate(self.model_copy(update={"issues": issues}))


class PendingBatchMetadata(DomainModel):
    batch_id: SourceContextId
    acquired_at: str
    acquired_at_precision: Literal["COLLECTION_ONLY"]
    acquired_at_prec: Literal["COLLECTION_ONLY"] | None = None
    acquired_at_prec_reason: None = None
    kind: FeedKind

    @model_validator(mode="after")
    def actual_acquisition_clock(self) -> Self:
        parse_iso_time(self.acquired_at)
        return self


class SourcePendingBatch(PendingBatchMetadata):
    source_id: SourceContextId
    rows: list[PendingSourceItem]
    coverage: PendingCoverage
    quarantines: list[PendingBatchQuarantine] = Field(default_factory=list)

    @model_validator(mode="after")
    def declared_batch_owner(self) -> Self:
        if any(row.discovery.source_id != self.source_id for row in self.rows):
            raise ValueError("Pending row conflicts with its actual source operation")
        if any(
            quarantine.origin.declared_batch_id != self.batch_id
            or quarantine.origin.operation_source_id != self.source_id
            for quarantine in self.quarantines
        ):
            raise ValueError("Pending batch conflicts with its preserved quarantine origin")
        if len({quarantine.evidence_id for quarantine in self.quarantines}) != len(self.quarantines):
            raise ValueError("Pending batch cannot repeat quarantine evidence identities")
        return self

    @property
    def may_commit_coverage(self) -> bool:
        return not self.rows and not any(quarantine.unresolved for quarantine in self.quarantines)

    def storage_record(self) -> dict[str, JsonValue]:
        type(self).model_validate(self)
        record = self.model_dump(mode="json", exclude={"rows"}, exclude_unset=True)
        record["rows"] = [row.storage_record() for row in self.rows]
        return record


class QuarantinedSourceBatch(DomainModel):
    quarantines: list[PendingBatchQuarantine] = Field(min_length=1)

    @property
    def may_commit_coverage(self) -> Literal[False]:
        return False

    def storage_record(self) -> dict[str, JsonValue]:
        type(self).model_validate(self)
        return self.model_dump(mode="json")


type PendingBatchRecord = SourcePendingBatch | QuarantinedSourceBatch


class MaterializedSourceObservation(DomainModel):
    captured: CapturedItem
    stored: SourceItem

    @model_validator(mode="after")
    def actual_materialization_identity(self) -> Self:
        material, receipt, stored = self.captured.discovery, self.captured.capture, self.stored
        if stored.item_id == receipt.item_id:
            if (
                stored.source_id != material.source_id
                or stored.external_id != material.external_id
                or stored.raw_ref != receipt.raw_ref
                or stored.raw_sha256 != receipt.raw_sha256
                or stored.observed_at != receipt.observed_at
            ):
                raise ValueError("Materialized item identity conflicts with its actual capture")
            if isinstance(stored, SourceLifecycle) and (
                (
                    stored.discovery_config_version is not None
                    and stored.discovery_config_version != receipt.discovery_config_version
                )
                or (
                    stored.discovery_code_version is not None
                    and stored.discovery_code_version != receipt.discovery_code_version
                )
            ):
                raise ValueError("Materialized item conflicts with its declared discovery versions")
        return self

    @property
    def deduplicated(self) -> bool:
        return self.captured.capture.item_id != self.stored.item_id


class CompletedPendingRow(DomainModel):
    original: PendingSourceItem
    observation: MaterializedSourceObservation

    @model_validator(mode="after")
    def actual_pending_identity(self) -> Self:
        before, captured = self.original, self.observation.captured
        if (
            before.item_id != captured.capture.item_id
            or before.feed != captured.capture.feed
            or before.observed_at != captured.capture.observed_at
            or before.discovery.source_id != captured.discovery.source_id
            or before.discovery.external_id != captured.discovery.external_id
            or before.discovery_config_version != captured.capture.discovery_config_version
            or before.discovery_code_version != captured.capture.discovery_code_version
        ):
            raise ValueError("Pending completion conflicts with its actual captured row identity")
        return self

    @property
    def fingerprint(self) -> str:
        return sha256(self.original.storage_record())


class SourceQuarantineArchive(DomainModel):
    checkpoint_kind: Literal["PENDING_QUARANTINE"]
    source_id: SourceContextId
    writer_versions: ResolvedVersionIdentity
    evidence: PendingBatchQuarantine
    updated_at: str
    updated_at_prec: Literal["COLLECTION_ONLY"]
    updated_at_prec_reason: None

    @model_validator(mode="after")
    def archive_owner_and_clock(self) -> Self:
        if self.source_id != self.evidence.origin.operation_source_id:
            raise ValueError("Quarantine archive conflicts with the actual operation owner")
        updated = parse_iso_time(self.updated_at)
        if updated < parse_iso_time(self.evidence.quarantined_at) or any(
            issue.recovery is not None and updated < parse_iso_time(issue.recovery.linked_at)
            for issue in self.evidence.issues
        ):
            raise ValueError("Quarantine archive clock precedes its captured evidence")
        return self


SOURCE_QUARANTINE_ARCHIVES = Collection("ops.source_checkpoints", SourceQuarantineArchive)
