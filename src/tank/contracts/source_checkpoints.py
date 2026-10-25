"""Operation-owned source checkpoint and instrument observation records."""

from typing import Literal, Self

from pydantic import ConfigDict, Field, JsonValue, RootModel, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.repository import Collection
from tank.contracts.sources import SourceFeed
from tank.util import parse_iso_time, sha256


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


class InstrumentObservation(DomainModel):
    market_type: Literal["SPOT", "PERP_LINEAR"]
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
            symbol: record.model_dump(mode="json", exclude_unset=True) for symbol, record in self.root.items()
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
