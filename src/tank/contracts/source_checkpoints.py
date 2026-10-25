"""Operation-owned source checkpoint and instrument observation records."""

from typing import Literal, Self

from pydantic import ConfigDict, Field, JsonValue, RootModel, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.util import sha256


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


class InstrumentSnapshotUpdate(DomainModel):
    source_id: Literal["S14"]
    checkpoint_key: str = Field(min_length=1)
    snapshot: InstrumentSnapshot
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
            **self.versions.model_dump(mode="json"),
        }
