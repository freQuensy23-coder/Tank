"""Typed instrument snapshot comparisons preserve observed and unknown restriction facts."""

import json
from collections.abc import Mapping, Sequence
from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.source_checkpoints import (
    CoinbaseInstrument,
    CoinbaseRestrictionFlags,
    InstrumentRecord,
    InstrumentSnapshot,
    VenueInstrument,
)
from tank.contracts.source_items import (
    DiscoveryObservation,
    InstrumentDiscovery,
    InstrumentDiscoveryEnvelope,
    SourceObservedClock,
)
from tank.contracts.sources import SourceFeed
from tank.util import sha256


class InstrumentPage(SourceObservedClock):
    payload: bytes


class InstrumentChange(DomainModel):
    symbol: str = Field(min_length=1)
    current: InstrumentRecord | None
    previous: InstrumentRecord | None

    @model_validator(mode="after")
    def has_observation(self) -> Self:
        if self.current is None and self.previous is None:
            raise ValueError("Instrument change requires an observed side")
        if any(
            record is not None and record.symbol != self.symbol for record in (self.current, self.previous)
        ):
            raise ValueError("Instrument change symbol conflicts with its observation")
        return self

    @property
    def kind(self) -> Literal["REMOVED", "ADDED", "STATE_UPDATE"]:
        return "REMOVED" if self.current is None else "ADDED" if self.previous is None else "STATE_UPDATE"

    def action(self) -> tuple[str, str | None] | tuple[str, str | None, str | None]:
        if self.previous is None:
            if self.current is None:
                raise ValueError("Added instrument requires its current observation")
            return ("ADDED", self.current.base)
        return (
            self.kind,
            self.previous.status,
            None if self.current is None else self.current.status,
        )


def instrument_wire(record: InstrumentRecord | None) -> dict[str, JsonValue] | None:
    if record is None:
        return None
    return record.model_dump(mode="json", exclude_unset=True, by_alias=True)


def evidence(record: InstrumentRecord | None) -> dict[str, JsonValue] | None:
    wire = None if record is None else record.model_dump(mode="json", exclude_unset=True, by_alias=False)
    if wire is not None and isinstance(record, CoinbaseInstrument) and record.status_flags is None:
        wire["status_flags"] = None
    return wire


def differs(current: InstrumentRecord | None, previous: InstrumentRecord | None) -> bool:
    if current is None or previous is None:
        return current != previous
    if (
        current.venue != previous.venue
        or current.acquisition_market_type != previous.acquisition_market_type
        or current.symbol != previous.symbol
        or current.status != previous.status
    ):
        return True
    optional_metadata = (
        (previous.base, current.base),
        (previous.quote, current.quote),
        (previous.contract_type, current.contract_type),
    )
    if any(
        before not in (None, "") and after not in (None, "") and before != after
        for before, after in optional_metadata
    ):
        return True
    if (
        isinstance(current, VenueInstrument)
        and isinstance(previous, VenueInstrument)
        and current.reported_contract is not None
        and previous.reported_contract is not None
        and current.reported_contract != previous.reported_contract
    ):
        return True
    if isinstance(current, CoinbaseInstrument) and isinstance(previous, CoinbaseInstrument):
        before_flags = CoinbaseRestrictionFlags() if previous.status_flags is None else previous.status_flags
        after_flags = CoinbaseRestrictionFlags() if current.status_flags is None else current.status_flags
        return bool(before_flags.known_changes(after_flags))
    return False


def instrument_changes(
    snapshot: InstrumentSnapshot,
    previous: InstrumentSnapshot | None,
    feed: SourceFeed,
    pages: Sequence[InstrumentPage],
    found_on: Mapping[str, int],
    completed_at: str,
) -> tuple[list[InstrumentDiscoveryEnvelope], str]:
    if feed.venue is None or feed.market_type is None:
        raise ValueError("Instrument comparison requires its declared venue and market type")
    completed_at = SourceObservedClock(observed_at=completed_at).observed_at
    for observed in (snapshot,) if previous is None else (snapshot, previous):
        if any(
            record.venue != feed.venue or record.acquisition_market_type != feed.market_type
            for record in observed.root.values()
        ):
            raise ValueError("Instrument snapshot conflicts with the actual feed owner")
    digest = sha256(snapshot.storage_record())
    if previous is None:
        return [], digest
    groups: dict[tuple[str, str | None] | tuple[str, str | None, str | None], list[InstrumentChange]] = {}
    for symbol in sorted(snapshot.root.keys() | previous.root.keys()):
        current, old = snapshot.root.get(symbol), previous.root.get(symbol)
        if differs(current, old):
            change = InstrumentChange(symbol=symbol, current=current, previous=old)
            groups.setdefault(change.action(), []).append(change)
    if groups and not pages:
        raise ValueError("Instrument changes require their actual response evidence")
    rows: list[InstrumentDiscoveryEnvelope] = []
    for action, group in groups.items():
        on_pages = {found_on[change.symbol] for change in group if change.symbol in found_on}
        if len(on_pages) == 1 and all(change.symbol in found_on for change in group):
            page = pages[on_pages.pop()]
            payload, observed_at = page.payload, page.observed_at
        else:
            payload, observed_at = b"\n".join(page.payload for page in pages), completed_at
        if len(group) == 1:
            change = group[0]
            external_id = (
                f"{feed.venue}|{feed.market_type}|{change.symbol}|{sha256(instrument_wire(change.current))}"
            )
            title = f"{change.symbol} instrument change"
            text = json.dumps(
                {"before": evidence(change.previous), "after": evidence(change.current)}, sort_keys=True
            )
        else:
            group_wire: list[JsonValue] = [
                [change.symbol, instrument_wire(change.current), instrument_wire(change.previous)]
                for change in group
            ]
            external_id = f"{feed.venue}|{feed.market_type}|changes|{sha256(group_wire)}"
            what = (
                f"{action[1]} instruments ADDED"
                if len(action) == 2
                else f"instruments {action[0]} {action[1]} → {action[2]}"
            )
            venue_name = "OKX" if feed.venue == "okx" else feed.venue.capitalize()
            title = f"{venue_name} instrument catalog: {len(group)} {what}"
            text = json.dumps(
                {
                    "changes": [
                        {
                            "symbol": change.symbol,
                            "change": change.kind,
                            "before": evidence(change.previous),
                            "after": evidence(change.current),
                        }
                        for change in group
                    ]
                },
                sort_keys=True,
            )
        rows.append(
            InstrumentDiscoveryEnvelope(
                discovery=InstrumentDiscovery(
                    source_id="S14",
                    url=feed.url,
                    published_at=None,
                    published_prec="COLLECTION_ONLY",
                    published_at_precision="COLLECTION_ONLY",
                    published_prec_reason="INCOMPLETE",
                    published_at_missing_reason="INCOMPLETE",
                    change=group[0].kind,
                    external_id=external_id,
                    title=title,
                    text=text,
                ),
                observation=DiscoveryObservation(observed_at=observed_at),
                payload=payload,
            )
        )
    return rows, digest
