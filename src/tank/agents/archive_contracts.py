"""Closed card and delivery facts consumed by terminal session archival."""

from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import Field, JsonValue, field_validator

from tank.contracts import DomainModel
from tank.util import parse_time


class ArchiveAlertIdentity(DomainModel):
    alert_id: str = Field(min_length=1, pattern=r"\S")


class ArchiveCardIdentity(ArchiveAlertIdentity):
    version: int = Field(ge=1)


class OrdinaryArchiveCard(ArchiveCardIdentity):
    kind: Literal["FULL", "PARTIAL", "UPDATE"]


class PreliminaryArchiveCard(ArchiveCardIdentity):
    kind: Literal["PRELIMINARY"]
    terminal_correction: bool


type ArchiveCard = OrdinaryArchiveCard | PreliminaryArchiveCard


class ArchiveFinalization(DomainModel):
    session_finalized_at: str | None

    @field_validator("session_finalized_at")
    @classmethod
    def observed_completion_clock(cls, value: str | None) -> str | None:
        if value is not None:
            parse_time(value)
        return value


class ArchiveDelivery(ArchiveCardIdentity):
    status: Literal["SENT"]


def archive_card(record: Mapping[str, JsonValue]) -> ArchiveCard:
    """Project actual stored card facts without requiring unrelated rendering fields."""
    identity = {
        "alert_id": record["alert_id"],
        "version": record["version"],
        "kind": record["kind"],
    }
    if record["kind"] == "PRELIMINARY":
        return PreliminaryArchiveCard.model_validate(
            {**identity, "terminal_correction": record["terminal_correction"]}
        )
    return OrdinaryArchiveCard.model_validate(identity)


def terminal_archive_card(cards: Sequence[ArchiveCard]) -> ArchiveCard | None:
    """Ordinary publication owns finalization once its chain exists."""
    ordinary = [card for card in cards if isinstance(card, OrdinaryArchiveCard)]
    chain: Sequence[ArchiveCard] = ordinary if ordinary else cards
    if not chain:
        return None
    latest = max(chain, key=lambda card: card.version)
    if isinstance(latest, OrdinaryArchiveCard):
        return latest if latest.kind == "UPDATE" else None
    return latest if latest.terminal_correction else None
