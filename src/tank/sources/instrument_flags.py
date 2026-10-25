"""Observed Coinbase restriction flags; omitted history never becomes current facts."""

from collections.abc import Mapping
from typing import Literal, Self

from pydantic import JsonValue

from tank.contracts import DomainModel

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

    @classmethod
    def from_snapshot(cls, record: Mapping[str, JsonValue]) -> Self:
        if "status_flags" not in record or record["status_flags"] is None:
            return cls()
        return cls.model_validate(record["status_flags"])

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
