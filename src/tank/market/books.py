"""Stateful level 2 books, continuity and venue checksum verification."""

import time
import zlib
from bisect import bisect_left, bisect_right, insort
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Literal, Self

from pydantic import JsonValue, TypeAdapter

from tank.market.book_contracts import BookInstrument, BookLevel, BookSummary, Number
from tank.market.normalize import SemanticError, precise, timestamp_fields
from tank.time_precision import collection_clock
from tank.util import iso

type LevelWire = Sequence[Number] | Mapping[str, Number]
type TimeInput = datetime | str | int | float | Decimal
type Side = Literal["bid", "ask"]

_JSON = TypeAdapter(dict[str, JsonValue])
_SIDES: tuple[Side, Side] = ("bid", "ask")


class OrderBook:
    def __init__(self, instrument: BookInstrument | Mapping[str, JsonValue | Decimal]) -> None:
        self.instrument = (
            instrument if isinstance(instrument, BookInstrument) else BookInstrument.from_wire(instrument)
        )
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.sequence: int | str | None = None
        self.integrity_ok = False
        self.gap_reason: str | None = "NO_SNAPSHOT"
        self.exchange_ts: str | None = None
        self.exchange_ts_prec: str | None = None

        self.verified = True
        self._next_verify = 0.0
        self.integrity_method = (
            "SEQUENCE"
            if self.instrument.venue_key.startswith(("binance", "okx"))
            else "CRC32"
            if self.instrument.venue_key.startswith("kraken")
            else "DOCUMENTED_LEVEL2_DELIVERY"
        )
        self._strings: dict[Side, dict[Decimal, tuple[str, str]]] = {"bid": {}, "ask": {}}

        self._prices: dict[Side, list[Decimal]] = {"bid": [], "ask": []}

    def _levels(self, side: Side, levels: Sequence[LevelWire], *, snapshot: bool = False) -> None:
        book = self.bids if side == "bid" else self.asks
        strings = self._strings[side]
        prices = self._prices[side]
        if snapshot:
            book.clear()
            strings.clear()
            prices.clear()
        for level in levels:
            try:
                normalized = (
                    BookLevel.from_wire(level["price"], level["qty"])
                    if isinstance(level, Mapping)
                    else BookLevel.from_wire(level[0], level[1])
                )
            except (ValueError, IndexError, KeyError) as exc:
                raise SemanticError("invalid book level") from exc
            price, size = normalized.price, normalized.quantity
            p, q = normalized.price_spelling, normalized.quantity_spelling
            if not size:
                if book.pop(price, None) is not None:
                    del prices[bisect_left(prices, price)]
                strings.pop(price, None)
            else:
                if price not in book and not snapshot:
                    insort(prices, price)
                book[price] = size
                strings[price] = (p, q)
        if snapshot:
            prices[:] = sorted(book)

    def snapshot(
        self,
        bids: Sequence[LevelWire],
        asks: Sequence[LevelWire],
        *,
        sequence: int | str | None = None,
        ts: TimeInput | None = None,
        checksum: int | str | None = None,
        ts_prec: str = "EXACT",
    ) -> None:
        self._levels("bid", bids, snapshot=True)
        self._levels("ask", asks, snapshot=True)
        self.sequence = sequence
        self.exchange_ts = iso(ts)
        self.exchange_ts_prec = ts_prec if ts is not None else "COLLECTION_ONLY"
        self.integrity_ok = bool(self.bids and self.asks)
        self.gap_reason = None if self.integrity_ok else "EMPTY_BOOK"

        self.pending_snapshot = {
            "instrument_id": self.instrument.instrument_id,
            "market_type": self.instrument.market_type,
            "venue": self.instrument.venue,
            "venue_symbol": self.instrument.venue_symbol,
            **timestamp_fields("exchange_ts", self.exchange_ts, self.exchange_ts_prec),
            **collection_clock().fields("received_ts"),
            "bids": bids,
            "asks": asks,
            "sequence": sequence,
            "checksum": checksum,
            "quantity_basis": "PROVIDER_RAW_LEVEL_SIZE",
            "contract_multiplier": self.instrument.contract_multiplier,
        }
        if checksum is not None:
            if self.instrument.venue_key.startswith("okx") and int(checksum) == 0:
                self.integrity_method = "SEQ_ID_PREV_SEQ_ID_CHECKSUM_DEPRECATED_20260623"
            else:
                self.verify(checksum)

    VERIFY_S = 1.0

    def update(
        self,
        bids: Sequence[LevelWire],
        asks: Sequence[LevelWire],
        *,
        sequence: int | str | None = None,
        previous: int | str | None = None,
        first: int | str | None = None,
        ts: TimeInput | None = None,
        checksum: int | str | None = None,
        verify: bool = False,
    ) -> bool:
        if self.sequence is None or not self.integrity_ok:
            raise SemanticError("book requires fresh snapshot")
        key = self.instrument.venue_key
        if key.startswith("okx"):
            if sequence == self.sequence and previous == self.sequence:
                return False
        elif sequence is not None and int(sequence) <= int(self.sequence):
            return False
        if key.startswith("binance"):
            valid = (
                (int(previous) == int(self.sequence))
                if previous is not None
                else (
                    first is not None
                    and sequence is not None
                    and int(first) <= int(self.sequence) + 1 <= int(sequence)
                )
            )
        elif key.startswith("okx"):
            valid = previous is not None and int(previous) == int(self.sequence)
        else:
            valid = True
        if not valid:
            self.integrity_ok = False
            self.gap_reason = "SEQUENCE_GAP"
            raise SemanticError("book sequence gap")
        self._levels("bid", bids)
        self._levels("ask", asks)
        if sequence is not None:
            self.sequence = sequence
        self.exchange_ts = iso(ts)
        self.exchange_ts_prec = "EXACT" if ts is not None else "COLLECTION_ONLY"
        if key.startswith("kraken"):
            for side in _SIDES:
                book = self.bids if side == "bid" else self.asks
                excess = len(book) - 1000
                if excess <= 0:
                    continue

                prices = self._prices[side]
                far = prices[:excess] if side == "bid" else prices[-excess:]
                for price in far:
                    book.pop(price)
                    self._strings[side].pop(price)
                if side == "bid":
                    del prices[:excess]
                else:
                    del prices[-excess:]
        if checksum is not None:
            if self.instrument.venue_key.startswith("okx") and int(checksum) == 0:
                self.integrity_method = "SEQ_ID_PREV_SEQ_ID_CHECKSUM_DEPRECATED_20260623"
                self.verified = True
            elif verify or time.monotonic() >= self._next_verify:
                self.verify(checksum)
                self.verified = True
                self._next_verify = time.monotonic() + self.VERIFY_S
            else:
                self.verified = False
        return True

    def verify(self, expected: int | str) -> bool:
        key = self.instrument.venue_key
        bids = self._prices["bid"][-25:][::-1]
        asks = self._prices["ask"][:25]
        if key.startswith("okx"):
            parts: list[str] = []
            for i in range(25):
                if i < len(bids):
                    parts.extend(self._strings["bid"][bids[i]])
                if i < len(asks):
                    parts.extend(self._strings["ask"][asks[i]])
            crc = zlib.crc32(":".join(parts).encode())
            crc = crc if crc < 2**31 else crc - 2**32
        elif key.startswith("kraken"):
            parts = []
            for side in reversed(_SIDES):
                prices = asks[:10] if side == "ask" else bids[:10]
                for p in prices:
                    for value in self._strings[side][p]:
                        parts.append(value.replace(".", "").lstrip("0"))
            crc = zlib.crc32("".join(parts).encode())
        else:
            raise SemanticError("checksum not defined for venue")
        self.integrity_ok = crc == int(expected)
        self.gap_reason = None if self.integrity_ok else "CHECKSUM_MISMATCH"
        if not self.integrity_ok:
            raise SemanticError("book checksum mismatch")
        return True

    def frozen(self) -> Self:
        """A private copy of the current state, safe to summarize in another thread."""
        copy = object.__new__(type(self))
        copy.__dict__.update(self.__dict__)
        copy.bids, copy.asks = dict(self.bids), dict(self.asks)
        copy._prices = {side: list(prices) for side, prices in self._prices.items()}
        copy._strings = {side: dict(strings) for side, strings in self._strings.items()}
        return copy

    @precise
    def summary(self, *, received_ts: TimeInput | None = None) -> dict[str, JsonValue]:
        if not self.bids or not self.asks:
            raise SemanticError("empty order book")
        bid, ask = self._prices["bid"][-1], self._prices["ask"][0]
        if ask <= bid:
            self.integrity_ok = False
            self.gap_reason = "CROSSED_BOOK"
        mid = (ask + bid) / 2
        multiplier = (
            self.instrument.contract_multiplier
            if self.instrument.venue_key == "okx.swap" and self.instrument.contract_multiplier is not None
            else Decimal(1)
        )
        if self.instrument.venue_key == "okx.swap" and self.instrument.contract_multiplier is None:
            raise SemanticError("OKX multiplier absent")
        fx = (
            Decimal(1)
            if self.instrument.quote_asset == "USD"
            else self.instrument.quote_usd_rate
            if self.instrument.quote_usd_rate is not None
            else None
        )
        depth: dict[Side, dict[str, dict[str, Decimal | None] | None]] = {}
        ranges: dict[Side, Decimal] = {}
        levels: dict[Side, list[tuple[Decimal, Decimal]]] = {}
        bands = (("p0_5", Decimal(".5")), ("p1", Decimal(1)), ("p2", Decimal(2)))
        for side in _SIDES:
            book = self.bids if side == "bid" else self.asks
            prices = self._prices[side]
            far = prices[0] if side == "bid" else prices[-1]
            covered = abs(far / mid - 1) * 100
            ranges[side] = covered

            if side == "bid":
                near = prices[bisect_left(prices, mid * (1 - Decimal(".02"))) :][::-1]
            else:
                near = prices[: bisect_right(prices, mid * (1 + Decimal(".02")))]
            levels[side] = [(p, book[p] * multiplier) for p in near]
            depth[side] = {}
            for name, pct in bands:
                if covered < pct:
                    depth[side][name] = None
                    continue
                inside = [(p, q) for p, q in levels[side] if abs(p / mid - 1) * 100 <= pct]
                depth[side][name] = {
                    "native": sum((q for p, q in inside), Decimal(0)),
                    "usd": sum((p * q * fx for p, q in inside), Decimal(0)) if fx is not None else None,
                }
        row = {
            "instrument_id": self.instrument.instrument_id,
            "token_id": self.instrument.token_id,
            **timestamp_fields("exchange_ts", self.exchange_ts, self.exchange_ts_prec),
            **collection_clock(received_ts).fields("received_ts"),
            "mid": mid,
            "best_bid": bid,
            "best_ask": ask,
            "spread_bps": (ask - bid) / mid * 10000,
            "depth": depth,
            "raw_levels_range_pct": "2",
            "raw_levels_scope": "WITHIN_TWO_PERCENT_OF_MID",
            "quote_asset": self.instrument.quote_asset,
            "quote_usd_rate": fx,
            "range_covered_pct": ranges,
            "levels_received": len(self.bids) + len(self.asks),
            "integrity_ok": self.integrity_ok,
            "integrity_method": self.integrity_method,
            "gap_reason": self.gap_reason,
        }
        row["bids"] = [[str(p), str(q)] for p, q in levels["bid"]]
        row["asks"] = [[str(p), str(q)] for p, q in levels["ask"]]
        return _JSON.validate_python(BookSummary.model_validate(row).model_dump(mode="json"))
