"""Stateful level 2 books, continuity and venue checksum verification."""

from __future__ import annotations

import time
import zlib
from bisect import bisect_left, bisect_right, insort
from decimal import Decimal

from tank.time_precision import collection_clock
from tank.util import iso, jsonable

from .normalize import SemanticError, decimal, precise, timestamp_fields, venue_key


class OrderBook:
    def __init__(self, instrument):
        self.instrument = instrument
        self.bids = {}
        self.asks = {}
        self.sequence = None
        self.integrity_ok = False
        self.gap_reason = "NO_SNAPSHOT"
        self.exchange_ts = None
        self.exchange_ts_prec = None
        # Whether the venue checksum was verified on the current book state. Updates
        # are verified at most once per VERIFY_S unless asked (a summary is due).
        self.verified = True
        self._next_verify = 0.0
        self.integrity_method = (
            "SEQUENCE"
            if venue_key(instrument).startswith(("binance", "okx"))
            else "CRC32"
            if venue_key(instrument).startswith("kraken")
            else "DOCUMENTED_LEVEL2_DELIVERY"
        )
        self._strings = {"bid": {}, "ask": {}}
        # Ascending prices per side, kept in step with bids/asks: best levels, the top
        # levels of a checksum, depth trimming and the 2% window are slices, not sorts.
        self._prices = {"bid": [], "ask": []}

    def _levels(self, side, levels, *, snapshot=False):
        book = self.bids if side == "bid" else self.asks
        strings = self._strings[side]
        prices = self._prices[side]
        if snapshot:
            book.clear()
            strings.clear()
            prices.clear()
        for level in levels:
            if isinstance(level, dict):
                p, q = (
                    format(level[k], "f") if isinstance(level[k], Decimal) else str(level[k])
                    for k in ("price", "qty")
                )
            else:
                p, q = str(level[0]), str(level[1])
            price, size = decimal(p), decimal(q)
            if price <= 0 or size < 0:
                raise SemanticError("invalid book level")
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

    def snapshot(self, bids, asks, *, sequence=None, ts=None, checksum=None, ts_prec="EXACT"):
        self._levels("bid", bids, snapshot=True)
        self._levels("ask", asks, snapshot=True)
        self.sequence = sequence
        self.exchange_ts = iso(ts)
        self.exchange_ts_prec = ts_prec if ts is not None else "COLLECTION_ONLY"
        self.integrity_ok = bool(self.bids and self.asks)
        self.gap_reason = None if self.integrity_ok else "EMPTY_BOOK"
        # Serialized by the archive writer thread, not on the capture path.
        self.pending_snapshot = (
            {
                "instrument_id": self.instrument["instrument_id"],
                "market_type": self.instrument["market_type"],
                "venue": self.instrument["venue"],
                "venue_symbol": self.instrument["venue_symbol"],
                **timestamp_fields("exchange_ts", self.exchange_ts, self.exchange_ts_prec),
                **collection_clock().fields("received_ts"),
                "bids": bids,
                "asks": asks,
                "sequence": sequence,
                "checksum": checksum,
                "quantity_basis": "PROVIDER_RAW_LEVEL_SIZE",
                "contract_multiplier": self.instrument.get("contract_multiplier"),
            }
        )
        if checksum is not None:
            if venue_key(self.instrument).startswith("okx") and int(checksum) == 0:
                self.integrity_method = "SEQ_ID_PREV_SEQ_ID_CHECKSUM_DEPRECATED_20260623"
            else:
                self.verify(checksum)

    VERIFY_S = 1.0

    def update(
        self, bids, asks, *, sequence=None, previous=None, first=None, ts=None, checksum=None, verify=False
    ):
        if self.sequence is None or not self.integrity_ok:
            raise SemanticError("book requires fresh snapshot")
        key = venue_key(self.instrument)
        if key.startswith("okx"):
            if sequence == self.sequence and previous == self.sequence:
                return False
        elif sequence is not None and int(sequence) <= int(self.sequence):
            return False
        if key.startswith("binance"):
            valid = (
                (int(previous) == int(self.sequence))
                if previous is not None
                else (first is not None and int(first) <= int(self.sequence) + 1 <= int(sequence))
            )
        elif key.startswith("okx"):
            valid = previous is not None and int(previous) == int(self.sequence)
        else:
            valid = True  # Kraken checksum and Coinbase full level2 delivery; Bybit u is not consecutive.
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
            for side, book in (("bid", self.bids), ("ask", self.asks)):
                excess = len(book) - 1000
                if excess <= 0:
                    continue
                # Drop the levels farthest from mid (usually one or two per update).
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
            if venue_key(self.instrument).startswith("okx") and int(checksum) == 0:
                self.integrity_method = "SEQ_ID_PREV_SEQ_ID_CHECKSUM_DEPRECATED_20260623"
                self.verified = True
            elif verify or time.monotonic() >= self._next_verify:
                self.verify(checksum)
                self.verified = True
                self._next_verify = time.monotonic() + self.VERIFY_S
            else:
                # A wrong state persists, so the next verification still catches it;
                # nothing is summarized from an unverified state.
                self.verified = False
        return True

    def verify(self, expected):
        key = venue_key(self.instrument)
        bids = self._prices["bid"][-25:][::-1]  # checksums cover the top 25 (OKX) / 10 (Kraken)
        asks = self._prices["ask"][:25]
        if key.startswith("okx"):
            parts = []
            for i in range(25):
                if i < len(bids):
                    parts.extend(self._strings["bid"][bids[i]])
                if i < len(asks):
                    parts.extend(self._strings["ask"][asks[i]])
            crc = zlib.crc32(":".join(parts).encode())
            crc = crc if crc < 2**31 else crc - 2**32
        elif key.startswith("kraken"):
            parts = []
            for side, prices in (("ask", asks[:10]), ("bid", bids[:10])):
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

    @precise
    def summary(self, *, received_ts=None):
        if not self.bids or not self.asks:
            raise SemanticError("empty order book")
        bid, ask = self._prices["bid"][-1], self._prices["ask"][0]
        if ask <= bid:
            self.integrity_ok = False
            self.gap_reason = "CROSSED_BOOK"
        mid = (ask + bid) / 2
        multiplier = (
            decimal(self.instrument["contract_multiplier"])
            if venue_key(self.instrument) == "okx.swap"
            and self.instrument.get("contract_multiplier") is not None
            else Decimal(1)
        )
        if venue_key(self.instrument) == "okx.swap" and self.instrument.get("contract_multiplier") is None:
            raise SemanticError("OKX multiplier absent")
        fx = (
            Decimal(1)
            if self.instrument.get("quote_asset") == "USD"
            else decimal(self.instrument["quote_usd_rate"])
            if self.instrument.get("quote_usd_rate") is not None
            else None
        )
        depth = {}
        ranges = {}
        levels = {}
        bands = (("p0_5", Decimal(".5")), ("p1", Decimal(1)), ("p2", Decimal(2)))
        for side, book in (("bid", self.bids), ("ask", self.asks)):
            prices = self._prices[side]
            far = prices[0] if side == "bid" else prices[-1]
            covered = abs(far / mid - 1) * 100
            ranges[side] = covered
            # Only levels within 2% of mid are sorted (a full book can hold tens of
            # thousands of levels); |p/mid - 1| <= x  <=>  p on the right side of mid(1 ∓ x).
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
        row = jsonable(
            {
                "instrument_id": self.instrument["instrument_id"],
                "token_id": self.instrument["token_id"],
                **timestamp_fields("exchange_ts", self.exchange_ts, self.exchange_ts_prec),
                **collection_clock(received_ts).fields("received_ts"),
                "mid": mid,
                "best_bid": bid,
                "best_ask": ask,
                "spread_bps": (ask - bid) / mid * 10000,
                "depth": depth,
                "raw_levels_range_pct": "2",
                "raw_levels_scope": "WITHIN_TWO_PERCENT_OF_MID",
                "quote_asset": self.instrument.get("quote_asset"),
                "quote_usd_rate": fx,
                "range_covered_pct": ranges,
                "levels_received": len(self.bids) + len(self.asks),
                "integrity_ok": self.integrity_ok,
                "integrity_method": self.integrity_method,
                "gap_reason": self.gap_reason,
            }
        )
        row["bids"] = [[str(p), str(q)] for p, q in levels["bid"]]
        row["asks"] = [[str(p), str(q)] for p, q in levels["ask"]]
        return row
