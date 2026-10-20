"""Stateful level 2 books, continuity and venue checksum verification."""

from __future__ import annotations

import zlib
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
        self.integrity_method = (
            "SEQUENCE"
            if venue_key(instrument).startswith(("binance", "okx"))
            else "CRC32"
            if venue_key(instrument).startswith("kraken")
            else "DOCUMENTED_LEVEL2_DELIVERY"
        )
        self._strings = {"bid": {}, "ask": {}}

    def _levels(self, side, levels, *, snapshot=False):
        book = self.bids if side == "bid" else self.asks
        strings = self._strings[side]
        if snapshot:
            book.clear()
            strings.clear()
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
                book.pop(price, None)
                strings.pop(price, None)
            else:
                book[price] = size
                strings[price] = (p, q)

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

    def update(self, bids, asks, *, sequence=None, previous=None, first=None, ts=None, checksum=None):
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
                if len(book) <= 1000:
                    continue
                keep = set(sorted(book, reverse=side == "bid")[:1000])
                for price in list(book):
                    if price not in keep:
                        book.pop(price)
                        self._strings[side].pop(price)
        if checksum is not None:
            if venue_key(self.instrument).startswith("okx") and int(checksum) == 0:
                self.integrity_method = "SEQ_ID_PREV_SEQ_ID_CHECKSUM_DEPRECATED_20260623"
            else:
                self.verify(checksum)
        return True

    def verify(self, expected):
        key = venue_key(self.instrument)
        bids = sorted(self.bids, reverse=True)
        asks = sorted(self.asks)
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
        bid, ask = max(self.bids), min(self.asks)
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
        for side, book in (("bid", self.bids), ("ask", self.asks)):
            far = min(book) if side == "bid" else max(book)
            covered = abs(far / mid - 1) * 100
            ranges[side] = covered
            depth[side] = {}
            for name, pct in (("p0_5", Decimal(".5")), ("p1", Decimal(1)), ("p2", Decimal(2))):
                if covered < pct:
                    depth[side][name] = None
                    continue
                levels = [(p, q * multiplier) for p, q in book.items() if abs(p / mid - 1) * 100 <= pct]
                depth[side][name] = {
                    "native": sum((q for p, q in levels), Decimal(0)),
                    "usd": sum((p * q * fx for p, q in levels), Decimal(0)) if fx is not None else None,
                }
        return jsonable(
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
                "bids": [
                    [p, q * multiplier]
                    for p, q in sorted(self.bids.items(), reverse=True)
                    if abs(p / mid - 1) <= Decimal(".02")
                ],
                "asks": [
                    [p, q * multiplier]
                    for p, q in sorted(self.asks.items())
                    if abs(p / mid - 1) <= Decimal(".02")
                ],
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
