"""Minute flow accumulator; persistent trade dedup is performed by MarketService."""

from datetime import timedelta
from decimal import Decimal as D
from itertools import pairwise

from tank.market.normalize import precise, timestamp_fields
from tank.time_precision import collection_clock, propagate_timestamp
from tank.util import iso, jsonable, parse_time


class FlowAccumulator:
    def __init__(self):
        self.bars = {}
        self.last = {}
        self.connected = {}
        self.boundaries = {}
        self.references = {}
        self.coverage_ranges = {}

        self.dirty = set()

    def prove_range(self, instrument_id, start, end, *, complete, source, reason=None, observed_at=None):
        proof = {
            "start": iso(start),
            "end": iso(end),
            "complete": bool(complete),
            "source": source,
            "reason": reason,
            **collection_clock(observed_at).fields("observed_at"),
        }
        recent = self.coverage_ranges.setdefault(instrument_id, [])
        if (
            recent
            and all(
                recent[-1].get(field) == proof.get(field)
                for field in ("start", "complete", "source", "reason")
            )
            and parse_time(recent[-1]["end"]) <= parse_time(end)
        ):
            recent[-1] = proof
        else:
            recent.append(proof)
        latest = max(parse_time(point["end"]) for point in recent)
        self.coverage_ranges[instrument_id] = [
            point for point in recent if parse_time(point["end"]) >= latest - timedelta(minutes=3)
        ]
        for key, bar in self.bars.items():
            if (
                key[0] == instrument_id
                and parse_time(key[1]) < parse_time(end)
                and parse_time(key[1]) + timedelta(minutes=1) > parse_time(start)
            ):
                self.dirty.add(key)
                existing = bar.setdefault("_coverage", [])
                if (
                    existing
                    and all(
                        existing[-1].get(field) == proof.get(field)
                        for field in ("start", "complete", "source", "reason")
                    )
                    and parse_time(existing[-1]["end"]) <= parse_time(end)
                ):
                    existing[-1] = proof
                else:
                    existing.append(proof)
        return proof

    def empty_minute(self, instrument, minute):
        key = (instrument["instrument_id"], iso(minute))
        if key in self.bars:
            return
        self.dirty.add(key)
        self.bars[key] = {
            "instrument_id": key[0],
            "token_id": instrument["token_id"],
            "venue": instrument["venue"],
            "market_type": instrument["market_type"],
            "minute_start": key[1],
            "quote_asset": instrument.get("quote_asset"),
            "buy_qty": D(0),
            "sell_qty": D(0),
            "buy_notional": D(0),
            "sell_notional": D(0),
            "n_buy": 0,
            "n_sell": 0,
            "sum_sq_qty": D(0),
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "vwap": None,
            "quote_volume": D(0),
            "volume_usd": D(0),
            "first_trade_id": None,
            "last_trade_id": None,
            "first_trade": None,
            "last_trade": None,
            "complete": False,
            "gap_reason": None,
            "zero_trade_minute": True,
            "size_buckets": {
                f"B{i}": {
                    "buy_qty": D(0),
                    "sell_qty": D(0),
                    "buy_notional": D(0),
                    "sell_notional": D(0),
                    "n": 0,
                }
                for i in range(1, 6)
            },
            "_trades": [],
            "_first_ts": None,
            "_last_ts": None,
            "_qty": D(0),
            "_quote": D(0),
            "_coverage": list(self.coverage_ranges.get(key[0], [])),
            "_prior_trade": self.trade_point(self.last.get(key[0]))
            if self.last.get(key[0]) and parse_time(self.last[key[0]]["ts"]) < parse_time(minute)
            else None,
        }

    @staticmethod
    def trade_point(trade):
        if trade is None:
            return None
        return {
            "trade_id": trade.get("trade_id"),
            "price": trade.get("price"),
            **propagate_timestamp(trade, "ts", precision_aliases=("timestamp_precision",)).fields("ts"),
        }

    def register_boundaries(self, instrument_id, points):
        for point in points:
            at = parse_time(point)
            minute = iso(at.replace(second=0, microsecond=0))
            self.boundaries.setdefault((instrument_id, minute), set()).add(at)
            self.dirty.add((instrument_id, minute))
            if at.second == 0 and at.microsecond == 0:
                previous = iso(at - timedelta(minutes=1))
                self.boundaries.setdefault((instrument_id, previous), set()).add(at)
                self.dirty.add((instrument_id, previous))

    @staticmethod
    def covered(start, end, ranges):
        if start == end:
            return True
        points = sorted(
            {start, end, *(max(start, min(end, parse_time(p[k]))) for p in ranges for k in ("start", "end"))}
        )
        for lo, hi in pairwise(points):
            candidates = [p for p in ranges if parse_time(p["start"]) <= lo and parse_time(p["end"]) >= hi]
            if not candidates or candidates[-1].get("complete") is not True:
                return False
        return True

    @precise
    def add(self, trade, *, continuity=True, emit_record=True):
        iid = trade["instrument_id"]
        ts = parse_time(trade["ts"])
        minute = iso(ts.replace(second=0, microsecond=0))
        key = (iid, minute)
        prior = self.last.get(iid)
        gap = None
        if (
            continuity
            and prior
            and trade["venue"] in ("binance", "coinbase", "kraken")
            and int(trade["trade_id"]) != int(prior["trade_id"]) + 1
        ):
            gap = "TRADE_ID_GAP"
        if (
            continuity
            and prior
            and trade["venue"] == "okx"
            and int(trade["trade_id"]) <= int(prior["trade_id"])
        ):
            gap = "NON_MONOTONIC_TRADE_ID"
        if continuity and trade["venue"] == "bybit":
            if trade.get("sequence") is None:
                gap = "BYBIT_SEQUENCE_UNAVAILABLE"
            elif (
                prior
                and prior.get("sequence") is not None
                and int(trade["sequence"]) < int(prior["sequence"])
            ):
                gap = "BYBIT_SEQUENCE_REGRESSION"
        if prior is None or parse_time(trade["ts"]) >= parse_time(prior["ts"]):
            self.last[iid] = trade
        q, p = D(trade["qty_native"]), D(trade["price"])
        usd = D(trade["notional_usd"]) if trade.get("notional_usd") is not None else None
        if key not in self.bars:
            self.bars[key] = {
                "instrument_id": iid,
                "token_id": trade["token_id"],
                "market_type": trade.get("market_type"),
                "venue": trade.get("venue"),
                "quote_asset": trade.get("quote_asset"),
                "minute_start": minute,
                "buy_qty": D(0),
                "sell_qty": D(0),
                "buy_notional": D(0) if usd is not None else None,
                "sell_notional": D(0) if usd is not None else None,
                "notional_reason": None if usd is not None else "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE",
                "n_buy": 0,
                "n_sell": 0,
                "sum_sq_qty": D(0),
                "open": p,
                "high": p,
                "low": p,
                "close": p,
                "vwap": p,
                "size_buckets": {
                    f"B{i}": {
                        "buy_qty": D(0),
                        "sell_qty": D(0),
                        "buy_notional": D(0),
                        "sell_notional": D(0),
                        "n": 0,
                    }
                    for i in range(1, 6)
                },
                "first_trade_id": trade["trade_id"],
                "last_trade_id": trade["trade_id"],
                "complete": False,
                "gap_reason": gap,
                "trade_aggregation": trade["trade_aggregation"],
                "_qty": D(0),
                "_quote": D(0),
                "_first_ts": ts,
                "_last_ts": ts,
                "_trades": [],
                "_coverage": list(self.coverage_ranges.get(iid, [])),
                "_prior_trade": self.trade_point(prior)
                if prior and parse_time(prior["ts"]) < ts.replace(second=0, microsecond=0)
                else None,
            }
        b = self.bars[key]
        self.dirty.add(key)
        b.pop("zero_trade_minute", None)
        b["_trades"].append(trade)
        side = trade["side"].lower()
        b[side + "_qty"] += q
        b["n_" + side] += 1
        b["sum_sq_qty"] += q * q
        if usd is None:
            b["notional_reason"] = "HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE"
            b["buy_notional"] = None
            b["sell_notional"] = None
            b["size_buckets"] = None
        elif b[side + "_notional"] is not None:
            b[side + "_notional"] += usd
            if b["size_buckets"] is not None:
                index = next((i for i, t in enumerate((1000, 10000, 100000, 1000000), 1) if usd < t), 5)
                bucket = b["size_buckets"][f"B{index}"]
                bucket[side + "_qty"] += q
                bucket[side + "_notional"] += usd
                bucket["n"] += 1
        b["high"] = max(b["high"], p) if b["high"] is not None else p
        b["low"] = min(b["low"], p) if b["low"] is not None else p
        if b.get("_first_ts") is None or ts < b["_first_ts"]:
            b["_first_ts"] = ts
            b["open"] = p
            b["first_trade_id"] = trade["trade_id"]
            b["first_trade"] = self.trade_point(trade)
        if b.get("_last_ts") is None or ts >= b["_last_ts"]:
            b["_last_ts"] = ts
            b["close"] = p
            b["last_trade_id"] = trade["trade_id"]
            b["last_trade"] = self.trade_point(trade)
        b.setdefault("first_trade", self.trade_point(trade))
        b["_qty"] += q
        b["_quote"] += q * p
        b["quote_volume"] = b["_quote"]
        b["volume_usd"] = (
            b["buy_notional"] + b["sell_notional"]
            if b["buy_notional"] is not None and b["sell_notional"] is not None
            else None
        )
        b["vwap"] = b["_quote"] / b["_qty"]
        if gap:
            b["gap_reason"] = gap
        return self.record(key, segments=False) if emit_record else None

    def record(self, key, *, complete=None, segments=True):
        bar = self.bars[key]
        if complete is not None:
            bar["complete"] = bool(complete and not bar["gap_reason"])
        record = {k: v for k, v in bar.items() if not k.startswith("_")}
        record.update(timestamp_fields("minute_start", record["minute_start"], "BUCKET(60s)"))
        record.update(collection_clock().fields("received_ts"))
        reasons = {}
        for field, source in (
            ("buy_usd", "buy_notional"),
            ("sell_usd", "sell_notional"),
            ("size_buckets", "size_buckets"),
        ):
            if record.get(source) is None:
                reasons[field] = "INCOMPLETE"
                reasons[source] = "INCOMPLETE"
        record["null_reasons"] = reasons
        if segments:
            minute = parse_time(record["minute_start"])
            end = minute + timedelta(minutes=1)
            points = sorted(p for p in self.boundaries.get(key, set()) if minute <= p <= end)
            ranges = bar.get("_coverage", [])
            intervals = {(minute, p) for p in points if p > minute} | {(p, end) for p in points if p < end}
            adjacent = sorted({minute, end, *points})
            intervals.update(pairwise(adjacent))
            record["boundary_segments"] = []
            for start, cutoff in sorted(intervals) if points else []:
                captured = [t for t in bar.get("_trades", []) if start <= parse_time(t["ts"]) < cutoff]
                accumulator = FlowAccumulator()
                for trade in captured:
                    accumulator.add(trade, continuity=False, emit_record=False)
                if captured:
                    segment = accumulator.record(next(iter(accumulator.bars)), segments=False)
                else:
                    segment = {
                        "buy_qty": "0",
                        "sell_qty": "0",
                        "buy_notional": "0",
                        "sell_notional": "0",
                        "n_buy": 0,
                        "n_sell": 0,
                        "sum_sq_qty": "0",
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": None,
                        "vwap": None,
                        "size_buckets": {
                            f"B{i}": {
                                "buy_qty": "0",
                                "sell_qty": "0",
                                "buy_notional": "0",
                                "sell_notional": "0",
                                "n": 0,
                            }
                            for i in range(1, 6)
                        },
                        "first_trade": None,
                        "last_trade": None,
                    }
                known = self.covered(start, cutoff, ranges)
                segment.update(
                    instrument_id=key[0],
                    **timestamp_fields("start", start, "EXACT"),
                    **timestamp_fields("end", cutoff, "EXACT"),
                    complete=known,
                    **collection_clock().fields("observed_at"),
                    coverage_reason=None if known else "INCOMPLETE",
                )
                record["boundary_segments"].append(segment)
            record["reference_points"] = []

            for boundary in sorted(set(points) | ({end} if bar.get("last_trade") is None else set())):
                explicit = self.references.get((key[0], iso(boundary)))
                last_trade = max(
                    (t for t in bar.get("_trades", []) if parse_time(t["ts"]) < boundary),
                    key=lambda t: parse_time(t["ts"]),
                    default=None,
                )
                earlier = [self.trade_point(last_trade)] if last_trade is not None else []
                if bar.get("_prior_trade"):
                    earlier.append(bar["_prior_trade"])
                latest = max(earlier, key=lambda p: parse_time(p["ts"])) if earlier else None
                known = bool(latest) and self.covered(minute, boundary, ranges)
                if (
                    explicit
                    and known
                    and latest
                    and (
                        not explicit.get("last_trade")
                        or parse_time(latest["ts"]) > parse_time(explicit["last_trade"]["ts"])
                    )
                ):
                    explicit = None
                point = explicit or {
                    **timestamp_fields("boundary", boundary, "EXACT"),
                    "last_trade": latest,
                    "complete": known,
                    **collection_clock().fields("observed_at"),
                    "coverage_reason": None if known else "INCOMPLETE",
                }
                record["reference_points"].append(dict(point, instrument_id=key[0]))
        return jsonable(record)

    def mark_gap(self, instrument_id, start, end, reason):
        for key, b in self.bars.items():
            if key[0] == instrument_id and parse_time(start).replace(second=0, microsecond=0) <= parse_time(
                key[1]
            ) <= parse_time(end):
                b["gap_reason"] = reason
                b["complete"] = False
                self.dirty.add(key)
