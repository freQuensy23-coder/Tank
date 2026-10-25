"""Deterministic minute flow aggregation over validated normalized trades."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from typing import Literal, overload

from tank.market.precision import precise
from tank.market.trade_contracts import (
    ActiveBoundarySegment,
    ActiveFlowBar,
    ActiveFlowSnapshot,
    BoundarySegment,
    CoverageProof,
    EmptyBoundarySegment,
    EmptyFlowBar,
    EmptyFlowSnapshot,
    FlowBar,
    FlowBucket,
    FlowBuckets,
    FlowIdentity,
    FlowKey,
    FlowReferencePoint,
    FlowSnapshot,
    NormalizedTrade,
    NullFlowField,
    TradeAggregation,
    TradePoint,
)
from tank.util import iso, parse_iso_time


@dataclass(slots=True)
class _Bucket:
    buy_qty: Decimal = Decimal(0)
    sell_qty: Decimal = Decimal(0)
    buy_notional: Decimal = Decimal(0)
    sell_notional: Decimal = Decimal(0)
    n: int = 0

    def snapshot(self) -> FlowBucket:
        return FlowBucket(
            buy_qty=self.buy_qty,
            sell_qty=self.sell_qty,
            buy_notional=self.buy_notional,
            sell_notional=self.sell_notional,
            n=self.n,
        )


type _Buckets = tuple[_Bucket, _Bucket, _Bucket, _Bucket, _Bucket]


def _zero_buckets() -> _Buckets:
    return _Bucket(), _Bucket(), _Bucket(), _Bucket(), _Bucket()


@dataclass(slots=True)
class _Bar:
    identity: FlowIdentity
    minute_start: datetime
    prior_trade: TradePoint | None
    coverage: list[CoverageProof]
    buy_qty: Decimal = Decimal(0)
    sell_qty: Decimal = Decimal(0)
    buy_notional: Decimal | None = Decimal(0)
    sell_notional: Decimal | None = Decimal(0)
    n_buy: int = 0
    n_sell: int = 0
    sum_sq_qty: Decimal = Decimal(0)
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    vwap: Decimal | None = None
    quote_volume: Decimal = Decimal(0)
    qty: Decimal = Decimal(0)
    first_trade: TradePoint | None = None
    last_trade: TradePoint | None = None
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    aggregation: TradeAggregation | None = None
    buckets: _Buckets | None = field(default_factory=_zero_buckets)
    trades: list[NormalizedTrade] = field(default_factory=list)
    complete: bool = False
    gap_reason: str | None = None
    empty: bool = True


class FlowAccumulator:
    def __init__(self, *, now: Callable[[], datetime]) -> None:
        self._now = now
        self.bars: dict[FlowKey, _Bar] = {}
        self.last: dict[str, NormalizedTrade] = {}
        self.boundaries: dict[FlowKey, set[datetime]] = {}
        self.references: dict[FlowKey, FlowReferencePoint] = {}
        self.coverage_ranges: dict[str, list[CoverageProof]] = {}
        self.dirty: set[FlowKey] = set()

    def _observed(self) -> datetime:
        return parse_iso_time(self._now())

    @staticmethod
    def _extend_proof(recent: list[CoverageProof], proof: CoverageProof) -> None:
        if recent:
            previous = recent[-1]
            if (
                previous.start == proof.start
                and previous.complete == proof.complete
                and previous.source == proof.source
                and previous.reason == proof.reason
                and previous.end <= proof.end
            ):
                recent[-1] = proof
                return
        recent.append(proof)

    def prove_range(
        self,
        instrument_id: str,
        start: datetime | str,
        end: datetime | str,
        *,
        complete: bool,
        source: str,
        observed_at: datetime | str,
        reason: str | None = None,
    ) -> CoverageProof:
        proof = CoverageProof(
            start=parse_iso_time(start),
            end=parse_iso_time(end),
            complete=complete,
            source=source,
            reason=reason,
            observed_at=parse_iso_time(observed_at),
            observed_at_prec="COLLECTION_ONLY",
            observed_at_prec_reason=None,
        )
        if instrument_id not in self.coverage_ranges:
            self.coverage_ranges[instrument_id] = []
        recent = self.coverage_ranges[instrument_id]
        self._extend_proof(recent, proof)
        latest = max(point.end for point in recent)
        self.coverage_ranges[instrument_id] = [
            point for point in recent if point.end >= latest - timedelta(minutes=3)
        ]
        for key, bar in self.bars.items():
            if (
                key[0] == instrument_id
                and bar.minute_start < proof.end
                and bar.minute_start + timedelta(minutes=1) > proof.start
            ):
                self.dirty.add(key)
                self._extend_proof(bar.coverage, proof)
        return proof

    @staticmethod
    def trade_point(trade: NormalizedTrade | None) -> TradePoint | None:
        if trade is None:
            return None
        return TradePoint(
            trade_id=trade.trade_id, price=trade.price, ts=trade.ts, ts_prec="EXACT", ts_prec_reason=None
        )

    def _new_bar(self, identity: FlowIdentity, minute: datetime, prior: NormalizedTrade | None) -> _Bar:
        earlier = self.trade_point(prior) if prior is not None and prior.ts < minute else None
        coverage = (
            list(self.coverage_ranges[identity.instrument_id])
            if identity.instrument_id in self.coverage_ranges
            else []
        )
        return _Bar(identity=identity, minute_start=minute, prior_trade=earlier, coverage=coverage)

    def empty_minute(self, instrument: FlowIdentity, minute: datetime | str) -> None:
        at = parse_iso_time(minute)
        key = (instrument.instrument_id, iso(at))
        if key in self.bars:
            return
        if at.second != 0 or at.microsecond != 0:
            raise ValueError("An empty flow minute requires an aligned minute boundary")
        identity = FlowIdentity(
            instrument_id=instrument.instrument_id,
            token_id=instrument.token_id,
            venue=instrument.venue,
            market_type=instrument.market_type,
            quote_asset=instrument.quote_asset,
        )
        self.dirty.add(key)
        self.bars[key] = self._new_bar(identity, at, self.last.get(instrument.instrument_id))

    def register_boundaries(self, instrument_id: str, points: Sequence[datetime | str]) -> None:
        for point in points:
            at = parse_iso_time(point)
            minute = iso(at.replace(second=0, microsecond=0))
            key = (instrument_id, minute)
            if key not in self.boundaries:
                self.boundaries[key] = set()
            self.boundaries[key].add(at)
            self.dirty.add(key)
            if at.second == 0 and at.microsecond == 0:
                previous = (instrument_id, iso(at - timedelta(minutes=1)))
                if previous not in self.boundaries:
                    self.boundaries[previous] = set()
                self.boundaries[previous].add(at)
                self.dirty.add(previous)

    @staticmethod
    def covered(start: datetime, end: datetime, ranges: Sequence[CoverageProof]) -> bool:
        if start == end:
            return True
        points = sorted(
            {start, end, *(max(start, min(end, at)) for proof in ranges for at in (proof.start, proof.end))}
        )
        for lo, hi in pairwise(points):
            candidates = [proof for proof in ranges if proof.start <= lo and proof.end >= hi]
            if not candidates or not candidates[-1].complete:
                return False
        return True

    @overload
    def add(
        self, trade: NormalizedTrade, *, continuity: bool = True, emit_record: Literal[True] = True
    ) -> FlowSnapshot: ...

    @overload
    def add(
        self, trade: NormalizedTrade, *, continuity: bool = True, emit_record: Literal[False]
    ) -> None: ...

    @precise
    def add(
        self, trade: NormalizedTrade, *, continuity: bool = True, emit_record: bool = True
    ) -> FlowSnapshot | None:
        trade = NormalizedTrade.model_validate(trade)
        iid, ts = trade.instrument_id, trade.ts
        minute = ts.replace(second=0, microsecond=0)
        key = (iid, iso(minute))
        prior = self.last.get(iid)
        gap = None
        if continuity and prior is not None:
            if (
                trade.venue in ("binance", "coinbase", "kraken")
                and int(trade.trade_id) != int(prior.trade_id) + 1
            ):
                gap = "TRADE_ID_GAP"
            if trade.venue == "okx" and int(trade.trade_id) <= int(prior.trade_id):
                gap = "NON_MONOTONIC_TRADE_ID"
        if continuity and trade.venue == "bybit":
            if trade.sequence is None:
                gap = "BYBIT_SEQUENCE_UNAVAILABLE"
            elif (
                prior is not None and prior.sequence is not None and int(trade.sequence) < int(prior.sequence)
            ):
                gap = "BYBIT_SEQUENCE_REGRESSION"
        if prior is None or ts >= prior.ts:
            self.last[iid] = trade
        if key not in self.bars:
            identity = FlowIdentity(
                instrument_id=iid,
                token_id=trade.token_id,
                venue=trade.venue,
                market_type=trade.market_type,
                quote_asset=trade.quote_asset,
            )
            self.bars[key] = self._new_bar(identity, minute, prior)
        bar = self.bars[key]
        self.dirty.add(key)
        bar.empty = False
        bar.aggregation = trade.trade_aggregation
        bar.trades.append(trade)
        qty, price, usd = trade.qty_native, trade.price, trade.notional_usd
        if trade.side == "BUY":
            bar.buy_qty += qty
            bar.n_buy += 1
        else:
            bar.sell_qty += qty
            bar.n_sell += 1
        bar.sum_sq_qty += qty * qty
        if usd is None:
            bar.buy_notional = None
            bar.sell_notional = None
            bar.buckets = None
        elif trade.side == "BUY" and bar.buy_notional is not None:
            bar.buy_notional += usd
        elif trade.side == "SELL" and bar.sell_notional is not None:
            bar.sell_notional += usd
        if usd is not None and bar.buckets is not None:
            index = next(
                (index for index, threshold in enumerate((1000, 10000, 100000, 1000000)) if usd < threshold),
                4,
            )
            bucket = bar.buckets[index]
            if trade.side == "BUY":
                bucket.buy_qty += qty
                bucket.buy_notional += usd
            else:
                bucket.sell_qty += qty
                bucket.sell_notional += usd
            bucket.n += 1
        bar.high = max(bar.high, price) if bar.high is not None else price
        bar.low = min(bar.low, price) if bar.low is not None else price
        if bar.first_ts is None or ts < bar.first_ts:
            bar.first_ts, bar.open, bar.first_trade = ts, price, self.trade_point(trade)
        if bar.last_ts is None or ts >= bar.last_ts:
            bar.last_ts, bar.close, bar.last_trade = ts, price, self.trade_point(trade)
        bar.qty += qty
        bar.quote_volume += qty * price
        bar.vwap = bar.quote_volume / bar.qty
        if gap is not None:
            bar.gap_reason = gap
        return self._snapshot(key) if emit_record else None

    @staticmethod
    def _bucket_snapshot(buckets: _Buckets | None) -> FlowBuckets | None:
        if buckets is None:
            return None
        return FlowBuckets(
            B1=buckets[0].snapshot(),
            B2=buckets[1].snapshot(),
            B3=buckets[2].snapshot(),
            B4=buckets[3].snapshot(),
            B5=buckets[4].snapshot(),
        )

    @precise
    def _snapshot(self, key: FlowKey) -> FlowSnapshot:
        bar = self.bars[key]
        received = self._observed()
        reasons: dict[NullFlowField, Literal["INCOMPLETE"]] = {}
        if bar.buy_notional is None:
            reasons["buy_usd"] = "INCOMPLETE"
            reasons["buy_notional"] = "INCOMPLETE"
        if bar.sell_notional is None:
            reasons["sell_usd"] = "INCOMPLETE"
            reasons["sell_notional"] = "INCOMPLETE"
        if bar.buckets is None:
            reasons["size_buckets"] = "INCOMPLETE"
        volume_usd = (
            bar.buy_notional + bar.sell_notional
            if bar.buy_notional is not None and bar.sell_notional is not None
            else None
        )
        if bar.empty:
            return EmptyFlowSnapshot(
                instrument_id=bar.identity.instrument_id,
                token_id=bar.identity.token_id,
                venue=bar.identity.venue,
                market_type=bar.identity.market_type,
                quote_asset=bar.identity.quote_asset,
                buy_qty=bar.buy_qty,
                sell_qty=bar.sell_qty,
                buy_notional=bar.buy_notional,
                sell_notional=bar.sell_notional,
                n_buy=bar.n_buy,
                n_sell=bar.n_sell,
                sum_sq_qty=bar.sum_sq_qty,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                vwap=bar.vwap,
                size_buckets=self._bucket_snapshot(bar.buckets),
                first_trade=bar.first_trade,
                last_trade=bar.last_trade,
                minute_start=bar.minute_start,
                minute_start_prec="BUCKET(60s)",
                minute_start_prec_reason=None,
                received_ts=received,
                received_ts_prec="COLLECTION_ONLY",
                received_ts_prec_reason=None,
                first_trade_id=bar.first_trade.trade_id if bar.first_trade is not None else None,
                last_trade_id=bar.last_trade.trade_id if bar.last_trade is not None else None,
                complete=bar.complete,
                gap_reason=bar.gap_reason,
                quote_volume=bar.quote_volume,
                volume_usd=volume_usd,
                null_reasons=reasons,
                zero_trade_minute=True,
            )
        assert bar.aggregation is not None
        return ActiveFlowSnapshot(
            instrument_id=bar.identity.instrument_id,
            token_id=bar.identity.token_id,
            venue=bar.identity.venue,
            market_type=bar.identity.market_type,
            quote_asset=bar.identity.quote_asset,
            buy_qty=bar.buy_qty,
            sell_qty=bar.sell_qty,
            buy_notional=bar.buy_notional,
            sell_notional=bar.sell_notional,
            n_buy=bar.n_buy,
            n_sell=bar.n_sell,
            sum_sq_qty=bar.sum_sq_qty,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            vwap=bar.vwap,
            size_buckets=self._bucket_snapshot(bar.buckets),
            first_trade=bar.first_trade,
            last_trade=bar.last_trade,
            minute_start=bar.minute_start,
            minute_start_prec="BUCKET(60s)",
            minute_start_prec_reason=None,
            received_ts=received,
            received_ts_prec="COLLECTION_ONLY",
            received_ts_prec_reason=None,
            first_trade_id=bar.first_trade.trade_id if bar.first_trade is not None else None,
            last_trade_id=bar.last_trade.trade_id if bar.last_trade is not None else None,
            complete=bar.complete,
            gap_reason=bar.gap_reason,
            quote_volume=bar.quote_volume,
            volume_usd=volume_usd,
            null_reasons=reasons,
            trade_aggregation=bar.aggregation,
            notional_reason="HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE" if volume_usd is None else None,
        )

    @staticmethod
    def _empty_segment(
        instrument_id: str, start: datetime, end: datetime, known: bool, observed: datetime
    ) -> EmptyBoundarySegment:
        return EmptyBoundarySegment(
            instrument_id=instrument_id,
            buy_qty=Decimal(0),
            sell_qty=Decimal(0),
            buy_notional=Decimal(0),
            sell_notional=Decimal(0),
            n_buy=0,
            n_sell=0,
            sum_sq_qty=Decimal(0),
            open=None,
            high=None,
            low=None,
            close=None,
            vwap=None,
            size_buckets=FlowAccumulator._bucket_snapshot(_zero_buckets()),
            first_trade=None,
            last_trade=None,
            start=start,
            start_prec="EXACT",
            start_prec_reason=None,
            end=end,
            end_prec="EXACT",
            end_prec_reason=None,
            complete=known,
            observed_at=observed,
            observed_at_prec="COLLECTION_ONLY",
            observed_at_prec_reason=None,
            coverage_reason=None if known else "INCOMPLETE",
        )

    @staticmethod
    def _active_segment(
        snapshot: ActiveFlowSnapshot, start: datetime, end: datetime, known: bool, observed: datetime
    ) -> ActiveBoundarySegment:
        return ActiveBoundarySegment(
            instrument_id=snapshot.instrument_id,
            token_id=snapshot.token_id,
            venue=snapshot.venue,
            market_type=snapshot.market_type,
            quote_asset=snapshot.quote_asset,
            buy_qty=snapshot.buy_qty,
            sell_qty=snapshot.sell_qty,
            buy_notional=snapshot.buy_notional,
            sell_notional=snapshot.sell_notional,
            n_buy=snapshot.n_buy,
            n_sell=snapshot.n_sell,
            sum_sq_qty=snapshot.sum_sq_qty,
            open=snapshot.open,
            high=snapshot.high,
            low=snapshot.low,
            close=snapshot.close,
            vwap=snapshot.vwap,
            size_buckets=snapshot.size_buckets,
            first_trade=snapshot.first_trade,
            last_trade=snapshot.last_trade,
            minute_start=snapshot.minute_start,
            minute_start_prec=snapshot.minute_start_prec,
            minute_start_prec_reason=snapshot.minute_start_prec_reason,
            received_ts=snapshot.received_ts,
            received_ts_prec=snapshot.received_ts_prec,
            received_ts_prec_reason=snapshot.received_ts_prec_reason,
            first_trade_id=snapshot.first_trade_id,
            last_trade_id=snapshot.last_trade_id,
            complete=known,
            gap_reason=snapshot.gap_reason,
            quote_volume=snapshot.quote_volume,
            volume_usd=snapshot.volume_usd,
            null_reasons=snapshot.null_reasons,
            trade_aggregation=snapshot.trade_aggregation,
            notional_reason=snapshot.notional_reason,
            start=start,
            start_prec="EXACT",
            start_prec_reason=None,
            end=end,
            end_prec="EXACT",
            end_prec_reason=None,
            observed_at=observed,
            observed_at_prec="COLLECTION_ONLY",
            observed_at_prec_reason=None,
            coverage_reason=None if known else "INCOMPLETE",
        )

    def record(self, key: FlowKey, *, complete: bool | None = None) -> FlowBar:
        bar = self.bars[key]
        if complete is not None:
            bar.complete = complete and bar.gap_reason is None
        snapshot = self._snapshot(key)
        minute, end = bar.minute_start, bar.minute_start + timedelta(minutes=1)
        points = (
            sorted(point for point in self.boundaries[key] if minute <= point <= end)
            if key in self.boundaries
            else []
        )
        intervals = {(minute, point) for point in points if point > minute} | {
            (point, end) for point in points if point < end
        }
        intervals.update(pairwise(sorted({minute, end, *points})))
        segments: list[BoundarySegment] = []
        for start, cutoff in sorted(intervals) if points else []:
            captured = [trade for trade in bar.trades if start <= trade.ts < cutoff]
            known = self.covered(start, cutoff, bar.coverage)
            if captured:
                accumulator = FlowAccumulator(now=self._now)
                for trade in captured:
                    accumulator.add(trade, continuity=False, emit_record=False)
                part = accumulator._snapshot(next(iter(accumulator.bars)))
                assert isinstance(part, ActiveFlowSnapshot)
                segments.append(self._active_segment(part, start, cutoff, known, self._observed()))
            else:
                segments.append(self._empty_segment(key[0], start, cutoff, known, self._observed()))
        references: list[FlowReferencePoint] = []
        boundaries = set(points)
        if bar.last_trade is None:
            boundaries.add(end)
        for boundary in sorted(boundaries):
            explicit = self.references.get((key[0], iso(boundary)))
            before = [trade for trade in bar.trades if trade.ts < boundary]
            last_trade = max(before, key=lambda trade: trade.ts) if before else None
            earlier: list[TradePoint] = []
            if last_trade is not None:
                trade_point = self.trade_point(last_trade)
                assert trade_point is not None
                earlier.append(trade_point)
            if bar.prior_trade is not None:
                earlier.append(bar.prior_trade)
            latest = max(earlier, key=lambda point: point.ts) if earlier else None
            known = latest is not None and self.covered(minute, boundary, bar.coverage)
            if (
                explicit is not None
                and known
                and latest is not None
                and (explicit.last_trade is None or latest.ts > explicit.last_trade.ts)
            ):
                explicit = None
            if explicit is None:
                point = FlowReferencePoint(
                    instrument_id=key[0],
                    boundary=boundary,
                    boundary_prec="EXACT",
                    boundary_prec_reason=None,
                    last_trade=latest,
                    complete=known,
                    observed_at=self._observed(),
                    observed_at_prec="COLLECTION_ONLY",
                    observed_at_prec_reason=None,
                    coverage_reason=None if known else "INCOMPLETE",
                )
            else:
                if explicit.instrument_id != key[0] or explicit.boundary != boundary:
                    raise ValueError("Flow reference identity does not match its indexed boundary")
                point = explicit
            references.append(point)
        if isinstance(snapshot, EmptyFlowSnapshot):
            return EmptyFlowBar(
                instrument_id=snapshot.instrument_id,
                token_id=snapshot.token_id,
                venue=snapshot.venue,
                market_type=snapshot.market_type,
                quote_asset=snapshot.quote_asset,
                buy_qty=snapshot.buy_qty,
                sell_qty=snapshot.sell_qty,
                buy_notional=snapshot.buy_notional,
                sell_notional=snapshot.sell_notional,
                n_buy=snapshot.n_buy,
                n_sell=snapshot.n_sell,
                sum_sq_qty=snapshot.sum_sq_qty,
                open=snapshot.open,
                high=snapshot.high,
                low=snapshot.low,
                close=snapshot.close,
                vwap=snapshot.vwap,
                size_buckets=snapshot.size_buckets,
                first_trade=snapshot.first_trade,
                last_trade=snapshot.last_trade,
                minute_start=snapshot.minute_start,
                minute_start_prec=snapshot.minute_start_prec,
                minute_start_prec_reason=snapshot.minute_start_prec_reason,
                received_ts=snapshot.received_ts,
                received_ts_prec=snapshot.received_ts_prec,
                received_ts_prec_reason=snapshot.received_ts_prec_reason,
                first_trade_id=snapshot.first_trade_id,
                last_trade_id=snapshot.last_trade_id,
                complete=snapshot.complete,
                gap_reason=snapshot.gap_reason,
                quote_volume=snapshot.quote_volume,
                volume_usd=snapshot.volume_usd,
                null_reasons=snapshot.null_reasons,
                boundary_segments=segments,
                reference_points=references,
                zero_trade_minute=True,
            )
        return ActiveFlowBar(
            instrument_id=snapshot.instrument_id,
            token_id=snapshot.token_id,
            venue=snapshot.venue,
            market_type=snapshot.market_type,
            quote_asset=snapshot.quote_asset,
            buy_qty=snapshot.buy_qty,
            sell_qty=snapshot.sell_qty,
            buy_notional=snapshot.buy_notional,
            sell_notional=snapshot.sell_notional,
            n_buy=snapshot.n_buy,
            n_sell=snapshot.n_sell,
            sum_sq_qty=snapshot.sum_sq_qty,
            open=snapshot.open,
            high=snapshot.high,
            low=snapshot.low,
            close=snapshot.close,
            vwap=snapshot.vwap,
            size_buckets=snapshot.size_buckets,
            first_trade=snapshot.first_trade,
            last_trade=snapshot.last_trade,
            minute_start=snapshot.minute_start,
            minute_start_prec=snapshot.minute_start_prec,
            minute_start_prec_reason=snapshot.minute_start_prec_reason,
            received_ts=snapshot.received_ts,
            received_ts_prec=snapshot.received_ts_prec,
            received_ts_prec_reason=snapshot.received_ts_prec_reason,
            first_trade_id=snapshot.first_trade_id,
            last_trade_id=snapshot.last_trade_id,
            complete=snapshot.complete,
            gap_reason=snapshot.gap_reason,
            quote_volume=snapshot.quote_volume,
            volume_usd=snapshot.volume_usd,
            null_reasons=snapshot.null_reasons,
            boundary_segments=segments,
            reference_points=references,
            trade_aggregation=snapshot.trade_aggregation,
            notional_reason=snapshot.notional_reason,
        )

    def mark_gap(self, instrument_id: str, start: datetime | str, end: datetime | str, reason: str) -> None:
        first, last = parse_iso_time(start).replace(second=0, microsecond=0), parse_iso_time(end)
        for key, bar in self.bars.items():
            if key[0] == instrument_id and first <= bar.minute_start <= last:
                bar.gap_reason = reason
                bar.complete = False
                self.dirty.add(key)
