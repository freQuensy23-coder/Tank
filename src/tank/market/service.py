from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from bisect import bisect_left, bisect_right
from collections import defaultdict
from contextlib import suppress
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, localcontext

import httpx

from tank.market_storage import (
    latest_market_trades,
    market_dependency_head,
    market_kline_days_recorded_since,
    market_range_by_instrument,
    market_series_day_counts,
    sampled_market_history,
)
from tank.time_precision import Timestamp, chosen_boundary, collection_clock, propagate_timestamp
from tank.util import iso, jsonable, new_id, parse_time, sha256, utcnow

from . import normalize as norm
from .adapters import VenueAdapter
from .archive import BookArchive, TradeArchive
from .capture import event_anchor
from .flow import FlowAccumulator
from .rate_gate import PriorityGate, binance_request_weight, public_request_constraints

# Reaction snapshots read books and tickers only this close to the window bounds.
REACTION_BOUND_MARGIN = timedelta(minutes=5)
REACTION_NEWEST_MARGIN = timedelta(minutes=15)
# Market Truth snapshots read tickers this far before each window bound (the latest ticker at
# or before it) and after it (marks within the sync tolerance of the OI points at the bound).
TICKER_ASOF_MARGIN = timedelta(minutes=15)
TICKER_MARK_MARGIN = timedelta(minutes=2)


def _merge_ranges(ranges):
    """Sorted, non-overlapping [start, end) ranges covering the given ones."""
    out = []
    for low, high in sorted(r for r in ranges if r[0] < r[1]):
        if out and low <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], high))
        else:
            out.append((low, high))
    return out


def _cut(rows_by_instrument, field, start, end):
    """[start, end) of each instrument's rows, which are ordered by `field`."""
    out = {}
    for iid in sorted(rows_by_instrument):
        rows = rows_by_instrument[iid]
        times = [parse_time(row[field]) for row in rows]
        out[iid] = rows[bisect_left(times, start) : bisect_left(times, end)]
    return out


def book_level_keeper():
    """Order-book levels only where Market Truth walks them, applied while rows stream.

    Slippage walks the levels of the newest integrity-checked book per venue (03 §5.5);
    older rows are read for depth, spread and mid. Two days of BTC levels held ~3.5 GB
    per snapshot, so rows outside each instrument's newest BOOK_LEVEL_ROWS drop their
    levels as soon as they fall out (the slack covers availability-bounded snapshots).
    """
    newest_ok = {}

    def keep(instrument_id, rows):
        index = len(rows) - 1

        def strip(i):
            if "bids" in rows[i] or "asks" in rows[i]:
                rows[i] = {k: v for k, v in rows[i].items() if k not in ("bids", "asks")}

        if rows[index].get("integrity_ok") is True:
            previous = newest_ok.get(instrument_id)
            if previous is not None and previous < index - BOOK_LEVEL_ROWS:
                strip(previous)
            newest_ok[instrument_id] = index
        if index >= BOOK_LEVEL_ROWS and index - BOOK_LEVEL_ROWS != newest_ok.get(instrument_id):
            strip(index - BOOK_LEVEL_ROWS)

    return keep


# Reads shared by the per-anchor snapshots of one snapshot_anchors() call.
from contextvars import ContextVar

_SHARED_READS = ContextVar("market_shared_snapshot_reads", default=None)
from .streams import STREAM_PING_INTERVAL_S, endpoint_keys, stream_group_loop

log = logging.getLogger(__name__)
# Raw journals are appended and fsynced at most this often (seconds); typed rows of
# the same prints follow in the same flush.
RAW_FLUSH_S = 5
LIVE_FIRST = ("mkt.flow_bars_1m", "mkt.tickers_24h")  # flushed ahead of other typed rows
FLUSH_ROWS = 500  # typed rows per write; a live flush can run between two of them
# Streamed mark/index/funding/OI context is stored at the hot tier's summary cadence
# (03 §5.1: book summaries every 10 s); the newest value is kept in memory between.
CONTEXT_S = 10
# Pre-event windows ending within this span are filled from venue REST only; a missing
# REST range stays an explicit gap instead of a whole-day archive parse.
REST_ONLY_WINDOW = timedelta(hours=48)
# Listed REST that cannot page trade history (03 §5.1): unproven minutes there keep this
# reason instead of a request. Bybit recent-trade lists only the last 1000 prints, so only
# minutes this recent are worth a request.
NO_TRADE_HISTORY = {"hyperliquid": "NO_LISTED_TRADE_BACKFILL"}
BYBIT_RECENT = timedelta(hours=1)
BYBIT_LIMIT = "SOURCE_LIMIT: Bybit recent-trade lists only the last 1000 prints"
# OI metrics that sources publish only as 5-min buckets (02 §4.2, §4.6).
SOURCE_BUCKETED_OI, SOURCE_BUCKET_S = ("bybit.linear.open-interest", "binance.usdm.openInterestHist"), 300.0
# Analysis windows around an anchor: PRE24 before it, reaction checkpoints up to 72 h after.
WINDOW_BEFORE, WINDOW_AFTER = timedelta(hours=24), timedelta(hours=72)
# Repairs go newest first in backfills of at most REPAIR_CHUNK, each persisted when done;
# the trailing REFERENCE_SPAN of spot (03 §5.7 reference venue) is repaired before anything else.
REPAIR_CHUNK, REFERENCE_SPAN = timedelta(hours=1), timedelta(hours=24)
PAGES_PER_FLUSH = 25
# Requests in flight per venue host (Kraken: 1).
HOST_IN_FLIGHT = 4
# Reference spot venues by repair speed (03 §5.7 needs one complete trailing 24 h).
REFERENCE_VENUES = ("binance", "coinbase", "okx", "kraken", "bybit")
# HOT tickers, OI and funding are polled this often so that their age at any minute cutoff
# stays within the 60 s HOT cadence of 03 §5.1 despite poll and loop latency.
HOT_POLL_S = 45
# Finished raw hours are sealed in their own pass, a few per pass.
SEAL_PASS_S, SEALS_PER_PASS = 10, 2
BOOK_SAVE_S = 8
# The event loop is reported stalled (stacks logged) when its heartbeat is this stale.
LOOP_STALL_S = 10
# A reused dependency head is recomputed at least this often (retention deletes and other
# processes are not in the write clock).
HEAD_REUSE_S = 300
# Past-day kline counts survive a restart in the data dir; a saved count is dropped when a
# kline of its day was recorded after the save, less this margin for queued writes.
DAY_COUNTS_SAVE_S = 60
DAY_COUNTS_MARGIN = timedelta(minutes=30)
SERIES_MARGIN = timedelta(hours=2)  # history a shared kline cursor holds before its window
SERIES_TRIM = timedelta(days=1)  # a cursor sheds history once its windows moved this far past it
SERIES_APPLY_ROWS = 20000  # kline rows a rebuild applies per worker-thread call
# Rows read only from public exchange endpoints never contain a Tank secret.
PUBLIC_MARKET = frozenset(
    {
        "mkt.flow_bars_1m",
        "mkt.klines_1m",
        "mkt.book_summaries",
        "mkt.oi_obs",
        "mkt.funding_obs",
        "mkt.liq_obs",
        "mkt.tickers_24h",
    }
)

WIDE_PAGE_ROWS = 500  # rows per fetch for wide rows: one fetchall stays well under a second
BOOK_LEVEL_ROWS = 30  # newest book rows per instrument that keep their levels in a snapshot
COLLECTIONS = {
    "flow_bars": ("mkt.flow_bars_1m", "minute_start"),
    "klines": ("mkt.klines_1m", "open_time"),
    "books": ("mkt.book_summaries", "exchange_ts"),
    "oi": ("mkt.oi_obs", "ts"),
    "funding": ("mkt.funding_obs", "as_of"),
    "liquidations": ("mkt.liq_obs", "ts"),
    "tickers": ("mkt.tickers_24h", "ts"),
    "supply": ("mkt.supply_obs", "ts"),
}


def funding_confirmations(observations, *, distinct_paths=False):
    """(REST, WEBSOCKET) pairs: each PUBLIC_REST CURRENT_ESTIMATE funding observation with the
    nearest WEBSOCKET one of its instrument and kind within 60 s (the first in input order on
    ties); with distinct_paths, both carry a source_path and the paths differ.

    WEBSOCKET rows are indexed by time once. The pairwise scan parsed both timestamps of every
    pair, quadratic in a BTC snapshot's funding rows, on the event loop (r111 profile, 21 Oct).
    """
    websocket = defaultdict(list)
    for index, ws in enumerate(observations):
        if (
            ws.get("capture_source") == "WEBSOCKET"
            and ws.get("kind") == "CURRENT_ESTIMATE"
            and ws.get("as_of")
            and (ws.get("source_path") or not distinct_paths)
        ):
            websocket[ws.get("instrument_id")].append((parse_time(ws["as_of"]), index, ws))
    for rows in websocket.values():
        rows.sort(key=lambda item: (item[0], item[1]))
    pairs = []
    for rest in observations:
        if rest.get("capture_source") != "PUBLIC_REST" or rest.get("kind") != "CURRENT_ESTIMATE":
            continue
        if distinct_paths and not rest.get("source_path"):
            continue
        rows = websocket.get(rest.get("instrument_id"), [])
        at = parse_time(rest["as_of"])
        lo = bisect_left(rows, at - timedelta(seconds=60), key=lambda item: item[0])
        hi = bisect_right(rows, at + timedelta(seconds=60), key=lambda item: item[0])
        near = [
            item
            for item in rows[lo:hi]
            if not distinct_paths or item[2]["source_path"] != rest["source_path"]
        ]
        if near:
            pairs.append(
                (rest, min(near, key=lambda item: (abs((item[0] - at).total_seconds()), item[1]))[2])
            )
    return pairs


# Flow fields a REST audit must reproduce for a complete stored bar to stand as written.
REST_SAME_FLOW = (
    "n_buy",
    "n_sell",
    "buy_qty",
    "sell_qty",
    "sum_sq_qty",
    "buy_notional",
    "sell_notional",
    "open",
    "high",
    "low",
    "close",
    "first_trade_id",
    "last_trade_id",
)
REST_VERIFIED_KEEP = 200000


def _same_flow(stored, audited):
    def value(row, field):
        item = row.get(field)
        if item is None or field in ("first_trade_id", "last_trade_id"):
            return None if item is None else str(item)
        return Decimal(str(item))

    return all(value(stored, field) == value(audited, field) for field in REST_SAME_FLOW)


class MarketService:
    def __init__(self, repo, settings, *, http=None):
        self.repo = repo
        self.settings = settings
        self.http = http or httpx.AsyncClient(timeout=30, headers={"User-Agent": settings.user_agent})
        self._owns_http = http is None
        registry = settings.load_config("registry")
        self.tokens = {r["token_id"]: r for r in registry["tokens"]}
        self.sector_baskets = deepcopy(registry.get("sector_baskets", []))
        self._sector_tokens = {
            identity
            for basket in self.sector_baskets
            if basket.get("sector") != "BENCHMARK"
            for identity in basket.get("constituents", [])
        }
        self.instruments = {
            r["instrument_id"]: dict(r) for r in registry["instruments"] if r.get("active", True)
        }
        self.semantics = settings.load_config("semantics")["metrics"]
        self.book_archive = BookArchive(settings.data_dir, instruments=self.instruments.values())
        self.trade_archive = TradeArchive(settings.data_dir, instruments=self.instruments.values())
        self._raw_written = 0.0
        self._raw_stats = {"rows": 0, "sealed": 0, "writes": 0, "max_write_s": 0.0, "max_pending": 0}
        self._raw_logged = time.monotonic()
        self._started = utcnow()
        self._hour_loads = {}
        # Newest already-stored point of REST histories (OI history, settled funding)
        # that every derivatives poll returns again in full.
        self._history_marks = {}
        self._context_due = {}
        self._reads = {}  # host -> [succeeded, failed] REST reads since the last minute log
        self._hot_since = {}  # hot token -> earliest anchor of its events
        self._frames = {}  # WebSocket endpoint -> frames received since the last minute log
        # instrument -> the last few times a klines poll succeeded. Venues omit minutes
        # without trades (Coinbase), so a missing candle is not a stale poll.
        self._klines_polled = {}
        # (instrument, UTC day) -> complete 1m closes, for whole past days (snapshot coverage).
        self._kline_day_counts = {}
        # (instrument, minute) of complete stored bars a REST audit proved without a rewrite.
        self._rest_verified = {}
        self._kline_day_counts_saved = ({}, 0.0)  # the counts last saved, and when
        self._pending_market = {}
        self._market_cache = {}
        self._market_flush_lock = asyncio.Lock()
        self._market_restore_lock = asyncio.Lock()
        self._market_restored = False
        self._capture_checkpoints = {}
        self._recovery_pending = set()
        self._active_backfills = {}
        # Earliest unrepaired checkpoint per instrument while its repair is running.
        self._pending_reconnects = {}
        self._repair_slots = asyncio.Semaphore(3)
        self._condition_heads_memo = {}
        self.flow = FlowAccumulator()
        self.books = {}
        self.streams = {}
        self._stop = None
        self._last_request = {}
        self._response_cache = {}
        self._cache_locks = {}
        self._request_locks = {}
        self._poll_due = {}
        self._metadata = set()
        self._book_due = {}
        self._fx = {}
        self._backfills = set()
        self._capture_waits = {}
        self._connected_since = {}
        self._zero_cursor = {}
        self._background = set()
        self._stream_groups = {}
        self._stream_controls = {}
        self._book_initial_bridge = {}
        self._stream_watermarks = {}
        self._price_contexts = {}
        self._ticker_contexts = {}
        self._book_confirmations = {}
        self._context_locks = {}
        self._flushed_bars = {}
        self._last_flow_flush = 0
        self._last_hot_expiry_refresh = 0
        try:
            self.thresholds = settings.load_config("thresholds")
        except FileNotFoundError:
            self.thresholds = {}

    def _redact(self, value):
        secrets = [
            getattr(self.settings, name, None)
            for name in (
                "etherscan_api_key",
                "tavily_api_key",
                "typesafe_api_key",
                "opencode_api_key",
                "openma_api_key",
                "codex_gateway_key",
                "telegram_token",
            )
        ]
        secrets = tuple(secret for secret in secrets if secret)

        def redact(item):
            if isinstance(item, str):
                for secret in secrets:
                    item = item.replace(secret, "[REDACTED]")
                return item
            if isinstance(item, dict):
                return {redact(k): redact(v) for k, v in item.items()}
            if isinstance(item, (list, tuple)):
                return [redact(v) for v in item]
            return item

        return redact(value)

    def provenance(self, row):
        return self._redact(
            dict(row, config_version=self.settings.config_version, code_version=self.settings.code_version)
        )

    async def _put(self, collection, key, row, *, immutable=False):
        if collection == "qa.reviews" and row.get("at") is not None:
            row = dict(row, **collection_clock(row["at"]).fields("at"))
        if collection == "mkt.backfill_runs":
            row = dict(row)
            for field in ("started_at", "finished_at"):
                row.update(
                    norm.timestamp_fields(field, row[field], "COLLECTION_ONLY")
                    if row.get(field)
                    else Timestamp(
                        None, None, "PENDING" if row.get("status") in ("PENDING", "RUNNING") else "INCOMPLETE"
                    ).fields(field)
                )
        if collection == "mkt.flow_bars_1m":
            row = dict(row, **norm.timestamp_fields("minute_start", row["minute_start"], "BUCKET(60s)"))
            if row.get("received_ts") is not None:
                row["received_ts_prec"] = "COLLECTION_ONLY"
        if collection.startswith("mkt."):
            if collection == "mkt.trades":
                raise ValueError("Individual trades belong only in normalized hourly Parquet")
            # Book summaries arrive JSON-ready (OrderBook.summary); re-walking their levels is waste.
            row = dict(row) if collection == "mkt.book_summaries" else jsonable(row)
            row = (
                dict(
                    row, config_version=self.settings.config_version, code_version=self.settings.code_version
                )
                if collection in PUBLIC_MARKET
                else self.provenance(row)
            )
            row.setdefault("recorded_at", iso())
            row.update(collection_clock(row["recorded_at"]).fields("recorded_at"))
            if collection in ("mkt.hot_tokens", "mkt.backfill_runs"):
                changed = await self.repo.put_market_batch(collection, [row], keys=[str(key)])
                return changed[0] if changed else row
            # Repeated points of these series are dropped before PostgreSQL; the
            # storage layer computes its own fingerprint for every written row.
            if collection in ("mkt.flow_bars_1m", "mkt.tickers_24h", "mkt.funding_obs"):
                cache = self._market_cache.setdefault(collection, {})
                content = self._semantic_content(row)
                old = cache.get(str(key))
                if old and self._semantic_content(old) == content:
                    return old
                cache[str(key)] = row
                if len(cache) > 10000:
                    cache.pop(next(iter(cache)))
            self._pending_market.setdefault(collection, {})[str(key)] = row
            return row
        if collection == "qa.reviews" and await self.repo.get(collection, str(key)) is not None:
            key = str(key) + ":" + new_id("REV")
            row = dict(row, review_id=key)
        return await self.repo.put(collection, str(key), self.provenance(jsonable(row)), immutable=immutable)

    async def mark_capture_gap(self, instrument, start, end, reason, *, preserve_verified=True):
        """Invalidate every affected aggregate proof, including partial boundaries."""
        iid = instrument["instrument_id"]
        lo, hi = parse_time(start), parse_time(end)
        first = lo.replace(second=0, microsecond=0)
        rows = await self._range(
            "mkt.flow_bars_1m",
            "minute_start",
            start=first,
            end=hi + timedelta(microseconds=1),
            filters={"instrument_id": iid},
        )
        by_minute = {row["minute_start"]: row for row in rows}
        for row in self._pending_market.get("mkt.flow_bars_1m", {}).values():
            if row["instrument_id"] == iid and first <= parse_time(row["minute_start"]) <= hi:
                by_minute[row["minute_start"]] = row
        cursor = first
        while cursor <= hi.replace(second=0, microsecond=0):
            key = (iid, iso(cursor))
            row = by_minute.get(key[1])
            private = self.flow.bars.get(key)
            trusted = row and (row.get("backfill_verified") or row.get("source_archive"))
            trusted = trusted or (private and private.get("_rest_verified")) or key in self._rest_verified
            if not (preserve_verified and trusted):
                left, right = max(lo, cursor), min(hi, cursor + timedelta(minutes=1))
                if left < right:
                    self.trade_archive.coverage(
                        iid, left, right, complete=False, reason=str(reason), source="CAPTURE_GAP"
                    )
                    if private is not None:
                        private["complete"] = False
                        private["gap_reason"] = str(reason)
                        private.setdefault("_coverage", []).append(
                            {"start": iso(left), "end": iso(right), "complete": False, "reason": str(reason)}
                        )
                    if row:
                        row = dict(row, complete=False, gap_reason=str(reason))
                        for segment in row.get("boundary_segments") or []:
                            if parse_time(segment["start"]) < right and parse_time(segment["end"]) > left:
                                segment.update(complete=False, coverage_reason="INCOMPLETE")
                        for point in row.get("reference_points") or []:
                            if left < parse_time(point["boundary"]) <= right:
                                point.update(complete=False, coverage_reason="INCOMPLETE")
                        await self._put("mkt.flow_bars_1m", iid + ":" + key[1], row)
            cursor += timedelta(minutes=1)
        await self._gap(
            instrument, "STREAM" if preserve_verified else "TRADE_INTEGRITY", reason, start=start, end=end
        )

    async def _gap(self, instrument, kind, reason, *, start=None, end=None):
        reason = str(reason)
        for secret in (self.settings.etherscan_api_key,):
            if secret:
                reason = reason.replace(secret, "[REDACTED]")
        row = {
            "gap_id": new_id("GAP"),
            "instrument_id": instrument["instrument_id"],
            "token_id": instrument["token_id"],
            "kind": kind,
            "reason": str(reason),
            **norm.timestamp_fields(
                "start",
                start if start is not None else iso(),
                None if start is not None else "COLLECTION_ONLY",
            ),
            **norm.timestamp_fields(
                "end", end if end is not None else iso(), None if end is not None else "COLLECTION_ONLY"
            ),
            **collection_clock(iso()).fields("observed_at"),
            "status": "OPEN",
        }
        await self._put("mkt.capture_gaps", row["gap_id"], row, immutable=True)
        return row

    @classmethod
    def _semantic_content(cls, value):
        if isinstance(value, dict):
            ignored = ("received_ts", "recorded_at", "observed_at", "updated_at")
            return {
                key: cls._semantic_content(item)
                for key, item in value.items()
                if key not in ("config_version", "code_version", "semantic_fingerprint")
                and not any(key == field or key.startswith(field + "_prec") for field in ignored)
            }
        if isinstance(value, list):
            return [cls._semantic_content(item) for item in value]
        return value

    async def _ensure_market_restore(self):
        if self._market_restored:
            return
        async with self._market_restore_lock:
            if self._market_restored:
                return
            for collection, groups, field in (
                ("mkt.tickers_24h", ("instrument_id",), "ts"),
                ("mkt.funding_obs", ("instrument_id", "kind"), "as_of"),
            ):
                rows = await self.repo.latest_market_records(collection, group_by=groups, time_field=field)
                for row in rows:
                    iid = row["instrument_id"]
                    key = (
                        iid + ":" + row["kind"] + ":" + row["as_of"]
                        if collection == "mkt.funding_obs"
                        else iid + ":" + row["ts"]
                    )
                    self._market_cache.setdefault(collection, {}).setdefault(key, row)
            # The reconnect backfill resumes from the last stored print (03 §5.2): the
            # newest flow bar that holds a trade carries its ID and time.
            for row in await latest_market_trades(
                self.repo, list(self.instruments), start=utcnow() - timedelta(days=2)
            ):
                iid, last = row["instrument_id"], row["last_trade"]
                self._capture_checkpoints[iid] = {
                    "instrument_id": iid,
                    "last_trade_id": last["trade_id"],
                    "last_trade": last,
                    "last_ts": last["ts"],
                }
                self.flow.last[iid] = dict(last, instrument_id=iid)
            # Prints of the last minutes before a restart are on disk: remember them so
            # none is taken twice, and resume the bar of the newest minute, which was
            # still in progress. Earlier bars are already final in PostgreSQL.
            since = (utcnow() - timedelta(minutes=3)).replace(second=0, microsecond=0)
            for iid in self.instruments:
                rows = await asyncio.to_thread(self.trade_archive.recent, iid, since)
                newest = max((r["ts"][:16] for r in rows), default=None)
                for row in rows:
                    self.trade_archive.remember(row)
                    if row["ts"][:16] == newest:
                        self.flow.add(row, continuity=False, emit_record=False)
            self._market_restored = True

    async def _raw(self, instrument, kind, payload):
        """Keep the initial Coinbase print in the planned trade Parquet only.

        REST snapshots and websocket frames are normalized into typed market
        rows. HLD §7.2 has no second raw frame journal for those same responses.
        Books and trades retain their original payload in their own Parquet.
        """
        if kind == "websocket" and isinstance(payload, dict) and payload.get("type") == "last_match":
            point = norm.trade(instrument, payload, source="WS_INITIAL_SNAPSHOT")
            self.trade_archive.append(self.provenance(point))

    async def flush_market(self, *, raw=None):
        """Write raw journals when due, then one multi-row write per typed collection (03 §14).

        Raw prints and books go to disk in one worker thread every RAW_FLUSH_S, or at
        once with `raw=True` (a backfill about to read its prints back, an archive
        import, shutdown); `raw=False` writes typed rows only. A typed bar can reach
        PostgreSQL at most RAW_FLUSH_S before the prints it was built from.
        """
        async with self._market_flush_lock:
            if raw or (raw is None and time.monotonic() - self._raw_written >= RAW_FLUSH_S):
                await self._write_raw()
            # Live bars and tickers go first, and ones queued while a large batch (a
            # startup kline backfill) is being written jump ahead of its next chunk.
            pending, self._pending_market = self._pending_market, {}
            queue = sorted(pending.items(), key=lambda item: item[0] not in LIVE_FIRST)
            try:
                while queue:
                    collection, values = queue[0]
                    keys = list(values)[:FLUSH_ROWS]
                    await self.repo.put_market_batch(collection, [values[key] for key in keys], keys=keys)
                    for key in keys:
                        row = values.pop(key)
                        if collection == "mkt.klines_1m" and row.get("open_time"):
                            # A count read between the queueing and this write saw the old row.
                            day = parse_time(row["open_time"]).replace(
                                hour=0, minute=0, second=0, microsecond=0
                            )
                            self._kline_day_counts.pop((row.get("instrument_id"), day), None)
                    if not values:
                        queue.pop(0)
                    if queue and queue[0][0] not in LIVE_FIRST:
                        live = [c for c in LIVE_FIRST if c in self._pending_market]
                        queue[:0] = [(c, self._pending_market.pop(c)) for c in live]
                    await asyncio.sleep(0)
            except Exception:
                # Rows queued since the take are newer and win over the retried ones.
                for collection, values in queue:
                    target = self._pending_market.setdefault(collection, {})
                    for key, row in values.items():
                        target.setdefault(key, row)
                raise

    async def _write_raw(self):
        trades, books = self.trade_archive.take(), self.book_archive.take()
        pending = sum(map(len, trades.values())) + sum(map(len, books.values()))
        started = time.monotonic()

        def write():
            # Seals run in _seal_loop: a backlog of finished hours (33 at once on r109 after
            # repairs) must never hold this flush, and with it every typed write, for minutes.
            result = self.trade_archive.write(trades, seal_limit=0)
            booked = self.book_archive.write(books, seal_limit=0)
            return result["rows"] + booked["rows"], len(result["sealed"]) + len(booked["sealed"])

        try:
            rows, sealed = await asyncio.to_thread(write)
        except Exception:
            self.trade_archive.restore(trades)
            self.book_archive.restore(books)
            raise
        self.trade_archive.written()
        self.book_archive.written()
        elapsed = time.monotonic() - started
        self._raw_written = time.monotonic()
        stats = self._raw_stats
        stats.update(
            rows=stats["rows"] + rows,
            sealed=stats["sealed"] + sealed,
            writes=stats["writes"] + 1,
            max_write_s=max(stats["max_write_s"], elapsed),
            max_pending=max(stats["max_pending"], pending),
        )
        if time.monotonic() - self._raw_logged >= 60:
            log.info(
                "raw archive: %d rows in %d writes, %d hours sealed, max write %.2fs, max batch %d rows",
                stats["rows"],
                stats["writes"],
                stats["sealed"],
                stats["max_write_s"],
                stats["max_pending"],
            )
            reads, self._reads = self._reads, {}
            log.info(
                "rest reads: %s",
                " ".join(f"{host}={ok}/{failed}" for host, (ok, failed) in sorted(reads.items())),
            )
            frames, self._frames = self._frames, {}
            log.info(
                "ws frames: %s",
                " ".join(f"{endpoint.split('//')[1]}={count}" for endpoint, count in sorted(frames.items())),
            )
            self._raw_logged = time.monotonic()
            self._raw_stats = {"rows": 0, "sealed": 0, "writes": 0, "max_write_s": 0.0, "max_pending": 0}

    async def _loop_watchdog(self):
        """Heartbeat for a daemon thread that logs the event loop thread's stack (and every
        busy worker thread's) when the loop has not run for LOOP_STALL_S: a stall names its
        culprit (r109: 160 s without a log line, MainThread on CPU)."""
        import sys
        import threading
        import traceback

        beat = [time.monotonic()]
        loop_thread = threading.get_ident()

        def watch():
            reported = 0.0
            while not self._stop.is_set():
                time.sleep(1)
                stale = time.monotonic() - beat[0]
                if stale < LOOP_STALL_S or time.monotonic() - reported < 30:
                    continue
                reported = time.monotonic()
                frames = sys._current_frames()
                stack = "".join(traceback.format_stack(frames.get(loop_thread), limit=40))
                others = [
                    f"thread {ident}: "
                    + " < ".join(f"{f.name}:{f.lineno}" for f in traceback.extract_stack(frame)[-6:])
                    for ident, frame in frames.items()
                    if ident not in (loop_thread, threading.get_ident())
                    and "tank" in "".join(f.filename for f in traceback.extract_stack(frame))
                ]
                log.warning(
                    "event loop stalled %.0fs; loop thread stack:\n%s\n%s", stale, stack, "\n".join(others)
                )

        threading.Thread(target=watch, name="loop-watchdog", daemon=True).start()
        while not self._stop.is_set():
            beat[0] = time.monotonic()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), 1)

    async def _seal_loop(self):
        """Seal finished quiet hours a few at a time, outside the market flush."""
        while not self._stop.is_set():
            try:
                sealed = await asyncio.to_thread(
                    lambda: [
                        *self.trade_archive.seal_due(limit=SEALS_PER_PASS),
                        *self.book_archive.seal_due(limit=SEALS_PER_PASS),
                    ]
                )
                self._raw_stats["sealed"] += len(sealed)
            except Exception as exc:  # noqa: BLE001 - the next pass retries; journals stay on disk
                log.warning("seal pass failed: %s", type(exc).__name__)
            try:
                await self._save_day_counts()
            except Exception as exc:  # noqa: BLE001 - a later pass saves; a restart recounts
                log.warning("series count save failed: %s", type(exc).__name__)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), SEAL_PASS_S)

    async def close(self):
        try:
            await self._flush_flow()
            await self.flush_market(raw=True)
            await asyncio.to_thread(self.trade_archive.close)
            await asyncio.to_thread(self.book_archive.close)
            with suppress(Exception):
                await self._save_day_counts(force=True)
        finally:
            if self._owns_http:
                await self.http.aclose()

    async def _request(self, method, url, **kwargs):
        host = httpx.URL(url).host
        cache_key = None
        cache_ttl = 0
        payload = kwargs.get("json", {})
        if host == "api.hyperliquid.xyz":
            if payload.get("type") == "metaAndAssetCtxs":
                cache_key, cache_ttl = "hyperliquid:contexts", 30
            elif payload.get("type") == "fundingHistory":
                cache_key = "hyperliquid:funding:" + sha256(
                    {"request": payload, "settlement_period": iso()[:13]}
                )
                cache_ttl = 3600
        for expired_key, (captured_mono, _) in list(self._response_cache.items()):
            ttl = 30 if expired_key == "hyperliquid:contexts" else 3600
            if time.monotonic() - captured_mono >= ttl:
                self._response_cache.pop(expired_key, None)
        if not cache_key:
            return await self._request_live(method, url, **kwargs)
        # One response per shared request: cache hits neither consume budget nor
        # change the original HTTP observation timestamp.
        cache_lock_key = (
            "hyperliquid:funding:" + payload["coin"] if payload.get("type") == "fundingHistory" else cache_key
        )
        async with self._cache_locks.setdefault(cache_lock_key, asyncio.Lock()):
            if cache_key in self._response_cache:
                cached_at, cached_body = self._response_cache[cache_key]
                if time.monotonic() - cached_at < cache_ttl:
                    return cached_body
            body = await self._request_live(method, url, **kwargs)
            observed_at = iso()
            rows = body[1] if payload.get("type") == "metaAndAssetCtxs" else body
            for row in rows:
                row["_capture_observed_at"] = observed_at
            self._response_cache[cache_key] = (time.monotonic(), body)
            return body

    async def _request_live(self, method, url, **kwargs):
        host = httpx.URL(url).host
        counts = self._reads.setdefault(host, [0, 0])
        try:
            body = await self._request_once(method, url, **kwargs)
        except Exception:
            counts[1] += 1
            raise
        counts[0] += 1
        return body

    async def _request_once(self, method, url, **kwargs):
        priority = kwargs.pop("_priority", 0)
        host = httpx.URL(url).host
        interval = (
            1
            if host == "api.kraken.com"
            else 0.21
            if host == "api.exchange.coinbase.com"
            else 0.35
            if host == "api.etherscan.io"
            else 0.08
        )
        # Several requests in flight per host (Kraken's 1 req/s counter stays serial); request
        # starts are still spaced by the host interval and every venue budget still applies.
        lock = self._request_locks.setdefault(
            host, PriorityGate(1 if host == "api.kraken.com" else HOST_IN_FLIGHT)
        )

        async def admit_budget():
            from tank.sources.rate_limits import shared_request_budgets

            provider = (
                "kraken"
                if host == "api.kraken.com"
                else "coinbase"
                if host == "api.exchange.coinbase.com"
                else None
            )
            if provider:
                await shared_request_budgets.acquire(provider, 1 if provider == "kraken" else 5, 1)
            if host in ("api.binance.com", "fapi.binance.com"):
                path = httpx.URL(url).path
                weight = binance_request_weight(host, path, kwargs.get("params", {}))
                if weight:
                    await shared_request_budgets.acquire(
                        "binance.spot" if host == "api.binance.com" else "binance.usdm",
                        3000 if host == "api.binance.com" else 1200,
                        60,
                        weight=weight,
                    )
                if host == "fapi.binance.com" and path in (
                    "/fapi/v1/fundingRate",
                    "/fapi/v1/fundingInfo",
                ):
                    await shared_request_budgets.acquire("binance.usdm.funding", 250, 300)
                if host == "fapi.binance.com" and path == "/futures/data/openInterestHist":
                    await shared_request_budgets.acquire("binance.usdm.statistics", 500, 300)
            for budget_key, capacity, window, charge in public_request_constraints(
                host, httpx.URL(url).path, kwargs.get("params", {}), kwargs.get("json", {})
            ):
                await shared_request_budgets.acquire(budget_key, capacity, window, weight=charge)

        for attempt in range(4):
            # Waiting for a weighted repair budget must not own the HTTP slot:
            # a newly ready cheap baseline request can still be served first.
            await admit_budget()
            async with lock.enter(priority):
                # Reserve the next start slot before sleeping: concurrent holders never burst.
                now = time.monotonic()
                start = max(now, self._last_request.get(host, 0) + interval)
                self._last_request[host] = start
                if start > now:
                    await asyncio.sleep(start - now)
                try:
                    response = await self.http.request(method, url, **kwargs)
                except httpx.HTTPError as exc:
                    raise RuntimeError(f"{host}: {type(exc).__name__}") from None
                self._last_request[host] = max(self._last_request[host], time.monotonic())
                if response.status_code in (418, 429):
                    delay = float(response.headers.get("Retry-After", "60"))
                    self._last_request[host] = max(
                        self._last_request[host], time.monotonic() + min(max(delay, 1), 60)
                    )
                    continue
                if response.is_error:
                    # The venue's error code/message is the gap evidence (bodies carry no credentials).
                    raise RuntimeError(f"{host}: HTTP {response.status_code} {response.text[:200]}")
                # Binance reports whole-IP usage (including sandbox traffic). Tank
                # separately enforces its own half via shared_request_budgets.
                weight = response.headers.get("X-MBX-USED-WEIGHT-1M")
                if weight and int(weight) >= (2400 if host == "fapi.binance.com" else 6000):
                    self._last_request[host] = max(self._last_request[host], time.monotonic() + 60)
                # JSON floats must never enter native quantity or monetary arithmetic.
                return json.loads(response.content, parse_float=Decimal)
        raise RuntimeError(f"{host}: rate limited after bounded retries")

    def adapter(self, instrument, *, priority=0):
        async def request(method, url, **kwargs):
            return await self._request(method, url, _priority=priority, **kwargs)

        return VenueAdapter(instrument, request)

    async def _fx_rate(self, instrument):
        quote = instrument.get("quote_asset", "").upper()
        if quote == "USD":
            instrument["quote_usd_rate"] = "1"
            return
        cached = self._fx.get(quote)
        if cached and (utcnow() - parse_time(cached["ts"])).total_seconds() < 300:
            instrument.update(
                quote_usd_rate=cached["rate"],
                quote_usd_rate_as_of=cached["ts"],
                quote_usd_rate_as_of_prec="COLLECTION_ONLY",
            )
            return
        if quote not in ("USDT", "USDC"):
            return
        # The listed Coinbase 24h stats (02 §4.3) carry the last price but no clock.
        body = await self._request("GET", f"https://api.exchange.coinbase.com/products/{quote}-USD/stats")
        rate = str(norm.decimal(body["last"]))
        ts = iso()
        self._fx[quote] = {"rate": rate, "ts": ts}
        instrument.update(
            quote_usd_rate=rate, quote_usd_rate_as_of=ts, quote_usd_rate_as_of_prec="COLLECTION_ONLY"
        )

    async def discover(self, instrument):
        iid = instrument["instrument_id"]
        metadata = await self.adapter(instrument).discover()
        instrument.update(metadata, resolved_at=iso())
        await self._put("reg.instruments", iid, instrument)
        self._metadata.add(iid)

    async def poll_once(self, instrument_id, *, derivatives=True, klines=True, ticker=True):
        await self._ensure_market_restore()
        instrument = self.instruments[instrument_id]
        adapter = self.adapter(instrument)
        if instrument_id not in self._metadata:
            try:
                await self.discover(instrument)
            except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                await self._gap(instrument, "METADATA", exc)
        try:
            await self._fx_rate(instrument)
        except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
            await self._gap(instrument, "QUOTE_FX", exc)
        # OI and funding first: a slow host (shared by every series of the venue) must not
        # push the 60 s HOT derivatives cadence behind klines and tickers (03 §5.1).
        for kind in ("derivatives", "klines", "ticker"):
            if kind == "derivatives" and (not derivatives or norm.venue_key(instrument).endswith(".spot")):
                continue
            if (kind == "klines" and not klines) or (kind == "ticker" and not ticker):
                continue
            try:
                if kind == "klines":
                    rows = await adapter.klines()
                    polled = iso()
                    await self._raw(instrument, "klines", rows)
                    for raw in rows:
                        row = norm.kline(instrument, raw)
                        await self._store_kline(instrument, row)
                    self._klines_polled[instrument_id] = (
                        self._klines_polled.get(instrument_id, []) + [polled]
                    )[-5:]
                elif kind == "ticker":
                    await self._ticker(instrument, await adapter.ticker())
                else:
                    oi, current, settled, interval, mark = await adapter.derivatives()
                    await self._raw(
                        instrument,
                        "derivatives",
                        {
                            "oi": oi,
                            "current": current,
                            "settled": settled,
                            "interval": interval,
                            "mark": mark,
                        },
                    )
                    if instrument.get("funding_interval_h") != interval:
                        instrument["funding_interval_h"] = interval
                        await self._put("reg.instruments", instrument_id, instrument)
                    histories = [r for r in oi if r.get("timestamp") is not None and "time" not in r]
                    oi = [r for r in oi if r not in histories] + (
                        self._fresh(instrument_id, "oi", histories, lambda r: r["timestamp"])[1]
                    )
                    for raw in oi:
                        row = norm.oi(
                            instrument,
                            raw,
                            mark=mark,
                            mark_ts=raw.get("_capture_observed_at", iso()),
                            mark_ts_prec="COLLECTION_ONLY",
                        )
                        if row.get("mark_price") is not None:
                            index = current.get("indexPrice", current.get("oraclePx")) if current else None
                            row.update(
                                index_price=str(index) if index is not None else None,
                                **norm.timestamp_fields(
                                    "index_as_of",
                                    raw.get("_capture_observed_at", iso()) if index is not None else None,
                                    "COLLECTION_ONLY" if index is not None else None,
                                ),
                            )
                        await self._put(
                            "mkt.oi_obs", instrument_id + ":" + row["metric_key"] + ":" + row["ts"], row
                        )
                    if current and mark is not None:
                        from .streams import save_mark

                        await save_mark(
                            self,
                            instrument,
                            mark,
                            current.get("_capture_observed_at", iso()),
                            index=current.get("indexPrice", current.get("oraclePx")),
                            index_ts=current.get("index_time", current.get("_capture_observed_at", iso())),
                            source="PUBLIC_REST",
                            received_ts=current.get("_capture_observed_at"),
                            index_clock_basis="SOURCE_TIME"
                            if current.get("index_time")
                            else "COLLECTION_TIME",
                        )
                    if current:
                        await self._funding(instrument, current, "CURRENT_ESTIMATE", interval)
                    predecessor, settled = self._fresh(
                        instrument_id,
                        "settled",
                        settled,
                        lambda r: r.get("fundingTime", r.get("fundingRateTimestamp", r.get("time"))),
                    )
                    ordered = sorted(
                        settled,
                        key=lambda r: parse_time(
                            r.get("fundingTime", r.get("fundingRateTimestamp", r.get("time")))
                        ),
                    )
                    previous = predecessor
                    for raw in ordered:
                        stamp = parse_time(
                            raw.get("fundingTime", raw.get("fundingRateTimestamp", raw.get("time")))
                        )
                        historical_interval = (
                            Decimal(str((stamp - previous).total_seconds())) / 3600
                            if previous is not None
                            else Decimal(1)
                            if norm.venue_key(instrument) == "hyperliquid.perp"
                            else None
                        )
                        previous = stamp
                        if historical_interval is None:
                            await self._gap(
                                instrument,
                                "FUNDING_INTERVAL",
                                "Oldest settled funding interval unobserved",
                                start=stamp,
                                end=stamp,
                            )
                            continue
                        await self._funding(instrument, raw, "SETTLED", historical_interval)
            except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                await self._gap(instrument, kind.upper(), exc)
        # Typed rows are written by the supervisor's flush every RAW_FLUSH_S; 181 pollers
        # each taking the flush lock would stall the stream consumers.

    def _fresh(self, instrument_id, kind, rows, stamp):
        """Points of a repeated REST history newer than those processed before, and the
        time of the newest older one (a settled rate's interval starts at it)."""
        key = (instrument_id, kind)
        last = self._history_marks.get(key)
        timed = [(parse_time(stamp(row)), row) for row in rows]
        if timed:
            newest = max(at for at, _ in timed)
            self._history_marks[key] = newest if last is None else max(last, newest)
        if last is None:
            return None, list(rows)
        older = [at for at, _ in timed if at <= last]
        return (max(older) if older else None), [row for at, row in timed if at > last]

    def context_due(self, instrument_id, kind):
        """Stream context (mark, index, funding estimate, OI) is stored once per CONTEXT_S."""
        now = time.monotonic()
        key = (instrument_id, kind)
        if self._context_due.get(key, 0) > now:
            return False
        self._context_due[key] = now + CONTEXT_S
        return True

    async def _funding(self, instrument, raw, kind, interval, *, source="PUBLIC_REST"):
        row = norm.funding(instrument, raw, kind=kind, interval_h=interval)
        row["capture_source"] = source
        row["source_path"] = norm.venue_key(instrument) + (
            ":websocket:funding" if source == "WEBSOCKET" else ":rest:funding"
        )
        ts = row["as_of"]
        key = instrument["instrument_id"] + ":" + kind + ":" + ts
        await self._ensure_market_restore()
        previous = self._market_cache.get("mkt.funding_obs", {}).get(key)
        observations = dict((previous or {}).get("source_observations") or {})
        if previous and not observations:
            prior_source = previous.get(
                "capture_source", "ARCHIVE" if previous.get("source_archive") else "UNKNOWN"
            )
            observations[prior_source] = dict(previous)
        metadata = {
            "recorded_at",
            "recorded_at_prec",
            "recorded_at_prec_reason",
            "received_ts",
            "code_version",
            "config_version",
        }

        def content(value):
            return {name: item for name, item in value.items() if name not in metadata}

        prior = observations.get(source)
        if prior is None or sha256(content(prior)) != sha256(content(row)):
            observations[source] = dict(row, **collection_clock(iso()).fields("recorded_at"))
        primary = "PUBLIC_REST" if kind == "SETTLED" else "WEBSOCKET"
        if primary not in observations:
            primary = "PUBLIC_REST" if "PUBLIC_REST" in observations else next(iter(observations))
        merged = dict(observations[primary])
        values = {
            name: {field: value[field] for field in ("rate", "interval_h", "rate_8h_eq")}
            for name, value in observations.items()
        }
        signatures = {
            tuple(norm.decimal(value[field]) for field in ("rate", "interval_h", "rate_8h_eq"))
            for value in observations.values()
        }
        merged.update(
            source_observations=observations,
            primary_source=primary,
            source_disagreement={"present": len(signatures) > 1, "values": values},
        )
        await self._put("mkt.funding_obs", key, merged)

    async def _store_kline(self, instrument, row):
        """Enrich missing exchange quote volume only with complete matching prints."""
        iid = instrument["instrument_id"]
        if row.get("quote_volume") is None:
            flow = self._market_cache.get("mkt.flow_bars_1m", {}).get(iid + ":" + row["open_time"])
            if flow and flow.get("complete") and flow.get("quote_volume") is not None:
                with localcontext() as context:
                    context.prec = 100
                    quantity = Decimal(str(flow["buy_qty"])) + Decimal(str(flow["sell_qty"]))
                if quantity == Decimal(str(row["volume_native"])):
                    row["quote_volume"] = flow["quote_volume"]
                    row["quote_volume_method"] = "COMPLETE_MATCHING_RAW_PRINTS"
                    if flow.get("volume_usd") is not None:
                        row["quote_volume_usd"] = flow["volume_usd"]
                        row["quote_volume_usd_reason"] = None
        self._kline_day_counts.pop(
            (iid, parse_time(row["open_time"]).replace(hour=0, minute=0, second=0, microsecond=0)), None
        )
        await self._put("mkt.klines_1m", iid + ":" + row["open_time"], row)

    async def save_ticker_observation(self, instrument, row):
        """Merge partial payloads for one actual (instrument, observation time).

        REST volume and stream/REST mark prices share the table's natural identity.
        Legacy mark-prefixed storage keys remain usable across upgrade/restart.
        """
        row = dict(jsonable(row), **norm.timestamp_fields("ts", row["ts"], row.get("ts_prec")))
        iid = instrument["instrument_id"]
        key = iid + ":" + row["ts"]
        await self._ensure_market_restore()
        previous = self._market_cache.get("mkt.tickers_24h", {}).get(key)
        row = dict(row)
        incoming_sources = dict(row.pop("field_sources", {}) or {})
        for fact in incoming_sources.values():
            basis = fact.get("clock_basis")
            fact["as_of_prec"] = (
                "EXACT"
                if basis == "SOURCE_TIME"
                else "COLLECTION_ONLY"
                if basis == "COLLECTION_TIME"
                else None
            )
            fact.update(norm.timestamp_fields("as_of", fact.get("as_of"), fact["as_of_prec"]))
            fact.update(
                norm.timestamp_fields(
                    "received_ts",
                    fact.get("received_ts"),
                    "COLLECTION_ONLY" if fact.get("received_ts") else None,
                )
            )
        sources = dict((previous or {}).get("field_sources") or {})
        unknown = dict((previous or {}).get("unclocked_components") or {})
        for component in ("mark", "index"):
            price_field, as_of = component + "_price", component + "_as_of"
            if row.get(price_field) is not None and not row.get(as_of):
                unknown[price_field] = {
                    "value": row[price_field],
                    "as_of": None,
                    "received_ts": row.get("received_ts"),
                    "capture_source": row.get("capture_source"),
                    "reason": "SOURCE_CLOCK_UNAVAILABLE",
                }
                row.pop(price_field, None)
                row.pop(as_of, None)
                incoming_sources.pop(price_field, None)
            elif (
                previous
                and previous.get(as_of)
                and row.get(as_of)
                and parse_time(row[as_of]) < parse_time(previous[as_of])
            ):
                row.pop(price_field, None)
                row.pop(as_of, None)
                incoming_sources.pop(price_field, None)
        merged = dict(previous or {})
        for field, value in row.items():
            if field in ("capture_source", "capture_sources"):
                continue  # Aggregate lineage is derived from the retained fields.
            if value is not None or merged.get(field) is None:
                merged[field] = value
                if value is not None and field in incoming_sources:
                    incoming = incoming_sources[field]
                    prior_source = sources.get(field, {})

                    def comparable(fact):
                        return {name: item for name, item in fact.items() if name != "received_ts"}

                    if (
                        not previous
                        or previous.get(field) != value
                        or comparable(prior_source) != comparable(incoming)
                    ):
                        sources[field] = incoming
        # Legacy rows may lack field-level lineage. Do not attribute their
        # retained fields to the newest partial source.
        for field in ("mark_price", "index_price", "last", "quote_volume", "volume_native", "volume_usd"):
            if merged.get(field) is not None and field not in sources:
                clock = merged.get(
                    "mark_as_of"
                    if field == "mark_price"
                    else "index_as_of"
                    if field == "index_price"
                    else "ts"
                )
                sources[field] = {
                    "capture_source": None,
                    "legacy_capture_source": (previous or {}).get("capture_source"),
                    "as_of": clock,
                    "received_ts": (previous or {}).get("received_ts"),
                    "clock_basis": "LEGACY_SOURCE_UNSPECIFIED",
                }
        merged["field_sources"] = sources
        for field, fact in sources.items():
            basis = fact.get("clock_basis")
            fact["as_of_prec"] = (
                "EXACT"
                if basis == "SOURCE_TIME"
                else "COLLECTION_ONLY"
                if basis == "COLLECTION_TIME"
                else None
            )
            fact.update(norm.timestamp_fields("as_of", fact.get("as_of"), fact["as_of_prec"]))
            fact.update(
                norm.timestamp_fields(
                    "received_ts",
                    fact.get("received_ts"),
                    "COLLECTION_ONLY" if fact.get("received_ts") else None,
                )
            )
            if field in ("mark_price", "index_price"):
                name = field.removesuffix("_price") + "_as_of"
                merged.update(norm.timestamp_fields(name, merged.get(name), fact["as_of_prec"]))
        if merged.get("received_ts"):
            merged.update(collection_clock(merged["received_ts"]).fields("received_ts"))
        if unknown:
            merged["unclocked_components"] = unknown
        transports = sorted(
            {value["capture_source"] for value in sources.values() if value.get("capture_source")}
        )
        merged["capture_sources"] = transports
        merged["source_provenance_complete"] = bool(sources) and all(
            value.get("capture_source") for value in sources.values()
        )
        merged["capture_source"] = (
            transports[0] if len(transports) == 1 and merged["source_provenance_complete"] else None
        )
        if merged.get("index_price") is not None:
            merged["index_reason"] = None
        if merged.get("volume_usd") is not None:
            merged["usd_reason"] = None
        return await self._put("mkt.tickers_24h", key, merged)

    async def _ticker(self, instrument, raw):
        received = raw.get("_capture_observed_at", iso())
        await self._raw(instrument, "ticker", raw)
        key = norm.venue_key(instrument)
        if key.startswith("binance"):
            native, quote, price = raw.get("volume"), raw.get("quoteVolume"), raw.get("lastPrice")
        elif key.startswith("okx"):
            native, quote, price = raw.get("vol24h"), raw.get("volCcy24h"), raw.get("last")
        elif key.startswith("bybit"):
            native, quote, price = raw.get("volume24h"), raw.get("turnover24h"), raw.get("lastPrice")
        elif key.startswith("coinbase"):
            native, quote, price = raw.get("volume"), None, raw.get("last")
        elif key.startswith("kraken"):
            native, quote, price = raw["v"][1], None, raw["c"][0]
        else:
            native, quote, price = None, raw.get("dayNtlVlm"), raw.get("markPx")
        # OKX swap vol24h is contracts, volCcy24h is BASE rather than quote.
        if key == "okx.swap":
            native = raw.get("volCcy24h")
            quote = None
        fx = instrument.get("quote_usd_rate")
        usd = norm.decimal(quote) * norm.decimal(fx) if quote is not None and fx is not None else None
        row = {
            "instrument_id": instrument["instrument_id"],
            "token_id": instrument["token_id"],
            "ts": iso(raw.get("ts", raw.get("closeTime", received))),
            "ts_prec": "EXACT"
            if raw.get("ts") is not None or raw.get("closeTime") is not None
            else "COLLECTION_ONLY",
            "received_ts": received,
            "received_ts_prec": "COLLECTION_ONLY",
            "volume_native": native,
            "quote_volume": quote,
            "volume_usd": usd,
            "last": price,
            "mark_price": raw.get("markPrice", raw.get("markPx")),
            "quote_asset": instrument.get("quote_asset"),
            "usd_reason": None if usd is not None else "24H_QUOTE_NOTIONAL_UNAVAILABLE",
        }
        source_path = key + (
            ":rest:metaAndAssetCtxs"
            if key == "hyperliquid.perp"
            else ":rest:tickers"
            if key.startswith("bybit")
            else ":rest:ticker"
        )
        row["capture_source"] = "PUBLIC_REST"
        row["field_sources"] = {
            field: {
                "capture_source": "PUBLIC_REST",
                "source_path": source_path,
                "as_of": row["ts"],
                "received_ts": received,
                "clock_basis": "SOURCE_TIME"
                if raw.get("ts") is not None or raw.get("closeTime") is not None
                else "COLLECTION_TIME",
            }
            for field in ("volume_native", "quote_volume", "volume_usd", "last")
            if row.get(field) is not None
        }
        if row["mark_price"] is not None:
            # REST tickers do not expose an independent exchange mark timestamp.
            # This is the actual HTTP observation clock, not an invented update time.
            row["mark_as_of"] = received
            row["field_sources"]["mark_price"] = {
                "capture_source": "PUBLIC_REST",
                "source_path": source_path,
                "as_of": received,
                "received_ts": received,
                "clock_basis": "COLLECTION_TIME",
            }
        await self.save_ticker_observation(instrument, row)

    async def ingest_trade(self, instrument, raw, *, backfill=False, row=None):
        """`row` is `raw` already normalized with the same source (a backfill page)."""
        row = row if row is not None else norm.trade(instrument, raw, source="REST" if backfill else "WS")
        iid = instrument["instrument_id"]
        await self._ensure_market_restore()
        minute = iso(parse_time(row["ts"]).replace(second=0, microsecond=0))
        bar_key = (iid, minute)
        canonical = self.trade_archive.canonical(row)
        if self.trade_archive.needs_load(canonical):
            await self._load_trade_hour(canonical)
        late = bar_key not in self.flow.bars and self.trade_archive.minute_archived(iid, minute)
        row.update(config_version=self.settings.config_version, code_version=self.settings.code_version)
        if not self.trade_archive.append(row):
            return False
        if backfill:
            return True
        if late:
            # The minute's bar already left memory: rebuild it from its archived prints.
            fresh = bar_key not in self.flow.bars
            self._merge_prints(bar_key, await self._archived_minute(iid, minute))
            if fresh:
                self.flow.bars[bar_key]["_coverage"] = self.trade_archive.coverage_for(iid, minute)
        else:
            self.flow.add(row, continuity=True, emit_record=False)
        checkpoint = self._capture_checkpoints.get(iid)
        if checkpoint is None or parse_time(row["ts"]) >= parse_time(checkpoint["last_ts"]):
            self._capture_checkpoints[iid] = {
                "instrument_id": iid,
                "last_trade_id": row["trade_id"],
                "last_trade": self.flow.trade_point(row),
                "last_ts": row["ts"],
            }
        return True

    async def _load_trade_hour(self, canonical):
        """Load once the identities of an hour an earlier process wrote; waiters share it."""
        task = self._hour_loads.get(canonical)
        if task is None:
            task = asyncio.ensure_future(asyncio.to_thread(self.trade_archive.load_ids, canonical))
            self._hour_loads[canonical] = task
        try:
            ids = await task
        finally:
            if task.done():
                self._hour_loads.pop(canonical, None)
        if self.trade_archive.needs_load(canonical):
            self.trade_archive.mark_loaded(canonical, ids)

    def _merge_prints(self, key, rows):
        """Add archived prints to a bar, skipping prints it already holds (a concurrent
        rebuild or a live print may have created the bar during the read)."""
        held = {str(t["trade_id"]) for t in self.flow.bars.get(key, {}).get("_trades", ())}
        for point in rows:
            if str(point["trade_id"]) not in held:
                held.add(str(point["trade_id"]))
                self.flow.add(point, continuity=False, emit_record=False)

    async def _archived_minute(self, iid, minute, *, hours=None):
        """Every print of one minute this capture holds: durable journals, plus rows
        buffered or being written. It never waits for a flush (stream consumers call it)."""
        hour = iso(parse_time(minute).replace(minute=0, second=0, microsecond=0))
        # Taken before the read: a write that completes meanwhile moves its rows to disk.
        pending = self.trade_archive.buffered(iid, minute)
        if hours is None or hour not in hours:
            grouped = await asyncio.to_thread(self.trade_archive.read_minutes, iid, hour)
            if hours is not None:
                hours.clear()
                hours[hour] = grouped
        else:
            grouped = hours[hour]
        rows = {str(r["trade_id"]): r for r in grouped.get(iso(parse_time(minute)), ())}
        for row in pending + self.trade_archive.buffered(iid, minute):
            rows.setdefault(str(row["trade_id"]), row)
        return sorted(rows.values(), key=lambda r: (r["ts"], str(r["trade_id"])))

    def summary_due(self, instrument_id):
        return self._book_due.get(instrument_id, 0) <= time.time()

    async def _save_book(self, instrument):
        iid = instrument["instrument_id"]
        if not self.books[iid].verified:
            return  # summarize only a checksum-verified state (the next one is <= 1 s away)
        # Summaries on a fixed wall-clock grid, from the first update at or after each grid
        # point, so a cutoff finds one younger than the 10 s cadence.
        now = time.time()
        if self._book_due.get(iid, 0) > now:
            return
        # The grid is a little finer than the cadence: frame gaps and loop lag (r109:
        # hyperliquid books 10.28 s old) must not push the age past it.
        interval = float(self.thresholds.get("book_save_interval_s", BOOK_SAVE_S))
        self._book_due[iid] = (now // interval + 1) * interval
        # Summarize a frozen copy in the thread pool; a crossed book found there is
        # recorded on the live book exactly as an in-place summary would.
        frozen = self.books[iid].frozen()
        row = await asyncio.to_thread(frozen.summary)
        if not frozen.integrity_ok and self.books[iid].integrity_ok:
            self.books[iid].integrity_ok, self.books[iid].gap_reason = False, frozen.gap_reason
        self.book_archive.add(
            iid,
            "BOOK_SUMMARY",
            {k: v for k, v in row.items() if k not in ("bids", "asks")},
            row["received_ts"],
        )
        pending_snapshot = getattr(self.books[iid], "pending_snapshot", None)
        if pending_snapshot is not None:
            self.book_archive.add(iid, "BOOK_SNAPSHOT", pending_snapshot, pending_snapshot["received_ts"])
            self.books[iid].pending_snapshot = None
        key = iid + ":" + row["exchange_ts"]
        listed = norm.venue_key(instrument) in ("binance.spot", "coinbase.spot")
        row.update(
            second_path_confirmed=False,
            second_path_reason="REST_CONFIRMATION_PENDING" if listed else "NO_LISTED_SECOND_PATH",
        )
        await self._put("mkt.book_summaries", key, row)
        if not row["integrity_ok"] or not listed:
            return
        previous = self._book_confirmations.get(iid)
        if previous is not None and not previous.done():
            return

        async def confirm():
            try:
                row.update(
                    await asyncio.wait_for(
                        self._independent_book(instrument, row),
                        timeout=float(self.thresholds.get("book_interval_s", 10)),
                    )
                )
            except Exception as exc:  # noqa: BLE001 - confirmation failure must not stall capture
                row.update(second_path_confirmed=False, second_path_reason=str(exc))
            await self._put("mkt.book_summaries", key, row)

        task = asyncio.create_task(confirm())
        self._book_confirmations[iid] = task
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _flush_flow(self, *, allow_recovery=True):
        now = utcnow()
        for iid, since in list(self._connected_since.items()):
            observed = self._stream_watermarks.get(iid)
            if not observed:
                continue
            through = min(parse_time(observed["ts"]), now)
            cursor = self._zero_cursor.get(iid, since.replace(second=0, microsecond=0))
            while cursor <= through.replace(second=0, microsecond=0):
                key = (iid, iso(cursor))
                if key not in self.flow.bars:
                    if not allow_recovery:
                        cursor += timedelta(minutes=1)
                        continue
                    # A minute whose prints were archived but whose bar left memory is
                    # rebuilt from them; any other unseen connected minute is empty.
                    rows = (
                        await self._archived_minute(iid, cursor)
                        if self.trade_archive.minute_archived(iid, cursor)
                        else []
                    )
                    if key not in self.flow.bars:  # no live print arrived during the read
                        self.flow.empty_minute(self.instruments[iid], cursor)
                        self.flow.bars[key]["_coverage"] = self.trade_archive.coverage_for(iid, cursor)
                        if rows:
                            self.flow.bars.pop(key)
                            self._merge_prints(key, rows)
                            self.flow.bars[key]["_coverage"] = self.trade_archive.coverage_for(iid, cursor)
                    else:
                        self._merge_prints(key, rows)
                bar = self.flow.bars[key]
                start, end = max(since, cursor), min(through, cursor + timedelta(minutes=1))
                if end > start:
                    complete = bar.get("gap_reason") is None
                    self.flow.prove_range(
                        iid,
                        start,
                        end,
                        complete=complete,
                        source="CONTINUOUS_STREAM",
                        reason=bar.get("gap_reason"),
                    )
                    self.trade_archive.coverage(
                        iid, start, end, complete=complete, reason=bar.get("gap_reason")
                    )
                cursor += timedelta(minutes=1)
            self._zero_cursor[iid] = max(
                since.replace(second=0, microsecond=0),
                through.replace(second=0, microsecond=0) - timedelta(minutes=1),
            )
        # Only bars that changed since the last flush are recorded again.
        dirty, self.flow.dirty = self.flow.dirty, set()
        for key, bar in list(self.flow.bars.items()):
            minute = parse_time(key[1])
            if key in dirty:
                coverage = bar.get("_coverage", [])
                covered = self.flow.covered(minute, minute + timedelta(minutes=1), coverage)
                complete = covered and not bar.get("gap_reason")
                row = self.flow.record(key, complete=complete)
                if not covered and not row.get("gap_reason"):
                    # Persist why the minute is unproven (stream down, REST repair pending);
                    # the in-memory bar keeps gap_reason None so a later proof completes it.
                    row["gap_reason"] = next(
                        (
                            p["reason"]
                            for p in reversed(coverage)
                            if p.get("complete") is not True and p.get("reason")
                        ),
                        "STREAM_COVERAGE_UNPROVEN",
                    )
                await self._put("mkt.flow_bars_1m", key[0] + ":" + key[1], row)
            if minute + timedelta(minutes=3) < now:
                self.flow.bars.pop(key, None)
                self._market_cache.get("mkt.flow_bars_1m", {}).pop(key[0] + ":" + key[1], None)

    @staticmethod
    def _ranges_cover(ranges, start, end):
        cursor = start
        for lo, hi in sorted(ranges):
            if lo > cursor:
                return False
            cursor = max(cursor, hi)
            if cursor >= end:
                return True
        return cursor >= end

    async def _persist_rest_flow_ranges(self, instrument, start, end, proven_ranges):
        """Audit newly captured REST evidence and replace whole bars in batches."""
        if not proven_ranges:
            return
        iid = instrument["instrument_id"]
        for lo, hi in proven_ranges:
            self.trade_archive.coverage(iid, lo, hi, complete=True, source="REST_RANGE_EXHAUSTED")
        await self.flush_market(raw=True)  # Raw journals are durable before any derived bar.
        previous = self.flow.references.get((iid, iso(start)), {}).get("last_trade")
        minute = parse_time(start).replace(second=0, microsecond=0)
        hours = {}  # the one archived hour this audit is reading
        # Complete bars already held for the range: a REST audit with the same prints
        # proves them without a rewrite. A rewrite moved recorded_at past snapshots
        # assembled meanwhile (r113: 48 complete coinbase WS bars re-recorded at 05:10).
        held = {
            row["minute_start"]: row
            async for row in self.repo.iter_market_projection(
                "mkt.flow_bars_1m",
                ["minute_start", "complete", *REST_SAME_FLOW],
                "minute_start",
                start=minute,
                end=parse_time(end),
                instrument_ids=[iid],
                batch_size=5000,
            )
        }
        for row in self._pending_market.get("mkt.flow_bars_1m", {}).values():
            if row["instrument_id"] == iid and minute <= parse_time(row["minute_start"]) < parse_time(end):
                held[row["minute_start"]] = row
        consecutive = norm.venue_key(instrument).split(".")[0] in ("binance", "coinbase", "kraken")

        def audit(minute, rows, coverage, boundaries, previous):
            """One minute's bar from its archived prints, built in a worker thread: it reads
            only its own copies, and the loop applies the trade-ID gaps it reports."""
            accumulator = FlowAccumulator()
            accumulator.boundaries = boundaries
            accumulator.references = self.flow.references
            accumulator.last[iid] = previous if previous else {}
            if not previous:
                accumulator.last.pop(iid, None)
            key = (iid, iso(minute))
            accumulator.empty_minute(instrument, minute)
            gap = None
            gaps = []
            if consecutive:
                # Check continuity in ID order: the archive's (ts, str id) order misplaces ids
                # across a digit boundary, or when WS and REST prints differ in ts precision.
                rows.sort(key=lambda r: int(r["trade_id"]))
            for point in rows:
                if (
                    previous
                    and consecutive
                    and point["trade_id"] != previous["trade_id"]
                    and int(point["trade_id"]) != int(previous["trade_id"]) + 1
                ):
                    gap = "REST_TRADE_ID_GAP"
                    gaps.append((previous["ts"], point["ts"]))
                accumulator.add(point, continuity=False, emit_record=False)
                previous = point
            bar = accumulator.bars[key]
            bar["_coverage"] = coverage
            bar["gap_reason"] = gap
            span = None
            if gap:
                span = max(parse_time(start), minute), min(parse_time(end), minute + timedelta(minutes=1))
                bar["_coverage"].append(
                    {"start": iso(span[0]), "end": iso(span[1]), "complete": False, "reason": gap}
                )
            complete = (
                accumulator.covered(minute, minute + timedelta(minutes=1), bar["_coverage"]) and gap is None
            )
            bar["_rest_verified"] = complete
            row = accumulator.record(key, complete=complete)
            row.update(backfill_verified=complete, capture_method="REST_RANGE_EXHAUSTED")
            if not complete and not row.get("gap_reason"):
                # Part of the minute lies outside every proof this process holds.
                row["gap_reason"] = next(
                    (
                        p["reason"]
                        for p in reversed(bar["_coverage"])
                        if p.get("complete") is not True and p.get("reason")
                    ),
                    "STREAM_COVERAGE_UNPROVEN",
                )
            return key, bar, row, gaps, span, previous

        while minute < parse_time(end):
            rows = await self._archived_minute(iid, minute, hours=hours)
            coverage = self.trade_archive.coverage_for(iid, minute)
            boundaries = {
                key: set(points)
                for key in ((iid, iso(minute)),)
                if (points := self.flow.boundaries.get(key)) is not None
            }
            key, bar, row, gaps, span, previous = await asyncio.to_thread(
                audit, minute, rows, coverage, boundaries, previous
            )
            for lo, hi in gaps:
                await self.mark_capture_gap(instrument, lo, hi, "REST_TRADE_ID_GAP", preserve_verified=False)
            if span:
                self.trade_archive.coverage(iid, *span, complete=False, reason="REST_TRADE_ID_GAP")
            stored = held.get(iso(minute))
            if (
                bar["_rest_verified"]
                and stored
                and stored.get("complete") is True
                and _same_flow(stored, row)
            ):
                self._rest_verified[key] = True
                while len(self._rest_verified) > REST_VERIFIED_KEEP:
                    self._rest_verified.pop(next(iter(self._rest_verified)))
            else:
                await self._put("mkt.flow_bars_1m", iid + ":" + iso(minute), row)
            if minute >= utcnow() - timedelta(minutes=3):
                self.flow.bars[key] = bar
            await asyncio.sleep(0)  # one minute's bar at a time: a repaired hour never holds the loop
            if minute.minute == 59:
                await self.flush_market(raw=False)
            minute += timedelta(minutes=1)
        await self.flush_market(raw=False)

    def _register_capture_boundaries(self, ids, cutoff, anchor=None):
        end = parse_time(cutoff)
        points = {end}
        for minutes in (15, 60, 240, 1440):
            points.update((end - timedelta(minutes=minutes), end - timedelta(minutes=2 * minutes)))
        if anchor is not None:
            at = parse_time(anchor.get("ts") if isinstance(anchor, dict) else anchor)
            points.update((at, at - (end - at)))
        for iid in ids:
            self.flow.register_boundaries(iid, points)

    def _record_reference(self, instrument, boundary, raw, source):
        point = norm.trade(instrument, raw, source="REST")
        if parse_time(point["ts"]) >= parse_time(boundary):
            raise norm.SemanticError("Reference trade must be strictly before requested boundary")
        iid = instrument["instrument_id"]
        self.flow.register_boundaries(iid, [boundary])
        self.flow.references[iid, iso(boundary)] = {
            **norm.timestamp_fields("boundary", boundary, "EXACT"),
            "complete": True,
            "last_trade": self.flow.trade_point(point),
            **collection_clock().fields("observed_at"),
            "coverage_reason": None,
            "source": source,
        }

    async def backfill(self, instrument, anchor, *, checkpoint=None):
        iid = instrument["instrument_id"]
        self._active_backfills[iid] = self._active_backfills.get(iid, 0) + 1
        try:
            return await self._backfill(instrument, anchor, checkpoint=checkpoint)
        finally:
            self._active_backfills[iid] -= 1
            if not self._active_backfills[iid]:
                self._active_backfills.pop(iid)
            await self.flush_market()
            queued = self._pending_reconnects.pop(iid, None)
            if queued and not self._active_backfills.get(iid) and not (self._stop and self._stop.is_set()):
                task = asyncio.create_task(self.backfill(instrument, iso(), checkpoint=queued))
                self._background.add(task)
                task.add_done_callback(self._background.discard)

    async def _backfill(self, instrument, anchor, *, checkpoint=None):
        """Page immutable prints. Unproven endpoints/retention remain explicit gaps."""
        key = norm.venue_key(instrument)
        start = parse_time(checkpoint["last_ts"]) if checkpoint else parse_time(anchor) - timedelta(hours=24)
        end = parse_time(anchor)
        # PRE24 itself uses start as its return anchor, requiring an actual
        # print strictly before start. Keep the analysis window unchanged.
        capture_start = start if checkpoint else start - timedelta(minutes=1)
        self._register_capture_boundaries({instrument["instrument_id"]}, end, {"ts": start})
        # A reconnect repair is short and completes live minutes; behind base polling
        # (priority 0) it never got a Kraken slot (1 req/s) on prod.
        adapter = self.adapter(instrument, priority=0 if checkpoint else 1)
        cursor = None
        seen = set()
        times = []
        proven_ranges = []
        kraken_forward_ranges = []
        first_window_trade = None
        reference_candidate = None
        # Binance timestamp slices cannot exceed one hour. Others paginate to oldest print.
        slices = (
            [
                (start + timedelta(hours=i), min(start + timedelta(hours=i + 1), end))
                for i in range(max(1, int((end - start).total_seconds() // 3600) + 1))
                if start + timedelta(hours=i) < end
            ]
            if key.startswith("binance")
            else [(capture_start, end)]
        )
        try:
            for slice_index, (lo, hi) in enumerate(slices):
                cursor = (
                    int(checkpoint["last_trade_id"]) + 1
                    if checkpoint
                    and checkpoint.get("last_trade_id")
                    and key.startswith("binance")
                    and slice_index == 0
                    else checkpoint.get("end_cursor")
                    if checkpoint and not key.startswith("binance")
                    else None
                )
                exhausted = False
                for page in range(10000):
                    rows, next_cursor = await adapter.trades(
                        start=lo, end=hi - timedelta(milliseconds=1), cursor=cursor
                    )
                    if not rows:
                        if key.startswith("binance"):
                            exhausted = True
                        break
                    await self._raw(
                        instrument,
                        "backfill_trades",
                        {
                            "request": {
                                "start": iso(lo),
                                "end": iso(hi - timedelta(milliseconds=1)),
                                "cursor": cursor,
                            },
                            "response": rows,
                        },
                    )
                    # A page is normalized once, in a worker thread (r113: twice, on the loop).
                    normalized = await asyncio.to_thread(
                        lambda rows=rows: sorted(
                            ((norm.trade(instrument, raw, source="REST"), raw) for raw in rows),
                            key=lambda x: parse_time(x[0]["ts"]),
                        )
                    )
                    for t, raw in normalized:
                        ts = parse_time(t["ts"])
                        if ts < start and (
                            reference_candidate is None or ts > parse_time(reference_candidate[0]["ts"])
                        ):
                            reference_candidate = (t, raw)
                        if (
                            key.startswith(("binance", "kraken"))
                            and start <= ts < end
                            and (
                                first_window_trade is None
                                or int(t["trade_id"]) < int(first_window_trade["trade_id"])
                            )
                        ):
                            first_window_trade = t
                        if (
                            ts < end
                            and (not checkpoint or capture_start <= ts)
                            and (not key.startswith("binance") or lo <= ts < hi)
                        ):
                            times.append(ts)
                            await self.ingest_trade(instrument, raw, backfill=True, row=dict(t))
                    # A flush per page waited on the shared fsynced journal write (prod r106:
                    # ~5k prints/min); the prints of PAGES_PER_FLUSH pages are bounded memory.
                    if page % PAGES_PER_FLUSH == PAGES_PER_FLUSH - 1:
                        await self.flush_market()
                    await asyncio.sleep(0)  # one page at a time: streams and polls keep their turns
                    if key.startswith("binance"):
                        if len(rows) < 1000 or parse_time(normalized[-1][0]["ts"]) >= hi:
                            exhausted = True
                            break
                        next_cursor = int(normalized[-1][0]["trade_id"]) + 1
                    elif key.startswith("kraken"):
                        if parse_time(normalized[-1][0]["ts"]) >= end:
                            exhausted = bool(times and min(times) <= start)
                            if not exhausted:
                                kraken_forward_ranges.append((lo, min(hi, utcnow())))
                            break
                    elif parse_time(normalized[0][0]["ts"]) <= start:
                        exhausted = True
                        break
                    if next_cursor is None or str(next_cursor) in seen:
                        break
                    seen.add(str(next_cursor))
                    cursor = next_cursor
                if exhausted:
                    proven_ranges.append((lo, min(hi, utcnow())))
        except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
            await self._gap(instrument, "PRE_EVENT_FLOW", exc, start=start, end=end)
        # Verified Binance print archives cover only fully published historical days.
        # Retention/ID gaps are retained as evidence even if an archive repairs coverage.
        # Fresh windows come from REST only (03 §5.1, lead ruling 2026-10-20): whole daily
        # archives are parsed only for replays of windows older than REST_ONLY_WINDOW.
        if not checkpoint and key.startswith("binance") and end < utcnow() - REST_ONLY_WINDOW:
            missing = not self._ranges_cover(proven_ranges, start, end)
            if missing:
                from .archive import fetch_archive

                day = start.date()
                while day <= end.date():
                    if day < utcnow().date():
                        try:
                            await fetch_archive(
                                self.repo,
                                self.settings,
                                instrument["venue_symbol"],
                                day.isoformat(),
                                market_type=instrument["market_type"],
                                kind="aggTrades",
                                http=self.http,
                                market=self,
                                window=(capture_start, end),
                            )
                        except Exception as exc:  # noqa: BLE001 - archive absence preserves explicit missing historical coverage
                            await self._gap(
                                instrument,
                                "TRADE_ARCHIVE",
                                exc,
                                start=day.isoformat(),
                                end=iso(parse_time(day.isoformat()) + timedelta(days=1)),
                            )
                    day += timedelta(days=1)
        if not checkpoint and reference_candidate and not key.startswith("binance"):
            self._record_reference(instrument, start, reference_candidate[1], "REST_ENUMERATION")
        # Binance aggregate IDs expose the actual preceding prints, including
        # quiet minutes: walk backwards by ID, never substitute a kline close.
        if not checkpoint and key == "binance.spot" and first_window_trade is None:
            # Official Spot REST ordering: endTime alone returns most recent
            # records up to that bound. Covers a wholly quiet PRE24 window.
            try:
                rows, _ = await adapter.trades(end=start - timedelta(milliseconds=1))
                await self._raw(
                    instrument,
                    "pre24_reference_trades",
                    {"end": iso(start - timedelta(milliseconds=1)), "response": rows},
                )
                earlier = [(norm.trade(instrument, raw), raw) for raw in rows]
                earlier = [pair for pair in earlier if parse_time(pair[0]["ts"]) < start]
                if earlier:
                    _, raw = max(earlier, key=lambda pair: int(pair[0]["trade_id"]))
                    await self.ingest_trade(instrument, raw, backfill=True)
                    self._record_reference(instrument, start, raw, "BINANCE_END_BOUND")
            except Exception as exc:  # noqa: BLE001 - source limitations remain gaps
                await self._gap(instrument, "PRE24_REFERENCE", exc, start=None, end=start)
        if not checkpoint and key.startswith("binance") and first_window_trade:
            before_id = int(first_window_trade["trade_id"])
            try:
                for _ in range(10):
                    if before_id <= 0:
                        break
                    lower_id = max(0, before_id - 1000)
                    rows, _ = await adapter.trades(cursor=lower_id)
                    await self._raw(
                        instrument, "pre24_reference_trades", {"fromId": lower_id, "response": rows}
                    )
                    predecessors = []
                    for raw in rows:
                        point = norm.trade(instrument, raw)
                        if int(point["trade_id"]) < before_id and parse_time(point["ts"]) < start:
                            predecessors.append((point, raw))
                    if predecessors:
                        _, raw = max(predecessors, key=lambda item: int(item[0]["trade_id"]))
                        await self.ingest_trade(instrument, raw, backfill=True)
                        self._record_reference(instrument, start, raw, "BINANCE_ID_PREDECESSOR")
                        break
                    before_id = lower_id
            except Exception as exc:  # noqa: BLE001 - preserve actual source failure
                await self._gap(instrument, "PRE24_REFERENCE", exc, start=None, end=start)
        # Backfill OHLC without pretending klines are aggressive flow.
        if (
            not checkpoint
            and key == "kraken.spot"
            and not self.flow.references.get((instrument["instrument_id"], iso(start)))
        ):
            # Kraken exposes a forward `since` cursor rather than a reverse ID.
            # Expand the time search only when a quiet interval has no print,
            # then follow its actual cursor until crossing the requested bound.
            span = timedelta(minutes=1)
            cursor = None
            candidate = None
            query_start = start - span
            for attempt in range(32):
                try:
                    rows, next_cursor = await adapter.trades(start=query_start, cursor=cursor)
                    await self._raw(
                        instrument,
                        "pre24_reference_trades",
                        {
                            "start": iso(query_start),
                            "cursor": cursor,
                            "response": rows,
                        },
                    )
                    points = sorted(
                        ((norm.trade(instrument, raw), raw) for raw in rows),
                        key=lambda pair: parse_time(pair[0]["ts"]),
                    )
                    earlier = [pair for pair in points if parse_time(pair[0]["ts"]) < start]
                    if earlier:
                        candidate = max(
                            [*earlier, *([candidate] if candidate else [])],
                            key=lambda pair: parse_time(pair[0]["ts"]),
                        )
                    crossed = any(parse_time(point["ts"]) >= start for point, _ in points)
                    if candidate and crossed:
                        await self.ingest_trade(instrument, candidate[1], backfill=True)
                        self._record_reference(instrument, start, candidate[1], "KRAKEN_SINCE_CURSOR")
                        break
                    if candidate and next_cursor is not None and str(next_cursor) != str(cursor):
                        cursor = next_cursor
                    else:
                        cursor = None
                        span *= 2
                        query_start = start - min(span, start - parse_time("1970-01-01T00:00:00Z"))
                except Exception as exc:  # noqa: BLE001 - retain source evidence
                    await self._gap(instrument, "PRE24_REFERENCE", exc, start=query_start, end=start)
                    break
            else:
                await self._gap(
                    instrument,
                    "PRE24_REFERENCE",
                    "PREDECESSOR_SEARCH_REQUEST_BUDGET_EXHAUSTED",
                    start=query_start,
                    end=start,
                )
        if kraken_forward_ranges and first_window_trade:
            # A forward since traversal through cutoff can start after the requested
            # boundary during a quiet interval. Native consecutive IDs plus an actual
            # earlier print prove that prefix; a clamped/truncated first page cannot.
            predecessor = self.flow.references.get((instrument["instrument_id"], iso(start)), {}).get(
                "last_trade"
            )
            previous_id = (
                checkpoint.get("last_trade_id")
                if checkpoint
                else (predecessor.get("trade_id") if predecessor else None)
            )
            if previous_id is not None and int(first_window_trade["trade_id"]) == int(previous_id) + 1:
                proven_ranges.extend(kraken_forward_ranges)
        if checkpoint and not self._ranges_cover(proven_ranges, start, end):
            await self._gap(instrument, "RECONNECT", "CHECKPOINT_RANGE_NOT_EXHAUSTED", start=start, end=end)
        # Bybit lists only its last 1000 public prints (03 §5.1): older windows are a
        # source limit, recorded as such rather than as a collection failure.
        limit = (
            " (SOURCE_LIMIT: Bybit recent-trade lists only the last 1000 prints)"
            if key.startswith("bybit")
            else ""
        )
        for hours in () if checkpoint else (24, 4, 1):
            lo = end - timedelta(hours=hours)
            if not self._ranges_cover(proven_ranges, lo, end):
                await self._gap(
                    instrument,
                    "PRE_EVENT_FLOW",
                    f"{hours}h window not fully covered by retained REST trades{limit}",
                    start=lo,
                    end=end,
                )
        if not checkpoint and not self.flow.references.get((instrument["instrument_id"], iso(start))):
            await self._gap(
                instrument,
                "PRE24_REFERENCE",
                "NO_RETAINED_TRADE_BEFORE_PRE24_START",
                start=capture_start,
                end=start,
            )
        await self._persist_rest_flow_ranges(instrument, start, end, proven_ranges)
        await self._backfill_klines(instrument, start, end, include_reference=not checkpoint)
        await self.flush_market()

    async def repair_windows(self, token_id, start, end, *, budget_s=None):
        """Repair every incomplete or missing persisted minute of [start, end) for a token's
        instruments over REST from the last proven print (03 §5.1 fill order, §5.2 step 2),
        within budget_s. Minutes REST cannot reach stay incomplete with their reason.
        Returns {instrument_id: {"incomplete_before", "incomplete_after"}}."""
        if getattr(self.settings, "replay", False):
            return {}  # a replay reads frozen evidence; it never fetches live market data
        deadline = time.monotonic() + budget_s if budget_s else None
        lo = parse_time(start).replace(second=0, microsecond=0)
        # The last finished minute is in (the restart minute of a startup repair); the
        # minute in progress is not.
        hi = min(parse_time(end), utcnow() - timedelta(seconds=5)).replace(second=0, microsecond=0)
        instruments = [
            i for i in self.instruments.values() if i["token_id"] == token_id and i.get("active", True)
        ]
        spot = [i for i in instruments if norm.venue_key(i).endswith(".spot")]
        perp = [i for i in instruments if i not in spot]
        recent = max(lo, hi - REFERENCE_SPAN)
        # One fast spot venue's trailing 24 h first, outside the shared repair slots: it is the
        # 03 §5.7 reference input. Then the other spot venues, derivatives, older history.
        reference = sorted(
            spot, key=lambda i: REFERENCE_VENUES.index(i["venue"]) if i["venue"] in REFERENCE_VENUES else 99
        )[:1]
        phases = [
            (reference, recent, hi, True),
            ([i for i in spot if i not in reference], recent, hi, False),
            (perp, recent, hi, False),
            (instruments, lo, recent, False),
        ]
        results = {}
        for group, a, b, reserved in phases:
            done = await asyncio.gather(
                *(self._repair_instrument(inst, a, b, deadline, reserved=reserved) for inst in group)
            )
            for inst, result in zip(group, done, strict=True):
                total = results.setdefault(
                    inst["instrument_id"], {"incomplete_before": 0, "incomplete_after": 0}
                )
                for name in total:
                    total[name] += result[name]
        return results

    async def _incomplete_runs(self, iid, lo, hi):
        """Contiguous [start, end) runs of minutes without a complete persisted bar, and the
        bars of [lo - 1 min, hi) by minute."""
        rows = await self._range(
            "mkt.flow_bars_1m",
            "minute_start",
            start=lo - timedelta(minutes=1),
            end=hi - timedelta(microseconds=1),
            filters={"instrument_id": iid},
        )
        bars = {parse_time(r["minute_start"]): r for r in rows}
        runs, minute = [], lo
        while minute < hi:
            if not (bars.get(minute) or {}).get("complete"):
                if runs and runs[-1][1] == minute:
                    runs[-1][1] = minute + timedelta(minutes=1)
                else:
                    runs.append([minute, minute + timedelta(minutes=1)])
            minute += timedelta(minutes=1)
        return runs, bars

    async def _repair_instrument(self, instrument, lo, hi, deadline, *, reserved=False):
        iid = instrument["instrument_id"]
        if hi <= lo:
            return {"incomplete_before": 0, "incomplete_after": 0}
        runs, bars = await self._incomplete_runs(iid, lo, hi)
        before = sum((b - a) // timedelta(minutes=1) for a, b in runs)
        venue = instrument["venue"]
        # Newest first, at most an hour per backfill: the event and primary windows come
        # before older history, and every finished hour is persisted (a restart keeps it).
        chunks = [
            (max(start, end - REPAIR_CHUNK * (n + 1)), end - REPAIR_CHUNK * n)
            for start, end in reversed(runs)
            for n in range(-(-(end - start) // REPAIR_CHUNK))
        ]
        for start, end in chunks:
            if venue in NO_TRADE_HISTORY or (venue == "bybit" and end < utcnow() - BYBIT_RECENT):
                continue
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                break
            checkpoint = {"instrument_id": iid, "last_ts": iso(start)}
            previous = bars.get(start - timedelta(minutes=1)) or {}
            if previous.get("complete") and (previous.get("last_trade") or {}).get("trade_id"):
                checkpoint.update(
                    last_trade_id=previous["last_trade"]["trade_id"], last_trade=previous["last_trade"]
                )
            elif venue == "kraken":
                checkpoint.update(await self._kraken_predecessor(instrument, start))
            # Paging venues continue below the first print of the newer minute (just repaired)
            # instead of walking back from now for every chunk.
            following = await self.repo.get("mkt.flow_bars_1m", iid + ":" + iso(end)) or {}
            if venue in ("coinbase", "okx") and (following.get("first_trade") or {}).get("trade_id"):
                checkpoint["end_cursor"] = following["first_trade"]["trade_id"]

            async def repair(end=end, checkpoint=checkpoint):
                if reserved:  # the reference venue never queues behind other repairs
                    await self.backfill(instrument, iso(end), checkpoint=checkpoint)
                    return
                async with self._repair_slots:  # repair CPU never takes the whole loop
                    await self.backfill(instrument, iso(end), checkpoint=checkpoint)

            try:
                await asyncio.wait_for(repair(), remaining)
            except TimeoutError:
                break
            done, _ = await self._incomplete_runs(iid, start, end)
            missing = sum((b - a) // timedelta(minutes=1) for a, b in done)
            log.info(
                "repair %s %s→%s: %d/%d minutes complete",
                iid,
                iso(start),
                iso(end),
                (end - start) // timedelta(minutes=1) - missing,
                (end - start) // timedelta(minutes=1),
            )
        runs, bars = await self._incomplete_runs(iid, lo, hi)
        reason = NO_TRADE_HISTORY.get(venue) or (
            BYBIT_LIMIT if venue == "bybit" else "REST_REPAIR_INCOMPLETE"
        )
        for start, end in runs:
            minute = start
            while minute < end:
                bar = bars.get(minute)
                if bar is not None and not bar.get("gap_reason"):
                    # An unproven minute always says why (never a null reason).
                    await self._put("mkt.flow_bars_1m", iid + ":" + iso(minute), dict(bar, gap_reason=reason))
                minute += timedelta(minutes=1)
        await self.flush_market()
        return {
            "incomplete_before": before,
            "incomplete_after": sum((b - a) // timedelta(minutes=1) for a, b in runs),
        }

    async def _kraken_predecessor(self, instrument, start):
        """The last Kraken print before start, found by a `since` page that crosses start:
        consecutive per-pair IDs then prove the repaired prefix."""
        adapter = self.adapter(instrument, priority=0)
        for minutes in (2, 10, 60, 240):
            rows, _ = await adapter.trades(start=start - timedelta(minutes=minutes))
            points = [norm.trade(instrument, raw, source="REST") for raw in rows]
            earlier = [p for p in points if parse_time(p["ts"]) < start]
            if earlier and any(parse_time(p["ts"]) >= start for p in points):
                point = max(earlier, key=lambda p: int(p["trade_id"]))
                return {"last_trade_id": point["trade_id"], "last_trade": self.flow.trade_point(point)}
        return {}

    async def _backfill_klines(
        self, instrument, start, end, *, include_reference=True, kind="PRE_EVENT_KLINES"
    ):
        """Actual closed candles, including the close at/before a return anchor."""
        start, end = parse_time(start), parse_time(end)
        first = start.replace(second=0, microsecond=0) - timedelta(minutes=1) if include_reference else start
        last = end.replace(second=0, microsecond=0)
        iid = instrument["instrument_id"]
        expected = max(0, int((last - first).total_seconds() // 60))
        expected_points = {first + timedelta(minutes=n) for n in range(expected)}

        async def observed():
            rows = await self._range(
                "mkt.klines_1m", "open_time", start=first, end=last, filters={"instrument_id": iid}
            )
            return {
                parse_time(row["open_time"])
                for row in rows
                if row.get("complete") is True and parse_time(row["open_time"]) + timedelta(minutes=1) <= end
            }

        points = await observed()
        adapter = self.adapter(instrument, priority=1)
        for offset in range(0, expected, 240):
            lo = first + timedelta(minutes=offset)
            hi = min(lo + timedelta(minutes=240), last)
            if all(lo + timedelta(minutes=n) in points for n in range(int((hi - lo).total_seconds() // 60))):
                continue
            try:
                rows = await adapter.klines(start=lo, end=hi)
                await self._raw(instrument, "backfill_klines", rows)
                for raw in rows:
                    row = norm.kline(instrument, raw)
                    if first <= parse_time(row["open_time"]) < last:
                        await self._store_kline(instrument, row)
            except Exception as exc:  # noqa: BLE001 - actual source failure remains explicit
                await self._gap(instrument, kind, exc, start=lo, end=hi)
        await self.flush_market()
        points = await observed()
        return {
            "instrument_id": iid,
            "start": iso(first),
            "end": iso(last),
            "expected_minutes": expected,
            "complete_minutes": len(points & expected_points),
            "complete": expected > 0 and expected_points <= points,
            "includes_anchor_close": include_reference,
        }

    async def _backfill_benchmark_klines(self, anchor):
        result = []
        for symbol in ("BTC", "ETH"):
            tokens = {token["token_id"] for token in self.tokens.values() if token.get("symbol") == symbol}
            candidates = [
                inst
                for inst in self.instruments.values()
                if inst["token_id"] in tokens
                and inst.get("active", True)
                and norm.venue_key(inst).endswith(".spot")
            ]
            if not candidates:
                continue
            instrument = min(
                candidates, key=lambda inst: (norm.venue_key(inst) != "binance.spot", inst["instrument_id"])
            )
            async with self._context_locks.setdefault(instrument["instrument_id"], asyncio.Lock()):
                result.append(
                    await self._backfill_klines(instrument, parse_time(anchor) - timedelta(days=1), anchor)
                )
        return result

    async def escalate(self, event_id, tokens):
        async with self.repo.lock("market-hot-set"), self.repo.transaction():
            now = iso()
            event = await self.repo.get("evt.events", event_id) or {}
            anchor_record = event_anchor(event) if event else {"type": "KNOWN_TO_US_AT", "ts": now}
            anchor = anchor_record["ts"]
            assessment = (
                await self.repo.get("evt.ems_assessments", event["current_ems_id"])
                if event.get("current_ems_id")
                else None
            )
            if assessment is None:
                assessments = await self.repo.list(
                    "evt.ems_assessments", filters={"event_id": event_id}, limit=10000
                )
                assessment = (
                    max(assessments, key=lambda r: (int(r.get("version", 0)), r.get("cutoff", "")))
                    if assessments
                    else {}
                )
            event_priority = {"LOW": 1, "MATERIAL": 2, "MAJOR": 3, "STRUCTURAL_SHOCK": 4}.get(
                assessment.get("class"), 1
            )
            for value in tokens:
                token_id = value["token_id"] if isinstance(value, dict) else value
                if token_id not in self.tokens:
                    raise ValueError("unknown token " + token_id)
                priority = (
                    int(value.get("priority", event_priority)) if isinstance(value, dict) else event_priority
                )
                key = token_id + ":" + event_id
                existing = await self.repo.get("mkt.hot_tokens", key)
                protected = await self.hot_protected(event_id, token_id)
                await self._put(
                    "mkt.hot_tokens",
                    key,
                    {
                        "token_id": token_id,
                        "event_id": event_id,
                        "priority": priority,
                        "since": existing.get("since", anchor) if existing else anchor,
                        "expires_at": None if protected else iso(parse_time(anchor) + timedelta(hours=72)),
                    },
                )
            hot = await self.repo.list("mkt.hot_tokens", limit=10000)
            group = {}
            for r in hot:
                group.setdefault(r["token_id"], []).append(r)
            cap = int(self.thresholds.get("hot_token_cap", 15))
            ranked = sorted(
                group,
                key=lambda t: (
                    max(x["priority"] for x in group[t]),
                    max(
                        x["since"] for x in group[t] if x["priority"] == max(y["priority"] for y in group[t])
                    ),
                ),
                reverse=True,
            )
            for token_id in ranked[cap:]:
                for r in group[token_id]:
                    await self.repo.delete("mkt.hot_tokens", token_id + ":" + r["event_id"])
            retained = set(ranked[:cap])
            for token_id in group:
                await self._record_capture_tier(
                    token_id, "HOT" if token_id in retained else "BASE", "ESCALATION_CAP"
                )
        if self.settings.replay:
            return
        await self._sync_streams()
        for token_id in retained:
            if token_id in [v.get("token_id") if isinstance(v, dict) else v for v in tokens]:
                await self._schedule_event_backfill(event_id, token_id, anchor)

    async def _capture_window_coverage(self, instrument, anchor):
        end = parse_time(anchor)
        result = {"instrument_id": instrument["instrument_id"], "windows": {}}
        for hours in (1, 4, 24):
            start = end - timedelta(hours=hours)
            lo = start.replace(second=0, microsecond=0)
            if start > lo:
                lo += timedelta(minutes=1)
            hi = end.replace(second=0, microsecond=0)
            expected = max(0, int((hi - lo).total_seconds() // 60))
            metrics = {}
            for name, collection, field in [
                ("flow", "mkt.flow_bars_1m", "minute_start"),
                ("klines", "mkt.klines_1m", "open_time"),
            ]:
                rows = await self._range(
                    collection,
                    field,
                    start=lo,
                    end=hi,
                    filters={"instrument_id": instrument["instrument_id"]},
                )
                complete = sum(1 for r in rows if r.get("complete"))
                metrics[name] = {
                    "expected_minutes": expected,
                    "complete_minutes": complete,
                    "missing_or_incomplete_minutes": max(0, expected - complete),
                    "complete": complete == expected and expected > 0,
                }
            result["windows"][f"W{hours}H"] = dict(
                start=iso(start),
                end=iso(end),
                observed_bucket_start=iso(lo),
                observed_bucket_end=iso(hi),
                timestamp_precision="BUCKET",
                unresolved_boundary_seconds=(lo - start).total_seconds() + (end - hi).total_seconds(),
                **metrics,
            )
        result["complete"] = all(
            w["flow"]["complete"] and w["klines"]["complete"] and w["unresolved_boundary_seconds"] == 0
            for w in result["windows"].values()
        )
        if norm.venue_key(instrument).endswith(".spot"):
            boundary = end - timedelta(days=1)
            bars = await self._range(
                "mkt.flow_bars_1m",
                "minute_start",
                start=boundary.replace(second=0, microsecond=0) - timedelta(minutes=1),
                end=boundary.replace(second=0, microsecond=0) + timedelta(minutes=1),
                filters={"instrument_id": instrument["instrument_id"]},
            )
            points = [
                point
                for bar in bars
                for point in (bar.get("reference_points") or [])
                if point.get("complete") is True and parse_time(point["boundary"]) == boundary
            ]
            reference = (
                max(points, key=lambda point: parse_time(point["last_trade"]["ts"]))["last_trade"]
                if points
                else None
            )
            result["pre24_reference"] = {
                "available": reference is not None,
                "trade_id": reference.get("trade_id") if reference else None,
                "ts": reference.get("ts") if reference else None,
                "ts_prec": reference.get("ts_prec") if reference else None,
                "reason": None if reference else "INCOMPLETE",
            }
            result["complete"] = result["complete"] and reference is not None
        return result

    async def wait_capture(self, event_id, token_id, *, timeout_s=30):
        """Await durable prerequisite or return an explicit bounded incomplete result.

        Capture workers and sockets continue independently; cancellation never cancels
        the prerequisite capture obligation.
        """
        started = time.monotonic()
        latest = None
        while True:
            rows = await self.repo.list(
                "mkt.backfill_runs", filters={"event_id": event_id, "token_id": token_id}, limit=10000
            )
            latest = max(rows, key=lambda row: row["started_at"]) if rows else None
            elapsed = time.monotonic() - started
            terminal = latest and latest["status"] in ("COMPLETE", "PARTIAL", "FAILED")
            if terminal or elapsed >= timeout_s:
                result = {
                    "status": latest["status"] if terminal else "INCOMPLETE",
                    "backfill_id": latest["backfill_id"] if latest else None,
                    "reason": latest.get("reason") if terminal else "BOUNDED_PREREQUISITE_WAIT_EXPIRED",
                    "bounded_wait_s": elapsed,
                    "observed_at": iso(),
                    "incomplete_windows_excluded": True,
                }
                if latest:
                    self._capture_waits[latest["backfill_id"]] = result
                return result
            await asyncio.sleep(min(0.25, max(0, timeout_s - elapsed)))

    async def _schedule_event_backfill(self, event_id, token_id, anchor):
        marker = (token_id, event_id, iso(anchor))
        if marker in self._backfills:
            return
        backfill_id = "BF-" + sha256({"event_id": event_id, "token_id": token_id, "anchor": iso(anchor)})
        old = await self.repo.get("mkt.backfill_runs", backfill_id)
        if old and old.get("status") in ("COMPLETE", "PARTIAL"):
            self._backfills.add(marker)
            return
        row = {
            "backfill_id": backfill_id,
            "event_id": event_id,
            "token_id": token_id,
            "anchor": iso(anchor),
            "started_at": iso(),
            "finished_at": None,
            "status": "PENDING",
            "reason": "PRE_EVENT_CAPTURE_PENDING",
            "result": None,
        }
        event = await self.repo.get("evt.events", event_id)
        selected = event_anchor(event) if event else None
        precision = selected.get("ts_prec") if selected and selected["ts"] == iso(anchor) else None
        row.update(norm.timestamp_fields("anchor", anchor, precision))
        await self._put("mkt.backfill_runs", backfill_id, row)
        self._backfills.add(marker)

        async def capture():
            try:
                row.update(status="RUNNING")
                await self._put("mkt.backfill_runs", backfill_id, row)
                instruments = [i for i in self.instruments.values() if i["token_id"] == token_id]
                for offset in (-86400, -14400, -3600, 900, 3600, 14400, 86400, 259200):
                    cutoff = (
                        parse_time(anchor) if offset < 0 else parse_time(anchor) + timedelta(seconds=offset)
                    )
                    begin = (
                        parse_time(anchor) + timedelta(seconds=offset) if offset < 0 else parse_time(anchor)
                    )
                    self._register_capture_boundaries(
                        {i["instrument_id"] for i in instruments}, cutoff, {"ts": begin}
                    )
                # Benchmark closes first: two cheap kline reads that market context needs,
                # which a long trade enumeration (or a restart during it) must not hold back.
                benchmarks = await self._backfill_benchmark_klines(anchor)

                async def capture_window(inst):
                    async with self._repair_slots:  # shares slots: never starves the reference venue
                        await self.backfill(inst, anchor)

                await asyncio.gather(*(capture_window(inst) for inst in instruments))
                # Whatever REST enumeration left unproven, and the post-anchor windows so far.
                try:
                    await self.repair_windows(
                        token_id,
                        parse_time(anchor) - WINDOW_BEFORE,
                        iso(min(utcnow(), parse_time(anchor) + WINDOW_AFTER)),
                    )
                except Exception as exc:  # noqa: BLE001 - repair leaves explicit gaps; capture still completes
                    log.warning("window repair for %s failed: %s", token_id, type(exc).__name__)
                coverage = await asyncio.gather(
                    *(self._capture_window_coverage(inst, anchor) for inst in instruments)
                )
                ready = bool(coverage) and all(r["complete"] for r in [*coverage, *benchmarks])
                row.update(
                    status="COMPLETE" if ready else "PARTIAL",
                    finished_at=iso(),
                    reason=None if ready else "INCOMPLETE_VENUE_WINDOWS_EXCLUDED",
                    result={"instruments": coverage, "benchmark_klines": benchmarks},
                )
                async with self.repo.transaction():
                    await self._put("mkt.backfill_runs", backfill_id, row)
            except asyncio.CancelledError:
                # Persisted RUNNING obligation is resumed from hot membership after restart.
                raise
            except Exception as exc:  # noqa: BLE001 - durable failed prerequisite must remain inspectable
                row.update(status="FAILED", finished_at=iso(), reason=str(exc))
                await self._put("mkt.backfill_runs", backfill_id, row)

        task = asyncio.create_task(capture())
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def capture_changes(self, since=None, end=None):
        """Durable historical completions and coalesced actual late normalized data."""
        async for row in self.repo.iter_range(
            "mkt.backfill_runs", "finished_at", start=since, end=end, batch_size=1000
        ):
            if row.get("status") not in ("COMPLETE", "PARTIAL", "FAILED") or not row.get("finished_at"):
                continue
            yield {
                **row,
                "change_id": "CAP-"
                + sha256(
                    {
                        "backfill_id": row["backfill_id"],
                        "status": row["status"],
                        "finished_at": row["finished_at"],
                    }
                ),
                "kind": "HISTORICAL_CAPTURE_TERMINAL",
            }
        async for change in self._late_normalized_changes(since, end):
            yield change

    async def _late_normalized_changes(self, since, end):
        fields = ["event_id", "token_id", "anchor", "checkpoint", "revision", "captured_at", "window_end"]
        latest = {}
        async for row in self.repo.iter_metadata("out.reaction_checkpoints", fields, batch_size=1000):
            if not row.get("anchor") or not row.get("window_end") or not row.get("captured_at"):
                continue  # Legacy unknown availability/window metadata is not invented.
            key = (row["event_id"], row["token_id"], row["checkpoint"])
            if key not in latest or int(row.get("revision") or 1) > int(latest[key].get("revision") or 1):
                latest[key] = row
        if not latest:
            return
        targets = {}
        for row in latest.values():
            anchor = row["anchor"]["ts"] if isinstance(row["anchor"], dict) else row["anchor"]
            key = (row["event_id"], row["token_id"], iso(anchor))
            target = targets.setdefault(
                key,
                {
                    "windows": [],
                    "digest": hashlib.sha256(),
                    "count": 0,
                    "highwater": None,
                    "market_from": None,
                },
            )
            target["windows"].append((parse_time(row["window_end"]), parse_time(row["captured_at"])))
        max_cutoff = max(parse_time(row["window_end"]) for row in latest.values())
        first_capture = min(parse_time(row["captured_at"]) for row in latest.values())
        start = max(parse_time(since), first_capture) if since is not None else first_capture
        benchmark_tokens = {
            token_id for token_id, token in self.tokens.items() if token.get("symbol") in ("BTC", "ETH")
        }
        peers = {
            token_id
            for token_id, token in self.tokens.items()
            if str(token.get("sector", "")).upper() == "DEFI"
        }
        families = [
            ("mkt.flow_bars_1m", "minute_start", 60),
            ("mkt.klines_1m", "open_time", 60),
            ("mkt.book_summaries", "exchange_ts", 0),
            ("mkt.oi_obs", "ts", 0),
            ("mkt.funding_obs", "as_of", 0),
            ("mkt.funding_obs", "funding_time", 0),
            ("mkt.liq_obs", "ts", 0),
            ("mkt.tickers_24h", "ts", 0),
            ("mkt.supply_obs", "ts", 0),
        ]
        for collection, semantic_field, offset in families:
            checked = 0
            async for row in self.repo.iter_late_arrivals(
                collection,
                "recorded_at",
                semantic_field,
                since=start,
                end=end or iso(),
                market_end=max_cutoff - timedelta(seconds=offset),
                batch_size=1000,
            ):
                checked += 1
                if checked % 200 == 0:
                    await asyncio.sleep(0)  # after a restart, every repaired row arrives "late"
                stamp = parse_time(row[semantic_field]) + timedelta(seconds=offset)
                arrival = parse_time(row["recorded_at"])
                source_token = row.get("token_id") or self.instruments.get(row.get("instrument_id"), {}).get(
                    "token_id"
                )
                if source_token is None:
                    continue
                signature = sha256({"collection": collection, "data": row})
                for key, target in targets.items():
                    _, token_id, anchor = key
                    comparator = source_token in benchmark_tokens and collection in (
                        "mkt.flow_bars_1m",
                        "mkt.klines_1m",
                    )
                    comparator = comparator or (source_token in peers and collection == "mkt.klines_1m")
                    if source_token != token_id and not comparator:
                        continue
                    if stamp < parse_time(anchor) - timedelta(days=90):
                        continue
                    if not any(
                        stamp <= cutoff and arrival >= captured for cutoff, captured in target["windows"]
                    ):
                        continue
                    target["digest"].update(signature.encode())
                    target["count"] += 1
                    target["highwater"] = (
                        max(target["highwater"], arrival) if target["highwater"] else arrival
                    )
                    # Earliest market time touched: windows ending before it are unchanged.
                    target["market_from"] = (
                        min(target["market_from"], stamp) if target["market_from"] else stamp
                    )
        for (event_id, token_id, anchor), target in targets.items():
            if target["count"]:
                fingerprint = target["digest"].hexdigest()
                yield {
                    "event_id": event_id,
                    "token_id": token_id,
                    "anchor": anchor,
                    "observed_at": iso(target["highwater"]),
                    "kind": "LATE_NORMALIZED_DATA",
                    "status": "AVAILABLE",
                    "change_id": "CAP-LIVE-"
                    + sha256(
                        {
                            "event_id": event_id,
                            "token_id": token_id,
                            "anchor": anchor,
                            "highwater": iso(target["highwater"]),
                            "fingerprint": fingerprint,
                        }
                    ),
                    "source_count": target["count"],
                    "market_from": iso(target["market_from"]),
                    "semantic_fingerprint": fingerprint,
                    "reason": "Actual late normalized data became available after frozen reaction capture",
                }

    async def capture_readiness(self, event_id, token_id, *, anchor=None):
        if anchor is not None:
            # Same deterministic primary key as schedule: exact anchor cannot inherit
            # a completed obligation for a previous version of the event time.
            backfill_id = "BF-" + sha256({"event_id": event_id, "token_id": token_id, "anchor": iso(anchor)})
            row = await self.repo.get("mkt.backfill_runs", backfill_id)
            if (
                row
                and row.get("event_id") == event_id
                and row.get("token_id") == token_id
                and iso(row["anchor"]) == iso(anchor)
            ):
                return row
            return {
                "status": "NOT_REQUESTED",
                "reason": "NO_DURABLE_CAPTURE_REQUEST_FOR_EXACT_ANCHOR",
                "anchor": iso(anchor),
            }
        latest = None
        async for row in self.repo.iter_range(
            "mkt.backfill_runs",
            "started_at",
            filters={"event_id": event_id, "token_id": token_id},
            batch_size=1000,
        ):
            if latest is None or row["started_at"] > latest["started_at"]:
                latest = row
        return latest or {"status": "NOT_REQUESTED", "reason": "NO_DURABLE_CAPTURE_REQUEST"}

    async def hot_protected(self, event_id, token_id, now=None):
        now = parse_time(now) if now is not None else utcnow()
        calls = await self.repo.list(
            "ana.calls", filters={"event_id": event_id, "token_id": token_id, "status": "OPEN"}, limit=1
        )
        if calls:
            return True
        async for condition in self.repo.iter_all(
            "ana.watch_conditions", filters={"event_id": event_id, "token_id": token_id, "status": "ARMED"}
        ):
            if condition.get("expires_at") is None or parse_time(condition["expires_at"]) > now:
                return True
        return False

    async def refresh_hot_expiry(self, now=None):
        """Reconcile persisted protection after restart and call/watch transitions."""
        async with self.repo.lock("market-hot-set"), self.repo.transaction():
            rows = await self.repo.list("mkt.hot_tokens", limit=10000)
            result = []
            for row in rows:
                protected = await self.hot_protected(row["event_id"], row["token_id"], now)
                event = await self.repo.get("evt.events", row["event_id"])
                anchor = event_anchor(event)["ts"] if event else row["since"]
                expiry = None if protected else iso(parse_time(anchor) + timedelta(hours=72))
                if row.get("expires_at") != expiry:
                    row = {**row, "expires_at": expiry}
                    await self._put("mkt.hot_tokens", row["token_id"] + ":" + row["event_id"], row)
                result.append(row)
            return result

    async def demote(self, token_id, event_id=None):
        async with self.repo.lock("market-hot-set"), self.repo.transaction():
            for row in await self.repo.list("mkt.hot_tokens", filters={"token_id": token_id}, limit=10000):
                if event_id is None or row["event_id"] == event_id:
                    await self.repo.delete("mkt.hot_tokens", token_id + ":" + row["event_id"])
            remaining = await self.repo.list("mkt.hot_tokens", filters={"token_id": token_id}, limit=1)
            await self._record_capture_tier(token_id, "HOT" if remaining else "BASE", "DEMOTION")
        await self._sync_streams()

    async def _record_capture_tier(self, token_id, tier, reason):
        from .supply import supply_cadences

        cadences = {
            "oi_funding_s": float(
                self.thresholds.get(
                    "hot_oi_funding_interval_s" if tier == "HOT" else "base_oi_funding_interval_s",
                    60 if tier == "HOT" else 300,
                )
            ),
            "kline_s": float(self.thresholds.get("base_kline_interval_s", 60)),
            "book_s": float(self.thresholds.get("book_interval_s", 10)),
            "supply_by_kind": supply_cadences(tier == "HOT"),
        }
        rows = [
            row
            async for row in self.repo.iter_all(
                "ops.capture_tier_transitions", filters={"token_id": token_id}
            )
        ]
        latest = (
            max(
                rows,
                key=lambda row: (
                    parse_time(row["effective_at"]),
                    row.get("transition_seq", 0),
                    row["transition_id"],
                ),
            )
            if rows
            else None
        )
        if (
            latest
            and latest["tier"] == tier
            and latest.get("capture_cadences") == cadences
            and latest.get("config_version") == self.settings.config_version
        ):
            return
        review_id = new_id("TIER")
        event_ids = sorted(
            row["event_id"]
            for row in await self.repo.list("mkt.hot_tokens", filters={"token_id": token_id}, limit=10000)
        )
        await self._put(
            "ops.capture_tier_transitions",
            review_id,
            {
                "transition_id": review_id,
                "transition_seq": max((row.get("transition_seq", 0) for row in rows), default=0) + 1,
                "event_ids": event_ids,
                "token_id": token_id,
                "tier": tier,
                **collection_clock(iso()).fields("effective_at"),
                "reason": reason,
                "capture_cadences": cadences,
            },
        )

    async def _sync_streams(self):
        if time.monotonic() - self._last_hot_expiry_refresh >= 60:
            await self.refresh_hot_expiry()
            self._last_hot_expiry_refresh = time.monotonic()
        rows = await self.repo.list("mkt.hot_tokens", limit=10000)
        hot = {r["token_id"] for r in rows}
        self._hot_since = {}
        for r in rows:
            if r["token_id"] not in self._hot_since or r["since"] < self._hot_since[r["token_id"]]:
                self._hot_since[r["token_id"]] = r["since"]
        desired = {}
        for iid, inst in self.instruments.items():
            if inst["token_id"] in hot:
                for endpoint in endpoint_keys(inst):
                    desired.setdefault(endpoint, set()).add(iid)
        for endpoint, task in list(self.streams.items()):
            if endpoint not in desired:
                self._stream_groups.pop(endpoint, None)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                self.streams.pop(endpoint, None)
                self._stream_controls.pop(endpoint, None)
        for endpoint, members in desired.items():
            previous = self._stream_groups.get(endpoint, set())
            self._stream_groups[endpoint] = members
            if self._stop is None or self.settings.replay:
                continue
            if endpoint not in self.streams or self.streams[endpoint].done():
                self._stream_controls[endpoint] = asyncio.Queue()
                self.streams[endpoint] = asyncio.create_task(stream_group_loop(self, endpoint))
            elif members != previous:
                self._stream_controls[endpoint].put_nowait(set(members))

    @norm.precise
    async def _independent_book(self, instrument, stream):
        """Second path (03 §5.9): only the listed REST book snapshots (02 §4.1, §4.3)."""
        key = norm.venue_key(instrument)
        s = instrument["venue_symbol"]
        if key == "binance.spot":
            raw = await self._request(
                "GET", "https://api.binance.com/api/v3/depth", params={"symbol": s, "limit": 5}, _priority=2
            )
        elif key == "coinbase.spot":
            raw = await self._request(
                "GET",
                "https://api.exchange.coinbase.com/products/" + s + "/book",
                params={"level": 2},
                _priority=2,
            )
        else:
            return {"second_path_confirmed": False, "second_path_reason": "NO_LISTED_SECOND_PATH"}
        bid, ask = raw["bids"][0][0], raw["asks"][0][0]
        ts = iso()  # neither listed snapshot carries a clock
        await self._raw(instrument, "independent_rest_book", raw)
        bid, ask = norm.decimal(bid), norm.decimal(ask)
        if bid <= 0 or ask <= bid:
            raise norm.SemanticError("independent REST book is empty or crossed")
        mid = (bid + ask) / 2
        spread = (ask - bid) / mid * 10000
        divergence = abs(mid / norm.decimal(stream["mid"]) - 1) * 10000
        tick = instrument.get("tick_size")
        tick_bps = norm.decimal(tick) / mid * 10000 if tick else Decimal(0)
        tolerance = max(spread, norm.decimal(stream["spread_bps"]), 2 * tick_bps)
        return jsonable(
            {
                "second_path_confirmed": divergence <= tolerance,
                "independent_rest": {
                    "best_bid": bid,
                    "best_ask": ask,
                    "mid": mid,
                    "spread_bps": spread,
                    "ts": iso(ts),
                    "ts_prec": "COLLECTION_ONLY",
                    "received_ts": iso(),
                    "received_ts_prec": "COLLECTION_ONLY",
                    "source": "PUBLIC_REST",
                    "price_divergence_bps": divergence,
                    "tolerance_bps": tolerance,
                    "tolerance_method": "MAX_OBSERVED_SPREAD_OR_TWO_TICKS",
                },
                "second_path_reason": None if divergence <= tolerance else "REST_STREAM_DISAGREEMENT",
            }
        )

    def _instrument_semantics(self, instruments):
        """The versioned field registry is authoritative, without runtime promotion."""
        return [
            dict(
                {
                    key: value
                    for key, value in definition.items()
                    if key
                    not in {
                        "confidence",
                        "semantic_confidence",
                        "verification_status",
                        "verified_at",
                        "verified_by",
                        "source_basis_confidence",
                    }
                },
                instrument_id=instrument["instrument_id"],
                scope="PER_INSTRUMENT",
            )
            for instrument in instruments
            for definition in self.semantics
            if definition["metric_key"].startswith(norm.venue_key(instrument) + ".")
        ]

    def _oi_key(self, instrument, raw):
        key = norm.venue_key(instrument)
        return (
            key + (".openInterestHist" if "sumOpenInterest" in raw else ".openInterest")
            if key == "binance.usdm"
            else key + (".ctx" if key.startswith("hyperliquid") else ".open-interest")
        )

    async def _range(self, collection, field, *, start=None, end=None, filters=None):
        return [
            row
            async for row in self.repo.iter_range(
                collection, field, start=start, end=end, filters=filters, batch_size=100000
            )
        ]

    async def _capture_metadata(self, token_id, cutoff):
        from .supply import supply_cadences

        end = parse_time(cutoff)
        history = [
            row
            async for row in self.repo.iter_all(
                "ops.capture_tier_transitions", filters={"token_id": token_id}
            )
        ]
        prior = [row for row in history if parse_time(row["effective_at"]) <= end]
        transition = (
            max(
                prior,
                key=lambda row: (
                    parse_time(row["effective_at"]),
                    row.get("transition_seq", 0),
                    row["transition_id"],
                ),
            )
            if prior
            else None
        )
        tier = transition["tier"] if transition else "UNKNOWN"
        is_hot = tier == "HOT"
        base = float(self.thresholds.get("base_kline_interval_s", 60))
        oi = float(
            self.thresholds.get(
                "hot_oi_funding_interval_s" if is_hot else "base_oi_funding_interval_s", 60 if is_hot else 300
            )
        )
        profile = (transition or {}).get("capture_cadences", {})
        base = profile.get("kline_s", base)
        oi = profile.get("oi_funding_s") if transition else None
        supply = profile.get(
            "supply_by_kind",
            {
                **supply_cadences(False),
                "EXCHANGE_INVENTORY": None,
                "WALLET_FLOWS": None,
            },
        )
        watermarks = {}
        coverage_start = {}
        states = {}
        push = {}
        end = parse_time(cutoff)
        for iid, instrument in self.instruments.items():
            if instrument["token_id"] != token_id or norm.venue_key(instrument).endswith(".spot"):
                continue
            key = norm.venue_key(instrument)
            if key == "hyperliquid.perp":
                states[iid] = "NOT_SUPPORTED"
                continue
            states[iid] = "CONNECTED" if iid in self._connected_since else "DISCONNECTED"
            observed = self._stream_watermarks.get(iid)
            since = self._connected_since.get(iid)
            if (
                observed
                and states[iid] == "CONNECTED"
                and since <= end
                and parse_time(observed["ts"]) >= since
            ):
                watermarks[iid] = iso(min(parse_time(observed["ts"]), end))
                coverage_start[iid] = iso(since)
            push[iid] = 1 if key == "binance.usdm" else 0.5 if key == "bybit.linear" else None
        return {
            "tier": tier,
            "tier_basis": "DURABLE_CAPTURE_TIER_TRANSITIONS"
            if transition
            else "CAPTURE_TIER_HISTORY_UNAVAILABLE",
            "tier_effective_at": transition["effective_at"] if transition else None,
            "hot_events": (transition or {}).get("event_ids", []),
            "collections": {
                "flow_bars": {"cadence_s": 60, "basis": "ONE_MINUTE_CLOSED_FLOW_BAR"},
                "klines": {
                    "cadence_s": base,
                    # End of the last successful poll at or before the cutoff, per instrument.
                    "observed_through": {
                        iid: max(at for at in polls if parse_time(at) <= end)
                        for iid, polls in self._klines_polled.items()
                        if self.instruments.get(iid, {}).get("token_id") == token_id
                        and any(parse_time(at) <= end for at in polls)
                    },
                    "watermark_basis": "LAST_SUCCESSFUL_POLL_BOUNDED_BY_CUTOFF",
                },
                "books": {
                    "cadence_s": profile.get("book_s", float(self.thresholds.get("book_interval_s", 10)))
                },
                "oi": {
                    "cadence_s": oi if tier != "UNKNOWN" else None,
                    "reason": "CAPTURE_TIER_HISTORY_UNAVAILABLE" if tier == "UNKNOWN" else None,
                    # Series a source publishes only in 5-min buckets cannot be faster than
                    # the bucket (03 §5.4: Bybit singleOpenInterest, never openInterest).
                    "cadence_s_by_metric": {
                        metric: max(oi, SOURCE_BUCKET_S) if oi is not None and tier != "UNKNOWN" else None
                        for metric in SOURCE_BUCKETED_OI
                    },
                    "cadence_basis_by_metric": dict.fromkeys(SOURCE_BUCKETED_OI, "SOURCE_BUCKET_300S"),
                },
                "funding": {
                    "cadence_s": oi if tier != "UNKNOWN" else None,
                    "reason": "CAPTURE_TIER_HISTORY_UNAVAILABLE" if tier == "UNKNOWN" else None,
                },
                "tickers": {"cadence_s": base},
                "supply": {"by_kind": supply},
                "liquidations": {
                    "cadence_s": STREAM_PING_INTERVAL_S,
                    "basis": "CONFIGURED_SHARED_SOCKET_HEALTH_INTERVAL",
                    "feed_push_cadence_s_by_instrument": push,
                    "observed_through": watermarks,
                    "coverage_start": coverage_start,
                    "coverage_status_by_instrument": states,
                    "watermark_basis": "OBSERVED_SHARED_PUBLIC_SOCKET_EXCHANGE_TIME_BOUNDED_BY_CUTOFF",
                },
            },
        }

    async def _condition_history(self, collection, field, instrument_id, cutoff, assembled_at):
        """Reuse immutable history, incorporating newly eligible and late corrected points."""
        cache = self.__dict__.setdefault("_condition_histories", {})
        locks = self.__dict__.setdefault("_condition_history_locks", {})
        key = (collection, field, instrument_id)
        async with locks.setdefault(key, asyncio.Lock()):
            previous = cache.get(key)
            start = cutoff - timedelta(days=90)
            filters = {"instrument_id": instrument_id}
            reload = previous is None or cutoff < previous["cutoff"] or previous.get("unknown_availability")
            if reload:
                rows = await self._range(
                    collection, field, start=start, end=cutoff + timedelta(microseconds=1), filters=filters
                )
                points = {}
            else:
                points = dict(previous["points"])
                rows = (
                    await self._range(
                        collection,
                        field,
                        start=previous["cutoff"] + timedelta(microseconds=1),
                        end=cutoff + timedelta(microseconds=1),
                        filters=filters,
                    )
                    if cutoff > previous["cutoff"]
                    else []
                )
                rows.extend(
                    [
                        r
                        async for r in self.repo.iter_late_arrivals(
                            collection,
                            "recorded_at",
                            field,
                            since=previous["assembled_at"],
                            end=assembled_at,
                            market_end=cutoff,
                            filters=filters,
                        )
                    ]
                )
            changes = []
            for row in rows:
                stamp = row.get(field)
                if stamp is None:
                    continue
                availability = row.get("recorded_at") or row.get("received_ts")
                if availability and parse_time(availability) > assembled_at:
                    continue
                identity = (row.get("instrument_id"), row.get("metric_key"), row.get("kind"), stamp)
                fingerprint = sha256(self._semantic_content(row))
                if identity not in points or sha256(self._semantic_content(points[identity])) != fingerprint:
                    changes.append((identity, fingerprint))
                points[identity] = row
            removed = [
                identity for identity, row in points.items() if not start <= parse_time(row[field]) <= cutoff
            ]
            for identity in removed:
                points.pop(identity)
            initial = reload
            cache[key] = {
                "points": points,
                "cutoff": cutoff,
                "assembled_at": assembled_at,
                "unknown_availability": any(row.get("recorded_at") is None for row in points.values()),
                "fingerprint": sha256([None if initial else previous["fingerprint"], changes, removed])
                if initial or changes or removed
                else previous["fingerprint"],
            }
            while len(cache) > 128:
                oldest = next(iter(cache))
                if oldest == key:
                    break
                cache.pop(oldest)
                locks.pop(oldest, None)
            return list(points.values())

    async def _condition_book_baseline(self, instrument_id, origin, assembled_at):
        end = origin - timedelta(microseconds=1)
        start = origin - timedelta(hours=1)
        head = await self.repo.market_dependency_head(
            "mkt.book_summaries",
            "exchange_ts",
            end,
            start=start,
            instrument_ids=[instrument_id],
            assembled_at=assembled_at,
        )
        cache = self.__dict__.setdefault("_condition_book_baselines", {})
        key = (instrument_id, iso(origin))
        cached = cache.get(key)
        if cached is None or head is None or cached["head"] != head:
            from tank.market_schema import MARKET_COLUMNS

            fields = [f for f in MARKET_COLUMNS["mkt.book_summaries"] if f not in ("bids", "asks")]
            rows = [
                r
                async for r in self.repo.iter_market_projection(
                    "mkt.book_summaries",
                    fields,
                    "exchange_ts",
                    start=start,
                    end=origin,
                    instrument_ids=[instrument_id],
                    assembled_at=assembled_at,
                )
            ]
            cache[key] = {"head": head, "rows": rows}
            while len(cache) > 128:
                cache.pop(next(iter(cache)))
        return cache[key]["rows"]

    async def _condition_kline_series(
        self,
        instrument,
        start,
        cutoff,
        assembled_at,
        *,
        kind,
        coverage_count,
        fixed_window=False,
        captured_rows=None,
        rolling=None,
    ):
        """Compact exact typed projections; histories never retain wide kline records.

        `rolling` names a cursor that later calls with a later cutoff extend instead of
        reading the whole history again (live snapshots).
        """
        from array import array
        from bisect import bisect_left
        from datetime import UTC, datetime
        from math import isfinite

        iid = instrument["instrument_id"]
        upper_open = cutoff - timedelta(minutes=1)
        fields = [
            "open_time",
            "open_time_prec",
            "recorded_at",
            "complete",
            "interval_s",
            "c" if kind == "price" else "volume_native",
        ]
        heads = (
            []
            if captured_rows is not None
            else [
                await self.repo.market_dependency_head(
                    "mkt.klines_1m",
                    "open_time",
                    upper_open,
                    start=start,
                    instrument_ids=[iid],
                    assembled_at=assembled_at,
                    order=order,
                )
                for order in ("availability", "semantic_latest", "semantic_earliest")
            ]
        )
        cache = self.__dict__.setdefault("_condition_series", {})
        locks = self.__dict__.setdefault("_condition_series_locks", {})
        # Concurrent historical checkpoints must not evict each other's rolling
        # cursor and repeatedly reload the same history. Live conditions retain
        # their incremental cursor; fixed snapshots have their own bounded keys.
        key = (iid, kind, rolling) if rolling else (iid, kind, start, cutoff) if fixed_window else (iid, kind)
        # A shared cursor holds SERIES_MARGIN of history before the window it was read
        # for, and keeps every bar it has read up to its latest cutoff: a reaction
        # checkpoint a few minutes behind the live snapshots is served from the same
        # cursor instead of rebuilding 90 days of minutes (r113 E2E #5).
        margin = SERIES_MARGIN if rolling or not fixed_window else timedelta(0)
        async with locks.setdefault(key, asyncio.Lock()):
            previous = cache.get(key)
            reset = (
                previous is None
                or start < previous["start"]
                or previous["unknown_availability"]
                or not any(heads)
                or assembled_at is not None
                and previous["assembled_at"] is not None
                and parse_time(assembled_at) < parse_time(previous["assembled_at"])
            )
            if not reset and (heads, start, cutoff) == previous["served"]["request"]:
                return dict(previous["series"], coverage_count=coverage_count)
            state_start = start - margin if reset else previous["start"]
            state_upper = upper_open if reset else max(upper_open, previous["upper_open"])
            times = array("q") if reset else previous["opens"][:]
            values = (array("d") if kind == "price" else []) if reset else previous["values"][:]
            complete = bytearray() if reset else previous["complete"][:]
            precise = bytearray() if reset else previous["precise"][:]
            epoch = datetime(1970, 1, 1, tzinfo=UTC)

            def microseconds(value):
                delta = parse_time(value) - epoch
                return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds

            unknown_availability = False
            changed = []

            def apply(row):
                nonlocal unknown_availability
                unknown_availability |= row.get("recorded_at") is None
                opened = parse_time(row["open_time"])
                if not state_start <= opened <= state_upper or row.get("interval_s") != 60:
                    return
                value = row.get("c" if kind == "price" else "volume_native")
                if kind == "price":
                    value = float(value) if value is not None else float("nan")
                    valid = row.get("complete") is True and isfinite(value) and value > 0
                else:
                    value = str(value) if value is not None else None
                    valid = row.get("complete") is True and value is not None and norm.decimal(value) >= 0
                stamp = microseconds(opened)
                index = bisect_left(times, stamp)
                known = row.get("open_time_prec") == "BUCKET(60s)"
                if index < len(times) and times[index] == stamp:
                    same_value = (
                        values[index] == value
                        or kind == "price"
                        and not isfinite(values[index])
                        and not isfinite(value)
                    )
                    if same_value and complete[index] == valid and precise[index] == known:
                        return
                    changed.append(
                        (stamp, value if kind == "volume" or isfinite(value) else None, valid, known)
                    )
                    values[index], complete[index], precise[index] = value, valid, known
                else:
                    changed.append(
                        (stamp, value if kind == "volume" or isfinite(value) else None, valid, known)
                    )
                    times.insert(index, stamp)
                    values.insert(index, value)
                    complete.insert(index, valid)
                    precise.insert(index, known)

            if captured_rows is not None:
                availability = parse_time(assembled_at)
                for row in captured_rows:
                    if row.get("recorded_at") is None or parse_time(row["recorded_at"]) <= availability:
                        apply(row)
            elif reset:
                # Decoded pages are applied in a worker thread: the loop only awaits SQL.
                page = []
                async for row in self.repo.iter_market_projection(
                    "mkt.klines_1m",
                    fields,
                    "open_time",
                    start=state_start,
                    end=upper_open + timedelta(microseconds=1),
                    instrument_ids=[iid],
                    assembled_at=assembled_at,
                    # One page: every keyset page re-sorts the rest of the range.
                    batch_size=200000,
                ):
                    page.append(row)
                    if len(page) == SERIES_APPLY_ROWS:
                        rows, page = page, []
                        await asyncio.to_thread(lambda rows=rows: [apply(row) for row in rows])
                await asyncio.to_thread(lambda: [apply(row) for row in page])
            else:
                if upper_open > previous["upper_open"]:
                    rows = [
                        row
                        async for row in self.repo.iter_market_projection(
                            "mkt.klines_1m",
                            fields,
                            "open_time",
                            start=previous["upper_open"] + timedelta(microseconds=1),
                            end=upper_open + timedelta(microseconds=1),
                            instrument_ids=[iid],
                            assembled_at=assembled_at,
                            batch_size=200000,
                        )
                    ]
                    await asyncio.to_thread(lambda: [apply(row) for row in rows])
                async for row in self.repo.iter_late_arrivals(
                    "mkt.klines_1m",
                    "recorded_at",
                    "open_time",
                    since=previous["assembled_at"],
                    end=assembled_at,
                    market_end=state_upper,
                    filters={"instrument_id": iid},
                ):
                    apply(row)
            if margin and start - margin > state_start + SERIES_TRIM:
                # The window moved a day past the cursor: drop what no later window reads.
                drop = bisect_left(times, microseconds(start - margin))
                state_start = start - margin
                times, values = times[drop:], values[drop:]
                complete, precise = complete[drop:], precise[drop:]

            def state_digest():
                if reset:
                    # The whole content of a freshly read cursor, hashed without
                    # per-point JSON (canonical JSON of 90 days of minutes took
                    # ~4% of the loop on r113).
                    digest = hashlib.sha256(repr((kind, iid, state_start, state_upper)).encode())
                    digest.update(times.tobytes())
                    digest.update(values.tobytes() if kind == "price" else json.dumps(values).encode())
                    digest.update(bytes(complete))
                    digest.update(bytes(precise))
                    return digest.hexdigest()
                if changed:
                    return sha256([previous["fingerprint"], changed])
                return previous["fingerprint"]

            state_fingerprint = await asyncio.to_thread(state_digest)
            served = None if reset else previous["served"]

            def window():
                """The served window [start, upper_open] of the cursor. It extends the last
                served one when it has the same start, ends no earlier and nothing that
                one had served changed since."""
                low = bisect_left(times, microseconds(start))
                high = bisect_left(times, microseconds(upper_open) + 1)
                opens, points = times[low:high], values[low:high]
                valid, known = complete[low:high], precise[low:high]
                append_only, changed_from = False, None
                if served:
                    served_low, served_high = (
                        microseconds(served["start"]),
                        microseconds(served["upper_open"]),
                    )
                    after = bisect_left(opens, served_high + 1)
                    append_only = (
                        served["start"] == start
                        and upper_open >= served["upper_open"]
                        and after < len(opens)
                        and not any(served_low <= point[0] <= served_high for point in changed)
                    )
                    changed_from = opens[after] if append_only else None
                if not append_only:
                    changed_from = min(
                        (point[0] for point in changed if opens and point[0] >= opens[0]), default=None
                    )
                if kind == "price":
                    output_times = array("q", (stamp + 60000000 for i, stamp in enumerate(opens) if valid[i]))
                    output_values = array("d", (value for i, value in enumerate(points) if valid[i]))
                    valid = bytearray([1]) * len(output_times)
                else:
                    output_times, output_values = opens, points
                exact = bool(known) and all(known)
                return output_times, output_values, valid, exact, append_only, changed_from

            (
                output_times,
                output_values,
                output_complete,
                exact,
                append_only,
                changed_from,
            ) = await asyncio.to_thread(window)
            fingerprint = sha256([state_fingerprint, iso(start), iso(upper_open)])
            series = {
                "times_us": output_times,
                "values": output_values,
                "complete": output_complete,
                "instrument_id": iid,
                "token_id": instrument["token_id"],
                "venue": instrument["venue"],
                "market_type": ("SPOT" if instrument["market_type"] == "SPOT" else "PERP")
                if kind == "volume"
                else instrument["market_type"],
                "source_market_type": instrument["market_type"],
                "interval_s": 60,
                "time_basis": "CLOSE_TIME" if kind == "price" else "OPEN_TIME",
                "precision": "BUCKET(60s)" if exact else None,
                "precision_reason": None if exact else "INCOMPLETE",
                "coverage_count": coverage_count,
                "dependency_fingerprint": fingerprint,
                "previous_dependency_fingerprint": served["fingerprint"] if served else None,
                "append_only": append_only,
                "changed_from_us": changed_from,
            }
            state = {
                "start": state_start,
                "upper_open": state_upper,
                "assembled_at": assembled_at,
                "opens": times,
                "values": values,
                "complete": complete,
                "precise": precise,
                "fingerprint": state_fingerprint,
                "served": {
                    "request": (heads, start, cutoff),
                    "start": start,
                    "upper_open": upper_open,
                    "fingerprint": fingerprint,
                },
                "series": series,
                "unknown_availability": unknown_availability,
            }
            if captured_rows is None:
                cache[key] = state
                while len(cache) > 256:
                    cache.pop(next(iter(cache)))
            return series

    async def _condition_heads(self, collection, field, start, cutoff, assembled_at, filters):
        """The three dependency heads of a window in one round trip. A head counts only rows
        recorded by its assembly clock, so for a later clock of the same window it is reused
        while nothing was written to its scope since (one condition evaluation asks for the
        same heads once per condition; prod r106 ran ~60-80 sequential heads per BTC pass)."""
        orders = ("availability", "semantic_latest", "semantic_earliest")
        key = (collection, field, tuple(sorted(filters.items())), iso(start), iso(cutoff))
        scope = next(
            ((name, filters[name]) for name in ("instrument_id", "token_id") if name in filters), (None, None)
        )
        written = getattr(self.repo, "_market_write_clock", {}).get((collection, *scope))
        at = parse_time(assembled_at) if assembled_at is not None else None
        memo = self._condition_heads_memo
        hit = memo.get(key)
        if (
            hit is not None
            and at is not None
            and hit["at"] <= at
            and (written is None or written <= hit["at"])
            and time.monotonic() - hit["computed"] < HEAD_REUSE_S
        ):
            return hit["heads"]
        found = await market_dependency_head(
            self.repo,
            collection,
            field,
            cutoff,
            start=start,
            filters=filters,
            assembled_at=assembled_at,
            orders=orders,
        )
        heads = [found[o] for o in orders]
        if at is not None:
            memo.pop(key, None)
            memo[key] = {"at": at, "heads": heads, "computed": time.monotonic()}
            if len(memo) > 4096:
                memo.pop(next(iter(memo)))
        return heads

    async def _condition_projected_history(
        self, collection, field, start, cutoff, assembled_at, *, fields, filters
    ):
        """Cache selected typed fields and apply only newly eligible/changed source points."""
        from tank.market_schema import MARKET_ALIASES

        fields = tuple(dict.fromkeys([*fields, field, "recorded_at"]))
        key = (collection, field, tuple(sorted(filters.items())), fields)
        cache = self.__dict__.setdefault("_condition_projections", {})
        heads = await self._condition_heads(collection, field, start, cutoff, assembled_at, filters)
        locks = self.__dict__.setdefault("_condition_projection_locks", {})
        async with locks.setdefault(key, asyncio.Lock()):
            previous = cache.get(key)
            reset = (
                previous is None
                or start < previous["start"]
                or cutoff < previous["cutoff"]
                or previous.get("unknown_availability")
            )
            reset = reset or not any(heads)
            if not reset and heads == previous["heads"]:
                return previous["rows"], previous["fingerprint"]
            points = {} if reset else dict(previous["points"])

            def apply(row):
                stamp = row.get(field)
                if stamp is None or not start <= parse_time(stamp) <= cutoff:
                    return
                identity = (
                    row.get("instrument_id"),
                    row.get("token_id"),
                    row.get("kind"),
                    row.get("metric_key"),
                    row.get("chain"),
                    stamp,
                    row.get("id")
                    if row.get("id") is not None
                    else (row.get("side"), row.get("qty_native"), row.get("price"))
                    if collection == "mkt.liq_obs"
                    else None,
                )
                projected = {f: row.get(f) for f in fields}
                projected.update(
                    {
                        alias: projected[target]
                        for alias, target in MARKET_ALIASES.get(collection, {}).items()
                        if target in projected
                    }
                )
                points[identity] = projected

            if reset:
                async for row in self.repo.iter_market_projection(
                    collection,
                    fields,
                    field,
                    start=start,
                    end=cutoff + timedelta(microseconds=1),
                    filters=filters,
                    assembled_at=assembled_at,
                    batch_size=10000,
                ):
                    apply(row)
            else:
                if cutoff > previous["cutoff"]:
                    async for row in self.repo.iter_market_projection(
                        collection,
                        fields,
                        field,
                        start=previous["cutoff"] + timedelta(microseconds=1),
                        end=cutoff + timedelta(microseconds=1),
                        filters=filters,
                        assembled_at=assembled_at,
                        batch_size=10000,
                    ):
                        apply(row)
                async for row in self.repo.iter_late_arrivals(
                    collection,
                    "recorded_at",
                    field,
                    since=previous["assembled_at"],
                    end=assembled_at,
                    market_end=cutoff,
                    filters=filters,
                ):
                    apply(row)
            points = {k: r for k, r in points.items() if start <= parse_time(r[field]) <= cutoff}
            rows = sorted(points.values(), key=lambda row: row[field])
            unknown = any(row.get("recorded_at") is None for row in rows)
            fingerprint = sha256([heads, rows if unknown else None])
            cache[key] = {
                "points": points,
                "rows": rows,
                "heads": heads,
                "start": start,
                "cutoff": cutoff,
                "assembled_at": assembled_at,
                "unknown_availability": unknown,
                "fingerprint": fingerprint,
            }
            while len(cache) > 256:
                cache.pop(next(iter(cache)))
            return rows, fingerprint

    async def _condition_composite_inputs(self, result, *, full_state):
        """Exact compact source graph for transient catalog conditions; never an MTA."""
        from array import array
        from bisect import bisect_right
        from datetime import UTC, datetime

        from tank.analysis.truth import condition_primary_bounds, condition_reference_close
        from tank.market_schema import MARKET_COLUMNS

        token_id = result["token_id"]
        end, assembled = parse_time(result["cutoff"]), result["assembled_at"]
        start, _ = condition_primary_bounds(result, end)
        anchor = parse_time(result["anchor"]["ts"]) if result.get("anchor") else start
        history_start = min(start, anchor, end - timedelta(days=1)) - timedelta(days=90)
        target = [i for i in self.instruments.values() if i["token_id"] == token_id]
        spot = [i for i in target if i["market_type"] == "SPOT"]
        perp = [i for i in target if i["market_type"] != "SPOT"]
        result["tokens"] = list(self.tokens.values())
        result["sector_baskets"] = deepcopy(self.sector_baskets)
        result["capture_metadata"] = await self._capture_metadata(token_id, end)
        result["flow_bars"], result["flow_segments"], result["reference_points"] = [], [], []
        heads = {}
        flow_heads = []
        for instrument in target if full_state else spot:
            iid = instrument["instrument_id"]
            bars, head = await self._condition_projected_history(
                "mkt.flow_bars_1m",
                "minute_start",
                end - timedelta(hours=48),
                end,
                assembled,
                fields=list(MARKET_COLUMNS["mkt.flow_bars_1m"]),
                filters={"instrument_id": iid},
            )
            if anchor < end - timedelta(hours=48):
                bars = list(bars)
                bars.extend(
                    await self._range(
                        "mkt.flow_bars_1m",
                        "minute_start",
                        start=anchor.replace(second=0, microsecond=0),
                        end=anchor.replace(second=0, microsecond=0) + timedelta(minutes=1),
                        filters={"instrument_id": iid},
                    )
                )
            flow_heads.append(head)
            for bar in bars:
                result["flow_segments"].extend(
                    dict(s, instrument_id=iid)
                    for s in (bar.get("boundary_segments") or [])
                    if parse_time(s["end"]) <= end
                )
                result["reference_points"].extend(
                    dict(p, instrument_id=iid)
                    for p in (bar.get("reference_points") or [])
                    if parse_time(p["boundary"]) <= end
                )
                if parse_time(bar["minute_start"]) + timedelta(minutes=1) <= end:
                    result["flow_bars"].append(bar)
        result["tickers"] = await self.repo.latest_market_records(
            "mkt.tickers_24h", cutoff=end, assembled_at=assembled, filters={"token_id": token_id}
        )
        # Only the recent target OHLC/volume fields are materialized. Historical
        # statistical prices and native quantities are compact typed vectors.
        result["klines"] = []
        recent_fields = [
            "instrument_id",
            "token_id",
            "venue",
            "market_type",
            "open_time",
            "open_time_prec",
            "open_time_prec_reason",
            "interval_s",
            "complete",
            "o",
            "h",
            "l",
            "c",
            "volume_native",
            "quote_volume",
            "quote_volume_usd",
            "quote_volume_usd_reason",
            "received_ts",
            "received_ts_prec",
            "recorded_at",
        ]
        for instrument in target if full_state else spot:
            rows, _ = await self._condition_projected_history(
                "mkt.klines_1m",
                "open_time",
                end - timedelta(hours=48),
                end - timedelta(minutes=1),
                assembled,
                fields=recent_fields,
                filters={"instrument_id": instrument["instrument_id"]},
            )
            result["klines"].extend(
                dict(
                    row,
                    venue=row.get("venue") or instrument["venue"],
                    market_type=row.get("market_type") or instrument["market_type"],
                )
                for row in rows
            )
        reference = condition_reference_close(result, end).get("ref_venue")
        all_spot = [i for i in self.instruments.values() if i["market_type"] == "SPOT"]
        counts = await self.repo.market_series_counts(
            [i["instrument_id"] for i in all_spot], end - timedelta(days=90), end, assembled
        )
        result["series_coverage_counts"] = counts
        chosen = {}
        for instrument in all_spot:
            symbol = self.tokens[instrument["token_id"]].get("symbol")
            current = chosen.get(symbol)
            if current is None or counts[instrument["instrument_id"]] > counts[current["instrument_id"]]:
                chosen[symbol] = instrument
        selected = []
        if reference:
            selected.append(self.instruments[reference])
        selected.extend(i for i in chosen.values() if i["token_id"] != token_id)
        result["stat_price_series"] = {}
        epoch = datetime(1970, 1, 1, tzinfo=UTC)

        def micros(value):
            delta = value - epoch
            return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds

        boundaries = [end - timedelta(seconds=n) for n in (0, 900, 3600, 14400, 86400)] + [start, anchor]
        price_heads = []
        preanchor_heads = []
        for instrument in selected:
            iid = instrument["instrument_id"]
            symbol = self.tokens[instrument["token_id"]].get("symbol")
            dense = iid == reference or symbol == "BTC"
            series_start = (
                history_start
                if dense or symbol == "ETH"
                else min(anchor, start, end - timedelta(hours=25)) - timedelta(minutes=1)
            )
            series = await self._condition_kline_series(
                instrument, series_start, end, assembled, kind="price", coverage_count=counts[iid]
            )
            if not dense:
                wanted = set()
                if symbol == "ETH":
                    wanted.update(
                        index for index, stamp in enumerate(series["times_us"]) if stamp % 3600000000 == 0
                    )
                for boundary in boundaries:
                    index = bisect_right(series["times_us"], micros(boundary)) - 1
                    if index >= 0:
                        wanted.add(index)
                order = sorted(wanted)
                series = dict(
                    series,
                    times_us=array("q", (series["times_us"][j] for j in order)),
                    values=array("d", (series["values"][j] for j in order)),
                )
            result["stat_price_series"][iid] = series
            price_heads.append(series["dependency_fingerprint"])
            if dense or symbol == "ETH":
                preanchor_heads.extend(
                    [
                        await self.repo.market_dependency_head(
                            "mkt.klines_1m",
                            "open_time",
                            anchor - timedelta(minutes=1, microseconds=1),
                            start=history_start,
                            instrument_ids=[iid],
                            assembled_at=assembled,
                            order=order,
                        )
                        for order in ("availability", "semantic_latest", "semantic_earliest")
                    ]
                )
        result["relative_history_dependency_fingerprint"] = sha256(
            [
                preanchor_heads,
                reference,
                [i["instrument_id"] for i in selected],
                self.settings.config_version,
                self.settings.code_version,
            ]
        )
        # Runtime confidence still needs actual target registry identities;
        # comparator identities are added only for statistical source lookup.
        result["instruments"] = target + [i for i in selected if i not in target]
        heads["relative"] = sha256(
            [
                price_heads,
                flow_heads,
                reference,
                result["reference_points"],
                counts,
                self.tokens,
                self.sector_baskets,
            ]
        )
        heads["spot"] = sha256([flow_heads, result["tickers"], target])
        if full_state:
            result["stat_volume_series"] = {}
            volume_heads = []
            for instrument in target:
                iid = instrument["instrument_id"]
                series = await self._condition_kline_series(
                    instrument,
                    history_start,
                    end,
                    assembled,
                    kind="volume",
                    coverage_count=counts.get(iid, 0),
                )
                if series["times_us"]:
                    result["stat_volume_series"][iid] = series
                volume_heads.append(series["dependency_fingerprint"])
            result["oi"], result["funding"], result["books"], result["liquidations"] = [], [], [], []
            derivative_heads, book_heads = [], []
            oi_heads, funding_heads = [], []
            for instrument in perp:
                iid = instrument["instrument_id"]
                for name, field, filters in (
                    ("oi", "ts", {"instrument_id": iid}),
                    ("funding", "funding_time", {"instrument_id": iid, "kind": "SETTLED"}),
                ):
                    collection = COLLECTIONS[name][0]
                    rows, head = await self._condition_projected_history(
                        collection,
                        field,
                        history_start,
                        end,
                        assembled,
                        fields=list(MARKET_COLUMNS[collection]),
                        filters=filters,
                    )
                    result[name].extend(rows)
                    derivative_heads.append(head)
                    (oi_heads if name == "oi" else funding_heads).append(head)
                result["funding"].extend(
                    await self.repo.latest_market_records(
                        "mkt.funding_obs",
                        group_by=("instrument_id", "metric_key", "kind"),
                        time_field="as_of",
                        filters={"instrument_id": iid},
                        cutoff=end,
                        assembled_at=assembled,
                    )
                )
                rows, head = await self._condition_projected_history(
                    "mkt.liq_obs",
                    "ts",
                    end - timedelta(hours=48),
                    end,
                    assembled,
                    fields=list(MARKET_COLUMNS["mkt.liq_obs"]),
                    filters={"instrument_id": iid},
                )
                result["liquidations"].extend(rows)
                derivative_heads.append(head)
                result["liquidations"].extend(
                    await self.repo.latest_market_records(
                        "mkt.liq_obs",
                        group_by=("instrument_id", "metric_key"),
                        time_field="ts",
                        filters={"instrument_id": iid},
                        start=end - timedelta(days=90),
                        cutoff=end,
                        assembled_at=assembled,
                    )
                )
            book_fields = [f for f in MARKET_COLUMNS["mkt.book_summaries"] if f not in ("bids", "asks")]
            for instrument in target:
                iid = instrument["instrument_id"]
                rows, head = await self._condition_projected_history(
                    "mkt.book_summaries",
                    "exchange_ts",
                    start,
                    end,
                    assembled,
                    fields=book_fields,
                    filters={"instrument_id": iid},
                )
                result["books"].extend(rows)
                result["books"].extend(await self._condition_book_baseline(iid, anchor, assembled))
                result["books"].extend(
                    await self.repo.latest_market_records(
                        "mkt.book_summaries",
                        time_field="exchange_ts",
                        filters={"instrument_id": iid, "integrity_ok": True},
                        cutoff=end,
                        assembled_at=assembled,
                    )
                )
                book_heads.append(head)
            result["supply"] = await self.repo.latest_market_records(
                "mkt.supply_obs",
                group_by=("token_id", "kind", "chain"),
                time_field="ts",
                filters={"token_id": token_id},
                cutoff=end,
                assembled_at=assembled,
            )
            result["supply"].extend(
                await self.repo.latest_market_records(
                    "mkt.supply_obs",
                    group_by=("token_id", "kind", "chain"),
                    time_field="ts",
                    filters={"token_id": token_id, "available": True},
                    cutoff=end,
                    assembled_at=assembled,
                )
            )
            supply_heads = []
            for kind in ("EXCHANGE_INVENTORY", "INVENTORY", "LENDING"):
                rows, head = await self._condition_projected_history(
                    "mkt.supply_obs",
                    "ts",
                    history_start if kind != "LENDING" else end - timedelta(hours=48),
                    end,
                    assembled,
                    fields=list(MARKET_COLUMNS["mkt.supply_obs"]),
                    filters={"token_id": token_id, "kind": kind},
                )
                result["supply"].extend(rows)
                supply_heads.append(head)
            result["semantics"] = self._instrument_semantics(target)
            observations = [
                o
                for point in result["funding"]
                for o in (
                    (point.get("source_observations") or {}).values()
                    if point.get("source_observations")
                    else [point]
                )
                if o.get("as_of") and parse_time(o["as_of"]) <= end
            ]
            result["funding_confirmations"] = [
                dict(rest, independent_path_verified=True, primary_source_path=ws["source_path"])
                for rest, ws in funding_confirmations(observations, distinct_paths=True)
            ]
            for name in ("funding", "books", "supply", "liquidations"):
                result[name] = list({sha256(row): row for row in result[name]}.values())
            heads["oi"] = sha256([oi_heads, result["semantics"], result["capture_metadata"]])
            heads["funding"] = sha256(
                [
                    funding_heads,
                    [r for r in result["funding"] if r.get("kind") == "CURRENT_ESTIMATE"],
                    result["semantics"],
                    result["capture_metadata"],
                ]
            )
            heads["dc"] = result["dependency_fingerprint"]
            heads["depth"] = sha256(
                [book_heads, result["books"], anchor.isoformat(), result["capture_metadata"]]
            )
            heads["close"] = sha256([price_heads, flow_heads])
            heads["derivatives"] = sha256(
                [derivative_heads, result["funding"], result["semantics"], result["capture_metadata"]]
            )
            heads["books"] = sha256([book_heads, result["books"], anchor.isoformat()])
            heads["supply"] = sha256([supply_heads, result["supply"]])
            heads["support"] = sha256([volume_heads, flow_heads, heads["relative"], heads["books"]])
        result.setdefault("domain_dependency_fingerprints", {}).update(heads)
        result["domain_dependency_fingerprints"]["state"] = result["dependency_fingerprint"]
        return result

    async def condition_snapshot(self, token_id, cutoff, *, anchor=None, requirements):
        """Load persisted condition inputs without flushing capture or reading raw files."""
        from tank.analysis.truth import DERIVATIVE_CONDITION_METRICS, SPOT_CONDITION_METRICS, WINDOWS

        requested = cutoff if isinstance(cutoff, Timestamp) else chosen_boundary(cutoff, precision="EXACT")
        if requested.value is None:
            raise ValueError("Condition snapshot requires a cutoff value")
        end = parse_time(requested.value)
        upper = end + timedelta(microseconds=1)
        if isinstance(anchor, dict):
            anchor = {
                **anchor,
                **propagate_timestamp(anchor, "ts", precision_aliases=("precision",)).fields("ts"),
            }
        elif anchor is not None:
            anchor = {"type": "T0", **norm.timestamp_fields("ts", anchor, None)}
        domains, durations = set(), []
        for requirement in requirements:
            metric, window = requirement["metric"], requirement.get("window", "W1H")
            domain = (
                "spot"
                if metric in SPOT_CONDITION_METRICS
                else "funding"
                if metric == "deriv.funding_vs_normal"
                else "oi"
                if metric in DERIVATIVE_CONDITION_METRICS
                else "close"
                if metric == "price.close_vs_level"
                else "dc"
                if metric == "dc.class"
                else "depth"
                if metric in ("liq.depth_p1_vs_pre", "liq.spread_ratio")
                else "composite"
            )
            domains.add(domain)
            duration = WINDOWS.get(window)
            if window == "EVENT" and anchor and anchor.get("ts"):
                duration = (end - parse_time(anchor["ts"])).total_seconds()
            if duration and duration > 0:
                durations.append(duration)
        if any(r["metric"] in ("mt.move_class", "mt.divergence_state") for r in requirements):
            # The complete graph subsumes primitive inputs; do not issue a second
            # independent history load for a mixed condition requirement batch.
            domains = {"composite"}
        instruments = [i for i in self.instruments.values() if i["token_id"] == token_id]
        spot = [i for i in instruments if i["market_type"] == "SPOT"]
        perp = [i for i in instruments if i["market_type"] != "SPOT"]
        result = {
            "token_id": token_id,
            "anchor": anchor,
            "instruments": instruments,
            "config_version": self.settings.config_version,
            "code_version": self.settings.code_version,
        }
        result.update(requested.fields("cutoff"))
        result.update(requested.fields("requested_cutoff"))
        result.update(collection_clock(iso()).fields("assembled_at"))
        if domains & {"spot", "close", "dc"}:
            duration = max(durations or [3600])
            if domains & {"close", "dc"}:
                duration = max(duration, 86400)
            start = (end - timedelta(seconds=duration)).replace(second=0, microsecond=0)
            bars = []
            for instrument in spot:
                bars.extend(
                    await self._range(
                        "mkt.flow_bars_1m",
                        "minute_start",
                        start=start,
                        end=upper,
                        filters={"instrument_id": instrument["instrument_id"]},
                    )
                )
            result["flow_segments"] = [
                dict(s, instrument_id=b["instrument_id"])
                for b in bars
                for s in (b.get("boundary_segments") or [])
                if parse_time(s["end"]) <= end
            ]
            result["reference_points"] = [
                dict(p, instrument_id=b["instrument_id"])
                for b in bars
                for p in (b.get("reference_points") or [])
                if parse_time(p["boundary"]) <= end
            ]
            result["flow_bars"] = [
                b for b in bars if parse_time(b["minute_start"]) + timedelta(minutes=1) <= end
            ]
        if domains & {"spot", "dc"}:
            result["tickers"] = []
            for instrument in spot:
                point = await self.repo.latest_before(
                    "mkt.tickers_24h", "ts", upper, filters={"instrument_id": instrument["instrument_id"]}
                )
                if point:
                    result["tickers"].append(point)
        if "close" in domains:
            close = end.replace(minute=0, second=0, microsecond=0)
            result["klines"] = []
            for instrument in spot:
                result["klines"].extend(
                    await self._range(
                        "mkt.klines_1m",
                        "open_time",
                        start=close - timedelta(minutes=1),
                        end=close,
                        filters={"instrument_id": instrument["instrument_id"]},
                    )
                )
        if domains & {"oi", "funding", "dc", "depth"}:
            result["capture_metadata"] = await self._capture_metadata(token_id, end)
            result["semantics"] = self._instrument_semantics(instruments if "dc" in domains else perp)
            for domain in domains & {"oi", "funding"}:
                collection, field = COLLECTIONS[domain]
                result[domain] = []
                for instrument in perp:
                    result[domain].extend(
                        await self._condition_history(
                            collection,
                            field,
                            instrument["instrument_id"],
                            end,
                            parse_time(result["assembled_at"]),
                        )
                    )
                    if domain == "funding":
                        result[domain].extend(
                            r
                            for r in await self._condition_history(
                                collection,
                                "funding_time",
                                instrument["instrument_id"],
                                end,
                                parse_time(result["assembled_at"]),
                            )
                            if r.get("as_of") is None
                        )
        if domains & {"depth", "dc"}:
            result["books"] = []
            for instrument in spot:
                iid = instrument["instrument_id"]
                point = await self.repo.latest_before(
                    "mkt.book_summaries", "exchange_ts", upper, filters={"instrument_id": iid}
                )
                if point:
                    result["books"].append(point)
                result["books"].extend(
                    await self.repo.latest_market_records(
                        "mkt.book_summaries",
                        time_field="exchange_ts",
                        filters={"instrument_id": iid, "integrity_ok": True},
                        cutoff=end,
                        assembled_at=result["assembled_at"],
                    )
                )
                if "depth" in domains and anchor and anchor.get("ts"):
                    origin = parse_time(anchor["ts"])
                    if origin <= end:
                        result["books"].extend(
                            await self._condition_book_baseline(iid, origin, result["assembled_at"])
                        )
        if "dc" in domains:
            result["tokens"] = list(self.tokens.values())
            result["sector_baskets"] = deepcopy(self.sector_baskets)
            primary_seconds = 14400
            if anchor and anchor.get("ts"):
                age = (end - parse_time(anchor["ts"])).total_seconds()
                if 900 <= age <= 86400:
                    primary_seconds = age
            primary_start = end - timedelta(seconds=primary_seconds)
            if "oi" not in domains:
                result["oi"] = []
                for instrument in perp:
                    iid = instrument["instrument_id"]
                    result["oi"].extend(
                        await self._range(
                            "mkt.oi_obs",
                            "ts",
                            start=primary_start - timedelta(seconds=60),
                            end=upper,
                            filters={"instrument_id": iid},
                        )
                    )
                    result["oi"].extend(
                        await self.repo.latest_market_records(
                            "mkt.oi_obs",
                            group_by=("instrument_id", "metric_key"),
                            time_field="ts",
                            filters={"instrument_id": iid},
                            cutoff=end,
                            assembled_at=result["assembled_at"],
                        )
                    )
            if "close" not in domains:
                result["klines"] = []
            for instrument in spot:
                result["klines"].extend(
                    await self.repo.latest_market_records(
                        "mkt.klines_1m",
                        time_field="open_time",
                        filters={"instrument_id": instrument["instrument_id"], "complete": True},
                        cutoff=end - timedelta(minutes=1),
                        assembled_at=result["assembled_at"],
                    )
                )
            for instrument in perp:
                result["klines"].extend(
                    await self._range(
                        "mkt.klines_1m",
                        "open_time",
                        start=end - timedelta(days=1),
                        end=upper,
                        filters={"instrument_id": instrument["instrument_id"]},
                    )
                )
            for name in ("funding", "liquidations", "supply"):
                if name == "funding" and "funding" in domains:
                    continue
                collection, field = COLLECTIONS[name]
                groups = (
                    ("token_id", "kind", "chain")
                    if name == "supply"
                    else ("instrument_id", "metric_key", "kind")
                    if name == "funding"
                    else ("instrument_id", "metric_key")
                )
                result[name] = await self.repo.latest_market_records(
                    collection,
                    group_by=groups,
                    time_field=field,
                    filters={"token_id": token_id},
                    cutoff=end,
                    assembled_at=result["assembled_at"],
                )
            result["supply"].extend(
                await self.repo.latest_market_records(
                    "mkt.supply_obs",
                    group_by=("token_id", "kind", "chain"),
                    time_field="ts",
                    filters={"token_id": token_id, "available": True},
                    cutoff=end,
                    assembled_at=result["assembled_at"],
                )
            )
        for name in ("books", "oi", "funding", "supply", "klines", "liquidations", "tickers"):
            if name in result:
                field = COLLECTIONS[name][1]
                result[name] = list(
                    {
                        sha256(row): row
                        for row in result[name]
                        if (stamp := row.get(field) or row.get("funding_time")) is not None
                        and end - timedelta(days=90) <= parse_time(stamp) <= end
                    }.values()
                )
        result["domain_dependency_fingerprints"] = {}
        for domain in domains - {"composite"}:
            if domain in {"oi", "funding"}:
                collection, field = COLLECTIONS[domain]
                heads = [
                    self._condition_histories[(collection, field, i["instrument_id"])]["fingerprint"]
                    for i in perp
                ]
                if domain == "funding":
                    heads.extend(
                        self._condition_histories[(collection, "funding_time", i["instrument_id"])][
                            "fingerprint"
                        ]
                        for i in perp
                    )
                dependency = [heads, result["semantics"], result["capture_metadata"]]
            elif domain in {"dc", "depth"}:
                fields = (
                    ["books", "anchor", "capture_metadata"]
                    if domain == "depth"
                    else [
                        "flow_bars",
                        "flow_segments",
                        "books",
                        "oi",
                        "funding",
                        "klines",
                        "tickers",
                        "supply",
                        "liquidations",
                        "capture_metadata",
                        "semantics",
                    ]
                )
                dependency = [result.get(field) for field in fields]
            else:
                fields = [
                    "flow_bars",
                    "flow_segments",
                    "reference_points",
                    "tickers" if domain == "spot" else "klines",
                ]
                dependency = [result.get(field) for field in fields]
            result["domain_dependency_fingerprints"][domain] = sha256(
                [
                    dependency,
                    instruments,
                    self.settings.config_version,
                    self.settings.code_version,
                ]
            )
        if "composite" in domains:
            heads = []
            for name, (collection, field) in COLLECTIONS.items():
                for order in ("availability", "semantic_latest", "semantic_earliest"):
                    heads.append(
                        await self.repo.market_dependency_head(
                            collection,
                            field,
                            end,
                            start=end - timedelta(days=90),
                            filters={"token_id": token_id} if name == "supply" else None,
                            instrument_ids=None
                            if name == "supply"
                            else [i["instrument_id"] for i in instruments],
                            assembled_at=result["assembled_at"],
                            order=order,
                        )
                    )
            # Benchmark and sector comparator klines are actual full-assessment dependencies.
            peers = [
                i["instrument_id"]
                for i in self.instruments.values()
                if i["market_type"] == "SPOT"
                and (
                    self.tokens[i["token_id"]].get("symbol") in ("BTC", "ETH")
                    or self.tokens[i["token_id"]].get("sector") not in (None, "BENCHMARK")
                    or i["token_id"] in self._sector_tokens
                )
            ]
            for order in ("availability", "semantic_latest", "semantic_earliest"):
                heads.append(
                    await self.repo.market_dependency_head(
                        "mkt.klines_1m",
                        "open_time",
                        end,
                        start=end - timedelta(days=90),
                        instrument_ids=peers,
                        assembled_at=result["assembled_at"],
                        order=order,
                    )
                )
            result["dependency_fingerprint"] = sha256(
                [
                    heads,
                    self.semantics,
                    instruments,
                    self.tokens,
                    self.sector_baskets,
                    await self._capture_metadata(token_id, end),
                    self.settings.config_version,
                    self.settings.code_version,
                ]
            )
        if any(
            r["metric"]
            in ("mt.move_class", "mt.divergence_state", "price.repricing_band", "price.vs_anchor_pct")
            for r in requirements
        ):
            result = await self._condition_composite_inputs(
                result,
                full_state=any(r["metric"] in ("mt.move_class", "mt.divergence_state") for r in requirements),
            )
        return result

    async def _series_counts(self, instrument_ids, start, cutoff, assembled):
        """Complete 1m closes per instrument in [start, cutoff). Whole UTC days before
        today are counted once per process; only the partial edge days are read again."""
        start, cutoff = parse_time(start), parse_time(cutoff)
        first = start.replace(hour=0, minute=0, second=0, microsecond=0)
        if first < start:
            first += timedelta(days=1)
        last = min(cutoff, utcnow()).replace(hour=0, minute=0, second=0, microsecond=0)
        if first >= last:
            return await self.repo.market_series_counts(instrument_ids, start, cutoff, assembled)
        cache = self._kline_day_counts
        days = [first + timedelta(days=n) for n in range((last - first).days)]
        # Read the cache before any await: an archive import evicts a recounted day meanwhile.
        counts = {
            (iid, day): cache[iid, day] for day in days for iid in instrument_ids if (iid, day) in cache
        }
        missing = [day for day in days if any((iid, day) not in counts for iid in instrument_ids)]
        if missing:
            found = await market_series_day_counts(
                self.repo, instrument_ids, missing[0], missing[-1] + timedelta(days=1), assembled
            )
            for day in missing:
                for iid in instrument_ids:
                    counts[iid, day] = cache[iid, day] = found.get((iid, day), 0)
        head = await self.repo.market_series_counts(instrument_ids, start, first, assembled)
        tail = await self.repo.market_series_counts(instrument_ids, last, cutoff, assembled)
        return {
            iid: head.get(iid, 0) + tail.get(iid, 0) + sum(counts[iid, day] for day in days)
            for iid in instrument_ids
        }

    def _day_counts_path(self):
        from pathlib import Path

        return Path(self.settings.data_dir) / "market" / "kline_day_counts.json"

    async def _load_day_counts(self):
        """Past-day counts an earlier process saved, less every day a kline was recorded
        into since (r113: a cold 90-day count was half of an MT snapshot)."""
        path = self._day_counts_path()

        def read():
            try:
                return json.loads(path.read_text())
            except (OSError, ValueError):
                return None

        saved = await asyncio.to_thread(read)
        if not isinstance(saved, dict) or saved.get("version") != 1:
            return 0
        today = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        changed = await market_kline_days_recorded_since(
            self.repo, parse_time(saved["saved_at"]) - DAY_COUNTS_MARGIN, today
        )
        loaded = 0
        # No await from the query to here: a write landing after it evicts what is set now.
        for iid, day, count in saved["counts"]:
            key = (iid, parse_time(day))
            if key[1] < today and key not in changed and key not in self._kline_day_counts:
                self._kline_day_counts[key] = int(count)
                loaded += 1
        return loaded

    async def _save_day_counts(self, *, force=False):
        counts, at = self._kline_day_counts_saved
        if counts == self._kline_day_counts or not force and time.monotonic() - at < DAY_COUNTS_SAVE_S:
            return
        counts = dict(self._kline_day_counts)
        payload = {
            "version": 1,
            "saved_at": iso(),
            "counts": [[iid, iso(day), count] for (iid, day), count in counts.items()],
        }
        path = self._day_counts_path()

        def write():
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_suffix(".tmp")
            temp.write_text(json.dumps(payload))
            temp.replace(path)

        await asyncio.to_thread(write)
        self._kline_day_counts_saved = (counts, time.monotonic())

    async def _warm_series_counts(self):
        """Count past days of every spot series once, a few instruments at a time, so the
        first Market Truth snapshot after a start does not pay for 90 days at once. Counts
        a previous process saved are reused first."""
        try:
            log.info("series counts restored: %d past days", await self._load_day_counts())
        except Exception as exc:  # noqa: BLE001 - the warm-up below counts what is missing
            log.warning("series count restore failed: %s", type(exc).__name__)
        spot = [iid for iid, inst in self.instruments.items() if norm.venue_key(inst).endswith(".spot")]
        end = utcnow()
        for offset in range(0, len(spot), 4):
            if self._stop is not None and self._stop.is_set():
                return
            try:
                await self._series_counts(spot[offset : offset + 4], end - timedelta(days=90), end, iso())
            except Exception as exc:  # noqa: BLE001 - a later snapshot counts what is missing
                log.warning("series count warm-up failed: %s", type(exc).__name__)
                return
            await asyncio.sleep(1)

    async def _shared_call(self, key, factory):
        """Inside snapshot_anchors(): one call per identical request, shared read-only."""
        shared = _SHARED_READS.get()
        if shared is None:
            return await factory()
        calls = shared.setdefault("calls", {})
        if key not in calls:
            calls[key] = await factory()
        return calls[key]

    def _shared_slice(self, collection, field, filters, ids, start, end):
        """Rows of [start, end) cut from the union range read once for all anchors (the
        row objects themselves, not copies; snapshots treat rows as read-only)."""
        shared = _SHARED_READS.get()
        if shared is None:
            return None
        hit = shared.get("ranges", {}).get((collection, field, repr(sorted((filters or {}).items()))))
        if hit is None or hit["start"] > start or hit["end"] < end or set(ids) - set(hit["rows"]):
            return None
        return _cut({iid: hit["rows"][iid] for iid in set(ids)}, field, start, end)

    async def _ranged(self, collection, field, ids, start, end, filters=None, *, books=False):
        sliced = self._shared_slice(collection, field, filters, ids, start, end)
        if sliced is not None:
            return sliced
        if books:
            # Levels only for the newest BOOK_LEVEL_ROWS rows and the newest checked one: in
            # the query on PostgreSQL, by stripping after decode in memory.
            return await market_range_by_instrument(
                self.repo,
                collection,
                field,
                ids,
                start,
                end,
                filters,
                batch_size=WIDE_PAGE_ROWS,
                keep=None if hasattr(self.repo, "_conn") else book_level_keeper(),
                level_rows=BOOK_LEVEL_ROWS if hasattr(self.repo, "_conn") else None,
            )
        # Wide rows (ticker field_sources) in small pages: one fetchall decodes a page.
        page = WIDE_PAGE_ROWS if collection == "mkt.tickers_24h" else 1000
        return await market_range_by_instrument(
            self.repo, collection, field, ids, start, end, filters, batch_size=page
        )

    async def snapshot_anchors(self, token_id, cutoff, anchors):
        """Snapshots of one token and cutoff for several anchors (e.g. PRE_24H/4H/1H) with
        shared reads, yielded one at a time as (anchor, snapshot) from the widest window to
        the narrowest. Each window collection is read once over the widest anchor's range;
        every snapshot is cut from it, and rows older than the next anchor's range are
        released before it is built, so memory stays near one snapshot of the widest
        window. Identical calls are made once. Each snapshot equals
        snapshot(token_id, cutoff, anchor) for its anchor; rows are shared between the
        snapshots, so callers must not modify them."""
        requested = cutoff if isinstance(cutoff, Timestamp) else chosen_boundary(cutoff, precision="EXACT")
        cutoff_dt = parse_time(requested.value)
        history_start = cutoff_dt - timedelta(days=90)
        snapshot_end = cutoff_dt + timedelta(microseconds=1)

        def recent_start(anchor):
            value = anchor.get("ts") if isinstance(anchor, dict) else anchor
            duration = max(timedelta(days=1), cutoff_dt - parse_time(value)) if value else timedelta(days=1)
            return max(history_start, cutoff_dt - 2 * duration - timedelta(minutes=1))

        ordered = sorted(anchors, key=recent_start)
        ids = sorted(r["instrument_id"] for r in self.instruments.values() if r["token_id"] == token_id)
        shared = {"ranges": {}}
        token = _SHARED_READS.set(shared)
        try:
            widest = recent_start(ordered[0])
            for name, (collection, field) in COLLECTIONS.items():
                if name == "supply":
                    continue
                rows = await self._ranged(collection, field, ids, widest, snapshot_end, books=name == "books")
                shared["ranges"][(collection, field, repr([]))] = {
                    "field": field,
                    "start": widest,
                    "end": snapshot_end,
                    "rows": rows,
                }
            funding = COLLECTIONS["funding"]
            narrowest = recent_start(ordered[-1])
            if narrowest > history_start:
                filters = {"kind": "SETTLED"}
                rows = await self._ranged(funding[0], funding[1], ids, history_start, narrowest, filters)
                shared["ranges"][(funding[0], funding[1], repr(sorted(filters.items())))] = {
                    "start": history_start,
                    "end": narrowest,
                    "rows": rows,
                }
            rows = await self._ranged(funding[0], "funding_time", ids, history_start, snapshot_end)
            shared["ranges"][(funding[0], "funding_time", repr([]))] = {
                "start": history_start,
                "end": snapshot_end,
                "rows": rows,
            }
            windows = [hit for hit in shared["ranges"].values() if "field" in hit]
            for anchor in ordered:
                start = recent_start(anchor)
                for hit in windows:
                    if hit["start"] < start:
                        hit["rows"], hit["start"] = (
                            _cut(hit["rows"], hit["field"], start, snapshot_end),
                            start,
                        )
                yield anchor, await self.snapshot(token_id, cutoff, anchor=anchor)
        finally:
            _SHARED_READS.reset(token)

    async def _window_rows(self, token_id, collection, field, ids, start, end, *, books=False):
        """The window rows of all of a token's instruments: one query per collection, or a cut
        of the union range inside snapshot_anchors()."""
        return await self._ranged(collection, field, ids, start, end, books=books)

    async def snapshot(self, token_id, cutoff, anchor=None, *, scope=None):
        """scope="REACTION": a reaction checkpoint (03 §8, 01 FR-R-01) measures its EVENT
        window, anchor to cutoff. Flow bars, klines, OI, funding and liquidations are read
        from the earlier of the anchor and cutoff - 24 h (the reference venue's trailing-24 h
        coverage, 03 §5.7); books and tickers only around the window bounds (the newest
        integrity-checked book and the marks next to the OI bounds). The default reads flow
        bars, klines, OI, funding and liquidations from 2 x the event window before the cutoff
        (previous-window comparisons), books over the last 24 h and the hour before the anchor,
        and tickers only around the window bounds (see the scope branches below)."""
        # Strings passed to this API select an analysis boundary. Timestamp
        # callers preserve the uncertainty of an event-derived schedule.
        requested = cutoff if isinstance(cutoff, Timestamp) else chosen_boundary(cutoff, precision="EXACT")
        if requested.value is None:
            raise ValueError("Snapshot requires a cutoff value")
        cutoff = requested.value
        cutoff_dt = parse_time(cutoff)
        history_start = cutoff_dt - timedelta(days=90)
        anchor_value = anchor.get("ts") if isinstance(anchor, dict) else anchor
        duration = (
            max(timedelta(days=1), cutoff_dt - parse_time(anchor_value))
            if anchor_value
            else timedelta(days=1)
        )
        recent_start = max(history_start, cutoff_dt - 2 * duration - timedelta(minutes=1))
        snapshot_end = cutoff_dt + timedelta(microseconds=1)
        bounded = {}
        if scope == "REACTION":
            anchor_dt = min(parse_time(anchor_value), cutoff_dt) if anchor_value else cutoff_dt
            recent_start = max(
                history_start, min(anchor_dt, cutoff_dt - timedelta(days=1)) - timedelta(minutes=1)
            )
            bounds = _merge_ranges(
                [
                    (anchor_dt - REACTION_BOUND_MARGIN, min(anchor_dt + REACTION_BOUND_MARGIN, snapshot_end)),
                    (cutoff_dt - REACTION_NEWEST_MARGIN, snapshot_end),
                ]
            )
            bounded = {"books": bounds, "tickers": bounds}
        elif scope is None:
            # Market Truth reads books over the last 24 h (W24H liquidity: hit-side depth,
            # bursts and replenishment around every flow bar) and the hour before the anchor
            # (pre-event depth); the latest ticker at or before each window bound and previous-
            # window bound (spot coverage estimate) and the marks next to the OI points at the
            # window bounds (03 §5.4 USD endpoints). Nothing else of two days of books/tickers.
            from tank.analysis.truth import WINDOWS

            lower = cutoff_dt - timedelta(days=1)
            if anchor_value:
                lower = min(lower, parse_time(anchor_value) - timedelta(hours=1))
            book_interval = timedelta(seconds=float(self.thresholds.get("book_interval_s", 10)))
            edges = {cutoff_dt, *(cutoff_dt - timedelta(seconds=s) for s in WINDOWS.values())}
            if anchor_value:
                edges.add(parse_time(anchor_value))
            bounded = {
                "books": [(max(recent_start, lower - book_interval), snapshot_end)],
                "tickers": _merge_ranges(
                    [
                        (
                            max(recent_start, edge - TICKER_ASOF_MARGIN),
                            min(edge + TICKER_MARK_MARGIN, snapshot_end),
                        )
                        for edge in edges
                    ]
                ),
            }
        else:
            raise ValueError(f"Unknown snapshot scope: {scope}")
        result = {
            "token_id": token_id,
            **requested.fields("cutoff"),
            **requested.fields("requested_cutoff"),
            "anchor": {
                **anchor,
                **propagate_timestamp(anchor, "ts", precision_aliases=("precision",)).fields("ts"),
            }
            if isinstance(anchor, dict)
            else {"type": "T0", **norm.timestamp_fields("ts", anchor, None)}
            if anchor is not None
            else None,
            "config_version": self.settings.config_version,
            "code_version": self.settings.code_version,
        }
        instruments = [r for r in self.instruments.values() if r["token_id"] == token_id]
        ids = {r["instrument_id"] for r in instruments}
        self._register_capture_boundaries(ids, cutoff, anchor)
        await self._flush_flow(allow_recovery=False)
        # Callers can hold a DB connection (a job lock) that a running flush is waiting for:
        # never queue behind it; that flush already carries the pending rows it took.
        if not self._market_flush_lock.locked():
            await self.flush_market()
        result["flow_segments"] = []
        result["reference_points"] = []
        for name, (collection, field) in COLLECTIONS.items():
            values = []
            # One query per series family for all of the token's instruments (BTC: 9),
            # split per instrument in the same order as one range read per instrument.
            if name in bounded:
                window_rows = {iid: [] for iid in ids}
                for low, high in bounded[name]:
                    part = await self._window_rows(
                        token_id, collection, field, ids, low, high, books=name == "books"
                    )
                    for iid in ids:
                        window_rows[iid].extend(part[iid])
            elif name != "supply":
                window_rows = await self._window_rows(
                    token_id, collection, field, ids, recent_start, snapshot_end, books=name == "books"
                )
            if name == "oi" and recent_start > history_start:
                sampled = {iid: [] for iid in ids}
                for row in await self._shared_call(
                    ("sampled", collection, field, tuple(sorted(ids)), history_start, recent_start),
                    lambda collection=collection, field=field: sampled_market_history(
                        self.repo,
                        collection,
                        field,
                        ("metric_key",),
                        sorted(ids),
                        history_start,
                        recent_start,
                        300,
                    ),
                ):
                    sampled[row["instrument_id"]].append(row)
            if name == "funding":
                settled_history = (
                    await self._ranged(
                        collection, field, ids, history_start, recent_start, {"kind": "SETTLED"}
                    )
                    if recent_start > history_start
                    else {}
                )
                settled_by_time = await self._ranged(
                    collection, "funding_time", ids, history_start, snapshot_end
                )
            for iid in ids if name != "supply" else [None]:
                # Event windows (03 §5.3-§5.5) are read in full from recent_start. Only
                # the normal ranges reach back 90 days (03 §0): OI at the base-tier
                # cadence (300 s, 03 §5.1), funding as its settled history; book,
                # ticker, flow and liquidation history beyond the windows is unused.
                if iid is None:
                    rows = await self._range(
                        collection,
                        field,
                        start=history_start,
                        end=snapshot_end,
                        filters={"token_id": token_id},
                    )
                else:
                    rows = list(window_rows[iid])
                if name == "oi" and recent_start > history_start:
                    rows = sampled[iid] + rows
                if name == "funding":
                    if recent_start > history_start:
                        rows = settled_history[iid] + rows
                    rows.extend(row for row in settled_by_time[iid] if row.get("as_of") is None)
                for row in rows:
                    if name == "flow_bars":
                        result["flow_segments"].extend(
                            dict(segment, instrument_id=iid)
                            for segment in (row.get("boundary_segments") or [])
                            if parse_time(segment["end"]) <= cutoff_dt
                        )
                        result["reference_points"].extend(
                            dict(point, instrument_id=iid)
                            for point in (row.get("reference_points") or [])
                            if parse_time(point["boundary"]) <= cutoff_dt
                        )
                    stamp = row.get(field) or row.get("funding_time") or row.get("settlement_time")
                    if stamp is None or parse_time(stamp) > cutoff_dt:
                        continue
                    if (
                        name in ("flow_bars", "klines")
                        and parse_time(stamp) + timedelta(minutes=1) > cutoff_dt
                    ):
                        continue
                    if name == "books" and row.get("independent_rest"):
                        independent = row["independent_rest"]
                        if any(parse_time(independent[f]) > cutoff_dt for f in ("ts",) if independent.get(f)):
                            row = dict(
                                row,
                                second_path_confirmed=False,
                                independent_rest=None,
                                second_path_reason="CONFIRMATION_AFTER_CUTOFF",
                            )
                    values.append(row)
            result[name] = sorted(
                values, key=lambda r: r.get(field) or r.get("funding_time") or r.get("settlement_time")
            )
        # Benchmarks plus the peers Market Truth can compare against (03 §5.7): the
        # token's sector, or the DeFi sectors when its sector has fewer than two peers.
        members = {}
        for t in self.tokens.values():
            if t.get("sector"):
                members.setdefault(t["sector"], set()).add(t["token_id"])
        for basket in self.sector_baskets:
            if basket.get("sector"):
                members.setdefault(basket["sector"], set()).update(basket.get("constituents", []))
        sector = next(
            (name for name, ids_ in members.items() if token_id in ids_ and name != "BENCHMARK"),
            self.tokens.get(token_id, {}).get("sector"),
        )
        same_sector = members.get(sector, set()) - {token_id} if sector != "BENCHMARK" else set()
        # IDENTITY_PAYMENTS has no sector adjustment at all (Market Truth: not applicable).
        if len(same_sector) < 2 and sector != "IDENTITY_PAYMENTS":
            same_sector |= {
                identity
                for name in (
                    "DEX_EXECUTION",
                    "LENDING_CREDIT",
                    "PERP_DEX",
                    "RWA",
                    "INTEROPERABILITY",
                    "SYNTHETIC_DOLLAR",
                )
                for identity in members.get(name, ())
            }
        peers = {
            t["token_id"]
            for t in self.tokens.values()
            if t.get("symbol") in ("BTC", "ETH") or t["token_id"] in same_sector
        } - {token_id}
        shared = _SHARED_READS.get()
        assembled = shared.setdefault("assembled", iso()) if shared is not None else iso()
        # A live snapshot (cutoff within the last hour) extends one cursor per series;
        # a historical cutoff reads its own fixed window.
        rolling = "LIVE_SNAPSHOT" if cutoff_dt >= utcnow() - timedelta(hours=1) else None
        captured_klines = None
        if recent_start == history_start:
            # Long event windows already loaded every target candle used by the
            # statistical histories. Project those same observations once more
            # in memory instead of reading the entire history twice from SQL.
            captured_klines = {iid: [] for iid in ids}
            for row in result["klines"]:
                captured_klines[row["instrument_id"]].append(row)
        all_spot = [
            inst
            for inst in self.instruments.values()
            if norm.venue_key(inst).endswith(".spot") and inst["token_id"] in peers | {token_id}
        ]
        counts = await self._shared_call(
            ("counts", tuple(inst["instrument_id"] for inst in all_spot), history_start, cutoff_dt),
            lambda: self._series_counts(
                [inst["instrument_id"] for inst in all_spot], history_start, cutoff_dt, assembled
            ),
        )
        result["series_coverage_counts"] = counts
        result["stat_price_series"] = {}
        result["stat_volume_series"] = {}
        latest_closes = (
            {
                row["instrument_id"]: row
                for row in await self._shared_call(
                    (
                        "latest_closes",
                        tuple(sorted(ids | {inst["instrument_id"] for inst in all_spot})),
                        cutoff_dt,
                    ),
                    lambda: self.repo.latest_market_records(
                        "mkt.klines_1m",
                        time_field="open_time",
                        instrument_ids=sorted(ids | {inst["instrument_id"] for inst in all_spot}),
                        start=cutoff_dt - timedelta(days=90),
                        cutoff=cutoff_dt - timedelta(minutes=1),
                        assembled_at=assembled,
                    ),
                )
            }
            if all_spot
            else {}
        )
        present_closes = {(row["instrument_id"], parse_time(row["open_time"])) for row in result["klines"]}

        async def retain_close(row):
            if row is not None:
                identity = (row["instrument_id"], parse_time(row["open_time"]))
                if identity not in present_closes:
                    present_closes.add(identity)
                    result["klines"].append(row)

        async def dense_prices(inst):
            iid = inst["instrument_id"]
            series = await self._condition_kline_series(
                inst,
                history_start,
                cutoff_dt,
                assembled,
                kind="price",
                coverage_count=counts.get(iid, 0),
                fixed_window=True,
                captured_rows=captured_klines.get(iid) if captured_klines is not None else None,
                rolling=rolling,
            )
            result["stat_price_series"][iid] = series
            # Exact reference prices remain original decimal observations, not
            # values converted back from statistical floats, even across gaps.
            wanted = set(series["times_us"][-1:])
            hourly = next((v for v in reversed(series["times_us"]) if v % 3600000000 == 0), None)
            if hourly is not None:
                wanted.add(hourly)
            epoch = parse_time("1970-01-01T00:00:00Z")
            for stamp in wanted:
                opened = epoch + timedelta(microseconds=int(stamp)) - timedelta(minutes=1)
                if (iid, opened) not in present_closes:
                    for row in await self.repo.latest_market_records(
                        "mkt.klines_1m",
                        time_field="open_time",
                        start=opened,
                        cutoff=opened,
                        assembled_at=assembled,
                        instrument_ids=[iid],
                    ):
                        await retain_close(row)

        for inst in instruments:
            iid = inst["instrument_id"]
            await retain_close(latest_closes.get(iid))
            if norm.venue_key(inst).endswith(".spot"):
                await dense_prices(inst)
            volume = await self._condition_kline_series(
                inst,
                history_start,
                cutoff_dt,
                assembled,
                kind="volume",
                coverage_count=counts.get(iid, 0),
                fixed_window=True,
                captured_rows=captured_klines.get(iid) if captured_klines is not None else None,
                rolling=rolling,
            )
            if volume["times_us"]:
                result["stat_volume_series"][iid] = volume
        chosen = {}
        for inst in all_spot:
            if inst["token_id"] not in peers:
                continue
            current = chosen.get(inst["token_id"])
            if current is None or counts[inst["instrument_id"]] > counts[current["instrument_id"]]:
                chosen[inst["token_id"]] = inst
        # Sector adjustment uses returns at the requested bounds, not a dense
        # 90-day history for every peer. Preserve the original 90-day venue
        # ranking, then retain the same as-of prices at those bounds. BTC/ETH
        # keep their full histories for beta, normal ranges and market context.
        from math import isfinite

        boundaries = [cutoff_dt - timedelta(seconds=n) for n in (0, 900, 3600, 14400, 86400)]
        if result["anchor"] and result["anchor"].get("ts"):
            boundaries.append(parse_time(result["anchor"]["ts"]))
        epoch = parse_time("1970-01-01T00:00:00Z")
        sector_ids = [
            inst["instrument_id"]
            for inst in chosen.values()
            if self.tokens[inst["token_id"]].get("symbol") not in {"BTC", "ETH"}
        ]
        sector_points = {iid: {} for iid in sector_ids}
        for boundary in sorted(set(boundaries)):
            upper_open = boundary - timedelta(minutes=1)
            if upper_open < history_start or not sector_ids:
                continue
            # One indexed query serves every chosen sector peer at this bound.
            # Only an invalid observed close needs a narrower backward seek.
            pending = await self.repo.latest_market_records(
                "mkt.klines_1m",
                time_field="open_time",
                instrument_ids=sector_ids,
                filters={"complete": True, "interval_s": 60},
                start=history_start,
                cutoff=upper_open,
                assembled_at=assembled,
            )
            for row in pending:
                iid = row["instrument_id"]
                while True:
                    value = float(row["c"]) if row.get("c") is not None else float("nan")
                    opened = parse_time(row["open_time"])
                    if isfinite(value) and value > 0:
                        delta = opened + timedelta(minutes=1) - epoch
                        stamp = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
                        sector_points[iid][stamp] = value
                        break
                    before = opened - timedelta(microseconds=1)
                    if before < history_start:
                        break
                    earlier = await self.repo.latest_market_records(
                        "mkt.klines_1m",
                        time_field="open_time",
                        instrument_ids=[iid],
                        filters={"complete": True, "interval_s": 60},
                        start=history_start,
                        cutoff=before,
                        assembled_at=assembled,
                    )
                    if not earlier:
                        break
                    row = earlier[0]
        for inst in self.instruments.values():
            if inst["token_id"] not in peers or not norm.venue_key(inst).endswith(".spot"):
                continue
            instruments.append(inst)
            if self.tokens[inst["token_id"]].get("symbol") not in {"BTC", "ETH"}:
                # Freshness is assessed per venue, including venues not chosen
                # for the sector return. Keep its original last observation.
                if inst["instrument_id"] in latest_closes:
                    await retain_close(latest_closes[inst["instrument_id"]])
                if chosen[inst["token_id"]]["instrument_id"] != inst["instrument_id"]:
                    continue
                points = sector_points[inst["instrument_id"]]
                times = sorted(points)
                result["stat_price_series"][inst["instrument_id"]] = {
                    "instrument_id": inst["instrument_id"],
                    "token_id": inst["token_id"],
                    "venue": inst["venue"],
                    "market_type": inst["market_type"],
                    "time_basis": "CLOSE_TIME",
                    "coverage_count": counts[inst["instrument_id"]],
                    "times_us": times,
                    "values": [points[stamp] for stamp in times],
                    "complete": [1] * len(times),
                }
                continue
            await retain_close(latest_closes.get(inst["instrument_id"]))
            if chosen[inst["token_id"]]["instrument_id"] == inst["instrument_id"]:
                await dense_prices(inst)
        result["semantics"] = self._instrument_semantics(instruments)
        result["instruments"] = instruments
        result["tokens"] = list(self.tokens.values())
        result["sector_baskets"] = deepcopy(self.sector_baskets)
        gaps = await self._shared_call(
            ("gaps", token_id),
            lambda: self.repo.list("mkt.capture_gaps", filters={"token_id": token_id}, limit=100000),
        )
        result["gaps"] = [g for g in gaps if parse_time(g["start"]) <= cutoff_dt]
        runs = await self._shared_call(
            ("runs", token_id),
            lambda: self.repo.list("mkt.backfill_runs", filters={"token_id": token_id}, limit=10000),
        )
        anchor_ts = (
            anchor.get("ts") if isinstance(anchor, dict) else iso(anchor) if anchor is not None else None
        )
        runs = [r for r in runs if anchor_ts is None or parse_time(r["anchor"]) == parse_time(anchor_ts)]
        result["backfill_runs"] = runs
        latest = max(runs, key=lambda r: r["started_at"]) if runs else None
        result["capture_status"] = {
            "status": latest["status"] if latest else "NOT_REQUESTED",
            "reason": latest.get("reason") if latest else "NO_DURABLE_CAPTURE_REQUEST",
            "backfill_id": latest.get("backfill_id") if latest else None,
            "observed_at": iso(),
            "bounded_wait_s": 0,
            "incomplete_windows_excluded": True,
        }
        if latest and latest["backfill_id"] in self._capture_waits:
            waited = self._capture_waits[latest["backfill_id"]]
            result["capture_status"]["bounded_wait_s"] = waited["bounded_wait_s"]
            if latest["status"] in ("PENDING", "RUNNING"):
                result["capture_status"].update(waited)
        result["funding_source_observations"] = [
            observation
            for point in result["funding"]
            for observation in (
                list(point["source_observations"].values()) if point.get("source_observations") else [point]
            )
        ]
        result["funding_confirmations"] = [
            {**rest, "independent_path_verified": True, "primary_source_path": ws.get("source_path")}
            for rest, ws in funding_confirmations(result["funding_source_observations"])
        ]
        result["capture_metadata"] = await self._shared_call(
            ("capture_metadata", token_id, cutoff), lambda: self._capture_metadata(token_id, cutoff)
        )
        result["capture_tier"] = result["capture_metadata"]["tier"]
        self._snapshot_heads(result, cutoff_dt)
        result.update(collection_clock(iso()).fields("assembled_at"))
        return result

    @staticmethod
    def _snapshot_heads(result, cutoff_dt):
        """Dependency heads for Market Truth's cross-assessment caches, from the returned data
        (no DB calls). The relative head covers exactly what can enter truth's pre-start price
        history (every price series point and spot kline before the analysis start: the
        anchor, else cutoff - 4 h), so it stays constant minute to minute for a fixed anchor."""
        from array import array
        from bisect import bisect_left

        anchor = result.get("anchor") or {}
        start = parse_time(anchor["ts"]) if anchor.get("ts") else cutoff_dt - timedelta(hours=4)
        epoch = parse_time("1970-01-01T00:00:00Z")
        delta = start - epoch
        start_us = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
        digest = hashlib.sha256()
        for iid in sorted(result.get("stat_price_series") or {}):
            series = result["stat_price_series"][iid]
            times = series.get("times_us") or []
            k = bisect_left(times, start_us)
            values = series.get("values") or []
            digest.update(repr((iid, series.get("token_id"), series.get("time_basis"))).encode())
            digest.update(array("q", times[:k]).tobytes())
            digest.update(
                array("d", values[:k]).tobytes()
                if isinstance(values, array)
                else repr(list(values[:k])).encode()
            )
            digest.update(bytes(series.get("complete", b"")[:k]))
        rows = sorted(
            (
                (
                    r["instrument_id"],
                    r["open_time"],
                    r.get("c"),
                    r.get("complete"),
                    r.get("interval_s"),
                    r.get("token_id"),
                )
                for r in result.get("klines", [])
                if r.get("open_time") and parse_time(r["open_time"]) < start
            ),
            key=repr,
        )
        digest.update(repr(rows).encode())
        result["relative_history_dependency_fingerprint"] = digest.hexdigest()
        result["domain_dependency_fingerprints"] = {
            name: sha256(sorted(r.get("semantic_fingerprint") or sha256(r) for r in result.get(name, [])))
            for name in ("oi", "funding")
        }

    async def coverage(self):
        hot = {r["token_id"] for r in await self.repo.list("mkt.hot_tokens", limit=10000)}
        result = []
        for iid, inst in self.instruments.items():
            state = self._capture_checkpoints.get(iid)
            gaps = await self.repo.list("mkt.capture_gaps", filters={"instrument_id": iid}, limit=10000)
            result.append(
                {
                    "instrument_id": iid,
                    "token_id": inst["token_id"],
                    "venue": inst["venue"],
                    "market_type": inst["market_type"],
                    "tier": "HOT" if inst["token_id"] in hot else "BASE",
                    "stream_connected": iid in self._connected_since,
                    "last_trade_ts": state.get("last_ts") if state else None,
                    "metadata_status": inst.get("metadata_status", "UNVERIFIED"),
                    "gap_count": len(gaps),
                    "latest_gap": gaps[-1] if gaps else None,
                }
            )
        from pathlib import Path

        paths = [
            ("binance", "SPOT"),
            ("binance", "PERP_LINEAR"),
            ("okx", "SPOT"),
            ("okx", "PERP_LINEAR"),
            ("bybit", "SPOT"),
            ("bybit", "PERP_LINEAR"),
            ("coinbase", "SPOT"),
            ("kraken", "SPOT"),
            ("hyperliquid", "PERP_LINEAR"),
        ]
        matrix = []
        for token_id in self.tokens:
            for venue, market_type in paths:
                rows = [
                    row
                    for row in result
                    if row["token_id"] == token_id
                    and row["venue"] == venue
                    and row["market_type"] == market_type
                ]
                matrix.append(
                    {
                        "token_id": token_id,
                        "venue": venue,
                        "market_type": market_type,
                        "status": "LIVE_VERIFIED"
                        if rows and all(row["metadata_status"] == "LIVE_VERIFIED" for row in rows)
                        else "CONFIGURED_UNTESTED"
                        if rows
                        else "NO_REGISTRY_INSTRUMENT",
                        "instruments": rows,
                    }
                )
        path = Path(self.settings.data_dir) / "market" / "coverage" / "latest.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps({"assembled_at": iso(), "matrix": matrix}, indent=2))
        temp.replace(path)
        return result

    async def _instrument_loop(self, instrument):
        iid = instrument["instrument_id"]
        deriv_due = 0
        audit_due = time.monotonic() + 60  # after the first poll of every series
        while not self._stop.is_set():
            started = time.monotonic()
            hot = instrument["token_id"] in self._hot_since  # hot set, refreshed by _sync_streams
            # HOT tickers and derivatives run on their own 60 s schedule (_hot_context_loop).
            do_deriv = started >= deriv_due and not hot
            try:
                await self.poll_once(iid, derivatives=do_deriv, ticker=not hot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                await self._gap(instrument, "POLL", exc)
            if time.monotonic() >= audit_due:
                # A poll returns only the newest candles: minutes lost while this process
                # was down or stalled are fetched back over REST (1m klines, trailing 25 h).
                audit_due = time.monotonic() + float(self.thresholds.get("kline_audit_interval_s", 3600))
                now = utcnow().replace(second=0, microsecond=0)
                try:
                    await self._backfill_klines(
                        instrument, now - timedelta(hours=25), now, include_reference=False, kind="KLINES_GAP"
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - the next audit retries; the gap stays visible
                    await self._gap(instrument, "KLINES_GAP", exc)
            if do_deriv:
                deriv_due = time.monotonic() + float(self.thresholds.get("base_oi_funding_interval_s", 300))
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._stop.wait(),
                    max(
                        1,
                        float(self.thresholds.get("base_kline_interval_s", 60))
                        - (time.monotonic() - started),
                    ),
                )

    async def _hot_context_loop(self, instrument):
        """HOT tickers, OI and funding every 60 s (03 §5.1), on their own schedule: the base
        cycle over every series of a venue shares one host and must not stretch it."""
        iid = instrument["instrument_id"]
        interval = float(self.thresholds.get("hot_poll_interval_s", HOT_POLL_S))
        while not self._stop.is_set():
            if instrument["token_id"] not in self._hot_since:
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), 5)
                continue
            started = time.monotonic()
            # The ticker first and on its own: it must not wait behind (or fail with) the
            # heavier derivative reads (r109: hyperliquid ticker 96.7 s old at a cutoff).
            for kinds in ({"derivatives": False}, {"ticker": False}):
                try:
                    await self.poll_once(iid, klines=False, **kinds)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                    await self._gap(instrument, "POLL", exc)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), max(1, interval - (time.monotonic() - started)))

    async def bootstrap_history(self, *, days=90):
        """Daily: import missing verified archive days, newest first; failures remain visible
        gaps and are fetched again on the next pass (the filesystem and the imported rows
        are the only state)."""
        while self._stop is None or not self._stop.is_set():
            await self._oi_history_pass(days)
            await self._bootstrap_pass(days)
            if self._stop is None:
                return
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), 86400)

    async def _oi_history_pass(self, days):
        """Bybit 5-min OI history back `days` (02 §4.6), older than the first stored sample,
        so venue-synchronized OI normal ranges have their 7+ days (03 §0). Rows use the live
        metric key and 300 s grid; samples already captured live are not rewritten."""
        end, start = utcnow(), utcnow() - timedelta(days=days)
        for inst in self.instruments.values():
            if norm.venue_key(inst) != "bybit.linear" or not inst.get("active", True):
                continue
            if self._stop is not None and self._stop.is_set():
                return
            iid = inst["instrument_id"]
            stored = await self._range(
                "mkt.oi_obs",
                "ts",
                start=start,
                end=end,
                filters={"instrument_id": iid, "metric_key": "bybit.linear.open-interest"},
            )
            upto = parse_time(stored[0]["ts"]) - timedelta(seconds=1) if stored else end
            if upto <= start:
                continue
            adapter, cursor = self.adapter(inst, priority=2), None
            try:
                for _ in range(1000):
                    rows, cursor = await adapter.oi_history(start=start, end=upto, cursor=cursor)
                    for raw in rows:
                        row = norm.oi(inst, raw)
                        await self._put("mkt.oi_obs", iid + ":" + row["metric_key"] + ":" + row["ts"], row)
                    await self.flush_market(raw=False)
                    if not rows or not cursor:
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - a venue failure stays a visible historical gap
                await self._gap(inst, "HISTORICAL_OI", exc, start=iso(start), end=iso(upto))

    async def _bootstrap_pass(self, days):
        from .archive import fetch_archive

        instruments = [i for i in self.instruments.values() if norm.venue_key(i).startswith("binance")]
        for age in range(1, days + 1):
            date = (utcnow() - timedelta(days=age)).date().isoformat()
            # OI is inexpensive and needed for uncontaminated normal distributions.
            for inst in instruments:
                if self._stop is not None and self._stop.is_set():
                    return
                kinds = ["metrics", "klines"] if norm.venue_key(inst) == "binance.usdm" else ["klines"]
                for kind in kinds:
                    try:
                        await fetch_archive(
                            self.repo,
                            self.settings,
                            inst["venue_symbol"],
                            date,
                            market_type=inst["market_type"],
                            kind=kind,
                            http=self.http,
                            market=self,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                        await self._gap(
                            inst,
                            "HISTORICAL_" + kind.upper(),
                            exc,
                            start=date + "T00:00:00Z",
                            end=iso(parse_time(date) + timedelta(days=1)),
                        )

    async def _supply_loop(self):
        from .supply import SupplyCapture

        capture = SupplyCapture(self)
        while not self._stop.is_set():
            hot = set(self._hot_since)
            try:
                await capture.poll(hot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                log.warning("supply capture failed: %s", type(exc).__name__)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), 60)

    async def _repair_on_start(self, repairs):
        for token_id, start, end in repairs:
            log.info("startup repair %s: %s → %s", token_id, iso(start), end)
            try:
                result = await self.repair_windows(token_id, start, end)
                log.info("startup repair %s: %s", token_id, result)
            except Exception as exc:  # noqa: BLE001 - one token's repair failure leaves explicit gaps
                log.warning("startup repair %s failed: %s", token_id, type(exc).__name__)

    async def run(self, stop):
        self._stop = stop
        if self.settings.replay:
            return
        workers = []
        try:
            async with self.repo.lock("market-hot-set"), self.repo.transaction():
                active_tokens = {
                    row["token_id"] for row in await self.repo.list("mkt.hot_tokens", limit=10000)
                }
                for token_id in self.tokens:
                    await self._record_capture_tier(
                        token_id, "HOT" if token_id in active_tokens else "BASE", "CAPTURE_POLICY_STARTED"
                    )
            await self._ensure_market_restore()
            await self._sync_streams()
            started = iso()
            earliest = {}
            for hot in await self.repo.list("mkt.hot_tokens", limit=10000):
                event = await self.repo.get("evt.events", hot["event_id"]) or {}
                at = parse_time(event_anchor(event)["ts"] if event else hot["since"])
                span = earliest.setdefault(hot["token_id"], [at - WINDOW_BEFORE, at + WINDOW_AFTER])
                span[0], span[1] = min(span[0], at - WINDOW_BEFORE), max(span[1], at + WINDOW_AFTER)
            if earliest:
                # A restart is a reconnect (03 §5.2 step 2): repair each hot token's windows
                # from the last persisted print up to this start; streams cover from here on.
                repairs = [
                    (token_id, start, iso(min(parse_time(started), end)))
                    for token_id, (start, end) in earliest.items()
                ]
                task = asyncio.create_task(self._repair_on_start(repairs))
                self._background.add(task)
                task.add_done_callback(self._background.discard)
            for hot in await self.repo.list("mkt.hot_tokens", limit=10000):
                event = await self.repo.get("evt.events", hot["event_id"]) or {}
                await self._schedule_event_backfill(
                    hot["event_id"], hot["token_id"], event_anchor(event)["ts"] if event else hot["since"]
                )
            workers = [asyncio.create_task(self._instrument_loop(inst)) for inst in self.instruments.values()]
            workers += [
                asyncio.create_task(self._hot_context_loop(inst)) for inst in self.instruments.values()
            ]
            workers.append(asyncio.create_task(self._warm_series_counts()))
            workers.append(asyncio.create_task(self._seal_loop()))
            workers.append(asyncio.create_task(self._loop_watchdog()))
            workers.append(asyncio.create_task(self._supply_loop()))
            workers.append(
                asyncio.create_task(
                    self.bootstrap_history(days=int(self.thresholds.get("historical_lookback_days", 90)))
                )
            )
            while not stop.is_set():
                await self._flush_flow()
                await self.flush_market()
                await self._sync_streams()
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), RAW_FLUSH_S)
        finally:
            for task in [*workers, *self.streams.values(), *self._background]:
                task.cancel()
            await asyncio.gather(*workers, *self.streams.values(), *self._background, return_exceptions=True)
            self.streams.clear()
            # Supervisors reuse this instance after transient failures. Only
            # the owner calls close() when the service lifetime actually ends.
            self._background.clear()
            self._stream_groups.clear()
            self._stream_controls.clear()
            self._connected_since.clear()
            self._stream_watermarks.clear()
