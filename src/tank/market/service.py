import asyncio
import hashlib
import json
import logging
import time
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Sequence
from contextlib import suppress
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import httpx

from tank.contracts.provenance import ResolvedVersionIdentity
from tank.market import normalize as norm
from tank.market.capture import TimeInput
from tank.market.capture_composition import build_capture_lifecycle
from tank.market.capture_contracts import (
    BackfillRun,
    CaptureReadiness,
    CaptureTier,
    CaptureWaitOutcome,
    HotToken,
    RepairOutcome,
    TokenEscalation,
)
from tank.market.rate_gate import PriorityGate
from tank.market_storage import (
    market_dependency_head,
    market_kline_days_recorded_since,
    market_range_by_instrument,
    market_rows_recorded_since,
    market_series_day_counts,
    sampled_market_history,
)
from tank.sources.capture_fetch import is_exchange_url
from tank.time_precision import Timestamp, chosen_boundary, collection_clock, propagate_timestamp
from tank.util import clock_iso, iso, jsonable, new_id, parse_time, sha256, utcnow

REACTION_BOUND_MARGIN = timedelta(minutes=5)
REACTION_NEWEST_MARGIN = timedelta(minutes=15)


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


def _bound_windows(windows):
    """Drop cached windows unused for WINDOW_TTL_S, then the least recently used ones until
    at most WINDOW_ROWS rows and WINDOW_ENTRIES keys remain."""
    now = time.monotonic()
    for key in list(windows):
        windows[key] = [lane for lane in windows[key] if now - lane.get("used", now) < WINDOW_TTL_S]
        if not windows[key]:
            windows.pop(key)
    lanes = sorted(
        ((lane.get("used", now), key, lane) for key, held in windows.items() for lane in held),
        key=lambda item: item[0],
    )
    total = sum(lane.get("size", 0) for _, _, lane in lanes)
    keys = len(windows)
    for _, key, lane in lanes:
        if total <= WINDOW_ROWS and keys <= WINDOW_ENTRIES:
            break
        windows[key].remove(lane)
        total -= lane.get("size", 0)
        if not windows[key]:
            windows.pop(key)
            keys -= 1


def _place_levels(points):
    """Book levels on the newest BOOK_LEVEL_ROWS rows and the newest integrity-checked row
    only, as a fresh read places them; other rows are replaced by copies without them."""
    newest_ok = next(
        (n for n in range(len(points) - 1, -1, -1) if points[n][2].get("integrity_ok") is True), None
    )
    for n in range(len(points) - BOOK_LEVEL_ROWS):
        when, key, row = points[n]
        if n != newest_ok and ("bids" in row or "asks" in row):
            points[n] = (when, key, {k: v for k, v in row.items() if k not in ("bids", "asks")})


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


_SHARED_READS = ContextVar("market_shared_snapshot_reads", default=None)

log = logging.getLogger(__name__)

TANK_CAPTURE_TABLES = frozenset(
    {
        "mkt.flow_bars_1m",
        "mkt.klines_1m",
        "mkt.book_summaries",
        "mkt.oi_obs",
        "mkt.funding_obs",
        "mkt.liq_obs",
        "mkt.tickers_24h",
        "mkt.trades",
        "mkt.capture_streams",
    }
)

HOT_SET_SYNC_S = 5

STREAM_PING_INTERVAL_S = 20
FLUSH_ROWS = 500

SOURCE_BUCKETED_OI, SOURCE_BUCKET_S = ("bybit.linear.open-interest", "binance.usdm.openInterestHist"), 300.0

HOST_IN_FLIGHT = 4
EXCHANGE_REST_OWNER = "exchange REST belongs to tank-capture (capture/README.md)"

STREAM_STATE_STALE_S = 30

CAPTURE_REQUEST_DEFAULT_S, CAPTURE_REQUEST_GRACE_S = 30, 2
CAPTURE_REQUEST_TERMINAL = ("COMPLETE", "PARTIAL", "FAILED", "EXPIRED")

LOOP_STALL_S = 10


HEAD_REUSE_S = 300


DAY_COUNTS_SAVE_S = 60
DAY_COUNTS_MARGIN = timedelta(minutes=30)
SERIES_MARGIN = timedelta(hours=2)
SERIES_TRIM = timedelta(days=1)
SERIES_APPLY_ROWS = 20000

WIDE_PAGE_ROWS = 500
BOOK_LEVEL_ROWS = 30


WINDOW_CACHED = frozenset(
    {"mkt.flow_bars_1m", "mkt.klines_1m", "mkt.oi_obs", "mkt.funding_obs", "mkt.book_summaries"}
)
WINDOW_MARGIN = timedelta(seconds=5)
WINDOW_ENTRIES = 32
WINDOW_LANES = 4
WINDOW_KEEP_S = 600


WINDOW_TTL_S = 300
WINDOW_ROWS = 300_000

SERIES_TTL_S = 1800
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


class _MarketCaptureClock:
    def now(self) -> datetime:
        return utcnow()

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, delay_s: float) -> None:
        await asyncio.sleep(delay_s)

    def identity(self, prefix: str) -> str:
        return new_id(prefix)


class MarketService:
    def __init__(self, repo, settings, *, versions: ResolvedVersionIdentity, http=None):
        self.repo = repo
        self.settings = settings
        self.versions = versions
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
        self._reads = {}
        self._hot_since = {}

        self._kline_day_counts = {}
        self._kline_day_counts_saved = ({}, 0.0)
        self._pending_market = {}
        self._market_flush_lock = asyncio.Lock()
        self._condition_heads_memo = {}
        self._stop = None
        self._last_request = {}
        self._request_locks = {}
        self._request_pace_locks = {}
        self.thresholds = settings.load_config("thresholds")
        self._capture = build_capture_lifecycle(
            repo, settings, frozenset(self.tokens), _MarketCaptureClock(), versions=versions
        )
        self.http = (
            httpx.AsyncClient(timeout=30, headers={"User-Agent": settings.user_agent})
            if http is None
            else http
        )
        self._owns_http = http is None

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
            dict(row, config_version=self.versions.config_version, code_version=self.versions.code_version)
        )

    async def _put(self, collection, key, row, *, immutable=False):
        if collection == "qa.reviews" and row.get("at") is not None:
            row = dict(row, **collection_clock(row["at"]).fields("at"))
        if collection in {"mkt.hot_tokens", "mkt.backfill_runs", "mkt.capture_requests"}:
            raise ValueError("Capture control records require the validated capture repository")
        if collection.startswith("mkt."):
            if collection in TANK_CAPTURE_TABLES:
                raise ValueError(collection + " is written by tank-capture only")
            row = self.provenance(jsonable(row))
            row.setdefault("recorded_at", clock_iso())
            row.update(collection_clock(row["recorded_at"]).fields("recorded_at"))
            self._pending_market.setdefault(collection, {})[str(key)] = row
            return row
        if collection == "qa.reviews" and await self.repo.get(collection, str(key)) is not None:
            key = str(key) + ":" + new_id("REV")
            row = dict(row, review_id=key)
        return await self.repo.put(collection, str(key), self.provenance(jsonable(row)), immutable=immutable)

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

    async def flush_market(self):
        """One multi-row write per typed collection this process queued (supply, tiers)."""
        async with self._market_flush_lock:
            pending, self._pending_market = self._pending_market, {}
            queue = list(pending.items())
            try:
                while queue:
                    collection, values = queue[0]
                    keys = list(values)[:FLUSH_ROWS]
                    await self.repo.put_market_batch(collection, [values[key] for key in keys], keys=keys)
                    for key in keys:
                        values.pop(key)
                    if not values:
                        queue.pop(0)
                    await asyncio.sleep(0)
            except Exception:
                for collection, values in queue:
                    target = self._pending_market.setdefault(collection, {})
                    for key, row in values.items():
                        target.setdefault(key, row)
                raise

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

    async def close(self):
        try:
            await self.flush_market()
            with suppress(Exception):
                await self._save_day_counts(force=True)
        finally:
            if self._owns_http:
                await self.http.aclose()

    async def _request(self, method, url, **kwargs):
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
        from tank.sources.rate_limits import shared_request_budgets

        priority = kwargs.pop("_priority", 0)
        host = httpx.URL(url).host
        if is_exchange_url(url):
            raise RuntimeError(EXCHANGE_REST_OWNER)
        interval = 0.35 if host == "api.etherscan.io" else 0.08
        lock = self._request_locks.setdefault(host, PriorityGate(HOST_IN_FLIGHT))
        for _attempt in range(4):
            async with lock.enter(priority):
                async with self._request_pace_locks.setdefault(host, asyncio.Lock()):
                    while True:
                        now = time.monotonic()
                        previous = self._last_request.get(host)
                        delay = 0 if previous is None else previous + interval - now
                        if delay <= 0:
                            self._last_request[host] = now
                            break
                        await asyncio.sleep(delay)
                try:
                    response = await self.http.request(method, url, **kwargs)
                except httpx.HTTPError as exc:
                    raise RuntimeError(f"{host}: {type(exc).__name__}") from None
                if response.status_code in (418, 429):
                    delay = float(response.headers.get("Retry-After", "60"))
                    self._last_request[host] = max(
                        self._last_request[host], time.monotonic() + min(max(delay, 1), 60)
                    )

                    banned = response.status_code == 418
                    await shared_request_budgets.block(
                        host,
                        utcnow() + timedelta(seconds=max(delay, 1) if banned else min(max(delay, 1), 60)),
                        f"HTTP {response.status_code}",
                    )
                    if banned and shared_request_budgets.ledger is not None:
                        raise RuntimeError(f"{host}: HTTP 418 banned for {delay:.0f}s")
                    continue
                if response.is_error:
                    raise RuntimeError(f"{host}: HTTP {response.status_code} {response.text[:200]}")

                return json.loads(response.content, parse_float=Decimal)
        raise RuntimeError(f"{host}: rate limited after bounded retries")

    async def repair_windows(
        self,
        token_id: str,
        start: TimeInput,
        end: TimeInput,
        *,
        budget_s: Decimal | float | int | None = None,
    ) -> RepairOutcome:
        budget = None if budget_s is None else Decimal(str(budget_s))
        return await self._capture.repair_windows(
            token_id,
            chosen_boundary(parse_time(start), precision="EXACT"),
            chosen_boundary(parse_time(end), precision="EXACT"),
            budget_s=budget,
        )

    async def escalate(self, event_id: str, tokens: Sequence[str | TokenEscalation]) -> None:
        requests = [TokenEscalation(token_id=value) if isinstance(value, str) else value for value in tokens]
        await self._capture.escalate(event_id, requests)
        if not self.settings.replay:
            await self._sync_hot_set()

    async def wait_capture(
        self, event_id: str, token_id: str, *, timeout_s: float = 30
    ) -> CaptureWaitOutcome:
        return await self._capture.wait_capture(event_id, token_id, timeout_s=timeout_s)

    async def _schedule_event_backfill(self, event_id: str, token_id: str, anchor: Timestamp) -> BackfillRun:
        return await self._capture.schedule_backfill(event_id, token_id, anchor)

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
                continue
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
                end=clock_iso() if end is None else end,
                market_end=max_cutoff - timedelta(seconds=offset),
                batch_size=1000,
            ):
                checked += 1
                if checked % 200 == 0:
                    await asyncio.sleep(0)
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

    async def capture_readiness(
        self, event_id: str, token_id: str, *, anchor: Timestamp | None = None
    ) -> CaptureReadiness:
        return await self._capture.readiness(event_id, token_id, anchor=anchor)

    async def hot_protected(self, event_id: str, token_id: str, now: TimeInput | None = None) -> bool:
        observed = utcnow() if now is None else parse_time(now)
        return await self._capture.hot_protected(event_id, token_id, observed)

    async def refresh_hot_expiry(self, now: TimeInput | None = None) -> list[HotToken]:
        observed = utcnow() if now is None else parse_time(now)
        return await self._capture.refresh_hot_expiry(observed)

    async def demote(self, token_id: str, event_id: str | None = None) -> None:
        await self._capture.demote(token_id, event_id)
        await self._sync_hot_set()

    async def _record_capture_tier(self, token_id: str, tier: CaptureTier, reason: str) -> None:
        await self._capture.record_tier(token_id, tier, reason)

    async def _sync_hot_set(self) -> None:
        self._hot_since = await self._capture.sync_hot_set()

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
        from tank.market.supply import supply_cadences

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
        stream = await self._stream_state(token_id)
        for iid, instrument in self.instruments.items():
            if instrument["token_id"] != token_id or norm.venue_key(instrument).endswith(".spot"):
                continue
            key = norm.venue_key(instrument)
            if key == "hyperliquid.perp":
                states[iid] = "NOT_SUPPORTED"
                continue
            states[iid] = "CONNECTED" if iid in stream.connected else "DISCONNECTED"
            observed = stream.watermarks.get(iid)
            since = stream.connected.get(iid)
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
                    "observed_through": {
                        iid: max(at for at in polls if parse_time(at) <= end)
                        for iid, polls in stream.polls.items()
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

    async def _stream_state(self, token_id=None):
        """Connection, exchange-time watermark, klines polls and last print by instrument from
        the mkt.capture_streams rows tank-capture rewrites every few seconds (a stale one is a
        stopped capture: not connected)."""
        state = SimpleNamespace(connected={}, watermarks={}, polls={}, last_trade={})
        fresh = utcnow() - timedelta(seconds=STREAM_STATE_STALE_S)
        filters = {"token_id": token_id} if token_id is not None else None
        for row in await self.repo.list("mkt.capture_streams", filters=filters, limit=10000):
            iid = row["instrument_id"]
            if iid not in self.instruments:
                continue
            if row.get("stream") == "WS":
                if row.get("last_trade_ts"):
                    state.last_trade[iid] = row["last_trade_ts"]
                if (
                    row.get("state") == "CONNECTED"
                    and row.get("connected_since")
                    and row.get("updated_at")
                    and parse_time(row["updated_at"]) >= fresh
                ):
                    state.connected[iid] = parse_time(row["connected_since"])
                    if row.get("watermark_ts"):
                        state.watermarks[iid] = {"ts": row["watermark_ts"]}
            elif row.get("stream") == "KLINES_POLL" and row.get("recent_polls"):
                state.polls[iid] = list(row["recent_polls"])
        return state

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
            else await self._condition_heads(
                "mkt.klines_1m", "open_time", start, upper_open, assembled_at, {"instrument_id": iid}
            )
        )
        cache = self.__dict__.setdefault("_condition_series", {})
        locks = self.__dict__.setdefault("_condition_series_locks", {})

        key = (iid, kind, rolling) if rolling else (iid, kind, start, cutoff) if fixed_window else (iid, kind)

        margin = SERIES_MARGIN if rolling or not fixed_window else timedelta(0)
        async with locks.setdefault(key, asyncio.Lock()):
            previous = cache.get(key)
            reset = (
                previous is None
                or start < previous["start"]
                or previous["unknown_availability"]
                or not any(heads)
            )
            behind = bool(
                not reset
                and assembled_at is not None
                and previous["assembled_at"] is not None
                and parse_time(assembled_at) < parse_time(previous["assembled_at"])
            )
            hidden = set()
            if behind:
                reset = upper_open > previous["upper_open"]
                if not reset:
                    hidden = await self._klines_recorded_between(
                        iid, assembled_at, previous["assembled_at"], upper_open
                    )
            if not reset and (heads, start, cutoff) == previous["served"]["request"]:
                previous["used"] = time.monotonic()
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
                page = []
                async for row in self.repo.iter_market_projection(
                    "mkt.klines_1m",
                    fields,
                    "open_time",
                    start=state_start,
                    end=upper_open + timedelta(microseconds=1),
                    instrument_ids=[iid],
                    assembled_at=assembled_at,
                    batch_size=200000,
                ):
                    page.append(row)
                    if len(page) == SERIES_APPLY_ROWS:
                        rows, page = page, []
                        await asyncio.to_thread(lambda rows=rows: [apply(row) for row in rows])
                await asyncio.to_thread(lambda: [apply(row) for row in page])
            elif not behind:
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
                drop = bisect_left(times, microseconds(start - margin))
                state_start = start - margin
                times, values = times[drop:], values[drop:]
                complete, precise = complete[drop:], precise[drop:]

            def state_digest():
                if reset:
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
                if hidden:
                    gone = {microseconds(at) for at in hidden}
                    keep = [i for i, stamp in enumerate(opens) if stamp not in gone]
                    opens = array("q", (opens[i] for i in keep))
                    points = array("d") if kind == "price" else []
                    points.extend(values[low + i] for i in keep)
                    valid = bytearray(valid[i] for i in keep)
                    known = bytearray(known[i] for i in keep)
                elif served:
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
            fingerprint = sha256([state_fingerprint, iso(start), iso(upper_open), sorted(hidden)])
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
                "assembled_at": previous["assembled_at"] if behind and not reset else assembled_at,
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

            if captured_rows is None and not (behind and not reset):
                state["used"] = time.monotonic()
                cache.pop(key, None)
                cache[key] = state
                while len(cache) > 256:
                    cache.pop(next(iter(cache)))
                for stale in [
                    k for k, held in cache.items() if state["used"] - held.get("used", 0) > SERIES_TTL_S
                ]:
                    cache.pop(stale, None)
            return series

    async def _klines_recorded_between(self, iid, after, upto, market_end):
        """Open times of the instrument's klines opened by `market_end` and recorded in (after, upto]."""
        found = set()
        async for row in self.repo.iter_late_arrivals(
            "mkt.klines_1m",
            "recorded_at",
            "open_time",
            since=parse_time(after) + timedelta(microseconds=1),
            end=parse_time(upto) + timedelta(microseconds=1),
            market_end=market_end,
            filters={"instrument_id": iid},
        ):
            found.add(row["open_time"])
        return found

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
                self.versions.config_version,
                self.versions.code_version,
            ]
        )

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
            domains = {"composite"}
        instruments = [i for i in self.instruments.values() if i["token_id"] == token_id]
        spot = [i for i in instruments if i["market_type"] == "SPOT"]
        perp = [i for i in instruments if i["market_type"] != "SPOT"]
        result = {
            "token_id": token_id,
            "anchor": anchor,
            "instruments": instruments,
            "config_version": self.versions.config_version,
            "code_version": self.versions.code_version,
        }
        result.update(requested.fields("cutoff"))
        result.update(requested.fields("requested_cutoff"))
        result.update(collection_clock(utcnow()).fields("assembled_at"))
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
                    self.versions.config_version,
                    self.versions.code_version,
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
                    self.versions.config_version,
                    self.versions.code_version,
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
            "saved_at": clock_iso(),
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
        except Exception as exc:
            log.warning("series count restore failed: %s", type(exc).__name__)
        spot = [iid for iid, inst in self.instruments.items() if norm.venue_key(inst).endswith(".spot")]
        end = utcnow()
        for offset in range(0, len(spot), 4):
            if self._stop is not None and self._stop.is_set():
                return
            try:
                await self._series_counts(
                    spot[offset : offset + 4], end - timedelta(days=90), end, clock_iso()
                )
            except Exception as exc:
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
        if collection not in WINDOW_CACHED:
            return await self._ranged_read(collection, field, ids, start, end, filters, books=books)
        return await self._ranged_cached(collection, field, ids, start, end, filters, books=books)

    async def _ranged_read(
        self, collection, field, ids, start, end, filters=None, *, books=False, keys_out=None
    ):
        if books:
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
                keys_out=keys_out,
            )

        page = WIDE_PAGE_ROWS if collection == "mkt.tickers_24h" else 1000
        return await market_range_by_instrument(
            self.repo, collection, field, ids, start, end, filters, batch_size=page, keys_out=keys_out
        )

    async def _ranged_cached(self, collection, field, ids, start, end, filters, *, books):
        """A window read that equals a fresh `_ranged_read` but, once the window was read,
        fetches only rows past its end and rows recorded since that read: a warm BTC
        snapshot re-read two days of five collections (~100k rows, ~10 s) every minute."""
        start, end = parse_time(start), parse_time(end)
        key = (collection, field, tuple(sorted(set(ids))), tuple(sorted((filters or {}).items())), books)
        windows = self.__dict__.setdefault("_windows", {})
        locks = self.__dict__.setdefault("_window_locks", {})
        async with locks.setdefault(key, asyncio.Lock()):
            lanes = windows.pop(key, [])
            entry = next(
                (
                    lane
                    for lane in lanes
                    if lane["start"] <= start <= lane["end"] and not (books and end < lane["end"])
                ),
                None,
            )
            if entry is not None:
                lanes.remove(entry)
            clock = utcnow()
            if entry is None:
                keys = {}
                out = await self._ranged_read(
                    collection, field, ids, start, end, filters, books=books, keys_out=keys
                )
                rows = {
                    iid: [
                        (parse_time(row[field]), k, row)
                        for k, row in zip(keys.get(iid, []), out[iid], strict=True)
                    ]
                    for iid in out
                }
                entry = {"start": start, "end": end, "clock": clock, "rows": rows, "starts": []}
            else:
                late = await market_rows_recorded_since(
                    self.repo,
                    collection,
                    field,
                    ids,
                    entry["clock"] - WINDOW_MARGIN,
                    start,
                    entry["end"],
                    filters,
                )
                fresh = {}
                if end > entry["end"]:
                    fresh = await self._ranged_read(
                        collection, field, ids, entry["end"], end, filters, books=books, keys_out=(keys := {})
                    )
                rows = entry["rows"]

                now = time.monotonic()
                entry["starts"] = [(at, s) for at, s in entry["starts"] if now - at < WINDOW_KEEP_S]
                entry["starts"].append((now, start))
                floor = min(s for _, s in entry["starts"])
                for iid in rows:
                    held = rows[iid]
                    first = bisect_left(held, floor, key=lambda point: point[0])
                    held = rows[iid] = held[first:]
                    changed = [(k, row) for i, row, k in late if i == iid]
                    if changed:
                        indexed = {point[1]: point for point in held}
                        for k, row in changed:
                            if k in indexed:
                                old = indexed[k][2]
                                if books and "bids" not in row and "bids" in old:
                                    row = dict(row, bids=old["bids"], asks=old.get("asks"))
                            indexed[k] = (parse_time(row[field]), k, row)
                        held[:] = sorted(indexed.values(), key=lambda point: (point[0], point[1]))
                    if fresh.get(iid):
                        held.extend(
                            (parse_time(row[field]), k, row)
                            for k, row in zip(keys[iid], fresh[iid], strict=True)
                        )
                entry.update(start=floor, end=max(end, entry["end"]), clock=clock)
                if books:
                    for held in rows.values():
                        _place_levels(held)
            entry["used"] = time.monotonic()
            entry["size"] = sum(len(held) for held in entry["rows"].values())
            windows[key] = [entry, *lanes][:WINDOW_LANES]
            _bound_windows(windows)
            return {
                iid: [
                    point[2]
                    for point in held[bisect_left(held, start, key=lambda point: point[0]) :]
                    if point[0] < end
                ]
                for iid, held in entry["rows"].items()
            }

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
            "config_version": self.versions.config_version,
            "code_version": self.versions.code_version,
        }
        instruments = [r for r in self.instruments.values() if r["token_id"] == token_id]
        ids = {r["instrument_id"] for r in instruments}
        result["flow_segments"] = []
        result["reference_points"] = []
        for name, (collection, field) in COLLECTIONS.items():
            values = []

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
        assembled = shared.setdefault("assembled", clock_iso()) if shared is not None else clock_iso()

        rolling = "LIVE_SNAPSHOT" if cutoff_dt >= utcnow() - timedelta(hours=1) else None
        captured_klines = None
        if recent_start == history_start:
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
        latest = (
            max(
                runs,
                key=lambda r: parse_time(r["recorded_at"] if r["started_at"] is None else r["started_at"]),
            )
            if runs
            else None
        )
        result["capture_status"] = {
            "status": latest["status"] if latest else "NOT_REQUESTED",
            "reason": latest.get("reason") if latest else "NO_DURABLE_CAPTURE_REQUEST",
            "backfill_id": latest.get("backfill_id") if latest else None,
            "observed_at": clock_iso(),
            "bounded_wait_s": 0,
            "incomplete_windows_excluded": True,
        }
        if latest and latest["backfill_id"] in self._capture.waits:
            waited = self._capture.waits[latest["backfill_id"]]
            result["capture_status"]["bounded_wait_s"] = waited.bounded_wait_s
            if latest["status"] in ("PENDING", "RUNNING"):
                result["capture_status"].update(waited.to_wire())
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
        result.update(collection_clock(utcnow()).fields("assembled_at"))
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
        stream = await self._stream_state()
        result = []
        for iid, inst in self.instruments.items():
            gaps = await self.repo.list("mkt.capture_gaps", filters={"instrument_id": iid}, limit=10000)
            result.append(
                {
                    "instrument_id": iid,
                    "token_id": inst["token_id"],
                    "venue": inst["venue"],
                    "market_type": inst["market_type"],
                    "tier": "HOT" if inst["token_id"] in hot else "BASE",
                    "stream_connected": iid in stream.connected,
                    "last_trade_ts": stream.last_trade.get(iid),
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
        temp.write_text(json.dumps({"assembled_at": clock_iso(), "matrix": matrix}, indent=2))
        temp.replace(path)
        return result

    async def _supply_loop(self):
        from tank.market.supply import SUPPLY_LOOP_SLEEP_S, SupplyCapture

        capture = SupplyCapture(self)
        while not self._stop.is_set():
            hot = set(self._hot_since)
            try:
                await capture.poll(hot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("supply capture failed: %s", type(exc).__name__)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), SUPPLY_LOOP_SLEEP_S)

    async def _record_started_tiers(self) -> None:
        await self._capture.record_started_tiers()

    async def _schedule_hot_backfills(self) -> None:
        await self._capture.schedule_hot_backfills()

    async def run(self, stop):
        """tank-capture holds the streams, pollers, backfills and exchange REST; this process
        keeps the hot set's tiers, expiry and capture obligations, supply capture and reads."""
        self._stop = stop
        if self.settings.replay:
            return
        workers = []
        try:
            await self._record_started_tiers()
            await self._sync_hot_set()
            await self._schedule_hot_backfills()
            workers = [
                asyncio.create_task(self._warm_series_counts()),
                asyncio.create_task(self._supply_loop()),
                asyncio.create_task(self._loop_watchdog()),
            ]
            checked = utcnow()
            while not stop.is_set():
                await self.flush_market()
                await self._sync_hot_set()
                if (utcnow() - checked).total_seconds() >= DAY_COUNTS_SAVE_S:
                    checked = await self._evict_recorded_days(checked)
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), HOT_SET_SYNC_S)
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def _evict_recorded_days(self, since):
        """Past-day kline counts of days tank-capture wrote into since `since`; the counts
        are saved after."""
        now = utcnow()
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for key in await market_kline_days_recorded_since(self.repo, since - DAY_COUNTS_MARGIN, today):
            self._kline_day_counts.pop(key, None)
        await self._save_day_counts()
        return now
