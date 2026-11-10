import asyncio
import hashlib
import json
import logging
import time
from contextlib import suppress
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import httpx

from tank.market import normalize as norm
from tank.market.capture import event_anchor
from tank.market.rate_gate import PriorityGate
from tank.market_storage import (
    market_kline_days_recorded_since,
    market_series_day_counts,
)
from tank.sources.capture_fetch import is_exchange_url
from tank.time_precision import Timestamp, chosen_boundary, collection_clock, propagate_timestamp
from tank.util import iso, jsonable, new_id, parse_time, sha256, utcnow

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


DAY_COUNTS_SAVE_S = 60
DAY_COUNTS_MARGIN = timedelta(minutes=30)

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
        self._reads = {}
        self._hot_since = {}

        self._kline_day_counts = {}
        self._kline_day_counts_saved = ({}, 0.0)
        self._pending_market = {}
        self._market_flush_lock = asyncio.Lock()
        self._stop = None
        self._last_request = {}
        self._request_locks = {}
        self._request_pace_locks = {}
        self._backfills = set()
        self._capture_waits = {}
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
                "foundry_api_key",
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
        if collection.startswith("mkt."):
            if collection in TANK_CAPTURE_TABLES:
                raise ValueError(collection + " is written by tank-capture only")
            row = self.provenance(jsonable(row))
            row.setdefault("recorded_at", iso())
            row.update(collection_clock(row["recorded_at"]).fields("recorded_at"))
            if collection in ("mkt.hot_tokens", "mkt.backfill_runs", "mkt.capture_requests"):
                changed = await self.repo.put_market_batch(collection, [row], keys=[str(key)])
                return changed[0] if changed else row
            self._pending_market.setdefault(collection, {})[str(key)] = row
            return row
        if collection == "qa.reviews" and await self.repo.get(collection, str(key)) is not None:
            key = str(key) + ":" + new_id("REV")
            row = dict(row, review_id=key)
        return await self.repo.put(collection, str(key), self.provenance(jsonable(row)), immutable=immutable)

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
        admit_retry = kwargs.pop("_admit_retry", None)
        host = httpx.URL(url).host
        if is_exchange_url(url):
            raise RuntimeError(EXCHANGE_REST_OWNER)
        interval = 0.35 if host == "api.etherscan.io" else 0.08
        lock = self._request_locks.setdefault(host, PriorityGate(HOST_IN_FLIGHT))
        for attempt in range(4):
            if attempt and admit_retry is not None:
                await admit_retry()
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

    async def repair_windows(self, token_id, start, end, *, budget_s=None):
        """Ask tank-capture to repair every incomplete or missing persisted minute of
        [start, end) for a token's instruments within budget_s (03 §5.1 fill order, §5.2
        step 2). Returns {instrument_id: {"incomplete_before", "incomplete_after"}}."""
        if getattr(self.settings, "replay", False):
            return {}
        return await self._request_repair(token_id, start, end, budget_s)

    async def _request_repair(self, token_id, start, end, budget_s):
        """repair_windows run by tank-capture: a mkt.capture_requests row it claims and
        finishes within the budget. Its result, or {} when none arrived in time."""
        requested = utcnow()
        budget = float(budget_s) if budget_s else CAPTURE_REQUEST_DEFAULT_S
        lo, hi = parse_time(start).replace(second=0, microsecond=0), parse_time(end)
        request_id = "CR-" + sha256(
            {"token_id": token_id, "start": iso(lo), "end": iso(hi), "requested_at": iso(requested)}
        )
        pending = Timestamp(None, None, "PENDING")
        await self._put(
            "mkt.capture_requests",
            request_id,
            {
                "request_id": request_id,
                "kind": "REPAIR_WINDOWS",
                "token_id": token_id,
                **chosen_boundary(lo, precision="EXACT").fields("start"),
                **chosen_boundary(hi, precision="EXACT").fields("end"),
                "budget_s": budget,
                **collection_clock(requested).fields("requested_at"),
                **collection_clock(requested + timedelta(seconds=budget)).fields("deadline"),
                **pending.fields("started_at"),
                **pending.fields("finished_at"),
                "status": "PENDING",
                "reason": None,
                "result": None,
            },
        )
        until = time.monotonic() + budget + CAPTURE_REQUEST_GRACE_S
        while True:
            row = await self.repo.get("mkt.capture_requests", request_id) or {}
            if row.get("status") in CAPTURE_REQUEST_TERMINAL or time.monotonic() >= until:
                break
            await asyncio.sleep(0.25)
        if row.get("status") in ("COMPLETE", "PARTIAL"):
            return (row.get("result") or {}).get("instruments") or {}
        log.warning(
            "window repair %s for %s: %s", request_id, token_id, row.get("reason") or row.get("status")
        )
        return {}

    async def escalate(self, event_id, tokens, anchor=None):
        """Make `tokens` hot for the event and request the pre-event capture at `anchor`.

        `anchor` is the Market Truth anchor chosen at the requested cutoff; without it the
        anchor is chosen now. Before a scheduled T0 that is T1 or known_to_us_at, so the
        capture obligation never covers future minutes; the hot expiry still runs from
        the event's own anchor, keeping the token hot through the scheduled time.
        """
        async with self.repo.lock("market-hot-set"), self.repo.transaction():
            now = iso()
            event = await self.repo.get("evt.events", event_id) or {}
            if anchor is None:
                anchor = event_anchor(event, at=now)["ts"] if event else now
            expiry_anchor = event_anchor(event)["ts"] if event else anchor
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
                        "expires_at": None
                        if protected
                        else iso(parse_time(expiry_anchor) + timedelta(hours=72)),
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
        await self._sync_hot_set()
        for token_id in retained:
            if token_id in [v.get("token_id") if isinstance(v, dict) else v for v in tokens]:
                await self._schedule_event_backfill(event_id, token_id, anchor)

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

        if old and old.get("status") in ("COMPLETE", "PARTIAL", "PENDING", "RUNNING"):
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
        selected = event_anchor(event, at=parse_time(anchor) + timedelta(microseconds=1)) if event else None
        precision = selected.get("ts_prec") if selected and selected["ts"] == iso(anchor) else None
        row.update(norm.timestamp_fields("anchor", anchor, precision))
        await self._put("mkt.backfill_runs", backfill_id, row)
        self._backfills.add(marker)

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
                end=end or iso(),
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

    async def capture_readiness(self, event_id, token_id, *, anchor=None):
        if anchor is not None:
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
        await self._sync_hot_set()

    def _ticker_cadence(self, hot):
        """The 24h-ticker poll interval tank-capture runs for a tier (its hot grid or base loop)."""
        if hot:
            return float(self.thresholds.get("hot_ticker_interval_s", 20))
        return float(self.thresholds.get("base_ticker_interval_s", 300))

    async def _record_capture_tier(self, token_id, tier, reason):
        from tank.market.supply import supply_cadences

        cadences = {
            "oi_funding_s": float(
                self.thresholds.get(
                    "hot_oi_funding_interval_s" if tier == "HOT" else "base_oi_funding_interval_s",
                    60 if tier == "HOT" else 300,
                )
            ),
            "kline_s": float(self.thresholds.get("base_kline_interval_s", 60)),
            "book_s": float(self.thresholds.get("book_interval_s", 10)),
            "ticker_s": self._ticker_cadence(tier == "HOT"),
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

    async def _sync_hot_set(self):
        """Hot expiry and the hot set this process scales supply by; tank-capture subscribes
        the same set from mkt.hot_tokens."""
        if time.monotonic() - self._last_hot_expiry_refresh >= 60:
            await self.refresh_hot_expiry()
            self._last_hot_expiry_refresh = time.monotonic()
        self._hot_since = {}
        for r in await self.repo.list("mkt.hot_tokens", limit=10000):
            if r["token_id"] not in self._hot_since or r["since"] < self._hot_since[r["token_id"]]:
                self._hot_since[r["token_id"]] = r["since"]

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
                "tickers": {"cadence_s": profile.get("ticker_s", self._ticker_cadence(is_hot))},
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

    def _peer_instruments(self, token_id):
        """The spot instruments of the benchmarks (BTC and ETH) and of the tokens of the same sector (the DeFi
        sectors when it has fewer than two) that the price-relative rules choose their reference prices from."""
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
        return [
            inst
            for inst in self.instruments.values()
            if norm.venue_key(inst).endswith(".spot") and inst["token_id"] in peers
        ]

    async def _registry_inputs(self, token_id, cutoff_dt, assembled):
        """What every Market Truth path reads of the registry and of the capture state: the instruments of the
        token and of its reference peers, their metric semantics, the tokens and sector baskets, the complete
        one-minute closes per spot instrument in the 90 days (the reference instrument of a token is the one with
        the most), and the capture metadata. No series is read."""
        instruments = [r for r in self.instruments.values() if r["token_id"] == token_id]
        spot = [
            inst
            for inst in self.instruments.values()
            if norm.venue_key(inst).endswith(".spot") and inst["token_id"] == token_id
        ]
        peers = self._peer_instruments(token_id)
        ids = [inst["instrument_id"] for inst in [*spot, *peers]]
        counts = (
            await self._series_counts(ids, cutoff_dt - timedelta(days=90), cutoff_dt, assembled)
            if ids
            else {}
        )
        instruments = [*instruments, *peers]
        return dict(
            instruments=instruments,
            semantics=self._instrument_semantics(instruments),
            tokens=list(self.tokens.values()),
            sector_baskets=deepcopy(self.sector_baskets),
            series_coverage_counts=counts,
            capture_metadata=await self._capture_metadata(token_id, cutoff_dt),
        )

    async def condition_snapshot(self, token_id, cutoff, *, anchor=None, requirements):
        """The registry and capture inputs of a condition evaluation at `cutoff`. The series themselves are read by
        the statements of Market Truth (03 section 5.9), not here."""
        requested = cutoff if isinstance(cutoff, Timestamp) else chosen_boundary(cutoff, precision="EXACT")
        if requested.value is None:
            raise ValueError("Condition snapshot requires a cutoff value")
        end = parse_time(requested.value)
        if isinstance(anchor, dict):
            anchor = {
                **anchor,
                **propagate_timestamp(anchor, "ts", precision_aliases=("precision",)).fields("ts"),
            }
        elif anchor is not None:
            anchor = {"type": "T0", **norm.timestamp_fields("ts", anchor, None)}
        result = {
            "token_id": token_id,
            "anchor": anchor,
            "config_version": self.settings.config_version,
            "code_version": self.settings.code_version,
        }
        result.update(requested.fields("cutoff"))
        result.update(requested.fields("requested_cutoff"))
        result.update(collection_clock(iso()).fields("assembled_at"))
        result.update(await self._registry_inputs(token_id, end, result["assembled_at"]))
        metrics = {r["metric"] for r in requirements}
        if metrics & {"dc.class", "mt.move_class", "mt.divergence_state"}:
            result["supply"] = await self._latest_supply(token_id, end, result["assembled_at"])
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
        except Exception as exc:
            log.warning("series count restore failed: %s", type(exc).__name__)
        spot = [iid for iid, inst in self.instruments.items() if norm.venue_key(inst).endswith(".spot")]
        end = utcnow()
        for offset in range(0, len(spot), 4):
            if self._stop is not None and self._stop.is_set():
                return
            try:
                await self._series_counts(spot[offset : offset + 4], end - timedelta(days=90), end, iso())
            except Exception as exc:
                log.warning("series count warm-up failed: %s", type(exc).__name__)
                return
            await asyncio.sleep(1)

    async def _latest_supply(self, token_id, cutoff, assembled=None):
        """The latest observation of each supply kind and chain of a token at or before `cutoff`, and the latest
        available one: what the supply rules read of the rows themselves. The numbers derived from the series are
        the supply statements' (03 section 5.6)."""
        rows = await self.repo.latest_market_records(
            "mkt.supply_obs",
            group_by=("token_id", "kind", "chain"),
            time_field="ts",
            filters={"token_id": token_id},
            cutoff=cutoff,
            assembled_at=assembled,
        )
        rows.extend(
            await self.repo.latest_market_records(
                "mkt.supply_obs",
                group_by=("token_id", "kind", "chain"),
                time_field="ts",
                filters={"token_id": token_id, "available": True},
                cutoff=cutoff,
                assembled_at=assembled,
            )
        )
        return list({sha256(row): row for row in rows}.values())

    async def snapshot(self, token_id, cutoff, anchor=None):
        """The registry and capture inputs of one Market Truth assessment at `cutoff` (03 section 5): the
        instruments, the registry facts, the capture state of the token, the backfill runs and the latest supply
        observations. The series are not read here; the statements of Market Truth read them (03 section 11.1)."""

        requested = cutoff if isinstance(cutoff, Timestamp) else chosen_boundary(cutoff, precision="EXACT")
        if requested.value is None:
            raise ValueError("Snapshot requires a cutoff value")
        cutoff = requested.value
        cutoff_dt = parse_time(cutoff)
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
        assembled = iso()
        result.update(await self._registry_inputs(token_id, cutoff_dt, assembled))
        result["supply"] = await self._latest_supply(token_id, cutoff_dt)
        runs = await self.repo.list("mkt.backfill_runs", filters={"token_id": token_id}, limit=10000)
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
        result["capture_tier"] = result["capture_metadata"]["tier"]
        result.update(collection_clock(iso()).fields("assembled_at"))
        return result

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
        temp.write_text(json.dumps({"assembled_at": iso(), "matrix": matrix}, indent=2))
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

    async def _record_started_tiers(self):
        async with self.repo.lock("market-hot-set"), self.repo.transaction():
            active_tokens = {row["token_id"] for row in await self.repo.list("mkt.hot_tokens", limit=10000)}
            for token_id in self.tokens:
                await self._record_capture_tier(
                    token_id, "HOT" if token_id in active_tokens else "BASE", "CAPTURE_POLICY_STARTED"
                )

    async def _schedule_hot_backfills(self):
        for hot in await self.repo.list("mkt.hot_tokens", limit=10000):
            event = await self.repo.get("evt.events", hot["event_id"]) or {}
            await self._schedule_event_backfill(
                hot["event_id"],
                hot["token_id"],
                event_anchor(event, at=iso())["ts"] if event else hot["since"],
            )

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
