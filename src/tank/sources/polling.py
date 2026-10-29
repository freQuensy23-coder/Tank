"""Feed acquisition and durable detail backlog inside SourceService's source loops.

Network acquisition never waits for article enrichment. Completed coverage moves
in acquisition order; failed originals stay in the checkpoint's pending batches.
"""

import asyncio
import json
import logging
import re
from copy import deepcopy
from dataclasses import asdict
from datetime import timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from tank.ops_entry_errors import report_entry_errors
from tank.sources import parsers
from tank.util import new_id, parse_time, sha256

log = logging.getLogger(__name__)
ARTICLE_KINDS = {"html", "rendered", "rendered_html", "html_diff"}


RECENT_READS = 50


def count_read(state, name, *, ok, n=1):
    """Cumulative per-feed read counters for the V0-A failed-read rate (01 §10)."""
    counters = state.setdefault(name, {"ok": 0, "failed": 0})
    counters["ok" if ok else "failed"] += n


def remember_reads(state, outcomes):
    """Keep the feed's last ``RECENT_READS`` reads as ``[finished_at, ok]`` pairs.

    Feed polls and article/detail fetches share ``recent_reads``, the window of the
    heartbeat's rolling failed-read rate (03 §7.6). A poll and a detail pass of the same
    feed can overlap, so the window is ordered by when each read finished."""
    if outcomes:
        reads = [*state.get("recent_reads", []), *([at, ok] for at, ok in outcomes)]
        state["recent_reads"] = sorted(reads, key=lambda read: parse_time(read[0]))[-RECENT_READS:]


async def recapture(repo, source, external_ids, *, apply=False):
    """Release entries a feed checkpoint skips as duplicates so the next poll ingests them anew.

    Lists every feed checkpoint of the source whose duplicate_ids hold one of external_ids.
    With apply, removes those ids and the feed's conditional-request validators, so the next
    poll reads the full feed and judges the entries by the current deduplication rule (03 §3.1).
    """
    wanted = {str(external_id) for external_id in external_ids}
    rows = []
    for index in range(len(source["feeds"])):
        key = f"{source['source_id']}:{index}"
        state = await repo.get("ops.source_checkpoints", key)
        listed = [x for x in (state or {}).get("duplicate_ids", []) if x in wanted]
        if not listed:
            continue
        rows.append({"checkpoint": key, "external_ids": listed, "released": apply})
        if apply:
            state["duplicate_ids"] = [x for x in state["duplicate_ids"] if x not in wanted]
            state.pop("etag", None)
            state.pop("last_modified", None)
            await repo.put("ops.source_checkpoints", key, state)
    return {"source_id": source["source_id"], "apply": apply, "checkpoints": rows}


def instrument_changes(snapshot, previous, feed, pages, found_on, completed_at):
    """S14 diff against the previous snapshot and the snapshot digest (CPU-bound, runs in a thread).

    Symbols that made the same move in one poll of a venue list form one item: one status
    transition (a venue-wide delisting reaches routing and the analyst once, not once per pair)
    or one newly listed base asset. Independent moves stay separate items.
    """
    if previous is not None and str(feed.get("venue", "")).lower() == "coinbase":
        previous = deepcopy(previous)
        for symbol, old in previous.items():
            if symbol in snapshot and "status_flags" not in old:
                old["status_flags"] = snapshot[symbol].get("status_flags", {})
    digest = sha256(snapshot)
    if previous is None:
        return [], digest
    changed = [
        (symbol, snapshot.get(symbol), previous.get(symbol))
        for symbol in sorted(set(snapshot) | set(previous))
        if snapshot.get(symbol) != previous.get(symbol)
    ]
    if not changed:
        return [], digest

    def kind(current, old):
        return "REMOVED" if current is None else "ADDED" if old is None else "STATE_UPDATE"

    def status(entry):
        return (entry or {}).get("status")

    def action(change):
        _, current, old = change
        if old is None:
            return ("ADDED", (current or {}).get("base"))
        return (kind(current, old), status(old), status(current))

    venue, market_type = feed.get("venue"), feed.get("market_type")
    groups = {}
    for change in changed:
        groups.setdefault(action(change), []).append(change)
    rows = []
    for key, group in groups.items():
        on_pages = {found_on[symbol] for symbol, _, _ in group if symbol in found_on}
        if len(on_pages) == 1 and all(symbol in found_on for symbol, _, _ in group):
            payload, observed_at = pages[on_pages.pop()]
        else:
            payload, observed_at = b"\n".join(p for p, _ in pages), completed_at
        row = {
            "url": feed["url"],
            "published_at": None,
            "observed_at": observed_at,
            "_payload": payload,
            "change": key[0],
        }
        if len(group) == 1:
            symbol, current, old = group[0]
            rows.append(
                {
                    **row,
                    "external_id": f"{venue}|{market_type}|{symbol}|{sha256(current)}",
                    "title": f"{symbol} instrument change",
                    "text": json.dumps({"before": old, "after": current}, sort_keys=True),
                }
            )
            continue
        what = (
            f"{key[1]} instruments ADDED"
            if key[0] == "ADDED"
            else f"instruments {key[0]} {key[1]} → {key[2]}"
        )
        rows.append(
            {
                **row,
                "external_id": f"{venue}|{market_type}|changes|{sha256(group)}",
                "title": f"{venue} {market_type}: {len(group)} {what}",
                "text": json.dumps(
                    {
                        "changes": [
                            {"symbol": symbol, "change": key[0], "before": old, "after": current}
                            for symbol, current, old in group
                        ]
                    },
                    sort_keys=True,
                ),
            }
        )
    return rows, digest


class SourcePoller:
    def __init__(self, service):
        self.service = service
        self.acquisition_locks = {}
        self.state_locks = {}
        self.processing_locks = {}

        self.details = asyncio.Semaphore(4)
        self.active_acquisitions = {}
        self.active_details = {}

    def now(self):
        from tank import sources

        return sources.utcnow()

    def stamp(self, value=None):
        from tank import sources

        return sources.iso(self.now() if value is None else value)

    def lock(self, mapping, key):
        return mapping.setdefault(key, asyncio.Lock())

    def feed(self, source, index):
        return dict(source["feeds"][index], source_limits=source.get("limits", {}))

    def interval(self, source, feed, escalated):
        return (
            self.service.escalated_interval_s
            if escalated
            else feed.get("interval_s", source.get("interval_s", 60))
        )

    async def state(self, key):

        return await self.service.repo.get("ops.source_checkpoints", key) or {}

    async def save(self, key, state):
        from tank.time_precision import collection_clock

        def annotate(value):
            if isinstance(value, dict):
                for field in (
                    "observed_at",
                    "acquired_at",
                    "captured_at",
                    "fetched_at",
                    "last_attempt_at",
                    "last_success_at",
                    "last_failure_at",
                    "detail_retry_at",
                ):
                    if value.get(field):
                        value.update(collection_clock(value[field]).fields(field))
                for name, child in list(value.items()):
                    if name != "instruments" and isinstance(child, (list, dict)):
                        annotate(child)
            elif isinstance(value, list):
                for child in value:
                    annotate(child)

        source_match = re.search(r"(?:^|:)(S\d{2})(?=:|$)", key)
        declared_source = source_match.group(1) if source_match else state.get("source_id")
        if declared_source is None and len(self.service.sources) == 1:
            declared_source = next(iter(self.service.sources))
        if declared_source is None:
            raise ValueError("Checkpoint requires its declared source identity")
        if state.get("source_id") not in {None, declared_source}:
            raise ValueError("Checkpoint source identity conflicts with its feed key")
        state["source_id"] = declared_source
        state.update(collection_clock(self.stamp()).fields("updated_at"))
        annotate(state)
        for field in (
            "fetched_at",
            "last_attempt_at",
            "last_success_at",
            "last_failure_at",
            "detail_retry_at",
        ):
            if state.get(field):
                state[field + "_precision"] = "COLLECTION_ONLY"
        await self.service.repo.put("ops.source_checkpoints", key, state)

    @staticmethod
    def needs_detail(sid, kind):
        """The item's raw payload is its article/detail page, fetched in the backlog."""
        return kind in ARTICLE_KINDS or sid in {"S01", "S09"}

    async def queue_rows(self, current, key, source, feed, rows, kind, coverage, token, acquired_at):
        """Queue only items not yet captured; write raw now unless a detail page is still due."""
        svc, sid = self.service, source["source_id"]
        pending = {
            str(row["external_id"]) for batch in current.get("pending_batches", []) for row in batch["rows"]
        }
        fresh = []
        for original in rows:
            row = dict(original)
            payload = row.pop("_payload", None)
            external = str(row["external_id"])
            row.setdefault("feed", feed["url"])
            row.setdefault("observed_at", acquired_at)
            if (
                external in pending
                or external in current.get("duplicate_ids", [])
                or await svc.observe_known(sid, external, row["feed"])
            ):
                continue
            if sid == "S02" and not svc.filing_matches(row):
                continue
            row["item_id"] = new_id("item")
            row.setdefault("discovery_config_version", svc.settings.config_version)
            row.setdefault("discovery_code_version", svc.settings.code_version)
            if not self.needs_detail(sid, kind):
                row["raw_ref"], row["raw_sha256"] = svc.raw(
                    payload, source_id=sid, item_id=row["item_id"], observed_at=row["observed_at"]
                )
            pending.add(external)
            fresh.append(row)
        current.setdefault("pending_batches", []).append(
            {
                "batch_id": sha256(key + "|" + token),
                "acquired_at": acquired_at,
                "acquired_at_precision": "COLLECTION_ONLY",
                "kind": kind,
                "rows": fresh,
                "coverage": coverage,
            }
        )

    async def retain_page(self, key, source, feed, rows, page_state, *, kind="json"):

        async with self.lock(self.state_locks, key):
            current = await self.state(key)
            await self.queue_rows(
                current,
                key,
                source,
                feed,
                rows,
                kind,
                {},
                "page:" + page_state["payload_sha256"] + "|" + str(page_state.get("fetched_at")),
                page_state.get("fetched_at") or self.stamp(),
            )
            await self.save(key, current)

    async def mark_request_start(self, key):
        async with self.lock(self.state_locks, key):
            current = await self.state(key)
            current["last_attempt_at"] = self.stamp()
            await self.save(key, current)

    async def _fetch_rows(self, source, feed, key, state):
        svc = self.service
        sid = source["source_id"]
        kind = str(feed.get("kind", feed.get("format", source.get("mode", "rss")))).lower()
        if sid == "S29":
            accounts = [account["handle"].lstrip("@") for account in svc.accounts.values()]
            result = await svc.xsensor.posts(feed["url"], accounts, state.get("account_cursors", {}))
            state.update(
                fetched_at=self.stamp(),
                account_cursors=result.cursors,
                entry_errors=[asdict(failure) for failure in result.failures],
                history_gap=result.gaps,
                run_failed=result.failed,
            )
            return result.rows, state, kind
        payload, fetched = await svc.fetch(feed, key, on_start=lambda: self.mark_request_start(key))
        state.update(fetched)
        if payload is None:
            return None, state, kind
        state["entry_errors"] = []
        if sid == "S14":
            snapshot = await asyncio.to_thread(parsers.instruments, payload, feed)
            pages = [(payload, state.get("fetched_at") or self.stamp())]
            found_on = dict.fromkeys(snapshot, 0)
            if str(feed.get("venue", "")).lower() == "bybit":
                cursor = json.loads(payload).get("result", {}).get("nextPageCursor")
                cursors = set()
                while cursor:
                    if cursor in cursors:
                        raise ValueError("Repeated Bybit instrument cursor")
                    cursors.add(cursor)
                    parts = urlsplit(feed["url"])
                    params = dict(parse_qsl(parts.query), cursor=cursor)
                    next_feed = dict(
                        feed,
                        url=urlunsplit(
                            (parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment)
                        ),
                    )
                    page, page_state = await svc.fetch(next_feed, key + ":cursor:" + sha256(cursor))
                    if page is None:
                        raise ValueError("Unexpected conditional paginated response")
                    page_snapshot = await asyncio.to_thread(parsers.instruments, page, feed)
                    snapshot.update(page_snapshot)
                    found_on.update(dict.fromkeys(page_snapshot, len(pages)))
                    pages.append((page, page_state.get("fetched_at") or self.stamp()))
                    cursor = json.loads(page).get("result", {}).get("nextPageCursor")

            snapshot_key = key + ":instruments"
            stored = await self.state(snapshot_key)
            previous = stored.get("instruments")
            if previous is None:
                previous = state.get("instruments")
                for batch in state.get("pending_batches", []):
                    if "instruments" in batch["coverage"]:
                        previous = batch["coverage"]["instruments"]
            state["instrument_snapshot_schema_version"] = 2
            rows, digest = await asyncio.to_thread(
                instrument_changes, snapshot, previous, feed, pages, found_on, self.stamp()
            )
            if stored.get("sha256") != digest:
                await self.save(snapshot_key, {"sha256": digest, "instruments": snapshot})
        elif kind in {"rss", "atom", "feed"}:
            rows = await asyncio.to_thread(parsers.feeds, payload, sid, feed)
            state["entry_errors"] = [
                {**error, "payload_sha256": state["payload_sha256"]} for error in rows.errors
            ]
            for row in rows:
                row["_payload"] = payload
        elif kind in ARTICLE_KINDS:
            rows = await asyncio.to_thread(parsers.article_links, payload, feed)
        else:
            rows, state = await self._json_history(source, feed, key, payload, state)
        return rows, state, kind

    async def _json_history(self, source, feed, key, payload, state):
        """Read newest-first windows back to the last completed identity.

        Bootstrap deliberately establishes the configured head window. Later polls
        traverse available history without an invented page cap. Schema stalls and
        exhaustion before a formerly known boundary are explicit gaps.
        """
        svc, sid = self.service, source["source_id"]
        rows = []
        checkpoint = state.get("discovery_checkpoint")
        page_feed = deepcopy(feed)
        page = payload
        page_state = state
        page_fingerprints = set()
        entry_errors = []
        offset = 0
        found = False
        exhausted = False
        page_number = int(dict(parse_qsl(urlsplit(feed["url"]).query)).get("pageNo", 1))
        last_head_check = parse_time(state.get("fetched_at") or self.stamp())
        while True:
            parsed = await asyncio.to_thread(parsers.json_items, page, sid)
            entry_errors.extend(
                {**error, "payload_sha256": page_state["payload_sha256"]} for error in parsed.errors
            )

            def identity(row):
                return row.get("proposal_id") if sid == "S24" else row["external_id"]

            retained = []
            for row in parsed:
                row.setdefault("observed_at", page_state.get("fetched_at") or self.stamp())
                row["_payload"] = page
                if identity(row) == checkpoint:
                    found = True

                    rows.append(row)
                    retained.append(row)
                    break
                rows.append(row)
                retained.append(row)
            if sid in {"S03", "S09", "S24"}:
                await self.retain_page(key, source, feed, retained, page_state)
            if not checkpoint or found or sid not in {"S03", "S09", "S24"}:
                break
            data = json.loads(page)
            signature = sha256([identity(row) for row in parsed])
            if signature in page_fingerprints:
                raise ValueError(f"{sid} pagination repeated a page before discovery checkpoint")
            page_fingerprints.add(signature)
            if sid == "S03":
                next_url = data.get("next_page_url")
                if not next_url:
                    exhausted = True
                    break
                if urlsplit(next_url).hostname != urlsplit(feed["url"]).hostname:
                    raise ValueError("Federal Register next_page_url changed publisher")
                page_feed = dict(feed, url=next_url)
            elif sid == "S09":
                params = dict(parse_qsl(urlsplit(feed["url"]).query))
                size = int(params.get("pageSize", 20))
                root = data.get("data", {})
                catalogs = root.get("catalogs", [])
                total = root.get("total")
                if total is None and len(catalogs) == 1:
                    total = catalogs[0].get("total")
                if (
                    not parsed.wire_count
                    or parsed.wire_count < size
                    or (total is not None and page_number * size >= int(total))
                ):
                    exhausted = True
                    break
                page_number += 1
                params["pageNo"] = str(page_number)
                parts = urlsplit(feed["url"])
                page_feed = dict(
                    feed,
                    url=urlunsplit(
                        (parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment)
                    ),
                )
            else:
                query = feed.get("body", {}).get("query", "")
                match = re.search(r"\bfirst\s*:\s*(\d+)", query)
                if not match:
                    raise ValueError("Snapshot configured query needs explicit first window")
                size = int(match.group(1))
                if parsed.wire_count < size:
                    exhausted = True
                    break
                offset += parsed.wire_count
                query = re.sub(r"\bskip\s*:\s*\d+\s*,?", "", query)
                query = re.sub(r"proposals\s*\(", f"proposals(skip: {offset}, ", query, count=1)
                page_feed = dict(feed, body=dict(feed["body"], query=query))

            escalated = await svc.is_escalated(sid)
            if (self.now() - last_head_check).total_seconds() >= self.interval(source, feed, escalated):
                head, head_state = await svc.fetch(feed, key, on_start=lambda: self.mark_request_start(key))
                last_head_check = self.now()
                if head is not None:
                    head_rows = await asyncio.to_thread(parsers.json_items, head, sid)
                    entry_errors.extend(
                        {**error, "payload_sha256": head_state["payload_sha256"]}
                        for error in head_rows.errors
                    )
                    for row in head_rows:
                        row.update(observed_at=head_state.get("fetched_at") or self.stamp(), _payload=head)
                    await self.retain_page(key, source, feed, head_rows, head_state)
            page, page_state = await svc.fetch(
                page_feed,
                key
                + ":history:"
                + str(page_number if sid == "S09" else offset if sid == "S24" else sha256(page_feed["url"])),
            )
            if page is None:
                raise ValueError(f"{sid} history returned 304 without retained page replay")
        state["entry_errors"] = entry_errors
        if checkpoint and not found and exhausted:
            state["history_gap"] = {
                "reason": "KNOWN_BOUNDARY_NOT_IN_AVAILABLE_HISTORY",
                "checkpoint": checkpoint,
                "observed_at": self.stamp(),
                "observed_at_precision": "COLLECTION_ONLY",
            }
        elif found:
            state["history_gap"] = None
        if sid in {"S03", "S09", "S24"} and rows and not (checkpoint and not found and exhausted):
            state["discovery_checkpoint"] = (
                rows[0].get("proposal_id") if sid == "S24" else rows[0]["external_id"]
            )
        if sid == "S24":
            pending = state.get("pending_proposals", [])
            returned_ids = {row["proposal_id"] for row in rows}
            pending_evidence = {}

            for start in range(0, len(pending), 1000):
                ids = pending[start : start + 1000]
                query = (
                    "query { proposals(first: 1000, where: {id_in: "
                    + json.dumps(ids)
                    + "}) { id title body state start end created scores scores_total space { id } } }"
                )
                extra, extra_state = await svc.fetch(
                    dict(feed, body={"query": query}), key + ":pending:" + str(start)
                )
                for proposal_id in ids:
                    pending_evidence[proposal_id] = extra_state
                if extra:
                    existing = {r["external_id"] for r in rows}
                    extra_rows = await asyncio.to_thread(parsers.json_items, extra, sid)
                    state["entry_errors"].extend(
                        {**error, "payload_sha256": extra_state["payload_sha256"]}
                        for error in extra_rows.errors
                    )
                    returned_ids.update(row["proposal_id"] for row in extra_rows)
                    for row in extra_rows:
                        if row["external_id"] not in existing:
                            row.update(
                                observed_at=extra_state.get("fetched_at") or self.stamp(), _payload=extra
                            )
                            rows.append(row)
            missing = set(pending) - returned_ids
            state["entry_errors"].extend(
                {
                    "reason": "PENDING_PROPOSAL_UNAVAILABLE",
                    "proposal_id": proposal_id,
                    "payload_sha256": pending_evidence[proposal_id].get("payload_sha256"),
                }
                for proposal_id in sorted(missing)
            )
            state["pending_proposals"] = sorted(
                {r["proposal_id"] for r in rows if r["state"] != "closed"} | missing
            )
        return rows, state

    async def acquire(self, source, index, *, due_only=False, escalated=False):
        sid = source["source_id"]
        key, feed = f"{sid}:{index}", self.feed(source, index)
        async with self.lock(self.acquisition_locks, key):
            state = await self.state(key)
            last = state.get("last_attempt_at") or state.get("last_success_at")
            if (
                due_only
                and last
                and (self.now() - parse_time(last)).total_seconds() < self.interval(source, feed, escalated)
            ):
                return {"items": 0, "errors": []}
            attempted = self.stamp()
            async with self.lock(self.state_locks, key):
                current = await self.state(key)
                current["last_attempt_at"] = attempted
                await self.save(key, current)
            try:
                rows, acquired, kind = await self._fetch_rows(source, feed, key, state)
                async with self.lock(self.state_locks, key):
                    current = await self.state(key)
                    if rows is not None:
                        coverage = {
                            field: acquired[field]
                            for field in (
                                "instrument_snapshot_schema_version",
                                "pending_proposals",
                                "discovery_checkpoint",
                                "etag",
                                "last_modified",
                                "account_cursors",
                            )
                            if field in acquired
                            and (
                                field
                                not in {
                                    "discovery_checkpoint",
                                    "etag",
                                    "last_modified",
                                }
                                or not acquired.get("entry_errors")
                            )
                        }
                        await self.queue_rows(
                            current,
                            key,
                            source,
                            feed,
                            rows,
                            kind,
                            coverage,
                            attempted,
                            acquired.get("fetched_at") or self.stamp(),
                        )
                    for field in (
                        "payload_sha256",
                        "fetched_at",
                        "http_status",
                        "scan_status",
                        "history_gap",
                        "entry_errors",
                    ):
                        if field in acquired:
                            current[field] = acquired[field]
                    current.pop("acquisition_error", None)
                    current.pop("instruments", None)
                    if not any(batch["rows"] for batch in current.get("pending_batches", [])):
                        current["article_errors"] = {}
                    read_ok = not acquired.get("run_failed")
                    count_read(current, "reads", ok=read_ok)
                    remember_reads(current, [(self.stamp(), read_ok)])
                    if not read_ok:
                        current["last_failure_at"] = self.stamp()
                    current.update(
                        status="DEGRADED"
                        if current.get("article_errors")
                        or current.get("entry_errors")
                        or current.get("history_gap")
                        or acquired.get("scan_status") == "GAP_LIMIT"
                        else "HEALTHY",
                        error=None,
                    )
                    if rows is None:
                        current["last_success_at"] = self.stamp()
                    elif not current.get("last_success_at") and current["status"] == "HEALTHY":
                        current["status"] = "PENDING"
                    await self.save(key, current)
                if not read_ok:
                    log.warning("Source feed %s read failed: the actor run failed", key)
            except Exception as exc:
                async with self.lock(self.state_locks, key):
                    current = await self.state(key)
                    current.pop("etag", None)
                    current.pop("last_modified", None)
                    count_read(current, "reads", ok=False)
                    remember_reads(current, [(self.stamp(), False)])
                    current.update(
                        status="DEGRADED",
                        acquisition_error=str(exc),
                        error=str(exc),
                        last_failure_at=self.stamp(),
                    )
                    await self.save(key, current)
                log.warning("Source feed %s read failed: %s", key, exc)
                return {"items": 0, "errors": [str(exc)]}
            await self.report_entry_errors(
                key, source, feed, acquired.get("entry_errors") or [], acquired.get("fetched_at") or attempted
            )
            return {"items": 0, "errors": []}

    async def report_entry_errors(self, key, source, feed, errors, seen_at):
        """Report this poll's entry errors and earlier polls whose report failed.

        A failed report never fails acquisition. Its poll stays in the checkpoint's
        ``entry_error_reports`` backlog and is retried on the next acquisition even if
        the feed no longer returns the broken entry; markers make the retry idempotent.
        """
        svc = self.service
        try:
            backlog = (await self.state(key)).get("entry_error_reports") or []
            polls = backlog + ([{"seen_at": seen_at, "entry_errors": errors}] if errors else [])
            failed = []
            for poll in polls:
                try:
                    await report_entry_errors(
                        svc.repo, svc.settings, source, feed, poll["entry_errors"], poll["seen_at"]
                    )
                except Exception:
                    log.exception("Entry error report for %s is kept for the next poll", key)
                    failed.append(poll)
            if failed or backlog:
                async with self.lock(self.state_locks, key):
                    current = await self.state(key)
                    current["entry_error_reports"] = failed
                    await self.save(key, current)
        except Exception:
            log.exception("Entry error report backlog for %s could not be saved", key)

    def _empty_body(self, url, detail_state):
        return {
            "url": url,
            "http_status": detail_state.get("http_status"),
            "payload_sha256": detail_state.get("payload_sha256"),
            "observed_at": self.stamp(),
            "observed_at_precision": "COLLECTION_ONLY",
            "reason": "EMPTY_ARTICLE_BODY",
        }

    async def _article(self, source, feed, key, item):
        """Fetch the article page; S13 may fall back to the publisher's own CMS slug URL."""
        svc, sid = self.service, source["source_id"]
        try:
            return await svc.fetch(
                {"url": item["url"], "source_limits": source.get("limits", {})},
                key + ":" + sha256(item["url"]),
            )
        except httpx.HTTPStatusError as exc:
            binding = item.get("publisher_cms_identity")
            fallback = item.get("publisher_cms_slug_url")
            origin = urlsplit(feed["url"])
            candidate = urlsplit(fallback or "")
            if not (
                sid == "S13"
                and exc.response.status_code == 404
                and isinstance(binding, dict)
                and re.fullmatch(r"[A-Za-z0-9-]+", binding.get("slug", ""))
                and candidate.scheme == origin.scheme
                and candidate.netloc == origin.netloc
                and candidate.path == origin.path.rstrip("/") + "/" + binding["slug"] + "/"
                and not candidate.query
                and not candidate.fragment
                and fallback != item["url"]
            ):
                raise
            article, detail_state = await svc.fetch(
                {"url": fallback, "source_limits": source.get("limits", {})},
                key + ":" + sha256(fallback),
            )
            if article is None or binding not in parsers.publisher_cms_entries(article):
                raise ValueError("Publisher CMS fallback identity/content mismatch") from None
            if urlsplit(detail_state.get("response_url") or fallback).netloc != origin.netloc:
                raise ValueError("Publisher CMS fallback left official host") from None
            item["publisher_cms_recovery"] = {
                "status": "VERIFIED_SAME_CMS_ENTRY",
                "canonical_url": item["url"],
                "resolved_url": fallback,
                "original_http_status": 404,
                "original_response_url": str(exc.response.url),
                "original_payload_sha256": sha256(exc.response.content),
                "observed_at": self.stamp(),
                "publisher_cms_identity": binding,
            }
            item["publisher_url_aliases"] = [item["url"], fallback]
            item["url"] = fallback
            return article, detail_state

    async def _item(self, source, feed, key, batch, original):
        svc, sid = self.service, source["source_id"]
        item = deepcopy(original)
        if self.needs_detail(sid, batch["kind"]):
            if sid == "S09":
                payload, detail_state = await svc.fetch(
                    {
                        "url": "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode="
                        + item["external_id"]
                    },
                    key + ":detail:" + item["external_id"],
                )
                if payload is None:
                    raise ValueError("Binance article returned 304 without a durable source item")
                detail_object = json.loads(payload)["data"]
                if not item.get("published_at") and detail_object.get("publishDate"):
                    item.update(
                        parsers.timestamp_fields(detail_object["publishDate"]),
                        published_at_basis="BINANCE_CMS_DETAIL_PUBLISH_DATE",
                    )
                item["text"] = parsers.binance_body(detail_object.get("body", ""))
                if not item["text"].strip():
                    return self._empty_body(item["url"], detail_state)
            else:
                payload, detail_state = await self._article(source, feed, key, item)
                if payload is None:
                    raise ValueError("Article returned 304 without a durable source item")
                detail = await asyncio.to_thread(parsers.article, payload, source_id=sid)
                if not detail["text"].strip():
                    return self._empty_body(item["url"], detail_state)

                item.update(
                    {
                        k: v
                        for k, v in detail.items()
                        if v
                        and not (
                            k.startswith("published_")
                            and not detail.get("published_at")
                            and item.get("published_at")
                        )
                    }
                )
            item["raw_ref"], item["raw_sha256"] = svc.raw(
                payload, source_id=sid, item_id=item["item_id"], observed_at=item["observed_at"]
            )
        stored = await svc.ingest(source, item)
        if stored["item_id"] == item["item_id"]:
            return None
        log.warning(
            "Source feed %s entry %s merged as a duplicate into item %s",
            key,
            item["external_id"],
            stored["item_id"],
        )
        return "DUPLICATE"

    async def drain(self, source, index, *, force=False, escalated=False):
        sid = source["source_id"]
        key, feed = f"{sid}:{index}", self.feed(source, index)
        async with self.lock(self.processing_locks, key):
            state = await self.state(key)
            if (
                not force
                and state.get("detail_retry_at")
                and parse_time(state.get("last_failure_at") or state["detail_retry_at"])
                + timedelta(seconds=self.interval(source, feed, escalated))
                > self.now()
            ):
                return {"items": 0, "errors": []}
            if any(
                "item_id" not in row for batch in state.get("pending_batches", []) for row in batch["rows"]
            ):
                async with self.lock(self.state_locks, key):
                    current = await self.state(key)
                    for batch in current.get("pending_batches", []):
                        batch["rows"] = [row for row in batch["rows"] if "item_id" in row]
                    await self.save(key, current)
                state = await self.state(key)
            batches = deepcopy(state.get("pending_batches", []))
            if not any(batch["rows"] for batch in batches) and state.get("article_errors"):
                async with self.lock(self.state_locks, key):
                    current = await self.state(key)
                    current["article_errors"] = {}
                    if current.get("status") == "DEGRADED" and not (
                        current.get("entry_errors")
                        or current.get("history_gap")
                        or current.get("acquisition_error")
                    ):
                        current.update(status="HEALTHY", error=None)
                    await self.save(key, current)
            if not batches:
                return {"items": 0, "errors": []}
            errors, completed, count, duplicates, outcomes = [], {}, 0, [], []
            article_errors = {}
            for batch in batches:

                async def process(row, current_batch=batch):
                    nonlocal count
                    async with self.details:
                        try:
                            error = await self._item(source, feed, key, current_batch, row)
                            if error == "DUPLICATE":
                                duplicates.append(str(row["external_id"]))
                                return None
                            if error:
                                article_errors[row["url"]] = error
                                errors.append("Article parse failure (empty visible text): " + row["url"])
                                return row
                            count += 1
                            return None
                        except httpx.HTTPStatusError as exc:
                            article_errors[row.get("url", row["external_id"])] = {
                                "url": row.get("url"),
                                "http_status": exc.response.status_code,
                                "payload_sha256": sha256(exc.response.content),
                                "observed_at": self.stamp(),
                                "observed_at_precision": "COLLECTION_ONLY",
                                "reason": "PUBLISHER_ARTICLE_NOT_FOUND"
                                if exc.response.status_code == 404
                                else "PUBLISHER_HTTP_FAILURE",
                            }
                            errors.append(f"Article HTTP{exc.response.status_code}: " + str(row.get("url")))
                            return row
                        except Exception as exc:
                            errors.append(str(exc))
                            return row

                async def settled(row, current_batch=batch):
                    failed = await process(row, current_batch)
                    if self.needs_detail(sid, current_batch["kind"]):
                        outcomes.append((self.stamp(), failed is None))
                    return failed

                remaining = await asyncio.gather(*(settled(row) for row in batch["rows"]))
                completed[batch["batch_id"]] = {
                    row["item_id"]
                    for row, failed in zip(batch["rows"], remaining, strict=True)
                    if failed is None
                }
            async with self.lock(self.state_locks, key):
                current = await self.state(key)
                done_count = sum(len(ids) for ids in completed.values())
                count_read(current, "detail_reads", ok=True, n=done_count)
                count_read(current, "detail_reads", ok=False, n=len(errors))
                remember_reads(current, outcomes)
                for batch in current.get("pending_batches", []):
                    if batch["batch_id"] in completed:
                        done = completed[batch["batch_id"]]
                        batch["rows"] = [row for row in batch["rows"] if row["item_id"] not in done]

                while current.get("pending_batches") and not current["pending_batches"][0]["rows"]:
                    batch = current["pending_batches"].pop(0)
                    current.update(batch["coverage"])
                    current.pop("instruments", None)
                current["article_errors"] = article_errors
                if duplicates:
                    current["duplicate_ids"] = (current.get("duplicate_ids", []) + duplicates)[-1000:]
                if errors:
                    log.warning(
                        "Source feed %s: %d detail reads failed: %s", key, len(errors), "; ".join(errors)
                    )
                    current.update(
                        status="DEGRADED",
                        error="; ".join(errors),
                        last_failure_at=self.stamp(),
                        detail_retry_at=self.stamp(
                            self.now() + timedelta(seconds=self.interval(source, feed, escalated))
                        ),
                    )
                    current.pop("etag", None)
                    current.pop("last_modified", None)
                else:
                    current.pop("detail_retry_at", None)
                    current.update(
                        status="DEGRADED"
                        if current.get("history_gap")
                        or current.get("entry_errors")
                        or current.get("acquisition_error")
                        or current.get("scan_status") == "GAP_LIMIT"
                        else "HEALTHY",
                        error="X_HISTORY_GAP_LIMIT" if current.get("scan_status") == "GAP_LIMIT" else None,
                        last_success_at=self.stamp(),
                    )
                await self.save(key, current)
            return {"items": count, "errors": errors}

    async def poll_once(self, source_id, *, due_only=False, escalated=False):
        source = self.service.sources[source_id]

        async def one_feed(index):
            acquired = await self.acquire(source, index, due_only=due_only, escalated=escalated)
            processed = await self.drain(source, index, force=not due_only, escalated=escalated)
            return {"items": processed["items"], "errors": acquired["errors"] + processed["errors"]}

        processed = await asyncio.gather(*(one_feed(i) for i in range(len(source.get("feeds", [])))))
        errors = [e for result in processed for e in result["errors"]]
        states = [await self.state(f"{source_id}:{i}") for i in range(len(source.get("feeds", [])))]
        return {
            "source_id": source_id,
            "items": sum(result["items"] for result in processed),
            "status": "DEGRADED"
            if errors or any(s.get("status") == "DEGRADED" for s in states)
            else "HEALTHY",
            "errors": errors,
        }

    async def loop(self, source, stop):
        sid = source["source_id"]
        while not stop.is_set():
            escalated = await self.service.is_escalated(sid)
            next_wake = 15.0
            for index in range(len(source.get("feeds", []))):
                key, feed = f"{sid}:{index}", self.feed(source, index)
                state = await self.state(key)
                last = state.get("last_attempt_at") or state.get("last_success_at")
                remaining = (
                    0
                    if not last
                    else max(
                        0,
                        self.interval(source, feed, escalated)
                        - (self.now() - parse_time(last)).total_seconds(),
                    )
                )
                task = self.active_acquisitions.get(key)
                if (task is None or task.done()) and remaining <= 0:
                    if task is not None:
                        task.result()
                    self.active_acquisitions[key] = asyncio.create_task(
                        self.acquire(source, index, due_only=True, escalated=escalated)
                    )
                    remaining = self.interval(source, feed, escalated)
                next_wake = min(next_wake, remaining if remaining > 0 else 15)
                detail = self.active_details.get(key)
                if state.get("pending_batches") and (detail is None or detail.done()):
                    if detail is not None:
                        detail.result()
                    self.active_details[key] = asyncio.create_task(
                        self.drain(source, index, escalated=escalated)
                    )

            active = [
                task
                for key, task in self.active_acquisitions.items()
                if key.startswith(sid + ":") and not task.done()
            ]
            waiter = asyncio.create_task(stop.wait())
            try:
                if active:
                    await asyncio.wait(
                        [waiter, *active], timeout=next_wake, return_when=asyncio.FIRST_COMPLETED
                    )
                else:
                    await asyncio.wait_for(asyncio.shield(waiter), timeout=next_wake)
            except TimeoutError:
                pass
            finally:
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)

    async def run(self, stop):
        loops = [asyncio.create_task(self.loop(source, stop)) for source in self.service.sources.values()]
        try:
            if loops:
                await asyncio.gather(*loops)
        finally:
            for task in loops:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*loops, return_exceptions=True)
            tasks = list(self.active_acquisitions.values()) + list(self.active_details.values())
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.active_acquisitions.clear()
            self.active_details.clear()
