"""Feed acquisition and durable detail backlog inside SourceService's source loops.

Network acquisition never waits for article enrichment. Completed coverage moves
in acquisition order; failed originals stay in the checkpoint's pending batches.
"""

import asyncio
import json
import logging
import re
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Literal, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from pydantic import JsonValue

import tank.sources.parsers as parsers
from tank.contracts.json import decode_unique_json
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.source_checkpoints import (
    INSTRUMENT_BASELINE_ARCHIVES,
    CompletedPendingRow,
    InstrumentAcquisitionScope,
    InstrumentBaselineArchive,
    InstrumentCoverage,
    InstrumentSnapshot,
    InstrumentSnapshotUpdate,
    MaterializedSourceObservation,
    ReadCounter,
    ScopedInstrumentBaseline,
    SourcePendingBatch,
)
from tank.contracts.source_errors import SourceEntryError
from tank.contracts.source_items import (
    ArticleDiscovery,
    ArticleLinkListing,
    BoundArticleLink,
    DiscoveredItem,
    DiscoveryEnvelope,
    DiscoveryObservation,
    GovernanceDiscovery,
    PendingArticleLink,
    PendingSourceItem,
    PendingSourceRow,
    PublisherCMSRecovery,
    RawSourceReference,
    SourceText,
    captured_item,
    discovered_item,
    parse_source_item,
)
from tank.contracts.source_parsing import BinanceArticleDetailReply
from tank.contracts.source_reports import (
    SourceReportIntent,
    SourceReportMode,
    SourceReportOutcome,
    SourceReportRequest,
    SourceReportSubject,
)
from tank.contracts.sources import (
    FeedKind,
    FeedPollOutcome,
    HttpFetchPlan,
    PolledSource,
    SourceAccount,
    SourceDefinition,
    SourceFeed,
    SourcePollOutcome,
    UnconfiguredSource,
)
from tank.repository import Repository as TypedRepository
from tank.sources.checkpoints import (
    ArticleFailure,
    CheckpointStore,
    CompletedSourceRow,
    FailedSourceRow,
    PendingCheckpointState,
    QuarantineArchiveRepository,
    SourceCadence,
    SourceDegradation,
    SourceDuplicateCache,
    SourceRowResult,
    decode_pending_checkpoint,
    pending_coverage,
)
from tank.sources.fetch import FetchedContent, FetchEvidence, FetchResult, NotModified
from tank.sources.http_client import RequestStarted
from tank.sources.instrument_adapters import parse_instrument_page
from tank.sources.instruments import InstrumentPage, instrument_changes
from tank.sources.reporting import SourceReportSpool
from tank.sources.xsensor import XSensor
from tank.time_precision import collection_clock
from tank.util import new_id, parse_time, sha256

log = logging.getLogger(__name__)
ARTICLE_KINDS = {"html", "rendered", "rendered_html", "html_diff"}


def count_read(
    state: dict[str, JsonValue],
    name: Literal["reads", "detail_reads"],
    *,
    ok: bool,
    n: int = 1,
) -> None:
    """Cumulative counts: absent means no prior reads; declared records must be complete."""
    if n < 0:
        raise ValueError("Read counts cannot decrease")
    counters = ReadCounter.model_validate(state[name]) if name in state else ReadCounter(ok=0, failed=0)
    updated = ReadCounter(
        ok=counters.ok + (n if ok else 0),
        failed=counters.failed + (0 if ok else n),
    )
    state[name] = updated.model_dump(mode="json")


class SourcePollingPort(Protocol):
    @property
    def repo(self) -> CheckpointStore: ...
    @property
    def versions(self) -> ResolvedVersionIdentity: ...
    @property
    def sources(self) -> Mapping[str, SourceDefinition]: ...
    @property
    def accounts(self) -> Mapping[str, SourceAccount]: ...
    @property
    def xsensor(self) -> XSensor: ...
    @property
    def escalated_interval_s(self) -> float: ...

    def now(self) -> datetime: ...
    def stamp(self, value: datetime | str | None = None) -> str: ...
    def raw(
        self, payload: bytes, *, source_id: str, item_id: str, observed_at: datetime | str
    ) -> tuple[str, str]: ...
    def filing_matches(self, item: SourceText) -> bool: ...
    async def observe_known(self, source_id: str, external_id: str | int, feed: str) -> bool: ...
    async def is_escalated(self, source_id: str, *, now: datetime | str | None = None) -> bool: ...
    async def fetch(
        self,
        source: SourceDefinition,
        feed: HttpFetchPlan,
        key: str,
        *,
        persist_http_evidence: bool,
        on_start: RequestStarted | None = None,
        cache_scope: str | None = None,
    ) -> FetchResult: ...
    async def ingest(
        self, source: SourceDefinition, item: Mapping[str, JsonValue]
    ) -> dict[str, JsonValue]: ...
    def admit_entry_errors(self, errors: JsonValue) -> list[SourceEntryError]: ...
    @property
    def error_reporting_mode(self) -> SourceReportMode: ...
    async def report_error_intent(self, intent: SourceReportIntent) -> SourceReportOutcome: ...


class SourcePoller:
    def __init__(self, service: SourcePollingPort) -> None:
        self.service = service
        self.archives = QuarantineArchiveRepository(service.repo, service.versions)
        self.reports = SourceReportSpool(service.repo, service.versions, lambda: collection_clock(self.now()))
        self.acquisition_locks: dict[str, asyncio.Lock] = {}
        self.state_locks: dict[str, asyncio.Lock] = {}
        self.processing_locks: dict[str, asyncio.Lock] = {}

        self.details = asyncio.Semaphore(4)
        self.active_acquisitions: dict[str, asyncio.Task[FeedPollOutcome]] = {}
        self.active_details: dict[str, asyncio.Task[FeedPollOutcome]] = {}

    def now(self) -> datetime:
        return self.service.now()

    def stamp(self, value: datetime | str | None = None) -> str:
        return self.service.stamp(value)

    def lock(self, mapping: dict[str, asyncio.Lock], key: str) -> asyncio.Lock:
        return mapping.setdefault(key, asyncio.Lock())

    def feed(self, source: SourceDefinition, index: int) -> SourceFeed:
        return source.feeds[index]

    def interval(self, source: SourceDefinition, feed: SourceFeed, escalated: bool) -> float:
        return self.service.escalated_interval_s if escalated else feed.interval_s

    @asynccontextmanager
    async def state_change(self, key: str) -> AsyncIterator[None]:
        async with (
            self.lock(self.state_locks, key),
            self.service.repo.lock("source-checkpoint:" + key),
            self.service.repo.transaction(),
        ):
            yield

    async def state(self, key: str) -> dict[str, JsonValue]:
        value = await self.service.repo.get("ops.source_checkpoints", key)
        return {} if value is None else value

    async def pending(
        self, source: SourceDefinition, key: str, state: Mapping[str, JsonValue]
    ) -> PendingCheckpointState:
        decoded = decode_pending_checkpoint(
            source, key, state, now=self.now(), versions=self.service.versions
        )
        return await self.archives.reconcile(decoded)

    async def save(self, source: SourceDefinition, key: str, state: dict[str, JsonValue]) -> None:
        async with self.service.repo.lock("source-checkpoint:" + key), self.service.repo.transaction():
            if "source_id" in state and state["source_id"] != source.source_id:
                raise ValueError("Checkpoint owner conflicts with the actual source operation")
            pending = await self.pending(source, key, state)
            now = self.now()
            pending = await self.archives.persist(pending, now=now)
            if "pending_batches" in state or pending.batches:
                state["pending_batches"] = pending.storage_records()
            state["source_id"] = source.source_id
            state.update(collection_clock(now).fields("updated_at"))
            for field in (
                "fetched_at",
                "last_attempt_at",
                "last_success_at",
                "last_failure_at",
                "detail_retry_at",
            ):
                if field in state and state[field] is not None:
                    value = state[field]
                    if not isinstance(value, str):
                        raise ValueError("Source operational checkpoint clock must be ISO text")
                    state.update(collection_clock(value).fields(field))
                    state[field + "_precision"] = "COLLECTION_ONLY"
            await self.service.repo.put("ops.source_checkpoints", key, state)

    @staticmethod
    def needs_detail(sid: str, kind: FeedKind) -> bool:
        """The item's raw payload is its article/detail page, fetched in the backlog."""
        return kind in ARTICLE_KINDS or sid in {"S01", "S09"}

    async def queue_rows(
        self,
        current: dict[str, JsonValue],
        key: str,
        source: SourceDefinition,
        feed: SourceFeed,
        rows: Sequence[DiscoveryEnvelope | ArticleLinkListing | Mapping[str, JsonValue | bytes]],
        kind: FeedKind,
        coverage: Mapping[str, JsonValue],
        acquired_at: str,
    ) -> None:
        """Validate discoveries before binding first-seen identity and durable raw evidence."""
        svc, sid = self.service, source.source_id
        checkpoint = await self.pending(source, key, current)
        pending = checkpoint.pending_external_ids()
        duplicates = SourceDuplicateCache.from_document(current)
        fresh: list[PendingSourceRow] = []
        for original in rows:
            material: DiscoveredItem | BoundArticleLink
            if isinstance(original, ArticleLinkListing):
                envelope = None
                material = BoundArticleLink(
                    source_id=sid,
                    external_id=original.external_id,
                    url=original.url,
                    title=original.title,
                    publisher_cms_identity=original.publisher_cms_identity,
                    publisher_cms_slug_url=original.publisher_cms_slug_url,
                )
            else:
                envelope = (
                    original if isinstance(original, DiscoveryEnvelope) else discovered_item(sid, original)
                )
                material = envelope.discovery
            if material.source_id != sid:
                raise ValueError("Discovery source identity conflicts with its configured source")
            external = material.external_id
            if (
                external in pending
                or external in duplicates.duplicate_ids
                or await svc.observe_known(sid, external, feed.url)
            ):
                continue
            if sid == "S02":
                if isinstance(material, BoundArticleLink):
                    raise ValueError("SEC filing admission requires the reported filing text")
                if not svc.filing_matches(material):
                    continue
            item_id = new_id("item")
            observed_at = (
                envelope.observation.observed_at
                if envelope is not None and envelope.observation.observed_at is not None
                else acquired_at
            )
            raw = None
            if not self.needs_detail(sid, kind):
                if envelope is None or envelope.payload is None:
                    raise ValueError("Source discovery has no raw response bytes")
                raw_ref, raw_sha256 = svc.raw(
                    envelope.payload, source_id=sid, item_id=item_id, observed_at=observed_at
                )
                raw = RawSourceReference(raw_ref=raw_ref, raw_sha256=raw_sha256)
            row: PendingSourceRow
            if isinstance(material, BoundArticleLink):
                row = PendingArticleLink(
                    discovery=material,
                    item_id=item_id,
                    feed=feed.url,
                    observed_at=observed_at,
                    discovery_config_version=svc.versions.config_version,
                    discovery_code_version=svc.versions.code_version,
                )
            else:
                row = PendingSourceItem(
                    discovery=material,
                    item_id=item_id,
                    feed=feed.url,
                    observed_at=observed_at,
                    discovery_config_version=svc.versions.config_version,
                    discovery_code_version=svc.versions.code_version,
                    raw=raw,
                )
            pending.add(external)
            fresh.append(row)
        batch = SourcePendingBatch(
            source_id=sid,
            batch_id=new_id("batch"),
            acquired_at=acquired_at,
            acquired_at_precision="COLLECTION_ONLY",
            kind=kind,
            rows=fresh,
            coverage=pending_coverage(coverage),
        )
        current["pending_batches"] = [*checkpoint.storage_records(), batch.storage_record()]

    async def retain_page(
        self,
        key: str,
        source: SourceDefinition,
        feed: SourceFeed,
        rows: Sequence[DiscoveryEnvelope],
        page_state: Mapping[str, JsonValue],
        *,
        kind: FeedKind = "json",
    ) -> None:

        async with self.state_change(key):
            current = await self.state(key)
            await self.queue_rows(
                current,
                key,
                source,
                feed,
                rows,
                kind,
                {},
                FetchEvidence.model_validate(page_state).fetched_at,
            )
            await self.save(source, key, current)

    async def mark_request_start(self, source: SourceDefinition, key: str) -> None:
        async with self.state_change(key):
            current = await self.state(key)
            current["last_attempt_at"] = self.stamp()
            await self.save(source, key, current)

    async def _fetch_rows(
        self, source: SourceDefinition, feed: SourceFeed, key: str, state: dict[str, JsonValue]
    ) -> tuple[
        list[DiscoveryEnvelope] | list[ArticleLinkListing] | None,
        dict[str, JsonValue],
        FeedKind,
        InstrumentSnapshotUpdate | None,
    ]:
        svc = self.service
        sid = source.source_id
        kind = feed.kind
        snapshot_update: InstrumentSnapshotUpdate | None = None
        acquisition_scope = InstrumentAcquisitionScope.from_feed(feed) if sid == "S14" else None
        original_state = deepcopy(state) if sid == "S14" else None
        if sid == "S29":
            accounts = [account.handle.lstrip("@") for account in svc.accounts.values()]
            cursor_wire = state.get("account_cursors", {})
            if not isinstance(cursor_wire, dict) or any(
                not isinstance(value, str) for value in cursor_wire.values()
            ):
                raise ValueError("Stored X account cursors require actual string cursor values")
            x_cursors = {name: value for name, value in cursor_wire.items() if isinstance(value, str)}
            result = await svc.xsensor.posts(
                feed.url,
                accounts,
                x_cursors,
                on_start=lambda: self.mark_request_start(source, key),
            )
            state.update(
                fetched_at=self.stamp(),
                account_cursors={name: value for name, value in result.cursors.items()},
                entry_errors=[failure.model_dump(mode="json") for failure in result.failures],
                history_gap=[value for value in result.gaps],
            )
            x_rows: list[DiscoveryEnvelope] = list(result.rows)
            return x_rows, state, kind, None
        _http_read = await svc.fetch(
            source,
            feed.request_plan(source.limits),
            key,
            persist_http_evidence=True,
            on_start=lambda: self.mark_request_start(source, key),
            cache_scope=None if acquisition_scope is None else acquisition_scope.digest,
        )
        if isinstance(_http_read, NotModified):
            return None, state, kind, None
        payload = _http_read.payload
        state.update(_http_read.receipt.storage_record())
        state["entry_errors"] = []
        if sid == "S14":
            parsed_page = await asyncio.to_thread(parse_instrument_page, payload, feed)
            snapshot = parsed_page.snapshot
            pages = [InstrumentPage(payload=payload, observed_at=_http_read.receipt.fetched_at)]
            found_on = dict.fromkeys(snapshot.root, 0)
            if feed.venue == "bybit":
                cursor = parsed_page.next_cursor
                cursors = set()
                while cursor:
                    if cursor in cursors:
                        raise ValueError("Repeated Bybit instrument cursor")
                    cursors.add(cursor)
                    parts = urlsplit(feed.url)
                    params = [
                        (name, value)
                        for name, value in parse_qsl(parts.query, keep_blank_values=True)
                        if name != "cursor"
                    ]
                    params.append(("cursor", cursor))
                    next_feed = feed.model_copy(
                        update={
                            "url": urlunsplit(
                                (parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment)
                            ),
                        }
                    )
                    _http_read = await svc.fetch(
                        source,
                        next_feed.request_plan(source.limits),
                        key + ":cursor:" + sha256(cursor),
                        persist_http_evidence=False,
                    )
                    if isinstance(_http_read, NotModified):
                        raise ValueError(
                            "Source detail/history returned 304 without retained content"
                        ) from None
                    page = _http_read.payload
                    parsed_page = await asyncio.to_thread(parse_instrument_page, page, feed)
                    page_snapshot = parsed_page.snapshot
                    snapshot = InstrumentSnapshot(root={**snapshot.root, **page_snapshot.root})
                    found_on.update(dict.fromkeys(page_snapshot.root, len(pages)))
                    pages.append(InstrumentPage(payload=page, observed_at=_http_read.receipt.fetched_at))
                    cursor = parsed_page.next_cursor

            if acquisition_scope is None:
                raise ValueError("Instrument catalog has no acquisition scope")
            snapshot_key = key + ":instruments"
            scope_key = snapshot_key + ":scope:" + acquisition_scope.digest
            stored = await self.state(snapshot_key)
            scoped = await self.state(scope_key)
            if "source_id" in stored and stored["source_id"] != sid:
                raise ValueError("Instrument baseline owner conflicts with its operation")
            pointer_scope = (
                InstrumentAcquisitionScope.model_validate(stored["acquisition_scope"])
                if "acquisition_scope" in stored
                else None
            )
            baseline = ScopedInstrumentBaseline.from_document(scoped) if scoped else None
            if baseline is None and pointer_scope == acquisition_scope:
                baseline = ScopedInstrumentBaseline.from_document(stored)
            if baseline is not None and baseline.acquisition_scope != acquisition_scope:
                raise ValueError("Instrument baseline key conflicts with its acquisition scope")
            state["instrument_snapshot_schema_version"] = 2
            state["cache_scope"] = acquisition_scope.digest
            instrument_rows, digest = await asyncio.to_thread(
                instrument_changes,
                snapshot,
                None if baseline is None else baseline.instruments,
                feed,
                pages,
                found_on,
                self.stamp(),
            )
            if baseline is None or baseline.sha256 != digest or pointer_scope != acquisition_scope:
                archives: list[InstrumentBaselineArchive] = []
                if stored and pointer_scope != acquisition_scope:
                    archives.append(
                        InstrumentBaselineArchive.capture(
                            snapshot_key, stored, captured_at=pages[0].observed_at, versions=svc.versions
                        )
                    )
                if original_state is not None and "instruments" in original_state:
                    archives.append(
                        InstrumentBaselineArchive.capture(
                            key, original_state, captured_at=pages[0].observed_at, versions=svc.versions
                        )
                    )
                snapshot_update = InstrumentSnapshotUpdate(
                    source_id="S14",
                    checkpoint_key=snapshot_key,
                    snapshot=snapshot,
                    acquisition_scope=acquisition_scope,
                    archives=archives,
                    sha256=digest,
                    versions=svc.versions,
                )
            instrument_discoveries: list[DiscoveryEnvelope] = list(instrument_rows)
            return instrument_discoveries, state, kind, snapshot_update
        elif kind in {"rss", "atom", "feed"}:
            parsed = await asyncio.to_thread(parsers.feeds, payload, sid, feed)
            state["entry_errors"] = [
                {
                    **error.model_dump(mode="json", exclude_unset=True),
                    "payload_sha256": state["payload_sha256"],
                }
                for error in parsed.errors
            ]
            rows = [
                DiscoveryEnvelope(discovery=entry.discovery, observation=entry.observation, payload=payload)
                for entry in parsed.discoveries
            ]
        elif kind in ARTICLE_KINDS:
            links = await asyncio.to_thread(parsers.article_links, payload, feed)
            return links, state, kind, snapshot_update
        else:
            json_rows, state = await self._json_history(source, feed, key, payload, state)
            return json_rows, state, kind, snapshot_update
        return rows, state, kind, snapshot_update

    async def _json_history(
        self,
        source: SourceDefinition,
        feed: SourceFeed,
        key: str,
        payload: bytes,
        state: dict[str, JsonValue],
    ) -> tuple[list[DiscoveryEnvelope], dict[str, JsonValue]]:
        """Read newest-first windows back to the last completed identity.

        Bootstrap deliberately establishes the configured head window. Later polls
        traverse available history without an invented page cap. Schema stalls and
        exhaustion before a formerly known boundary are explicit gaps.
        """
        svc, sid = self.service, source.source_id
        rows: list[DiscoveryEnvelope] = []
        checkpoint = state.get("discovery_checkpoint")
        page_feed = deepcopy(feed)
        page = payload
        page_state = state
        page_fingerprints = set()
        entry_errors: list[JsonValue] = []
        offset = 0
        found = False
        exhausted = False
        page_number = int(dict(parse_qsl(urlsplit(feed.url).query)).get("pageNo", 1))
        last_head_check = parse_time(FetchEvidence.model_validate(state).fetched_at)
        while True:
            parsed = await asyncio.to_thread(parsers.json_items, page, sid)
            entry_errors.extend(
                {
                    **error.model_dump(mode="json", exclude_unset=True),
                    "payload_sha256": page_state["payload_sha256"],
                }
                for error in parsed.errors
            )

            def identity(row: DiscoveryEnvelope) -> str:
                if sid == "S24":
                    if not isinstance(row.discovery, GovernanceDiscovery):
                        raise ValueError("Snapshot acquisition requires a governance discovery")
                    return row.discovery.proposal_id
                return row.discovery.external_id

            retained = []
            for entry in parsed.discoveries:
                row = DiscoveryEnvelope(
                    discovery=entry.discovery,
                    observation=DiscoveryObservation.model_validate(
                        collection_clock(FetchEvidence.model_validate(page_state).fetched_at).fields(
                            "observed_at"
                        )
                    ),
                    payload=page,
                )
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
            signature_values: list[JsonValue] = [identity(row) for row in parsed.discoveries]
            signature = sha256(signature_values)
            if signature in page_fingerprints:
                raise ValueError(f"{sid} pagination repeated a page before discovery checkpoint")
            page_fingerprints.add(signature)
            if sid == "S03":
                next_url = data.get("next_page_url")
                if not next_url:
                    exhausted = True
                    break
                if urlsplit(next_url).hostname != urlsplit(feed.url).hostname:
                    raise ValueError("Federal Register next_page_url changed publisher")
                page_feed = feed.model_copy(update={"url": next_url})
            elif sid == "S09":
                params = dict(parse_qsl(urlsplit(feed.url).query))
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
                parts = urlsplit(feed.url)
                page_feed = feed.model_copy(
                    update={
                        "url": urlunsplit(
                            (parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment)
                        ),
                    }
                )
            else:
                if feed.body is None or not isinstance(feed.body["query"], str):
                    raise ValueError("Snapshot configured query must be text")
                query = feed.body["query"]
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
                page_feed = feed.model_copy(update={"body": {**feed.body, "query": query}})

            escalated = await svc.is_escalated(sid)
            if (self.now() - last_head_check).total_seconds() >= self.interval(source, feed, escalated):
                _http_read = await svc.fetch(
                    source,
                    feed.request_plan(source.limits),
                    key,
                    persist_http_evidence=True,
                    on_start=lambda: self.mark_request_start(source, key),
                )
                head = _http_read.payload
                last_head_check = self.now()
                if isinstance(_http_read, FetchedContent):
                    head_state = _http_read.receipt.storage_record()
                    head_rows = await asyncio.to_thread(parsers.json_items, _http_read.payload, sid)
                    entry_errors.extend(
                        {
                            **error.model_dump(mode="json", exclude_unset=True),
                            "payload_sha256": head_state["payload_sha256"],
                        }
                        for error in head_rows.errors
                    )
                    captured_head = [
                        DiscoveryEnvelope(
                            discovery=entry.discovery,
                            observation=DiscoveryObservation.model_validate(
                                collection_clock(_http_read.receipt.fetched_at).fields("observed_at")
                            ),
                            payload=head,
                        )
                        for entry in head_rows.discoveries
                    ]
                    await self.retain_page(key, source, feed, captured_head, head_state)
            _http_read = await svc.fetch(
                source,
                page_feed.request_plan(source.limits),
                key
                + ":history:"
                + str(page_number if sid == "S09" else offset if sid == "S24" else sha256(page_feed.url)),
                persist_http_evidence=False,
            )
            if isinstance(_http_read, NotModified):
                raise ValueError("Source detail/history returned 304 without retained content") from None
            page = _http_read.payload
            page_state = _http_read.receipt.storage_record()
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
            state["discovery_checkpoint"] = identity(rows[0])
        if sid == "S24":
            pending_wire = state.get("pending_proposals", [])
            if not isinstance(pending_wire, list) or any(
                not isinstance(value, str) or not value for value in pending_wire
            ):
                raise ValueError("Snapshot pending identities require actual nonempty proposal IDs")
            pending = [value for value in pending_wire if isinstance(value, str)]
            returned_ids = {identity(row) for row in rows}
            pending_evidence = {}

            for start in range(0, len(pending), 1000):
                ids = pending[start : start + 1000]
                query = (
                    "query { proposals(first: 1000, where: {id_in: "
                    + json.dumps(ids)
                    + "}) { id title body state start end created scores scores_total space { id } } }"
                )
                _http_read = await svc.fetch(
                    source,
                    feed.model_copy(update={"body": {"query": query}}).request_plan(source.limits),
                    key + ":pending:" + str(start),
                    persist_http_evidence=False,
                )
                extra = _http_read.payload
                if isinstance(_http_read, NotModified):
                    raise ValueError("Source detail/history returned 304 without retained content") from None
                extra_state = _http_read.receipt.storage_record()
                for proposal_id in ids:
                    pending_evidence[proposal_id] = extra_state
                if extra:
                    existing = {r.discovery.external_id for r in rows}
                    extra_rows = await asyncio.to_thread(parsers.json_items, extra, sid)
                    entry_errors.extend(
                        {
                            **error.model_dump(mode="json", exclude_unset=True),
                            "payload_sha256": extra_state["payload_sha256"],
                        }
                        for error in extra_rows.errors
                    )
                    returned_ids.update(identity(row) for row in extra_rows.discoveries)
                    for entry in extra_rows.discoveries:
                        if entry.discovery.external_id not in existing:
                            rows.append(
                                DiscoveryEnvelope(
                                    discovery=entry.discovery,
                                    observation=DiscoveryObservation.model_validate(
                                        collection_clock(_http_read.receipt.fetched_at).fields("observed_at")
                                    ),
                                    payload=extra,
                                )
                            )
            missing = set(pending) - returned_ids
            entry_errors.extend(
                {
                    "reason": "PENDING_PROPOSAL_UNAVAILABLE",
                    "proposal_id": proposal_id,
                    "payload_sha256": pending_evidence[proposal_id].get("payload_sha256"),
                }
                for proposal_id in sorted(missing)
            )
            pending_ids = sorted(
                {
                    r.discovery.proposal_id
                    for r in rows
                    if isinstance(r.discovery, GovernanceDiscovery) and r.discovery.state != "closed"
                }
                | missing
            )
            state["pending_proposals"] = [identity for identity in pending_ids]
        state["entry_errors"] = entry_errors
        return rows, state

    async def acquire(
        self, source: SourceDefinition, index: int, *, due_only: bool = False, escalated: bool = False
    ) -> FeedPollOutcome:
        sid = source.source_id
        key, feed = f"{sid}:{index}", self.feed(source, index)
        async with self.lock(self.acquisition_locks, key):
            state = await self.state(key)
            cadence = SourceCadence.from_document(state)
            if (
                due_only
                and cadence.poll_remaining(now=self.now(), interval_s=self.interval(source, feed, escalated))
                > 0
            ):
                return FeedPollOutcome(items=0, errors=[])
            attempted = self.stamp()
            async with self.state_change(key):
                current = await self.state(key)
                current["last_attempt_at"] = attempted
                await self.save(source, key, current)
            try:
                rows, acquired, kind, snapshot_update = await self._fetch_rows(source, feed, key, state)
                acquired_clock = acquired.get("fetched_at")
                if rows is not None and not isinstance(acquired_clock, str):
                    raise ValueError("Acquired source rows require their actual collection clock")
                fresh_errors = (
                    self.service.admit_entry_errors(acquired["entry_errors"]) if rows is not None else []
                )
                if rows is not None:
                    acquired["entry_errors"] = [
                        error.model_dump(mode="json", exclude_unset=True) for error in fresh_errors
                    ]
                async with self.state_change(key):
                    current = await self.state(key)
                    if rows is not None:
                        if not isinstance(acquired_clock, str):
                            raise ValueError("Acquired source rows have no collection clock")
                        coverage = {
                            field: acquired[field]
                            for field in (
                                "instrument_snapshot_schema_version",
                                "cache_scope",
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
                            acquired_clock,
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
                    pending = await self.pending(source, key, current)
                    if not any(
                        batch.rows for batch in pending.batches if isinstance(batch, SourcePendingBatch)
                    ):
                        current["article_errors"] = {}
                    count_read(current, "reads", ok=True)
                    current.update(
                        status="DEGRADED"
                        if current.get("article_errors")
                        or current.get("entry_errors")
                        or current.get("history_gap")
                        or acquired.get("scan_status") == "GAP_LIMIT"
                        else "HEALTHY",
                        error=None,
                    )
                    if pending.unresolved_quarantines:
                        current.update(status="DEGRADED", error="PENDING_CHECKPOINT_QUARANTINE")
                    if rows is None and not pending.batches:
                        current["last_success_at"] = self.stamp()
                    elif not current.get("last_success_at") and current["status"] == "HEALTHY":
                        current["status"] = "PENDING"
                    self.reports.reconcile(current, sid)
                    if fresh_errors:
                        if not isinstance(acquired_clock, str):
                            raise ValueError("Source entry errors have no collection clock")
                        self.reports.stage(
                            current,
                            SourceReportRequest(
                                subject=SourceReportSubject(
                                    source_id=sid, source_name=source.name, feed_url=feed.url
                                ),
                                errors=fresh_errors,
                                seen_at=acquired_clock,
                            ),
                            self.service.error_reporting_mode,
                        )
                    async with self.service.repo.transaction():
                        await self.save(source, key, current)
                        if snapshot_update is not None:
                            if snapshot_update.source_id != source.source_id:
                                raise ValueError(
                                    "Prepared snapshot conflicts with its actual source operation"
                                )
                            archives = TypedRepository(self.service.repo, INSTRUMENT_BASELINE_ARCHIVES)
                            for archive in snapshot_update.archives:
                                if await archives.get(archive.storage_key) is None:
                                    await archives.put(archive.storage_key, archive, immutable=True)
                            await self.save(
                                source,
                                snapshot_update.scoped_checkpoint_key,
                                snapshot_update.storage_record(),
                            )
                            await self.save(
                                source, snapshot_update.checkpoint_key, snapshot_update.storage_record()
                            )
            except Exception as exc:
                async with self.state_change(key):
                    current = await self.state(key)
                    current.pop("etag", None)
                    current.pop("last_modified", None)
                    count_read(current, "reads", ok=False)
                    current.update(
                        status="DEGRADED",
                        acquisition_error=str(exc),
                        error=str(exc),
                        last_failure_at=self.stamp(),
                    )
                    await self.save(source, key, current)
                await self.drain_reports(sid, key)
                return FeedPollOutcome(items=0, errors=[str(exc)])
            await self.drain_reports(sid, key)
            return FeedPollOutcome(items=0, errors=[])

    async def drain_reports(self, source_id: str, key: str) -> None:
        try:
            await self.reports.drain(source_id, key, self.service.report_error_intent)
        except Exception as error:
            log.error("Source report checkpoint %s remains unresolved after %s", key, type(error).__name__)

    def _empty_body(self, url: str, receipt: FetchEvidence) -> ArticleFailure:
        return ArticleFailure(
            url=url,
            http_status=receipt.http_status,
            payload_sha256=receipt.payload_sha256,
            observed_at=self.stamp(),
            observed_at_precision="COLLECTION_ONLY",
            reason="EMPTY_ARTICLE_BODY",
        )

    async def _article(
        self,
        source: SourceDefinition,
        feed: SourceFeed,
        key: str,
        material: ArticleDiscovery | BoundArticleLink,
    ) -> tuple[FetchedContent, PublisherCMSRecovery | None]:
        """Fetch a page and admit an observed same-entry CMS recovery separately."""
        svc, sid = self.service, source.source_id
        try:
            result = await svc.fetch(
                source,
                HttpFetchPlan(url=material.url, source_limits=source.limits),
                key + ":" + sha256(material.url),
                persist_http_evidence=False,
            )
            if isinstance(result, NotModified):
                raise ValueError("Source detail/history returned 304 without retained content")
            return result, None
        except httpx.HTTPStatusError as exc:
            binding = material.publisher_cms_identity
            fallback = material.publisher_cms_slug_url
            origin = urlsplit(feed.url)
            candidate = urlsplit(fallback) if fallback is not None else None
            if not (
                sid == "S13"
                and exc.response.status_code == 404
                and binding is not None
                and fallback is not None
                and candidate is not None
                and candidate.scheme == origin.scheme
                and candidate.netloc == origin.netloc
                and candidate.path == origin.path.rstrip("/") + "/" + binding.slug + "/"
                and not candidate.query
                and not candidate.fragment
                and fallback != material.url
            ):
                raise
            result = await svc.fetch(
                source,
                HttpFetchPlan(url=fallback, source_limits=source.limits),
                key + ":" + sha256(fallback),
                persist_http_evidence=False,
            )
            if isinstance(result, NotModified):
                raise ValueError("Source detail/history returned 304 without retained content") from None
            if binding not in parsers.publisher_cms_entries(result.payload):
                raise ValueError("Publisher CMS fallback identity/content mismatch") from None
            if urlsplit(result.receipt.response_url).netloc != origin.netloc:
                raise ValueError("Publisher CMS fallback left official host") from None
            return result, PublisherCMSRecovery(
                status="VERIFIED_SAME_CMS_ENTRY",
                canonical_url=material.url,
                resolved_url=fallback,
                original_http_status=404,
                original_response_url=str(exc.response.url),
                original_payload_sha256=sha256(exc.response.content),
                observed_at=self.stamp(),
                publisher_cms_identity=binding,
            )

    async def _item(
        self, source: SourceDefinition, feed: SourceFeed, key: str, kind: FeedKind, original: PendingSourceRow
    ) -> MaterializedSourceObservation | ArticleFailure:
        svc, sid = self.service, source.source_id
        item = original.storage_record()
        item.pop("pending_phase", None)
        material = original.discovery
        article_link = isinstance(original, PendingArticleLink)
        if self.needs_detail(sid, kind):
            if sid == "S09":
                _http_read = await svc.fetch(
                    source,
                    HttpFetchPlan(
                        url="https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode="
                        + material.external_id,
                        source_limits=source.limits,
                    ),
                    key + ":detail:" + material.external_id,
                    persist_http_evidence=False,
                )
                if isinstance(_http_read, NotModified):
                    raise ValueError("Source detail/history returned 304 without retained content") from None
                payload = _http_read.payload
                detail_object = BinanceArticleDetailReply.model_validate(decode_unique_json(payload)).data
                if item.get("published_at") is None and detail_object.publishDate is not None:
                    item.update(
                        parsers.timestamp_fields(detail_object.publishDate, epoch_unit="ms"),
                        published_at_basis="BINANCE_CMS_DETAIL_PUBLISH_DATE",
                    )
                body = parsers.binance_body(detail_object.body)
                item["text"] = body
                if not body.strip():
                    return self._empty_body(material.url, _http_read.receipt)
            else:
                if not isinstance(material, (ArticleDiscovery, BoundArticleLink)):
                    raise ValueError("Article enrichment requires an admitted article discovery")
                _http_read, recovery = await self._article(source, feed, key, material)
                payload = _http_read.payload
                if recovery is not None:
                    item["publisher_cms_recovery"] = recovery.model_dump(mode="json", exclude_unset=True)
                    item["publisher_url_aliases"] = [recovery.canonical_url, recovery.resolved_url]
                    item["url"] = recovery.resolved_url
                detail = await asyncio.to_thread(
                    parsers.article, payload, source_id=sid, page_url=_http_read.receipt.response_url
                )
                if not detail.text.strip():
                    return self._empty_body(material.url, _http_read.receipt)

                detail_record = detail.storage_record()
                if article_link:
                    item.update(detail_record)
                else:
                    item.update(
                        {
                            k: v
                            for k, v in detail_record.items()
                            if v
                            and not (
                                k.startswith("published_")
                                and detail.publication.value is None
                                and item.get("published_at")
                            )
                        }
                    )
            item["raw_ref"], item["raw_sha256"] = svc.raw(
                payload, source_id=sid, item_id=original.item_id, observed_at=original.observed_at
            )
        stored = await svc.ingest(source, item)

        return MaterializedSourceObservation(
            captured=captured_item(sid, item),
            stored=parse_source_item(stored),
        )

    async def process_pending_row(
        self,
        source: SourceDefinition,
        feed: SourceFeed,
        key: str,
        batch: SourcePendingBatch,
        row: PendingSourceRow,
    ) -> SourceRowResult:
        async with self.details:
            try:
                result = await self._item(source, feed, key, batch.kind, row)
                if isinstance(result, ArticleFailure):
                    return FailedSourceRow(
                        original=row,
                        error="Article parse failure (empty visible text): " + row.discovery.url,
                        article_failure=result,
                    )
                observation = MaterializedSourceObservation.model_validate(result)
                return CompletedSourceRow(
                    completion=CompletedPendingRow(original=row, observation=observation)
                )
            except httpx.HTTPStatusError as exc:
                return FailedSourceRow(
                    original=row,
                    error=f"Article HTTP{exc.response.status_code}: " + row.discovery.url,
                    article_failure=ArticleFailure(
                        url=row.discovery.url,
                        http_status=exc.response.status_code,
                        payload_sha256=sha256(exc.response.content),
                        observed_at=self.stamp(),
                        observed_at_precision="COLLECTION_ONLY",
                        reason="PUBLISHER_ARTICLE_NOT_FOUND"
                        if exc.response.status_code == 404
                        else "PUBLISHER_HTTP_FAILURE",
                    ),
                )
            except Exception as exc:
                return FailedSourceRow(original=row, error=str(exc), article_failure=None)

    async def drain(
        self, source: SourceDefinition, index: int, *, force: bool = False, escalated: bool = False
    ) -> FeedPollOutcome:
        key = f"{source.source_id}:{index}"
        feed = self.feed(source, index)
        async with self.lock(self.processing_locks, key):
            async with self.state_change(key):
                state = await self.state(key)
                pending = await self.pending(source, key, state)
                await self.save(source, key, state)
            cadence = SourceCadence.from_document(state)
            if (
                not force
                and cadence.detail_remaining(
                    now=self.now(), interval_s=self.interval(source, feed, escalated)
                )
                > 0
            ):
                return FeedPollOutcome(items=0, errors=[])
            results: list[SourceRowResult] = []
            for batch in pending.batches:
                if isinstance(batch, SourcePendingBatch):
                    results.extend(
                        await asyncio.gather(
                            *(self.process_pending_row(source, feed, key, batch, row) for row in batch.rows)
                        )
                    )
            completed = [result.completion for result in results if isinstance(result, CompletedSourceRow)]
            failures = [result for result in results if isinstance(result, FailedSourceRow)]
            duplicates = [
                completion.original.discovery.external_id
                for completion in completed
                if completion.observation.deduplicated
            ]
            count = sum(not completion.observation.deduplicated for completion in completed)
            errors = [failure.error for failure in failures]
            async with self.state_change(key):
                current = await self.state(key)
                latest = await self.pending(source, key, current)
                latest = latest.link_completions(completed, now=self.now()).complete_rows(completed)
                latest = await self.archives.persist(latest, now=self.now())
                coverage = latest.coverage_commit()
                for batch in coverage.completed:
                    current.update(batch.coverage.model_dump(mode="json", exclude_unset=True))
                    if batch.kind == "instruments":
                        current["cache_scope"] = (
                            batch.coverage.cache_scope
                            if isinstance(batch.coverage, InstrumentCoverage)
                            else None
                        )
                current["pending_batches"] = [batch.storage_record() for batch in coverage.remaining]
                current.pop("instruments", None)
                current["article_errors"] = {
                    failure.article_failure.url: failure.article_failure.model_dump(
                        mode="json", exclude_unset=True
                    )
                    for failure in failures
                    if failure.article_failure is not None
                }
                count_read(current, "detail_reads", ok=True, n=len(completed))
                count_read(current, "detail_reads", ok=False, n=len(failures))
                if duplicates:
                    cache = SourceDuplicateCache.from_document(current)
                    current["duplicate_ids"] = [
                        identity for identity in (cache.duplicate_ids + duplicates)[-1000:]
                    ]
                if latest.unresolved_quarantines:
                    errors.append("PENDING_CHECKPOINT_QUARANTINE")
                if errors:
                    current.update(
                        status="DEGRADED",
                        error="; ".join(errors),
                        last_failure_at=self.stamp(),
                        detail_retry_at=self.stamp(
                            self.now() + timedelta(seconds=self.interval(source, feed, escalated))
                        ),
                    )
                    if failures:
                        current.pop("etag", None)
                        current.pop("last_modified", None)
                else:
                    current.pop("detail_retry_at", None)
                    degradation = SourceDegradation.from_document(current)
                    current.update(
                        status="DEGRADED" if degradation.blocked else "HEALTHY",
                        error="X_HISTORY_GAP_LIMIT" if degradation.scan_status == "GAP_LIMIT" else None,
                        last_success_at=self.stamp(),
                    )
                await self.save(source, key, current)
            return FeedPollOutcome(items=count, errors=errors)

    async def poll_once(
        self,
        source_id: str,
        *,
        due_only: bool = False,
        escalated: bool = False,
    ) -> SourcePollOutcome:
        source = self.service.sources[source_id]
        if not source.feeds:
            return UnconfiguredSource(source_id=source_id, items=0, errors=[])

        async def one_feed(index: int) -> FeedPollOutcome:
            acquired = await self.acquire(source, index, due_only=due_only, escalated=escalated)
            processed = await self.drain(source, index, force=not due_only, escalated=escalated)
            return FeedPollOutcome(items=processed.items, errors=acquired.errors + processed.errors)

        processed = await asyncio.gather(*(one_feed(i) for i in range(len(source.feeds))))
        errors = [error for result in processed for error in result.errors]
        states = [await self.state(f"{source_id}:{i}") for i in range(len(source.feeds))]
        return PolledSource(
            source_id=source_id,
            items=sum(result.items for result in processed),
            status="DEGRADED"
            if errors or any(state.get("status") == "DEGRADED" for state in states)
            else "HEALTHY",
            errors=errors,
        )

    async def loop(self, source: SourceDefinition, stop: asyncio.Event) -> None:
        sid = source.source_id
        while not stop.is_set():
            escalated = await self.service.is_escalated(sid)
            next_wake = 15.0
            for index in range(len(source.feeds)):
                key, feed = f"{sid}:{index}", self.feed(source, index)
                state = await self.state(key)
                remaining = SourceCadence.from_document(state).poll_remaining(
                    now=self.now(), interval_s=self.interval(source, feed, escalated)
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

    async def run(self, stop: asyncio.Event) -> None:
        loops = [asyncio.create_task(self.loop(source, stop)) for source in self.service.sources.values()]
        try:
            if loops:
                await asyncio.gather(*loops)
        finally:
            for loop_task in loops:
                if not loop_task.done():
                    loop_task.cancel()
            await asyncio.gather(*loops, return_exceptions=True)
            tasks = list(self.active_acquisitions.values()) + list(self.active_details.values())
            for work_task in tasks:
                if not work_task.done():
                    work_task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.active_acquisitions.clear()
            self.active_details.clear()
