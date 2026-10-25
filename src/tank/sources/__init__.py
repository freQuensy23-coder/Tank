"""Durable raw-first polling for the 34 logical information sources.

Filtered items have a null relevance decision until FILTER_ITEM completes.
The classifier replaces the unknown decision with canonical SEND/SKIP and
persists probability/model provenance; source ingestion never guesses relevance.
"""

import asyncio
import re
import ssl
from collections.abc import Mapping
from datetime import datetime
from functools import cached_property
from urllib.parse import urlsplit

import certifi
import httpx
from pydantic import JsonValue

from tank.contracts.configuration import Thresholds
from tank.contracts.http_policy import HttpRefused
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.source_items import SourceText, captured_item
from tank.contracts.sources import (
    HttpFetchPlan,
    SourceAccountRegistryView,
    SourceApiLimitsConfiguration,
    SourceConfiguration,
    SourceDefinition,
    SourceFilingRegistryView,
)
from tank.http_admission import ConfiguredHttpAdmission
from tank.ops_progress import RepositorySourceProgress
from tank.request_budget import BudgetConstraint, shared_request_budgets
from tank.sources.capture_fetch import capture_fetch, is_exchange_url
from tank.sources.fetch import FetchEvidence, FetchResult, HttpCheckpointProjection, SourceHttpReader
from tank.sources.http_client import RequestStarted, SourceHttpTransport
from tank.sources.ingestion import SourceIngestion
from tank.sources.raw_store import RawItemStorage
from tank.sources.xsensor import XSensor
from tank.util import iso, parse_iso_time, parse_time, utcnow


def tls_context() -> ssl.SSLContext:
    """The TLS hello of Python's standard HTTPS client (http.client sets post_handshake_auth).

    WordPress.com's bot check (S12 blog.kraken.com) answers httpx's default hello with
    403/429 and the standard one with the feed.
    """
    context = ssl.create_default_context(cafile=certifi.where())
    context.post_handshake_auth = True
    return context


class SourceService:
    def __init__(self, repo, settings, *, versions: ResolvedVersionIdentity, http=None) -> None:
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        cfg = SourceConfiguration.model_validate(settings.load_config("sources"))
        registry = settings.load_config("registry")
        account_registry = SourceAccountRegistryView.model_validate(
            {name: registry[name] for name in ("x_accounts",) if name in registry}
        )
        self.api_limits = SourceApiLimitsConfiguration.model_validate(settings.load_config("api_limits"))
        self.http_admission = ConfiguredHttpAdmission(self.api_limits, self.versions, shared_request_budgets)
        self.thresholds = Thresholds.model_validate(settings.load_config("thresholds"))
        self.escalated_interval_s = self.thresholds.escalated_interval_s
        self.repo = repo
        self.settings = settings
        self.http = (
            http
            if http is not None
            else httpx.AsyncClient(
                timeout=20,
                follow_redirects=True,
                headers={"User-Agent": settings.user_agent},
                verify=tls_context(),
            )
        )
        self.transport = SourceHttpTransport(self.http, self._admit_request)
        self.owns_http = http is None
        self.xsensor = XSensor(self.http, getattr(settings, "apify_api_token", None))
        self.configuration = cfg
        self.sources = {source.source_id: source for source in cfg.sources}
        self.source_host_limits: dict[str, int] = {}
        for source in cfg.sources:
            per_min = source.limits.rate_per_min
            if per_min is None:
                continue
            for feed in source.feeds:
                for url in [feed.url, feed.official_index_url]:
                    host = urlsplit(url).hostname if url is not None else None
                    if host:
                        self.source_host_limits[host] = min(
                            per_min, self.source_host_limits.get(host, per_min)
                        )
        self.accounts = {
            account.handle.lower().lstrip("@"): account for account in account_registry.x_accounts
        }
        from tank.sources.polling import SourcePoller

        self.poller = SourcePoller(self)

    def now(self) -> datetime:
        return utcnow()

    def stamp(self, value: datetime | str | None = None) -> str:
        return iso(parse_iso_time(self.now() if value is None else value))

    def raw(
        self, payload: bytes, *, source_id: str, item_id: str, observed_at: datetime | str
    ) -> tuple[str, str]:
        """Store the raw payload of one captured item (02 §7.2); nothing else is stored raw."""
        from tank.sources.raw_store import write_raw_item

        return write_raw_item(self.settings.data_dir, source_id, item_id, observed_at, payload)

    async def _admit_request(self, feed: HttpFetchPlan, request: httpx.Request) -> None:
        await self.acquire_budget(feed, request=request)

    async def acquire_budget(self, feed: HttpFetchPlan, *, request: httpx.Request | None = None) -> None:
        actual = self.http.build_request(feed.method, feed.url) if request is None else request
        host = actual.url.host
        if not host:
            raise ValueError("Source fetch requires an absolute request destination")
        capacities = [
            value
            for value in [
                feed.requests_per_minute,
                feed.source_limits.rate_per_min if feed.source_limits is not None else None,
                self.source_host_limits.get(host),
            ]
            if value is not None
        ]
        constraints = (
            (BudgetConstraint(key=host, capacity=float(min(capacities)), window_s=60, weight=1, host=host),)
            if capacities
            else ()
        )
        if is_exchange_url(str(actual.url)):
            if constraints:
                await shared_request_budgets.acquire_many(constraints)
            return
        result = await self.http_admission.admit_with_constraints(actual, constraints)
        if isinstance(result, HttpRefused):
            raise ValueError("Source HTTP admission refused: " + result.reason)

    async def _send_fetch(
        self,
        feed: HttpFetchPlan,
        headers: Mapping[str, str],
        key: str,
        on_start: RequestStarted | None,
    ) -> httpx.Response:
        if is_exchange_url(feed.url):
            await self.acquire_budget(feed)
            if on_start is not None:
                await on_start()
            return await capture_fetch(
                self.repo,
                feed.method,
                feed.url,
                headers=headers,
                body=feed.body if feed.method == "POST" else None,
                source_id=key.split(":", 1)[0],
            )
        return await self.transport.request(feed, headers, on_start)

    async def _record_fetch(self, key: str, receipt: FetchEvidence) -> None:
        async with self.poller.lock(self.poller.state_locks, key):
            evidence = await self.poller.state(key)
            evidence.update(receipt.storage_record())
            await self.poller.save(key, evidence)

    async def fetch(
        self,
        feed: HttpFetchPlan,
        key: str,
        *,
        on_start: RequestStarted | None = None,
    ) -> FetchResult:
        checkpoint = await self.repo.get("ops.source_checkpoints", key)
        cache = HttpCheckpointProjection.from_document(checkpoint)
        return await SourceHttpReader(self._send_fetch, self._record_fetch, self.now).read(
            feed, key, cache, on_start
        )

    def _ingestion(self) -> SourceIngestion:
        return SourceIngestion(
            self.repo,
            self.versions,
            self.accounts,
            RawItemStorage(self.settings.data_dir),
            RepositorySourceProgress(self.repo, self.settings),
        )

    async def captured(self, *, source_id: str, external_id: str) -> dict[str, JsonValue] | None:
        """Expose the explicit durable-record boundary for captured source lookups."""
        item = await self._ingestion().captured(source_id, external_id)
        return item.storage_record() if item is not None else None

    async def observe_known(self, source_id: str, external_id: str | int, feed: str) -> bool:
        """Normalize the external identifier before the typed observation workflow."""
        if isinstance(external_id, bool) or not str(external_id).strip():
            raise ValueError("Source entry has no valid external identity")
        return await self._ingestion().observe_known(source_id, str(external_id), feed)

    async def ingest(self, source: SourceDefinition, item: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        """Decode the flat capture wire and serialize the typed ingestion result."""
        captured = captured_item(source.source_id, item)
        stored = await self._ingestion().ingest(source, captured)
        return stored.storage_record()

    async def poll_once(self, source_id, *, due_only=False, escalated=False):
        return await self.poller.poll_once(source_id, due_only=due_only, escalated=escalated)

    @cached_property
    def _filing_registry_pattern(self) -> re.Pattern[str]:
        """One immutable registry-label pattern for the service's captured configuration."""
        registry = SourceFilingRegistryView.from_document(self.settings.load_config("registry"))
        ambiguous = {"near", "world", "curve", "quant", "cow", "lit", "lighter", "plasma"}
        candidates = [
            "Curve Finance",
            "Curve DAO",
            "CoW Protocol",
            "CoW Swap",
            "CoW DAO",
            "Lighter.xyz",
            "Lighter DEX",
            "Lighter exchange",
            "Quant Network",
            "Overledger",
            "NEAR Protocol",
            "NEAR Foundation",
            "Plasma chain",
            "Plasma network",
            "Worldcoin",
            "World Network",
            "World App",
            "World Chain",
            "World ID",
            "Tools for Humanity",
        ]
        for token in registry.tokens:
            candidates.extend([token.name, token.symbol])
        for entity in registry.entities:
            if entity.kind == "REGULATOR":
                continue
            candidates.extend(entity.names)
            if not entity.names and entity.name is not None:
                candidates.append(entity.name)
            candidates.extend(alias for alias in entity.aliases if '"' not in alias and " only " not in alias)
        names = sorted(
            {n.casefold() for n in candidates if n and n.casefold() not in ambiguous}, key=len, reverse=True
        )
        pattern = re.compile(r"(?<!\w)(?:" + "|".join(re.escape(n) for n in names) + r")(?!\w)")
        return pattern

    def filing_matches(self, item: SourceText) -> bool:
        value = (item.title + " " + item.text).lower()
        if re.search(r"\blighter\b", value) and re.search(r"\blit\b", value):
            return True
        if re.search(r"\bplasma\b", value) and re.search(r"\bxpl\b", value):
            return True
        if self._filing_registry_pattern.search(value):
            return True
        crypto_context = re.search(
            r"\b(?:crypto(?:currency|currencies|assets?)?|digital\s+assets?|bitcoin|ether(?:eum)?|blockchain|tokens?|tokenized|stablecoins?|defi|decentralized)\b",
            value,
        )
        return bool(re.search(r"\b(?:trust|etf)\b", value)) and bool(crypto_context)

    async def check(self, *, probe=False, check_accounts=False):
        if probe:
            from tank.sources.probe import probe_feeds

            results = await probe_feeds(self)
        else:
            results = await self._saved_health()
        if check_accounts:
            results.extend(await self._account_checks())
        return results

    async def _saved_health(self):
        results = []
        for sid, source in self.sources.items():
            states = [
                await self.repo.get("ops.source_checkpoints", f"{sid}:{i}")
                for i, _ in enumerate(source.feeds)
            ]
            results.append(
                {
                    "source_id": sid,
                    "configured": bool(source.feeds),
                    "states": states,
                    "status": "DEGRADED"
                    if any(s and s.get("status") == "DEGRADED" for s in states)
                    else "HEALTHY"
                    if states and all(state and state.get("status") == "HEALTHY" for state in states)
                    else "UNVERIFIED",
                }
            )
        return results

    async def _account_checks(self):
        results = []
        for handle, account in self.accounts.items():
            try:
                response = await self.http.get(
                    "https://publish.twitter.com/oembed", params={"url": f"https://x.com/{handle}"}
                )
                results.append(
                    {
                        "handle": handle,
                        "exists": response.status_code == 200,
                        "identity_status": "REQUIRES_OWNER_EVIDENCE",
                        "role": account.role,
                    }
                )
            except httpx.HTTPError as exc:
                results.append({"handle": handle, "exists": None, "error": str(exc)})
        return results

    async def close(self):
        if self.owns_http:
            await self.http.aclose()

    async def is_escalated(self, source_id, *, now=None):
        moment = parse_time(now) if now is not None else utcnow()
        rows = await self.repo.list("ops.source_escalations", filters={"source_id": source_id}, limit=10000)
        return any(
            row.get("status", "ACTIVE") == "ACTIVE"
            and not row.get("confirmed_at")
            and not row.get("cancelled_at")
            and row.get("starts_at") is not None
            and row.get("expires_at") is not None
            and parse_time(row["starts_at"]) <= moment < parse_time(row["expires_at"])
            for row in rows
        )

    async def control_once(self, source_id):
        """Re-read durable controls independently of the normal HTTP poll cadence."""
        return await self.poll_once(source_id, due_only=True, escalated=await self.is_escalated(source_id))

    async def run(self, stop):
        tasks = [asyncio.create_task(self.poller.run(stop))]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
