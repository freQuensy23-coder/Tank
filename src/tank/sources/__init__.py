"""Durable raw-first polling for the 34 logical information sources.

Filtered items have a null relevance decision until FILTER_ITEM completes.
The classifier replaces the unknown decision with canonical SEND/SKIP and
persists probability/model provenance; source ingestion never guesses relevance.
"""

import asyncio
import re
import ssl
from contextlib import suppress
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

import certifi
import httpx

import tank.sources.parsers as parsers
from tank.ops_progress import item_progress
from tank.sources.capture_fetch import capture_fetch, is_exchange_url
from tank.sources.configuration import HttpFetchPlan, SourceConfiguration, SourceDefinition
from tank.sources.rate_limits import shared_request_budgets
from tank.sources.xsensor import XSensor
from tank.util import TimeInput, iso, parse_time, sha256, utcnow


def tls_context():
    """The TLS hello of Python's standard HTTPS client (http.client sets post_handshake_auth).

    WordPress.com's bot check (S12 blog.kraken.com) answers httpx's default hello with
    403/429 and the standard one with the feed.
    """
    context = ssl.create_default_context(cafile=certifi.where())
    context.post_handshake_auth = True
    return context


class SourceService:
    def __init__(self, repo, settings, *, http=None):
        cfg = SourceConfiguration.model_validate(settings.load_config("sources"))
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
        self.owns_http = http is None
        self.xsensor = XSensor(self.http, getattr(settings, "apify_api_token", None))
        self.configuration = cfg
        self.sources = {source.source_id: source for source in cfg.sources}
        self.source_host_limits = {}
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
            a["handle"].lower().lstrip("@"): a for a in settings.load_config("registry").get("x_accounts", [])
        }
        self.locks = {}
        try:
            self.api_limits = settings.load_config("api_limits").get("limits", [])
        except FileNotFoundError:
            self.api_limits = []
        try:
            self.thresholds = settings.load_config("thresholds")
        except FileNotFoundError:
            self.thresholds = {}
        self.escalated_interval_s = self.thresholds.get("escalated_interval_s", 15)
        from tank.sources.polling import SourcePoller

        self.poller = SourcePoller(self)

    def now(self) -> datetime:
        return utcnow()

    def stamp(self, value: TimeInput | None = None) -> str:
        return iso(self.now() if value is None else value)

    def raw(self, payload, *, source_id, item_id, observed_at):
        """Store the raw payload of one captured item (02 §7.2); nothing else is stored raw."""
        from tank.sources.raw_store import write_raw_item

        return write_raw_item(self.settings.data_dir, source_id, item_id, observed_at, payload)

    async def acquire_budget(self, feed: HttpFetchPlan) -> None:
        host = urlsplit(feed.url).hostname or ""

        provider = "sec.gov" if host == "sec.gov" or host.endswith(".sec.gov") else host
        weight = feed.request_weight
        for rule in self.api_limits:
            if rule.get("provider") != provider or is_exchange_url(feed.url):
                continue
            if rule.get("tank_max_requests_per_s") is not None:
                await shared_request_budgets.acquire(provider, rule["tank_max_requests_per_s"], 1, host=host)
            if rule.get("tank_max_weight_per_min") is not None:
                await shared_request_budgets.acquire(
                    provider, rule["tank_max_weight_per_min"], 60, weight=weight, host=host
                )
        capacities = [
            value
            for value in [
                feed.requests_per_minute,
                feed.source_limits.rate_per_min if feed.source_limits is not None else None,
                self.source_host_limits.get(host),
            ]
            if value is not None
        ]
        per_min = min(capacities) if capacities else None
        if per_min is not None:
            await shared_request_budgets.acquire(host, per_min, 60, host=host)

    async def fetch(self, feed: HttpFetchPlan, key, *, on_start=None):
        state = deepcopy(await self.repo.get("ops.source_checkpoints", key) or {})
        headers = dict(feed.headers)
        if state.get("etag") and not state.get("article_errors") and not state.get("entry_errors"):
            headers["If-None-Match"] = state["etag"]
        if state.get("last_modified") and not state.get("article_errors") and not state.get("entry_errors"):
            headers["If-Modified-Since"] = state["last_modified"]
        error = None
        for attempt in range(4):
            try:
                await self.acquire_budget(feed)
                if on_start is not None:
                    await on_start()
                if is_exchange_url(feed.url):
                    response = await capture_fetch(
                        self.repo,
                        feed.method,
                        feed.url,
                        headers=headers,
                        body=feed.body if feed.method.upper() == "POST" else None,
                        source_id=key.split(":", 1)[0],
                    )
                else:
                    response = await self.http.request(
                        feed.method,
                        feed.url,
                        headers=headers,
                        json=feed.body if feed.method.upper() == "POST" else None,
                        data=feed.form if feed.method.upper() == "POST" else None,
                    )
                if response.status_code == 304:
                    return None, state
                payload = response.content
                received_at = iso()
                digest = sha256(payload)

                if re.fullmatch(r"S\d{2}:\d+", key):
                    async with self.poller.lock(self.poller.state_locks, key):
                        evidence = await self.poller.state(key)
                        evidence.update(
                            payload_sha256=digest,
                            fetched_at=received_at,
                            http_status=response.status_code,
                            response_url=str(response.url),
                        )
                        await self.poller.save(key, evidence)
                response.raise_for_status()
                state.update(
                    etag=response.headers.get("etag"),
                    last_modified=response.headers.get("last-modified"),
                    payload_sha256=digest,
                    fetched_at=received_at,
                    http_status=response.status_code,
                    response_url=str(response.url),
                )
                return payload, state
            except (httpx.HTTPError, TimeoutError) as exc:
                error = exc
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404:
                    raise
                if attempt < 3:
                    delay = 2 ** (attempt + 1)
                    if isinstance(exc, httpx.HTTPStatusError):
                        retry = exc.response.headers.get("retry-after")
                        if retry:
                            try:
                                delay = max(delay, float(retry))
                            except ValueError:
                                with suppress(ValueError, TypeError):
                                    delay = max(
                                        delay,
                                        (parse_time(parsers.published(retry)) - utcnow()).total_seconds(),
                                    )
                    await asyncio.sleep(delay)
        if error is None:
            raise RuntimeError("Source fetch exhausted retries without a recorded failure")
        raise error

    async def captured(self, **filters):
        """The stored item matching (source_id, external_id) or content_hash (03 §2.9)."""
        rows = await self.repo.list("ingest.source_items", filters=filters, limit=1)
        return rows[0] if rows else None

    async def observe_known(self, source_id, external_id, feed):
        """True when the item is already captured; a sighting in another feed counts once (03 §3.1)."""
        item = await self.captured(source_id=source_id, external_id=str(external_id))
        if not item:
            return False
        if feed not in item.get("observed_feeds", [item.get("feed")]):
            async with self.repo.lock("ingest-dedup"), self.repo.transaction():
                await self._count_observation(item["item_id"], feed)
        return True

    async def _count_observation(self, item_id, feed):
        old = await self.repo.get("ingest.source_items", item_id)
        old["observation_count"] = old.get("observation_count", 1) + 1
        old["observed_feeds"] = old.get("observed_feeds", [old.get("feed")]) + [feed]
        await self.repo.put("ingest.source_items", item_id, old)
        return old

    async def ingest(self, source: SourceDefinition, item):
        """Store one captured item whose raw payload is already at item["raw_ref"]."""
        sid = source.source_id
        identity = item.get("external_id")
        if isinstance(identity, bool) or not isinstance(identity, (str, int)) or not str(identity).strip():
            raise ValueError("Source entry has no valid external identity")
        body = " ".join(
            (item.get("title", "") + " " + item.get("text", "") + " " + (item.get("quoted") or "")).split()
        )

        content_hash = (
            sha256(body)
            if body
            else sha256(
                {"source_id": sid, "external_id": item["external_id"], "media": item.get("media", [])}
            )
        )
        external = str(item["external_id"])
        feed = item.get("feed") or sid
        item_id = item["item_id"]
        async with self.repo.lock("ingest-dedup"), self.repo.transaction():
            existing = await self.captured(source_id=sid, external_id=external) or await self.captured(
                content_hash=content_hash
            )
            if not existing and not body and sid != "S29":
                raise ValueError("Source entry has no normalized content")
            if existing:
                if existing["item_id"] == item_id:
                    return existing

                Path(item["raw_ref"]).unlink(missing_ok=True)
                if feed in existing.get("observed_feeds", [existing.get("feed")]):
                    return existing
                return await self._count_observation(existing["item_id"], feed)
            observed_at = item.get("observed_at") or iso()
            raw_ref, raw_hash = item["raw_ref"], item["raw_sha256"]
            account = self.accounts.get(
                (item.get("monitored_author") or item.get("reposted_by") or item.get("author", ""))
                .lower()
                .lstrip("@"),
                {},
            )
            role = account.get("role")
            from tank.sources.organization import resolve_author_organization

            author_organization = resolve_author_organization(source, item, self.accounts)
            from tank.sources.policy import is_filtered_source

            filtered = is_filtered_source(sid, item.get("subtype"), role)
            priority = (
                2
                if sid in {"S30", "S31", "S32", "S33", "S34"}
                or (sid == "S25" and item.get("subtype") == "commits")
                or (sid == "S29" and role == "OTHER")
                else 1
            )
            record = dict(
                item,
                item_id=item_id,
                source_id=sid,
                feed=feed,
                source=source.name,
                source_name=source.name,
                author=item.get("author") or source.name,
                author_role=role,
                monitored_accounts=[
                    item.get("monitored_author") or item.get("reposted_by") or item.get("author")
                ]
                if sid == "S29"
                else [],
                original_author_role=self.accounts.get(item.get("author", "").lower().lstrip("@"), {}).get(
                    "role"
                ),
                author_organization=author_organization,
                deferrable=priority == 2,
                external_id=external,
                raw_ref=raw_ref,
                raw_sha256=raw_hash,
                content_hash=content_hash,
                observed_at=observed_at,
                observed_at_precision="COLLECTION_ONLY",
                published_at_precision=item.get("published_at_precision")
                or (None if item.get("published_at") else "COLLECTION_ONLY"),
                published_prec="BUCKET"
                if str(item.get("published_at_precision", "")).startswith("BUCKET")
                or item.get("published_at_precision") == "DAY"
                else "EXACT"
                if item.get("published_at_precision") in {"EXACT", "SECOND"}
                else "COLLECTION_ONLY",
                published_at_bucket_size_s=86400
                if item.get("published_at_precision") in {"DAY", "BUCKET(1d)"}
                else item.get("published_at_bucket_size_s"),
                published_prec_reason=None
                if item.get("published_at")
                and item.get("published_at_precision") in {"EXACT", "SECOND", "DAY", "BUCKET(1d)"}
                else item.get("published_prec_reason") or "INCOMPLETE",
                published_at_missing_reason=None
                if item.get("published_at")
                else item.get("published_at_missing_reason")
                or item.get("published_prec_reason")
                or "INCOMPLETE",
                published_at_reason=None
                if item.get("published_at")
                else item.get("published_at_reason") or item.get("published_prec_reason") or "INCOMPLETE",
                source_class=source.source_class,
                relevance={"decision": None, "pending_reason": "AWAITING_FILTER"}
                if filtered
                else {"decision": "ALWAYS"},
                extraction_status="PENDING",
                observation_count=1,
                config_version=self.settings.config_version,
                code_version=self.settings.code_version,
            )
            from tank.time_precision import collection_clock

            record.update(collection_clock(record["observed_at"]).fields("observed_at"))
            await self.repo.put("ingest.source_items", item_id, record)
            await item_progress(
                self.repo,
                self.settings,
                item_id,
                "RECEIVED",
                "Ожидает проверки Jev" if filtered else "Jev не требуется; ожидает маршрутизации к агенту",
            )
            if not filtered:
                await item_progress(
                    self.repo, self.settings, item_id, "JEV_BYPASSED", "Источник проходит напрямую к агенту"
                )
            await self.repo.enqueue(
                "FILTER_ITEM" if filtered else "ROUTE_ITEM",
                {"item_id": item_id},
                item_id,
                priority=priority,
                max_attempts=2,
            )
            return record

    async def poll_once(self, source_id, *, due_only=False, escalated=False):
        return await self.poller.poll_once(source_id, due_only=due_only, escalated=escalated)

    def _filing_pattern(self):
        """One compiled pattern of registry names per configuration version (S02 filter)."""
        cached = getattr(self, "_filing_cache", None)
        if cached and cached[0] == self.settings.config_version:
            return cached[1]
        registry = self.settings.load_config("registry")
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
        for token in registry.get("tokens", []):
            candidates.extend([token.get("name", ""), token.get("symbol", "")])
        for entity in registry.get("entities", []):
            if entity.get("kind") == "REGULATOR":
                continue
            candidates.extend(entity.get("names", []) or [entity.get("name", "")])
            candidates.extend(
                alias for alias in entity.get("aliases", []) if '"' not in alias and " only " not in alias
            )
        names = sorted(
            {n.casefold() for n in candidates if n and n.casefold() not in ambiguous}, key=len, reverse=True
        )
        pattern = re.compile(r"(?<!\w)(?:" + "|".join(re.escape(n) for n in names) + r")(?!\w)")
        self._filing_cache = (self.settings.config_version, pattern)
        return pattern

    def filing_matches(self, item):
        value = (item.get("title", "") + " " + item.get("text", "")).lower()
        if re.search(r"\blighter\b", value) and re.search(r"\blit\b", value):
            return True
        if re.search(r"\bplasma\b", value) and re.search(r"\bxpl\b", value):
            return True
        if self._filing_pattern().search(value):
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
                        "role": account.get("role"),
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
