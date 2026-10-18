"""Durable raw-first polling for the 34 logical information sources.

Filtered items have a null relevance decision until FILTER_ITEM completes.
The classifier replaces the unknown decision with canonical SEND/SKIP and
persists probability/model provenance; source ingestion never guesses relevance.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
import zstandard

from tank.util import canonical_json, iso, new_id, parse_time, sha256, utcnow

from . import parsers
from .browser import BrowserReader
from .rate_limits import shared_request_budgets
from .xsensor import XSensor


class SourceService:
    def __init__(self, repo, settings, *, http=None):
        self.repo = repo
        self.settings = settings
        self.http = http or httpx.AsyncClient(
            timeout=20, follow_redirects=True, headers={"User-Agent": settings.user_agent}
        )
        self.owns_http = http is None
        self.browser = BrowserReader(settings)
        self.xsensor = XSensor(settings, raw_capture=self.raw)
        cfg = settings.load_config("sources")
        self.sources = {s["source_id"]: s for s in cfg["sources"]}
        self.source_host_limits = {}
        for source in cfg["sources"]:
            per_min = source.get("limits", {}).get("rate_per_min")
            if per_min is None:
                continue
            for feed in source.get("feeds", []):
                for url in [feed["url"], feed.get("official_index_url", "")]:
                    host = urlsplit(url).hostname
                    if host:
                        self.source_host_limits[host] = min(
                            per_min, self.source_host_limits.get(host, per_min)
                        )
        self.accounts = {
            a["handle"].lower().lstrip("@"): a for a in settings.load_config("registry").get("x_accounts", [])
        }
        self.locks = {}
        self.dtcc_public_keys = {}
        try:
            self.api_limits = settings.load_config("api_limits").get("limits", [])
        except FileNotFoundError:
            self.api_limits = []
        try:
            self.thresholds = settings.load_config("thresholds")
        except FileNotFoundError:
            self.thresholds = {}
        self.escalated_interval_s = self.thresholds.get("escalated_interval_s", 15)

    def raw(self, payload):
        digest = sha256(payload)
        path = Path(self.settings.data_dir) / "raw" / "sources" / digest[:2] / (digest + ".zst")
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            temporary = path.with_suffix("." + new_id("raw") + ".tmp")
            temporary.write_bytes(zstandard.ZstdCompressor(level=3).compress(payload))
            temporary.replace(path)
        return str(path), digest

    async def acquire_budget(self, feed):
        host = urlsplit(feed["url"]).hostname or ""
        provider = (
            "sec.gov"
            if host == "sec.gov" or host.endswith(".sec.gov")
            else "binance.spot"
            if host == "api.binance.com"
            else "binance.usdm"
            if host == "fapi.binance.com"
            else "coinbase"
            if host.endswith("coinbase.com")
            else "kraken"
            if host.endswith("kraken.com")
            else host
        )
        weight = feed.get("request_weight", 1)
        if provider in {"binance.spot", "binance.usdm"}:
            from tank.market.rate_gate import binance_request_weight

            parts = urlsplit(feed["url"])
            weight = binance_request_weight(host, parts.path, dict(parse_qsl(parts.query)))
        for rule in self.api_limits:
            if rule.get("provider") != provider:
                continue
            if rule.get("tank_max_requests_per_s") is not None:
                await shared_request_budgets.acquire(provider, rule["tank_max_requests_per_s"], 1)
            if rule.get("tank_max_weight_per_min") is not None:
                await shared_request_budgets.acquire(
                    provider, rule["tank_max_weight_per_min"], 60, weight=weight
                )
        capacities = [
            value
            for value in [
                feed.get("requests_per_minute"),
                feed.get("source_limits", {}).get("rate_per_min"),
                self.source_host_limits.get(host),
            ]
            if value is not None
        ]
        per_min = min(capacities) if capacities else None
        if per_min is not None:
            await shared_request_budgets.acquire(host, per_min, 60)

    async def fetch(self, feed, key):
        state = await self.repo.get("ops.source_checkpoints", key) or {}
        headers = dict(feed.get("headers", {}))
        if state.get("etag") and not state.get("article_errors"):
            headers["If-None-Match"] = state["etag"]
        if state.get("last_modified") and not state.get("article_errors"):
            headers["If-Modified-Since"] = state["last_modified"]
        error = None
        for attempt in range(4):
            try:
                await self.acquire_budget(feed)
                response = await self.http.request(
                    feed.get("method", "GET"),
                    feed["url"],
                    headers=headers,
                    json=feed.get("body") if feed.get("method", "GET").upper() == "POST" else None,
                )
                if response.status_code == 304:
                    return None, state
                payload = response.content
                ref, digest = self.raw(payload)
                response.raise_for_status()
                state.update(
                    etag=response.headers.get("etag"),
                    last_modified=response.headers.get("last-modified"),
                    raw_ref=ref,
                    raw_sha256=digest,
                    fetched_at=iso(),
                    http_status=response.status_code,
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
                                try:
                                    delay = max(
                                        delay,
                                        (parse_time(parsers.published(retry)) - utcnow()).total_seconds(),
                                    )
                                except (ValueError, TypeError):
                                    pass
                    await asyncio.sleep(delay)
        raise error

    async def resolve_dtcc_bootstrap(self, feed, key):
        """Resolve the publisher's current component chunk from its own manifest.

        Never fall back to a stale content-hashed URL after a deployment change.
        """
        from urllib.parse import urljoin

        page_url = feed["official_index_url"]
        page, _ = await self.fetch({"url": page_url}, key + ":bootstrap:index")
        tree = parsers.HTMLParser(page)
        urls = [urljoin(page_url, n.attributes["src"]) for n in tree.css("script[src]")]
        urls = [url for url in urls if urlsplit(url).netloc == urlsplit(page_url).netloc]
        webpack_url = next((url for url in urls if "/webpack-" in url), None)
        if not webpack_url:
            raise ValueError("DTCC official webpack manifest missing")
        manifest, _ = await self.fetch({"url": webpack_url}, key + ":bootstrap:manifest")
        manifest = manifest.decode()
        registry = None
        for index, url in enumerate(urls):
            if url == webpack_url:
                continue
            script, _ = await self.fetch({"url": url}, key + ":bootstrap:registry:" + str(index))
            candidate = script.decode()
            match = re.search(r"PressReleaseSearchModuleBlock:.*?Promise\.all\(\[(.*?)\]\)", candidate)
            if match:
                registry = match.group(1)
                break
        if registry is None:
            raise ValueError("DTCC official pressroom component manifest changed")
        ids = re.findall(r"\.e\((\d+)\)", registry)
        # Current Next.js manifests encode anonymous chunks as id.hash.js.
        separator = "." if '+"."+({' in manifest else "-"
        for chunk_id in reversed(ids):
            match = re.search(r"(?:\b|,)" + chunk_id + r':"([a-f0-9]+)"', manifest)
            if not match:
                continue
            url = urljoin(page_url, "/_next/static/chunks/" + chunk_id + separator + match.group(1) + ".js")
            script, _ = await self.fetch({"url": url}, key + ":bootstrap:component:" + chunk_id)
            match = re.search(r'apiKey:"([^"\\]+)"', script.decode())
            if match:
                return match.group(1)
        raise ValueError("DTCC public query configuration missing from current component")

    async def dtcc_payload(self, feed, key, state, *, _refresh=False):
        """Read DTCC's own pressroom search API, newest-first through checkpoint.

        The publisher exposes its anonymous search-only token in its public JS.
        It is fetched dynamically and never copied to configuration or reports.
        """
        script_url = feed["bootstrap_script_url"]
        cached = self.dtcc_public_keys.get(script_url)
        if not cached or (utcnow() - cached[1]).total_seconds() > 3600:
            if feed.get("bootstrap_discovery"):
                public_key = await self.resolve_dtcc_bootstrap(feed, key)
            else:
                # Explicit fixture/legacy mode only; production config resolves the
                # currently deployed publisher manifest rather than pinning a hash.
                response = await self.http.get(script_url, timeout=20)
                response.raise_for_status()
                self.raw(response.content)
                match = re.search(r'apiKey:"([^"\\]+)"', response.text)
                if not match:
                    raise ValueError("DTCC public search bootstrap schema changed")
                public_key = match.group(1)
            cached = (public_key, utcnow())
            self.dtcc_public_keys[script_url] = cached
        rows = []
        raw_pages = []
        offset = 0
        checkpoint = state.get("dtcc_checkpoint")
        while True:
            body = dict(feed.get("body", {}), firstResult=offset)
            try:
                page, page_state = await self.fetch(
                    dict(feed, method="POST", body=body, headers={"Authorization": "Bearer " + cached[0]}),
                    key + ":page:" + str(offset),
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {401, 403}:
                    self.dtcc_public_keys.pop(script_url, None)
                    if not _refresh:
                        return await self.dtcc_payload(feed, key, state, _refresh=True)
                raise
            if page is None:
                raise ValueError("Unexpected conditional response from DTCC search")
            data = json.loads(page)
            if "results" not in data or "totalCount" not in data:
                raise ValueError("DTCC search response schema changed")
            raw_pages.append({"raw_ref": page_state["raw_ref"], "raw_sha256": page_state["raw_sha256"]})
            page_rows = data["results"]
            for row in page_rows:
                if row.get("clickUri") == checkpoint:
                    page_rows = page_rows[: page_rows.index(row) + 1]
                    break
            rows.extend(page_rows)
            if any(row.get("clickUri") == checkpoint for row in page_rows):
                break
            offset += len(page_rows)
            if offset >= data["totalCount"]:
                break
            if not page_rows:
                raise ValueError("DTCC pagination stalled before totalCount/checkpoint")
            await asyncio.sleep(0.5)
        payload = canonical_json({"results": rows, "raw_pages": raw_pages}).encode()
        ref, digest = self.raw(payload)
        return payload, dict(state, raw_ref=ref, raw_sha256=digest)

    async def ingest(self, source, item, raw_ref, raw_hash):
        sid = source["source_id"]
        body = " ".join(
            (item.get("title", "") + " " + item.get("text", "") + " " + (item.get("quoted") or "")).split()
        )
        # Empty/media-only posts are durable evidence, not invented textual claims.
        content_hash = (
            sha256(body)
            if body
            else sha256(
                {"source_id": sid, "external_id": item["external_id"], "media": item.get("media", [])}
            )
        )
        external = str(item["external_id"])
        async with self.repo.lock("ingest-dedup"), self.repo.transaction():
            dedup = await self.repo.get(
                "ops.source_dedup", sha256(sid + "|" + external)
            ) or await self.repo.get("ops.source_dedup", "content:" + content_hash)
            # Index entries can omit text for a previously captured full article.
            # Preserve that durable evidence and count the real observation first.
            if not dedup and not body and sid != "S29":
                raise ValueError("Source entry has no normalized content")
            if dedup:
                old = await self.repo.get("ingest.source_items", dedup["item_id"])
                old["observation_count"] = old.get("observation_count", 1) + 1
                old["last_observed_at"] = item.get("observed_at") or iso()
                if sid == "S29" and old.get("source_id") == "S29":
                    monitored = (
                        item.get("monitored_author") or item.get("reposted_by") or item.get("author", "")
                    )
                    role_now = self.accounts.get(monitored.lower().lstrip("@"), {}).get("role")
                    old.setdefault("monitored_account_evidence", {})[monitored] = {
                        "raw_ref": raw_ref,
                        "raw_sha256": raw_hash,
                        "observed_at": iso(),
                    }
                    old["monitored_accounts"] = list(
                        dict.fromkeys(old.get("monitored_accounts", []) + [monitored])
                    )
                    if role_now == "ORGANIZATION":
                        old.update(monitored_author=monitored, author_role=role_now, deferrable=False)
                        old["relevance"] = {"decision": "ALWAYS"}
                        if not getattr(self.settings, "capture_only", False):
                            await self.repo.enqueue(
                                "ROUTE_ITEM",
                                {"item_id": old["item_id"]},
                                old["item_id"],
                                priority=1,
                                max_attempts=2,
                            )
                await self.repo.put("ingest.source_items", old["item_id"], old)
                return old
            item_id = new_id("item")
            account = self.accounts.get(
                (item.get("monitored_author") or item.get("reposted_by") or item.get("author", ""))
                .lower()
                .lstrip("@"),
                {},
            )
            role = account.get("role")
            filtered = (
                sid in {"S30", "S31", "S32", "S33", "S34"}
                or (sid == "S25" and item.get("subtype") == "commits")
                or (sid == "S29" and role != "ORGANIZATION")
            )
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
                feed=item.get("feed") or source.get("feed") or sid,
                source=source.get("name", sid),
                source_name=source.get("name", sid),
                author=item.get("author") or source.get("name", sid),
                author_role=role,
                monitored_accounts=[
                    item.get("monitored_author") or item.get("reposted_by") or item.get("author")
                ]
                if sid == "S29"
                else [],
                original_author_role=self.accounts.get(item.get("author", "").lower().lstrip("@"), {}).get(
                    "role"
                ),
                author_organization=account.get("entity_id"),
                deferrable=priority == 2,
                external_id=external,
                raw_ref=raw_ref,
                raw_sha256=raw_hash,
                content_hash=content_hash,
                observed_at=item.get("observed_at") or iso(),
                published_at_precision=item.get("published_at_precision")
                or ("SECOND" if item.get("published_at") else None),
                published_prec="BUCKET"
                if item.get("published_at_precision") == "DAY"
                else "EXACT"
                if item.get("published_at")
                else "COLLECTION_ONLY",
                published_at_missing_reason=None
                if item.get("published_at")
                else "SOURCE_DID_NOT_PROVIDE_TIME",
                source_class=source.get(
                    "source_class",
                    source.get(
                        "class",
                        "TECHNICAL_EVIDENCE"
                        if sid == "S25"
                        else "SOCIAL_LEAD"
                        if sid == "S29"
                        else "SECONDARY"
                        if int(sid[1:]) >= 30
                        else "PRIMARY",
                    ),
                ),
                relevance={"decision": None, "pending_reason": "AWAITING_FILTER"}
                if filtered
                else {"decision": "ALWAYS"},
                extraction_status="PENDING",
                observation_count=1,
                config_version=self.settings.config_version,
                code_version=self.settings.code_version,
            )
            await self.repo.put("ingest.source_items", item_id, record)
            index = {"item_id": item_id}
            await self.repo.put("ops.source_dedup", sha256(sid + "|" + external), index)
            await self.repo.put("ops.source_dedup", "content:" + content_hash, index)
            if not getattr(self.settings, "capture_only", False):
                await self.repo.enqueue(
                    "FILTER_ITEM" if filtered else "ROUTE_ITEM",
                    {"item_id": item_id},
                    item_id,
                    priority=priority,
                    max_attempts=2,
                )
            return record

    async def poll_once(self, source_id, *, due_only=False, escalated=False):
        source = self.sources[source_id]
        if source_id == "S29" and not getattr(self.settings, "x_enabled", True):
            return {
                "source_id": source_id,
                "items": 0,
                "status": "DISABLED",
                "reason": "X_DISABLED_BY_OPERATOR",
                "errors": [],
            }
        lock = self.locks.setdefault(source_id, asyncio.Lock())
        async with lock:
            count = 0
            failures = []
            for idx, original in enumerate(source.get("feeds", [])):
                feed = dict(original, source_limits=source.get("limits", {}))
                key = f"{source_id}:{idx}"
                state = await self.repo.get("ops.source_checkpoints", key) or {}
                last_attempt = state.get("last_attempt_at") or state.get("last_success_at")
                if (
                    due_only
                    and last_attempt
                    and (utcnow() - parse_time(last_attempt)).total_seconds()
                    < (
                        self.escalated_interval_s
                        if escalated
                        else feed.get("interval_s", source.get("interval_s", 60))
                    )
                ):
                    continue
                kind = str(feed.get("kind", feed.get("format", source.get("mode", "rss")))).lower()
                attempted_at = iso()
                article_failures = []
                state["last_attempt_at"] = attempted_at
                try:
                    if source_id == "S29":
                        rows, _ = await self.xsensor.posts(
                            self.settings.x_list_id or source.get("list_id"),
                            state.get("status_id"),
                            known_ids=state.get("recent_status_ids", []),
                        )
                        captures = self.xsensor.raw_pages
                        if not captures:
                            raise ValueError("X browser captured no raw HTML evidence")
                        state.update(
                            raw_ref=captures[0]["raw_ref"],
                            raw_sha256=captures[0]["raw_sha256"],
                            scan_status=self.xsensor.scan_status,
                        )
                        for item in rows:
                            item["browser_raw_refs"] = captures
                    else:
                        if kind == "dtcc_search":
                            payload, state = await self.dtcc_payload(feed, key, state)
                        elif source_id == "S19":
                            payload = await self.browser.html(feed["url"])
                            ref, digest = self.raw(payload)
                            state.update(raw_ref=ref, raw_sha256=digest)
                        else:
                            payload, state = await self.fetch(feed, key)
                        if payload is None:
                            state.update(
                                last_attempt_at=attempted_at,
                                last_success_at=iso(),
                                status="HEALTHY",
                                error=None,
                            )
                            await self.repo.put("ops.source_checkpoints", key, state)
                            continue
                        if source_id == "S14":
                            snapshot = parsers.instruments(payload, feed)
                            if str(feed.get("venue", "")).lower() == "bybit":
                                page_data = json.loads(payload)
                                cursor = page_data.get("result", {}).get("nextPageCursor")
                                cursors = set()
                                while cursor:
                                    if cursor in cursors:
                                        raise ValueError("Repeated Bybit instrument cursor")
                                    cursors.add(cursor)
                                    parts = urlsplit(feed["url"])
                                    params = dict(parse_qsl(parts.query))
                                    params["cursor"] = cursor
                                    next_feed = dict(
                                        feed,
                                        url=urlunsplit(
                                            (
                                                parts.scheme,
                                                parts.netloc,
                                                parts.path,
                                                urlencode(params),
                                                parts.fragment,
                                            )
                                        ),
                                    )
                                    next_payload, _next_state = await self.fetch(
                                        next_feed, key + ":cursor:" + sha256(cursor)
                                    )
                                    if next_payload is None:
                                        raise ValueError("Unexpected conditional paginated response")
                                    snapshot.update(parsers.instruments(next_payload, feed))
                                    page_data = json.loads(next_payload)
                                    cursor = page_data.get("result", {}).get("nextPageCursor")
                            previous = state.get("instruments")
                            rows = []
                            if previous is not None:
                                for symbol in sorted(set(snapshot) | set(previous)):
                                    if snapshot.get(symbol) != previous.get(symbol):
                                        current = snapshot.get(symbol)
                                        old = previous.get(symbol)
                                        rows.append(
                                            {
                                                "external_id": f"{feed.get('venue')}|{feed.get('market_type')}|{symbol}|{sha256(current)}",
                                                "title": f"{symbol} instrument change",
                                                "text": canonical_json({"before": old, "after": current}),
                                                "url": feed["url"],
                                                "published_at": None,
                                                "change": "REMOVED"
                                                if current is None
                                                else "ADDED"
                                                if old is None
                                                else "STATE_UPDATE",
                                            }
                                        )
                            state["instruments"] = snapshot
                        elif kind in {"rss", "atom", "feed"}:
                            rows = parsers.feeds(payload, source_id, feed)
                        elif kind in {"html", "rendered", "rendered_html", "html_diff"} or source_id == "S19":
                            rows = parsers.article_links(payload, feed)
                        else:
                            rows = parsers.json_items(payload, source_id)
                            if source_id == "S24":
                                pending = state.get("pending_proposals", [])
                                if pending:
                                    query = (
                                        "query { proposals(first: 1000, where: {id_in: "
                                        + json.dumps(pending)
                                        + "}) { id title body state start end created scores scores_total space { id } } }"
                                    )
                                    extra, extra_state = await self.fetch(
                                        dict(feed, body={"query": query}), key + ":pending"
                                    )
                                    if extra:
                                        existing = {r["external_id"] for r in rows}
                                        for r in parsers.json_items(extra, source_id):
                                            if r["external_id"] not in existing:
                                                r["proposal_raw_ref"] = extra_state["raw_ref"]
                                                r["proposal_raw_sha256"] = extra_state["raw_sha256"]
                                                rows.append(r)
                                state["pending_proposals"] = list(
                                    {r["proposal_id"] for r in rows if r["state"] != "closed"}
                                )
                    for item in rows:
                        item.setdefault("feed", feed["url"])
                        item.setdefault("observed_at", state.get("fetched_at") or attempted_at)
                        if source_id == "S02" and not self.filing_matches(item):
                            continue
                        if kind in {
                            "html",
                            "rendered",
                            "rendered_html",
                            "html_diff",
                            "dtcc_search",
                        } or source_id in {
                            "S01",
                            "S19",
                        }:
                            seen = await self.repo.get(
                                "ops.source_dedup", sha256(source_id + "|" + str(item["external_id"]))
                            )
                            if not seen:
                                if source_id == "S19":
                                    article = await self.browser.html(item["url"])
                                    article_ref, article_hash = self.raw(article)
                                    state_article = {"raw_ref": article_ref, "raw_sha256": article_hash}
                                else:
                                    try:
                                        article, state_article = await self.fetch(
                                            {"url": item["url"], "source_limits": source.get("limits", {})},
                                            key + ":" + sha256(item["url"]),
                                        )
                                    except httpx.HTTPStatusError as exc:
                                        if exc.response.status_code != 404:
                                            raise
                                        error_ref, error_hash = self.raw(exc.response.content)
                                        error = {
                                            "url": item["url"],
                                            "http_status": 404,
                                            "raw_ref": error_ref,
                                            "raw_sha256": error_hash,
                                            "observed_at": iso(),
                                            "reason": "PUBLISHER_ARTICLE_NOT_FOUND",
                                        }
                                        state.setdefault("article_errors", {})[item["url"]] = error
                                        article_failures.append(item["url"])
                                        failures.append("Article HTTP404: " + item["url"])
                                        # Do not create an item/dedup entry for failed evidence.
                                        continue
                                if article is not None:
                                    state.get("article_errors", {}).pop(item["url"], None)
                                if article is not None:
                                    detail = parsers.article(article)
                                    if not detail["text"].strip():
                                        state.setdefault("article_errors", {})[item["url"]] = {
                                            "url": item["url"],
                                            "http_status": state_article.get("http_status"),
                                            "raw_ref": state_article["raw_ref"],
                                            "raw_sha256": state_article["raw_sha256"],
                                            "observed_at": iso(),
                                            "reason": "EMPTY_ARTICLE_BODY",
                                        }
                                        article_failures.append(item["url"])
                                        failures.append(
                                            "Article parse failure (empty visible text): " + item["url"]
                                        )
                                        continue
                                    item.update({k: v for k, v in detail.items() if v})
                                    item["discovery_raw_ref"] = state["raw_ref"]
                                    item["discovery_raw_sha256"] = state["raw_sha256"]
                                    item["article_raw_ref"] = state_article["raw_ref"]
                                    item["article_raw_sha256"] = state_article["raw_sha256"]
                        if source_id == "S09" and not await self.repo.get(
                            "ops.source_dedup", sha256(source_id + "|" + str(item["external_id"]))
                        ):
                            detail, dstate = await self.fetch(
                                {
                                    "url": "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode="
                                    + item["external_id"]
                                },
                                key + ":detail:" + item["external_id"],
                            )
                            if detail:
                                obj = json.loads(detail)["data"]
                                body = obj.get("body", "")
                                item["text"] = parsers.binance_body(body)
                                item["detail_raw_ref"] = dstate["raw_ref"]
                                item["detail_raw_sha256"] = dstate["raw_sha256"]
                        await self.ingest(
                            source,
                            item,
                            item.get("article_raw_ref")
                            or item.get("detail_raw_ref")
                            or item.get("proposal_raw_ref")
                            or state["raw_ref"],
                            item.get("article_raw_sha256")
                            or item.get("detail_raw_sha256")
                            or item.get("proposal_raw_sha256")
                            or state["raw_sha256"],
                        )
                        count += 1
                    if kind == "dtcc_search" and rows and not state.get("article_errors"):
                        state["dtcc_checkpoint"] = rows[0]["external_id"]
                    if source_id == "S29" and rows:
                        if self.xsensor.scan_status != "GAP_LIMIT":
                            state["status_id"] = rows[0]["external_id"]
                            state["recent_status_ids"] = list(
                                dict.fromkeys(
                                    [r["external_id"] for r in rows] + state.get("recent_status_ids", [])
                                )
                            )[:500]
                        else:
                            failures.append(
                                "X bounded scan reached its limit; new items retained, historical gap remains"
                            )
                    state.update(
                        last_attempt_at=attempted_at, last_success_at=iso(), status="HEALTHY", error=None
                    )
                    if article_failures or state.get("article_errors"):
                        state.update(status="DEGRADED", error="PUBLISHER_ARTICLE_FAILURES")
                        if not article_failures:
                            failures.append("Pending publisher article failures remain unresolved")
                    if source_id == "S29" and self.xsensor.scan_status == "GAP_LIMIT":
                        state.update(status="DEGRADED", error="X_HISTORY_GAP_LIMIT")
                    await self.repo.put("ops.source_checkpoints", key, state)
                except Exception as exc:  # noqa: BLE001 — isolate one failed feed from all other sources
                    failures.append(str(exc))
                    state.update(
                        last_attempt_at=attempted_at, status="DEGRADED", error=str(exc), last_failure_at=iso()
                    )
                    await self.repo.put("ops.source_checkpoints", key, state)
            return {
                "source_id": source_id,
                "items": count,
                "status": "DEGRADED" if failures else "HEALTHY",
                "errors": failures,
            }

    def filing_matches(self, item):
        value = (item.get("title", "") + " " + item.get("text", "")).lower()
        registry = self.settings.load_config("registry")
        ambiguous = {"near", "world", "curve", "quant", "cow", "lit", "lighter", "plasma"}
        crypto_context = bool(
            re.search(
                r"\b(?:crypto(?:currency|currencies|assets?)?|digital\s+assets?|bitcoin|ether(?:eum)?|blockchain|tokens?|tokenized|stablecoins?|defi|decentralized)\b",
                value,
            )
        )
        qualified = [
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
        candidates = list(qualified)
        if re.search(r"\blighter\b", value) and re.search(r"\blit\b", value):
            return True
        if re.search(r"\bplasma\b", value) and re.search(r"\bxpl\b", value):
            return True
        for token in registry.get("tokens", []):
            candidates.extend([token.get("name", ""), token.get("symbol", "")])
        for entity in registry.get("entities", []):
            if entity.get("kind") == "REGULATOR":
                continue
            candidates.extend(entity.get("names", []) or [entity.get("name", "")])
            candidates.extend(
                alias for alias in entity.get("aliases", []) if '"' not in alias and " only " not in alias
            )
        for name in candidates:
            if not name:
                continue
            if name.casefold() in ambiguous:
                continue
            if re.search(r"(?<!\w)" + re.escape(name.casefold()) + r"(?!\w)", value):
                return True
        return bool(re.search(r"\b(?:trust|etf)\b", value)) and crypto_context

    async def check(self, *, probe=False, include_browser=False, check_accounts=False):
        if probe:
            from .probe import probe_feeds

            results = await probe_feeds(self, include_browser=include_browser)
        else:
            results = await self._saved_health()
        if check_accounts:
            results.extend(await self._account_checks())
        return results

    async def _saved_health(self):
        results = []
        for sid, source in self.sources.items():
            if sid == "S29" and not getattr(self.settings, "x_enabled", True):
                results.append(
                    {
                        "source_id": sid,
                        "configured": bool(source.get("feeds")),
                        "states": [],
                        "status": "DISABLED",
                        "reason": "X_DISABLED_BY_OPERATOR",
                    }
                )
                continue
            states = [
                await self.repo.get("ops.source_checkpoints", f"{sid}:{i}")
                for i, _ in enumerate(source.get("feeds", []))
            ]
            results.append(
                {
                    "source_id": sid,
                    "configured": bool(source.get("feeds")),
                    "states": states,
                    "status": "DEGRADED"
                    if any(s and s.get("status") == "DEGRADED" for s in states)
                    else "HEALTHY"
                    if states and all(states)
                    else "UNVERIFIED",
                }
            )
        return results

    async def _account_checks(self):
        if not getattr(self.settings, "x_enabled", True):
            return [{"source_id": "S29", "status": "DISABLED", "reason": "X_DISABLED_BY_OPERATOR"}]
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
        await self.browser.close()
        await self.xsensor.close()
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
        async def loop(source):
            while not stop.is_set():
                started = utcnow()
                await self.control_once(source["source_id"])
                # A new escalation must be noticed within 15 seconds even for a
                # source normally read every ten minutes. due_only still prevents
                # network reads until its normal/escalated feed deadline.
                remaining = max(0, 15 - (utcnow() - started).total_seconds())
                try:
                    await asyncio.wait_for(stop.wait(), timeout=remaining)
                except TimeoutError:
                    pass

        try:
            tasks = [loop(source) for sid, source in self.sources.items() if sid != "S29"]
            if "S29" in self.sources and getattr(self.settings, "x_enabled", True):
                tasks.append(self.xsensor.run(stop, lambda: self.control_once("S29")))
            if tasks:
                await asyncio.gather(*tasks)
            else:
                await stop.wait()
        finally:
            await self.close()
