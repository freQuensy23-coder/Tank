"""Validated domain tools exposed to OpenMA. External material is evidence, never instructions."""

from __future__ import annotations

import asyncio
import copy
import re
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlparse

import httpx

from tank.util import canonical_json, iso, new_id, parse_time, sha256, utcnow

STATES = ["RUMOR", "PROPOSED", "APPROVED", "SIGNED", "DEPLOYED", "LIVE", "MONEY_FLOWING"]
INTEGRATION = [
    "COMPATIBLE",
    "IN_DEVELOPMENT",
    "BUILT",
    "SUBMITTED",
    "APPROVED",
    "LIVE",
    "MATERIAL_USAGE",
    "ECONOMIC_FLOW",
]
LAYERS = [
    "ISSUER_PRODUCT",
    "VENUE_DEX",
    "SETTLEMENT_L1_L2",
    "LIQUIDITY_EXECUTION",
    "LENDING_COLLATERAL",
    "ORACLE_DATA",
    "INTEROPERABILITY",
    "CUSTODY_COMPLIANCE",
]
IMPLEMENTATION = dict(zip(STATES, [0, 4, 6, 6, 7, 8, 10]))


def normalized(s):
    return " ".join(str(s).split())


def freshness(age):
    for hours, grade in [(1, "FRESH"), (4, "NORMAL"), (8, "MEDIUM"), (24, "POOR"), (48, "OLD")]:
        if age <= hours * 3600:
            return grade
    return "STALE"


class EventToolService:
    TOOLS = {
        "get_item",
        "search_tank_items",
        "list_open_events",
        "get_event",
        "get_registry",
        "get_token_facts",
        "web_search",
        "fetch_page",
        "record_event",
        "add_observation",
        "merge_into",
        "set_assessment",
        "set_beneficiaries",
        "get_market_truth",
        "get_decision",
        "publish_card",
        "schedule_milestone",
        "propose_pattern",
        "record_review",
    }

    def __init__(self, repo, settings, market_truth, decision_engine, *, http=None, search=None):
        self.repo, self.settings, self.truth, self.engine = repo, settings, market_truth, decision_engine
        self.http = http
        self.search_service = search

    def provenance(self, ctx):
        return {
            k: v
            for k, v in dict(
                config_version=self.settings.config_version,
                code_version=self.settings.code_version,
                session_config_version=ctx.get("session_config_version", ctx.get("config_version")),
                session_code_version=ctx.get("session_code_version", ctx.get("code_version")),
                model_id=ctx.get("model_id"),
                prompt_version=ctx.get("prompt_version"),
                agent_version=ctx.get("agent_version"),
                session_id=ctx.get("session_id"),
            ).items()
        }

    async def dispatch(self, tool, arguments, context):
        if tool not in self.TOOLS:
            raise ValueError(f"Unknown Tank tool: {tool}")
        if not isinstance(arguments, dict):
            raise ValueError("Tool arguments must be an object")
        context = dict(context or {})
        if context.get("session_id"):
            stored_session = await self.repo.get("ops.agent_sessions", context["session_id"])
            if stored_session:
                context["event_id"] = stored_session.get("event_id")
        scoped = {
            "add_observation",
            "set_assessment",
            "set_beneficiaries",
            "get_market_truth",
            "get_decision",
            "publish_card",
            "schedule_milestone",
        }
        analyst = context.get("role") == "analyst" or (
            context.get("session_id") and context.get("role") not in {"system", "scout"}
        )
        if analyst:
            bound = context.get("event_id")
            if tool in scoped and (not bound or arguments.get("event_id") != bound):
                return {
                    "accepted": False,
                    "reason": "EVENT_SCOPE",
                    "error": "Analyst mutations must target the session bound event; record_event first for an unbound session",
                }
            if tool == "record_event" and bound:
                return {
                    "accepted": False,
                    "reason": "EVENT_ALREADY_BOUND",
                    "event_id": bound,
                    "error": "Use add_observation for the session event",
                }
            if tool == "merge_into" and not bound:
                return {
                    "accepted": False,
                    "reason": "EVENT_SCOPE",
                    "error": "Merge requires a bound source event",
                }
        if (context or {}).get("role") == "scout" and tool not in {
            "web_search",
            "search_tank_items",
            "record_review",
        }:
            raise ValueError("Scout cannot mutate event state")
        if tool == "set_beneficiaries":
            novel = [
                edge
                for edge in arguments.get("edges", [])
                if isinstance(edge, dict) and edge.get("layer") not in LAYERS
            ]
            if novel:
                claims = {
                    row["claim_id"]: row for row in await self.rows("evt.claims", arguments.get("event_id"))
                }
                proposals = []
                for edge in novel:
                    evidence = [
                        ref
                        for ref in edge.get("evidence", [])
                        if ref in claims and claims[ref].get("accepted")
                    ]
                    if not evidence:
                        raise ValueError("Novel beneficiary layer requires accepted evidence")
                    proposal = await self.propose_pattern(
                        {
                            "hook": "BENEFICIARY_LAYER",
                            "subject": "BENEFICIARY_LAYER:" + str(edge.get("layer")),
                            "evidence": evidence,
                        },
                        dict(context or {}),
                    )
                    proposals.append(proposal["prp_id"])
                return {
                    "accepted": False,
                    "reason": "Noncanonical beneficiary layer requires scout review",
                    "proposal_ids": proposals,
                }
        schema_relative = (
            "skills/"
            + ("tank-scout-reviewer" if tool == "record_review" else "tank-event-analyst")
            + f"/{tool}.schema.json"
        )
        if self.settings.has_config(schema_relative):
            validate_schema(arguments, self.settings.read_config_json(schema_relative))
        ctx = dict(context or {})
        event_id = arguments.get("event_id") or ctx.get("event_id")
        if tool in {
            "record_event",
            "add_observation",
            "merge_into",
            "set_assessment",
            "set_beneficiaries",
            "publish_card",
            "schedule_milestone",
            "get_decision",
        }:
            async with self.repo.transaction():
                async with self.repo.lock(
                    "event:" + str(event_id or ctx.get("session_id") or arguments.get("item_id"))
                ):
                    return await getattr(self, tool)(arguments, ctx)
        return await getattr(self, tool)(arguments, ctx)

    async def rows(self, table, event_id=None, *, filters=None):
        query = dict(filters or {})
        if event_id is not None:
            query["event_id"] = event_id
        return [row async for row in self.repo.iter_all(table, filters=query or None)]

    async def latest_record(self, table, event_id, token_id):
        return await self.repo.latest_before(
            table,
            "cutoff",
            iso(utcnow() + timedelta(microseconds=1)),
            filters={"event_id": event_id, "token_id": token_id},
        )

    async def first_alert(self, event_id, kind=None):
        filters = {"event_id": event_id}
        if kind:
            filters["kind"] = kind
        async for row in self.repo.iter_range("out.alerts", "generated_at", filters=filters, batch_size=1):
            return row
        return None

    async def require(self, table, key):
        row = await self.repo.get(table, key)
        if row is None:
            raise ValueError(f"Missing {table}: {key}")
        return row

    async def get_item(self, a, c):
        return await self.require("ingest.source_items", a["item_id"])

    async def search_tank_items(self, a, c):
        days = a.get("days", 90)
        if not isinstance(days, (int, float)) or not 0 < days <= 90:
            raise ValueError("days must be >0 and <=90")
        cutoff = utcnow() - timedelta(days=days)
        sources = a.get("sources", [])
        terms = a["query"].casefold().split()
        rows = [
            r
            async for r in self.repo.iter_range(
                "ingest.source_items", "observed_at", start=iso(cutoff), end=iso()
            )
        ]
        return {
            "items": sorted(
                [
                    r
                    for r in rows
                    if parse_time(r["observed_at"]) >= cutoff
                    and (not sources or r["source_id"] in sources)
                    and all(t in (r.get("title", "") + " " + r.get("text", "")).casefold() for t in terms)
                ],
                key=lambda r: r["observed_at"],
                reverse=True,
            )[:20]
        }

    async def list_open_events(self, a, c):
        q = a.get("entity_or_token", "").casefold()
        return {
            "events": [
                r
                for r in await self.rows("evt.events")
                if r["lifecycle"] not in {"CLOSED", "MERGED", "INVALIDATED"}
                and (not q or q in canonical_json(r).casefold())
            ]
        }

    async def get_event(self, a, c):
        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        return {
            "event": event,
            **{
                table.split(".")[-1]: await self.rows(table, eid)
                for table in [
                    "evt.event_observations",
                    "evt.claims",
                    "evt.state_changes",
                    "evt.ems_assessments",
                    "evt.beneficiary_maps",
                    "evt.beneficiary_edges",
                ]
            },
        }

    async def get_registry(self, a, c):
        q = a.get("entity_or_token", "").casefold()
        return {
            name: [
                r
                for r in self.settings.load_config("registry").get(name, [])
                if not q or q in canonical_json(r).casefold()
            ]
            for name in ["entities", "tokens", "instruments", "x_accounts"]
        } | {"dependency_map": self.settings.load_config("dependency_map")}

    async def get_token_facts(self, a, c):
        token = a["token"]
        registry = await self.get_registry({"entity_or_token": token}, c)
        return {
            "registry": registry,
            **{
                t.split(".")[-1]: await self.rows(t, filters={"token_id": token})
                for t in ["mkt.supply_obs", "mkt.unlocks", "evt.capture_chains"]
            },
        }

    def validate_evidence_url(self, url):
        parsed = urlparse(url)
        secret_names = {
            "apikey",
            "api_key",
            "access_token",
            "authorization",
            "client_secret",
            "api-token",
            "auth_token",
        }
        if (
            parsed.username
            or parsed.password
            or any(
                name.casefold() in secret_names for name, _ in parse_qsl(parsed.query, keep_blank_values=True)
            )
        ):
            raise ValueError("Credential-bearing URLs cannot be stored as evidence")
        configured = [
            getattr(self.settings, name, None)
            for name in [
                "etherscan_api_key",
                "typesafe_api_key",
                "openrouter_api_key",
                "openma_api_key",
                "telegram_token",
                "codex_gateway_key",
            ]
        ]
        decoded = unquote(url)
        if any(value and value in decoded for value in configured):
            raise ValueError("Credential-bearing URLs cannot be stored as evidence")

    def validate_evidence_bytes(self, data):
        configured = [
            getattr(self.settings, name, None)
            for name in [
                "etherscan_api_key",
                "typesafe_api_key",
                "openrouter_api_key",
                "openma_api_key",
                "telegram_token",
                "codex_gateway_key",
            ]
        ]
        if any(value and len(value) >= 8 and value.encode("utf-8") in data for value in configured):
            raise ValueError("Credential-bearing content cannot be stored as evidence")

    async def save_web(
        self, url, text, title, c, source_class="SECONDARY", raw_bytes=None, media_type="text/plain"
    ):
        self.validate_evidence_url(url)
        original = raw_bytes if raw_bytes is not None else text.encode("utf-8")
        self.validate_evidence_bytes(original)
        digest = sha256(original)
        iid = (
            "web-"
            + sha256(
                {
                    "url": url,
                    "text": text,
                    "raw_sha256": digest,
                    "media_type": media_type,
                    "session": c.get("session_id"),
                }
            )[:32]
        )
        raw = Path(self.settings.data_dir) / "raw" / "web" / f"{digest}.txt"
        raw.parent.mkdir(parents=True, exist_ok=True)
        raw.write_bytes(original)
        row = dict(
            item_id=iid,
            source_id="WEB",
            feed="web:search" if media_type == "application/json" else "web:page",
            published_prec="COLLECTION_ONLY",
            relevance={"decision": "ALWAYS", "reason": "AGENT_REQUESTED_EVIDENCE"},
            external_id=iid,
            raw_ref=str(raw),
            raw_sha256=digest,
            raw_media_type=media_type,
            evidence_kind="SEARCH_RESULT" if media_type == "application/json" else "FETCHED_PAGE",
            raw_size_bytes=len(original),
            readable_text_sha256=sha256(text),
            content_hash=sha256(normalized(text)),
            url=url,
            title=title,
            text=text,
            observed_at=iso(),
            published_at=None,
            source_class=source_class,
            extraction_status="PENDING",
            **self.provenance(c),
        )
        existing = await self.repo.get("ingest.source_items", iid)
        if existing:
            return existing
        return await self.repo.put("ingest.source_items", iid, row)

    async def web_search(self, a, c):
        from tank.search import DuckDuckGoSearchService, SearchError

        if self.search_service is None:
            self.search_service = DuckDuckGoSearchService(user_agent=self.settings.user_agent)

        async def count_attempt(attempt):
            key = new_id("search-usage")
            await self.repo.put(
                "ops.llm_usage",
                key,
                dict(
                    usage_id=key,
                    call="WEB_SEARCH",
                    provider="duckduckgo",
                    session_id=c.get("session_id"),
                    event_id=c.get("event_id"),
                    call_id=c.get("call_id"),
                    attempt=attempt,
                    web_searches=1,
                    usage={"web_searches": 1},
                    cost_usd="0",
                    billing_basis="FREE",
                    at=iso(),
                    model="duckduckgo",
                    effort="none",
                    input_tokens=0,
                    cache_read_tokens=0,
                    cache_write_tokens=0,
                    output_tokens=0,
                    stop_reason="SEARCH_ATTEMPT",
                    query_sha256=sha256(a["query"]),
                    created_at=iso(),
                    **{key: value for key, value in self.provenance(c).items() if key not in {"session_id"}},
                ),
                immutable=True,
            )

        try:
            response = await self.search_service.search(a["query"], on_attempt=count_attempt)
        except SearchError as error:
            return {"items": [], "search": error.as_dict()}
        items = []
        for result in response.results:
            item = await self.save_web(
                result["href"],
                result.get("body", ""),
                result.get("title", ""),
                c,
                raw_bytes=canonical_json(result).encode("utf-8"),
                media_type="application/json",
            )
            items.append(item)
        return {
            "items": items,
            "provider": "duckduckgo",
            "attempts": response.attempts,
            "billing_basis": "FREE",
        }

    async def fetch_page(self, a, c):
        self.validate_evidence_url(a["url"])
        if urlparse(a["url"]).scheme != "https":
            raise ValueError("Only HTTPS evidence fetches are allowed")
        async with (
            httpx.AsyncClient(timeout=20, follow_redirects=False)
            if self.http is None
            else _borrow(self.http) as client
        ):
            async with client.stream("GET", a["url"], headers={"User-Agent": self.settings.user_agent}) as r:
                r.raise_for_status()
                chunks = []
                size = 0
                async for chunk in r.aiter_bytes():
                    size += len(chunk)
                    if size > 2 * 1024 * 1024:
                        raise ValueError("Page exceeds 2 MB")
                    chunks.append(chunk)
        from selectolax.parser import HTMLParser

        original = b"".join(chunks)
        self.validate_evidence_bytes(original)
        # Persist the exact response before parsing; extraction failures retain the source bytes.
        digest = sha256(original)
        raw_path = Path(self.settings.data_dir) / "raw" / "web" / f"{digest}.txt"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(original)
        html = original.decode("utf-8", errors="replace")
        tree = HTMLParser(html)
        for node in tree.css("script,style,noscript"):
            node.decompose()
        return await self.save_web(
            a["url"],
            tree.text(separator=" ", strip=True),
            "Fetched page",
            c,
            raw_bytes=original,
            media_type=r.headers.get("content-type", "application/octet-stream"),
        )

    def validate_time(self, x, item):
        public = x["public_at"]
        value = public["value"] if isinstance(public, dict) else public
        age = (parse_time(item["observed_at"]) - parse_time(value)).total_seconds()
        if age < 0:
            raise ValueError("V7 public_at is later than observed_at")
        if freshness(age) != x["freshness"]:
            raise ValueError("V7 freshness does not match public_at age")
        quote = public.get("quote") if isinstance(public, dict) else None
        if quote and normalized(quote) not in normalized(item["text"]):
            raise ValueError("V7 public_at quote is not verbatim")
        return iso(value)

    def primary_source(self, item, claim):
        if item.get("source_class") in {"PRIMARY", "TECHNICAL_EVIDENCE", "MARKET_DATA"}:
            return True
        registry = self.settings.load_config("registry")
        if item.get("source_id") == "WEB":
            if item.get("evidence_kind") == "SEARCH_RESULT":
                return False
            host = (urlparse(item.get("url", "")).hostname or "").lower()
            subject = str(claim.get("subject", "")).casefold()
            subject_entity = next(
                (
                    e
                    for e in registry.get("entities", [])
                    if subject
                    in [
                        str(n).casefold()
                        for n in [
                            e.get("entity_id", ""),
                            e.get("name", ""),
                            *e.get("names", []),
                            *e.get("aliases", []),
                        ]
                    ]
                ),
                None,
            )
            for source in self.settings.load_config("sources").get("sources", []):
                # Multi-organization aggregators never inherit trust merely by domain.
                if source.get("source_id") in {
                    "S14",
                    "S23",
                    "S24",
                    "S25",
                    "S29",
                    "S30",
                    "S31",
                    "S32",
                    "S33",
                    "S34",
                }:
                    continue
                identity = subject_entity is not None and source["source_id"] in subject_entity.get(
                    "official_sources", []
                )
                source_names = [source.get("name", ""), *source.get("aliases", [])]
                identity = identity or subject in [str(n).casefold() for n in source_names]
                domains = {
                    (urlparse(f.get("official_index_url", f.get("url", ""))).hostname or "").lower()
                    for f in source.get("feeds", [])
                }
                if identity and any(
                    host == d or (d.startswith("www.") and host == d[4:]) for d in domains if d
                ):
                    return True
            return False
        handle = str(item.get("author", "")).lstrip("@").casefold()
        account = next(
            (
                a
                for a in registry.get("x_accounts", [])
                if str(a.get("handle", "")).lstrip("@").casefold() == handle
            ),
            None,
        )
        if not account or account.get("role") not in {"ORGANIZATION", "FOUNDER_C_LEVEL"}:
            return False
        authorized = {account.get("entity_id")}
        authorized.update(
            authority["entity_id"]
            for authority in account.get("official_authorities", [])
            if authority.get("entity_id") and authority.get("evidence_refs")
        )
        # Authority applies only to explicitly registered organizations and exact aliases.
        subject = str(claim.get("subject", "")).casefold()
        for entity in registry.get("entities", []):
            if entity["entity_id"] not in authorized:
                continue
            names = [
                entity["entity_id"],
                entity.get("name", ""),
                *entity.get("names", []),
                *entity.get("aliases", []),
            ]
            if any(name and str(name).casefold() == subject for name in names):
                return True
        return False

    async def validate_claims(self, claims, item, c, certainty, obs_id, eid):
        evidence_items = [item] + [
            r
            for r in (
                await self.rows(
                    "ingest.source_items", filters={"session_id": c["session_id"], "source_id": "WEB"}
                )
                if c.get("session_id")
                else []
            )
            if r.get("session_id") == c.get("session_id")
            and r.get("source_id") == "WEB"
            and c.get("session_id")
        ]
        permitted = {row["item_id"] for row in evidence_items}
        if c.get("session_id"):
            for call in await self.rows("ops.tool_calls", filters={"session_id": c["session_id"]}):
                result = call.get("result", {})
                if (
                    call.get("tool", call.get("tool_name")) == "get_item"
                    and isinstance(result, dict)
                    and result.get("item_id")
                ):
                    permitted.add(result["item_id"])
                if call.get("tool", call.get("tool_name")) == "search_tank_items" and isinstance(
                    result, dict
                ):
                    permitted.update(row["item_id"] for row in result.get("items", []) if row.get("item_id"))
            known = {row["item_id"] for row in evidence_items}
            for item_id in sorted(permitted - known):
                record = await self.require("ingest.source_items", item_id)
                evidence_items.append(record)
        accepted = []
        dropped = []
        for raw in claims:
            claim = copy.deepcopy(raw)
            quote = claim.pop("quote", None)
            evidence = claim.get("evidence") or (
                [{"item_id": item["item_id"], "quote": quote}] if quote else []
            )
            resolved = []
            for e in evidence:
                matches = [
                    r
                    for r in evidence_items
                    if (r["item_id"] == e.get("item_id") or quote)
                    and e.get("quote")
                    and normalized(e["quote"]) in normalized(r["text"])
                ]
                if matches:
                    resolved.append({"item_id": matches[0]["item_id"], "quote": e["quote"]})
            if not evidence or len(resolved) != len(evidence):
                dropped.append({"claim": raw, "reason": "V1 quote not found in permitted evidence"})
                continue
            if claim.get("type") not in {
                "FACT",
                "STATE",
                "NUMBER",
                "RELATION",
                "TIMESTAMP",
                "MECHANISM",
                "MILESTONE",
            }:
                raise ValueError("Invalid claim type")
            verified = certainty == "HIGH" and all(
                self.primary_source(next(r for r in evidence_items if r["item_id"] == e["item_id"]), claim)
                for e in resolved
            )
            text = " ".join(e["quote"] for e in resolved).casefold()
            state = claim.get("state_claim")
            if state in {"LIVE", "MONEY_FLOWING", "MATERIAL_USAGE", "ECONOMIC_FLOW"}:
                future = bool(re.search(r"\b(will|tomorrow|scheduled|plans? to|expected to)\b", text))
                for timestamp in re.findall(
                    r"\d{4}-\d{2}-\d{2}(?:[tT ][0-9:.]+(?:Z|[+-]\d{2}:?\d{2})?)?", text
                ):
                    try:
                        future = future or parse_time(timestamp) > parse_time(item["observed_at"])
                    except ValueError:
                        pass
                opening_time = re.search(r"\btrading opens(?: at| on)?\s+(\d{1,2}):(\d{2})\s*utc\b", text)
                if opening_time:
                    observed = parse_time(item["observed_at"])
                    effective = observed.replace(
                        hour=int(opening_time[1]), minute=int(opening_time[2]), second=0, microsecond=0
                    )
                    future = future or effective > observed
                live = not future and any(
                    term in text
                    for term in [
                        "now live",
                        "available to",
                        "launched",
                        "trading opens",
                        "instrument trades",
                        "instrument actually trades",
                    ]
                )
                if re.search(
                    r"\b(not|never|isn.t|aren.t)\s+(?:yet\s+)?(?:now live|live|available|launched|trading)",
                    text,
                ):
                    live = False
                economic = not future and any(
                    term in text for term in ["fees accrued", "buyback executed", "economic flow observed"]
                )
                usage = not future and any(
                    term in text for term in ["independent usage", "material usage observed"]
                )
                if state == "MONEY_FLOWING" and economic:
                    pass
                elif state == "ECONOMIC_FLOW" and economic:
                    pass
                elif state == "MATERIAL_USAGE" and usage:
                    pass
                elif live:
                    state = "LIVE"
                elif "deployed" in text and not future:
                    state = "DEPLOYED"
                elif "built" in text and not future:
                    state = "BUILT" if claim.get("directionality") == "SOURCE_SIDE_SUPPORT" else "DEPLOYED"
                elif "signed" in text:
                    state = "SIGNED"
                elif verified and (
                    future
                    or any(
                        term in text
                        for term in [
                            "approved",
                            "announced",
                            "will",
                            "scheduled",
                            "plans to",
                            "plan to",
                            "trading opens",
                        ]
                    )
                ):
                    state = "APPROVED"
                elif any(term in text for term in ["proposed", "planned", "announced", "will", "scheduled"]):
                    state = "PROPOSED"
                else:
                    state = "RUMOR"
            if any(w in text for w in ["compatible", "sdk", "repository", "github"]):
                claim["directionality"] = "SOURCE_SIDE_SUPPORT"
                if state in INTEGRATION and INTEGRATION.index(state) > 2:
                    state = "BUILT"
            claim.update(
                claim_id=new_id("clm"),
                obs_id=obs_id,
                event_id=eid,
                state_claim=state,
                evidence=resolved,
                verified=verified,
                accepted=True,
                **self.provenance(c),
            )
            await self.repo.put("evt.claims", claim["claim_id"], claim)
            accepted.append(claim)
        return accepted, dropped

    def event_fact_basis(self, statement, claims, entities, tokens):
        normalized_statement = normalized(statement).casefold()
        scope = {str(entity["entity_id"]).casefold() for entity in entities} | {
            str(token).casefold() for token in tokens
        }
        registry = self.settings.load_config("registry")
        for entity in registry.get("entities", []):
            if entity["entity_id"].casefold() in scope:
                scope.update(
                    str(name).casefold()
                    for name in [entity.get("name", ""), *entity.get("names", []), *entity.get("aliases", [])]
                )
        basis = []
        for claim in claims:
            if not claim.get("verified") or claim["type"] not in {"FACT", "STATE", "RELATION", "MECHANISM"}:
                continue
            quotes = [normalized(entry["quote"]).casefold() for entry in claim["evidence"]]
            object_text = normalized(claim.get("object", "")).casefold()
            same_fact = any(normalized_statement and normalized_statement in quote for quote in quotes)
            related = (
                str(claim.get("subject", "")).casefold() in scope
                and object_text
                and object_text in normalized_statement
                and any(object_text in quote for quote in quotes)
            )
            if same_fact or related:
                basis.append(claim["claim_id"])
        return basis

    async def bind_reservation(self, reservation_id, status, event_id=None):
        async with self.repo.lock("routing"):
            reservation = await self.require("ops.routing_reservations", reservation_id)
            reservation["status"] = status
            if event_id:
                reservation["event_id"] = event_id
            await self.repo.put("ops.routing_reservations", reservation_id, reservation)

    async def record_event(self, a, c):
        x = a.get("extraction", a)
        required = {
            "item_id",
            "is_event",
            "statement",
            "certainty",
            "certainty_reason",
            "entities",
            "tokens",
            "event_time",
            "public_at",
            "freshness",
            "claims",
            "next_milestone",
            "checked_sources",
        }
        if required - set(x):
            raise ValueError("Missing extraction fields: " + ",".join(sorted(required - set(x))))
        item = await self.require("ingest.source_items", x["item_id"])
        public = self.validate_time(x, item)
        if x["certainty"] not in {"LOW", "MEDIUM", "HIGH"} or not x["certainty_reason"]:
            raise ValueError("certainty and reason required")
        if not x["is_event"]:
            if c.get("reservation_id"):
                await self.bind_reservation(c["reservation_id"], "CLOSED")
            item["extraction_status"] = "SKIPPED"
            await self.repo.put("ingest.source_items", item["item_id"], item)
            return {"is_event": False}
        eid = new_id("evt")
        obs = new_id("obs")
        entities = []
        registry = self.settings.load_config("registry").get("entities", [])
        for e in x["entities"]:
            found = next(
                (
                    r
                    for r in registry
                    if e.get("entity_id") == r.get("entity_id")
                    or e["name"].casefold()
                    in [
                        str(v).casefold()
                        for v in [r.get("name", ""), *r.get("names", []), *r.get("aliases", [])]
                    ]
                ),
                None,
            )
            entities.append(
                {"entity_id": found["entity_id"] if found else "UNRESOLVED:" + e["name"], "role": e["role"]}
            )
        claims, dropped = await self.validate_claims(x["claims"], item, c, x["certainty"], obs, eid)
        primary_fact_basis = self.event_fact_basis(x["statement"], claims, entities, x["tokens"])
        certainty = x["certainty"]
        certainty_reason = x["certainty_reason"]
        if certainty == "HIGH" and not primary_fact_basis:
            certainty = "MEDIUM"
            certainty_reason += (
                "; HIGH not verified: no accepted primary or artifact claim supports the event fact"
            )
        event_time = copy.deepcopy(x["event_time"])
        if event_time.get("value") and (
            not event_time.get("quote") or normalized(event_time["quote"]) not in normalized(item["text"])
        ):
            event_time["precision"] = "BUCKET_1D"
        event = dict(
            event_id=eid,
            statement=x["statement"],
            entities=entities,
            tokens_mentioned=x["tokens"],
            theme_tags=x.get("theme_tags", []),
            event_time=event_time,
            public_at=public,
            public_at_precision=x["public_at"].get("precision"),
            public_at_basis=x["public_at"]["basis"],
            known_to_us_at=item["observed_at"],
            freshness=x["freshness"],
            event_state="RUMOR",
            state_verified=False,
            certainty=certainty,
            certainty_reason=certainty_reason,
            certainty_evidence_claims=primary_fact_basis,
            checked_sources=copy.deepcopy(x["checked_sources"]),
            timing_confidence="HIGH"
            if x["public_at"]["precision"] == "EXACT" and item["source_class"] == "PRIMARY"
            else "MEDIUM",
            best_source_class=item["source_class"],
            next_milestone=x["next_milestone"],
            lifecycle="DETECTED",
            **self.provenance(c),
        )
        await self.apply_states(event, claims, c)
        await self.repo.put("evt.events", eid, event)
        await self.repo.put(
            "evt.event_observations",
            obs,
            dict(
                obs_id=obs,
                event_id=eid,
                item_id=item["item_id"],
                link_type="ORIGIN",
                public_at=public,
                freshness=x["freshness"],
                match_method="AGENT",
                observed_at=item["observed_at"],
                **self.provenance(c),
            ),
        )
        item["extraction_status"] = "DONE"
        await self.repo.put("ingest.source_items", item["item_id"], item)
        if c.get("session_id"):
            session = await self.repo.get("ops.agent_sessions", c["session_id"])
            if session:
                session["event_id"] = eid
                await self.repo.put("ops.agent_sessions", c["session_id"], session)
        milestone = x.get("next_milestone")
        if claims and item.get("source_id") == "S24" and item.get("end"):
            await self.schedule_milestone(
                {"event_id": eid, "milestone": "Snapshot vote ends", "due_at": item["end"], "source": "S24"},
                c,
            )
        elif milestone and all(milestone.get(key) for key in ["kind", "due_at", "source_id"]):
            await self.schedule_milestone(
                {
                    "event_id": eid,
                    "milestone": milestone["kind"],
                    "due_at": milestone["due_at"],
                    "source": milestone["source_id"],
                },
                c,
            )
        if c.get("reservation_id"):
            await self.bind_reservation(c["reservation_id"], "BOUND", eid)
        return {"event_id": eid, "accepted_claims": claims, "dropped_claims": dropped}

    async def apply_states(self, event, claims, c):
        old = event["event_state"]
        states = [r["state_claim"] for r in claims if r.get("state_claim") in STATES]
        event["event_state"] = max([old, *states], key=STATES.index)
        event["state_verified"] = (
            event.get("state_verified", False)
            if old == event["event_state"]
            else any(r["verified"] and r["state_claim"] == event["event_state"] for r in claims)
        )
        if any(r["verified"] and r.get("state_claim") == event["event_state"] for r in claims):
            event["state_verified"] = True
        if old != event["event_state"]:
            sid = new_id("state")
            await self.repo.put(
                "evt.state_changes",
                sid,
                dict(
                    state_change_id=sid,
                    event_id=event["event_id"],
                    ladder="EVENT",
                    from_state=old,
                    to_state=event["event_state"],
                    evidence=[r["claim_id"] for r in claims if r.get("state_claim") == event["event_state"]],
                    changed_at=iso(),
                    **self.provenance(c),
                ),
                immutable=True,
            )
        for r in claims:
            marker = r.get("state_claim")
            if marker in {"CANCELLED", "REVERSED", "REJECTED", "PAUSED"}:
                event["negative_marker"] = marker
                await self.contradiction(
                    event["event_id"],
                    "UNRESOLVED" if marker == "PAUSED" else "FATAL",
                    marker,
                    [r["claim_id"]],
                    c,
                )

    async def contradiction(self, eid, kind, description, evidence, c):
        key = new_id("ctr")
        row = dict(
            contradiction_id=key,
            event_id=eid,
            type=kind,
            description=description,
            evidence=evidence,
            detected_at=iso(),
            **self.provenance(c),
        )
        await self.repo.put("ana.contradictions", key, row)
        return row

    async def add_observation(self, a, c):
        event = await self.require("evt.events", a["event_id"])
        item = await self.require("ingest.source_items", a["item_id"])
        link = a.get("link", a.get("link_type"))
        if link not in {"CORROBORATION", "STATE_UPDATE", "CONTRADICTION", "DENIAL"}:
            raise ValueError("Invalid observation link")
        if "public_at" not in a or "freshness" not in a:
            raise ValueError("V7 observation public_at and freshness required")
        public = self.validate_time(a, item)
        obs = new_id("obs")
        certainty = a.get("certainty", event["certainty"])
        claims, dropped = await self.validate_claims(a["claims"], item, c, certainty, obs, event["event_id"])
        verified_claims = [claim for claim in claims if claim["verified"]]
        confirmation_basis = self.event_fact_basis(
            event["statement"], claims, event["entities"], event["tokens_mentioned"]
        )
        if confirmation_basis and link not in {"CONTRADICTION", "DENIAL"}:
            event.update(
                certainty="HIGH",
                certainty_reason="Primary verification from accepted claims: "
                + ", ".join(confirmation_basis),
                verified_at=iso(),
            )
            event.setdefault("verification_history", []).append(
                {
                    "at": iso(),
                    "claim_ids": [claim["claim_id"] for claim in verified_claims],
                    "item_id": item["item_id"],
                    "certainty": "HIGH",
                }
            )
        before = event["event_state"]
        await self.apply_states(event, claims, c)
        contradictions = []
        if link == "DENIAL":
            if not any(r["verified"] for r in claims):
                raise ValueError("DENIAL requires accepted primary evidence")
            event["lifecycle"] = "INVALIDATED"
            contradictions = [
                await self.contradiction(
                    event["event_id"], "FATAL", "Official denial", [r["claim_id"] for r in claims], c
                )
            ]
        elif link == "CONTRADICTION" and claims:
            contradictions = [
                await self.contradiction(
                    event["event_id"],
                    "UNRESOLVED",
                    "Contradicting observation",
                    [r["claim_id"] for r in claims],
                    c,
                )
            ]
        elif link == "STATE_UPDATE" and event["event_state"] == before:
            link = "CORROBORATION"
        milestone = event.get("next_milestone")
        explicit = a.get("milestone_outcome")
        state_confirmation = event["event_state"] != before or link == "DENIAL"
        matching_source = bool(milestone and item["source_id"] == milestone["source_id"])
        if milestone and item.get("source_id") == "WEB" and item.get("evidence_kind") != "SEARCH_RESULT":
            source = next(
                (
                    source
                    for source in self.settings.load_config("sources")["sources"]
                    if source["source_id"] == milestone["source_id"]
                ),
                {},
            )
            host = (urlparse(item.get("url", "")).hostname or "").lower()
            matching_source = any(
                host == (urlparse(feed.get("official_index_url", feed.get("url", ""))).hostname or "").lower()
                for feed in source.get("feeds", [])
            )
        if (
            milestone
            and verified_claims
            and (state_confirmation or explicit)
            and matching_source
            and parse_time(item["observed_at"]) >= parse_time(milestone["due_at"]) - timedelta(minutes=5)
        ):
            quotes = [entry["quote"] for claim in verified_claims for entry in claim["evidence"]]
            if explicit and (
                not explicit.get("quote")
                or not any(normalized(explicit["quote"]) in normalized(quote) for quote in quotes)
            ):
                raise ValueError("Milestone outcome requires an accepted primary quote")
            resolution = dict(
                milestone,
                resolution_id=new_id("milestone"),
                status="CONFIRMED",
                confirmed_at=iso(),
                outcome=explicit["outcome"]
                if explicit
                else "Official denial"
                if link == "DENIAL"
                else event["event_state"],
                quotes=[explicit["quote"]] if explicit else quotes,
                evidence=[claim["claim_id"] for claim in verified_claims],
            )
            event.setdefault("milestone_resolutions", []).append(resolution)
            event.update(
                next_milestone=None,
                milestone_resolved=True,
                milestone_resolution_id=resolution["resolution_id"],
            )
            for escalation in await self.rows("ops.source_escalations", event["event_id"]):
                if escalation["source_id"] == milestone["source_id"] and escalation.get(
                    "trigger", ""
                ).startswith("MILESTONE:"):
                    escalation.update(status="CONFIRMED", confirmed_at=iso(), evidence=resolution["evidence"])
                    await self.repo.put("ops.source_escalations", escalation["escalation_id"], escalation)
        if claims and link != "CORROBORATION" and event["lifecycle"] != "INVALIDATED":
            event["lifecycle"] = "SCORED"
        await self.repo.put("evt.events", event["event_id"], event)
        await self.repo.put(
            "evt.event_observations",
            obs,
            dict(
                obs_id=obs,
                event_id=event["event_id"],
                item_id=item["item_id"],
                link_type=link,
                public_at=public,
                freshness=a["freshness"],
                match_method="AGENT",
                observed_at=item["observed_at"],
                **self.provenance(c),
            ),
        )
        return {
            "event_state": event["event_state"],
            "accepted_claims": claims,
            "dropped_claims": dropped,
            "contradictions": contradictions,
            "link_type": link,
        }

    async def merge_into(self, a, c):
        source = await self.require("evt.events", c["event_id"])
        target = await self.require("evt.events", a["event_id"])
        if source["event_id"] == target["event_id"]:
            return target
        if parse_time(target["known_to_us_at"]) > parse_time(source["known_to_us_at"]):
            raise ValueError("Merge target must be the earlier event")
        async with self.repo.lock("event:" + target["event_id"]):
            target["known_to_us_at"] = min(target["known_to_us_at"], source["known_to_us_at"], key=parse_time)
            for table, key in [("evt.event_observations", "obs_id"), ("evt.claims", "claim_id")]:
                for row in await self.rows(table, source["event_id"]):
                    row["event_id"] = target["event_id"]
                    await self.repo.put(table, row[key], row)
                    if key == "obs_id":
                        await self.repo.enqueue(
                            "AGENT_MESSAGE",
                            {
                                "kind": "ITEM_ADDED",
                                "event_id": target["event_id"],
                                "item_id": row["item_id"],
                                "session_id": target.get("session_id"),
                            },
                            "merge:" + row["obs_id"],
                        )
            source.update(lifecycle="MERGED", merged_into=target["event_id"])
            await self.repo.put("evt.events", source["event_id"], source)
            await self.repo.put("evt.events", target["event_id"], target)
        return target

    async def set_assessment(self, a, c):
        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        x = copy.deepcopy(a["assessment"])
        claims = await self.rows("evt.claims", eid)
        accepted = {r["claim_id"]: r for r in claims if r["accepted"]}
        criterion = x["criterion"]
        quote = x.get("criterion_quote", "")
        klass = x["class"]
        caps = []
        if klass not in {"LOW", "MATERIAL", "MAJOR", "STRUCTURAL_SHOCK"}:
            raise ValueError("Invalid EMS class")
        if criterion not in {
            "G1",
            "G2",
            "G3",
            "G4",
            "G5",
            "G6",
            "M1",
            "M2",
            "M3",
            "M4",
            "M5",
            "M6",
            "M7",
            "MATERIAL",
            "LOW",
        }:
            raise ValueError("Invalid class criterion")
        evidence_ok = bool(quote) and any(
            normalized(quote) in normalized(e["quote"]) for r in accepted.values() for e in r["evidence"]
        )
        if klass == "STRUCTURAL_SHOCK" and (
            event["certainty"] != "HIGH"
            or not event["state_verified"]
            or STATES.index(event["event_state"]) < 2
            or not criterion.startswith("G")
            or not evidence_ok
        ):
            klass = "MAJOR"
            caps.append("STRUCTURAL_GATE")
        if klass == "MAJOR" and (
            event["certainty"] == "LOW"
            or not evidence_ok
            or criterion not in {"M1", "M2", "M3", "M4", "M5", "M6", "M7", "G1", "G2", "G3", "G4", "G5", "G6"}
        ):
            klass = "MATERIAL"
            caps.append("MAJOR_EVIDENCE")
        if criterion == "M1" and not any(
            v in quote.casefold() for v in ["binance", "coinbase", "okx", "bybit", "kraken", "robinhood"]
        ):
            klass = "MATERIAL"
            caps.append("M1_VENUE")
        if (
            event["event_state"] == "PROPOSED"
            and criterion not in {"M4", "M5"}
            and klass in {"MAJOR", "STRUCTURAL_SHOCK"}
        ):
            klass = "MATERIAL"
            caps.append("PROPOSED")
        universe = {r["token_id"] for r in self.settings.load_config("registry")["tokens"]}
        current_maps = await self.rows("evt.beneficiary_maps", eid)
        current_map = max(current_maps, key=lambda row: int(row["map_version"])) if current_maps else None
        current_edges = [
            edge
            for edge in await self.rows("evt.beneficiary_edges", eid)
            if current_map and int(edge["map_version"]) == int(current_map["map_version"])
        ]
        concerned = set(event["tokens_mentioned"]) | {edge.get("token_id") for edge in current_edges}
        reaction_tokens = set(current_map["analysis_set"]) if current_map else set(event["tokens_mentioned"])
        if not concerned & universe:
            klass = "LOW"
            caps.append("NO_UNIVERSE_TOKEN")
        if event["certainty"] == "LOW" and klass in {"MAJOR", "STRUCTURAL_SHOCK"}:
            klass = "MATERIAL"
            caps.append("LOW_CERTAINTY")
        dims = x.get("dims", {})
        state_proof = [
            claim["claim_id"]
            for claim in accepted.values()
            if claim.get("state_claim") == event["event_state"]
        ]
        if isinstance(dims.get("implementation"), dict):
            dims["implementation"].update(
                value=IMPLEMENTATION[event["event_state"]] if state_proof else None,
                evidence=state_proof,
                reason=None if state_proof else "NO_ACCEPTED_STATE_PROOF",
            )
        for name in [
            "economic_directness",
            "magnitude",
            "permanence",
            "distribution_expansion",
            "mechanical_flows",
            "surprise",
            "implementation",
        ]:
            d = dims.get(name)
            if not isinstance(d, dict):
                raise ValueError("Every dimension requires value, rationale and evidence")
            if d.get("value") is not None and (
                not isinstance(d["value"], (int, float)) or not 0 <= d["value"] <= 10
            ):
                raise ValueError("Dimension outside 0..10")
            if not d.get("rationale") or any(ref not in accepted for ref in d.get("evidence", [])):
                raise ValueError("Dimension evidence/rationale invalid")
            if d.get("value") is not None and not d.get("evidence"):
                raise ValueError("Numeric dimension requires accepted claim evidence: " + name)
            if d.get("value") is None and not d.get("reason"):
                raise ValueError("Missing score requires reason")
        if dims["magnitude"].get("value") is not None and not re.search(
            r"\d", dims["magnitude"]["rationale"]
        ):
            dims["magnitude"].update(value=None, reason="NOT_ESTIMABLE")
        horizon = x.get("impact_horizon", {})
        if horizon.get("impact_start") == "NOW" and STATES.index(event["event_state"]) < STATES.index("LIVE"):
            raise ValueError("NOW requires LIVE or evidenced immediate market access")
        if horizon.get("ramp_shape") == "CONDITIONAL" and not horizon.get("dependencies"):
            raise ValueError("Conditional ramp requires dependencies")
        for chain in x.pop("capture_chains", []):
            score = chain.get("capture_score")
            mechanism = chain.get("token_mechanism", {})
            applied = []
            primary = any(accepted.get(ref, {}).get("verified") for ref in mechanism.get("evidence", []))
            if score is not None:
                if all(
                    not isinstance(chain.get(step), dict) or chain[step].get("value") is None
                    for step in ["eligible_activity", "effective_fees", "protocol_capture"]
                ) and not any(ref in accepted for ref in mechanism.get("evidence", [])):
                    score = min(score, 3)
                    applied.append("C1")
                if not primary:
                    score = min(score, 4)
                    applied.append("C2")
                if not mechanism.get("active"):
                    score = min(score, 5)
                    applied.append("C3")
                fees = chain.get("effective_fees")
                fees_value = fees.get("value", fees.get("number")) if isinstance(fees, dict) else fees
                if fees_value is None:
                    score = min(score, 7)
                    applied.append("C4")
            chain.update(
                event_id=eid,
                version=max(
                    (
                        int(r["version"])
                        for r in await self.rows("evt.capture_chains", eid)
                        if r["token_id"] == chain["token_id"]
                    ),
                    default=0,
                )
                + 1,
                capture_score=score,
                capture_class="UNKNOWN"
                if score is None
                else "DIRECT"
                if score >= 7 and primary and mechanism.get("active")
                else "INDIRECT"
                if score >= 4
                else "NARRATIVE_ONLY",
                conditional=not mechanism.get("active"),
                caps_applied=applied,
                **self.provenance(c),
            )
            await self.repo.put("evt.capture_chains", f"{eid}:{chain['token_id']}:{chain['version']}", chain)
        if "structural_gates" not in x:
            proof = (
                [
                    claim["claim_id"]
                    for claim in accepted.values()
                    if any(normalized(quote) in normalized(entry["quote"]) for entry in claim["evidence"])
                ]
                if quote
                else []
            )
            x["structural_gates"] = (
                [{"gate": criterion, "satisfied": bool(evidence_ok), "evidence": proof}]
                if criterion in {"G1", "G2", "G3", "G4", "G5", "G6"}
                else []
            )
        for gate in x["structural_gates"]:
            if any(ref not in accepted for ref in gate.get("evidence", [])):
                raise ValueError("Structural gate references unaccepted evidence")
            if gate.get("satisfied") and not gate.get("evidence"):
                raise ValueError("Satisfied structural gate requires accepted evidence")
        ems = new_id("ems")
        x.update(
            ems_id=ems,
            event_id=eid,
            version=max((int(r["version"]) for r in await self.rows("evt.ems_assessments", eid)), default=0)
            + 1,
            cutoff=iso(),
            dims=dims,
            **{"class": klass},
            class_reasons=x.get("class_reasons", []) + caps,
            **self.provenance(c),
        )
        await self.repo.put("evt.ems_assessments", ems, x)
        event.update(
            current_ems_id=ems,
            lifecycle="INVALIDATED"
            if event["lifecycle"] == "INVALIDATED"
            else "ANALYZING"
            if klass in {"MAJOR", "STRUCTURAL_SHOCK"}
            else "TRACKED",
        )
        await self.repo.put("evt.events", eid, event)
        if klass in {"MAJOR", "STRUCTURAL_SHOCK"}:
            anchor = (
                event["event_time"].get("value")
                if event["event_time"].get("precision") == "EXACT" and event["state_verified"]
                else event["public_at"]
            )
            for token in sorted(
                (reaction_tokens & universe)
                | {
                    t["token_id"]
                    for t in self.settings.load_config("registry")["tokens"]
                    if t.get("symbol") in {"BTC", "ETH"}
                }
            ):
                for label, minutes in [
                    ("PRE_24H", -1440),
                    ("PRE_4H", -240),
                    ("PRE_1H", -60),
                    ("POST_15M", 15),
                    ("POST_1H", 60),
                    ("POST_4H", 240),
                    ("POST_24H", 1440),
                    ("POST_72H", 4320),
                ]:
                    due = parse_time(anchor) + timedelta(minutes=minutes)
                    await self.repo.enqueue(
                        "REACTION_CAPTURE",
                        {
                            "event_id": eid,
                            "token_id": token,
                            "checkpoint": label,
                            "anchor": {
                                "type": "T0"
                                if event["event_time"].get("precision") == "EXACT" and event["state_verified"]
                                else "T1",
                                "ts": anchor,
                            },
                            "retro": due < utcnow(),
                        },
                        f"{eid}:{token}:{label}",
                        run_after=iso(max(due, utcnow())),
                        expires_at=iso(max(due, utcnow()) + timedelta(hours=6)),
                    )
            entity_ids = {entry["entity_id"] for entry in event["entities"]}
            registry = self.settings.load_config("registry")
            entity_ids.update(
                token.get("entity_id") for token in registry["tokens"] if token["token_id"] in concerned
            )
            source_ids = {
                sid
                for entity in registry["entities"]
                if entity["entity_id"] in entity_ids
                for sid in entity.get("official_sources", [])
            }
            for sid in source_ids:
                await self.escalate_source(sid, eid, "MAJOR", utcnow(), utcnow() + timedelta(hours=2), c)
            if klass == "STRUCTURAL_SHOCK":
                deadline = iso(parse_time(event["known_to_us_at"]) + timedelta(minutes=3))
                x.update(partial_required=True, first_card_deadline=deadline)
                event["first_card_deadline"] = deadline
                await self.repo.put("evt.events", eid, event)
        return x

    async def set_beneficiaries(self, a, c):
        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        version = (
            max((int(r["map_version"]) for r in await self.rows("evt.beneficiary_maps", eid)), default=0) + 1
        )
        claims = {r["claim_id"]: r for r in await self.rows("evt.claims", eid)}
        registry = self.settings.load_config("registry")
        tokens = {t["token_id"] for t in registry["tokens"]}
        chains = await self.rows("evt.capture_chains", eid)
        edges = []
        for raw in a["edges"]:
            e = copy.deepcopy(raw)
            if (
                isinstance(e.get("exposure_score"), bool)
                or not isinstance(e.get("exposure_score"), (int, float))
                or not 0 <= e["exposure_score"] <= 10
            ):
                raise ValueError("Exposure score must be numeric 0..10")
            if e["layer"] not in LAYERS or e["role"] not in {"PRIMARY", "SECONDARY", "COMPETITOR", "LOSER"}:
                raise ValueError("Invalid layer or beneficiary role")
            if not e.get("evidence") or any(
                ref not in claims or not claims[ref]["accepted"] for ref in e["evidence"]
            ):
                raise ValueError("Beneficiary requires accepted evidence")
            if not e.get("causal_chain"):
                raise ValueError("Beneficiary requires causal/share-loss mechanism")
            chain = max(
                (r for r in chains if r["token_id"] == e.get("token_id")),
                key=lambda r: int(r["version"]),
                default={},
            )
            score = chain.get("capture_score")
            state = e.get("integration_state", "COMPATIBLE")
            support = [claims[r] for r in e["evidence"]]
            supported = [r.get("state_claim") for r in support if r.get("state_claim") in INTEGRATION]
            state = max(supported, key=INTEGRATION.index) if supported else "COMPATIBLE"
            if (
                all(r.get("directionality") == "SOURCE_SIDE_SUPPORT" for r in support)
                and INTEGRATION.index(state) > 2
            ):
                state = "BUILT"
            e["tradable"] = e.get("token_id") in tokens and any(
                i["token_id"] == e.get("token_id") for i in registry["instruments"]
            )
            mature = (
                STATES.index(event["event_state"]) >= STATES.index("APPROVED")
                if e["role"] == "PRIMARY"
                else INTEGRATION.index(state) >= 2
            )
            e.update(
                edge_id=new_id("edge"),
                event_id=eid,
                map_version=version,
                integration_state=None if e["role"] == "PRIMARY" else state,
                capture_ref=[eid, e.get("token_id"), chain.get("version")],
                capture_score=score,
                capture_class=chain.get("capture_class", "UNKNOWN"),
                capture=chain.get("capture_class", "UNKNOWN"),
                mechanism_evidence=chain.get("token_mechanism", {}).get("evidence", []),
                credible=e["role"] in {"PRIMARY", "SECONDARY"}
                and score is not None
                and score >= 4
                and e["exposure_score"] >= 6
                and mature
                and e["edge_certainty"] in {"MEDIUM", "HIGH"}
                and e["tradable"],
                **self.provenance(c),
            )
            await self.repo.put("evt.beneficiary_edges", e["edge_id"], e)
            edges.append(e)
        primary = [e for e in edges if e["role"] == "PRIMARY" and e["tradable"]]
        secondary = sorted(
            [e for e in edges if e["role"] == "SECONDARY" and e["credible"]],
            key=lambda e: e["exposure_score"] * e["capture_score"],
            reverse=True,
        )[:4]
        losers = sorted(
            [e for e in edges if e["role"] == "LOSER" and e["tradable"]],
            key=lambda e: e["exposure_score"],
            reverse=True,
        )[:2]
        # Registry dependencies are compatibility candidates, never proof of adoption.
        context_entities = {e["entity_id"] for e in event["entities"]}
        dependency = self.settings.load_config("dependency_map")
        for dep in dependency.get("edges", []):
            if (
                dep.get("entity_id") not in context_entities
                and dep.get("token_id") not in event["tokens_mentioned"]
            ):
                continue
            for layer in dep.get("layers_served", []):
                if layer not in LAYERS or any(
                    e["entity_id"] == dep["entity_id"] and e["layer"] == layer for e in edges
                ):
                    continue
                e = dict(
                    edge_id=new_id("edge"),
                    event_id=eid,
                    map_version=version,
                    entity_id=dep["entity_id"],
                    token_id=dep.get("token_id"),
                    layer=layer,
                    role="SECONDARY",
                    direction="POSITIVE",
                    causal_chain=dep.get("description", "Registry compatibility candidate"),
                    integration_state="COMPATIBLE",
                    directionality="SOURCE_SIDE_SUPPORT",
                    exposure_score=4,
                    capture_score=None,
                    capture_class="UNKNOWN",
                    tradable=dep.get("token_id") in tokens
                    and any(i["token_id"] == dep.get("token_id") for i in registry["instruments"]),
                    edge_certainty="LOW",
                    credible=False,
                    credibility_reasons=[
                        "Dependency-map compatibility alone does not establish adoption or capture"
                    ],
                    evidence=[],
                    dependency_ref=sha256(dep),
                    **self.provenance(c),
                )
                await self.repo.put("evt.beneficiary_edges", e["edge_id"], e)
                edges.append(e)
        notes = a.get("layer_reasons", {})
        layers = {}
        for layer in LAYERS:
            found = [e["edge_id"] for e in edges if e["layer"] == layer]
            credible = [edge for edge in edges if edge["layer"] == layer and edge.get("credible")]
            if not credible and not notes.get(layer):
                raise ValueError(f"Layer {layer} requires NONE/UNKNOWN reason")
            layers[layer] = {
                "status": "FOUND" if credible else a.get("layer_statuses", {}).get(layer, "UNKNOWN"),
                "edges": found,
                "note": notes.get(layer, "Evidence-backed edges"),
            }
        result = dict(
            event_id=eid,
            map_version=version,
            cutoff=iso(),
            layers=layers,
            no_credible_secondary_reason=None
            if secondary
            else a.get("no_credible_secondary_reason")
            or "No SECONDARY edge satisfies the credibility requirements in §4.7. "
            + "; ".join(layer + ": " + layers[layer]["note"] for layer in LAYERS),
            analysis_set=list(dict.fromkeys(e["token_id"] for e in primary + secondary + losers)),
            **self.provenance(c),
        )
        await self.repo.put("evt.beneficiary_maps", f"{eid}:{version}", result)
        return result

    async def managed_tokens(self, event_id):
        calls = await self.rows("ana.calls", event_id)
        active = {row["token_id"] for row in calls if row.get("status") == "OPEN"}
        event = await self.repo.get("evt.events", event_id) or {}
        pending = set(event.get("call_expired_ids", []))
        active.update(row["token_id"] for row in calls if row["call_id"] in pending)
        first_full = await self.first_alert(event_id, "FULL")
        shown = set(first_full.get("shown_tokens", []) if first_full else [])
        return active | shown

    async def get_market_truth(self, a, c):
        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        assessment = await self.require("evt.ems_assessments", event["current_ems_id"])
        managed = await self.managed_tokens(eid)
        if assessment["class"] not in {"MAJOR", "STRUCTURAL_SHOCK"} and not set(a["tokens"]) <= managed:
            raise ValueError("Market escalation requires MAJOR+ or existing call/shown-token management")
        maps = await self.rows("evt.beneficiary_maps", eid)
        allowed = set(
            max(maps, key=lambda r: int(r["map_version"]))["analysis_set"]
            if maps
            else event["tokens_mentioned"]
        )
        allowed |= await self.managed_tokens(eid)
        if not set(a["tokens"]) <= allowed:
            raise ValueError("Tokens outside analysis set")
        cutoff = iso(a["cutoff"]) if a.get("cutoff") else iso(utcnow().replace(second=0, microsecond=0))
        cutoff_minute = iso(parse_time(cutoff).replace(second=0, microsecond=0))
        async with self.repo.transaction():
            jobs = []
            for token in a["tokens"]:
                escalation = await self.repo.enqueue(
                    "ESCALATE_TOKENS",
                    {"event_id": eid, "token_id": token, "tokens": [token]},
                    f"{token}:{eid}",
                    priority=0,
                    max_attempts=3,
                )
                jobs.append(
                    await self.repo.enqueue(
                        "RUN_MARKET_TRUTH",
                        {
                            "event_id": eid,
                            "token_id": token,
                            "cutoff": cutoff,
                            "_depends_on": [escalation["job_id"]],
                        },
                        f"{eid}:{token}:{cutoff_minute}",
                        priority=1,
                        max_attempts=2,
                        expires_at=iso(utcnow() + timedelta(minutes=30)),
                    )
                )
        truths = []
        for job in jobs:
            deadline = asyncio.get_running_loop().time() + 120
            while True:
                current = await self.require("ops.jobs", job["job_id"])
                if current["status"] == "DONE":
                    result = current.get("result")
                    if result is None:
                        raise ValueError("Market truth job completed without persisted result")
                    truths.append(result)
                    break
                if current["status"] in {"FAILED", "EXPIRED"}:
                    raise ValueError(
                        "Market truth job is terminally failed or expired; inspect its durable job record"
                    )
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError("Market truth jobs are pending; retry this call with the same call_id")
                await asyncio.sleep(0.25)
        if maps:
            mapping = max(maps, key=lambda r: int(r["map_version"]))
            edges = [
                e
                for e in await self.rows("evt.beneficiary_edges", eid)
                if int(e["map_version"]) == int(mapping["map_version"])
            ]
            ranks = {"NEGATIVE": 0, "UNPRICED": 1, "EARLY": 2, "SUBSTANTIAL": 3, "HEAVY": 4}
            candidates = []
            for edge in edges:
                mt = next((t for t in truths if t["token_id"] == edge.get("token_id")), None)
                if edge.get("credible") and mt and mt.get("liquidity", {}).get("liquidity_state") == "NORMAL":
                    relative = mt.get("relative", {})
                    band = relative.get("repricing_band")
                    pct = (relative.get("windows", {}).get("EVENT") or {}).get("pctile")
                    if band in ranks and pct is not None:
                        candidates.append((ranks[band], float(pct), -edge["exposure_score"], edge["edge_id"]))
            mapping["least_repriced_edge"] = min(candidates)[-1] if candidates else None
            mapping["least_repriced_reason"] = (
                "Lowest repricing band, event percentile, then highest exposure among credible NORMAL-liquidity beneficiaries"
                if candidates
                else "No credible beneficiary has complete repricing and NORMAL liquidity evidence"
            )
            await self.repo.put("evt.beneficiary_maps", f"{eid}:{mapping['map_version']}", mapping)
        return {"market_truth": truths, "requested_cutoff": cutoff, "cutoff_minute": cutoff_minute}

    async def get_decision(self, a, c):
        eid = a["event_id"]
        token = a.get("token", a.get("token_id"))
        event = await self.require("evt.events", eid)
        event["contradictions"] = [
            row for row in await self.rows("ana.contradictions", eid) if row.get("token_id") in {None, token}
        ]
        assessment = await self.require("evt.ems_assessments", event["current_ems_id"])
        edges = [r for r in await self.rows("evt.beneficiary_edges", eid) if r.get("token_id") == token]
        latest_truth = await self.latest_record("ana.market_truth", eid, token)
        maps = await self.rows("evt.beneficiary_maps", eid)
        current_version = max((int(m["map_version"]) for m in maps), default=None)
        historical_edges = edges
        edges = [e for e in edges if int(e["map_version"]) == current_version]
        management_only = not edges and token in await self.managed_tokens(eid)
        if management_only and historical_edges:
            edges = [max(historical_edges, key=lambda row: int(row["map_version"]))]
        if not edges or latest_truth is None:
            raise ValueError("Decision requires current-map beneficiary and market truth")
        calls = [
            r for r in await self.rows("ana.calls", eid) if r["token_id"] == token and r["status"] == "OPEN"
        ]
        conditions = [r for r in await self.rows("ana.watch_conditions", eid) if r["token_id"] == token]
        expired_ids = set(event.get("call_expired_ids", []))
        expired_calls = [
            row
            for row in await self.rows("ana.calls", eid)
            if row["token_id"] == token
            and row.get("closed_by") == "EXPIRED"
            and (not expired_ids or row["call_id"] in expired_ids)
        ]
        accepted_observations = {
            row["obs_id"] for row in await self.rows("evt.claims", eid) if row.get("accepted")
        }
        material_observations = [
            row
            for row in await self.rows("evt.event_observations", eid)
            if row.get("link_type") in {"STATE_UPDATE", "DENIAL", "CONTRADICTION"}
            and row["obs_id"] in accepted_observations
        ]
        latest_expiry = max(
            (parse_time(row["closed_at"]) for row in expired_calls if row.get("closed_at")), default=None
        )
        new_fact_after_expiry = latest_expiry is not None and any(
            parse_time(row["observed_at"]) > latest_expiry for row in material_observations
        )
        if expired_calls and not calls and not new_fact_after_expiry:
            frozen = await self.latest_record("ana.decisions", eid, token)
            if not frozen:
                raise ValueError("Expired call requires its frozen decision")
            result = copy.deepcopy(frozen)
            result.update(
                dec_id=new_id("dec"),
                cutoff=iso(),
                tracking_ended=True,
                actionable=False,
                conditions=[],
                conditions_shown=[],
                previous_decision_id=frozen["dec_id"],
                expired_call_ids=[row["call_id"] for row in expired_calls],
                **self.provenance(c),
            )
            await self.reconcile_conditions(result, conditions, [], c)
            await self.repo.put("ana.decisions", result["dec_id"], result)
            return result
        result = self.engine.evaluate(
            event,
            assessment,
            max(
                edges,
                key=lambda e: (
                    e.get("role") == "PRIMARY",
                    e.get("credible", False),
                    e.get("exposure_score", 0),
                ),
            ),
            latest_truth,
            open_call=max(calls, key=lambda r: parse_time(r["opened_at"])) if calls else None,
            conditions=[
                condition
                for condition in conditions
                if condition.get("status") == "FIRED" and not condition.get("consumed_by")
            ],
        )
        if management_only:
            result["management_only"] = True
            if not calls:
                result["actionable"] = False
                result["removed_from_analysis"] = True

        result.update(
            dec_id=result.get("dec_id", new_id("dec")),
            event_id=eid,
            token_id=token,
            market_truth_id=latest_truth["mta_id"],
            mta_id=latest_truth["mta_id"],
            trade_reasons=result.get(
                "trade_reasons", [result["trade_rule"]] if result.get("trade_rule") else []
            ),
            contradictions=[
                row["contradiction_id"]
                for row in event.get("contradictions", [])
                if not row.get("resolved_at")
            ],
            cutoff=result.get("cutoff", iso()),
            **self.provenance(c),
        )
        relative = latest_truth.get("relative", {})
        anchor = (relative.get("anchor") or {}).get("ref_price")
        entry = relative.get("entry_ref_price", anchor)
        if entry is not None and anchor is not None:
            result.update(
                entry_ref_price=str(entry),
                invalidation_price=str(min(Decimal(str(anchor)), Decimal(str(entry)))),
            )
        await self.reconcile_conditions(result, conditions, calls, c)
        if isinstance(result.get("outlook"), dict):
            outlook = dict(
                result["outlook"],
                out_id=new_id("out"),
                event_id=eid,
                token_id=token,
                cutoff=result["cutoff"],
                **self.provenance(c),
            )
            await self.repo.put("ana.outlooks", outlook["out_id"], outlook)
            result["outlook_id"] = outlook["out_id"]
        await self.repo.put("ana.decisions", result["dec_id"], result)
        return result

    async def reconcile_conditions(self, result, existing, calls, context):
        """Keep one durable watch per expression; firings are consumed exactly once."""
        now = utcnow()
        expiry = now + timedelta(days=7)
        if calls:
            expiry = min(expiry, max(parse_time(call["opened_at"]) for call in calls) + timedelta(days=7))
        for old in existing:
            if old.get("status") == "FIRED" and not old.get("consumed_by"):
                old.update(consumed_by=result["dec_id"], consumed_at=iso(now))
                await self.repo.put("ana.watch_conditions", old["cond_id"], old)
        reconciled = []
        used = set()
        for proposed in result.get("conditions", []):
            signature = canonical_json({"role": proposed["role"], "expr": proposed["expr"]})
            matches = [
                old
                for old in existing
                if old.get("status") in {"ARMED", "FIRED"}
                and parse_time(old.get("expires_at", iso(expiry))) > now
                and canonical_json({"role": old["role"], "expr": old["expr"]}) == signature
            ]
            if matches:
                condition = max(matches, key=lambda old: parse_time(old.get("created_at", iso(now))))
                condition.setdefault("created_at", iso(now))
                condition.setdefault("expires_at", iso(expiry))
                condition["last_shown_by"] = result["dec_id"]
            else:
                due = proposed.get("expr", {}).get("due_at")
                deadline = min(expiry, parse_time(due) + timedelta(hours=1)) if due else expiry
                condition = dict(
                    proposed,
                    cond_id=proposed.get("cond_id", new_id("cond")),
                    event_id=result["event_id"],
                    token_id=result["token_id"],
                    created_by=result["dec_id"],
                    created_at=iso(now),
                    expires_at=iso(deadline),
                    **self.provenance(context),
                )
            used.add(condition["cond_id"])
            reconciled.append(condition)
            await self.repo.put("ana.watch_conditions", condition["cond_id"], condition)
        for old in existing:
            if old["cond_id"] not in used and old.get("status") == "ARMED":
                old.update(status="CANCELLED", cancelled_at=iso(now), superseded_by=result["dec_id"])
                await self.repo.put("ana.watch_conditions", old["cond_id"], old)
        result["conditions"] = reconciled
        result["conditions_shown"] = [condition["cond_id"] for condition in reconciled][:2]
        if isinstance(result.get("outlook"), dict):
            result["outlook"]["conditions"] = {
                role.lower(): [condition["cond_id"] for condition in reconciled if condition["role"] == role]
                for role in ["UPGRADE", "DOWNGRADE"]
            }

    async def publish_card(self, a, c):
        from tank.analysis.cards import alert_gate, compose_card, lint_card

        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        assessment = await self.require("evt.ems_assessments", event["current_ems_id"])
        if event["lifecycle"] in {"CLOSED", "MERGED"}:
            return {"published": False, "reason": "TERMINAL_EVENT"}
        event["thresholds"] = self.settings.load_config("thresholds")
        newest = await self.repo.list_latest("out.alerts", "generated_at", filters={"event_id": eid}, limit=1)
        previous = newest[0] if newest else None
        initial_full = await self.first_alert(eid, "FULL")
        active_maps = await self.rows("evt.beneficiary_maps", eid)
        active_tokens = (
            set(max(active_maps, key=lambda row: int(row["map_version"]))["analysis_set"])
            if active_maps
            else set(event.get("tokens_mentioned", []))
        )
        active_tokens |= await self.managed_tokens(eid)
        decisions = []
        for token in sorted(active_tokens):
            decision = await self.latest_record("ana.decisions", eid, token)
            if decision:
                decisions.append(decision)
        event["claims"] = await self.rows("evt.claims", eid)
        event["contradictions"] = await self.rows("ana.contradictions", eid)
        last_sent = (
            await self.repo.latest_before(
                "out.deliveries",
                "at",
                iso(utcnow() + timedelta(microseconds=1)),
                filters={"alert_id": previous["alert_id"], "channel": "telegram:alerts", "status": "SENT"},
            )
            if previous
            else None
        )
        comparison = (
            await self.repo.get("out.alerts", f"{last_sent['alert_id']}:{last_sent['version']}")
            if last_sent
            else None
        )
        gate_previous = None
        if comparison or previous:
            baseline = comparison or previous
            gate_previous = dict(
                baseline,
                decisions=[await self.require("ana.decisions", ref) for ref in baseline["decisions"]],
                published_at=last_sent["at"] if last_sent else None,
                ems_class=baseline["card_struct"]["assessment"]["class"],
                event_state=baseline["card_struct"]["event"]["event_state"],
                state_verified=baseline["card_struct"]["event"].get("state_verified"),
                negative_marker=baseline["card_struct"]["event"].get("negative_marker"),
            )
        kind = a["kind"]
        if kind == "PARTIAL" and previous:
            return {"published": False, "reason": "FIRST_CARD_ALREADY_GENERATED"}
        completing_partial = kind == "FULL" and previous and previous["kind"] == "PARTIAL"
        pinned_for_gate = initial_full.get("shown_tokens", []) if initial_full else []
        gate_decisions = (
            [row for row in decisions if row["token_id"] in pinned_for_gate]
            if a["kind"] == "UPDATE" and pinned_for_gate
            else decisions
        )
        ok, reason = alert_gate(
            event,
            assessment,
            previous=None if completing_partial else gate_previous,
            decisions=gate_decisions,
        )
        deferred = not ok and reason == "UPDATE_RATE_LIMIT" and a["kind"] == "UPDATE"
        if not ok and not deferred:
            return {"published": False, "reason": reason}
        immediate = any(
            row.get("type") == "FATAL" and not row.get("resolved_at") for row in event["contradictions"]
        ) or any(
            row.get("action") in {"START", "EXIT"}
            and not row.get("tracking_ended")
            and row.get("actionable") is not False
            for row in gate_decisions
        )
        delivery_not_before = (
            iso(
                parse_time(last_sent["at"])
                + timedelta(seconds=event["thresholds"].get("update_rate_limit_s", 900))
            )
            if deferred and last_sent
            else iso()
        )
        kind = a["kind"]
        if kind not in {"FULL", "PARTIAL", "UPDATE"}:
            raise ValueError("Invalid alert kind")
        if kind == "UPDATE" and not previous:
            raise ValueError("UPDATE requires a published original")
        if kind == "PARTIAL" and assessment["class"] != "STRUCTURAL_SHOCK":
            raise ValueError("PARTIAL requires STRUCTURAL_SHOCK")
        beneficiaries = await self.rows("evt.beneficiary_edges", eid)
        current_maps = await self.rows("evt.beneficiary_maps", eid)
        if current_maps:
            event["beneficiary_map"] = max(current_maps, key=lambda row: int(row["map_version"]))
        map_version = max((int(m["map_version"]) for m in current_maps), default=None)
        beneficiaries = [e for e in beneficiaries if int(e["map_version"]) == map_version]
        event["beneficiary_edges"] = copy.deepcopy(beneficiaries)
        maps = await self.rows("evt.beneficiary_maps", eid)
        least = max(maps, key=lambda r: int(r["map_version"])).get("least_repriced_edge") if maps else None
        all_edges = await self.rows("evt.beneficiary_edges", eid)
        pinned = initial_full.get("shown_tokens", []) if initial_full else []
        if kind == "UPDATE" and pinned:
            beneficiaries = []
            for token in pinned:
                candidates = [edge for edge in all_edges if edge.get("token_id") == token]
                if candidates:
                    beneficiaries.append(max(candidates, key=lambda row: int(row["map_version"])))
        elif kind == "PARTIAL":
            beneficiaries = []
        else:
            primaries = sorted(
                [edge for edge in beneficiaries if edge["role"] == "PRIMARY" and edge.get("tradable")],
                key=lambda row: -row.get("exposure_score", 0),
            )
            secondaries = sorted(
                [edge for edge in beneficiaries if edge["role"] == "SECONDARY" and edge.get("credible")],
                key=lambda row: (row["edge_id"] != least, -row.get("exposure_score", 0)),
            )
            remaining = sorted(
                [
                    edge
                    for edge in beneficiaries
                    if edge["role"] in {"SECONDARY", "LOSER"}
                    and edge.get("tradable")
                    and (edge["role"] == "LOSER" or edge.get("credible"))
                ],
                key=lambda row: -row.get("exposure_score", 0),
            )
            selected = primaries[:1] + secondaries[:1]
            used = {edge.get("token_id") for edge in selected}
            selected += next(([edge] for edge in remaining if edge.get("token_id") not in used), [])
            beneficiaries = selected[:3]
        truth_by_token = {}
        for token in sorted(active_tokens):
            decision = next((row for row in decisions if row["token_id"] == token), None)
            truth = (
                await self.repo.get("ana.market_truth", decision["market_truth_id"])
                if decision and decision.get("market_truth_id")
                else await self.latest_record("ana.market_truth", eid, token)
            )
            if truth:
                truth_by_token[token] = truth
        truths = list(truth_by_token.values())
        if kind == "FULL":
            required = (
                set(max(current_maps, key=lambda row: int(row["map_version"]))["analysis_set"])
                if current_maps
                else set()
            )
            decision_tokens = {decision["token_id"] for decision in decisions}
            missing_truth = required - set(truth_by_token)
            missing_decision = required - decision_tokens
            if not current_maps or missing_truth or missing_decision:
                return {
                    "published": False,
                    "reason": "ANALYSIS_INCOMPLETE",
                    "missing_market_truth": sorted(missing_truth),
                    "missing_decisions": sorted(missing_decision),
                }
        ctx = {
            "event": event,
            "assessment": assessment,
            "beneficiaries": beneficiaries,
            "decisions": decisions,
            "market_truths": truths,
        }
        verdict = a.get("verdict", "")
        why = a.get("why", [])
        if kind != "PARTIAL" and len(why) not in {2, 3}:
            raise ValueError("FULL/UPDATE require 2–3 WHY bullets")
        text = compose_card(
            event, assessment, beneficiaries, decisions, truths, kind=kind, verdict=verdict, why=why
        )
        violations = lint_card(text, ctx)
        previous_publish = [
            row
            for row in (
                await self.rows("ops.tool_calls", filters={"session_id": c["session_id"]})
                if c.get("session_id")
                else []
            )
            if row.get("tool", row.get("tool_name")) == "publish_card"
            and row.get("arguments", row.get("input", {})).get("event_id") == eid
            and isinstance(row.get("result"), dict)
        ]
        last_publish = (
            max(
                previous_publish,
                key=lambda row: parse_time(
                    row.get("completed_at", row.get("answered_at", row.get("started_at")))
                ),
            )
            if previous_publish
            else None
        )
        rejected = (
            [last_publish]
            if last_publish
            and last_publish["result"].get("reason") == "LINTER"
            and last_publish.get("arguments", last_publish.get("input", {})).get("kind") == kind
            else []
        )
        removed = []
        if violations and rejected:
            bad = {v["sentence"] for v in violations}
            removed = list(bad)
            verdict = "" if any(verdict in line for line in bad) else verdict
            why = [w for w in why if not any(w in line for line in bad)]
            text = compose_card(
                event, assessment, beneficiaries, decisions, truths, kind=kind, verdict=verdict, why=why
            )
            violations = lint_card(text, ctx)
        if violations:
            return {"published": False, "reason": "LINTER", "violations": violations}
        if removed:
            ctx["removed_prose"] = removed
        previous_failed = previous and bool(
            await self.repo.list_latest(
                "out.deliveries",
                "at",
                filters={
                    "alert_id": previous["alert_id"],
                    "version": previous["version"],
                    "status": "FAILED",
                },
                limit=1,
            )
        )
        if (
            previous
            and last_sent
            and not previous_failed
            and previous["version"] > last_sent["version"]
            and previous["card_text"] == text
        ):
            return {
                "published": True,
                "alert_id": previous["alert_id"],
                "version": previous["version"],
                "reason": "IDENTICAL_PENDING",
                "deferred": previous.get("rate_limited", False),
                "delivery_not_before": previous.get("delivery_not_before"),
            }
        cutoff = iso()
        observations = await self.rows("evt.event_observations", eid)
        claims = await self.rows("evt.claims", eid)
        ids = {o["item_id"] for o in observations} | {e["item_id"] for r in claims for e in r["evidence"]}
        bundle = {
            "cutoff": cutoff,
            "event": event,
            "source_items": [await self.require("ingest.source_items", i) for i in sorted(ids)],
            "observations": observations,
            "claims": claims,
            "state_changes": await self.rows("evt.state_changes", eid),
            "ems": assessment,
            "capture_chains": await self.rows("evt.capture_chains", eid),
            "beneficiary_map": await self.rows("evt.beneficiary_maps", eid),
            "beneficiary_edges": all_edges,
            "market_truth": truths,
            "outlooks": [
                (await self.require("ana.outlooks", decision["outlook_id"]))
                if decision.get("outlook_id")
                else decision["outlook"]
                for decision in decisions
                if decision.get("outlook_id") or isinstance(decision.get("outlook"), dict)
            ],
            "decisions": decisions,
            "contradictions": await self.rows("ana.contradictions", eid),
            "data_confidence": [m["data_confidence"] for m in truths],
            "unresolved_fields": [],
            "config_hash": self.settings.config_version,
            "analysis_inputs_hash": sha256(
                {
                    name: self.settings.load_config(name)
                    for name in ["registry", "thresholds", "semantics", "dependency_map"]
                }
            ),
            "code_version": self.settings.code_version,
            "model_ids": [],
            "agent_version": c.get("agent_version"),
            "skill_version": c.get("skill_version"),
            "session_id": c.get("session_id"),
            "tool_call_ids": [c["call_id"]] if c.get("call_id") else [],
            "request_log": [],
        }
        calls_by_key = {}
        for row in await self.rows("ops.tool_calls", eid):
            calls_by_key[(row.get("session_id"), row.get("call_id"))] = row
        if c.get("session_id"):
            for row in await self.rows("ops.tool_calls", filters={"session_id": c["session_id"]}):
                calls_by_key[(row.get("session_id"), row.get("call_id"))] = row
        calls = list(calls_by_key.values())
        producing_records = [
            event,
            assessment,
            *claims,
            *bundle["source_items"],
            *bundle["observations"],
            *bundle["state_changes"],
            *bundle["capture_chains"],
            *bundle["beneficiary_map"],
            *bundle["beneficiary_edges"],
            *bundle["market_truth"],
            *bundle["outlooks"],
            *bundle["decisions"],
        ]
        producing_sessions = {
            record.get("session_id") for record in producing_records if record.get("session_id")
        }
        if c.get("session_id"):
            producing_sessions.add(c["session_id"])
        sessions = [
            row
            for sid in sorted(producing_sessions)
            if (row := await self.repo.get("ops.agent_sessions", sid)) is not None
        ]
        producers = producing_records + sessions + [c]
        bundle["model_ids"] = sorted({row["model_id"] for row in producers if row.get("model_id")})
        bundle["prompt_versions"] = sorted(
            {row["prompt_version"] for row in producers if row.get("prompt_version")}
        )
        bundle["agent_versions"] = sorted(
            {row["agent_version"] for row in producers if row.get("agent_version")}
        )
        bundle["config_versions"] = sorted(
            {row["config_version"] for row in producers if row.get("config_version")}
        )
        bundle["code_versions"] = sorted(
            {row["code_version"] for row in producers if row.get("code_version")}
        )
        if bundle["agent_version"] is None and len(bundle["agent_versions"]) == 1:
            bundle["agent_version"] = bundle["agent_versions"][0]
        if not bundle["model_ids"]:
            bundle["unresolved_fields"].append(
                {"field": "model_ids", "reason": "No producing record or session declares a model"}
            )
        if bundle["skill_version"] is None:
            bundle["unresolved_fields"].append(
                {
                    "field": "skill_version",
                    "reason": "Producing session did not expose a deployed skill version",
                }
            )
        if bundle["agent_version"] is None:
            bundle["unresolved_fields"].append(
                {
                    "field": "agent_version",
                    "reason": "Producing session did not expose a deployed agent version",
                }
            )
        bundle["request_log"] = calls
        bundle["tool_call_ids"] = sorted(
            set([r["call_id"] for r in calls if r.get("call_id")] + bundle["tool_call_ids"])
        )
        # Canonical ordering makes the exact frozen payload independently hashable.
        for k, v in bundle.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                v.sort(key=canonical_json)
        digest = sha256(bundle)
        await self.repo.put(
            "out.evidence_bundles",
            digest,
            {"bundle_id": "EVB-" + digest, "sha256": digest, "payload": bundle},
            immutable=True,
        )
        aid = previous["alert_id"] if previous else new_id("alert")
        version = previous["version"] + 1 if previous else 1
        alert = dict(
            alert_id=aid,
            version=version,
            kind=kind,
            delivery_not_before=delivery_not_before,
            rate_limited=deferred,
            delivery_immediate=immediate,
            shown_tokens=[edge["token_id"] for edge in beneficiaries],
            expired_call_ids=event.get("call_expired_ids", []) if event.get("call_expired") else [],
            milestone_resolution_id=event.get("milestone_resolution_id")
            if event.get("milestone_resolved")
            else None,
            terminal_correction=kind == "UPDATE"
            and event["lifecycle"] == "INVALIDATED"
            and any(r["type"] == "FATAL" for r in event["contradictions"]),
            event_id=eid,
            generated_at=cutoff,
            card_text=text,
            card_struct=ctx,
            decisions=[
                d["dec_id"]
                for d in decisions
                if d["token_id"] in {edge["token_id"] for edge in beneficiaries}
            ],
            evidence_bundle_id=digest,
            linter={
                "passed": True,
                "violations": [],
                "regenerations": len(rejected),
                "removed_prose": removed,
            },
            supersedes_version=version - 1 if previous else None,
            **self.provenance(c),
        )
        await self.repo.put("out.alerts", f"{aid}:{version}", alert, immutable=True)
        delivery = new_id("delivery")
        await self.repo.put(
            "out.deliveries",
            delivery,
            {
                "delivery_id": delivery,
                "alert_id": aid,
                "version": version,
                "channel": "telegram:alerts",
                "status": "GENERATED",
                "attempt": 0,
                "at": cutoff,
            },
            immutable=True,
        )
        await self.repo.enqueue(
            "DELIVER",
            {"alert_id": aid, "version": version, "channel": "telegram:alerts"},
            f"{aid}:{version}:telegram:alerts",
            priority=0,
            run_after=delivery_not_before,
            max_attempts=5,
        )
        return {
            "published": True,
            "alert_id": aid,
            "version": version,
            "evidence_bundle_id": digest,
            "deferred": deferred,
            "delivery_not_before": delivery_not_before,
        }

    async def escalate_source(self, source_id, event_id, trigger, starts_at, expires_at, context):
        key = f"{source_id}:{event_id}:{trigger}"
        row = dict(
            escalation_id=key,
            source_id=source_id,
            event_id=event_id,
            trigger=trigger,
            status="ACTIVE",
            starts_at=iso(starts_at),
            expires_at=iso(expires_at),
            interval_s=15,
            **self.provenance(context),
        )
        return await self.repo.put("ops.source_escalations", key, row)

    async def schedule_milestone(self, a, c):
        event = await self.require("evt.events", a["event_id"])
        due = iso(a["due_at"])
        source = a["source"]
        source_id = source.get("source_id") if isinstance(source, dict) else source
        if not await self.repo.get("reg.sources", source_id) and not any(
            source["source_id"] == source_id for source in self.settings.load_config("sources")["sources"]
        ):
            raise ValueError("Milestone source must be registered")
        event["next_milestone"] = {"kind": a["milestone"], "due_at": due, "source_id": source_id}
        await self.repo.put("evt.events", event["event_id"], event)
        job = await self.repo.enqueue(
            "MILESTONE_CHECK",
            {
                "event_id": event["event_id"],
                "milestone": a["milestone"],
                "due_at": due,
                "source_id": source_id,
            },
            f"{event['event_id']}:{a['milestone']}:{due}",
            run_after=due,
            expires_at=iso(max(parse_time(due), utcnow()) + timedelta(hours=2)),
        )
        await self.escalate_source(
            source_id,
            event["event_id"],
            "MILESTONE:" + a["milestone"] + ":" + due,
            parse_time(due) - timedelta(minutes=5),
            parse_time(due) + timedelta(hours=2),
            c,
        )
        return {"job_id": job["job_id"]}

    async def record_review(self, a, c):
        if c.get("role") != "scout" or c.get("proposal_id") != a["proposal_id"]:
            raise ValueError("Scout review requires its proposal session")
        row = await self.require("scout.expansion_proposals", a["proposal_id"])
        scores = a.get("scores", {})
        if set(scores) != {"overlap", "observability", "reusability", "decision_value"} or any(
            isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 10
            for value in scores.values()
        ):
            raise ValueError("Scout review requires four numeric scores 0..10")
        if a.get("recommendation") not in {"SUBCATEGORY", "TEST_MORE", "NEW_CATEGORY", "REJECT"}:
            raise ValueError("Invalid scout recommendation")
        row["recommendation"] = a["recommendation"]
        duplicate = a.get("duplicate_of")
        if duplicate:
            if row.get("duplicate_of") not in {None, duplicate}:
                raise ValueError("Duplicate target is already recorded")
            if duplicate == row["prp_id"]:
                raise ValueError("Proposal cannot duplicate itself")
            target = await self.require("scout.expansion_proposals", duplicate)
            if target.get("human_action"):
                raise ValueError("Duplicate target must be open")
            if not row.get("duplicate_of"):
                target["occurrences"] += row["occurrences"]
                target["evidence"] = list(dict.fromkeys(target["evidence"] + row["evidence"]))
                await self.repo.put("scout.expansion_proposals", duplicate, target)
            row["duplicate_of"] = duplicate
        row["adversarial_review"] = {
            "scores": scores,
            "recommendation": a["recommendation"],
            "duplicate_of": duplicate,
            "verdict": a["verdict"],
            "reason": a["reason"],
            "evidence": a["evidence"],
            **self.provenance(c),
        }
        await self.repo.put("scout.expansion_proposals", row["prp_id"], row)
        return row

    async def propose_pattern(self, a, c):
        subject = a["subject"]
        name = subject if isinstance(subject, str) else subject["concept_name"]
        async with self.repo.lock("scout:" + sha256(name.casefold())):
            return await self._propose_pattern(a, c)

    async def _propose_pattern(self, a, c):
        if a["hook"] not in {
            "BENEFICIARY_LAYER",
            "MARKET_TRUTH_STATE",
            "SOURCE_OPPORTUNITY",
            "ALTERNATIVE_EXPLANATION",
        }:
            raise ValueError("Unknown scout hook")
        if not a.get("evidence"):
            raise ValueError("Scout evidence required")
        subject = a["subject"]
        subject = {"concept_name": subject} if isinstance(subject, str) else subject
        existing = next(
            (
                p
                for p in await self.rows("scout.expansion_proposals")
                if p["concept_name"].casefold() == subject["concept_name"].casefold()
                and not p.get("human_action")
            ),
            None,
        )
        if existing and existing.get("duplicate_of"):
            existing = await self.require("scout.expansion_proposals", existing["duplicate_of"])
        row = existing or dict(
            prp_id=new_id("prp"),
            hook=a["hook"],
            **subject,
            occurrences=0,
            evidence=[],
            recommendation="SUBCATEGORY",
            confidence="LOW",
            **self.provenance(c),
        )
        row["occurrences"] += 1
        row["evidence"] += a["evidence"]
        await self.repo.put("scout.expansion_proposals", row["prp_id"], row)
        await self.repo.enqueue(
            "SCOUT_REVIEW", {"prp_id": row["prp_id"]}, f"{row['prp_id']}:{row['occurrences']}", priority=4
        )
        return row


class _borrow:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, *args):
        return False


def validate_schema(value, schema, path="$"):
    """Validate the JSON-schema subset used by the checked-in tool declarations."""
    kinds = {
        "object": dict,
        "array": list,
        "string": str,
        "boolean": bool,
        "integer": int,
        "number": (int, float),
        "null": type(None),
    }
    declared = schema.get("type")
    declared = [declared] if isinstance(declared, str) else declared
    if declared and not any(
        isinstance(value, kinds[t]) and not (t in {"integer", "number"} and isinstance(value, bool))
        for t in declared
    ):
        raise ValueError(f"{path}: invalid type")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError(f"{path}: invalid enum")
    if isinstance(value, dict):
        for key in schema.get("required", []):
            if key not in value:
                raise ValueError(f"{path}: missing {key}")
        props = schema.get("properties", {})
        if schema.get("additionalProperties") is False and set(value) - set(props):
            raise ValueError(f"{path}: unknown fields {sorted(set(value) - set(props))}")
        for key, item in value.items():
            if key in props:
                validate_schema(item, props[key], path + "." + key)
    if isinstance(value, list):
        for n, item in enumerate(value):
            validate_schema(item, schema.get("items", {}), f"{path}[{n}]")
    for key, op in [("minimum", lambda a, b: a < b), ("maximum", lambda a, b: a > b)]:
        if (
            key in schema
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
            and op(value, schema[key])
        ):
            raise ValueError(f"{path}: violates {key}")
