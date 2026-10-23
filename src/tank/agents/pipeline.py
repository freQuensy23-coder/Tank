"""Version-pinned Jev classification (03 §3.1). Cutoffs come from config/thresholds.yaml."""

import asyncio
import re
from datetime import timedelta
from decimal import Decimal

import httpx

from tank.agents.jev import MODEL, binding, classify
from tank.ops_progress import item_progress
from tank.time_precision import collection_clock, offset_schedule, propagate_timestamp
from tank.util import iso, sha256, utcnow

JEV_PROMPT_VERSION = "jev-v2"


FILTER = "Is `item` about crypto: crypto assets, crypto exchanges, crypto regulation or enforcement, blockchain networks, or one of these projects: Bitcoin, Ethereum, Uniswap, PancakeSwap, Aerodrome, Curve, 1inch, 0x, CoW Protocol, Hyperliquid, Lighter, Aave, Morpho, Euler, Ondo, LayerZero, Quant, Avalanche, NEAR, Plasma, Ethena, World (Worldcoin)?"
FILTER_CRITERIA = {
    "true": "About crypto, even if it reports nothing new or important.",
    "false": "General finance, technology or politics that does not touch crypto.",
}
ROUTE = "Does `item` report the same real-world event as `event`, or a new development of that event?"
ROUTE_CRITERIA = {
    "true": "The same event, or a development of it: a confirmation, denial, stage change, new detail or new party.",
    "false": "A different event, even one about the same project, company, exchange or token.",
}
TEXT_LIMIT = 12000


def _clean(value):
    return " ".join(str(value or "").split())


def _words(value):
    return re.findall(r"\w+", value.casefold())


def _truncate(text, limit=TEXT_LIMIT):
    if len(text) <= limit:
        return text
    cut = text[:limit]
    end = max(cut.rfind(". "), cut.rfind("? "), cut.rfind("! "))
    return (cut[: end + 1] if end > limit // 2 else cut[: cut.rfind(" ")]) + " …"


class ConfigurationError(ValueError):
    pass


def priority(item):
    sid = item.get("source_id")
    deferred = item.get(
        "deferrable",
        sid in {"S30", "S31", "S32", "S33", "S34"}
        or (sid == "S25" and item.get("subtype") == "commits")
        or (sid == "S29" and item.get("author_role") == "OTHER"),
    )
    return 2 if deferred and item.get("event_class") not in ("MAJOR", "STRUCTURAL_SHOCK") else 1


class Pipeline:
    def __init__(self, repo, settings, *, http=None):
        self.repo, self.settings = repo, settings
        self.http = http or httpx.AsyncClient(timeout=15)
        self.owns_http = http is None

    def cutoff(self, name):
        value = self.settings.load_config("thresholds").get(name)
        if value is None or isinstance(value, bool) or not 0 <= float(value) <= 1:
            raise ConfigurationError(f"{name} requires a calibrated probability")
        return float(value)

    @staticmethod
    def item_view(item):
        """The item as Jev sees it: source, author when it adds anything, title, body once, quote."""
        source = _clean(item.get("source") or item.get("source_name") or item.get("source_id"))
        author = _clean(item.get("author"))
        title = _clean(item.get("title"))
        body = _clean(item.get("text") or item.get("body"))

        if title and source:
            for separator in (" - ", " | ", " — "):
                if title.casefold().endswith((separator + source).casefold()):
                    title = title[: -len(separator + source)].rstrip()
        if body.casefold().startswith(title.casefold()):
            body = body[len(title) :].lstrip(" -|:—")
        if set(_words(body)) <= set(_words(title)) | set(_words(source)):
            body = ""
        view = {"source": source}
        if author and author.casefold() != source.casefold():
            view["author"] = author
        if title:
            view["title"] = title
        if body:
            view["text"] = _truncate(body)
        if _clean(item.get("quoted")):
            view["quoted"] = _truncate(_clean(item["quoted"]))
        return view

    @classmethod
    def state(cls, item):
        return {"item": cls.item_view(item)}

    async def score(self, item, name, state):
        return (await self._score_record(item, name, state))["score"]

    def score_identity(self, item, name, state):
        questions = {
            name: {
                "type": "noul",
                "instructions": FILTER if name == "relevant" else ROUTE,
                "criteria": FILTER_CRITERIA if name == "relevant" else ROUTE_CRITERIA,
            }
        }
        pinned = binding(getattr(self.settings, "opencode_api_key", None))
        state_fingerprint = sha256(state)
        question_fingerprint = sha256(questions)
        request_fingerprint = sha256({"model": pinned.request_model, "state": state, "questions": questions})
        key = sha256(
            {
                "item_id": item["item_id"],
                "model": pinned.request_model,
                "provider": pinned.provider,
                "base_url": pinned.base_url,
                "prompt_version": JEV_PROMPT_VERSION,
                "questions": questions,
                "state_fingerprint": state_fingerprint,
            }
        )
        return key, questions, pinned, state_fingerprint, question_fingerprint, request_fingerprint

    async def _score_record(self, item, name, state):
        key, questions, pinned, state_fingerprint, question_fingerprint, request_fingerprint = (
            self.score_identity(item, name, state)
        )
        cached = await self.repo.get("ops.llm_usage", key)
        if cached:
            return cached
        if getattr(self.settings, "replay", False):
            raise ValueError(
                "Replay requires stored Jev score for the exact wire question and state; live classification is disabled"
            )
        api_key = getattr(self.settings, "opencode_api_key", None)
        data = await classify(self.http, api_key=api_key, state=state, questions=questions)
        score = float(data["answers"][name]["noul"])
        if not 0 <= score <= 1 or data.get("model") not in pinned.resolved_models:
            raise ValueError("Jev returned invalid score or unexpected resolved model")
        usage = data.get("usage", {})
        tokens = usage.get("input_tokens")
        reported_cost = usage.get("cost")
        amount = Decimal(str(reported_cost)) if reported_cost is not None else None
        if amount is not None and (not amount.is_finite() or amount < 0):
            raise ValueError("Invalid provider-reported Jev cost")
        cost = str(amount) if amount is not None else None
        cost_reason = None if amount is not None else "OpenCode does not report usage.cost"
        billing_basis = "OPENCODE_REPORTED"
        completed_at = collection_clock()
        record = await self.repo.put(
            "ops.llm_usage",
            key,
            {
                "usage_id": key,
                "item_id": item["item_id"],
                "call": "FILTER" if name == "relevant" else "ROUTE",
                "model_id": data["model"],
                "semantic_model_id": MODEL,
                "requested_model_id": pinned.request_model,
                "jev_provider": pinned.provider,
                "endpoint": pinned.base_url + "/v1/systemone",
                "resolved_model_id": data["model"],
                "response_id": data.get("id"),
                "upstream_provider": data.get("provider"),
                "model_pin_evidence": "OpenCode serves Jev 1.13 under this model name; no dated snapshot or patch suffix is exposed",
                "billing_basis": billing_basis,
                "billing_currency": "USD",
                "prompt_version": JEV_PROMPT_VERSION,
                "questions": questions,
                "state_fingerprint": state_fingerprint,
                "question_fingerprint": question_fingerprint,
                "request_fingerprint": request_fingerprint,
                "session_id": None,
                "score": score,
                "usage": usage,
                "input_tokens": tokens,
                "output_tokens": usage.get("output_tokens"),
                "effort": None,
                "stop_reason": "NON_GENERATIVE_CLASSIFICATION",
                "web_searches": 0,
                "missing_fields": {
                    "effort": "Not applicable to nongenerative Jev classification",
                    **({"cost_usd": cost_reason} if cost is None else {}),
                    **({"input_tokens": "Provider omitted input tokens"} if tokens is None else {}),
                    **(
                        {"output_tokens": "Provider omitted output tokens"}
                        if usage.get("output_tokens") is None
                        else {}
                    ),
                    "cache_read_tokens": "Not exposed by Jev API",
                    "cache_write_tokens": "Not exposed by Jev API",
                },
                "cost_usd": cost,
                "cost_reason": cost_reason,
                **completed_at.fields("at"),
                **completed_at.fields("created_at"),
                "subject_id": item["item_id"],
                "model": data["model"],
                "config_version": self.settings.config_version,
                "code_version": self.settings.code_version,
            },
            immutable=True,
        )
        return record

    async def filter_item(self, payload):
        async with self.repo.lock("filter:" + payload["item_id"]):
            item = await self.repo.get("ingest.source_items", payload["item_id"])
            if item is None:
                raise KeyError(payload["item_id"])

            score = None
            if item.get("relevance", {}).get("decision") != "ALWAYS":
                cutoff = self.cutoff("filter_cutoff")
                score_record = await self._score_record(item, "relevant", self.state(item))
                score = score_record["score"]
            async with self.repo.lock("ingest-dedup"), self.repo.transaction():
                item = await self.repo.get("ingest.source_items", payload["item_id"])
                if item is None:
                    raise KeyError(payload["item_id"])
                promoted = item.get("relevance", {}).get("decision") == "ALWAYS"
                if not promoted:
                    if score < cutoff:
                        item["extraction_status"] = "SKIPPED"
                    item.update(
                        filter_score=score,
                        relevance={
                            "decision": "SEND" if score >= cutoff else "SKIP",
                            "p_relevant": score,
                            "model": score_record["resolved_model_id"],
                            "provider": score_record["jev_provider"],
                            "requested_model": score_record["requested_model_id"],
                            "semantic_model_id": score_record["semantic_model_id"],
                            "model_pin_evidence": score_record["model_pin_evidence"],
                            "usage_id": score_record["usage_id"],
                        },
                        status="RELEVANT" if score >= cutoff else "SKIPPED",
                    )
                    await self.repo.put("ingest.source_items", item["item_id"], item)
                await item_progress(
                    self.repo,
                    self.settings,
                    item["item_id"],
                    "JEV_BYPASSED" if promoted else "JEV_PASSED" if score >= cutoff else "JEV_REJECTED",
                    "Источник проходит напрямую к агенту"
                    if promoted
                    else f"score={score:.4f}; порог={cutoff:.4f}",
                )
                if promoted or score >= cutoff:
                    await self.repo.enqueue(
                        "ROUTE_ITEM",
                        {"item_id": item["item_id"]},
                        item["item_id"],
                        priority=priority(item),
                        max_attempts=2,
                        expires_at=offset_schedule(
                            propagate_timestamp(item, "observed_at")
                            if item.get("observed_at")
                            else collection_clock(),
                            timedelta(hours=6),
                        ),
                    )
            return item

    def identities(self, item):
        registry = self.settings.load_config("registry")
        text = " ".join(
            str(item.get(k) or "") for k in ("title", "text", "body", "quoted", "author_organization")
        )
        raw_tokens = item.get("tokens_mentioned", item.get("tokens", []))
        result = {e["entity_id"] if isinstance(e, dict) else e for e in item.get("entities", [])} | set(
            raw_tokens
        )

        symbols = {
            str(row.get("symbol", "")).casefold(): row["token_id"] for row in registry.get("tokens", [])
        }
        for value in raw_tokens:
            if isinstance(value, str) and value.removeprefix("$").casefold() in symbols:
                result.add(symbols[value.removeprefix("$").casefold()])

        def matches(name):
            return name and re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", text, re.IGNORECASE)

        for row in registry.get("entities", []):
            entity = row.get("entity_id")

            hit = any(matches(name) for name in row.get("aliases", [])) or any(
                matches(rule["alias"]) and any(matches(q) for q in rule["requires_any"])
                for rule in row.get("qualified_aliases", [])
            )
            if hit:
                result.update([entity, *row.get("tokens", [])])
        for row in registry.get("tokens", []):
            if row.get("symbol") and matches("$" + row["symbol"]):
                result.update([row.get("entity_id"), row["token_id"]])
        authors = {
            str(item.get(key) or "").lstrip("@").lower()
            for key in ("author", "monitored_author", "reposted_by")
        }
        for account in registry.get("x_accounts", []):
            if str(account["handle"]).lstrip("@").lower() in authors:
                result.add(account.get("entity_id"))
        for row in registry.get("tokens", []):
            if row.get("token_id") in result or row.get("entity_id") in result:
                result.update([row.get("entity_id"), row.get("token_id")])
        return result - {None}

    def event_view(self, event):
        """The candidate event as Jev sees it: the agent's statement, readable names, stage."""
        registry = self.settings.load_config("registry")
        names = {
            row["entity_id"]: (row.get("aliases") or row.get("names") or [row["entity_id"]])[0]
            for row in registry.get("entities", [])
        }
        symbols = {row["token_id"]: row.get("symbol") for row in registry.get("tokens", [])}

        def label(value, table):
            key = value.get("entity_id") if isinstance(value, dict) else value
            key = str(key).removeprefix("UNRESOLVED:")
            return table.get(key) or key.split(":")[-1]

        view = {"statement": _clean(event.get("statement"))}
        entities = list(dict.fromkeys(label(e, names) for e in event.get("entities", [])))
        tokens = list(
            dict.fromkeys(label(t, symbols) for t in event.get("tokens_mentioned", event.get("tokens", [])))
        )
        if entities:
            view["entities"] = entities
        if tokens:
            view["tokens"] = tokens
        stage = event.get("event_state", event.get("stage"))
        if stage:
            view["stage"] = str(stage).lower()
        return view

    def routing_state(self, item, event):
        return {"item": self.item_view(item), "event": self.event_view(event)}

    async def _candidate_events(self, identities, since, now):

        recent = {
            row["event_id"]
            async for row in self.repo.iter_range(
                "ops.agent_sessions", "last_item_at", start=since, end=iso(now + timedelta(microseconds=1))
            )
            if row.get("event_id") and row.get("status") != "FAILED"
        }
        events = []

        for event_id in sorted(recent):
            event = await self.repo.get("evt.events", event_id)
            if not event or event.get("lifecycle", event.get("status")) in (
                "CLOSED",
                "MERGED",
                "INVALIDATED",
            ):
                continue
            event_ids = {
                e["entity_id"] if isinstance(e, dict) else e for e in event.get("entities", [])
            } | set(event.get("tokens_mentioned", event.get("tokens", [])))
            if identities.intersection(event_ids):
                events.append(event)
        return events

    async def candidate_events(self, item):
        """The exact online candidate builder, also used by isolated calibration."""
        now = utcnow()
        since = iso(
            now - timedelta(days=self.settings.load_config("thresholds").get("routing_window_days", 3))
        )
        return await self._candidate_events(self.identities(item), since, now)

    async def route_item(self, payload):
        async with self.repo.lock("routing:" + payload["item_id"]):
            item = await self.repo.get("ingest.source_items", payload["item_id"])
            if item is None:
                raise KeyError(payload["item_id"])
            if item.get("routing_message"):
                return item
            identities = self.identities(item)
            now = utcnow()
            since = iso(
                now - timedelta(days=self.settings.load_config("thresholds").get("routing_window_days", 3))
            )
            best, best_score = None, -1
            scores = {}
            candidates = tuple(await self._candidate_events(identities, since, now))
            if candidates:
                cutoff = self.cutoff("routing_cutoff")

                requests = [
                    asyncio.create_task(self.score(item, "same_event", self.routing_state(item, event)))
                    for event in candidates
                ]
                try:
                    results = await asyncio.gather(*requests, return_exceptions=True)
                except BaseException:
                    for request in requests:
                        request.cancel()
                    await asyncio.gather(*requests, return_exceptions=True)
                    raise
                for result in results:
                    if isinstance(result, BaseException):
                        raise result

                for event, score in zip(candidates, results, strict=True):
                    scores[event["event_id"]] = score
                    if score >= cutoff and score > best_score:
                        best, best_score = event, score

            added = best is not None
            message = {"kind": "ITEM_ADDED" if added else "NEW_ITEM", "item_id": item["item_id"]}
            if best:
                message["event_id"] = best["event_id"]
            route = {
                "decision": "ADDED" if added else "NEW_SESSION",
                "event_id": best["event_id"] if best else None,
                "p_same": best_score if added else None,
            }
            async with self.repo.lock("ingest-dedup"), self.repo.transaction():
                item = await self.repo.get("ingest.source_items", payload["item_id"])
                if item is None:
                    raise KeyError(payload["item_id"])
                item.update(
                    route=route,
                    routing_message=message,
                    routing_scores=scores,
                    route_event_id=best["event_id"] if best else None,
                    status="ROUTED",
                )
                await self.repo.put("ingest.source_items", item["item_id"], item)
                await item_progress(
                    self.repo,
                    self.settings,
                    item["item_id"],
                    "AGENT_QUEUED",
                    "Маршрутизация: существующее событие" if added else "Маршрутизация: новая сессия",
                    event_id=route["event_id"],
                )
                await self.repo.enqueue(
                    "AGENT_MESSAGE", message, item["item_id"] + ":" + message["kind"], priority=priority(item)
                )
            return item

    async def close(self):
        if self.owns_http:
            await self.http.aclose()
