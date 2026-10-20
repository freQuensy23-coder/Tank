"""Version-pinned Jev classification. Cutoffs must come from labeled calibration."""

import asyncio
import re
from datetime import timedelta
from decimal import Decimal

import httpx

from tank.time_precision import collection_clock, offset_schedule, propagate_timestamp
from tank.util import iso, sha256, utcnow

from .jev import MODEL, classify, provider_binding

JEV_PROMPT_VERSION = "jev-v1"

FILTER = "Is this text about crypto - crypto assets, crypto exchanges, crypto regulation or enforcement, blockchain networks, or one of these projects: Bitcoin, Ethereum, Uniswap, PancakeSwap, Aerodrome, Curve, 1inch, 0x, CoW Protocol, Hyperliquid, Lighter, Aave, Morpho, Euler, Ondo, LayerZero, Quant, Avalanche, NEAR, Plasma, Ethena, World (Worldcoin)? Whether it reports anything new or important does not matter here. General finance, technology or politics that does not touch crypto does not count."
ROUTE = "Does the item report the same real-world event as the event described, or a new development of it - a confirmation, denial, stage change, new detail or new party? Another event about the same project does not count."


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
    def state(item):
        return {
            "source": item.get("source") or item.get("source_name") or item.get("source_id"),
            "author": item.get("author")
            or item.get("source")
            or item.get("source_name")
            or item.get("source_id"),
            "text": "\n\n".join(
                dict.fromkeys(str(v) for v in (item.get("title"), item.get("text") or item.get("body")) if v)
            ),
            "quoted": item.get("quoted"),
        }

    async def score(self, item, name, state):
        return (await self._score_record(item, name, state))["score"]

    def score_identity(self, item, name, state):
        questions = {
            name: {
                "type": "noul",
                "instructions": FILTER if name == "relevant" else ROUTE,
                "criteria": {
                    "true": "About crypto" if name == "relevant" else "Same event or a development of it",
                    "false": "Not about crypto" if name == "relevant" else "A different event",
                },
            }
        }
        binding = provider_binding(getattr(self.settings, "jev_provider", "openrouter"))
        state_fingerprint = sha256(state)
        question_fingerprint = sha256(questions)
        request_fingerprint = sha256({"model": binding.request_model, "state": state, "questions": questions})
        key = sha256(
            {
                "item_id": item["item_id"],
                "model": binding.request_model,
                "provider": binding.provider,
                "base_url": binding.base_url,
                "prompt_version": JEV_PROMPT_VERSION,
                "questions": questions,
                "state_fingerprint": state_fingerprint,
            }
        )
        return key, questions, binding, state_fingerprint, question_fingerprint, request_fingerprint

    async def _score_record(self, item, name, state):
        key, questions, binding, state_fingerprint, question_fingerprint, request_fingerprint = (
            self.score_identity(item, name, state)
        )
        cached = await self.repo.get("ops.llm_usage", key)
        if cached:
            return cached
        if getattr(self.settings, "replay", False):
            raise ValueError(
                "Replay requires stored Jev score for the exact wire question and state; live classification is disabled"
            )
        api_key = getattr(self.settings, "openrouter_api_key", None)
        if not api_key:
            raise ConfigurationError("Jev requires OPENROUTER_API_KEY or TANK_OPENROUTER_API_KEY")
        data = await classify(
            self.http, api_key=api_key, state=state, questions=questions, provider=binding.provider
        )
        score = float(data["answers"][name]["noul"])
        if not 0 <= score <= 1 or data.get("model") not in binding.resolved_models:
            raise ValueError("Jev returned invalid score or unexpected resolved model")
        usage = data.get("usage", {})
        tokens = usage.get("input_tokens")
        reported_cost = usage.get("cost")
        amount = Decimal(str(reported_cost)) if reported_cost is not None else None
        if amount is not None and (not amount.is_finite() or amount < 0):
            raise ValueError("Invalid OpenRouter reported Jev cost")
        cost = str(amount) if amount is not None else None
        cost_reason = None if amount is not None else "OpenRouter omitted billed usage.cost"
        billing_basis = "OPENROUTER_REPORTED"
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
                "requested_model_id": binding.request_model,
                "jev_provider": binding.provider,
                "endpoint": binding.base_url + "/v1/systemone",
                "resolved_model_id": data["model"],
                "response_id": data.get("id"),
                "upstream_provider": data.get("provider"),
                "model_pin_evidence": "OpenRouter dated 1.13 snapshot; upstream patch suffix is not exposed",
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
            # Source promotion can happen during the model request. Keep network
            # work outside the shared ingestion lock, then re-read before writing.
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
        result = {e["entity_id"] if isinstance(e, dict) else e for e in item.get("entities", [])} | set(
            item.get("tokens_mentioned", item.get("tokens", []))
        )

        def matches(name):
            return name and re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", text, re.IGNORECASE)

        for row in registry.get("entities", []):
            entity = row.get("entity_id")
            # Display names are not matching rules: e.g. "near" needs its
            # registered qualifier. The frozen registry is the only alias policy.
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

    @classmethod
    def routing_state(cls, item, event):
        return {
            **cls.state(item),
            "event": {
                "statement": event.get("statement"),
                "entities": event.get("entities", []),
                "tokens": event.get("tokens_mentioned", event.get("tokens", [])),
                "stage": event.get("event_state", event.get("stage")),
                "known_to_us_at": event.get("known_to_us_at"),
            },
        }

    async def _candidate_events(self, identities, since, now):
        # iter_range is half-open; PostgreSQL timestamps have microsecond
        # resolution. Include the batch clock itself without admitting future rows.
        recent = {
            row["event_id"]
            async for row in self.repo.iter_range(
                "ops.agent_sessions", "last_item_at", start=since, end=iso(now + timedelta(microseconds=1))
            )
            if row.get("event_id")
        }
        events = []
        # Preserve stable event-key order, including the production tie rule.
        for event_id in sorted(recent):
            event = await self.repo.get("evt.events", event_id)
            if not event or event.get("lifecycle", event.get("status")) in (
                "CLOSED",
                "MERGED",
                "INVALIDATED",
                "FAILED",
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
                # Each exact item/event question is independent. Collect every
                # result before selecting so a failed pair cannot yield a partial
                # winner; successful usage rows remain available on ordinary retry.
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
                # Reduce in the stable candidate order, independent of completion
                # order, retaining the first event on equal probabilities.
                for event, score in zip(candidates, results, strict=True):
                    scores[event["event_id"]] = score
                    if score >= cutoff and score > best_score:
                        best, best_score = event, score
            # §3.1: only extracted events are candidates. With no qualifying
            # candidate, enqueue NEW_ITEM immediately, identified by this item.
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
                # A new observation or official-source promotion may arrive
                # during Jev's call. Preserve that current ingestion record.
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
                await self.repo.enqueue(
                    "AGENT_MESSAGE", message, item["item_id"] + ":" + message["kind"], priority=priority(item)
                )
            return item

    async def close(self):
        if self.owns_http:
            await self.http.aclose()
