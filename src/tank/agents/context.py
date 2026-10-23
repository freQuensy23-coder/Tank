"""Human-readable model inputs; durable identities are resolved only on the host."""

import copy
import re

CONTEXT_VERSION = 1
SCOPED_TOOLS = {
    "add_observation",
    "set_assessment",
    "set_beneficiaries",
    "get_market_truth",
    "get_decision",
    "publish_card",
    "schedule_milestone",
    "set_watch_condition",
}
PRIVATE_FIELDS = {
    "provenance",
    "prompt_binding",
    "prompt_label",
    "routing_scores",
    "routing_message",
    "route",
    "raw_ref",
    "raw_path",
    "storage_key",
    "measurement_metadata",
    "measurement_metadata_access",
    "full_tool_result",
    "session_code_version",
    "session_config_version",
    "code_version",
    "config_version",
    "prompt_version",
    "agent_version",
    "model_id_basis",
    "model_id_reason",
    "agent_version_basis",
    "agent_version_reason",
    "prompt_version_basis",
    "prompt_version_reason",
    "fixture_reference_provenance",
    "native_execution_states",
    "sessions",
    "parameters",
    "collection",
    "relevance",
    "content_base64",
    "tools_sha256",
    "publication_receipt",
    "dependency_ref",
    "discovery_code_version",
    "discovery_config_version",
    "model_pin_evidence",
    "external_id",
    "feed",
    "consumed_by",
    "retired_message_ids",
    "canonical_key",
    "metric_key",
    "snapshot_key",
    "defillama_protocol_slugs",
    "defillama_protocol_slug",
    "fee_protocol_slug",
    "unlock_slug",
}
LITERAL_FIELDS = {"text", "quoted", "title", "quote", "url", "query"}
OPAQUE = re.compile(
    r"\b(?:ITM|EVT|OBS|CLM|EMS|EDG|MTA|DEC|OUT|ALR|CND|STC|CAL|PRP|DLV|REQ|CTR|REV|FIX)-[A-Z0-9]{26}\b"
    r"|\b(?:session|agent|skill|call|env|file|tgf|web|search|evt|itm|clm|obs|ems|mta|dec|cond|prp)_[A-Za-z0-9_-]{20,}\b"
    r"|\b[0-9a-fA-F]{32,64}\b"
    r"|\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b"
)


def identifier_field(key):
    name = re.sub(r"([a-z])([A-Z])", r"\1_\2", str(key)).lower()
    return name in {"id", "ids", "hash", "hashes", "sha256", "version"} or name.endswith(
        ("_id", "_ids", "_hash", "_hashes", "_sha256", "_version", "_ref")
    )


def private_field(key):
    return str(key) in PRIVATE_FIELDS or identifier_field(key)


def clean_text(text):
    return "".join(
        part if part.startswith(("http://", "https://")) else OPAQUE.sub("[internal reference omitted]", part)
        for part in re.split(r"(https?://[^\s<>\"]+)", text)
    )


def clean_value(value):
    """Final recursive guard for diagnostics, prose and unknown provider fields."""
    if isinstance(value, dict):
        return {
            clean_text(str(key)): item
            if key in LITERAL_FIELDS and isinstance(item, str)
            else clean_value(item)
            for key, item in value.items()
            if not private_field(key) and not OPAQUE.search(str(key))
        }
    if isinstance(value, list):
        return [clean_value(item) for item in value]
    return clean_text(value) if isinstance(value, str) else value


def reject_private_input(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if identifier_field(key):
                raise ValueError(
                    "Use source URLs, verbatim quotes and project names; internal fields are not accepted"
                )
            if key not in LITERAL_FIELDS or not isinstance(item, str):
                reject_private_input(item)
    elif isinstance(value, list):
        for item in value:
            reject_private_input(item)
    elif isinstance(value, str) and clean_text(value) != value:
        raise ValueError("Use the displayed source or claim description, not an internal reference")


def document_reference(row):
    description = {key: row[key] for key in ("url", "title", "observed_at") if row.get(key)}
    if not row.get("url") and not row.get("title") and row.get("text"):
        description["excerpt"] = str(row["text"])[:200]
    return description


def event_reference(row):
    return {key: row[key] for key in ("statement", "public_at") if row.get(key)}


def claim_reference(row):
    return {key: row[key] for key in ("quote", "subject", "predicate", "object") if row.get(key)}


class ModelContext:
    def __init__(self, repo, settings, session):
        self.repo, self.settings, self.session = repo, settings, session
        try:
            self.registry = settings.load_config("registry")
        except (KeyError, AttributeError):
            self.registry = {}
        self.tokens = {r["token_id"]: r for r in self.registry.get("tokens", [])}
        self.entities = {r["entity_id"]: r for r in self.registry.get("entities", [])}
        self.instruments = {r["instrument_id"]: r for r in self.registry.get("instruments", [])}
        try:
            self.sources = {r["source_id"]: r for r in settings.load_config("sources").get("sources", [])}
        except (KeyError, AttributeError):
            self.sources = {}
        self.catalog = copy.deepcopy(session.get("model_catalog") or {})
        self.additions = {}

    def token_name(self, key):
        row = self.tokens.get(key)
        if row:
            return f"{row.get('project') or row.get('name') or row.get('symbol')} ({row.get('symbol')})"
        return str(key).removeprefix("UNRESOLVED:").rsplit(":", 1)[-1]

    def entity_name(self, key):
        row = self.entities.get(key)
        return (
            (row.get("name") or (row.get("aliases") or row.get("names") or [key])[0])
            if row
            else str(key).removeprefix("UNRESOLVED:")
        )

    def instrument_name(self, key):
        row = self.instruments.get(key)
        if row:
            return " ".join(str(row.get(k) or "") for k in ("venue", "market_type", "venue_symbol")).strip()
        return " ".join(str(key).split(":"))

    def source_name(self, key):
        row = self.sources.get(key) or {}
        return row.get("name") or row.get("url")

    def remember(self, kind, key, description):
        if key and description:
            description = clean_value(description)
            self.catalog.setdefault(kind, {})[key] = description
            self.additions.setdefault(kind, {})[key] = description

    async def save(self):
        if not self.additions or not self.session.get("session_id"):
            return
        sid = self.session["session_id"]
        async with self.repo.lock("session:" + sid):
            stored = await self.repo.get("ops.agent_sessions", sid)
            if stored:
                catalog = stored.setdefault("model_catalog", {})
                for kind, values in self.additions.items():
                    catalog.setdefault(kind, {}).update(values)
                await self.repo.put("ops.agent_sessions", sid, stored)

    async def describe_claim(self, row):
        description = claim_reference(row)
        sources = []
        for entry in row.get("evidence") or []:
            if isinstance(entry, dict) and entry.get("item_id"):
                source = await self.reference("documents", entry["item_id"])
                if source and source not in sources:
                    sources.append(source)
        if sources:
            description["sources"] = sources
        if "verified" in row:
            description["verified"] = row["verified"]
        if row.get("state_claim"):
            description["stage"] = row["state_claim"]
        return description

    async def reference(self, kind, key):
        if key in self.catalog.get(kind, {}):
            return self.catalog[kind][key]
        table, describe = {
            "documents": ("ingest.source_items", document_reference),
            "claims": ("evt.claims", claim_reference),
            "events": ("evt.events", event_reference),
            "conditions": (
                "ana.watch_conditions",
                lambda r: {"text": r.get("human_text"), "conditions": r.get("expr")},
            ),
            "proposals": ("scout.expansion_proposals", lambda r: {"subject": r.get("concept_name")}),
        }[kind]
        row = await self.repo.get(table, key) if key else None
        if row:
            description = await self.describe_claim(row) if kind == "claims" else describe(row)
            self.remember(kind, key, description)
            return description
        return None

    async def project(self, value):
        if isinstance(value, list):
            return [await self.project(item) for item in value]
        if isinstance(value, str):
            if value in self.tokens:
                return self.token_name(value)
            if value in self.instruments:
                return self.instrument_name(value)
            if value in self.entities:
                return self.entity_name(value)
            if value in self.sources:
                return self.source_name(value)
            for kind in ("claims", "documents", "events", "conditions", "proposals"):
                if OPAQUE.fullmatch(value) and value in self.catalog.get(kind, {}):
                    return clean_value(self.catalog[kind][value])
            return clean_text(value)
        if not isinstance(value, dict):
            return value
        result = {}
        for kind, field, describe, label in (
            ("documents", "item_id", document_reference, "document"),
            ("claims", "claim_id", claim_reference, "claim"),
            ("events", "event_id", event_reference, None),
            (
                "conditions",
                "cond_id",
                lambda r: {"text": r.get("human_text"), "conditions": r.get("expr")},
                "condition",
            ),
            ("proposals", "prp_id", lambda r: {"subject": r.get("concept_name")}, "proposal"),
        ):
            if value.get(field):
                description = await self.describe_claim(value) if kind == "claims" else describe(value)
                if not any(description.values()):
                    description = await self.reference(kind, value[field])
                if description:
                    self.remember(kind, value[field], description)
                    if label:
                        result[label] = clean_value(description)
        for key, item in value.items():
            if key == "token_id" and item:
                result["token"] = self.token_name(item)
            elif key == "token_ids" and item:
                result["tokens"] = [self.token_name(v) for v in item]
            elif key == "instrument_ids" and item:
                result["instruments"] = [self.instrument_name(v) for v in item]
            elif key == "entity_ids" and item:
                result["entities"] = [self.entity_name(v) for v in item]
            elif key == "entity_id" and item:
                result["entity"] = self.entity_name(item)
            elif key == "instrument_id" and item:
                result["instrument"] = self.instrument_name(item)
            elif key == "source_id" and item:
                name = self.source_name(item)
                if name:
                    result["source"] = name
            elif key in {"claim_ids", "supersedes_claim_ids", "certainty_evidence_claims"}:
                result[{"claim_ids": "claims", "supersedes_claim_ids": "supersedes_claims"}.get(key, key)] = [
                    ref for ref in [await self.reference("claims", v) for v in item or []] if ref
                ]
            elif key == "evidence" and isinstance(item, list):
                refs = []
                for entry in item:
                    if isinstance(entry, str):
                        ref = None
                        for kind in ("claims", "documents", "events"):
                            ref = await self.reference(kind, entry)
                            if ref:
                                break
                        if ref:
                            refs.append(clean_value(ref))
                        elif not OPAQUE.search(entry):
                            refs.append(clean_text(entry))
                    else:
                        refs.append(await self.project(entry))
                result[key] = refs
            elif not private_field(key):
                named = (
                    self.token_name(key)
                    if key in self.tokens
                    else self.instrument_name(key)
                    if key in self.instruments
                    else key
                )
                if not OPAQUE.search(str(named)):
                    result[named] = (
                        item if key in LITERAL_FIELDS and isinstance(item, str) else await self.project(item)
                    )
        return clean_value(result)

    async def result(self, tool, result):
        projected = await self.project(result)
        await self.save()
        if tool == "publish_card" and isinstance(projected, dict):
            return {
                k: projected[k]
                for k in (
                    "published",
                    "reason",
                    "delivery_status",
                    "delivery_not_before",
                    "error",
                    "actual",
                    "max",
                    "excess",
                    "sections",
                    "preview",
                )
                if k in projected
            }
        return projected

    async def message(self, payload, item):
        content = {"message": await self.project(payload), "item": await self.project(item)}
        if self.session.get("event_id"):
            event = await self.repo.get("evt.events", self.session["event_id"])
            if event:
                claims = [
                    r async for r in self.repo.iter_all("evt.claims", filters={"event_id": event["event_id"]})
                ]
                content["current_event"] = await self.project({**event, "claims": claims})
        if self.session.get("proposal_id"):
            proposal = await self.repo.get("scout.expansion_proposals", self.session["proposal_id"])
            content["proposal"] = await self.project(proposal)
            content["open_proposals"] = [
                await self.project(r)
                async for r in self.repo.iter_all("scout.expansion_proposals")
                if not r.get("human_action")
            ]
        await self.save()
        return clean_value(content)

    def select(self, kind, description, *, exclude=()):
        if (
            not isinstance(description, dict)
            or not description
            or not any(v is not None for v in description.values())
        ):
            raise ValueError("Supply the displayed source URL, claim quote or event statement")
        matches = [
            key
            for key, candidate in self.catalog.get(kind, {}).items()
            if key not in exclude
            and all(candidate.get(k) == v for k, v in description.items() if v is not None)
        ]
        if len(matches) != 1:
            raise ValueError(
                "Description is unavailable or ambiguous; read the source/event and include its displayed date or full quote"
            )
        return matches[0]

    def token_key(self, name):
        matches = [
            r["token_id"]
            for r in self.registry.get("tokens", [])
            if str(name).casefold()
            in {
                self.token_name(r["token_id"]).casefold(),
                str(r.get("symbol", "")).casefold(),
                str(r.get("project", "")).casefold(),
                str(r.get("name", "")).casefold(),
            }
        ]
        if len(set(matches)) > 1:
            raise ValueError("Token name is ambiguous; include the project and ticker as displayed")
        return matches[0] if matches else name

    def entity_key(self, name):
        matches = [
            r["entity_id"]
            for r in self.registry.get("entities", [])
            if str(name).casefold()
            in {
                str(r.get("name", "")).casefold(),
                *[str(v).casefold() for v in r.get("aliases", []) + r.get("names", [])],
            }
        ]
        if len(set(matches)) > 1:
            raise ValueError("Project name is ambiguous")
        return matches[0] if matches else "UNRESOLVED:" + str(name)

    def source_key(self, name):
        sources = self.settings.load_config("sources").get("sources", [])
        matches = [r["source_id"] for r in sources if name in {r.get("name"), r.get("url")}]
        if len(matches) != 1:
            raise ValueError("Use the registered source name or its official URL")
        return matches[0]

    def claim_key(self, ref):
        return self.select("claims", ref)

    def restore(self, value, parent=""):
        if isinstance(value, list):
            return [self.restore(v, parent) for v in value]
        if not isinstance(value, dict):
            return value
        result = {}
        for key, item in value.items():
            if key == "document":
                result["item_id"] = self.select("documents", item)
            elif key == "claim":
                result["claim_id"] = self.claim_key(item)
            elif key == "supersedes_claims":
                result["supersedes_claim_ids"] = [self.claim_key(v) for v in item]
            elif key == "claims" and parent in {"approval_dependency", "negative_thesis"}:
                result["claim_ids"] = [self.claim_key(v) for v in item]
            elif key == "tokens" and parent == "approval_dependency":
                result["token_ids"] = [self.token_key(v) for v in item]
            elif key == "token" and parent in {"capture_chains", "edges"}:
                result["token_id"] = self.token_key(item) if item is not None else None
            elif key == "entity" and parent == "edges":
                result["entity_id"] = self.entity_key(item) if item is not None else None
            elif key == "instrument":
                matches = [
                    r["instrument_id"]
                    for r in self.registry.get("instruments", [])
                    if self.instrument_name(r["instrument_id"]) == item
                ]
                if len(matches) != 1:
                    raise ValueError("Use the displayed venue, market type and symbol")
                result["instrument_id"] = matches[0]
            elif key == "source" and parent == "next_milestone":
                result["source_id"] = self.source_key(item) if item else None
            elif key == "evidence" and isinstance(item, list):
                result[key] = []
                for ref in item:
                    if isinstance(ref, dict) and "document" in ref:
                        result[key].append(self.restore(ref))
                    elif isinstance(ref, dict) and "quote" in ref:
                        result[key].append(self.claim_key(ref))
                    elif isinstance(ref, dict) and "statement" in ref:
                        result[key].append(self.select("events", ref))
                    elif isinstance(ref, dict) and "url" in ref:
                        result[key].append(self.select("documents", ref))
                    else:
                        raise ValueError("Evidence must cite a displayed source or verbatim claim")
            else:
                result[key] = self.restore(item, key)
        if parent == "entities" and "name" in result:
            result["entity_id"] = None
        return result

    async def arguments(self, tool, arguments):
        reject_private_input(arguments)
        for field, kind in (("event_id", "events"), ("item_id", "documents"), ("proposal_id", "proposals")):
            if self.session.get(field):
                await self.reference(kind, self.session[field])
        if self.session.get("event_id"):
            async for row in self.repo.iter_all("evt.claims", filters={"event_id": self.session["event_id"]}):
                self.remember("claims", row["claim_id"], await self.describe_claim(row))
        result = self.restore(copy.deepcopy(arguments))
        if tool in SCOPED_TOOLS:
            if not self.session.get("event_id"):
                raise ValueError("Record the current event before assessing or publishing it")
            result["event_id"] = self.session["event_id"]
        if tool in {"get_event", "merge_into"}:
            target = arguments.get("event")
            result.pop("event", None)
            result["event_id"] = (
                self.select(
                    "events", target, exclude=(self.session.get("event_id"),) if tool == "merge_into" else ()
                )
                if target
                else self.session.get("event_id")
            )
            if not result["event_id"]:
                raise ValueError("Select an event by its displayed statement and public time")
        if tool in {"record_event", "add_observation", "get_item"}:
            active = next(
                (
                    row
                    for row in self.session.get("message_log", [])
                    if row.get("message_id") == self.session.get("active_message_id")
                ),
                {},
            )
            trigger = active.get("payload") or self.session.get("last_payload") or {}
            if tool in {"record_event", "add_observation"}:
                result["item_id"] = trigger.get("item_id") or self.session.get("item_id")
            else:
                result.setdefault("item_id", trigger.get("item_id") or self.session.get("item_id"))
            if not result["item_id"]:
                raise ValueError("Read the source document first")
        if tool == "set_watch_condition" and arguments.get("action") == "DISARM":
            result.pop("condition", None)
            result["cond_id"] = self.select("conditions", arguments.get("condition"))
        if tool == "record_review":
            result["proposal_id"] = self.session.get("proposal_id")
            if arguments.get("duplicate_of"):
                result["duplicate_of"] = self.select("proposals", arguments["duplicate_of"])
        if tool == "schedule_milestone":
            result["source"] = self.source_key(arguments["source"])
        if tool == "search_tank_items" and result.get("sources"):
            result["sources"] = [self.source_key(v) for v in result["sources"]]
        if result.get("entity_or_token"):
            result["entity_or_token"] = self.token_key(result["entity_or_token"])
        for field in ("token", "tokens"):
            if field in result:
                result[field] = (
                    [self.token_key(v) for v in result[field]]
                    if isinstance(result[field], list)
                    else self.token_key(result[field])
                )
        await self.save()
        return result
