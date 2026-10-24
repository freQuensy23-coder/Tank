"""Model context is text: record IDs never appear in it and registry objects read «name · key».
Tool arguments name records by ID and registry objects by key; the host validates every one."""

import copy
import re

from tank.event_tools import SCOPED_TOOLS, validate_schema

CONTEXT_VERSION = 2
AGENT_SKILLS = {False: "tank-event-analyst", True: "tank-scout-reviewer"}
ID_LOOKUP = {
    False: "tank-data data guide, «Finding record IDs for tool arguments»",
    True: "your skill, «Finding record IDs»",
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
    "created_by",
    "updated_by",
    "last_shown_by",
    "rearmed_from",
    "cancelled_by",
    "opened_by_alert",
}
LITERAL_FIELDS = {"text", "quoted", "title", "quote", "url", "query"}
DIAGNOSTIC_FIELDS = {"error", "errors", "reason", "diagnostic", "diagnostics"}
OPAQUE = re.compile(
    r"\b(?:ITM|EVT|OBS|CLM|EMS|EDG|MTA|DEC|OUT|ALR|CND|STC|CAL|PRP|DLV|REQ|CTR|REV|FIX)-[A-Z0-9]{26}\b"
    r"|\b(?:session|agent|skill|call|env|file|tgf|web|search|evt|itm|clm|obs|ems|mta|dec|cond|prp)_[A-Za-z0-9_-]{20,}\b"
    r"|\b[0-9a-fA-F]{32,64}\b"
    r"|\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b"
)
RECORDS = {
    "item": ("ingest.source_items", "source document"),
    "event": ("evt.events", "event"),
    "claim": ("evt.claims", "claim"),
    "condition": ("ana.watch_conditions", "watch condition"),
    "proposal": ("scout.expansion_proposals", "proposal"),
}
REGISTRY = {
    "token": "registry token_id",
    "entity": "registry entity_id",
    "instrument": "registry instrument_id",
    "source": "registered source_id",
}
QUERIES = {
    "item": "Source document by URL",
    "event": "Events by statement, time or token",
    "claim": "Claims of an event",
    "condition": "Watch conditions of an event",
    "proposal": "Proposals by subject",
    "token": "Registry keys",
    "entity": "Registry keys",
    "instrument": "Registry keys",
    "source": "Registry keys",
}
ID_FIELDS = {
    "item_id": "item",
    "event_id": "event",
    "claim_id": "claim",
    "claim_ids": "claim",
    "supersedes_claim_ids": "claim",
    "criterion_claim_id": "claim",
    "cond_id": "condition",
    "proposal_id": "proposal",
    "duplicate_of": "proposal",
    "token_id": "token",
    "token_ids": "token",
    "entity_id": "entity",
    "criterion_venue_id": "entity",
    "instrument_id": "instrument",
    "source_id": "source",
    "source_ids": "source",
}
CLAIM_EVIDENCE_TOOLS = {"set_assessment", "set_beneficiaries"}
PATTERN_TOOLS = {"propose_pattern", "record_review"}
LEGACY_REFERENCES = {
    "document": "item_id",
    "event": "event_id",
    "condition": "cond_id",
    "claim": "claim_id",
    "claims": "claim_ids",
    "supersedes_claims": "supersedes_claim_ids",
    "sources": "source_ids",
    "source": "source_id",
    "entity_or_token": "token_id or entity_id",
    "token": "token_id",
    "tokens": "token_ids",
    "entity": "entity_id",
    "instrument": "instrument_id",
    "criterion_quote": "criterion_claim_id",
    "criterion_venue": "criterion_venue_id",
}
PREFIXES = (
    ("ITM-", "item"),
    ("web-", "item"),
    ("EVT-", "event"),
    ("CLM-", "claim"),
    ("CND-", "condition"),
    ("PRP-", "proposal"),
)
OPEN_LIFECYCLES_EXCLUDED = {"CLOSED", "MERGED", "INVALIDATED"}
CONTEXT_TRANSITION = {
    False: (
        "Tank changed its tool contract. Follow the current tool definitions and this note over earlier tool "
        "instructions and skill text that cite records by description. Tool arguments name records only by ID. "
        "Registry objects now read «Name · key» (World (WLD) · world:WLD, Uniswap · uniswap): token_id, token_ids, "
        "entity_id, instrument_id and source_id take the key after « · »; an entity or token outside the registry "
        "is named (entities[].name, token_names, entity_name, token_name). "
        "Finding record IDs for tool arguments, with read-only SQL in your sandbox "
        '(psql "$TANK_DB_URL" -Atc "<query>"): '
        "«Source document by URL»: SELECT item_id, observed_at, document->>'evidence_kind' FROM "
        "ingest.source_items WHERE url = '<url>' ORDER BY observed_at DESC (a page read with fetch_page is its own "
        "WEB document). "
        "«Claims of an event»: SELECT claim_id, quote, subject, predicate, object, verified FROM evt.claims WHERE "
        "document->>'event_id' = '<event_id>' ORDER BY claim_id; the current event: SELECT e.event_id, e.statement "
        "FROM evt.events e JOIN evt.event_observations o ON o.event_id = e.event_id JOIN ingest.source_items i ON "
        "i.item_id = o.item_id WHERE i.url = '<incoming URL>'. "
        "«Events by statement, time or token»: SELECT event_id, statement, public_at, lifecycle FROM evt.events "
        "WHERE strpos(lower(statement), lower('<words>')) > 0 AND lifecycle NOT IN ('CLOSED', 'MERGED', "
        "'INVALIDATED') ORDER BY known_to_us_at DESC LIMIT 20. "
        "«Watch conditions of an event»: SELECT cond_id, token_id, document->>'human_text', document->'expr' FROM "
        "ana.watch_conditions WHERE event_id = '<event_id>' AND status = 'ARMED'. "
        "«Registry keys»: reg.tokens (token_id, symbol), reg.entities and reg.entity_aliases (entity_id, alias), "
        "reg.instruments (instrument_id), reg.sources (source_id, name). "
        "The current event, the incoming source and the current proposal stay implicit. A claim quoted from the "
        "incoming source needs no evidence; a quote from another document cites {item_id, quote}. Assessment and "
        "beneficiary evidence, criterion_claim_id and supersedes_claim_ids are claim IDs of the current event; "
        "merge_into takes the earlier event's event_id; DISARM takes cond_id. Tank refuses an unknown ID, one of "
        "the wrong kind or one outside the tool's scope, and names the argument."
    ),
    True: (
        "Tank changed its tool contract. Follow the current tool definitions and this note over earlier "
        "instructions that cite records by description. record_review cites evidence only by ID - {claim_id}, "
        "{event_id} or {item_id} - and names a duplicate in duplicate_of by its proposal ID; the current proposal "
        "stays implicit. Registry objects read «Name · key»; search_tank_items.source_ids take the key after « · ». "
        "Your sandbox shell is now enabled, only for read-only SQL lookups of record IDs "
        '(psql "$TANK_DB_URL" -Atc "<query>"); this replaces the earlier rule to use only web_search, '
        "search_tank_items and record_review. Finding record IDs: "
        "«Proposals by subject»: SELECT prp_id, concept_name, document->'evidence' FROM scout.expansion_proposals "
        "WHERE human_action IS NULL AND strpos(lower(concept_name), lower('<words>')) > 0 ORDER BY prp_id. "
        "«Source document by URL»: SELECT item_id, observed_at, title FROM ingest.source_items WHERE url = '<url>' "
        "ORDER BY observed_at DESC. "
        "«Events by statement, time or token»: SELECT event_id, statement, public_at FROM evt.events WHERE "
        "strpos(lower(statement), lower('<words>')) > 0 ORDER BY known_to_us_at DESC LIMIT 20. "
        "«Claims of an event»: SELECT claim_id, quote, subject, predicate, object FROM evt.claims WHERE "
        "document->>'event_id' = '<event_id>' ORDER BY claim_id. "
        "«Registry keys»: SELECT source_id, name FROM reg.sources. "
        "Tank refuses an unknown ID or one of the wrong kind, and names the argument."
    ),
}


def identifier_field(key):
    name = re.sub(r"([a-z])([A-Z])", r"\1_\2", str(key)).lower()
    return name in {"id", "ids", "hash", "hashes", "sha256", "version"} or name.endswith(
        ("_id", "_ids", "_hash", "_hashes", "_sha256", "_version", "_ref")
    )


def private_field(key):
    name = re.sub(r"([a-z])([A-Z])", r"\1_\2", str(key)).lower()
    return name in PRIVATE_FIELDS or name.endswith("_provenance") or identifier_field(key)


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


def labelled(name, key):
    """How model context shows a registry object: its readable name, then the key tools take."""
    return f"{name} · {key}"


def named(label):
    return ("an " if label[0] in "aeiou" else "a ") + label


def record_kind(value):
    """The record kind named by an ID's prefix, when it has one."""
    if isinstance(value, str):
        for prefix, kind in PREFIXES:
            if value.startswith(prefix):
                return kind
    return None


def id_kind(tool, field):
    """The record kind an argument field takes, or None for text."""
    if field == "evidence" and tool in CLAIM_EVIDENCE_TOOLS:
        return "claim"
    return ID_FIELDS.get(field)


def references(tool, value, path="$", field=None):
    """Every record ID in tool arguments, with its argument path and kind."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield from references(tool, item, f"{path}.{key}", key)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from references(tool, item, f"{path}[{index}]", field)
    elif isinstance(value, str) and (kind := id_kind(tool, field)):
        yield path, kind, value


def reject_text_ids(tool, value, path="$", field=None):
    """Record IDs go only in ID fields; authored text, names and reasons stay free of them."""
    if isinstance(value, dict):
        for key, item in value.items():
            reject_text_ids(tool, item, f"{path}.{key}", key)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            reject_text_ids(tool, item, f"{path}[{index}]", field)
    elif (
        isinstance(value, str)
        and not id_kind(tool, field)
        and field not in LITERAL_FIELDS
        and clean_text(value) != value
    ):
        raise ValueError(f"{path}: write this text without internal record IDs; IDs go only in ID fields")


def legacy_reference(value, schema, path="$"):
    """The first old description-based reference field, with the ID field that replaced it."""
    if isinstance(value, dict) and isinstance(schema, dict):
        properties = {}
        for option in (schema, *schema.get("anyOf", [])):
            properties.update(option.get("properties", {}))
        for key, item in value.items():
            if key not in properties:
                if key in LEGACY_REFERENCES:
                    return f"{path}.{key}", LEGACY_REFERENCES[key]
                continue
            found = legacy_reference(item, properties[key], f"{path}.{key}")
            if found:
                return found
    elif isinstance(value, list) and isinstance(schema, dict):
        for index, item in enumerate(value):
            found = legacy_reference(item, schema.get("items"), f"{path}[{index}]")
            if found:
                return found
    return None


def novel_layers_allowed(schema):
    """set_beneficiaries also takes a layer outside the eight: the domain turns it into a
    BENEFICIARY_LAYER scout proposal (03 §10) instead of an edge."""
    schema = copy.deepcopy(schema)
    schema["properties"]["edges"]["items"]["properties"]["layer"].pop("enum", None)
    return schema


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
        self.scout = bool(session.get("proposal_id"))
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
        self.registry_labels = {
            **{key: self.entity_label(key) for key in self.entities},
            **{key: self.source_label(key) for key in self.sources},
            **{key: self.instrument_label(key) for key in self.instruments},
            **{key: self.token_label(key) for key in self.tokens},
        }
        self.registry_pattern = (
            re.compile(
                r"(?<![\w:-])(?:"
                + "|".join(re.escape(key) for key in sorted(self.registry_labels, key=len, reverse=True))
                + r")(?![\w:-]|\.[\w-])"
            )
            if self.registry_labels
            else None
        )
        self.described = {}

    def token_label(self, key):
        row = self.tokens.get(key)
        if row:
            return labelled(
                f"{row.get('project') or row.get('name') or row.get('symbol')} ({row.get('symbol')})", key
            )
        return str(key).removeprefix("UNRESOLVED:").rsplit(":", 1)[-1]

    def entity_label(self, key):
        row = self.entities.get(key)
        if not row:
            return str(key).removeprefix("UNRESOLVED:")
        name = row.get("name")
        if not name or name == key:
            name = (row.get("aliases") or row.get("names") or [key])[0]
        return labelled(name, key)

    def instrument_label(self, key):
        row = self.instruments.get(key)
        if row:
            return labelled(
                " ".join(str(row.get(k) or "") for k in ("venue", "market_type", "venue_symbol")).strip(), key
            )
        return " ".join(str(key).split(":"))

    def source_label(self, key):
        row = self.sources.get(key)
        return labelled(row.get("name") or row.get("url") or "registered source", key) if row else None

    def diagnostic_text(self, value):
        if self.registry_pattern is None:
            return clean_text(value)
        pattern = self.registry_pattern
        return "".join(
            part
            if part.startswith(("http://", "https://"))
            else clean_text(pattern.sub(lambda match: self.registry_labels[match[0]], part))
            for part in re.split(r"(https?://[^\s<>\"]+)", value)
        )

    def condition_reference(self, row):
        description = {"text": row.get("human_text"), "conditions": row.get("expr")}
        if row.get("token_id"):
            description["token"] = self.token_label(row["token_id"])
        for field in ("role", "created_at"):
            if row.get(field):
                description[field] = row[field]
        return description

    async def describe_claim(self, row):
        description = claim_reference(row)
        sources = []
        for entry in row.get("evidence") or []:
            if isinstance(entry, dict) and entry.get("item_id"):
                source = await self.reference("item", entry["item_id"])
                if source and source not in sources:
                    sources.append(source)
        if sources:
            description["sources"] = sources
        if "verified" in row:
            description["verified"] = row["verified"]
        if row.get("state_claim"):
            description["stage"] = row["state_claim"]
        return description

    async def describe(self, kind, row):
        if kind == "claim":
            return await self.describe_claim(row)
        if kind == "item":
            return document_reference(row)
        if kind == "event":
            return event_reference(row)
        if kind == "condition":
            return self.condition_reference(row)
        return {"subject": row.get("concept_name")}

    async def reference(self, kind, key):
        """A readable description of a referenced record, read once per projection."""
        if not isinstance(key, str) or not key:
            return None
        if (kind, key) not in self.described:
            row = await self.repo.get(RECORDS[kind][0], key)
            self.described[kind, key] = await self.describe(kind, row) if row else None
        return self.described[kind, key]

    async def project(self, value, *, diagnostic=False):
        if isinstance(value, list):
            return [await self.project(item, diagnostic=diagnostic) for item in value]
        if isinstance(value, str):
            if value in self.tokens:
                return self.token_label(value)
            if value in self.instruments:
                return self.instrument_label(value)
            if value in self.entities:
                return self.entity_label(value)
            if value in self.sources:
                return self.source_label(value)
            kind = record_kind(value)
            if kind and OPAQUE.search(value):
                described = await self.reference(kind, value)
                if described:
                    return clean_value(described)
            return self.diagnostic_text(value) if diagnostic else clean_text(value)
        if not isinstance(value, dict):
            return value
        result = {}
        for kind, field, label in (
            ("item", "item_id", "document"),
            ("claim", "claim_id", "claim"),
            ("condition", "cond_id", "condition"),
            ("proposal", "prp_id", "proposal"),
        ):
            if value.get(field):
                description = await self.describe(kind, value)
                if not any(description.values()):
                    description = await self.reference(kind, value[field])
                if description:
                    result[label] = clean_value(description)
        for key, item in value.items():
            if key == "token_id" and item:
                result["token"] = self.token_label(item)
            elif key == "token_ids" and item:
                result["tokens"] = [self.token_label(v) for v in item]
            elif key == "instrument_ids" and item:
                result["instruments"] = [self.instrument_label(v) for v in item]
            elif key == "entity_ids" and item:
                result["entities"] = [self.entity_label(v) for v in item]
            elif key == "entity_id" and item:
                result["entity"] = self.entity_label(item)
            elif key == "instrument_id" and item:
                result["instrument"] = self.instrument_label(item)
            elif key == "source_id" and item:
                name = self.source_label(item)
                if name:
                    result["source"] = name
            elif key in {"claim_ids", "supersedes_claim_ids", "certainty_evidence_claims"}:
                result[{"claim_ids": "claims", "supersedes_claim_ids": "supersedes_claims"}.get(key, key)] = [
                    ref for ref in [await self.reference("claim", v) for v in item or []] if ref
                ]
            elif key == "evidence" and isinstance(item, list):
                refs = []
                for entry in item:
                    if isinstance(entry, str):
                        ref = None
                        for kind in (
                            [record_kind(entry)] if record_kind(entry) else ["claim", "item", "event"]
                        ):
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
                    self.token_label(key)
                    if key in self.tokens
                    else self.instrument_label(key)
                    if key in self.instruments
                    else key
                )
                if not OPAQUE.search(str(named)):
                    result[named] = (
                        item
                        if key in LITERAL_FIELDS
                        and isinstance(item, str)
                        and (not diagnostic or key in {"quote", "quoted", "url", "query"})
                        else await self.project(item, diagnostic=diagnostic or key in DIAGNOSTIC_FIELDS)
                    )
        return clean_value(result)

    async def result(self, tool, result):
        projected = await self.project(result)
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
        return clean_value(content)

    def lookup(self, kind):
        if kind in REGISTRY:
            return (
                "Use the registry key shown after « · » next to the name in Tank's messages and results "
                f"(Name · key); a key not shown yet is in {ID_LOOKUP[self.scout]}, «{QUERIES[kind]}»."
            )
        return f"Look it up with read-only SQL: {ID_LOOKUP[self.scout]}, «{QUERIES[kind]}»."

    def public_schema(self, tool):
        reader = getattr(self.settings, "read_config_json", None)
        if reader is None:
            return None
        try:
            tools = reader(f"skills/{AGENT_SKILLS[self.scout]}/tools.json")
        except (OSError, ValueError, KeyError):
            return None
        for row in tools:
            if row.get("name") == tool:
                return row["input_schema"]
        raise ValueError(f"{tool} is not a tool of this agent")

    def validate_shape(self, tool, arguments):
        schema = self.public_schema(tool)
        if schema is None:
            return
        if tool == "set_beneficiaries":
            schema = novel_layers_allowed(schema)
        legacy = legacy_reference(arguments, schema)
        if legacy:
            raise ValueError(
                f"{legacy[0]} is no longer accepted: pass {legacy[1]}, the record's ID. "
                f"Look IDs up with read-only SQL: {ID_LOOKUP[self.scout]}."
            )
        try:
            validate_schema(arguments, schema)
        except ValueError as error:
            text = str(error)
            path = text.split(":", 1)[0]
            if "unknown fields" in text or re.search(r"(_id|_ids|evidence|duplicate_of)(\[\d+\])*$", path):
                text += (
                    ". Arguments name records only by their IDs (see the tool definition); "
                    f"look IDs up with read-only SQL: {ID_LOOKUP[self.scout]}."
                )
            raise ValueError(text) from None

    def trigger(self):
        active = next(
            (
                row
                for row in self.session.get("message_log", [])
                if row.get("message_id") == self.session.get("active_message_id")
            ),
            {},
        )
        return active, (active.get("payload") if active else self.session.get("last_payload")) or {}

    def bind(self, tool, result):
        """Fill the implicit records: the current event, the incoming source and the current proposal."""
        current = self.session.get("event_id")
        if tool in SCOPED_TOOLS:
            if not current:
                raise ValueError(f"{tool} works on the current event; record it with record_event first")
            result["event_id"] = current
        elif tool == "get_event":
            result.setdefault("event_id", current)
            if not result["event_id"]:
                raise ValueError(
                    "$.event_id: there is no current event yet; pass the event_id to read. "
                    + self.lookup("event")
                )
        elif tool == "merge_into":
            if not current:
                raise ValueError("Record the current event before merging it")
            if not result.get("event_id"):
                raise ValueError(
                    "$.event_id: name the earlier open event to merge into. " + self.lookup("event")
                )
            if result["event_id"] == current:
                raise ValueError(
                    "$.event_id: this is the current event; pass the event_id of the earlier open event "
                    "that covers the same development. " + self.lookup("event")
                )
        if tool in {"record_event", "add_observation", "get_item"}:
            active, trigger = self.trigger()
            if tool == "add_observation":
                source = trigger.get("item_id") or (
                    self.session.get("item_id") if not active and not trigger else None
                )
                if source and result.get("item_id", source) != source:
                    raise ValueError(
                        "$.item_id: this observation is bound to the incoming source; "
                        "cite other documents in the claims' evidence"
                    )
                if source:
                    result["item_id"] = source
                elif not result.get("item_id"):
                    raise ValueError(
                        "$.item_id: this callback has no incoming source; pass the item_id of the document "
                        "that establishes the update (a page read with fetch_page is stored as one). "
                        + self.lookup("item")
                    )
            elif tool == "record_event":
                result["item_id"] = trigger.get("item_id") or self.session.get("item_id")
                if not result["item_id"]:
                    raise ValueError("record_event records the incoming source; this session has none")
            else:
                result.setdefault("item_id", trigger.get("item_id") or self.session.get("item_id"))
                if not result["item_id"]:
                    raise ValueError(
                        "$.item_id: there is no incoming source; pass the item_id to read. "
                        + self.lookup("item")
                    )
        if tool == "record_review":
            result["proposal_id"] = self.session.get("proposal_id")

    async def check(self, tool, path, kind, value):
        """One record ID: it exists, has the expected kind and lies in the tool's scope."""
        if kind in REGISTRY:
            known = {
                "token": self.tokens,
                "entity": self.entities,
                "instrument": self.instruments,
                "source": self.sources,
            }[kind]
            registered = value in known or (
                kind == "source" and await self.repo.get("reg.sources", value) is not None
            )
            if not registered:
                raise ValueError(f"{path}: not a {REGISTRY[kind]}. {self.lookup(kind)}")
            return
        table, label = RECORDS[kind]
        row = await self.repo.get(table, value)
        if row is None:
            other = record_kind(value)
            if other and other != kind and await self.repo.get(RECORDS[other][0], value):
                raise ValueError(
                    f"{path}: this is the ID of {named(RECORDS[other][1])}, not of {named(label)}. "
                    + self.lookup(kind)
                )
            raise ValueError(f"{path}: no {label} has this ID. {self.lookup(kind)}")
        current = self.session.get("event_id")
        if kind == "claim" and tool not in PATTERN_TOOLS and row.get("event_id") != current:
            raise ValueError(f"{path}: this is not a claim of the current event. {self.lookup('claim')}")
        if kind == "condition" and row.get("event_id") != current:
            raise ValueError(
                f"{path}: this watch condition belongs to another event. {self.lookup('condition')}"
            )
        if tool == "merge_into" and path == "$.event_id" and row.get("lifecycle") in OPEN_LIFECYCLES_EXCLUDED:
            raise ValueError(
                f"$.event_id: that event is {row['lifecycle']}; merge into an open event. {self.lookup('event')}"
            )
        if tool == "record_review" and path == "$.duplicate_of":
            if value == self.session.get("proposal_id"):
                raise ValueError(
                    "$.duplicate_of: this is the current proposal; name the earlier one, or null"
                )
            if row.get("human_action"):
                raise ValueError(
                    f"$.duplicate_of: that proposal is already decided; name an open proposal. "
                    f"{self.lookup('proposal')}"
                )

    async def arguments(self, tool, arguments):
        """Validate the model's arguments and every record ID in them, then bind the implicit records."""
        if not isinstance(arguments, dict):
            raise ValueError("Tool arguments must be an object")
        self.validate_shape(tool, arguments)
        reject_text_ids(tool, arguments)
        for path, kind, value in references(tool, arguments):
            await self.check(tool, path, kind, value)
        result = copy.deepcopy(arguments)
        self.bind(tool, result)
        return result
