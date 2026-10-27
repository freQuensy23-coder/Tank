"""Model context is text: record IDs never appear in it and registry objects read «name · key».
Tool arguments name records by ID and registry objects by key; the host validates every one."""

import copy
import re
from collections.abc import AsyncIterator, Iterator
from typing import Literal, Protocol

from pydantic import BaseModel, JsonValue

from tank.agents.context_binding import ContextBinding
from tank.agents.context_records import (
    ClaimEvidenceOwner,
    ClaimReference,
    ConditionReference,
    ContextMessageProjection,
    DocumentReference,
    EventMergeState,
    EventOwnership,
    EventReference,
    ProposalAvailability,
    ProposalReference,
    RecordReference,
    reference_wire,
)
from tank.agents.context_registry import ContextConfiguration, ContextRegistry
from tank.agents.message_dispatch import MessageDispatchRequest
from tank.contracts import DomainModel
from tank.contracts.tool_schema import SCOPED_TOOLS, parse_tool_definitions, validate_schema
from tank.util import JsonInput, jsonable

type RecordKind = Literal["item", "event", "claim", "condition", "proposal"]
type ReferenceKind = RecordKind | Literal["token", "entity", "instrument", "source"]


class LegacyReference(DomainModel):
    path: str
    replacement: str


class ToolArgumentReference(DomainModel):
    path: str
    kind: ReferenceKind
    value: str


CONTEXT_VERSION = 3
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
    "native_execution_observation",
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
ID_FIELDS: dict[str, ReferenceKind] = {
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
CLAIM_REFERENCE_LABELS = {
    "claim_ids": "claims",
    "supersedes_claim_ids": "supersedes_claims",
    "certainty_evidence_claims": "certainty_evidence_claims",
}
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
PREFIXES: tuple[tuple[str, RecordKind], ...] = (
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
        "MATERIAL leads now have an optional PRELIMINARY publication path. Keep the original class and certainty, "
        "including LOW certainty. Use it only for a concrete sourced lead with a potentially substantial "
        "consequence; supply what, why and unconfirmed. LOW event class still stops. PRELIMINARY needs no "
        "MarketTruth or decisions and never satisfies ordinary card requirements. The first ordinary card "
        "remains FULL or STRUCTURAL_SHOCK PARTIAL. set_watch_condition can instead arm a timed event review "
        "with review_at and review_question, without token_id or metric conditions. On CONDITION_FIRED "
        "with review_question, investigate that question, record new evidence and reassess the same event; "
        "do not go directly to get_decision. Schedule another review only with a concrete question and expiry. "
        "If an official DENIAL invalidates an event whose only cards are PRELIMINARY, publish one more "
        "PRELIMINARY at once stating the denial in what and why, without unconfirmed; Tank sends it in the "
        "same thread marked DENIED. "
        "These additions override older instructions to always stop on MATERIAL. "
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
        "OpenMA's full built-in toolset is now enabled for you: shell, programs, files, browser, web search and fetch "
        '(record IDs: psql "$TANK_DB_URL" -Atc "<query>"); this replaces the earlier rule to use only web_search, '
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


def identifier_field(key: str) -> bool:
    name = re.sub(r"([a-z])([A-Z])", r"\1_\2", key).lower()
    return name in {"id", "ids", "hash", "hashes", "sha256", "version"} or name.endswith(
        ("_id", "_ids", "_hash", "_hashes", "_sha256", "_version", "_ref")
    )


def private_field(key: str) -> bool:
    name = re.sub(r"([a-z])([A-Z])", r"\1_\2", key).lower()
    return name in PRIVATE_FIELDS or name.endswith("_provenance") or identifier_field(key)


def clean_text(text: str) -> str:
    return "".join(
        part if part.startswith(("http://", "https://")) else OPAQUE.sub("[internal reference omitted]", part)
        for part in re.split(r"(https?://[^\s<>\"]+)", text)
    )


def clean_value(value: JsonInput) -> JsonInput:
    """Final recursive guard for diagnostics, prose and unknown provider fields."""
    if isinstance(value, BaseModel):
        return clean_value(value.model_dump(mode="python"))
    if isinstance(value, dict):
        return {
            clean_text(key): item if key in LITERAL_FIELDS and isinstance(item, str) else clean_value(item)
            for key, item in value.items()
            if not private_field(key) and not OPAQUE.search(key)
        }
    if isinstance(value, (list, tuple, set)):
        return [clean_value(item) for item in value]
    return clean_text(value) if isinstance(value, str) else value


def labelled(name: str, key: str) -> str:
    """How model context shows a registry object: its readable name, then the key tools take."""
    return f"{name} · {key}"


def named(label: str) -> str:
    return ("an " if label[0] in "aeiou" else "a ") + label


def record_kind(value: JsonInput) -> RecordKind | None:
    """The record kind named by an ID's prefix, when it has one."""
    if isinstance(value, str):
        for prefix, kind in PREFIXES:
            if value.startswith(prefix):
                return kind
    return None


def id_kind(tool: str, field: str | None) -> ReferenceKind | None:
    """The record kind an argument field takes, or None for text."""
    if field is None:
        return None
    if field == "evidence" and tool in CLAIM_EVIDENCE_TOOLS:
        return "claim"
    return ID_FIELDS.get(field)


def references(
    tool: str, value: JsonValue, path: str = "$", field: str | None = None
) -> Iterator[ToolArgumentReference]:
    """Every record ID in tool arguments, with its argument path and kind."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield from references(tool, item, f"{path}.{key}", key)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from references(tool, item, f"{path}[{index}]", field)
    elif isinstance(value, str) and (kind := id_kind(tool, field)):
        yield ToolArgumentReference(path=path, kind=kind, value=value)


def reject_text_ids(tool: str, value: JsonValue, path: str = "$", field: str | None = None) -> None:
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


def legacy_reference(value: JsonValue, schema: JsonValue, path: str = "$") -> LegacyReference | None:
    """The first old description-based reference field, with the ID field that replaced it."""
    if isinstance(value, dict) and isinstance(schema, dict):
        properties: dict[str, JsonValue] = {}
        options: list[JsonValue] = [schema]
        if "anyOf" in schema:
            alternatives = schema["anyOf"]
            if not isinstance(alternatives, list):
                raise TypeError("Configured tool schema anyOf must be an array")
            options.extend(alternatives)
        for option in options:
            if isinstance(option, bool):
                continue
            if not isinstance(option, dict):
                raise TypeError("Configured tool schema alternatives must be schema objects")
            if "properties" in option:
                fields = option["properties"]
                if not isinstance(fields, dict):
                    raise TypeError("Configured tool schema properties must be an object")
                properties.update(fields)
        for key, item in value.items():
            if key not in properties:
                if key in LEGACY_REFERENCES:
                    return LegacyReference(path=f"{path}.{key}", replacement=LEGACY_REFERENCES[key])
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


def novel_layers_allowed(schema: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """set_beneficiaries also takes a layer outside the eight: the domain turns it into a
    BENEFICIARY_LAYER scout proposal (03 §10) instead of an edge."""
    schema = copy.deepcopy(schema)
    layer = schema
    for field in ("properties", "edges", "items", "properties", "layer"):
        child = layer[field]
        if not isinstance(child, dict):
            raise TypeError("Configured beneficiary layer schema must contain object-valued fields")
        layer = child
    layer.pop("enum", None)
    return schema


def wire_string(value: JsonValue, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Context field {field} must be a nonempty string")
    return value


def wire_strings(value: JsonValue, field: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"Context field {field} must be an array")
    return [wire_string(item, field) for item in value]


class ContextRecordReader(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...

    def iter_all(
        self,
        collection: str,
        *,
        filters: dict[str, JsonValue] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[dict[str, JsonValue]]: ...


def document_reference(row: dict[str, JsonValue]) -> DocumentReference:
    source = DocumentReference.model_validate(
        {field: row.get(field) for field in ("url", "title", "observed_at")} | {"excerpt": None}
    )
    if source.url is None and source.title is None and "text" in row:
        text = row["text"]
        if not isinstance(text, str):
            raise ValueError("Source reference text must be a string")
        return DocumentReference(
            url=source.url, title=source.title, observed_at=source.observed_at, excerpt=text[:200]
        )
    return source


def event_reference(row: dict[str, JsonValue]) -> EventReference:
    return EventReference.model_validate({field: row.get(field) for field in ("statement", "public_at")})


def claim_reference(row: dict[str, JsonValue]) -> ClaimReference:
    return ClaimReference.model_validate(
        {field: row.get(field) for field in ("quote", "subject", "predicate", "object", "verified")}
        | {"sources": None, "stage": row.get("state_claim")}
    )


class ModelContext:
    def __init__(
        self,
        repo: ContextRecordReader,
        settings: ContextConfiguration,
        session: dict[str, JsonValue],
    ) -> None:
        self.repo, self.settings = repo, settings
        self.binding = ContextBinding.from_session(session)
        self.scout = self.binding.proposal_id is not None
        self.registry = ContextRegistry.from_configuration(settings)
        self.tokens = {row.token_id: row for row in self.registry.tokens}
        self.entities = {row.entity_id: row for row in self.registry.entities}
        self.instruments = {row.instrument_id: row for row in self.registry.instruments}
        self.sources = {row.source_id: row for row in self.registry.sources}
        self.registry_labels = {
            **{key: self.entity_label(key) for key in self.entities},
            **{key: labelled(row.name, key) for key, row in self.sources.items()},
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
        self.described: dict[tuple[RecordKind, str], RecordReference | None] = {}

    def token_label(self, key: str) -> str:
        row = self.tokens.get(key)
        if row is not None:
            return labelled(f"{row.display_name()} ({row.symbol})", key)
        return key.removeprefix("UNRESOLVED:").rsplit(":", 1)[-1]

    def entity_label(self, key: str) -> str:
        row = self.entities.get(key)
        if row is None:
            return key.removeprefix("UNRESOLVED:")
        return labelled(row.display_name(), key)

    def instrument_label(self, key: str) -> str:
        row = self.instruments.get(key)
        if row is not None:
            return labelled(f"{row.venue} {row.market_type} {row.venue_symbol}", key)
        return " ".join(key.split(":"))

    def source_label(self, key: str) -> str | None:
        row = self.sources.get(key)
        return labelled(row.name, key) if row is not None else None

    def diagnostic_text(self, value: str) -> str:
        if self.registry_pattern is None:
            return clean_text(value)
        pattern = self.registry_pattern
        return "".join(
            part
            if part.startswith(("http://", "https://"))
            else clean_text(pattern.sub(lambda match: self.registry_labels[match[0]], part))
            for part in re.split(r"(https?://[^\s<>\"]+)", value)
        )

    def condition_reference(self, row: dict[str, JsonValue]) -> ConditionReference:
        token_id = row.get("token_id")
        if token_id is not None and not isinstance(token_id, str):
            raise ValueError("Condition reference token identity must be a string")
        return ConditionReference.model_validate(
            {
                "text": row.get("human_text"),
                "conditions": row.get("expr"),
                "token": self.token_label(token_id) if token_id is not None else None,
                "role": row.get("role"),
                "created_at": row.get("created_at"),
            }
        )

    async def describe_claim(self, row: dict[str, JsonValue]) -> ClaimReference:
        description = claim_reference(row)
        evidence = row.get("evidence")
        sources: list[DocumentReference] | None = None
        if evidence is not None:
            sources = []
            if not isinstance(evidence, list):
                raise ValueError("Claim evidence must be an array")
            for entry in evidence:
                if not isinstance(entry, dict):
                    raise ValueError("Claim evidence must contain source reference objects")
                owner = ClaimEvidenceOwner.model_validate({"item_id": entry.get("item_id")})
                if owner.item_id is not None:
                    source = await self.reference("item", owner.item_id)
                    if isinstance(source, DocumentReference) and source not in sources:
                        sources.append(source)
        return ClaimReference(
            quote=description.quote,
            subject=description.subject,
            predicate=description.predicate,
            object=description.object,
            sources=sources,
            verified=description.verified,
            stage=description.stage,
        )

    async def describe(self, kind: RecordKind, row: dict[str, JsonValue]) -> RecordReference:
        match kind:
            case "claim":
                return await self.describe_claim(row)
            case "item":
                return document_reference(row)
            case "event":
                return event_reference(row)
            case "condition":
                return self.condition_reference(row)
            case "proposal":
                return ProposalReference.model_validate({"subject": row.get("concept_name")})

    async def reference(self, kind: RecordKind, key: str) -> RecordReference | None:
        """A readable description of a referenced record, read once per projection."""
        if not key:
            raise ValueError("Record references require a nonempty identity")
        if (kind, key) not in self.described:
            row = await self.repo.get(RECORDS[kind][0], key)
            self.described[kind, key] = await self.describe(kind, row) if row is not None else None
        return self.described[kind, key]

    async def project(self, value: JsonInput, *, diagnostic: bool = False) -> JsonValue:
        return await self.project_wire(jsonable(value), diagnostic=diagnostic)

    async def project_wire(self, value: JsonValue, *, diagnostic: bool = False) -> JsonValue:
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
                    return jsonable(clean_value(reference_wire(described)))
            return self.diagnostic_text(value) if diagnostic else clean_text(value)
        if not isinstance(value, dict):
            return value
        result: dict[str, JsonValue] = {}
        fields: tuple[tuple[RecordKind, str, str], ...] = (
            ("item", "item_id", "document"),
            ("claim", "claim_id", "claim"),
            ("condition", "cond_id", "condition"),
            ("proposal", "prp_id", "proposal"),
        )
        for kind, field, label in fields:
            if field in value and value[field] is not None:
                identity = wire_string(value[field], field)
                direct = await self.describe(kind, value)
                description: RecordReference | None = direct
                if not any(reference_wire(direct).values()):
                    description = await self.reference(kind, identity)
                if description is not None:
                    result[label] = jsonable(clean_value(reference_wire(description)))
        for key, item in value.items():
            if key == "token_id" and item is not None:
                result["token"] = self.token_label(wire_string(item, key))
            elif key == "token_ids" and item is not None:
                result["tokens"] = [self.token_label(v) for v in wire_strings(item, key)]
            elif key == "instrument_ids" and item is not None:
                result["instruments"] = [self.instrument_label(v) for v in wire_strings(item, key)]
            elif key == "entity_ids" and item is not None:
                result["entities"] = [self.entity_label(v) for v in wire_strings(item, key)]
            elif key == "entity_id" and item is not None:
                result["entity"] = self.entity_label(wire_string(item, key))
            elif key == "instrument_id" and item is not None:
                result["instrument"] = self.instrument_label(wire_string(item, key))
            elif key == "source_id" and item is not None:
                name = self.source_label(wire_string(item, key))
                if name:
                    result["source"] = name
            elif key in CLAIM_REFERENCE_LABELS:
                result[CLAIM_REFERENCE_LABELS[key]] = [
                    reference_wire(ref)
                    for ref in [await self.reference("claim", v) for v in wire_strings(item, key)]
                    if ref is not None
                ]
            elif key == "evidence" and isinstance(item, list):
                refs: list[JsonValue] = []
                for entry in item:
                    if isinstance(entry, str):
                        ref = None
                        inferred = record_kind(entry)
                        candidates: tuple[RecordKind, ...] = (
                            (inferred,) if inferred is not None else ("claim", "item", "event")
                        )
                        for candidate in candidates:
                            ref = await self.reference(candidate, entry)
                            if ref:
                                break
                        if ref:
                            refs.append(jsonable(clean_value(reference_wire(ref))))
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
                if not OPAQUE.search(named):
                    result[named] = (
                        item
                        if key in LITERAL_FIELDS
                        and isinstance(item, str)
                        and (not diagnostic or key in {"quote", "quoted", "url", "query"})
                        else await self.project(item, diagnostic=diagnostic or key in DIAGNOSTIC_FIELDS)
                    )
        return jsonable(clean_value(result))

    async def result(self, tool: str, result: JsonInput) -> JsonValue:
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

    async def message(self, request: MessageDispatchRequest, item: JsonValue) -> dict[str, JsonValue]:
        request = MessageDispatchRequest.model_validate(request, by_name=True)
        current_event: JsonValue = None
        proposal: JsonValue = None
        open_proposals: list[JsonValue] | None = None
        event_scope = False
        if self.binding.event_id is not None:
            event = await self.repo.get("evt.events", self.binding.event_id)
            if event is not None:
                claims: list[JsonValue] = [
                    row
                    async for row in self.repo.iter_all(
                        "evt.claims", filters={"event_id": self.binding.event_id}
                    )
                ]
                current_event = await self.project({**event, "claims": claims})
                event_scope = True
        proposal_scope = self.binding.proposal_id is not None
        if self.binding.proposal_id is not None:
            proposal = await self.project(
                await self.repo.get("scout.expansion_proposals", self.binding.proposal_id)
            )
            open_proposals = [
                await self.project(row)
                async for row in self.repo.iter_all("scout.expansion_proposals")
                if ProposalAvailability.model_validate({"human_action": row.get("human_action")}).human_action
                is None
            ]
        content = ContextMessageProjection(
            message=await self.project(request.original_wire),
            item=await self.project(item),
            current_event=current_event,
            proposal=proposal,
            open_proposals=open_proposals,
        )
        public = jsonable(
            clean_value(content.to_wire(event_scope=event_scope, proposal_scope=proposal_scope))
        )
        if not isinstance(public, dict):
            raise TypeError("Context message projection must remain an object")
        return public

    def lookup(self, kind: ReferenceKind) -> str:
        if kind in REGISTRY:
            return (
                "Use the registry key shown after « · » next to the name in Tank's messages and results "
                f"(Name · key); a key not shown yet is in {ID_LOOKUP[self.scout]}, «{QUERIES[kind]}»."
            )
        return f"Look it up with read-only SQL: {ID_LOOKUP[self.scout]}, «{QUERIES[kind]}»."

    def public_schema(self, tool: str) -> dict[str, JsonValue]:
        tools = parse_tool_definitions(
            self.settings.read_config_json(f"skills/{AGENT_SKILLS[self.scout]}/tools.json")
        )
        for definition in tools:
            if definition.name == tool:
                return definition.input_schema
        raise ValueError(f"{tool} is not a tool of this agent")

    def validate_shape(self, tool: str, arguments: dict[str, JsonValue]) -> None:
        schema = self.public_schema(tool)
        if tool == "set_beneficiaries":
            schema = novel_layers_allowed(schema)
        legacy = legacy_reference(arguments, schema)
        if legacy:
            raise ValueError(
                f"{legacy.path} is no longer accepted: pass {legacy.replacement}, the record's ID. "
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

    def bind(self, tool: str, result: dict[str, JsonValue]) -> None:
        """Fill the implicit records: the current event, the incoming source and the current proposal."""
        current = self.binding.event_id
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
            if tool == "add_observation":
                source = self.binding.observation_source()
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
                result["item_id"] = self.binding.observation_source()
                if not result["item_id"]:
                    raise ValueError("record_event records the incoming source; this session has none")
            else:
                result.setdefault("item_id", self.binding.observation_source())
                if not result["item_id"]:
                    raise ValueError(
                        "$.item_id: there is no incoming source; pass the item_id to read. "
                        + self.lookup("item")
                    )
        if tool == "record_review":
            result["proposal_id"] = self.binding.proposal_id

    async def check(self, tool: str, path: str, kind: ReferenceKind, value: str) -> None:
        """One record ID: it exists, has the expected kind and lies in the tool's scope."""
        if kind in REGISTRY:
            registered = (
                kind == "token"
                and value in self.tokens
                or kind == "entity"
                and value in self.entities
                or kind == "instrument"
                and value in self.instruments
                or kind == "source"
                and (value in self.sources or await self.repo.get("reg.sources", value) is not None)
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
        current = self.binding.event_id
        if kind == "claim" and tool not in PATTERN_TOOLS:
            owner = EventOwnership.model_validate({key: row[key] for key in ("event_id",) if key in row})
            if owner.event_id != current:
                raise ValueError(f"{path}: this is not a claim of the current event. {self.lookup('claim')}")
        if kind == "condition":
            owner = EventOwnership.model_validate({key: row[key] for key in ("event_id",) if key in row})
            if owner.event_id != current:
                raise ValueError(
                    f"{path}: this watch condition belongs to another event. {self.lookup('condition')}"
                )
        if tool == "merge_into" and path == "$.event_id":
            state = EventMergeState.model_validate({key: row[key] for key in ("lifecycle",) if key in row})
            if state.lifecycle in OPEN_LIFECYCLES_EXCLUDED:
                raise ValueError(
                    f"$.event_id: that event is {state.lifecycle}; merge into an open event. {self.lookup('event')}"
                )
        if tool == "record_review" and path == "$.duplicate_of":
            if value == self.binding.proposal_id:
                raise ValueError(
                    "$.duplicate_of: this is the current proposal; name the earlier one, or null"
                )
            availability = ProposalAvailability.model_validate({"human_action": row.get("human_action")})
            if availability.human_action is not None:
                raise ValueError(
                    f"$.duplicate_of: that proposal is already decided; name an open proposal. "
                    f"{self.lookup('proposal')}"
                )

    async def arguments(self, tool: str, arguments: JsonValue) -> dict[str, JsonValue]:
        """Validate the model's arguments and every record ID in them, then bind the implicit records."""
        if not isinstance(arguments, dict):
            raise ValueError("Tool arguments must be an object")
        self.validate_shape(tool, arguments)
        reject_text_ids(tool, arguments)
        for reference in references(tool, arguments):
            await self.check(tool, reference.path, reference.kind, reference.value)
        result = copy.deepcopy(arguments)
        self.bind(tool, result)
        return result
