"""Classification provenance and read-only routing query contracts."""

from collections.abc import Mapping
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, JsonValue, field_serializer, model_validator

from tank.agents.jev import JevBinding, JevQuestion, JevUsage
from tank.analysis.identity import EntityIdentity, TokenIdentity
from tank.contracts import DomainModel
from tank.contracts.scalars import ZeroInt
from tank.time_precision import Timestamp

type ClassificationName = Literal["relevant", "same_event"]
type EventLifecycle = Literal[
    "DETECTED",
    "SCORED",
    "TRACKED",
    "ANALYZING",
    "ALERTED",
    "MONITORING",
    "COOLDOWN",
    "CLOSED",
    "MERGED",
    "INVALIDATED",
]


class ClassificationIdentity(DomainModel):
    usage_id: str
    questions: dict[str, JevQuestion]
    binding: JevBinding
    state_fingerprint: str
    question_fingerprint: str
    request_fingerprint: str


class MissingClassificationFields(DomainModel):
    effort: str
    cache_read_tokens: str
    cache_write_tokens: str
    cost_usd: str | None = None
    input_tokens: str | None = None
    output_tokens: str | None = None

    def to_wire(self) -> dict[str, str]:
        fields = {
            "effort": self.effort,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
        }
        if self.cost_usd is not None:
            fields["cost_usd"] = self.cost_usd
        if self.input_tokens is not None:
            fields["input_tokens"] = self.input_tokens
        if self.output_tokens is not None:
            fields["output_tokens"] = self.output_tokens
        return fields


class JevScoreRecord(DomainModel):
    usage_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    item_id: str
    call: Literal["FILTER", "ROUTE"]
    model_id: str
    semantic_model_id: Literal["jev-1.13.0"]
    requested_model_id: Literal["jev-1.13-free", "jev-1.13"]
    jev_provider: Literal["opencode"]
    endpoint: Literal["https://opencode.ai/inference/systemone/v1/systemone"]
    resolved_model_id: str
    response_id: str | None
    upstream_provider: str | None
    model_pin_evidence: str
    billing_basis: Literal["OPENCODE_REPORTED"]
    billing_currency: Literal["USD"]
    prompt_version: str
    questions: dict[str, JevQuestion] = Field(min_length=1)
    state_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    question_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    request_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    session_id: None
    score: float = Field(ge=0, le=1)
    usage: JevUsage
    input_tokens: int | None = Field(ge=0)
    output_tokens: int | None = Field(ge=0)
    effort: None
    stop_reason: Literal["NON_GENERATIVE_CLASSIFICATION"]
    web_searches: ZeroInt
    missing_fields: MissingClassificationFields
    cost_usd: Decimal | None = Field(ge=0)
    cost_reason: str | None
    at: str
    at_prec: Literal["COLLECTION_ONLY"]
    at_prec_reason: None
    created_at: str
    created_at_prec: Literal["COLLECTION_ONLY"]
    created_at_prec_reason: None
    subject_id: str
    model: str
    config_version: str
    code_version: str

    @field_serializer("missing_fields")
    def serialize_missing_fields(self, fields: MissingClassificationFields) -> dict[str, str]:
        return fields.to_wire()

    @model_validator(mode="after")
    def coherent_provenance(self) -> Self:
        if self.item_id != self.subject_id:
            raise ValueError("Classification subject differs from its source item")
        if self.model != self.model_id or self.resolved_model_id != self.model_id:
            raise ValueError("Classification model identities disagree")
        if self.model_id != self.requested_model_id:
            raise ValueError("Classification resolved model differs from its pinned request")
        expected = "relevant" if self.call == "FILTER" else "same_event"
        if set(self.questions) != {expected}:
            raise ValueError("Classification ledger questions disagree with its call")
        if self.input_tokens != self.usage.input_tokens or self.output_tokens != self.usage.output_tokens:
            raise ValueError("Classification accounting differs from its provider usage")
        if self.cost_usd != self.usage.cost:
            raise ValueError("Classification cost differs from its provider usage")
        if self.cost_usd is None and (self.cost_reason is None or self.missing_fields.cost_usd is None):
            raise ValueError("Unreported classification cost requires its explicit reason")
        if self.cost_usd is not None and (
            self.cost_reason is not None or self.missing_fields.cost_usd is not None
        ):
            raise ValueError("Reported classification cost cannot be marked missing")
        if (self.input_tokens is None) != (self.missing_fields.input_tokens is not None):
            raise ValueError("Classification input token availability lacks a consistent reason")
        if (self.output_tokens is None) != (self.missing_fields.output_tokens is not None):
            raise ValueError("Classification output token availability lacks a consistent reason")
        Timestamp(self.at, self.at_prec, self.at_prec_reason)
        Timestamp(self.created_at, self.created_at_prec, self.created_at_prec_reason)
        if self.at != self.created_at:
            raise ValueError("Classification completion and creation clocks disagree")
        return self


class RoutingEntity(EntityIdentity):
    tokens: list[str] = Field(default_factory=list)


class RoutingAccount(DomainModel):
    handle: str = Field(min_length=1)
    entity_id: str | None = None


class RoutingRegistry(DomainModel):
    entities: list[RoutingEntity]
    tokens: list[TokenIdentity]
    accounts: list[RoutingAccount]

    @classmethod
    def from_wire(cls, record: Mapping[str, JsonValue]) -> Self:
        projected: dict[str, JsonValue] = {}
        for wire_name, domain_name, fields in (
            (
                "entities",
                "entities",
                ("entity_id", "name", "names", "aliases", "qualified_aliases", "tokens"),
            ),
            ("tokens", "tokens", ("token_id", "symbol", "entity_id")),
            ("x_accounts", "accounts", ("handle", "entity_id")),
        ):
            rows = record.get(wire_name, [])
            if not isinstance(rows, list):
                raise ValueError("Routing registry namespaces must be arrays")
            values: list[JsonValue] = []
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError("Routing registry namespaces must contain objects")
                values.append({field: row[field] for field in fields if field in row})
            projected[domain_name] = values
        return cls.model_validate(projected)


class RoutingCandidate(DomainModel):
    event_id: str = Field(min_length=1)
    statement: str = Field(min_length=1)
    lifecycle: EventLifecycle
    entities: list[str]
    tokens_mentioned: list[str]
    event_state: str = Field(min_length=1)

    @classmethod
    def from_wire(cls, record: Mapping[str, JsonValue]) -> Self:
        raw_entities = record["entities"]
        if not isinstance(raw_entities, list):
            raise ValueError("Routing candidate entities must be an array")
        entities: list[JsonValue] = []
        for entity in raw_entities:
            if isinstance(entity, dict):
                entities.append(entity["entity_id"])
            else:
                entities.append(entity)
        return cls.model_validate(
            {
                "event_id": record["event_id"],
                "statement": record["statement"],
                "lifecycle": record["lifecycle"],
                "entities": entities,
                "tokens_mentioned": record["tokens_mentioned"],
                "event_state": record["event_state"],
            }
        )


class RoutingSession(DomainModel):
    event_id: str | None
    status: Literal["OPEN", "FAILED", "ENDED"]

    @classmethod
    def from_wire(cls, record: Mapping[str, JsonValue]) -> Self:
        return cls.model_validate({"event_id": record["event_id"], "status": record["status"]})
