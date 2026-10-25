"""Native polling state and required scout/session identity projections."""

import re
from typing import Literal, Self

from pydantic import Field, JsonValue

from tank.agents.context_binding import ContextBinding
from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.contracts.provenance import AgentProvenance
from tank.contracts.session_admission import BUSY_RUNTIME_STATES, SessionRuntimeStatus
from tank.time_precision import Timestamp


class ExecutionPoll(DomainModel):
    deadline: float
    delay: float = Field(gt=0)
    signature: tuple[str, ...]


class HistoryPoll(DomainModel):
    deadline: float
    delay: float = Field(gt=0)


class ScoutProposalIdentity(DomainModel):
    prp_id: str = Field(min_length=1)


class ScoutSessionReference(DomainModel):
    session_id: str = Field(min_length=1)
    proposal_id: str = Field(min_length=1)


class LegacyPromptDescriptor(DomainModel):
    prompt_version: str | None = Field(min_length=1)
    prompt_version_basis: str | None = Field(min_length=1)
    prompt_version_reason: str | None
    prompt_label: str | None = Field(min_length=1)
    agent_version: str | None = Field(min_length=1)
    model_id: str | None = Field(min_length=1)

    @classmethod
    def from_record(cls, record: dict[str, JsonValue]) -> Self:
        return cls.model_validate({field: record.get(field) for field in cls.model_fields})

    def has_resolved_snapshot(self) -> bool:
        return (
            self.prompt_version_basis == "RESOLVED_SESSION_AGENT_SNAPSHOT"
            and self.prompt_version is not None
            and re.fullmatch(r"[a-f0-9]{64}", self.prompt_version) is not None
        )

    def recovery_label(self) -> str | None:
        if self.prompt_label is not None:
            return self.prompt_label
        if self.prompt_version_basis is None:
            return self.prompt_version
        return None

    def document(self) -> dict[str, JsonValue]:
        value = decode_json(self.model_dump_json())
        if not isinstance(value, dict):
            raise TypeError("Legacy prompt descriptor must serialize as an object")
        return value


class SessionPromptState(DomainModel):
    session_id: str = Field(min_length=1)
    descriptor: LegacyPromptDescriptor
    has_legacy_descriptor: bool

    @classmethod
    def from_record(cls, record: dict[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "session_id": record["session_id"],
                "descriptor": LegacyPromptDescriptor.from_record(record),
                "has_legacy_descriptor": "legacy_prompt_descriptor" in record,
            }
        )


class PromptRecoveryRecord(DomainModel):
    provenance: AgentProvenance
    recovered_at: Timestamp
    legacy: LegacyPromptDescriptor | None

    def provenance_document(self) -> dict[str, JsonValue]:
        value = decode_json(self.provenance.model_dump_json())
        if not isinstance(value, dict):
            raise TypeError("Recovered provenance must serialize as an object")
        return value

    def recovery_document(self) -> dict[str, JsonValue]:
        return {**self.provenance_document(), **self.recovered_at.fields("recovered_at")}


class LegacyToolContext(DomainModel):
    version: int | None = Field(ge=0)
    active_message_id: str | None = Field(min_length=1)
    legacy_message_ids: list[str]

    @classmethod
    def from_record(cls, record: dict[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "version": record.get("model_context_version"),
                "active_message_id": record.get("active_message_id"),
                "legacy_message_ids": record.get("legacy_model_message_ids", []),
            }
        )

    def needs_migration(self, current_version: int) -> bool:
        return self.version is None or self.version < current_version

    def has_legacy_turn(self, current_version: int) -> bool:
        return self.needs_migration(current_version) or (
            self.active_message_id is not None and self.active_message_id in self.legacy_message_ids
        )


class ToolMessageVersion(DomainModel):
    message_id: str = Field(min_length=1)
    context_version: int | None = Field(ge=0)


class SessionToolBoundaryState(DomainModel):
    session_id: str = Field(min_length=1)
    proposal_id: str | None = Field(min_length=1)
    context: LegacyToolContext
    runtime_status: SessionRuntimeStatus | None
    tools_sha256: str | None = Field(pattern=r"^[a-f0-9]{64}$")
    context_note_version: int | None = Field(ge=0)
    messages: tuple[ToolMessageVersion, ...]
    prompt: LegacyPromptDescriptor

    @classmethod
    def from_record(cls, record: dict[str, JsonValue]) -> Self:
        binding = ContextBinding.from_session(record)
        journal = record.get("message_log", [])
        if not isinstance(journal, list):
            raise ValueError("Tool boundary input journal must be an array")
        messages: list[ToolMessageVersion] = []
        for row in journal:
            if not isinstance(row, dict):
                raise ValueError("Tool boundary journal must contain input objects")
            messages.append(
                ToolMessageVersion.model_validate(
                    {"message_id": row["message_id"], "context_version": row.get("model_context_version")}
                )
            )
        return cls.model_validate(
            {
                "session_id": binding.session_id,
                "proposal_id": binding.proposal_id,
                "context": LegacyToolContext.from_record(record),
                "runtime_status": record.get("runtime_status"),
                "tools_sha256": record.get("tools_sha256"),
                "context_note_version": record.get("context_note_version"),
                "messages": tuple(messages),
                "prompt": LegacyPromptDescriptor.from_record(record),
            }
        )

    def is_busy(self) -> bool:
        return self.context.active_message_id is not None or self.runtime_status in BUSY_RUNTIME_STATES

    def legacy_inputs(self, current_version: int) -> list[str]:
        return [
            message.message_id
            for message in self.messages
            if message.context_version is None or message.context_version < current_version
        ]


class NativeToolSyncResult(DomainModel):
    status: Literal["UNCHANGED", "BUSY", "UPDATED", "READ_ONLY"]
