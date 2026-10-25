"""Generation provenance shared by analysis and agent orchestration."""

from pydantic import Field, field_validator

from tank.contracts import DomainModel


class ResolvedVersionIdentity(DomainModel):
    config_version: str = Field(min_length=1)
    code_version: str = Field(min_length=1)

    @field_validator("config_version", "code_version")
    @classmethod
    def declared_identity(cls, value: str) -> str:
        if not value.strip() or value.casefold() in {"unversioned", "unknown"}:
            raise ValueError("Resolved provenance requires a declared version identity")
        return value


class ResolvedSkillVersion(DomainModel):
    type: str
    skill_id: str
    version: str


class PromptBinding(DomainModel):
    snapshot_fields: list[str]
    resolved_skill_versions: list[ResolvedSkillVersion]


class GenerationProvenance(DomainModel):
    config_version: str
    code_version: str
    session_config_version: str | None
    session_code_version: str | None
    model_id: str | None
    model_id_basis: str | None
    model_id_reason: str | None
    prompt_version: str | None
    prompt_version_basis: str | None
    prompt_version_reason: str | None
    prompt_label: str | None
    prompt_binding: PromptBinding | None
    agent_version: str | None
    agent_version_basis: str | None
    agent_version_reason: str | None
    session_id: str | None


class AgentProvenance(DomainModel):
    """Legacy session snapshots can lack an agent identifier; never attribute them to a current agent."""

    agent_id: str | None
    model_id: str
    model_id_basis: str
    agent_version: str | None
    agent_version_basis: str | None
    agent_version_reason: str | None
    prompt_label: str | None
    prompt_version: str | None
    prompt_version_basis: str | None
    prompt_version_reason: str | None
    prompt_binding: PromptBinding | None
