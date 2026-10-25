"""Translate native session snapshots into explicit, auditable generation provenance."""

from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.openma import (
    AgentSkillReference,
    BuiltinToolObservation,
    CustomToolObservation,
    ManagedAgentEffort,
    ToolObservation,
    UnsupportedToolObservation,
    observe_openma_tool,
)
from tank.contracts.provenance import AgentProvenance, PromptBinding, ResolvedSkillVersion
from tank.util import sha256

_PROMPT_FIELDS = ("system", "skills", "tools", "model", "mcp_servers", "multiagent", "_oma")
_MISSING_VERSION = "Deployed agent version not exposed by remote snapshot or configuration"


def _version(value: JsonValue) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value
    if isinstance(value, int) and not isinstance(value, bool) and value >= 1:
        return str(value)
    raise ValueError("Native agent version must be a nonempty string or positive integer")


def _text(value: JsonValue, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Native agent {field} must be a nonempty string")
    return value


def resolve_agent_provenance(
    remote: dict[str, JsonValue],
    declaration: dict[str, JsonValue],
    *,
    declared_agent_id: str | None,
    declared_model_id: str,
) -> AgentProvenance:
    raw_snapshot = remote.get("agent")
    if raw_snapshot is None or isinstance(raw_snapshot, str):
        snapshot: dict[str, JsonValue] = {}
    elif isinstance(raw_snapshot, dict):
        snapshot = raw_snapshot
    else:
        raise ValueError("Native session agent snapshot has invalid shape")
    version = _version(snapshot.get("version"))
    version_basis = "RESOLVED_SESSION_AGENT"
    if version is None:
        version = _version(declaration.get("agent_version"))
        version_basis = "DEPLOYMENT_DECLARATION"
    model = snapshot.get("model")
    if model is None:
        model_id, model_basis = declared_model_id, "DEPLOYMENT_DECLARATION"
    elif isinstance(model, dict):
        model_id = _text(model["id"], "model.id")
        model_basis = "RESOLVED_SESSION_AGENT"
    else:
        raise ValueError("Native agent model snapshot has invalid shape")
    agent_id = _text(snapshot["id"], "id") if "id" in snapshot else declared_agent_id
    skills: list[ResolvedSkillVersion] = []
    complete = all(field in snapshot for field in ("system", "skills", "tools"))
    if complete:
        raw_skills = snapshot["skills"]
        if not isinstance(raw_skills, list):
            raise ValueError("Native agent skills snapshot must be a list")
        for raw_skill in raw_skills:
            if not isinstance(raw_skill, dict):
                raise ValueError("Native agent resolved skill has invalid shape")
            skill_version = _version(raw_skill.get("version"))
            if skill_version is None or skill_version == "latest":
                complete = False
                break
            skills.append(
                ResolvedSkillVersion(
                    type=_text(raw_skill["type"], "skill.type"),
                    skill_id=_text(raw_skill["skill_id"], "skill.skill_id"),
                    version=skill_version,
                )
            )
    prompt: dict[str, JsonValue] = {field: snapshot.get(field) for field in _PROMPT_FIELDS}
    prompt_label = declaration.get("prompt_version")
    if prompt_label is not None:
        prompt_label = _text(prompt_label, "prompt label")
    return AgentProvenance(
        agent_id=agent_id,
        model_id=model_id,
        model_id_basis=model_basis,
        agent_version=version,
        agent_version_basis=version_basis if version is not None else None,
        agent_version_reason=_MISSING_VERSION if version is None else None,
        prompt_label=prompt_label,
        prompt_version=sha256(prompt) if complete else None,
        prompt_version_basis="RESOLVED_SESSION_AGENT_SNAPSHOT" if complete else None,
        prompt_version_reason=None if complete else "INCOMPLETE: remote resolved prompt snapshot unavailable",
        prompt_binding=PromptBinding(snapshot_fields=sorted(prompt), resolved_skill_versions=skills)
        if complete
        else None,
    )


class NativeResolvedEffort(DomainModel):
    type: ManagedAgentEffort


class NativeResolvedModelIdentity(DomainModel):
    id: str = Field(min_length=1)
    effort: NativeResolvedEffort | None = None

    @model_validator(mode="after")
    def reported_effort_is_not_null(self) -> Self:
        if "effort" in self.model_fields_set and self.effort is None:
            raise ValueError("Native model effort can be omitted but cannot be null")
        return self


class NativeResolvedPromptHeader(DomainModel):
    id: str = Field(min_length=1)
    version: int = Field(ge=1)
    model: NativeResolvedModelIdentity
    system: str | None = Field(repr=False)
    skills: list[AgentSkillReference]
    tools: list[dict[str, JsonValue]] = Field(repr=False)
    mcp_servers: list[dict[str, JsonValue]] = Field(repr=False)
    multiagent: dict[str, JsonValue] | None = Field(repr=False)
    extension: dict[str, JsonValue] | None = Field(default=None, validation_alias="_oma", repr=False)

    @model_validator(mode="after")
    def reported_extension_is_not_null(self) -> Self:
        if "extension" in self.model_fields_set and self.extension is None:
            raise ValueError("Native agent extension can be omitted but cannot be null")
        return self


class NativePermissionPolicy(DomainModel):
    type: Literal["always_allow", "always_ask"]


class NativeResolvedDefaultTools(DomainModel):
    enabled: bool
    permission_policy: NativePermissionPolicy


class NativeResolvedToolSwitch(DomainModel):
    name: str = Field(min_length=1)
    enabled: bool
    permission_policy: NativePermissionPolicy


class NativePromptObservation(DomainModel):
    provenance: AgentProvenance
    effort: ManagedAgentEffort | None
    tools: tuple[ToolObservation, ...]


def _complete_resolved_tool(raw: dict[str, JsonValue], observed: ToolObservation) -> None:
    if isinstance(observed, UnsupportedToolObservation):
        raise ValueError("Native resolved prompt contains an unsupported tool type")
    if isinstance(observed, CustomToolObservation):
        if observed.description is None or observed.input_schema is None:
            raise ValueError("Native resolved custom tool contract is incomplete")
        if observed.input_schema.get("type") != "object":
            raise ValueError("Native resolved custom tool requires its object input schema")
        return
    if observed.default_enabled is None or observed.configs is None:
        raise ValueError("Native resolved tool configuration is incomplete")
    NativeResolvedDefaultTools.model_validate(raw["default_config"])
    configs = raw["configs"]
    if not isinstance(configs, list):
        raise ValueError("Native resolved tool configurations must be an array")
    for config in configs:
        if not isinstance(config, dict):
            raise ValueError("Native resolved tool configuration must be an object")
        NativeResolvedToolSwitch.model_validate(
            {field: config[field] for field in ("name", "enabled", "permission_policy")}
        )
        if isinstance(observed, BuiltinToolObservation) and config["type"] != config["name"]:
            raise ValueError("Native resolved builtin tool type differs from its name")


def observe_resolved_native_prompt(
    remote: dict[str, JsonValue], *, prompt_label: str | None
) -> NativePromptObservation:
    agent = remote["agent"]
    if not isinstance(agent, dict):
        raise ValueError("Native session must report its resolved agent snapshot")
    model = agent["model"]
    if not isinstance(model, dict):
        raise ValueError("Native session must report its resolved model identity")
    prompt = NativeResolvedPromptHeader.model_validate(
        {
            **{
                field: agent[field]
                for field in ("id", "version", "system", "skills", "tools", "mcp_servers", "multiagent")
            },
            "model": {
                "id": model["id"],
                **({"effort": model["effort"]} if "effort" in model else {}),
            },
            **({"_oma": agent["_oma"]} if "_oma" in agent else {}),
        }
    )
    tools = tuple(observe_openma_tool(raw) for raw in prompt.tools)
    for raw, observed in zip(prompt.tools, tools, strict=True):
        _complete_resolved_tool(raw, observed)
    provenance = resolve_agent_provenance(
        remote,
        {"prompt_version": prompt_label},
        declared_agent_id=prompt.id,
        declared_model_id=prompt.model.id,
    )
    return NativePromptObservation(
        provenance=provenance,
        effort=prompt.model.effort.type if prompt.model.effort is not None else None,
        tools=tools,
    )
