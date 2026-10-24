"""Translate native session snapshots into explicit, auditable generation provenance."""

from pydantic import JsonValue

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
