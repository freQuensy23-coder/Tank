"""Typed native session discovery and creation projections."""

from typing import Literal, Protocol, Self

from pydantic import Field, JsonValue, model_validator

from tank.agents.provenance import resolve_agent_provenance
from tank.contracts import DomainModel
from tank.contracts.openma import AgentSkillReference, ManagedAgentEffort
from tank.contracts.provenance import AgentProvenance
from tank.contracts.session_admission import SessionTarget
from tank.util import sha256


class NativeReplayIdentity(DomainModel):
    run_id: str = Field(min_length=1)
    source_session_id: str | None = Field(min_length=1)
    evidence_sha256: str | None = Field(pattern=r"^[a-f0-9]{64}$")
    cutoff: str | None = Field(min_length=1)

    @model_validator(mode="after")
    def bound_evidence(self) -> Self:
        if self.evidence_sha256 is not None and self.cutoff is None:
            raise ValueError("Recorded replay evidence requires its bound cutoff")
        return self


class NativeSessionResolution(DomainModel):
    target: SessionTarget
    role: Literal["ANALYST", "SCOUT"]
    replay: NativeReplayIdentity | None

    @classmethod
    def from_wire(
        cls, payload: dict[str, JsonValue], *, role: Literal["ANALYST", "SCOUT"], replay: bool
    ) -> Self:
        if not replay and any(
            payload.get(field) is not None
            for field in (
                "replay_run_id",
                "replay_source_session_id",
                "replay_evidence_sha256",
                "replay_cutoff",
            )
        ):
            raise ValueError("Replay identity requires explicit replay composition")
        replay_identity = (
            NativeReplayIdentity.model_validate(
                {
                    "run_id": payload.get("replay_run_id"),
                    "source_session_id": payload.get("replay_source_session_id"),
                    "evidence_sha256": payload.get("replay_evidence_sha256"),
                    "cutoff": payload.get("replay_cutoff"),
                }
            )
            if replay
            else None
        )
        return cls(
            target=SessionTarget.model_validate(
                {field: payload.get(field) for field in ("session_id", "event_id", "item_id", "proposal_id")}
            ),
            role=role,
            replay=replay_identity,
        )


class NativeSessionDeployment(DomainModel):
    agent_id: str = Field(min_length=1)
    environment_id: str = Field(min_length=1)
    prompt_version: str = Field(min_length=1)
    effort: ManagedAgentEffort

    @classmethod
    def from_wire(
        cls, config: dict[str, JsonValue], *, role: Literal["ANALYST", "SCOUT"], replay: bool
    ) -> Self:
        return cls.model_validate(
            {
                "agent_id": config["scout_agent_id" if role == "SCOUT" else "agent_id"],
                "environment_id": config["replay_environment_id" if replay else "environment_id"],
                "prompt_version": config["prompt_version"],
                "effort": config["reasoning_effort"],
            }
        )


class NativeSessionIdentity(DomainModel):
    identity: str = Field(min_length=1)
    scout: bool
    model_context_version: int = Field(ge=1)
    replay_run_id: str | None = Field(min_length=1)

    def marker(self) -> str:
        return sha256(self.model_dump(mode="json", exclude_none=True))


class NativeSessionIdentityMetadata(DomainModel):
    tank_identity: str = Field(min_length=1)
    tank_replay_run_id: str | None = Field(default=None, min_length=1)
    tank_replay_evidence_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    tank_replay_cutoff: str | None = Field(default=None, min_length=1)

    @classmethod
    def from_binding(cls, identity: str, replay: NativeReplayIdentity | None) -> Self:
        return cls(
            tank_identity=identity,
            tank_replay_run_id=replay.run_id if replay is not None else None,
            tank_replay_evidence_sha256=replay.evidence_sha256 if replay is not None else None,
            tank_replay_cutoff=replay.cutoff if replay is not None else None,
        )

    @model_validator(mode="after")
    def complete_identity_binding(self) -> Self:
        if self.tank_replay_evidence_sha256 is not None and (
            self.tank_replay_run_id is None or self.tank_replay_cutoff is None
        ):
            raise ValueError("Recorded session metadata requires its replay run and cutoff")
        if self.tank_replay_cutoff is not None and self.tank_replay_run_id is None:
            raise ValueError("Replay cutoff metadata requires its run identity")
        return self

    def expected_fields(self) -> dict[str, str]:
        fields = {"tank_identity": self.tank_identity}
        if self.tank_replay_run_id is not None:
            fields["tank_replay_run_id"] = self.tank_replay_run_id
        if self.tank_replay_evidence_sha256 is not None:
            fields["tank_replay_evidence_sha256"] = self.tank_replay_evidence_sha256
        if self.tank_replay_cutoff is not None:
            fields["tank_replay_cutoff"] = self.tank_replay_cutoff
        return fields


class NativeSessionCreate(DomainModel):
    agent: str = Field(min_length=1)
    environment_id: str = Field(min_length=1)
    metadata: NativeSessionIdentityMetadata


class NativeSessionHeader(DomainModel):
    id: str = Field(min_length=1)
    environment_id: str = Field(min_length=1)
    metadata: dict[str, str]


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


class NativeSessionObservation(DomainModel):
    session_id: str = Field(min_length=1)
    environment_id: str = Field(min_length=1)
    metadata: dict[str, str]
    provenance: AgentProvenance
    effort: ManagedAgentEffort | None

    def verify_binding(self, wanted: NativeSessionCreate) -> None:
        if self.environment_id != wanted.environment_id or self.provenance.agent_id != wanted.agent:
            raise ValueError("Native session belongs to a different deployment binding")
        if any(self.metadata.get(key) != value for key, value in wanted.metadata.expected_fields().items()):
            raise ValueError("Native session identity metadata does not match the requested binding")


class NativeSessionPage(DomainModel):
    sessions: list[NativeSessionObservation]
    next_page: str | None = Field(min_length=1)


def observe_native_session(
    wire: dict[str, JsonValue], deployment: NativeSessionDeployment
) -> NativeSessionObservation:
    header = NativeSessionHeader.model_validate(
        {field: wire[field] for field in ("id", "environment_id", "metadata")}
    )
    agent = wire["agent"]
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
    provenance = resolve_agent_provenance(
        wire,
        {"prompt_version": deployment.prompt_version},
        declared_agent_id=deployment.agent_id,
        declared_model_id=prompt.model.id,
    )
    return NativeSessionObservation(
        session_id=header.id,
        environment_id=header.environment_id,
        metadata=header.metadata,
        provenance=provenance,
        effort=prompt.model.effort.type if prompt.model.effort is not None else None,
    )


def observe_native_session_page(
    wire: dict[str, JsonValue], deployment: NativeSessionDeployment
) -> NativeSessionPage:
    data = wire["data"]
    if not isinstance(data, list):
        raise ValueError("Native session page data must be an array")
    sessions: list[NativeSessionObservation] = []
    for row in data:
        if not isinstance(row, dict):
            raise ValueError("Native session page entries must be objects")
        sessions.append(observe_native_session(row, deployment))
    return NativeSessionPage.model_validate({"sessions": sessions, "next_page": wire["next_page"]})


class NativeOpeningSource(DomainModel):
    item_id: str | None = Field(min_length=1)
    opened_by_item: str | None = Field(min_length=1)

    @model_validator(mode="after")
    def consistent_origin(self) -> Self:
        if (
            self.item_id is not None
            and self.opened_by_item is not None
            and self.item_id != self.opened_by_item
        ):
            raise ValueError("Durable opening source identities disagree")
        return self

    def opening_item(self) -> str | None:
        return self.opened_by_item if self.opened_by_item is not None else self.item_id


class NativeSessionEndpoint(Protocol):
    async def request(
        self,
        method: Literal["GET", "POST"],
        path: str,
        *,
        json: JsonValue = None,
        params: dict[str, str | int] | None = None,
        idem: str | None = None,
    ) -> dict[str, JsonValue]: ...


class NativeSessionDiscovery:
    def __init__(self, endpoint: NativeSessionEndpoint, deployment: NativeSessionDeployment) -> None:
        self.endpoint = endpoint
        self.deployment = deployment

    async def discover_or_create(self, wanted: NativeSessionCreate, *, idem: str) -> NativeSessionObservation:
        if (
            wanted.agent != self.deployment.agent_id
            or wanted.environment_id != self.deployment.environment_id
        ):
            raise ValueError("Native creation request differs from its configured deployment")
        remote: NativeSessionObservation | None = None
        page: str | None = None
        seen_pages: set[str] = set()
        while True:
            params: dict[str, str | int] = {"limit": 100}
            if page is not None:
                params["page"] = page
            listing = observe_native_session_page(
                await self.endpoint.request("GET", "sessions", params=params), self.deployment
            )
            for row in listing.sessions:
                if row.metadata.get("tank_identity") == wanted.metadata.tank_identity:
                    row.verify_binding(wanted)
                    if remote is not None:
                        raise ValueError("Native session discovery repeated the requested identity")
                    remote = row
            page = listing.next_page
            if page is None:
                break
            if page in seen_pages:
                raise ValueError("Native session discovery repeated a pagination cursor")
            seen_pages.add(page)
        if remote is None:
            remote = observe_native_session(
                await self.endpoint.request(
                    "POST", "sessions", json=wanted.model_dump(mode="json", exclude_none=True), idem=idem
                ),
                self.deployment,
            )
            remote.verify_binding(wanted)
        return remote


class NativeStoredSessionBinding(DomainModel):
    session_id: str = Field(min_length=1)
    replay_run_id: str | None = Field(min_length=1)
    replay_source_session_id: str | None = Field(min_length=1)
    replay_evidence_sha256: str | None = Field(pattern=r"^[a-f0-9]{64}$")
    replay_cutoff: str | None = Field(min_length=1)

    @classmethod
    def from_record(cls, record: dict[str, JsonValue]) -> Self:
        return cls.model_validate(
            {
                "session_id": record["session_id"],
                **{
                    field: record.get(field)
                    for field in (
                        "replay_run_id",
                        "replay_source_session_id",
                        "replay_evidence_sha256",
                        "replay_cutoff",
                    )
                },
            }
        )

    def matches_origin_scope(self, request: NativeSessionResolution) -> bool:
        if request.replay is not None and request.replay.source_session_id is not None:
            return self.session_id == request.replay.source_session_id
        return self.replay_run_id is None

    def verify(self, request: NativeSessionResolution) -> None:
        if request.target.session_id is not None and request.target.session_id != self.session_id:
            raise ValueError("Stored session identity differs from the requested resource")
        replay = request.replay
        if replay is None:
            if self.replay_run_id is not None:
                raise ValueError("Live input cannot reuse a replay session")
            return
        if self.replay_run_id != replay.run_id:
            raise ValueError("Replay cannot reuse an original session")
        if replay.evidence_sha256 is not None and replay.evidence_sha256 != self.replay_evidence_sha256:
            raise ValueError("Stored replay evidence binding differs from the requested input")
        if replay.cutoff is not None and replay.cutoff != self.replay_cutoff:
            raise ValueError("Stored replay cutoff differs from the requested input")
        if replay.source_session_id is not None and replay.source_session_id != self.replay_source_session_id:
            raise ValueError("Stored replay source session differs from the requested input")


def native_role_for_message(kind: JsonValue) -> Literal["ANALYST", "SCOUT"]:
    match kind:
        case "SCOUT_REVIEW":
            return "SCOUT"
        case "NEW_ITEM" | "ITEM_ADDED" | "USER_QUESTION" | "CONDITION_FIRED" | "CALL_EXPIRED" | "MILESTONE":
            return "ANALYST"
        case _:
            raise ValueError("Unsupported native session input kind")
