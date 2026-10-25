"""Closed OpenMA configuration and synchronization boundary contracts."""

from collections.abc import Sequence
from pathlib import PurePosixPath
from typing import Literal, Protocol, Self

from pydantic import Field, JsonValue, SecretStr, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedSkillVersion, ResolvedVersionIdentity
from tank.contracts.tool_schema import ToolDefinition, parse_tool_definitions

type ManagedSkillName = Literal["tank-event-analyst", "tank-data", "tank-scout-reviewer"]
type ManagedAgentName = Literal["tank-event-analyst", "tank-scout-reviewer"]
type ManagedEnvironmentName = Literal["tank-analysis", "tank-replay"]
type ManagedAgentEffort = Literal["low", "medium", "high", "xhigh", "max"]


class OpenMAModelPrices(DomainModel):
    input_per_million: int | float = Field(ge=0)
    output_per_million: int | float = Field(ge=0)
    cache_read_per_million: int | float = Field(ge=0)
    cache_creation_per_million: int | float = Field(ge=0)


class OpenMAConfiguration(DomainModel):
    version: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    scout_agent_id: str = Field(min_length=1)
    environment_id: str = Field(min_length=1)
    replay_environment_id: str = Field(min_length=1)
    release: str = Field(pattern=r"^[a-f0-9]{40}$")
    prompt_version: str = Field(min_length=1)
    model_id: str = Field(min_length=1)
    reasoning_effort: ManagedAgentEffort
    model_prices: dict[str, OpenMAModelPrices]
    pricing_basis: str = Field(min_length=1)
    model_access: Literal["CHATGPT_SUBSCRIPTION_CODEX_OAUTH"]
    gateway_base_url: str = Field(min_length=1)
    model_card_provider: str = Field(min_length=1)
    openma_overlay: str = Field(min_length=1)
    max_running_sessions: int = Field(ge=1)

    @model_validator(mode="after")
    def declared_model_prices(self) -> Self:
        if self.agent_id == self.scout_agent_id:
            raise ValueError("Managed analyst and scout require distinct agent identities")
        if self.environment_id == self.replay_environment_id:
            raise ValueError("Managed live and replay require distinct environment identities")
        if self.model_id not in self.model_prices:
            raise ValueError("OpenMA model requires explicitly configured prices")
        return self

    def agent_id_for(self, name: ManagedAgentName) -> str:
        match name:
            case "tank-event-analyst":
                return self.agent_id
            case "tank-scout-reviewer":
                return self.scout_agent_id
            case _:
                raise ValueError("Unknown managed OpenMA agent")

    def environment_id_for(self, name: ManagedEnvironmentName) -> str:
        match name:
            case "tank-analysis":
                return self.environment_id
            case "tank-replay":
                return self.replay_environment_id
            case _:
                raise ValueError("Unknown managed OpenMA environment")


class SkillFile(DomainModel):
    relative_path: str = Field(min_length=1)
    content: bytes = Field(repr=False)

    @field_validator("relative_path")
    @classmethod
    def relative_config_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if not path.parts or path.is_absolute() or ".." in path.parts or path.as_posix() != value:
            raise ValueError("Skill upload requires a relative captured file path")
        return value


class ToolEnablement(DomainModel):
    enabled: bool


class ToolSwitch(DomainModel):
    name: str = Field(min_length=1)
    enabled: bool


class ManagedBuiltinToolset(DomainModel):
    type: Literal["agent_toolset_20260401"]
    default_config: ToolEnablement
    configs: list[ToolSwitch]


class ManagedCustomTool(ToolDefinition):
    type: Literal["custom"]


type ManagedTool = ManagedBuiltinToolset | ManagedCustomTool


class ManagedToolManifest(DomainModel):
    agent: ManagedAgentName
    tools: tuple[ManagedTool, ...] = Field(min_length=1)

    @classmethod
    def from_definitions(cls, agent: ManagedAgentName, value: JsonValue) -> Self:
        definitions = parse_tool_definitions(value)
        builtin = ManagedBuiltinToolset(
            type="agent_toolset_20260401",
            default_config=ToolEnablement(enabled=True),
            configs=[ToolSwitch(name="browser", enabled=True)],
        )
        return cls(
            agent=agent,
            tools=(
                builtin,
                *(
                    ManagedCustomTool(
                        type="custom",
                        name=definition.name,
                        description=definition.description,
                        input_schema=definition.input_schema,
                    )
                    for definition in definitions
                ),
            ),
        )


class NativeSessionToolOverride(DomainModel):
    tools: tuple[ManagedTool, ...] = Field(min_length=1)


class NativeSessionToolUpdate(DomainModel):
    agent: NativeSessionToolOverride


class ProviderSkillVersion(ResolvedSkillVersion):
    type: Literal["custom", "anthropic"]
    skill_id: str = Field(min_length=1)
    version: str = Field(min_length=1)


class ManagedSkillVersion(ProviderSkillVersion):
    type: Literal["custom"]


class AnthropicSkillVersion(ProviderSkillVersion):
    type: Literal["anthropic"]


type AgentSkillReference = ManagedSkillVersion | AnthropicSkillVersion


class SkillResourceIdentity(DomainModel):
    type: Literal["custom", "anthropic"]
    skill_id: str = Field(min_length=1)


class RegisteredSkill(DomainModel):
    skill_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    sha256: str | None = Field(pattern=r"^[a-f0-9]{64}$")

    def reference(self) -> ManagedSkillVersion:
        return ManagedSkillVersion(type="custom", skill_id=self.skill_id, version=self.version)


class ManagedAgentBinding(DomainModel):
    skills: tuple[ManagedSkillName, ...] = Field(min_length=1)


class UnrestrictedNetwork(DomainModel):
    type: Literal["unrestricted"]


class LimitedNetwork(DomainModel):
    type: Literal["limited"]
    allowed_hosts: list[str]
    allow_mcp_servers: bool
    allow_package_managers: bool


type ManagedNetwork = UnrestrictedNetwork | LimitedNetwork


class ManagedEnvironmentBinding(DomainModel):
    networking: ManagedNetwork


class AgentModelSelection(DomainModel):
    id: str = Field(min_length=1)
    effort: ManagedAgentEffort


class PiHarness(DomainModel):
    harness: Literal["pi"]


class ManagedAgentMetadata(DomainModel):
    skill_mount: Literal["inline"]
    tank_skills: str
    retained_entries: dict[str, str] = Field(exclude=True, repr=False)

    @model_validator(mode="after")
    def unrelated_metadata_only(self) -> Self:
        if {"skill_mount", "tank_skills"}.intersection(self.retained_entries):
            raise ValueError("Retained metadata cannot replace managed agent fields")
        return self


class ManagedAgentDefinition(DomainModel):
    name: ManagedAgentName
    model: AgentModelSelection
    oma: PiHarness = Field(serialization_alias="_oma")
    system: str = Field(min_length=1)
    tools: list[ManagedTool]
    skills: list[AgentSkillReference]
    metadata: ManagedAgentMetadata


class ModelCardCost(DomainModel):
    input: int | float = Field(ge=0)
    output: int | float = Field(ge=0)
    cache_read: int | float = Field(ge=0, serialization_alias="cacheRead")
    cache_write: int | float = Field(ge=0, serialization_alias="cacheWrite")


class ModelCardCompatibility(DomainModel):
    supports_max_output_tokens: bool = Field(serialization_alias="supportsMaxOutputTokens")


class ModelCardPiConfiguration(DomainModel):
    api: Literal["openai-responses"]
    reasoning: bool
    cost: ModelCardCost
    compat: ModelCardCompatibility


class ModelCardDefinition(DomainModel):
    model_id: str = Field(min_length=1)
    model: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    base_url: str = Field(min_length=1)
    pi_config: ModelCardPiConfiguration
    is_default: bool


class OpenMACredentials(DomainModel):
    api_key: SecretStr = Field(min_length=1)
    gateway_key: SecretStr = Field(min_length=1)


class ModelCardIdentity(DomainModel):
    id: str = Field(min_length=1)
    model_id: str = Field(min_length=1)


class ToolSwitchObservation(DomainModel):
    name: str = Field(min_length=1)
    enabled: bool | None


class BuiltinToolObservation(DomainModel):
    kind: Literal["builtin"]
    type: Literal["agent_toolset_20260401"]
    default_enabled: bool | None
    configs: list[ToolSwitchObservation] | None


class CustomToolObservation(DomainModel):
    kind: Literal["custom"]
    name: str = Field(min_length=1)
    description: str | None
    input_schema: dict[str, JsonValue] | None


class McpToolObservation(DomainModel):
    kind: Literal["mcp"]
    type: Literal["mcp_toolset"]
    mcp_server_name: str = Field(min_length=1)
    default_enabled: bool | None
    configs: list[ToolSwitchObservation] | None


class UnsupportedToolObservation(DomainModel):
    kind: Literal["unsupported"]
    type: str = Field(min_length=1)


type ToolObservation = (
    BuiltinToolObservation | CustomToolObservation | McpToolObservation | UnsupportedToolObservation
)


class ManagedAgentObservation(DomainModel):
    id: str = Field(min_length=1)
    version: int = Field(ge=1)
    name: str | None
    model_id: str | None = Field(min_length=1)
    model_effort: str | None
    system: str | None
    tools: list[ToolObservation] | None
    skills: list[AgentSkillReference] | None
    registered_skills: dict[ManagedSkillName, RegisteredSkill]
    registrations_declared: bool
    retained_metadata: dict[str, str] = Field(repr=False)
    skill_mount: str | None
    harness: str | None


class EnvironmentNetworkObservation(DomainModel):
    type: str | None
    allowed_hosts: list[str] | None
    allow_mcp_servers: bool | None
    allow_package_managers: bool | None


class EnvironmentPackagesObservation(DomainModel):
    pip: list[str] | None
    apt: list[str] | None


class ManagedEnvironmentObservation(DomainModel):
    id: str = Field(min_length=1)
    networking: EnvironmentNetworkObservation | None
    packages: EnvironmentPackagesObservation | None


class PublishedSkill(DomainModel):
    skill_id: str = Field(min_length=1)
    version: str = Field(min_length=1)


class OpenMAContractViolation(RuntimeError):
    """A successful remote write did not confirm the declared immutable agent binding."""


class AgentVersionIdentity(DomainModel):
    id: str = Field(min_length=1)
    version: int = Field(ge=1)


class AgentUpdateReceipt(AgentVersionIdentity):
    model: AgentModelSelection
    definition: ManagedAgentObservation

    @model_validator(mode="after")
    def exact_observed_binding(self) -> Self:
        if (
            self.definition.id != self.id
            or self.definition.version != self.version
            or self.definition.model_id != self.model.id
            or self.definition.model_effort != self.model.effort
        ):
            raise ValueError("Agent update receipt must identify its exact observed version and binding")
        return self


class AgentSyncResult(DomainModel):
    status: Literal["CURRENT", "UPDATED"]
    version: int = Field(ge=1)


class OpenMASyncReport(DomainModel):
    skills: dict[ManagedSkillName, Literal["CURRENT", "NEW_VERSION", "CREATED"]]
    agents: dict[ManagedAgentName, AgentSyncResult]
    environments: dict[ManagedEnvironmentName, Literal["CURRENT", "DIFFERS"]]

    @model_validator(mode="after")
    def complete_managed_report(self) -> Self:
        if set(self.skills) != {"tank-event-analyst", "tank-data", "tank-scout-reviewer"}:
            raise ValueError("Sync report requires every managed skill")
        if set(self.agents) != {"tank-event-analyst", "tank-scout-reviewer"}:
            raise ValueError("Sync report requires every managed agent")
        if set(self.environments) != {"tank-analysis", "tank-replay"}:
            raise ValueError("Sync report requires every managed environment")
        return self


class OpenMASyncPlan(DomainModel):
    configuration: OpenMAConfiguration
    versions: ResolvedVersionIdentity
    credentials: OpenMACredentials
    skill_files: dict[ManagedSkillName, tuple[SkillFile, ...]]
    agent_tools: dict[ManagedAgentName, list[ManagedTool]]

    @model_validator(mode="after")
    def complete_managed_namespaces(self) -> Self:
        if set(self.skill_files) != {"tank-event-analyst", "tank-data", "tank-scout-reviewer"}:
            raise ValueError("Sync plan requires every managed skill")
        if set(self.agent_tools) != {"tank-event-analyst", "tank-scout-reviewer"}:
            raise ValueError("Sync plan requires every managed agent")
        for files in self.skill_files.values():
            paths = [file.relative_path for file in files]
            if "SKILL.md" not in paths:
                raise ValueError("Managed skill requires its captured SKILL.md")
            if len(paths) != len(set(paths)):
                raise ValueError("Managed skill upload cannot repeat a captured path")
        return self


class OpenMAOperations(Protocol):
    async def cards(self) -> list[ModelCardIdentity]: ...
    async def publish_card(self, card: ModelCardDefinition, existing_id: str | None) -> None: ...
    async def agent(self, identity: str) -> ManagedAgentObservation: ...
    async def publish_skill(
        self, files: tuple[SkillFile, ...], current: RegisteredSkill | None
    ) -> PublishedSkill: ...
    async def publish_agent(
        self, identity: str, version: int, wanted: ManagedAgentDefinition
    ) -> AgentUpdateReceipt: ...
    async def environment(self, identity: str) -> ManagedEnvironmentObservation: ...


def observe_openma_tool(value: JsonValue) -> ToolObservation:
    if not isinstance(value, dict):
        raise OpenMAContractViolation("OpenMA tool observation must be an object")
    kind = value["type"]
    if kind == "custom":
        return CustomToolObservation.model_validate(
            {
                "kind": "custom",
                "name": value["name"],
                "description": value.get("description"),
                "input_schema": value.get("input_schema"),
            }
        )
    if kind == "agent_toolset_20260401" or kind == "mcp_toolset":
        default = value.get("default_config")
        if default is not None and not isinstance(default, dict):
            raise OpenMAContractViolation("OpenMA default tool configuration must be an object")
        configs = value.get("configs")
        switches: list[ToolSwitchObservation] | None = None
        if configs is not None:
            if not isinstance(configs, list):
                raise OpenMAContractViolation("OpenMA tool configurations must be an array")
            switches = []
            for raw in configs:
                if not isinstance(raw, dict):
                    raise OpenMAContractViolation("OpenMA tool configuration must be an object")
                switches.append(
                    ToolSwitchObservation.model_validate({"name": raw["name"], "enabled": raw.get("enabled")})
                )
            switches.sort(key=lambda switch: switch.name)
        if kind == "mcp_toolset":
            return McpToolObservation.model_validate(
                {
                    "kind": "mcp",
                    "type": "mcp_toolset",
                    "mcp_server_name": value["mcp_server_name"],
                    "default_enabled": None if default is None else default.get("enabled"),
                    "configs": switches,
                }
            )
        return BuiltinToolObservation.model_validate(
            {
                "kind": "builtin",
                "type": "agent_toolset_20260401",
                "default_enabled": None if default is None else default.get("enabled"),
                "configs": switches,
            }
        )
    if not isinstance(kind, str) or not kind:
        raise OpenMAContractViolation("OpenMA tool observation requires its actual type")
    return UnsupportedToolObservation(kind="unsupported", type=kind)


def _indexed_openma_tools(tools: Sequence[ToolObservation]) -> dict[str, ToolObservation]:
    indexed: dict[str, ToolObservation] = {}
    for tool in tools:
        if isinstance(tool, CustomToolObservation):
            key = "custom:" + tool.name
        elif isinstance(tool, McpToolObservation):
            key = "mcp:" + tool.mcp_server_name
        else:
            key = "builtin:" + tool.type
        if key in indexed:
            raise OpenMAContractViolation("OpenMA tool observation repeats an identity")
        indexed[key] = tool
    return indexed


def openma_tools_match(observed: Sequence[ToolObservation], desired: Sequence[ManagedTool]) -> bool:
    expected: list[ToolObservation] = []
    for tool in desired:
        if isinstance(tool, ManagedCustomTool):
            expected.append(
                CustomToolObservation(
                    kind="custom",
                    name=tool.name,
                    description=tool.description,
                    input_schema=tool.input_schema,
                )
            )
        else:
            expected.append(
                BuiltinToolObservation(
                    kind="builtin",
                    type=tool.type,
                    default_enabled=tool.default_config.enabled,
                    configs=sorted(
                        (
                            ToolSwitchObservation(name=switch.name, enabled=switch.enabled)
                            for switch in tool.configs
                        ),
                        key=lambda switch: switch.name,
                    ),
                )
            )
    return _indexed_openma_tools(observed) == _indexed_openma_tools(expected)
