"""OpenMA protocol reconciliation uses captured input and preserves normalized provider behavior."""

import asyncio
import json
import logging
import shutil
from pathlib import Path
from typing import Literal

import httpx
import pytest
import yaml
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank import openma_sync
from tank.contracts import DomainModel
from tank.contracts.json import decode_json, encode_json
from tank.contracts.openma import ManagedSkillName, OpenMAContractViolation, OpenMASyncReport
from tank.settings import Settings, resolve_versions

ROOT = Path(__file__).resolve().parents[1]


class SyncFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


CONFIG = SyncFixtureSettings(config_dir=ROOT / "config").openma()


def settings_for(config_dir: Path, tmp_path: Path) -> Settings:
    return resolve_versions(
        SyncFixtureSettings(
            config_dir=config_dir,
            data_dir=tmp_path / "data",
            openma_api_key="synthetic-oma-key",
            codex_gateway_key="synthetic-gateway-key",
        )
    )


def fixture_object(value: JsonValue) -> dict[str, JsonValue]:
    assert isinstance(value, dict)
    return value


def fixture_array(value: JsonValue) -> list[JsonValue]:
    assert isinstance(value, list)
    return value


def fixture_text(value: JsonValue) -> str:
    assert isinstance(value, str)
    return value


def fixture_integer(value: JsonValue) -> int:
    assert isinstance(value, int) and not isinstance(value, bool)
    return value


class ObservedRequest(DomainModel):
    method: str
    path: str
    query: str


class FakeOpenMA:
    """The observed provider wire adapter includes normalized tools and positional skill references."""

    def __init__(self, settings: Settings) -> None:
        self.requests: list[ObservedRequest] = []
        self.cards: list[dict[str, JsonValue]] = []
        self.skills: dict[str, dict[str, JsonValue]] = {}
        self.agents: dict[str, dict[str, JsonValue]] = {}
        self.versions: dict[str, dict[int, dict[str, JsonValue]]] = {}
        self.environments: dict[str, dict[str, JsonValue]] = {}
        for name, binding in openma_sync.AGENTS.items():
            references: list[JsonValue] = []
            for skill in binding.skills:
                identity = "skill_" + skill
                self.skills[identity] = {"id": identity, "display_title": None, "latest_version": "1"}
                references.append({"skill_id": identity, "type": "custom", "version": "1"})
            tools: list[JsonValue] = [
                self.normalized(tool) for tool in openma_sync.agent_tools(settings, name)
            ]
            identity = CONFIG.agent_id_for(name)
            self.agents[identity] = {
                "id": identity,
                "name": name,
                "version": 1,
                "model": {"id": CONFIG.model_id, "effort": {"type": "medium"}},
                "system": openma_sync.SYSTEM,
                "tools": tools,
                "skills": references,
                "metadata": {
                    "skill_mount": "inline",
                    "tank_skills": json.dumps(
                        {
                            skill: {"skill_id": "skill_" + skill, "version": "1", "sha256": None}
                            for skill in binding.skills
                        },
                        sort_keys=True,
                    ),
                },
            }
        for agent in self.agents.values():
            self.remember(agent)
        for environment_name, environment in openma_sync.ENVIRONMENTS.items():
            identity = CONFIG.environment_id_for(environment_name)
            self.environments[identity] = {
                "id": identity,
                "config": {
                    "type": "cloud",
                    "networking": fixture_object(decode_json(environment.networking.model_dump_json())),
                    "packages": {
                        "type": "packages",
                        "pip": list(openma_sync.PIP),
                        "apt": list(openma_sync.APT),
                        "npm": [],
                    },
                },
            }

    def remember(self, agent: dict[str, JsonValue]) -> None:
        identity = fixture_text(agent["id"])
        version = fixture_integer(agent["version"])
        self.versions.setdefault(identity, {})[version] = fixture_object(decode_json(encode_json(agent)))

    @staticmethod
    def normalized(tool: dict[str, JsonValue]) -> dict[str, JsonValue]:
        result = dict(tool)
        if result["type"] != "custom":
            result["configs"] = [
                {
                    **fixture_object(row),
                    "permission_policy": {"type": "always_allow"},
                    "type": fixture_object(row)["name"],
                }
                for row in fixture_array(result["configs"])
            ]
        return result

    def writes(self, prefix: str) -> list[ObservedRequest]:
        return [
            request
            for request in self.requests
            if request.method == "POST" and request.path.startswith(prefix)
        ]

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requests.append(
            ObservedRequest(method=request.method, path=path, query=request.url.query.decode())
        )
        assert request.headers["x-api-key"] == "synthetic-oma-key"
        assert "skills-2025-10-02" in request.headers["anthropic-beta"]
        if path == "/v1/oma/model_cards":
            if request.method == "POST":
                card = {"id": "card_1", **fixture_object(decode_json(request.content))}
                self.cards.append(card)
                return httpx.Response(200, json=card)
            return httpx.Response(200, json={"data": self.cards})
        if path.startswith("/v1/oma/model_cards/"):
            return httpx.Response(200, json=decode_json(request.content))
        if path.startswith("/v1/agents/"):
            agent = self.agents[path.rsplit("/", 1)[1]]
            if request.method == "POST":
                body = fixture_object(decode_json(request.content))
                assert body["version"] == agent["version"]
                assert all(isinstance(value, str) for value in fixture_object(body["metadata"]).values())
                agent.update({key: value for key, value in body.items() if key != "version"})
                agent["tools"] = [
                    self.normalized(fixture_object(tool)) for tool in fixture_array(body["tools"])
                ]
                agent["version"] = fixture_integer(agent["version"]) + 1
                self.remember(agent)
            requested_version = request.url.params.get("version")
            if request.method == "GET" and requested_version is not None:
                return httpx.Response(
                    200, json=self.versions[fixture_text(agent["id"])][int(requested_version)]
                )
            return httpx.Response(200, json=agent)
        if path.startswith("/v1/environments/"):
            return httpx.Response(200, json=self.environments[path.rsplit("/", 1)[1]])
        if path.endswith("/versions") and request.method == "POST":
            skill = self.skills[path.split("/")[3]]
            assert b'name="files[]"' in request.content and b"SKILL.md" in request.content
            skill["latest_version"] = str(int(fixture_text(skill["latest_version"])) + 1)
            return httpx.Response(200, json={"type": "skill_version", "version": skill["latest_version"]})
        if path == "/v1/skills" and request.method == "POST":
            identity = f"skill_new_{len(self.skills)}"
            self.skills[identity] = {"id": identity, "latest_version": "1"}
            return httpx.Response(200, json=self.skills[identity])
        raise AssertionError(f"unexpected {request.method} {path}")


async def run(settings: Settings, fake: FakeOpenMA) -> OpenMASyncReport:
    async with httpx.AsyncClient(
        base_url="http://oma", transport=httpx.MockTransport(fake.handler)
    ) as client:
        return await openma_sync.sync(settings, client)


async def test_first_start_versions_every_skill_and_agent_then_nothing_changes(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    first = await run(settings, fake)
    assert first.skills == dict.fromkeys(openma_sync.SKILLS, "NEW_VERSION")
    assert {result.status for result in first.agents.values()} == {"UPDATED"}
    assert {result.version for result in first.agents.values()} == {2}
    assert first.environments == {"tank-analysis": "CURRENT", "tank-replay": "CURRENT"}
    assert fake.cards[0]["api_key"] == "synthetic-gateway-key"
    analyst = fake.agents[CONFIG.agent_id]
    assert [fixture_object(skill)["skill_id"] for skill in fixture_array(analyst["skills"])] == [
        "skill_tank-event-analyst",
        "skill_tank-data",
    ]
    metadata = fixture_object(analyst["metadata"])
    registered = fixture_object(decode_json(fixture_text(metadata["tank_skills"])))
    assert fixture_object(registered["tank-data"])["sha256"] == openma_sync.skill_digest(
        settings, "tank-data"
    )
    names = {fixture_object(tool).get("name") for tool in fixture_array(analyst["tools"])}
    assert {"send_telegram_file", "set_watch_condition"} <= names
    fake.requests.clear()
    second = await run(settings, fake)
    assert set(second.skills.values()) == {"CURRENT"}
    assert {result.status for result in second.agents.values()} == {"CURRENT"}
    assert fake.writes("/v1/agents/") == [] and fake.writes("/v1/skills") == []


async def test_uncaptured_configuration_is_refused_before_remote_mutation(tmp_path: Path) -> None:
    settings = SyncFixtureSettings(
        config_dir=ROOT / "config",
        data_dir=tmp_path / "data",
        openma_api_key="synthetic-oma-key",
        codex_gateway_key="synthetic-gateway-key",
    )
    fake = FakeOpenMA(settings_for(ROOT / "config", tmp_path))
    with pytest.raises(ValueError, match="captured"):
        await run(settings, fake)
    assert fake.requests == []


def copied_config(tmp_path: Path) -> Path:
    config = tmp_path / "config"
    shutil.copytree(ROOT / "config", config)
    return config


async def test_a_changed_tool_definition_publishes_only_the_analyst_agent(tmp_path: Path) -> None:
    config = copied_config(tmp_path)
    settings = settings_for(config, tmp_path)
    fake = FakeOpenMA(settings)
    await run(settings, fake)
    tools_path = config / "skills/tank-event-analyst/tools.json"
    tools = fixture_array(decode_json(tools_path.read_bytes()))
    tool = fixture_object(tools[0])
    tool["description"] = fixture_text(tool["description"]) + " Changed."
    tools_path.write_text(json.dumps(tools))
    changed = settings_for(config, tmp_path)
    fake.requests.clear()
    report = await run(changed, fake)
    assert report.skills["tank-event-analyst"] == "NEW_VERSION"
    assert report.skills["tank-data"] == "CURRENT"
    assert report.agents["tank-event-analyst"].status == "UPDATED"
    assert report.agents["tank-scout-reviewer"].status == "CURRENT"
    assert len(fake.writes("/v1/agents/")) == 1


async def test_builtin_toolset_drift_in_openma_is_corrected(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    await run(settings, fake)
    analyst = fake.agents[CONFIG.agent_id]
    fixture_object(fixture_array(analyst["tools"])[0])["configs"] = [{"name": "browser", "enabled": False}]
    report = await run(settings, fake)
    assert report.agents["tank-event-analyst"].status == "UPDATED"
    toolset = fixture_object(fixture_array(analyst["tools"])[0])
    assert toolset["default_config"] == {"enabled": True}
    assert [
        (fixture_object(row)["name"], fixture_object(row)["enabled"])
        for row in fixture_array(toolset["configs"])
    ] == [("browser", True)]


async def test_a_skill_missing_from_its_agent_is_created(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    fake.agents[CONFIG.scout_agent_id]["skills"] = []
    fixture_object(fake.agents[CONFIG.scout_agent_id]["metadata"])["tank_skills"] = "{}"
    report = await run(settings, fake)
    assert report.skills["tank-scout-reviewer"] == "CREATED"
    first = fixture_object(fixture_array(fake.agents[CONFIG.scout_agent_id]["skills"])[0])
    assert fixture_text(first["skill_id"]).startswith("skill_new_")


async def test_a_differing_environment_is_reported_not_raised(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    deployed = fixture_object(fake.environments[CONFIG.environment_id]["config"])
    fixture_array(fixture_object(deployed["packages"])["pip"]).append("pyarrow")
    with caplog.at_level(logging.ERROR, logger="tank.openma_sync"):
        report = await run(settings, fake)
    assert report.environments["tank-analysis"] == "DIFFERS"
    assert any("tank-analysis" in record.getMessage() for record in caplog.records)


async def test_unreachable_openma_never_blocks_and_stops_with_the_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    monkeypatch.setattr(openma_sync, "RETRY_S", 0.01)
    calls: list[str] = []

    def down(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        raise httpx.ConnectError("refused", request=request)

    stop = asyncio.Event()
    task = asyncio.create_task(
        openma_sync.keep_in_sync(settings, stop, transport_factory=lambda: httpx.MockTransport(down))
    )
    while len(calls) < 3:
        await asyncio.sleep(0.01)
    stop.set()
    assert await asyncio.wait_for(task, 1) is None


def test_prompt_label_follows_the_analyst_skill_version() -> None:
    skill = (ROOT / "config/skills/tank-event-analyst/SKILL.md").read_text()
    version = next(
        line.split(":", 1)[1].strip() for line in skill.splitlines() if line.startswith("version:")
    )
    assert CONFIG.prompt_version == version


@pytest.mark.parametrize("name", openma_sync.SKILLS)
def test_skill_upload_holds_exactly_the_baked_files(tmp_path: Path, name: ManagedSkillName) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    paths = [file.relative_path for file in openma_sync.skill_files(settings, name)]
    expected = sorted(
        path.relative_to(ROOT / "config/skills" / name).as_posix()
        for path in (ROOT / "config/skills" / name).rglob("*")
        if path.is_file()
    )
    assert sorted(paths) == expected
    assert "SKILL.md" in paths


@pytest.mark.parametrize(
    "defect",
    [
        "missing_limit",
        "boolean_price",
        "missing_model_price",
        "unknown_field",
        "duplicate_agent",
        "duplicate_environment",
    ],
)
async def test_invalid_complete_configuration_fails_before_any_openma_request(
    tmp_path: Path,
    defect: Literal[
        "missing_limit",
        "boolean_price",
        "missing_model_price",
        "unknown_field",
        "duplicate_agent",
        "duplicate_environment",
    ],
) -> None:
    original = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(original)
    config_dir = copied_config(tmp_path)
    document = original.load_config("openma")
    if defect == "missing_limit":
        del document["max_running_sessions"]
    elif defect == "boolean_price":
        prices = fixture_object(document["model_prices"])
        fixture_object(prices[fixture_text(document["model_id"])])["input_per_million"] = True
    elif defect == "missing_model_price":
        document["model_prices"] = {}
    elif defect == "duplicate_agent":
        document["scout_agent_id"] = document["agent_id"]
    elif defect == "duplicate_environment":
        document["replay_environment_id"] = document["environment_id"]
    else:
        document["allow_unknown_tool_defaults"] = True
    (config_dir / "openma.yaml").write_text(yaml.safe_dump(document))
    with pytest.raises(ValueError):
        await run(settings_for(config_dir, tmp_path), fake)
    assert fake.requests == []


@pytest.mark.parametrize("credential", ["openma_api_key", "codex_gateway_key"])
async def test_missing_required_credentials_fail_before_remote_effects(
    tmp_path: Path, credential: Literal["openma_api_key", "codex_gateway_key"]
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    incomplete = settings.model_copy(update={credential: None})
    with pytest.raises(ValueError):
        await run(incomplete, fake)
    assert fake.requests == []


async def test_all_tool_manifests_are_validated_before_any_remote_write(tmp_path: Path) -> None:
    directory = copied_config(tmp_path)
    path = directory / "skills/tank-scout-reviewer/tools.json"
    path.write_text('{"not_a_tool_array":"synthetic"}')
    settings = settings_for(directory, tmp_path)
    fake = FakeOpenMA(settings_for(ROOT / "config", tmp_path))
    with pytest.raises(ValueError, match="JSON array"):
        await run(settings, fake)
    assert fake.requests == []


async def test_captured_files_and_tools_are_unchanged_by_later_disk_edits(tmp_path: Path) -> None:
    directory = copied_config(tmp_path)
    settings = settings_for(directory, tmp_path)
    fake = FakeOpenMA(settings)
    expected = openma_sync.skill_digest(settings, "tank-event-analyst")
    (directory / "skills/tank-event-analyst/tools.json").write_text("invalid later disk content")
    report = await run(settings, fake)
    assert report.agents["tank-event-analyst"].status == "UPDATED"
    assert openma_sync.skill_digest(settings, "tank-event-analyst") == expected
    metadata = fixture_object(fake.agents[CONFIG.agent_id]["metadata"])
    registered = fixture_object(decode_json(fixture_text(metadata["tank_skills"])))
    assert fixture_object(registered["tank-event-analyst"])["sha256"] == expected


async def test_provider_metadata_survives_owned_update_and_second_sync_is_current(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    for agent in fake.agents.values():
        metadata = fixture_object(agent["metadata"])
        metadata["provider_tag"] = "Opaque provider state"
        metadata["empty_provider_label"] = ""
        agent["provider_extension"] = {"unconsumed": [1, "extension"]}
    first = await run(settings, fake)
    assert {outcome.status for outcome in first.agents.values()} == {"UPDATED"}
    for agent in fake.agents.values():
        metadata = fixture_object(agent["metadata"])
        assert metadata["provider_tag"] == "Opaque provider state"
        assert metadata["empty_provider_label"] == ""
        assert agent["provider_extension"] == {"unconsumed": [1, "extension"]}
    fake.requests.clear()
    second = await run(settings, fake)
    assert {outcome.status for outcome in second.agents.values()} == {"CURRENT"}
    assert fake.writes("/v1/agents/") == []


async def test_invalid_provider_document_logs_only_sanitized_error_class(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    stop = asyncio.Event()
    private = "FAKE_UNCONFIGURED_OPENMA_PROVIDER_CREDENTIAL_71293"

    def malformed(request: httpx.Request) -> httpx.Response:
        response = fake.handler(request)
        if request.method == "GET" and request.url.path.startswith("/v1/agents/"):
            body = fixture_object(decode_json(response.content))
            body["version"] = private
            stop.set()
            return httpx.Response(200, json=body)
        return response

    with caplog.at_level(logging.WARNING, logger="tank.openma_sync"):
        result = await openma_sync.keep_in_sync(
            settings, stop, transport_factory=lambda: httpx.MockTransport(malformed)
        )
    assert result is None
    assert "ValidationError" in caplog.text
    assert private not in caplog.text
    assert "synthetic-oma-key" not in caplog.text and "synthetic-gateway-key" not in caplog.text
    assert fake.writes("/v1/agents/") == [] and fake.writes("/v1/skills") == []


@pytest.mark.parametrize("model_shape", ["id_only", "normalized", "extended"])
async def test_provider_model_shapes_and_response_extensions_remain_idempotent(
    tmp_path: Path, model_shape: Literal["id_only", "normalized", "extended"]
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    for agent in fake.agents.values():
        if model_shape == "id_only":
            agent["model"] = CONFIG.model_id
        elif model_shape == "extended":
            fixture_object(agent["model"])["provider_extension"] = {"unconsumed": True}

    def wrapped(request: httpx.Request) -> httpx.Response:
        response = fake.handler(request)
        if request.method == "GET" and request.url.path.startswith("/v1/agents/"):
            return httpx.Response(
                200, json={"data": decode_json(response.content), "provider_page_metadata": {"opaque": True}}
            )
        return response

    async with httpx.AsyncClient(base_url="http://oma", transport=httpx.MockTransport(wrapped)) as client:
        first = await openma_sync.sync(settings, client)
        assert {outcome.status for outcome in first.agents.values()} == {"UPDATED"}
        fake.requests.clear()
        second = await openma_sync.sync(settings, client)
    assert {outcome.status for outcome in second.agents.values()} == {"CURRENT"}
    assert fake.writes("/v1/agents/") == []


@pytest.mark.parametrize("resource", ["agent", "environment"])
async def test_response_identity_is_bound_to_the_requested_resource(
    tmp_path: Path, resource: Literal["agent", "environment"]
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)

    def wrong_identity(request: httpx.Request) -> httpx.Response:
        response = fake.handler(request)
        selected = "/v1/agents/" if resource == "agent" else "/v1/environments/"
        if request.method == "GET" and request.url.path.startswith(selected):
            body = fixture_object(decode_json(response.content))
            body["id"] = "different-resource"
            return httpx.Response(200, json=body)
        return response

    async with httpx.AsyncClient(
        base_url="http://oma", transport=httpx.MockTransport(wrong_identity)
    ) as client:
        with pytest.raises(ValueError, match="requested identity"):
            await openma_sync.sync(settings, client)
    if resource == "agent":
        assert fake.writes("/v1/skills") == [] and fake.writes("/v1/agents/") == []


async def test_missing_captured_skill_document_fails_before_remote_effects(tmp_path: Path) -> None:
    directory = copied_config(tmp_path)
    (directory / "skills/tank-scout-reviewer/SKILL.md").unlink()
    settings = settings_for(directory, tmp_path)
    fake = FakeOpenMA(settings_for(ROOT / "config", tmp_path))
    with pytest.raises(ValueError, match="captured SKILL.md"):
        await run(settings, fake)
    assert fake.requests == []


async def test_model_card_uses_declared_provider_endpoint_and_price_scalars(tmp_path: Path) -> None:
    directory = copied_config(tmp_path)
    captured = settings_for(directory, tmp_path)
    document = captured.load_config("openma")
    prices = fixture_object(document["model_prices"])
    rate = fixture_object(prices[fixture_text(document["model_id"])])
    rate["input_per_million"] = 1.25
    rate["output_per_million"] = 2
    rate["cache_read_per_million"] = 0.5
    rate["cache_creation_per_million"] = 3
    document["gateway_base_url"] = "http://synthetic-gateway:8790/v1"
    document["model_card_provider"] = "synthetic-provider"
    (directory / "openma.yaml").write_text(yaml.safe_dump(document))
    settings = settings_for(directory, tmp_path)
    fake = FakeOpenMA(settings)
    await run(settings, fake)
    card = fake.cards[0]
    assert card["provider"] == "synthetic-provider"
    assert card["base_url"] == "http://synthetic-gateway:8790/v1"
    costs = fixture_object(fixture_object(card["pi_config"])["cost"])
    assert costs == {"input": 1.25, "output": 2, "cacheRead": 0.5, "cacheWrite": 3}
    assert type(costs["input"]) is float and type(costs["output"]) is int


async def test_retry_clients_own_fresh_transport_instances_and_close_each_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    monkeypatch.setattr(openma_sync, "RETRY_S", 0.001)
    stop = asyncio.Event()

    class TrackedTransport(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.close_count = 0
            self.request_count = 0

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            self.request_count += 1
            raise httpx.ConnectError("Synthetic retry", request=request)

        async def aclose(self) -> None:
            self.close_count += 1

    instances: list[TrackedTransport] = []

    def factory() -> httpx.AsyncBaseTransport:
        instance = TrackedTransport()
        instances.append(instance)
        if len(instances) == 3:
            stop.set()
        return instance

    assert await openma_sync.keep_in_sync(settings, stop, transport_factory=factory) is None
    assert len(instances) == 3
    assert all(instance.request_count == instance.close_count == 1 for instance in instances)


async def test_declared_skill_mount_drift_is_corrected_without_reuploading_skills(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    await run(settings, fake)
    fixture_object(fake.agents[CONFIG.agent_id]["metadata"])["skill_mount"] = "different"
    fake.requests.clear()
    report = await run(settings, fake)
    assert report.agents["tank-event-analyst"].status == "UPDATED"
    assert report.agents["tank-scout-reviewer"].status == "CURRENT"
    assert set(report.skills.values()) == {"CURRENT"}
    assert fixture_object(fake.agents[CONFIG.agent_id]["metadata"])["skill_mount"] == "inline"
    assert fake.writes("/v1/skills") == []


@pytest.mark.parametrize("field", ["name", "system", "model", "tools", "skills"])
async def test_uninitialized_provider_agent_is_reconciled_without_fabricated_current_values(
    tmp_path: Path, field: Literal["name", "system", "model", "tools", "skills"]
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    fake.agents[CONFIG.agent_id][field] = None
    first = await run(settings, fake)
    assert first.agents["tank-event-analyst"].status == "UPDATED"
    fake.requests.clear()
    second = await run(settings, fake)
    assert {outcome.status for outcome in second.agents.values()} == {"CURRENT"}
    assert fake.writes("/v1/agents/") == []


@pytest.mark.parametrize("field", ["config", "networking", "packages", "pip"])
async def test_unreported_environment_configuration_is_reported_as_difference(
    tmp_path: Path, field: Literal["config", "networking", "packages", "pip"]
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    environment = fake.environments[CONFIG.environment_id]
    if field == "config":
        environment["config"] = None
    else:
        configuration = fixture_object(environment["config"])
        if field in {"networking", "packages"}:
            configuration[field] = None
        else:
            del fixture_object(configuration["packages"])["pip"]
    report = await run(settings, fake)
    assert report.environments["tank-analysis"] == "DIFFERS"
    assert report.environments["tank-replay"] == "CURRENT"
    assert fake.writes("/v1/environments/") == []


@pytest.mark.parametrize("field", ["description", "input_schema"])
async def test_unreported_custom_tool_fields_are_reconciled_from_required_manifest(
    tmp_path: Path, field: Literal["description", "input_schema"]
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    custom = fixture_object(fixture_array(fake.agents[CONFIG.agent_id]["tools"])[1])
    custom[field] = None
    first = await run(settings, fake)
    assert first.agents["tank-event-analyst"].status == "UPDATED"
    fake.requests.clear()
    second = await run(settings, fake)
    assert second.agents["tank-event-analyst"].status == "CURRENT"
    assert fake.writes("/v1/agents/") == []


async def test_empty_declared_tool_description_is_a_valid_provider_string(tmp_path: Path) -> None:
    directory = copied_config(tmp_path)
    path = directory / "skills/tank-event-analyst/tools.json"
    tools = fixture_array(decode_json(path.read_bytes()))
    fixture_object(tools[0])["description"] = ""
    path.write_text(json.dumps(tools))
    settings = settings_for(directory, tmp_path)
    fake = FakeOpenMA(settings)
    await run(settings, fake)
    second = await run(settings, fake)
    assert {outcome.status for outcome in second.agents.values()} == {"CURRENT"}


async def test_changed_effort_publishes_confirmed_version_and_unchanged_effort_is_current(
    tmp_path: Path,
) -> None:
    directory = copied_config(tmp_path)
    settings = settings_for(directory, tmp_path)
    fake = FakeOpenMA(settings)
    await run(settings, fake)
    document = settings.load_config("openma")
    document["reasoning_effort"] = "high"
    (directory / "openma.yaml").write_text(yaml.safe_dump(document))
    changed = settings_for(directory, tmp_path)
    fake.requests.clear()
    report = await run(changed, fake)
    assert {outcome.status for outcome in report.agents.values()} == {"UPDATED"}
    assert {outcome.version for outcome in report.agents.values()} == {3}
    assert all(fixture_object(agent["model"])["effort"] == "high" for agent in fake.agents.values())
    for name in report.agents:
        assert any(
            request.method == "GET"
            and request.path == "/v1/agents/" + CONFIG.agent_id_for(name)
            and request.query == "version=3"
            for request in fake.requests
        )
    assert fake.writes("/v1/skills") == []
    fake.requests.clear()
    unchanged = await run(changed, fake)
    assert {outcome.status for outcome in unchanged.agents.values()} == {"CURRENT"}
    assert fake.writes("/v1/agents/") == []


@pytest.mark.parametrize("failure", ["unreported", "different", "wrong_version"])
async def test_unconfirmed_readback_stops_without_repeated_publish(
    tmp_path: Path, failure: Literal["unreported", "different", "wrong_version"]
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)

    def unconfirmed(request: httpx.Request) -> httpx.Response:
        response = fake.handler(request)
        if request.method == "GET" and request.url.params.get("version") is not None:
            body = fixture_object(decode_json(response.content))
            if failure == "unreported":
                fixture_object(body["model"]).pop("effort")
            elif failure == "different":
                fixture_object(body["model"])["effort"] = {"type": "low"}
            else:
                body["version"] = fixture_integer(body["version"]) + 1
            return httpx.Response(200, json=body)
        return response

    with pytest.raises(OpenMAContractViolation):
        await asyncio.wait_for(
            openma_sync.keep_in_sync(
                settings, asyncio.Event(), transport_factory=lambda: httpx.MockTransport(unconfirmed)
            ),
            timeout=1,
        )
    assert len(fake.writes("/v1/agents/")) == 1


async def test_readback_is_bound_to_published_version_not_concurrently_changed_latest(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)

    def concurrent(request: httpx.Request) -> httpx.Response:
        response = fake.handler(request)
        if request.method == "POST" and request.url.path == "/v1/agents/" + CONFIG.agent_id:
            latest = fake.agents[CONFIG.agent_id]
            latest["version"] = fixture_integer(latest["version"]) + 1
            fixture_object(latest["model"])["effort"] = "low"
            fake.remember(latest)
        return response

    async with httpx.AsyncClient(base_url="http://oma", transport=httpx.MockTransport(concurrent)) as client:
        report = await openma_sync.sync(settings, client)
    assert report.agents["tank-event-analyst"].version == 2
    assert fake.agents[CONFIG.agent_id]["version"] == 3
    assert any(request.query == "version=2" for request in fake.requests)


@pytest.mark.parametrize("layout", ["reordered", "missing", "extra", "duplicate"])
async def test_metadata_free_skill_references_cannot_authorize_role_updates(
    tmp_path: Path, layout: Literal["reordered", "missing", "extra", "duplicate"]
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    analyst = fake.agents[CONFIG.agent_id]
    fixture_object(analyst["metadata"]).pop("tank_skills")
    references = fixture_array(analyst["skills"])
    if layout == "reordered":
        references.reverse()
    elif layout == "missing":
        references.pop()
    elif layout == "extra":
        references.append({"type": "custom", "skill_id": "unrelated-skill", "version": "7"})
    else:
        references.append(references[0])
    original = encode_json(analyst)
    before = encode_json({identity: record for identity, record in fake.skills.items()})
    with pytest.raises(OpenMAContractViolation):
        await run(settings, fake)
    assert fake.writes("/v1/skills") == [] and fake.writes("/v1/agents/") == []
    assert encode_json(analyst) == original
    assert encode_json({identity: record for identity, record in fake.skills.items()}) == before


async def test_explicit_skill_bindings_ignore_provider_order_and_preserve_unrelated_references(
    tmp_path: Path,
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    analyst = fake.agents[CONFIG.agent_id]
    references = fixture_array(analyst["skills"])
    references.reverse()
    unrelated: dict[str, JsonValue] = {"type": "custom", "skill_id": "unrelated-skill", "version": "7"}
    references.append(unrelated)
    fake.skills["unrelated-skill"] = {"id": "unrelated-skill", "latest_version": "7"}
    expected = {
        "skill_tank-event-analyst": b"name: tank-event-analyst",
        "skill_tank-data": b"name: tank-data",
        "skill_tank-scout-reviewer": b"name: tank-scout-reviewer",
    }

    def owned_upload(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/versions"):
            identity = request.url.path.split("/")[3]
            assert identity in expected and expected[identity] in request.content
        return fake.handler(request)

    async with httpx.AsyncClient(
        base_url="http://oma", transport=httpx.MockTransport(owned_upload)
    ) as client:
        first = await openma_sync.sync(settings, client)
        fake.requests.clear()
        second = await openma_sync.sync(settings, client)
    assert first.agents["tank-event-analyst"].status == "UPDATED"
    assert {outcome.status for outcome in second.agents.values()} == {"CURRENT"}
    assert unrelated in fixture_array(analyst["skills"])
    assert fake.skills["unrelated-skill"]["latest_version"] == "7"
    assert fake.writes("/v1/skills") == []


async def test_metadata_free_empty_skill_set_creates_new_resources_without_mutating_old_ones(
    tmp_path: Path,
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    original_ids = set(fake.skills)
    for agent in fake.agents.values():
        agent["skills"] = []
        fixture_object(agent["metadata"]).pop("tank_skills")
    report = await run(settings, fake)
    assert set(report.skills.values()) == {"CREATED"}
    assert all(fake.skills[identity]["latest_version"] == "1" for identity in original_ids)
    assert len(fake.writes("/v1/skills")) == 3
    assert not any(request.path.endswith("/versions") for request in fake.writes("/v1/skills"))


async def test_duplicate_declared_role_resources_are_refused_before_publication(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    metadata = fixture_object(fake.agents[CONFIG.agent_id]["metadata"])
    roles = fixture_object(decode_json(fixture_text(metadata["tank_skills"])))
    fixture_object(roles["tank-data"])["skill_id"] = "skill_tank-event-analyst"
    metadata["tank_skills"] = json.dumps(roles)
    with pytest.raises(OpenMAContractViolation, match="multiple managed roles"):
        await run(settings, fake)
    assert not any(request.method == "POST" for request in fake.requests)


async def test_unsupported_effort_is_rejected_before_any_remote_effect(tmp_path: Path) -> None:
    directory = copied_config(tmp_path)
    source = settings_for(directory, tmp_path)
    fake = FakeOpenMA(source)
    document = source.load_config("openma")
    document["reasoning_effort"] = "minimal"
    (directory / "openma.yaml").write_text(yaml.safe_dump(document))
    with pytest.raises(ValueError):
        await run(settings_for(directory, tmp_path), fake)
    assert fake.requests == []


@pytest.mark.parametrize("same_id_as_managed", [False, True])
async def test_unrelated_anthropic_reference_keeps_its_kind_identity_and_version(
    tmp_path: Path, same_id_as_managed: bool
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    analyst = fake.agents[CONFIG.agent_id]
    reference: dict[str, JsonValue] = {
        "type": "anthropic",
        "skill_id": "skill_tank-event-analyst" if same_id_as_managed else "vendor-skill",
        "version": "builtin-2026-10",
    }
    fixture_array(analyst["skills"]).append(reference)
    first = await run(settings, fake)
    assert first.agents["tank-event-analyst"].status == "UPDATED"
    assert reference in fixture_array(analyst["skills"])
    fake.requests.clear()
    second = await run(settings, fake)
    assert second.agents["tank-event-analyst"].status == "CURRENT"
    assert reference in fixture_array(analyst["skills"])
    assert fake.writes("/v1/agents/") == [] and fake.writes("/v1/skills") == []


@pytest.mark.parametrize("tag", [None, True, "unknown-provider"])
async def test_malformed_or_unknown_skill_type_is_not_coerced_into_custom(
    tmp_path: Path, tag: JsonValue
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    reference = fixture_object(fixture_array(fake.agents[CONFIG.agent_id]["skills"])[0])
    reference["type"] = tag
    with pytest.raises((ValueError, OpenMAContractViolation)):
        await run(settings, fake)
    assert not any(request.method == "POST" for request in fake.requests)


@pytest.mark.parametrize("field", ["tools", "skills", "system", "tank_skills", "skill_mount"])
async def test_full_owned_state_must_be_confirmed_on_the_published_version(
    tmp_path: Path, field: Literal["tools", "skills", "system", "tank_skills", "skill_mount"]
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)

    def missing_owned_fact(request: httpx.Request) -> httpx.Response:
        response = fake.handler(request)
        if request.method == "GET" and request.url.params.get("version") is not None:
            body = fixture_object(decode_json(response.content))
            if field in {"tank_skills", "skill_mount"}:
                fixture_object(body["metadata"]).pop(field)
            elif field in {"tools", "skills"}:
                fixture_array(body[field]).pop()
            else:
                body["system"] = "Contradictory observed system"
            return httpx.Response(200, json=body)
        return response

    with pytest.raises(OpenMAContractViolation):
        await asyncio.wait_for(
            openma_sync.keep_in_sync(
                settings,
                asyncio.Event(),
                transport_factory=lambda: httpx.MockTransport(missing_owned_fact),
            ),
            timeout=1,
        )
    assert len(fake.writes("/v1/agents/")) == 1


@pytest.mark.parametrize(
    "failure", ["missing_harness", "wrong_harness", "missing_metadata", "changed_metadata"]
)
async def test_harness_and_retained_metadata_require_observed_confirmation(
    tmp_path: Path,
    failure: Literal["missing_harness", "wrong_harness", "missing_metadata", "changed_metadata"],
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    fixture_object(fake.agents[CONFIG.agent_id]["metadata"])["provider_tag"] = "preserve exactly"

    def contradictory(request: httpx.Request) -> httpx.Response:
        response = fake.handler(request)
        if request.method == "GET" and request.url.params.get("version") is not None:
            body = fixture_object(decode_json(response.content))
            if failure == "missing_harness":
                fixture_object(body["_oma"]).pop("harness")
            elif failure == "wrong_harness":
                fixture_object(body["_oma"])["harness"] = "acp"
            elif failure == "missing_metadata":
                fixture_object(body["metadata"]).pop("provider_tag")
            else:
                fixture_object(body["metadata"])["provider_tag"] = "changed"
            return httpx.Response(200, json=body)
        return response

    with pytest.raises(OpenMAContractViolation):
        await asyncio.wait_for(
            openma_sync.keep_in_sync(
                settings, asyncio.Event(), transport_factory=lambda: httpx.MockTransport(contradictory)
            ),
            timeout=1,
        )
    assert len(fake.writes("/v1/agents/")) == 1


async def test_additional_provider_metadata_after_publication_is_preserved_as_an_extension(
    tmp_path: Path,
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    fixture_object(fake.agents[CONFIG.agent_id]["metadata"])["provider_tag"] = "preserved"

    def additional(request: httpx.Request) -> httpx.Response:
        response = fake.handler(request)
        if request.method == "GET" and request.url.params.get("version") is not None:
            body = fixture_object(decode_json(response.content))
            fixture_object(body["metadata"])["server_generated_tag"] = "new"
            return httpx.Response(200, json=body)
        return response

    async with httpx.AsyncClient(base_url="http://oma", transport=httpx.MockTransport(additional)) as client:
        result = await openma_sync.sync(settings, client)
    assert {outcome.status for outcome in result.agents.values()} == {"UPDATED"}
