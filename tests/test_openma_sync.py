import asyncio
import json
import logging
import shutil
from pathlib import Path

import httpx
import pytest
import yaml

from tank import openma_sync
from tank.settings import Settings, resolve_versions

ROOT = Path(__file__).resolve().parents[1]
CONFIG = yaml.safe_load((ROOT / "config/openma.yaml").read_text())


def settings_for(config_dir, tmp_path: Path):
    return resolve_versions(
        Settings(
            config_dir=config_dir,
            data_dir=tmp_path / "data",
            openma_api_key="synthetic-oma-key",
            codex_gateway_key="synthetic-gateway-key",
            _env_file=None,
        )
    )


class FakeOpenMA:
    """The OpenMA API shapes observed on production: normalized tools, positional skills."""

    def __init__(self, settings) -> None:
        self.requests = []
        self.cards = []
        self.skills = {}
        self.agents = {}
        self.environments = {}
        for name, (key, skill_names) in openma_sync.AGENTS.items():
            skills = []
            for skill in skill_names:
                skill_id = "skill_" + skill
                self.skills[skill_id] = {"id": skill_id, "display_title": None, "latest_version": "1"}
                skills.append({"skill_id": skill_id, "type": "custom", "version": "1"})
            tools = [self.normalized(tool) for tool in openma_sync.agent_tools(settings, name)]
            self.agents[CONFIG[key]] = {
                "id": CONFIG[key],
                "name": name,
                "version": 1,
                "model": {"id": CONFIG["model_id"], "effort": {"type": "medium"}},
                "system": openma_sync.SYSTEM,
                "tools": tools,
                "skills": skills,
                "metadata": {"skill_mount": "inline"},
            }
        for key, network in openma_sync.ENVIRONMENTS.values():
            self.environments[CONFIG[key]] = {
                "id": CONFIG[key],
                "config": {
                    "type": "cloud",
                    "networking": dict(network),
                    "packages": {
                        "type": "packages",
                        "pip": list(openma_sync.PIP),
                        "apt": list(openma_sync.APT),
                        "npm": [],
                    },
                },
            }

    @staticmethod
    def normalized(tool):
        tool = dict(tool)
        if tool.get("type") != "custom":
            tool["configs"] = [
                {**row, "permission_policy": {"type": "always_allow"}, "type": row["name"]}
                for row in tool["configs"]
            ]
        return tool

    def writes(self, prefix):
        return [(m, p) for m, p in self.requests if m == "POST" and p.startswith(prefix)]

    def handler(self, request: pytest.FixtureRequest):
        path = request.url.path
        self.requests.append((request.method, path))
        assert request.headers["x-api-key"] == "synthetic-oma-key"
        assert "skills-2025-10-02" in request.headers["anthropic-beta"]
        if path == "/v1/oma/model_cards":
            if request.method == "POST":
                card = {"id": "card_1", **json.loads(request.content)}
                self.cards.append(card)
                return httpx.Response(200, json=card)
            return httpx.Response(200, json={"data": self.cards})
        if path.startswith("/v1/oma/model_cards/"):
            return httpx.Response(200, json=json.loads(request.content))
        if path.startswith("/v1/agents/"):
            agent = self.agents[path.rsplit("/", 1)[1]]
            if request.method == "POST":
                body = json.loads(request.content)
                assert body["version"] == agent["version"]
                assert all(isinstance(v, str) for v in body["metadata"].values())
                agent.update({k: v for k, v in body.items() if k != "version"})
                agent["tools"] = [self.normalized(tool) for tool in body["tools"]]
                agent["version"] += 1
            return httpx.Response(200, json=agent)
        if path.startswith("/v1/environments/"):
            return httpx.Response(200, json=self.environments[path.rsplit("/", 1)[1]])
        if path.endswith("/versions") and request.method == "POST":
            skill = self.skills[path.split("/")[3]]
            assert b'name="files[]"' in request.content and b"SKILL.md" in request.content
            skill["latest_version"] = str(int(skill["latest_version"]) + 1)
            return httpx.Response(200, json={"type": "skill_version", "version": skill["latest_version"]})
        if path == "/v1/skills" and request.method == "POST":
            skill_id = f"skill_new_{len(self.skills)}"
            self.skills[skill_id] = {"id": skill_id, "latest_version": "1"}
            return httpx.Response(200, json=self.skills[skill_id])
        raise AssertionError(f"unexpected {request.method} {path}")


async def run(settings, fake):
    async with httpx.AsyncClient(
        base_url="http://oma", transport=httpx.MockTransport(fake.handler)
    ) as client:
        return await openma_sync.sync(settings, client)


async def test_first_start_versions_every_skill_and_agent_then_nothing_changes(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    first = await run(settings, fake)
    assert first["skills"] == dict.fromkeys(openma_sync.SKILLS, "NEW_VERSION")
    assert set(first["agents"].values()) == {"VERSION 2"}
    assert first["environments"] == {"tank-analysis": "CURRENT", "tank-replay": "CURRENT"}
    assert fake.cards[0]["api_key"] == "synthetic-gateway-key"
    analyst = fake.agents[CONFIG["agent_id"]]
    assert [s["skill_id"] for s in analyst["skills"]] == ["skill_tank-event-analyst", "skill_tank-data"]
    assert json.loads(analyst["metadata"]["tank_skills"])["tank-data"]["sha256"] == openma_sync.skill_digest(
        settings, "tank-data"
    )
    names = {tool.get("name") for tool in analyst["tools"]}
    assert {"send_telegram_file", "set_watch_condition"} <= names
    fake.requests.clear()
    second = await run(settings, fake)
    assert set(second["skills"].values()) == {"CURRENT"} and set(second["agents"].values()) == {"CURRENT"}
    assert fake.writes("/v1/agents/") == [] and fake.writes("/v1/skills") == []


async def test_the_serve_settings_without_a_snapshot_upload_every_skill_file(tmp_path: Path) -> None:
    settings = Settings(
        config_dir=ROOT / "config",
        data_dir=tmp_path / "data",
        openma_api_key="synthetic-oma-key",
        codex_gateway_key="synthetic-gateway-key",
        _env_file=None,
    )
    fake = FakeOpenMA(settings_for(ROOT / "config", tmp_path))
    report = await run(settings, fake)
    assert report["skills"] == dict.fromkeys(openma_sync.SKILLS, "NEW_VERSION")


def copied_config(tmp_path: Path):
    config = tmp_path / "config"
    shutil.copytree(ROOT / "config", config)
    return config


async def test_a_changed_tool_definition_publishes_only_the_analyst_agent(tmp_path: Path) -> None:
    config = copied_config(tmp_path)
    settings = settings_for(config, tmp_path)
    fake = FakeOpenMA(settings)
    await run(settings, fake)
    tools_path = config / "skills/tank-event-analyst/tools.json"
    tools = json.loads(tools_path.read_text())
    tools[0]["description"] = tools[0]["description"] + " Changed."
    tools_path.write_text(json.dumps(tools))
    changed = settings_for(config, tmp_path)
    fake.requests.clear()
    report = await run(changed, fake)
    assert report["skills"]["tank-event-analyst"] == "NEW_VERSION"
    assert report["skills"]["tank-data"] == "CURRENT"
    assert report["agents"]["tank-event-analyst"].startswith("VERSION")
    assert report["agents"]["tank-scout-reviewer"] == "CURRENT"
    assert len(fake.writes("/v1/agents/")) == 1


async def test_builtin_toolset_drift_in_openma_is_corrected(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    await run(settings, fake)
    analyst = fake.agents[CONFIG["agent_id"]]
    analyst["tools"][0]["configs"] = [{"name": "browser", "enabled": False}]
    report = await run(settings, fake)
    assert report["agents"]["tank-event-analyst"].startswith("VERSION")
    assert analyst["tools"][0]["default_config"] == {"enabled": True}
    assert [(r["name"], r["enabled"]) for r in analyst["tools"][0]["configs"]] == [("browser", True)]


async def test_a_skill_missing_from_its_agent_is_created(tmp_path: Path) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    fake.agents[CONFIG["scout_agent_id"]]["skills"] = []
    report = await run(settings, fake)
    assert report["skills"]["tank-scout-reviewer"] == "CREATED"
    assert fake.agents[CONFIG["scout_agent_id"]]["skills"][0]["skill_id"].startswith("skill_new_")


async def test_a_differing_environment_is_reported_not_raised(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    fake = FakeOpenMA(settings)
    fake.environments[CONFIG["environment_id"]]["config"]["packages"]["pip"].append("pyarrow")
    with caplog.at_level(logging.ERROR, logger="tank.openma_sync"):
        report = await run(settings, fake)
    assert report["environments"]["tank-analysis"] == "DIFFERS"
    assert any("tank-analysis" in r.getMessage() for r in caplog.records)


async def test_unreachable_openma_never_blocks_and_stops_with_the_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    monkeypatch.setattr(openma_sync, "RETRY_S", 0.01)
    calls = []

    def down(request: pytest.FixtureRequest) -> None:
        calls.append(request.url.path)
        raise httpx.ConnectError("refused", request=request)

    stop = asyncio.Event()
    task = asyncio.create_task(openma_sync.keep_in_sync(settings, stop, transport=httpx.MockTransport(down)))
    while len(calls) < 3:
        await asyncio.sleep(0.01)
    stop.set()
    assert await asyncio.wait_for(task, 1) is None


def test_prompt_label_follows_the_analyst_skill_version() -> None:
    skill = (ROOT / "config/skills/tank-event-analyst/SKILL.md").read_text()
    version = next(
        line.split(":", 1)[1].strip() for line in skill.splitlines() if line.startswith("version:")
    )
    assert CONFIG["prompt_version"] == version


@pytest.mark.parametrize("name", openma_sync.SKILLS)
def test_skill_upload_holds_exactly_the_baked_files(tmp_path: Path, name) -> None:
    settings = settings_for(ROOT / "config", tmp_path)
    paths = [path for path, _ in openma_sync.skill_files(settings, name)]
    expected = sorted(
        p.relative_to(ROOT / "config/skills" / name).as_posix()
        for p in (ROOT / "config/skills" / name).rglob("*")
        if p.is_file()
    )
    assert sorted(paths) == expected
    assert "SKILL.md" in paths
