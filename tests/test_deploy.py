import importlib.util
import json
import subprocess
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "deploy" / f"{name}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_openma_preparation_is_offline_and_has_responses_model(tmp_path):
    result = module("prepare_openma").prepare(ROOT / "config", tmp_path)
    assert result["sessions_created"] == 0
    card = json.loads((tmp_path / "model-card.json").read_text())
    assert card["base_url"] == "http://tank:8790/v1"
    assert card["pi_config"]["api"] == "openai-responses"
    assert "api_key" not in card
    assert card["provider"] == "tank-codex"
    agent = json.loads((tmp_path / "tank-event-analyst.json").read_text())
    assert agent["_oma"]["harness"] == "pi"
    assert agent["metadata"] == {"skill_mount": "inline"}
    assert not any(t.get("type") == "mcp" for t in agent["tools"])
    replay = json.loads((tmp_path / "tank-replay.json").read_text())
    assert replay["config"]["networking"]["allowed_hosts"] == ["postgres"]


def test_registration_real_api_shapes_with_synthetic_credentials(tmp_path, monkeypatch):
    for name in ["oma_api_key", "codex_gateway_key"]:
        (tmp_path / name).write_text("synthetic-only-" + name)
    monkeypatch.setenv("TANK_SECRETS_DIR", str(tmp_path))
    calls = []

    def response(request):
        calls.append(request)
        if request.url.path == "/v1/oma/model_cards" and request.method == "GET":
            return httpx.Response(200, json={"data": []})
        if request.url.path == "/v1/oma/model_cards":
            body = json.loads(request.content)
            assert body["pi_config"]["api"] == "openai-responses"
            assert body["api_key"].startswith("synthetic-only-")
            return httpx.Response(201, json={"id": "model_card_test"})
        if request.url.path == "/v1/skills":
            return httpx.Response(201, json={"id": "skill_test", "latest_version": "test1"})
        if request.url.path == "/v1/agents":
            body = json.loads(request.content)
            if body["name"] == "tank-scout-reviewer":
                assert len(body["skills"]) == 1
                native = body["tools"][0]
                assert native["default_config"]["enabled"] is False
                assert all(config["enabled"] is False for config in native["configs"])
                assert {tool["name"] for tool in body["tools"] if tool["type"] == "custom"} == {
                    "web_search",
                    "search_tank_items",
                    "record_review",
                }
            return httpx.Response(201, json={"id": "agent_test"})
        if request.url.path == "/v1/environments":
            return httpx.Response(201, json={"id": "environment_test"})
        pytest.fail("Unexpected external request " + str(request.url))

    with httpx.Client(base_url="http://openma:8787", transport=httpx.MockTransport(response)) as client:
        result = module("prepare_openma").prepare(
            ROOT / "config", tmp_path / "out", apply=True, client=client
        )
    assert result["sessions_created"] == 0
    assert all("/sessions" not in r.url.path for r in calls)
    assert (tmp_path / "out" / "openma.proposed.yaml").exists()
    assert not any(
        "synthetic-only" in p.read_text(errors="ignore") for p in (tmp_path / "out").glob("*.json")
    )


def test_config_identity_manifest_matches_runtime_algorithm(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_text("tokens: []\n")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(tmp_path),
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "user.name=Test",
            "add",
            ".",
        ],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(tmp_path),
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "user.name=Test",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    manifest = module("prepare_config").prepare_config(config)
    from tank.settings import _capture_tree, _digest

    assert manifest["content_sha256"] == _digest(_capture_tree(config, {".yaml", ".yml", ".json", ".md"}))
    assert manifest["git_dirty"] is False
    (config / "registry.yaml").write_text("tokens: [changed]\n")
    assert module("prepare_config").prepare_config(config)["git_dirty"] is True


def test_overlay_refuses_changed_pinned_source(tmp_path, monkeypatch):
    prep = module("prepare_openma_source")

    class Result:
        stdout = prep.PIN

    monkeypatch.setattr(prep.subprocess, "run", lambda *a, **k: Result())
    source = tmp_path / "source"
    source.mkdir()
    hashes = json.loads((ROOT / "deploy" / "openma-source-hashes.json").read_text())
    for path in hashes:
        p = source / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("changed source")
    with pytest.raises(ValueError, match="hash mismatch"):
        prep.prepare(source, tmp_path / "dest")


def test_network_hba_confines_roles_and_rejects_broad_or_guessed_defaults(tmp_path):
    helper = module("prepare_network")
    text = helper.prepare("172.29.0.0/24", "10.21.0.0/24", tmp_path / "hba")
    assert "host tank,tank_replay tank 172.29.0.0/24 scram-sha-256" in text
    assert "host tank_replay tank_replay_agent 10.21.0.0/24 scram-sha-256" in text
    assert "host tank tank_agent 10.21.0.0/24 scram-sha-256" in text
    assert "172.29.0.19/32" in helper.prepare("172.29.0.0/24", "172.29.0.19/32", tmp_path / "nat-hba")
    assert "local all postgres peer" in text
    assert text.endswith("host all all ::/0 reject\n")
    assert "host all all 0.0.0.0/0 reject" in text
    for compose, sandbox in [
        ("0.0.0.0/0", "10.21.0.0/24"),
        ("172.29.0.1/24", "10.21.0.0/24"),
        ("8.8.8.0/24", "10.21.0.0/24"),
        ("172.29.0.0/30", "10.21.0.0/24"),
    ]:
        with pytest.raises(ValueError):
            helper.prepare(compose, sandbox, tmp_path / "bad")


def test_bootstrap_hba_denies_all_sandbox_roles_until_actual_source_is_observed(tmp_path):
    helper = module("prepare_network")
    text = helper.prepare("172.30.42.0/24", None, tmp_path / "bootstrap", bootstrap_only=True)
    assert "host tank,tank_replay tank " in text
    assert "host oma oma " in text
    assert "host tank tank_agent " not in text
    assert "host tank_replay tank_replay_agent " not in text
    assert "host all all 0.0.0.0/0 reject" in text
    with pytest.raises(ValueError):
        helper.prepare("172.30.42.0/24", "172.30.42.3/32", tmp_path / "bad", bootstrap_only=True)


def test_sandbox_route_defaults_and_validated_isolated_override():
    import shutil

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node executable required for actual deployment entrypoint contract")
    script = """
import {sandboxDatabaseRoute} from './deploy/openma_entrypoint.mjs';
const readSecret = name => {if(name!=='tank_agent_db_password')throw Error('wrong secret');return 'synthetic:p@ss';};
const defaults=sandboxDatabaseRoute({},readSecret);
const isolated=sandboxDatabaseRoute({TANK_SANDBOX_DATABASE_NAME:'tank_integration_extract_20261018',TANK_SANDBOX_DATABASE_ROLE:'tank_integration_extract_agent'},readSecret);
let rejected=0;
for(const value of ['', 'tank;DROP', '../tank', 'A', 'x'.repeat(64)])for(const key of ['TANK_SANDBOX_DATABASE_NAME','TANK_SANDBOX_DATABASE_ROLE']){try{sandboxDatabaseRoute({[key]:value},readSecret);}catch{rejected++;}}
console.log(JSON.stringify({defaults,isolated,rejected}));
"""
    result = subprocess.run(
        [node, "--input-type=module", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True
    )
    data = json.loads(result.stdout)
    assert data["defaults"] == "postgresql://tank_agent:synthetic%3Ap%40ss@postgres:5432/tank"
    assert data["isolated"].endswith("@postgres:5432/tank_integration_extract_20261018")
    assert data["rejected"] == 10


def test_replay_route_resolves_only_declared_private_service_for_l4_acl():
    import shutil

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node executable required")
    script = """import {replayDatabaseRoute} from './deploy/openma_entrypoint.mjs';
let called=false;
const resolved=await replayDatabaseRoute(()=> 'synthetic',async(host,opts)=>{if(host!=='postgres'||opts.family!==4)throw Error('wrong host');called=true;return {address:'172.30.42.2'};});
let denied=0;for(const address of ['8.8.8.8','0.0.0.0','127.0.0.1','::1'])try{await replayDatabaseRoute(()=> 'synthetic',async()=>({address}));}catch{denied++;}
console.log(JSON.stringify({called,resolved,denied}));"""
    result = subprocess.run(
        [node, "--input-type=module", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True
    )
    data = json.loads(result.stdout)
    assert data["called"] and data["denied"] == 4
    assert data["resolved"]["url"] == "postgresql://tank_replay_agent:synthetic@172.30.42.2:5432/tank_replay"
    prep = (ROOT / "deploy/prepare_openma_source.py").read_text()
    assert "allowed.push(env.TANK_SANDBOX_DATABASE_IPV4)" in prep
