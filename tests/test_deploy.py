import importlib.util
import json
import subprocess
from pathlib import Path

import httpx
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "deploy" / f"{name}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_compose_topology_and_credential_boundaries():
    c = yaml.safe_load((ROOT / "compose.yaml").read_text())
    assert set(c["services"]) == {"tank", "openma", "postgres"}
    for service in c["services"].values():
        assert service["restart"] == "unless-stopped"
        assert not service.get("ports")
        assert service["logging"]["options"]["max-file"] == "5"
    tank = c["services"]["tank"]
    oma = c["services"]["openma"]
    assert tank["environment"]["TANK_JEV_PROVIDER"] == "openrouter"
    assert tank["environment"]["TANK_OPENROUTER_API_KEY_FILE"] == "/run/secrets/openrouter_api_key"
    assert "openrouter_api_key" in tank["secrets"] and "openrouter_api_key" in oma["secrets"]
    assert "typesafe_api_key" not in c["secrets"]
    assert "x_session" not in tank["secrets"] and "x_session" not in c["secrets"]
    assert tank["environment"]["TANK_X_ENABLED"] == "${TANK_X_ENABLED:-true}"
    assert tank["environment"]["TANK_X_LIST_ID"] == "${TANK_X_LIST_ID:-}"
    x = yaml.safe_load((ROOT / "compose.x.yaml").read_text())
    assert x["services"]["tank"]["secrets"] == ["x_session"]
    assert any(
        isinstance(v, dict) and v.get("target") == "/config" and v.get("read_only") for v in tank["volumes"]
    )
    assert "codex-auth:/codex-auth" in tank["volumes"]
    assert all("codex-auth" not in str(v) for v in oma["volumes"])
    assert "/dev/kvm:/dev/kvm" in oma["devices"]
    assert not {"telegram_bot_token", "x_session", "codex_gateway_key"} & set(oma["secrets"])
    assert {"pg-data", "tank-data", "tank-browser", "codex-auth", "backup"} <= c["volumes"].keys()
    assert all("${TANK_SECRETS_DIR" in v["file"] for v in c["secrets"].values())


def test_image_and_context_are_secret_free():
    ignore = (ROOT / ".dockerignore").read_text()
    for pattern in [
        ".env",
        "**/auth.json",
        "deploy/operator.env",
        "deploy/generated",
        "secrets",
        "browser",
        "data",
    ]:
        assert pattern in ignore
    image = (ROOT / "Dockerfile").read_text()
    assert (
        "python:3.12-" in image
        and "uv sync --frozen" in image
        and "playwright install --with-deps chromium" in image
    )
    assert "USER tank" in image
    for obsolete in [
        "com.tank.service.plist",
        "com.openma.server.plist",
        "service-wrapper.sh",
        "openma-wrapper.sh",
        "operator.env.example",
    ]:
        assert not (ROOT / "deploy" / obsolete).exists()


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


def test_local_oci_export_verifier_checks_referenced_blobs_and_safe_paths(tmp_path):
    import hashlib
    import io
    import tarfile

    helper = module("prepare_sandbox")
    payload = b'{"architecture":"arm64","os":"linux"}'
    digest = hashlib.sha256(payload).hexdigest()
    descriptor = {
        "mediaType": "application/vnd.oci.image.config.v1+json",
        "size": len(payload),
        "digest": "sha256:" + digest,
    }
    members = {
        "oci-layout": b'{"imageLayoutVersion":"1.0.0"}',
        "index.json": json.dumps({"schemaVersion": 2, "manifests": [descriptor]}).encode(),
        "blobs/sha256/" + digest: payload,
    }
    archive = tmp_path / "oci.tar"

    def bundle(content):
        with tarfile.open(archive, "w") as tar:
            for name, data in content.items():
                member = tarfile.TarInfo(name)
                member.size = len(data)
                tar.addfile(member, io.BytesIO(data))

    bundle(members)
    assert helper.prepare(archive, tmp_path / "valid")["verified_descriptors"] == 1
    members["blobs/sha256/" + digest] = b"corrupted"
    bundle(members)
    with pytest.raises(ValueError, match="digest/size"):
        helper.prepare(archive, tmp_path / "invalid")
    assert not (tmp_path / "invalid").exists()
    bundle({"../escape": b"unsafe"})
    with pytest.raises(ValueError, match="Unsafe"):
        helper.prepare(archive, tmp_path / "unsafe")
