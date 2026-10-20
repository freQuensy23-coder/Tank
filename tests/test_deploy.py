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
    assert oma["environment"]["LITEBOX_MEMORY_MIB"] == "${TANK_SANDBOX_MEMORY_MIB:-2048}"
    assert oma["environment"]["LITEBOX_CPUS"] == "${TANK_SANDBOX_CPUS:-2}"
    assert tank["environment"]["TANK_JEV_PROVIDER"] == "openrouter"
    assert tank["environment"]["TANK_OPENROUTER_API_KEY_FILE"] == "/run/secrets/openrouter_api_key"
    assert "openrouter_api_key" in tank["secrets"] and "openrouter_api_key" in oma["secrets"]
    assert "typesafe_api_key" not in c["secrets"]
    assert "x_session" not in tank["secrets"] and "x_session" not in c["secrets"]
    assert tank["environment"]["TANK_X_ENABLED"] == "${TANK_X_ENABLED:-false}"
    assert "TANK_X_LIST_ID" not in tank["environment"]
    x = yaml.safe_load((ROOT / "compose.x.yaml").read_text())
    assert x["services"]["tank"]["secrets"] == ["x_session"]
    assert x["services"]["tank"]["environment"]["TANK_X_ENABLED"] == "true"
    assert any(
        isinstance(v, dict) and v.get("target") == "/config" and v.get("read_only") for v in tank["volumes"]
    )
    assert "codex-auth:/codex-auth" in tank["volumes"]
    assert "sealed-replay-evidence:/data/replay/evidence" in tank["volumes"]
    assert "sealed-replay-evidence:/app/replay-evidence:ro" in oma["volumes"]
    assert oma["environment"]["TANK_REPLAY_EVIDENCE_ROOT"] == "/app/replay-evidence"
    assert all("tank-data" not in str(volume) for volume in oma["volumes"])
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


def test_colima_installer_uses_only_approved_api_credentials_and_preserves_internal_keys(tmp_path):
    helper = module("prepare_colima_secrets")
    source = tmp_path / "approved.env"
    source.write_text(
        "OPENROUTER_API_KEY=synthetic-or\nTelegram_BOT_TOKEN=synthetic-telegram\nETHERSCAN=synthetic-etherscan\n"
    )
    payload = helper.payload_from_env(source)
    assert payload["openrouter_api_key"] == {"value": "synthetic-or", "preserve": False}
    assert payload["telegram_bot_token"]["value"] == "synthetic-telegram"
    assert payload["etherscan_api_key"]["value"] == "synthetic-etherscan"
    assert all(payload[name]["preserve"] for name in helper.GENERATED)
    assert "x_session" not in payload and "typesafe_api_key" not in payload


def test_boxlite_bundle_rejects_static_shim_changed_firmware_and_bad_hash(tmp_path, monkeypatch):
    import hashlib
    import struct

    helper = module("prepare_boxlite_runtime")
    template = json.loads((ROOT / "deploy/boxlite/provenance-template.json").read_text())
    root = tmp_path / "bundle"
    root.mkdir()
    interpreter = b"/lib/ld-linux-aarch64.so.1\0"
    elf = bytearray(120 + len(interpreter))
    elf[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<H", elf, 18, 183)
    struct.pack_into("<Q", elf, 32, 64)
    struct.pack_into("<HH", elf, 54, 56, 1)
    struct.pack_into("<I", elf, 64, 3)
    struct.pack_into("<Q", elf, 72, 120)
    struct.pack_into("<Q", elf, 96, len(interpreter))
    elf[120:] = interpreter
    for name in template["files"]:
        (root / name).write_bytes(bytes(elf) if name == "boxlite-shim" else name.encode())
    template["files"] = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in template["files"]
    }
    template["artifact_sha256"] = template["files"]["boxlite-shim"]
    base = tmp_path / "recipe"
    (base / "boxlite").mkdir(parents=True)
    (base / "boxlite/provenance-template.json").write_text(json.dumps(template))
    monkeypatch.setattr(helper, "BASE", base)
    (root / "provenance.json").write_text(json.dumps(template))
    assert helper.verify_runtime(root)["artifact_sha256"] == template["artifact_sha256"]
    (root / ".env").write_text("synthetic")
    with pytest.raises(ValueError, match="Unexpected file"):
        helper.verify_runtime(root)
    (root / ".env").unlink()
    (root / "libkrunfw.so.5").write_bytes(b"changed")
    with pytest.raises(ValueError, match="hash mismatch"):
        helper.verify_runtime(root)
    forged = json.loads(json.dumps(template))
    forged["files"]["libkrunfw.so.5"] = hashlib.sha256(b"changed").hexdigest()
    (root / "provenance.json").write_text(json.dumps(forged))
    with pytest.raises(ValueError, match="Original pinned runtime modified"):
        helper.verify_runtime(root)
    (root / "libkrunfw.so.5").write_bytes(b"libkrunfw.so.5")
    struct.pack_into("<HH", elf, 54, 56, 0)
    (root / "boxlite-shim").write_bytes(elf)
    forged = json.loads(json.dumps(template))
    forged["files"]["boxlite-shim"] = hashlib.sha256(elf).hexdigest()
    forged["artifact_sha256"] = forged["files"]["boxlite-shim"]
    (root / "provenance.json").write_text(json.dumps(forged))
    with pytest.raises(ValueError, match="dynamic loader"):
        helper.verify_runtime(root)


def test_boxlite0106_rejects_relabelled_or_mixed_version_artifacts(tmp_path, monkeypatch):
    import hashlib
    import struct

    helper = module("prepare_boxlite_runtime")
    expected = json.loads((ROOT / "deploy/boxlite/v0.10.6/qualified-artifact.json").read_text())
    root, recipe = tmp_path / "bundle", tmp_path / "recipe"
    root.mkdir()
    interpreter = b"/lib/ld-linux-aarch64.so.1\0"
    elf = bytearray(120 + len(interpreter))
    elf[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<H", elf, 18, 183)
    struct.pack_into("<Q", elf, 32, 64)
    struct.pack_into("<HH", elf, 54, 56, 1)
    struct.pack_into("<I", elf, 64, 3)
    struct.pack_into("<Q", elf, 72, 120)
    struct.pack_into("<Q", elf, 96, len(interpreter))
    elf[120:] = interpreter
    for name in expected["files"]:
        (root / name).write_bytes(bytes(elf) if name == "boxlite-shim" else name.encode())
    expected["files"] = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in expected["files"]
    }
    expected["shim_sha256"] = expected["files"]["boxlite-shim"]
    target = recipe / "boxlite/v0.10.6/qualified-artifact.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(expected))
    monkeypatch.setattr(helper, "BASE", recipe)
    provenance = root / "provenance.json"
    provenance.write_text(json.dumps(expected))
    # Normal 0.10.6 deployment now uses the separate strict-DNS registration;
    # the historical baseline remains immutable for builder pin validation.
    registration = {
        "status": "QUALIFIED_NATIVE_REPLAY_NETWORK",
        "source": expected["source"],
        "files": expected["files"],
        "shim_sha256": expected["shim_sha256"],
        "provenance_sha256": hashlib.sha256(provenance.read_bytes()).hexdigest(),
        "qualification": {"status": "PASS", "managed_session_or_full_E2E_acceptance": False},
    }
    (target.parent / "replay-dns-qualified-artifact.json").write_text(json.dumps(registration))
    (root / "SHA256SUMS").write_text(
        "".join(f"{digest}  {name}\n" for name, digest in sorted(expected["files"].items()))
    )
    assert helper.verify_runtime(root)["boxlite_version"] == "0.10.6"

    relabelled = json.loads(json.dumps(expected))
    relabelled["source"]["commit"] = "0" * 40
    provenance.write_text(json.dumps(relabelled))
    with pytest.raises(ValueError, match="provenance hash mismatch"):
        helper.verify_runtime(root)
    mixed = json.loads(json.dumps(expected))
    (root / "libkrunfw.so.5").write_bytes(b"different runtime firmware")
    mixed["files"]["libkrunfw.so.5"] = hashlib.sha256(b"different runtime firmware").hexdigest()
    provenance.write_text(json.dumps(mixed))
    with pytest.raises(ValueError, match="Strict replay DNS-qualified"):
        helper.verify_runtime(root)
    relabelled["source"]["version"] = "0.10.7"
    provenance.write_text(json.dumps(relabelled))
    with pytest.raises(ValueError, match="Unsupported"):
        helper.verify_runtime(root)


def test_boxlite0106_sdk_lock_is_exact_and_has_no_old_sdk_resolution():
    import hashlib

    base = ROOT / "deploy/boxlite/v0.10.6"
    metadata = json.loads((base / "openma-sdk-patch.json").read_text())
    lock = base / metadata["lockfile_asset"]
    assert hashlib.sha256(lock.read_bytes()).hexdigest() == metadata["lockfile_sha256"]
    parsed = yaml.safe_load(lock.read_text())
    assert parsed["overrides"]["@boxlite-ai/boxlite"] == "0.10.6"
    sdk_packages = [name for name in parsed["packages"] if name.startswith("@boxlite-ai/boxlite")]
    assert sdk_packages and all(name.endswith("@0.10.6") for name in sdk_packages)


def test_raw_new_runtime_export_is_not_relabelled_or_marked_qualified(tmp_path):
    helper = module("prepare_boxlite_runtime")
    (tmp_path / "guest-resize2fs").write_bytes(b"unqualified fresh export")
    with pytest.raises(ValueError, match="versioned provenance and qualification"):
        helper.prepare(tmp_path)
    assert not (tmp_path / "provenance.json").exists()


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
