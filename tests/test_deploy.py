import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SANDBOX_OPTIONS = (
    "?options=-c%20search_path%3Dreg%2Cingest%2Cevt%2Cmkt%2Cana%2Cout%2Cops%2Cscout%2Cqa%2Cpublic"
)


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "deploy" / f"{name}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_postgres_image_hba_confines_every_role_to_its_compose_address():
    rules = (ROOT / "deploy/postgres/pg_hba.conf").read_text().splitlines()
    assert rules[:2] == ["local all postgres peer", "local all all reject"]
    assert "host tank,tank_replay tank 172.30.60.0/24 scram-sha-256" in rules
    assert "host oma oma 172.30.60.0/24 scram-sha-256" in rules
    assert "host tank tank_agent 172.30.60.10/32 scram-sha-256" in rules
    assert "host tank_replay tank_replay_agent 172.30.60.10/32 scram-sha-256" in rules
    assert rules[-2:] == ["host all all 0.0.0.0/0 reject", "host all all ::/0 reject"]
    assert not any(rule.split()[-1] in ("trust", "password", "md5") for rule in rules)


@pytest.mark.integration
@pytest.mark.node
@pytest.mark.process
def test_sandbox_route_defaults_and_validated_isolated_override():
    import shutil

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node executable required for actual deployment entrypoint contract")
    script = """
import {sandboxDatabaseRoute} from './vendor/openma/tank-deploy/openma_entrypoint.mjs';
const readSecret = name => {if(name!=='tank_agent_db_password')throw Error('wrong secret');return 'synthetic:p@ss';};
const defaults=sandboxDatabaseRoute({},readSecret);
const isolated=sandboxDatabaseRoute({TANK_SANDBOX_DATABASE_NAME:'tank_integration_extract_20261018',TANK_SANDBOX_DATABASE_ROLE:'tank_integration_extract_agent'},readSecret);
let rejected=0;
for(const value of ['', 'tank;DROP', '../tank', 'A', 'x'.repeat(64)])for(const key of ['TANK_SANDBOX_DATABASE_NAME','TANK_SANDBOX_DATABASE_ROLE']){try{sandboxDatabaseRoute({[key]:value},readSecret);}catch{rejected++;}}
console.log(JSON.stringify({defaults,isolated,rejected}));
"""
    result = subprocess.run(
        [node, "--input-type=module", "-e", script],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=3,
    )
    data = json.loads(result.stdout)
    assert (
        data["defaults"] == "postgresql://tank_agent:synthetic%3Ap%40ss@postgres:5432/tank" + SANDBOX_OPTIONS
    )
    assert data["isolated"].endswith("@postgres:5432/tank_integration_extract_20261018" + SANDBOX_OPTIONS)
    assert data["rejected"] == 10


@pytest.mark.integration
@pytest.mark.node
@pytest.mark.process
def test_replay_route_resolves_only_declared_private_service_for_l4_acl():
    import shutil

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node executable required")
    script = """import {replayDatabaseRoute} from './vendor/openma/tank-deploy/openma_entrypoint.mjs';
let called=false;
const resolved=await replayDatabaseRoute(()=> 'synthetic',async(host,opts)=>{if(host!=='postgres'||opts.family!==4)throw Error('wrong host');called=true;return {address:'172.30.42.2'};});
let denied=0;for(const address of ['8.8.8.8','0.0.0.0','127.0.0.1','::1'])try{await replayDatabaseRoute(()=> 'synthetic',async()=>({address}));}catch{denied++;}
console.log(JSON.stringify({called,resolved,denied}));"""
    result = subprocess.run(
        [node, "--input-type=module", "-e", script],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=3,
    )
    data = json.loads(result.stdout)
    assert data["called"] and data["denied"] == 4
    assert data["resolved"]["url"] == (
        "postgresql://tank_replay_agent:synthetic@172.30.42.2:5432/tank_replay" + SANDBOX_OPTIONS
    )
