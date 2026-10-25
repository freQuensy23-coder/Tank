import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Literal

import pytest
from pydantic import BaseModel, ConfigDict

ROOT = Path(__file__).resolve().parents[1]


class Receipt(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class SandboxRouteReceipt(Receipt):
    defaults: str
    isolated: str
    rejected: int


class ReplayDatabaseReceipt(Receipt):
    address: str
    url: str


class ReplayRouteReceipt(Receipt):
    called: bool
    resolved: ReplayDatabaseReceipt
    denied: int


def node_probe(mode: Literal["sandbox", "replay"]) -> str:
    node = shutil.which("node")
    if node is None:
        pytest.fail("Pinned Node 24.22.0 is required for deployment contracts")
    version = subprocess.run([node, "--version"], check=True, capture_output=True, text=True, timeout=3)
    assert version.stdout.strip() == "v24.22.0", "Deployment contracts require pinned Node 24.22.0"
    result = subprocess.run(
        [node, str(ROOT / "deploy/openma-route-probe.mts"), mode],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=3,
    )
    return result.stdout


def test_postgres_image_hba_confines_every_role_to_its_compose_address() -> None:
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
def test_sandbox_route_defaults_and_validated_isolated_override() -> None:
    data = SandboxRouteReceipt.model_validate_json(node_probe("sandbox"))
    assert data.defaults == "postgresql://tank_agent:synthetic%3Ap%40ss@postgres:5432/tank"
    assert data.isolated.endswith("@postgres:5432/tank_integration_extract_20261018")
    assert data.rejected == 10


@pytest.mark.integration
@pytest.mark.node
@pytest.mark.process
def test_replay_route_resolves_only_declared_private_service_for_l4_acl() -> None:
    data = ReplayRouteReceipt.model_validate_json(node_probe("replay"))
    assert data.called and data.denied == 4
    assert data.resolved.url == "postgresql://tank_replay_agent:synthetic@172.30.42.2:5432/tank_replay"


def capture_compile_step() -> str:
    lines = (ROOT / "capture/Dockerfile").read_text().splitlines()
    compile_line = next(index for index, line in enumerate(lines) if "go build -trimpath" in line)
    first = compile_line
    while not lines[first].startswith("RUN "):
        first -= 1
    last = compile_line
    while lines[last].endswith("\\"):
        last += 1
    instruction = "\n".join(lines[first : last + 1]).removeprefix("RUN ")
    return re.sub(r"--mount=\S+", "", instruction).replace("\\\n", " ")


@pytest.mark.parametrize("build", ["", "dev", "ab" * 20])
def test_capture_build_identity_guard_precedes_compiler(tmp_path: Path, build: str) -> None:
    binary = tmp_path / "go"
    captured = tmp_path / "compiler-arguments"
    binary.write_text('#!/bin/sh\nprintf \'%s\\n\' "$@" > "$COMPILER_CAPTURE"\n')
    binary.chmod(0o700)
    result = subprocess.run(
        ["/bin/sh", "-c", capture_compile_step()],
        cwd=tmp_path,
        env={
            "PATH": str(tmp_path) + ":" + os.defpath,
            "GIT_SHA": build,
            "TARGETOS": "linux",
            "TARGETARCH": "amd64",
            "COMPILER_CAPTURE": str(captured),
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=3,
    )
    if build == "":
        assert result.returncode != 0
        assert not captured.exists()
        assert "GIT_SHA must not be empty" in result.stderr
    else:
        assert result.returncode == 0, result.stderr
        assert "internal/config.Build=" + build in captured.read_text()
