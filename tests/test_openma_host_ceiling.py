import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest
import yaml


@pytest.mark.integration
@pytest.mark.process
def test_openma_unchanged_image_reconciles_host_ceiling_and_historical_rollback(tmp_path):
    root = Path(__file__).resolve().parents[1]
    project = "tank-host-ceiling-" + uuid.uuid4().hex
    image = "postgres:16-bookworm"
    key = "OMA_SESSION_EXECUTION_CONCURRENCY"
    host = "TANK_ANALYSIS_CONCURRENCY"
    current, previous = "a" * 40, "b" * 40
    engine = shutil.which("docker")
    assert engine
    env = {**os.environ, "engine": engine, "project": project, "image": image}
    env.pop(host, None)
    target = yaml.safe_load((root / "compose.yaml").read_text())["services"]["openma"]["environment"][key]
    for release in (current, previous):
        settings = {"FIXTURE": "1"}
        if release == current:
            settings[key] = target
        service = {
            "image": image,
            "pull_policy": "never",
            "network_mode": "none",
            "init": True,
            "entrypoint": ["sleep", "300"],
            "environment": settings,
        }
        stack = tmp_path / "releases" / release / "stack"
        stack.mkdir(parents=True)
        (stack / "compose.yaml").write_text(
            yaml.safe_dump({"name": project, "services": {"openma": service}})
        )
    source = (root / "scripts/deploy/deploy.sh").read_text()
    functions = source[source.index("grafana_enabled() {") : source.index("\nroll() {")]
    shell = (
        functions
        + """
    docker() {
      case ${!#} in
        tank-openma-1) "$engine" "${@:1:$#-1}" "$project-openma-1" ;;
        ghcr.io/*) "$engine" "${@:1:$#-1}" "$image" ;;
        *) "$engine" "$@" ;;
      esac
    }
    start_backend "$1" openma
    """
    )

    def start(release, cap):
        (tmp_path / ".env").write_text(f"{host}={cap}\n" if cap is not None else "")
        subprocess.run(
            ["bash", "-eu", "-o", "pipefail", "-c", shell, "fixture", release],
            cwd=tmp_path,
            check=True,
            timeout=30,
            env=env,
        )
        row = json.loads(subprocess.check_output([engine, "inspect", f"{project}-openma-1"], timeout=5))[0]
        assert row["State"]["Running"] and row["HostConfig"]["NetworkMode"] == "none"
        values = dict(value.split("=", 1) for value in row["Config"]["Env"])
        assert values.get(key) == (None if release == previous else str(cap or 8))
        return row["Id"]

    try:
        original = start(previous, None)
        capped = start(current, 1)
        assert capped != original and start(current, 1) == capped
        uncapped = start(current, None)
        assert uncapped != capped and start(current, None) == uncapped
        recapped = start(current, 1)
        assert recapped != uncapped and start(previous, 1) != recapped
    finally:
        subprocess.run(
            [
                engine,
                "compose",
                "-p",
                project,
                "-f",
                tmp_path / "releases" / current / "stack/compose.yaml",
                "down",
                "-v",
            ],
            cwd=tmp_path,
            env=env,
            check=True,
            timeout=30,
        )
