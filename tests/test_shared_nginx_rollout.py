import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HOST = "grafana.192-0-2-1.sslip.io"
NEW = "a" * 40
OLD = "b" * 40


@pytest.mark.parametrize(("override", "nginx"), [(True, True), (False, False), (False, True)])
def test_rollout_selects_one_ingress_and_recovers_historical_release(tmp_path, override, nginx):
    stack = tmp_path / "releases" / NEW / "stack"
    old = tmp_path / "releases" / OLD / "stack"
    for release in (stack, old):
        (release / "deploy/grafana/https").mkdir(parents=True)
        for path in (
            "compose.grafana.yaml",
            "deploy/grafana/https/Caddyfile",
            "deploy/grafana/https/compose.yaml",
        ):
            (release / path).touch()
        (release / "deploy/grafana/production-version").write_text("1\n")
    shared = stack / "deploy/grafana/nginx"
    shared.mkdir()
    (shared / "compose.yaml").touch()
    (tmp_path / "deployed").write_text(OLD + "\n")
    script = (ROOT / "scripts/deploy/deploy.sh").read_text()
    hooks = """docker() {
  case "$1" in
    compose)
      printf '%s %s\\n' "$TANK_VERSION" "$*" >> calls
      case "$*" in *' port grafana 3000') echo 127.0.0.1:13000 ;; esac ;;
    ps) : ;;
    exec) echo 1 ;;
    *) return 1 ;;
  esac
}
python3() { [[ $1 == grafana.py ]]; }
curl() {
  case "${!#}" in */login) printf '200' ;; */api/dashboards/uid/*) printf '401' ;; *) return 1 ;; esac
}
content() { return 0; }
healthy() { return 0; }
nsenter() { return 0; }
verify() { return 1; }
"""
    marker = "previous=$(cat deployed 2>/dev/null || true)"
    enabled = tmp_path / "sites-enabled"
    if nginx:
        enabled.mkdir()
    site = enabled / "zzzz-tank-grafana"
    if override:
        site.write_text(f"server_name {HOST};\n")
    script = script.replace(marker, hooks + marker).replace("/etc/nginx/sites-enabled", str(enabled))
    (tmp_path / "deploy.sh").write_text(script)
    result = subprocess.run(
        ["bash", "deploy.sh", NEW],
        cwd=tmp_path,
        env={**os.environ, "LIMIT": "0"},
        capture_output=True,
        text=True,
        timeout=4,
    )
    if nginx and not override:
        assert result.returncode == 1 and "Provision the Nexus" in result.stdout
        assert not (tmp_path / "calls").exists()
        return
    assert result.returncode == 1 and f"rolled back to {OLD}" in result.stdout, result.stdout + result.stderr
    calls = (tmp_path / "calls").read_text().splitlines()
    for sha in (NEW, OLD):
        rows = [line for line in calls if line.startswith(sha)]
        assert any(" up " in row and row.endswith(" grafana") for row in rows)
        assert all(("/https/compose.yaml" in row) == (not override) for row in rows)
        assert all(("/nginx/compose.yaml" in row) == (override and sha == NEW) for row in rows)
        assert any(row.endswith(" grafana-edge") for row in rows) != override


@pytest.mark.integration
@pytest.mark.process
def test_shared_nginx_certificate_viewer_and_rollback():
    subprocess.run([sys.executable, ROOT / "tests/grafana/shared-nginx/edge-run.py"], check=True, timeout=280)
