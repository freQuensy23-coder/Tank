import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HOST = "grafana.192-0-2-1.sslip.io"
NEW = "a" * 40
OLD = "b" * 40


@pytest.mark.parametrize("override", [True, False])
def test_rollout_selects_one_ingress_and_recovers_historical_release(tmp_path, override):
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
    (shared / "site.py").write_text("import sys\nwith open('site', 'a') as f: print(*sys.argv[1:], file=f)\n")
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
python3() {
  if [[ $1 != grafana.py ]]; then command python3 "$@"; return $?; fi
  if [[ $2 == external-host ]]; then printf '%s\\n' "$FIXTURE_HOST"; fi
}
curl() {
  case "${!#}" in */login) printf '200' ;; */api/dashboards/uid/*) printf '401' ;; *) return 1 ;; esac
}
content() { return 0; }
healthy() { return 0; }
nsenter() { return 0; }
verify() { return 1; }
"""
    marker = "previous=$(cat deployed 2>/dev/null || true)"
    script = script.replace(marker, hooks + marker)
    (tmp_path / "deploy.sh").write_text(script)
    result = subprocess.run(
        ["bash", "deploy.sh", NEW],
        cwd=tmp_path,
        env={**os.environ, "LIMIT": "0", "FIXTURE_HOST": HOST if override else ""},
        capture_output=True,
        text=True,
        timeout=4,
    )
    assert result.returncode == 1 and f"rolled back to {OLD}" in result.stdout, result.stdout + result.stderr
    calls = (tmp_path / "calls").read_text().splitlines()
    for sha in (NEW, OLD):
        rows = [line for line in calls if line.startswith(sha)]
        assert any(" up " in row and row.endswith(" grafana") for row in rows)
        assert all(("/https/compose.yaml" in row) == (not override) for row in rows)
        assert all(("/nginx/compose.yaml" in row) == (override and sha == NEW) for row in rows)
        assert any(row.endswith(" grafana-edge") for row in rows) != override
    if override:
        assert (tmp_path / "site").read_text().splitlines() == [f"apply {HOST} 13000", "remove"]
    else:
        assert not (tmp_path / "site").exists()


@pytest.mark.integration
@pytest.mark.process
def test_shared_nginx_certificate_viewer_and_rollback():
    subprocess.run([sys.executable, ROOT / "tests/grafana/shared-nginx/edge-run.py"], check=True, timeout=280)


@pytest.mark.integration
@pytest.mark.parametrize("host", [None, HOST, "", "grafana.256-0-0-1.sslip.io", "example.invalid/path"])
def test_external_host_projection_uses_only_the_protected_deployment_setting(tmp_path, host):
    envfile = tmp_path / ".env"
    envfile.write_text("" if host is None else f"GRAFANA_EXTERNAL_HOST='{host}'\n")
    envfile.chmod(0o600)
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path)}
    result = subprocess.run(
        [sys.executable, ROOT / "scripts/deploy/grafana.py", "external-host"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=8,
    )
    assert (result.returncode == 0) == (host in (HOST, None)), result.stderr
    assert result.stdout.strip() == (host if host == HOST else "")
