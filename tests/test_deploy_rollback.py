import os
import subprocess
from pathlib import Path

import pytest
import yaml

CANDIDATE = "a" * 40
PREVIOUS = "b" * 40


@pytest.mark.parametrize(
    "mode,expected_status",
    [("success", 0), ("roll_failed", 1), ("term", 143), ("int", 130)],
)
def test_actual_deploy_orchestration_rolls_back_failures_and_signals(tmp_path, mode, expected_status):
    source = (Path(__file__).parents[1] / "scripts/deploy/deploy.sh").read_text()
    hook = """roll() {
  printf '%s\\n' "$1" >> roll-calls
  if [ "$1" = "$CANDIDATE" ]; then
    case "$MODE" in
      roll_failed) return 7 ;;
      term) kill -TERM $$ ;;
      int) kill -INT $$ ;;
    esac
  fi
  return 0
}
verify() { return 0; }
docker() { return 0; }
apify() { return 0; }
"""
    marker = "previous=$(cat deployed 2>/dev/null || true)"
    assert source.count(marker) == 1
    script = tmp_path / "deploy.sh"
    script.write_text(source.replace(marker, hook + marker))
    (tmp_path / "deployed").write_text(PREVIOUS + "\n")
    (tmp_path / "compose.yaml").write_text("fixture compose\n")
    result = subprocess.run(
        ["bash", str(script), CANDIDATE],
        capture_output=True,
        text=True,
        timeout=4,
        env={**os.environ, "MODE": mode, "CANDIDATE": CANDIDATE, "LIMIT": "30"},
    )
    assert result.returncode == expected_status, result.stdout + result.stderr
    calls = (tmp_path / "roll-calls").read_text().splitlines()
    expected_calls = [CANDIDATE] if mode == "success" else [CANDIDATE, PREVIOUS]
    assert calls == expected_calls
    expected_marker = CANDIDATE if mode == "success" else PREVIOUS
    assert (tmp_path / "deployed").read_text() == expected_marker + "\n"
    assert not (tmp_path / "deployed.next").exists()


def test_historical_target_uses_current_main_deployment_helper(tmp_path):
    root = Path(__file__).parents[1]
    workflow = yaml.safe_load((root / ".github/workflows/deploy.yml").read_text())
    steps = workflow["jobs"]["deploy"]["steps"]
    select_target = next(step["run"] for step in steps if step.get("id") == "target")
    repository = tmp_path / "repository"
    repository.mkdir()
    environment = {
        **os.environ,
        "GIT_AUTHOR_NAME": "Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.test",
        "GIT_COMMITTER_NAME": "Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.test",
    }

    def git(*arguments):
        result = subprocess.run(
            ["git", *arguments],
            cwd=repository,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=3,
        )
        return result.stdout.strip()

    git("init", "--initial-branch=main")
    helper = repository / "scripts/deploy/deploy.sh"
    helper.parent.mkdir(parents=True)
    helper.write_text('python3 "releases/$SHA/apify_deploy.py" prepare\n')
    (helper.parent / "apify_deploy.py").write_text("trusted helper\n")
    worker = repository / "apify/x-reader/main.py"
    worker.parent.mkdir(parents=True)
    worker.write_text("historical worker\n")
    config = repository / "config/sources.yaml"
    config.parent.mkdir()
    config.write_text("historical source config\n")
    git("add", ".")
    git("commit", "-m", "historical deployment helper")
    historical = git("rev-parse", "HEAD")
    current_helper = (root / "scripts/deploy/deploy.sh").read_text()
    helper.write_text(current_helper)
    git("add", ".")
    git("commit", "-m", "current Tank-only deployment helper")
    git("update-ref", "refs/remotes/origin/main", "HEAD")
    runner = tmp_path / "runner"
    runner.mkdir()
    output = tmp_path / "output"
    subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", select_target],
        cwd=repository,
        env={
            **environment,
            "INPUT_SHA": historical,
            "RUNNER_TEMP": str(runner),
            "GITHUB_OUTPUT": str(output),
        },
        check=True,
        capture_output=True,
        text=True,
        timeout=4,
    )
    assert git("rev-parse", "HEAD") == historical
    assert "apify_deploy.py" in helper.read_text()
    assert (runner / "tank-orchestrator/deploy.sh").read_text() == current_helper
    assert (runner / "tank-orchestrator/worker/main.py").read_text() == "historical worker\n"
    assert (runner / "tank-orchestrator/worker-source.yaml").read_text() == "historical source config\n"
    assert output.read_text() == f"sha={historical}\n"
