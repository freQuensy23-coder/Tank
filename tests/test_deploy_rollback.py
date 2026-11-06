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


@pytest.mark.parametrize(
    "mode,expected_status,expected_calls,journal_retained",
    [
        ("prepare_term", 143, ["prepare", "rollback-intent", "rollback", "previous-compose", "clear"], False),
        ("commit_term", 143, ["prepare", "activate", "finish", "settled"], False),
        ("intent_failed", 1, ["prepare", "activate", "rollback-intent"], True),
        (
            "marker_failed",
            1,
            ["prepare", "activate", "rollback-intent", "rollback", "previous-compose"],
            True,
        ),
        (
            "clear_failed",
            1,
            ["prepare", "activate", "rollback-intent", "rollback", "previous-compose", "clear"],
            True,
        ),
    ],
)
def test_transaction_signal_and_recovery_boundaries(
    tmp_path, mode, expected_status, expected_calls, journal_retained
):
    source = (Path(__file__).parents[1] / "scripts/deploy/deploy.sh").read_text()
    hook = r"""roll() {
  printf '%s\n' "$1" >> rolls
  if [ "$1" = "$SHA" ] && [ "$MODE" != commit_term ]; then
    if [ "$MODE" = marker_failed ]; then mkdir deployed.next; fi
    return 1
  fi
}
verify() { return 0; }
docker() { return 0; }
apify() {
  printf '%s\n' "$1" >> apify-calls
  case "$1" in
    prepare)
      : > worker.pending
      if [ "$MODE" = prepare_term ]; then kill -TERM $$; fi
      ;;
    rollback-intent) [ "$MODE" != intent_failed ] || return 1 ;;
    finish)
      if [ "$MODE" = commit_term ]; then kill -TERM $$; fi
      ;;
    settled) rm worker.pending ;;
    clear)
      [ "$MODE" != clear_failed ] || return 1
      rm worker.pending
      ;;
  esac
  return 0
}
"""
    marker = "previous=$(cat deployed 2>/dev/null || true)"
    assert source.count(marker) == 1
    script = tmp_path / "deploy.sh"
    script.write_text(source.replace(marker, hook + marker))
    (tmp_path / "deployed").write_text(PREVIOUS + "\n")
    (tmp_path / "compose.yaml").write_text("prior compose\n")
    result = subprocess.run(
        ["bash", str(script), CANDIDATE],
        capture_output=True,
        text=True,
        timeout=4,
        env={**os.environ, "MODE": mode},
    )
    assert result.returncode == expected_status, result.stdout + result.stderr
    assert (tmp_path / "apify-calls").read_text().splitlines() == expected_calls
    assert (tmp_path / "worker.pending").exists() == journal_retained
    assert (tmp_path / "deployed").read_text().strip() == (CANDIDATE if mode == "commit_term" else PREVIOUS)
    if mode in {"intent_failed", "marker_failed", "clear_failed"}:
        assert "rolled back to" not in result.stdout


def test_signal_before_commit_guard_rolls_back(tmp_path):
    source = (Path(__file__).parents[1] / "scripts/deploy/deploy.sh").read_text()
    marker = "      finishing=yes"
    assert source.count(marker) == 1
    source = source.replace(marker, "      kill -TERM $$\n" + marker)
    hook = r"""roll() { printf '%s\n' "$1" >> rolls; }
verify() { return 0; }
apify() {
  case "$1" in
    prepare) : > worker.pending ;;
    clear) rm worker.pending ;;
  esac
}
"""
    point = "previous=$(cat deployed 2>/dev/null || true)"
    script = tmp_path / "deploy.sh"
    script.write_text(source.replace(point, hook + point))
    (tmp_path / "deployed").write_text(PREVIOUS + "\n")
    (tmp_path / "compose.yaml").write_text("prior compose\n")
    result = subprocess.run(["bash", str(script), CANDIDATE], capture_output=True, text=True, timeout=4)
    assert result.returncode == 143, result.stdout + result.stderr
    assert (tmp_path / "rolls").read_text().splitlines() == [CANDIDATE, PREVIOUS]
    assert (tmp_path / "deployed").read_text().strip() == PREVIOUS
    assert not (tmp_path / "worker.pending").exists()


@pytest.mark.parametrize("point", ["before_finish", "after_rollback", "before_clear", "after_clear"])
def test_interrupt_at_transaction_boundaries(tmp_path, point):
    source = (Path(__file__).parents[1] / "scripts/deploy/deploy.sh").read_text()
    hook = r"""roll() { printf '%s\n' "$1" >> rolls; }
verify() { return 0; }
docker() { return 0; }
apify() {
  printf '%s\n' "$1" >> calls
  case "$1" in
    settled) return 2 ;;
    previous-release) printf '%s\n' "$PREVIOUS" ;;
    clear) rm worker.pending ;;
  esac
}
"""
    anchor = "previous=$(cat deployed 2>/dev/null || true)"
    source = source.replace(anchor, hook + anchor)
    anchors = {
        "before_finish": "      apify finish",
        "after_rollback": "    restore_compose",
        "before_clear": "    apify clear",
        "after_clear": "    trap 'interrupt 130' INT",
    }
    anchor = anchors[point]
    assert source.count(anchor) == 1
    source = source.replace(anchor, "    kill -TERM $$\n" + anchor)
    script = tmp_path / "deploy.sh"
    script.write_text(source)
    (tmp_path / "deployed").write_text(PREVIOUS + "\n")
    (tmp_path / "compose.yaml").write_text("prior compose\n")
    (tmp_path / "worker.previous-compose.yaml").write_text("prior compose\n")
    (tmp_path / "worker.pending").write_text("pending\n")
    if point == "before_finish":
        (tmp_path / "worker.pending").unlink()
        hook_prepare = "    prepare) : > worker.pending ;;\n"
        source = source.replace("    settled) return 2 ;;", hook_prepare + "    settled) return 2 ;;")
        script.write_text(source)
    result = subprocess.run(
        ["bash", str(script), CANDIDATE],
        capture_output=True,
        text=True,
        timeout=4,
        env={**os.environ, "PREVIOUS": PREVIOUS},
    )
    assert result.returncode == 143, result.stdout + result.stderr
    calls = (tmp_path / "calls").read_text().splitlines()
    assert calls.count("rollback") == 1
    assert calls.count("clear") == 1
    assert calls.count("finish") == 0
    assert (tmp_path / "rolls").read_text().splitlines() == (
        [CANDIDATE, PREVIOUS] if point == "before_finish" else [PREVIOUS]
    )
    assert (tmp_path / "deployed").read_text() == PREVIOUS + "\n"
    assert not (tmp_path / "worker.pending").exists()


@pytest.mark.parametrize("signal,status", [("TERM", 143), ("INT", 130)])
@pytest.mark.parametrize("rollback_fails", [False, True])
def test_redelivery_signal_after_recovery_registration(tmp_path, signal, status, rollback_fails):
    source = (Path(__file__).parents[1] / "scripts/deploy/deploy.sh").read_text()
    hook = r"""roll() { printf '%s %s\n' "$1" "$(cat compose.yaml)" >> rolls; }
apify() {
  printf '%s\n' "$1" >> calls
  case "$1" in
    settled) return 2 ;;
    previous-release) printf '%s\n' "$PREVIOUS" ;;
    rollback) [ "$ROLLBACK_FAILS" = no ] ;;
    clear) rm worker.pending ;;
  esac
}
"""
    marker = "previous=$(cat deployed 2>/dev/null || true)"
    source = source.replace(marker, hook + marker)
    marker = "    worker_started=yes"
    assert source.count(marker) == 1
    instrumentation = r"""    armed=no
    trap 'if [ "$armed" = yes ]; then trap - DEBUG; kill -s "$TEST_SIGNAL" $$; elif [[ "$BASH_COMMAND" == worker_started=yes* ]]; then armed=yes; fi' DEBUG
"""
    source = source.replace(marker, instrumentation + marker)
    script = tmp_path / "deploy.sh"
    script.write_text(source)
    (tmp_path / "deployed").write_text(CANDIDATE + "\n")
    (tmp_path / "compose.yaml").write_text("candidate compose\n")
    (tmp_path / "worker.previous-compose.yaml").write_text("prior compose\n")
    (tmp_path / "worker.pending").write_text("pending\n")
    result = subprocess.run(
        ["bash", str(script), CANDIDATE],
        capture_output=True,
        text=True,
        timeout=4,
        env={
            **os.environ,
            "PREVIOUS": PREVIOUS,
            "TEST_SIGNAL": signal,
            "ROLLBACK_FAILS": "yes" if rollback_fails else "no",
        },
    )
    assert result.returncode == (1 if rollback_fails else status), result.stdout + result.stderr
    calls = (tmp_path / "calls").read_text().splitlines()
    assert calls == ["settled", "previous-release", "rollback-intent", "rollback", "previous-compose"] + (
        [] if rollback_fails else ["clear"]
    )
    assert (tmp_path / "rolls").read_text().splitlines() == [f"{PREVIOUS} prior compose"]
    assert (tmp_path / "worker.pending").exists() == rollback_fails
    assert (tmp_path / "deployed").read_text() == (CANDIDATE if rollback_fails else PREVIOUS) + "\n"
