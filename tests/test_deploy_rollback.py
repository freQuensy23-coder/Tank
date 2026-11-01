import os
import subprocess
from pathlib import Path

import pytest

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
"""
    marker = "previous=$(cat deployed 2>/dev/null || true)"
    assert source.count(marker) == 1
    script = tmp_path / "deploy.sh"
    script.write_text(source.replace(marker, hook + marker))
    (tmp_path / "deployed").write_text(PREVIOUS + "\n")
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
