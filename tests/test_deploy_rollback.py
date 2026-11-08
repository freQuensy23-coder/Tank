import os
import re
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
    assert output.read_text() == f"sha={historical}\n"


BTC_SINCE = "2026-11-08T09:30:00Z"


@pytest.mark.parametrize(
    "hot_set,btc_since,lag,print_age,holes,stale_klines,failure",
    [
        ("psql-fails", "BASE", "10", "5", "", "", "hot token set unreadable: unreadable"),
        ("clickhouse-fails", "BASE", "10", "5", "", "", None),
        ("1", "BASE", "10", "99999", "binance:spot:BTCUSDT 09:33", "", None),
        (
            "1",
            "BASE",
            "10",
            "5",
            "",
            "kraken:spot:XBT/USD",
            "BTC klines not recorded by this capture in the last 120 s: kraken:spot:XBT/USD",
        ),
        ("0", "BASE", "9999", "99999", "", "", None),
        ("1", "BASE", "9999", "5", "", "", "no bar recorded for 9999s"),
        ("", "BASE", "10", "5", "", "", "hot token set unreadable: "),
        ("1", "", "10", "5", "", "", "BTC tier unreadable: "),
        ("1", BTC_SINCE, "10", "99999", "", "", "newest BTC print in mkt.trades 99999s old"),
        (
            "1",
            BTC_SINCE,
            "10",
            "5",
            "binance:spot:BTCUSDT 09:33",
            "",
            "incomplete BTC minutes: binance:spot:BTCUSDT 09:33",
        ),
        ("1", BTC_SINCE, "10", "5", "", "", None),
    ],
)
def test_verify_checks_btc_prints_only_while_btc_is_hot(
    tmp_path, hot_set, btc_since, lag, print_age, holes, stale_klines, failure
):
    """A base-tier token has REST klines but no prints or flow bars (02 §4.9): the post-deploy check
    always needs fresh BTC klines written after the capture handoff, needs flow bars only while a
    token is hot, and reads BTC prints and complete minutes only while BTC is hot, counting minutes
    from at least two minutes after BTC became hot."""
    source = (Path(__file__).parents[1] / "scripts/deploy/deploy.sh").read_text()
    hook = """docker() {
  case "$1" in
    ps) echo capture-1 ;;
    inspect) case "$*" in *Health*) echo healthy ;; *StartedAt*) echo 2026-11-08T09:00:00Z ;; *) echo running ;; esac ;;
    logs) ;;
    exec)
      sql=$(cat)
      printf '%s\\n' "$sql" >> queries
      if [ -n "$PSQL_FAIL" ] && [[ "$*" == *psql* ]]; then return 2; fi
      if [ -n "$CLICKHOUSE_FAIL" ] && [[ "$*" == *clickhouse-client* ]]; then return 210; fi
      case "$sql" in
        *"count(*) from mkt.hot_tokens"*) echo "$HOT_SET" ;;
        *"token_id = 'bitcoin:BTC'"*) echo "$BTC_SINCE" ;;
        *klines_1m*) echo "$STALE_KLINES" ;;
        *"LEFT ANTI JOIN"*) echo "$HOLES" ;;
        *mkt.trades*) echo "$PRINT_AGE" ;;
        *recorded_at*) echo "$LAG" ;;
      esac ;;
  esac
}
capture_handoff=2026-11-08T09:40:00Z
if verify 2026-11-08T09:38:15Z; then exit 0; else exit 1; fi
"""
    marker = "previous=$(cat deployed 2>/dev/null || true)"
    assert source.count(marker) == 1
    script = tmp_path / "deploy.sh"
    script.write_text(source.replace(marker, hook + marker))
    result = subprocess.run(
        ["bash", str(script), CANDIDATE],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=4,
        env={
            **os.environ,
            "BTC_SINCE": btc_since,
            "LAG": lag,
            "PRINT_AGE": print_age,
            "HOLES": holes,
            "STALE_KLINES": stale_klines,
            "PSQL_FAIL": "1" if hot_set == "psql-fails" else "",
            "CLICKHOUSE_FAIL": "1" if hot_set == "clickhouse-fails" else "",
            "HOT_SET": "0" if hot_set == "clickhouse-fails" else hot_set,
        },
    )
    failures = [line for line in result.stdout.splitlines() if line.startswith("FAIL ")]
    if hot_set == "psql-fails":
        assert failures == [f"FAIL {failure}", "FAIL BTC tier unreadable: unreadable"], result.stdout
        assert result.returncode == 1
        return
    if hot_set == "clickhouse-fails":
        assert failures == [
            "FAIL BTC klines not recorded by this capture in the last 120 s: kline query failed",
            "FAIL OI or funding older than 10 min: OI and funding query failed",
        ], result.stdout
        assert result.returncode == 1
        return
    assert failures == ([f"FAIL {failure}"] if failure else []), result.stdout + result.stderr
    assert result.returncode == (1 if failure else 0)
    queries = (tmp_path / "queries").read_text().split("\n")
    klines = [query for query in queries if "klines_1m" in query]
    assert len(klines) == 1 and "kraken:spot:XBT/USD" in klines[0] and "2026-11-08T09:40:00Z" in klines[0]
    assert any("flow_bars_1m FINAL WHERE minute_start" in query for query in queries) == (hot_set == "1")
    btc_hot = btc_since == BTC_SINCE
    assert any("mkt.trades" in query for query in queries) == btc_hot
    if btc_hot:
        [minutes] = [query for query in queries if "LEFT ANTI JOIN" in query]
        assert f"parseDateTime64BestEffort('{BTC_SINCE}', 6, 'UTC')) + INTERVAL 2 MINUTE" in minutes


@pytest.mark.parametrize("old_capture", [True, False])
def test_roll_records_the_capture_handoff_for_verification(tmp_path, old_capture):
    """verify() accepts only klines recorded after the old capture stopped, so roll() records that
    moment to the microsecond on every path, with or without a capture to replace."""
    source = (Path(__file__).parents[1] / "scripts/deploy/deploy.sh").read_text()
    hook = """docker() {
  case "$1" in
    compose) return 0 ;;
    ps)
      calls=$(( $(cat ps-calls 2>/dev/null || echo 0) + 1 )); echo "$calls" > ps-calls
      if [ -n "$OLD_CAPTURE" ]; then echo old-capture; fi
      if [ "$calls" -gt 1 ]; then echo new-capture; fi ;;
    inspect) case "$*" in *Labels*) echo same ;; *Pid*) echo 1 ;; *) echo healthy ;; esac ;;
    stop|rm) date -u +%FT%T.%6NZ >> stopped ;;
  esac
}
nsenter() { return 0; }
sleep() { :; }
roll "$CANDIDATE" > roll.log
printf '%s\n' "$capture_handoff" > handoff
exit 0
"""
    marker = "previous=$(cat deployed 2>/dev/null || true)"
    assert source.count(marker) == 1
    script = tmp_path / "deploy.sh"
    script.write_text(source.replace(marker, hook + marker))
    result = subprocess.run(
        ["bash", str(script), CANDIDATE],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=4,
        env={**os.environ, "CANDIDATE": CANDIDATE, "OLD_CAPTURE": "1" if old_capture else ""},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    handoff = (tmp_path / "handoff").read_text().strip()
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", handoff), handoff
    assert (tmp_path / "stopped").exists() == old_capture
    if old_capture:
        assert all(stop <= handoff for stop in (tmp_path / "stopped").read_text().split())
