"""Actual subprocess joining, partial failure and bounded owned-child drain."""

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from scripts.ci.prepare_integration_setup import SetupContext, SignalState, run

STAND = """import os, pathlib, sys, time
name=pathlib.Path(sys.argv[0]).name
root=pathlib.Path(os.environ['SETUP_STAND'])
with (root/'events').open('a') as stream:
    stream.write(name+':start\\n')
if name=='go' and sys.argv[1]=='env':
    print(root/sys.argv[2])
    raise SystemExit(0)
if name=='docker' and sys.argv[1]=='run':
    print('synthetic-owned-cid')
if name in ('go','npm'):
    time.sleep(float(os.environ['SETUP_STAND_DELAY']))
with (root/'events').open('a') as stream:
    stream.write(name+':done\\n')
if name=='npm':
    raise SystemExit(int(os.environ['SETUP_STAND_NPM_CODE']))
"""


@pytest.fixture
def setup_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SetupContext:
    for relative in ("capture/go.mod", "capture/go.sum", "tests/node-gateway/package-lock.json"):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("synthetic frozen-input stand")
    environment = tmp_path / "github-env"
    environment.touch()
    binary = tmp_path / "bin"
    binary.mkdir()
    for name in ("go", "git", "docker", "npm"):
        executable = binary / name
        executable.write_text("#!" + sys.executable + "\n" + STAND)
        executable.chmod(0o700)
    monkeypatch.setenv("PATH", str(binary) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("SETUP_STAND", str(tmp_path))
    monkeypatch.setenv("SETUP_STAND_DELAY", "0.05")
    monkeypatch.setenv("SETUP_STAND_NPM_CODE", "0")
    monkeypatch.setenv("GITHUB_ENV", str(environment))
    monkeypatch.setenv("GITHUB_RUN_ID", "17")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    return SetupContext(root=tmp_path, run_id=17, attempt=2, github_env=environment)


def test_actual_children_all_join_even_when_one_fails(
    setup_context: SetupContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SETUP_STAND_NPM_CODE", "7")
    result = run(setup_context, SignalState())
    assert {row.name: row.code for row in result.results} == {"go": 0, "gateway": 7, "postgres": 0}
    assert len({row.pid for row in result.results}) == 3
    assert not result.successful()
    assert (setup_context.root / ".test-results/postgres-container-id").read_text() == "synthetic-owned-cid\n"
    assert "go:done" in (setup_context.root / "events").read_text()
    assert (setup_context.root / ".test-results/setup/gateway.log").is_file()


def test_signal_before_launch_starts_no_child(setup_context: SetupContext) -> None:
    result = run(setup_context, SignalState(received=signal.SIGINT))
    assert result.results == []
    assert not result.successful()
    assert not (setup_context.root / "events").exists()


def test_signal_drains_actual_registered_children(
    setup_context: SetupContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SETUP_STAND_DELAY", "3")
    state = SignalState()

    def interrupt() -> None:
        deadline = time.monotonic() + 2
        while not (setup_context.root / "events").exists():
            if time.monotonic() >= deadline:
                return
            time.sleep(0.01)
        state.received = signal.SIGTERM

    thread = threading.Thread(target=interrupt)
    thread.start()
    result = run(setup_context, state)
    thread.join()
    assert result.interrupted_signal == signal.SIGTERM
    assert not result.successful()
    assert any(row.code < 0 for row in result.results)
    for row in result.results:
        with pytest.raises(ProcessLookupError):
            os.kill(row.pid, 0)
    assert "go:done" not in (setup_context.root / "events").read_text()


def test_success_requires_every_actual_task(setup_context: SetupContext) -> None:
    result = run(setup_context, SignalState())
    assert result.successful()
    assert {row.name: row.code for row in result.results} == {"go": 0, "gateway": 0, "postgres": 0}


def test_actual_main_signal_reports_all_child_statuses(
    setup_context: SetupContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts.ci import prepare_integration_setup

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_WORKSPACE", str(setup_context.root))
    monkeypatch.setenv("SETUP_STAND_DELAY", "3")
    with (setup_context.root / "main.log").open("wb") as output:
        child = subprocess.Popen(
            [sys.executable, str(Path(prepare_integration_setup.__file__))], stdout=output, stderr=output
        )
        try:
            deadline = time.monotonic() + 2
            while not (setup_context.root / "events").exists():
                if child.poll() is not None or time.monotonic() >= deadline:
                    pytest.fail("actual setup main did not launch its owned stand")
                time.sleep(0.01)
            child.send_signal(signal.SIGTERM)
            assert child.wait(timeout=5) == 128 + signal.SIGTERM
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
    report = prepare_integration_setup.SetupReport.model_validate(
        json.loads((setup_context.root / ".test-results/setup-results.json").read_text())
    )
    assert report.interrupted_signal == signal.SIGTERM
    assert not report.successful()
    assert report.results
    assert any(row.code < 0 for row in report.results)
    assert "go:done" not in (setup_context.root / "events").read_text()
