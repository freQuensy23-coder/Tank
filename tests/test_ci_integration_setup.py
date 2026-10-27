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

from scripts.ci.managed_clickhouse import CIContext
from scripts.ci.prepare_integration_setup import SetupContext, SignalState, run
from scripts.ci.schema_template import read_template

pytestmark = [pytest.mark.integration, pytest.mark.process, pytest.mark.timeout(10)]

STAND = """import json, os, pathlib, sys, time
name=pathlib.Path(sys.argv[0]).name
root=pathlib.Path(os.environ['SETUP_STAND'])
with (root/'events').open('a') as stream:
    stream.write(name+':start\\n')
if name=='go' and sys.argv[1]=='env':
    print(root/sys.argv[2])
    raise SystemExit(0)
if name=='docker' and sys.argv[1]=='run':
    print('synthetic-owned-cid')
if name=='docker' and sys.argv[1]=='pull':
    raise SystemExit(int(os.environ['SETUP_STAND_CH_CODE']))
if name=='docker' and sys.argv[1]=='create':
    arguments=sys.argv[2:]
    labels=dict(arguments[index+1].split('=',1) for index,value in enumerate(arguments) if value=='--label')
    environment=[arguments[index+1] for index,value in enumerate(arguments) if value=='--env']
    state={
        'Id':'c'*64, 'Image':'sha256:'+'d'*64, 'Name':'/'+arguments[arguments.index('--name')+1],
        'Config':{'Image':arguments[-1], 'Labels':labels, 'Env':environment,
            'Healthcheck':{'Test':['CMD-SHELL',arguments[arguments.index('--health-cmd')+1]],
                'Interval':5000000000,'Timeout':3000000000,'Retries':30}},
        'HostConfig':{'PortBindings':{'8123/tcp':[{'HostIp':'','HostPort':'8123'}]}},
        'State':{'Running':True,'Health':{'Status':'healthy'}},
    }
    (root/'clickhouse-inspection.json').write_text(json.dumps(state))
    print('c'*64)
if name=='docker' and sys.argv[1]=='inspect':
    print((root/'clickhouse-inspection.json').read_text())
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
    tests = tmp_path / "tests"
    tests.mkdir(exist_ok=True)
    (tests / "test_collection_only.py").write_text(
        "import pytest\npytestmark=pytest.mark.integration\ndef test_never_execute():\n    raise AssertionError('collection executed a test')\n"
    )
    script_root = tmp_path / "scripts/ci"
    script_root.mkdir(parents=True)
    original = Path(__file__).resolve().parents[1] / "scripts/ci/collect_integration.py"
    (script_root / "collect_integration.py").write_bytes(original.read_bytes())
    (tmp_path / "pyproject.toml").write_text('[tool.pytest.ini_options]\nmarkers=["integration"]\n')
    (tmp_path / "conftest.py").write_text("""import os
from pathlib import Path
import pytest

def pytest_addoption(parser):
    parser.addoption('--postgres-schema-template')

def pytest_configure(config):
    value=config.getoption('postgres_schema_template')
    assert value==os.environ['TANK_CI_SCHEMA_TEMPLATE']
    Path('observed-schema-name').write_text(value)
    with Path('events').open('a') as stream:
        stream.write('collection:start\\n')

def pytest_sessionfinish(session, exitstatus):
    with Path('events').open('a') as stream:
        stream.write('collection:done\\n')
    code=int(os.environ['SETUP_STAND_COLLECTION_CODE'])
    if code:
        pytest.exit('synthetic collector refusal', returncode=code)
""")
    monkeypatch.setenv("SETUP_STAND_COLLECTION_CODE", "0")
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
    monkeypatch.setenv("SETUP_STAND_CH_CODE", "0")
    monkeypatch.setenv("GITHUB_REPOSITORY", "synthetic/tank")
    monkeypatch.setenv("GITHUB_WORKFLOW_REF", "synthetic/tank/.github/workflows/ci.yml@refs/pull/1/merge")
    monkeypatch.setenv("TANK_CI_SHARD", "1")
    monkeypatch.setenv("GITHUB_ENV", str(environment))
    monkeypatch.setenv("GITHUB_RUN_ID", "17")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    return SetupContext(
        root=tmp_path,
        run_id=17,
        attempt=2,
        github_env=environment,
        shard=1,
        repository="synthetic/tank",
        workflow_ref="synthetic/tank/.github/workflows/ci.yml@refs/pull/1/merge",
    )


def test_actual_children_all_join_even_when_one_fails(
    setup_context: SetupContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SETUP_STAND_NPM_CODE", "7")
    result = run(setup_context, SignalState())
    assert {row.name: row.code for row in result.results} == {
        "go": 0,
        "gateway": 7,
        "postgres": 0,
        "clickhouse": 0,
        "collection": 0,
    }
    assert len({row.pid for row in result.results}) == 5
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
    assert {row.name: row.code for row in result.results} == {
        "go": 0,
        "gateway": 0,
        "postgres": 0,
        "clickhouse": 0,
        "collection": 0,
    }


def test_actual_main_signal_reports_all_child_statuses(
    setup_context: SetupContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts.ci import prepare_integration_setup

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_WORKSPACE", str(setup_context.root))
    monkeypatch.setenv("SETUP_STAND_DELAY", "3")
    with (setup_context.root / "main.log").open("wb") as output:
        child = subprocess.Popen(
            [sys.executable, "-m", "scripts.ci.prepare_integration_setup"],
            cwd=Path(prepare_integration_setup.__file__).resolve().parents[2],
            stdout=output,
            stderr=output,
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


def test_clickhouse_failure_still_joins_other_fixed_children(
    setup_context: SetupContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SETUP_STAND_CH_CODE", "9")
    result = run(setup_context, SignalState())
    assert {row.name: row.code for row in result.results} == {
        "go": 0,
        "gateway": 0,
        "postgres": 0,
        "clickhouse": 1,
        "collection": 0,
    }
    assert not result.successful()
    assert "go:done" in (setup_context.root / "events").read_text()
    assert not (setup_context.root / ".test-results/clickhouse-container.json").exists()


def test_actual_collector_failure_is_joined_and_cannot_admit_execution(
    setup_context: SetupContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SETUP_STAND_COLLECTION_CODE", "8")
    result = run(setup_context, SignalState())
    assert {row.name for row in result.results} == {"go", "gateway", "postgres", "clickhouse", "collection"}
    assert next(row.code for row in result.results if row.name == "collection") == 8
    assert not result.successful()
    assert "go:done" in (setup_context.root / "events").read_text()
    assert "AssertionError" not in (setup_context.root / ".test-results/setup/collection.log").read_text()


def test_actual_collection_uses_the_owned_future_template_without_claiming_seal(
    setup_context: SetupContext,
) -> None:
    result = run(setup_context, SignalState())
    owner = CIContext.model_validate(setup_context.model_dump(exclude={"github_env"}))
    allocation = read_template(owner)
    assert allocation.phase == "ALLOCATED_NOT_PREPARED"
    assert allocation.name == (setup_context.root / "observed-schema-name").read_text()
    assert "TANK_CI_SCHEMA_TEMPLATE=" + allocation.name + "\n" in setup_context.github_env.read_text()
    collection = json.loads((setup_context.root / ".test-results/integration-collection.json").read_text())
    assert len(collection) == 1
    assert result.successful()
    with pytest.raises(FileExistsError):
        run(setup_context, SignalState())
