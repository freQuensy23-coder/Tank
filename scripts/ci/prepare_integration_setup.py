"""Join fixed independent setup tasks without weakening any owned child failure."""

import argparse
import os
import signal
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import IO, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from scripts.ci.managed_clickhouse import CIContext
from scripts.ci.schema_template import allocate_template

GO_SETUP = 'export GOENV=off GOTOOLCHAIN=local\nfor variable in GOMODCACHE GOCACHE GOPATH; do\n  value="$(go env "$variable")"\n  export "$variable=$value"\n  echo "$variable=$value" >> "$GITHUB_ENV"\ndone\nprintf \'%s\\n\' GOENV=off GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOFLAGS=-mod=readonly >> "$GITHUB_ENV"\nGOPROXY=https://proxy.golang.org GOSUMDB=sum.golang.org go mod download\ngit diff --exit-code -- go.mod go.sum\nCGO_ENABLED=0 GOPROXY=off GOSUMDB=off go build -mod=readonly -o "$RUNNER_TEMP/capture-parity-prime" ./cmd/capture-parity\n'
POSTGRES_SETUP = "mkdir -p .test-results\ncontainer_id=\"$(docker run --detach --name tank-test-ci \\\n  --label \"tank.ci-owner=$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT\" \\\n  --publish 127.0.0.1:5432:5432 \\\n  --tmpfs /var/lib/postgresql/data:rw --shm-size 1g \\\n  --env POSTGRES_DB=tank_test_ci_admin --env POSTGRES_USER=postgres \\\n  --env POSTGRES_PASSWORD=tank-ci-disposable \\\n  postgres:16-bookworm \\\n  -c max_locks_per_transaction=1024 -c max_connections=300 \\\n  -c fsync=on -c synchronous_commit=on)\"\necho \"$container_id\" > .test-results/postgres-container-id\ndeadline=$((SECONDS + 60))\nuntil docker exec \"$container_id\" pg_isready -h 127.0.0.1 -U postgres -d tank_test_ci_admin >/dev/null 2>&1; do\n  if (( SECONDS >= deadline )); then\n    docker logs \"$container_id\"\n    exit 1\n  fi\n  sleep 1\ndone\ndocker exec \"$container_id\" psql -U postgres -d tank_test_ci_admin -At \\\n  -c \"SELECT json_build_object('version', version(), 'max_locks_per_transaction', current_setting('max_locks_per_transaction'), 'max_connections', current_setting('max_connections'), 'fsync', current_setting('fsync'), 'synchronous_commit', current_setting('synchronous_commit'))\" \\\n  > .test-results/postgres-settings.json\n"


class SetupContext(CIContext):
    model_config = ConfigDict(strict=True, extra="forbid")
    github_env: Path

    @model_validator(mode="after")
    def owned(self) -> Self:
        if not self.root.is_absolute() or not self.root.is_dir() or self.root.is_symlink():
            raise ValueError("setup requires its actual workspace")
        for relative in ("capture/go.mod", "capture/go.sum", "tests/node-gateway/package-lock.json"):
            if not (self.root / relative).is_file():
                raise ValueError("frozen setup inputs are missing")
        if not self.github_env.is_file() or self.github_env.is_symlink():
            raise ValueError("the actual Actions environment file is required")
        return self


type TaskName = Literal["go", "gateway", "postgres", "clickhouse", "collection"]


@dataclass
class SignalState:
    received: int | None = None


@dataclass
class Child:
    name: TaskName
    process: subprocess.Popen[bytes]
    log: IO[bytes]


class CommandResult(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    name: TaskName
    pid: int = Field(gt=0)
    code: int


class SetupReport(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    results: list[CommandResult]
    interrupted_signal: int | None

    def successful(self) -> bool:
        return (
            self.interrupted_signal is None
            and len(self.results) == 5
            and {r.name for r in self.results} == {"go", "gateway", "postgres", "clickhouse", "collection"}
            and all(r.code == 0 for r in self.results)
        )


def install_handlers(state: SignalState) -> None:
    def requested(number: int, frame: FrameType | None) -> None:
        state.received = number

    for number in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        signal.signal(number, requested)


def drain(children: list[Child]) -> None:
    for child in children:
        if child.process.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(child.process.pid, signal.SIGTERM)
    deadline = time.monotonic() + 5
    while any(child.process.poll() is None for child in children) and time.monotonic() < deadline:
        time.sleep(0.01)
    for child in children:
        if child.process.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(child.process.pid, signal.SIGKILL)
    for child in children:
        child.process.wait(timeout=5)


def run(context: SetupContext, state: SignalState) -> SetupReport:
    directory = context.root / ".test-results" / "setup"
    if directory.parent.is_symlink() or directory.is_symlink():
        raise ValueError("setup output cannot be symbolic")
    directory.mkdir(parents=True, exist_ok=True)
    if directory.stat().st_uid != os.geteuid():
        raise ValueError("setup output is not owned")
    owner = CIContext.model_validate(context.model_dump(exclude={"github_env"}))
    allocated = allocate_template(owner)
    with context.github_env.open("a") as stream:
        stream.write("TANK_CI_SCHEMA_TEMPLATE=" + allocated.name + "\n")
    environment = (
        dict(os.environ)
        | context.environment()
        | {
            "GITHUB_ENV": str(context.github_env),
            "TANK_CI_SCHEMA_TEMPLATE": allocated.name,
            "PYTHONPATH": os.pathsep.join(str(context.root / name) for name in ("scripts/ci", "tests")),
        }
    )
    specifications: tuple[tuple[TaskName, list[str], Path], ...] = (
        ("go", ["bash", "-euo", "pipefail", "-c", GO_SETUP], context.root / "capture"),
        (
            "gateway",
            ["npm", "ci", "--prefix", "tests/node-gateway", "--ignore-scripts", "--no-audit", "--no-fund"],
            context.root,
        ),
        ("postgres", ["bash", "-euo", "pipefail", "-c", POSTGRES_SETUP], context.root),
        (
            "clickhouse",
            [sys.executable, "-m", "scripts.ci.managed_clickhouse", "start"],
            Path(__file__).resolve().parents[2],
        ),
        (
            "collection",
            [
                sys.executable,
                str(Path(sys.executable).with_name("pytest")),
                "-p",
                "collect_integration",
                "-q",
                "--collect-only",
                "-m",
                "integration",
                "--postgres-schema-template",
                allocated.name,
            ],
            context.root,
        ),
    )
    children: list[Child] = []
    try:
        for name, command, cwd in specifications:
            if state.received is not None:
                break
            log = (directory / (name + ".log")).open("xb")
            try:
                process = subprocess.Popen(
                    command,
                    cwd=cwd,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    env=environment,
                )
            except BaseException:
                log.close()
                raise
            children.append(Child(name, process, log))
        while any(child.process.poll() is None for child in children):
            if state.received is not None:
                drain(children)
                break
            time.sleep(0.01)
        results = [
            CommandResult(name=child.name, pid=child.process.pid, code=child.process.wait())
            for child in children
        ]
        return SetupReport(results=results, interrupted_signal=state.received)
    except BaseException:
        drain(children)
        raise
    finally:
        for child in children:
            child.log.close()
        for child in children:
            body = (directory / (child.name + ".log")).read_bytes()
            os.write(1, ("setup task " + child.name + "\n").encode())
            os.write(1, body)


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    context = SetupContext.model_validate(
        CIContext.from_environment(os.environ).model_dump() | {"github_env": Path(os.environ["GITHUB_ENV"])}
    )
    state = SignalState()
    install_handlers(state)
    report = run(context, state)
    (context.root / ".test-results" / "setup-results.json").write_text(report.model_dump_json() + "\n")
    print(report.model_dump_json())
    if state.received is not None:
        raise SystemExit(128 + state.received)
    raise SystemExit(0 if report.successful() else 1)


if __name__ == "__main__":
    main()
