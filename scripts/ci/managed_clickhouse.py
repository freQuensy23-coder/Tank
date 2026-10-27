"""Start and drain only this CI launch's disposable ClickHouse container."""

import argparse
import os
import re
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from tank.contracts.json import decode_unique_json

IMAGE = "clickhouse/clickhouse-server:25.8"
HEALTH_COMMAND = "clickhouse-client --user=tank_ci --password=tank-ci-disposable --query='SELECT 1'"
HEALTH_INTERVAL_NS = 5_000_000_000
HEALTH_TIMEOUT_NS = 3_000_000_000
HEALTH_RETRIES = 30
INTENT = "clickhouse-intent.json"
RECEIPT = "clickhouse-container.json"

CID = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
NONCE = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]


class Record(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True, hide_input_in_errors=True)


class Projection(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", frozen=True, hide_input_in_errors=True)


class CIContext(Record):
    root: Path
    repository: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")]
    run_id: int = Field(gt=0)
    attempt: int = Field(gt=0)
    shard: int = Field(ge=0, le=2)
    workflow_ref: str

    @model_validator(mode="after")
    def actual_workspace(self) -> Self:
        if not self.root.is_absolute() or not self.root.is_dir() or self.root.is_symlink():
            raise ValueError("CI setup requires its actual workspace")
        prefix = self.repository + "/.github/workflows/ci.yml@"
        if not self.workflow_ref.startswith(prefix) or not self.workflow_ref.removeprefix(prefix):
            raise ValueError("CI setup requires the declared CI workflow")
        return self

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> Self:
        if environment["GITHUB_ACTIONS"] != "true":
            raise ValueError("CI setup requires its explicit Actions context")
        run, attempt, shard = (
            environment[key] for key in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "TANK_CI_SHARD")
        )
        if (
            re.fullmatch(r"[1-9][0-9]*", run) is None
            or re.fullmatch(r"[1-9][0-9]*", attempt) is None
            or shard not in {"0", "1", "2"}
        ):
            raise ValueError("CI setup requires actual canonical run/attempt/shard identities")
        return cls(
            root=Path(environment["GITHUB_WORKSPACE"]),
            repository=environment["GITHUB_REPOSITORY"],
            run_id=int(run),
            attempt=int(attempt),
            shard=int(shard),
            workflow_ref=environment["GITHUB_WORKFLOW_REF"],
        )

    def environment(self) -> dict[str, str]:
        return {
            "GITHUB_ACTIONS": "true",
            "GITHUB_WORKSPACE": str(self.root),
            "GITHUB_REPOSITORY": self.repository,
            "GITHUB_RUN_ID": str(self.run_id),
            "GITHUB_RUN_ATTEMPT": str(self.attempt),
            "TANK_CI_SHARD": str(self.shard),
            "GITHUB_WORKFLOW_REF": self.workflow_ref,
        }

    def directory(self) -> Path:
        path = self.root / ".test-results"
        if path.is_symlink():
            raise ValueError("CI setup state cannot be symbolic")
        path.mkdir(mode=0o700, exist_ok=True)
        actual = path.stat()
        if actual.st_uid != os.geteuid() or actual.st_mode & 0o022:
            raise ValueError("CI setup state requires its actual owner without foreign writes")
        return path


class Intent(Record):
    schema_version: Literal["tank-ci-clickhouse-intent-v1"]
    owner: CIContext
    nonce: NONCE

    @property
    def name(self) -> str:
        return f"tank-ci-ch-{self.owner.run_id}-{self.owner.attempt}-{self.owner.shard}-{self.nonce[:12]}"


class Receipt(Record):
    schema_version: Literal["tank-ci-clickhouse-receipt-v1"]
    intent: Intent
    container_id: CID


class Labels(Projection):
    owner: str = Field(alias="tank.ci-owner")
    repository: str = Field(alias="tank.ci-repository")
    run_id: str = Field(alias="tank.ci-run")
    attempt: str = Field(alias="tank.ci-attempt")
    shard: str = Field(alias="tank.ci-shard")
    nonce: NONCE = Field(alias="tank.ci-launch")
    service: Literal["clickhouse"] = Field(alias="tank.ci-service")


class ContainerConfig(Projection):
    image: str = Field(alias="Image")
    labels: Labels = Field(alias="Labels")


class OwnedContainer(Projection):
    identifier: CID = Field(alias="Id")
    image_id: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")] = Field(alias="Image")
    name: str = Field(alias="Name")
    config: ContainerConfig = Field(alias="Config")

    def require_owner(self, intent: Intent, expected_id: str | None) -> None:
        labels = self.config.labels
        owner = intent.owner
        if (
            self.name != "/" + intent.name
            or self.config.image != IMAGE
            or labels.owner != f"{owner.run_id}-{owner.attempt}"
            or labels.repository != owner.repository
            or labels.run_id != str(owner.run_id)
            or labels.attempt != str(owner.attempt)
            or labels.shard != str(owner.shard)
            or labels.nonce != intent.nonce
            or (expected_id is not None and self.identifier != expected_id)
        ):
            raise ValueError("Refusing a foreign CI ClickHouse container")


class RunningState(Projection):
    running: bool = Field(alias="Running")


class RunningContainer(Projection):
    state: RunningState = Field(alias="State")


class Healthcheck(Projection):
    test: list[str] = Field(alias="Test")
    interval: int = Field(alias="Interval")
    timeout: int = Field(alias="Timeout")
    retries: int = Field(alias="Retries")


class StartupConfig(Projection):
    environment: list[str] = Field(alias="Env")
    healthcheck: Healthcheck = Field(alias="Healthcheck")


class PortBinding(Projection):
    host_ip: str = Field(alias="HostIp")
    host_port: str = Field(alias="HostPort")


class HostConfig(Projection):
    port_bindings: dict[str, list[PortBinding]] = Field(alias="PortBindings")


class Health(Projection):
    status: Literal["starting", "healthy", "unhealthy"] = Field(alias="Status")


class HealthState(Projection):
    health: Health = Field(alias="Health")


class ContainerSettings(Projection):
    config: StartupConfig = Field(alias="Config")
    host: HostConfig = Field(alias="HostConfig")
    state: HealthState = Field(alias="State")

    @model_validator(mode="after")
    def original_contract(self) -> Self:
        check = self.config.healthcheck
        if (
            check.test != ["CMD-SHELL", HEALTH_COMMAND]
            or check.interval != HEALTH_INTERVAL_NS
            or check.timeout != HEALTH_TIMEOUT_NS
            or check.retries != HEALTH_RETRIES
        ):
            raise ValueError("ClickHouse health contract changed")
        if set(self.host.port_bindings) != {"8123/tcp"}:
            raise ValueError("ClickHouse published port contract changed")
        bindings = self.host.port_bindings["8123/tcp"]
        if (
            len(bindings) != 1
            or bindings[0].host_ip not in {"", "0.0.0.0"}
            or bindings[0].host_port != "8123"
        ):
            raise ValueError("ClickHouse published binding changed")
        expected = {
            "CLICKHOUSE_USER": "tank_ci",
            "CLICKHOUSE_PASSWORD": "tank-ci-disposable",
            "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
        }
        actual: dict[str, str] = {}
        for item in self.config.environment:
            key, separator, value = item.partition("=")
            if key in expected:
                if not separator or key in actual:
                    raise ValueError("ClickHouse environment is ambiguous")
                actual[key] = value
        if actual != expected:
            raise ValueError("ClickHouse synthetic authentication contract changed")
        return self


class CleanupReport(Record):
    kind: Literal["REMOVED", "ALREADY_ABSENT", "NOT_STARTED", "UNOBSERVED_ACCEPTANCE"]
    context: CIContext
    container_id: CID | None
    logs_exit: int | None
    removal_exit: int | None


def write_owned(path: Path, record: BaseModel) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(record.model_dump_json() + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def read_owned[T: BaseModel](path: Path, model: type[T]) -> T:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(descriptor, "rb") as stream:
        actual = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(actual.st_mode)
            or actual.st_uid != os.geteuid()
            or stat.S_IMODE(actual.st_mode) != 0o600
        ):
            raise ValueError("CI state requires its exact regular-file owner/mode")
        raw = stream.read()
    decode_unique_json(raw)
    return model.model_validate_json(raw)


def docker(arguments: list[str], *, timeout: float | None = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *arguments],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def inspection(identifier: str) -> bytes:
    response = docker(["inspect", "--format", "{{json .}}", identifier])
    if response.returncode:
        raise ValueError("Docker container identity could not be read")
    raw = response.stdout.encode()
    decode_unique_json(raw)
    return raw


def by_name(intent: Intent) -> OwnedContainer | None:
    response = docker(
        ["container", "ls", "--all", "--quiet", "--no-trunc", "--filter", "name=^/" + intent.name + "$"]
    )
    if response.returncode:
        raise ValueError("Docker container presence could not be read")
    identifiers = response.stdout.splitlines()
    if not identifiers:
        return None
    if len(identifiers) != 1 or re.fullmatch(r"[0-9a-f]{64}", identifiers[0]) is None:
        raise ValueError("Docker container identity is ambiguous")
    return OwnedContainer.model_validate_json(inspection(identifiers[0]))


def by_identifier(identifier: str) -> OwnedContainer | None:
    response = docker(["container", "ls", "--all", "--quiet", "--no-trunc", "--filter", "id=" + identifier])
    if response.returncode:
        raise ValueError("Docker accepted container presence could not be read")
    identifiers = response.stdout.splitlines()
    if not identifiers:
        return None
    if identifiers != [identifier]:
        raise ValueError("Docker accepted container identity changed")
    return OwnedContainer.model_validate_json(inspection(identifier))


def start(context: CIContext) -> Receipt:
    directory = context.directory()
    intent = Intent(schema_version="tank-ci-clickhouse-intent-v1", owner=context, nonce=uuid.uuid4().hex)
    write_owned(directory / INTENT, intent)
    owner = f"{context.run_id}-{context.attempt}"
    pulled = docker(["pull", IMAGE], timeout=None)
    if pulled.returncode:
        raise ValueError("The pinned ClickHouse image could not be prepared")
    arguments = [
        "create",
        "--name",
        intent.name,
        "--label",
        "tank.ci-owner=" + owner,
        "--label",
        "tank.ci-repository=" + context.repository,
        "--label",
        "tank.ci-run=" + str(context.run_id),
        "--label",
        "tank.ci-attempt=" + str(context.attempt),
        "--label",
        "tank.ci-shard=" + str(context.shard),
        "--label",
        "tank.ci-launch=" + intent.nonce,
        "--label",
        "tank.ci-service=clickhouse",
        "--publish",
        "8123:8123",
        "--env",
        "CLICKHOUSE_USER=tank_ci",
        "--env",
        "CLICKHOUSE_PASSWORD=tank-ci-disposable",
        "--env",
        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
        "--health-cmd",
        HEALTH_COMMAND,
        "--health-interval",
        "5s",
        "--health-timeout",
        "3s",
        "--health-retries",
        "30",
        IMAGE,
    ]
    response = docker(arguments)
    if response.returncode:
        raise ValueError("ClickHouse create did not return an accepted container receipt")
    receipt = Receipt(
        schema_version="tank-ci-clickhouse-receipt-v1", intent=intent, container_id=response.stdout.strip()
    )
    write_owned(directory / RECEIPT, receipt)
    owned = OwnedContainer.model_validate_json(inspection(receipt.container_id))
    owned.require_owner(intent, receipt.container_id)
    started = docker(["start", receipt.container_id])
    if started.returncode:
        raise ValueError("The accepted owned ClickHouse container could not start")
    while True:
        raw = inspection(receipt.container_id)
        owned = OwnedContainer.model_validate_json(raw)
        owned.require_owner(intent, receipt.container_id)
        if not RunningContainer.model_validate_json(raw).state.running:
            raise ValueError("Owned ClickHouse exited before becoming healthy")
        settings = ContainerSettings.model_validate_json(raw)
        if settings.state.health.status == "healthy":
            print(receipt.model_dump_json(), flush=True)
            return receipt
        if settings.state.health.status == "unhealthy":
            raise ValueError("Owned ClickHouse health checks did not succeed")
        time.sleep(1)


def cleanup(context: CIContext) -> CleanupReport:
    directory = context.directory()
    path = directory / INTENT
    if not path.exists() and not path.is_symlink():
        return CleanupReport(
            kind="NOT_STARTED", context=context, container_id=None, logs_exit=None, removal_exit=None
        )
    intent = read_owned(path, Intent)
    if intent.owner != context:
        raise ValueError("CI cleanup intent belongs to another run/attempt/shard")
    receipt_path = directory / RECEIPT
    receipt = (
        read_owned(receipt_path, Receipt) if receipt_path.exists() or receipt_path.is_symlink() else None
    )
    if receipt is not None and receipt.intent != intent:
        raise ValueError("CI cleanup container receipt does not bind its intent")
    owned = by_identifier(receipt.container_id) if receipt is not None else by_name(intent)
    if owned is None:
        if receipt is not None and by_name(intent) is not None:
            raise ValueError("CI cleanup name was reused after its accepted container disappeared")
        return CleanupReport(
            kind="ALREADY_ABSENT" if receipt is not None else "UNOBSERVED_ACCEPTANCE",
            context=context,
            container_id=receipt.container_id if receipt is not None else None,
            logs_exit=None,
            removal_exit=None,
        )
    owned.require_owner(intent, receipt.container_id if receipt is not None else None)
    logs = docker(["logs", owned.identifier])
    (directory / "clickhouse.log").write_text(logs.stdout + logs.stderr)
    owned_again = OwnedContainer.model_validate_json(inspection(owned.identifier))
    owned_again.require_owner(intent, owned.identifier)
    removal = docker(["rm", "--force", owned.identifier])
    return CleanupReport(
        kind="REMOVED",
        context=context,
        container_id=owned.identifier,
        logs_exit=logs.returncode,
        removal_exit=removal.returncode,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["start", "cleanup"])
    arguments = parser.parse_args()
    try:
        context = CIContext.from_environment(os.environ)
        if arguments.command == "start":
            start(context)
        else:
            report = cleanup(context)
            write_owned(context.directory() / "clickhouse-cleanup.json", report)
            print(report.model_dump_json())
            if (
                report.kind == "UNOBSERVED_ACCEPTANCE"
                or report.logs_exit not in {None, 0}
                or report.removal_exit not in {None, 0}
            ):
                raise SystemExit(1)
    except (KeyError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"CI ClickHouse refused: {type(error).__name__}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
