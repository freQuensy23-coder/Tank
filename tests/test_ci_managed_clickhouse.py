"""Exact launch identity and uncertainty controls for disposable CI ClickHouse."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import JsonValue, ValidationError

from scripts.ci import managed_clickhouse as ch


def context(tmp_path: Path) -> ch.CIContext:
    return ch.CIContext(
        root=tmp_path,
        repository="synthetic/tank",
        run_id=123,
        attempt=2,
        shard=1,
        workflow_ref="synthetic/tank/.github/workflows/ci.yml@refs/pull/1/merge",
    )


def intent(tmp_path: Path) -> ch.Intent:
    return ch.Intent(schema_version="tank-ci-clickhouse-intent-v1", owner=context(tmp_path), nonce="a" * 32)


def owned(launch: ch.Intent, identifier: str = "b" * 64) -> dict[str, JsonValue]:
    owner = launch.owner
    return {
        "Id": identifier,
        "Image": "sha256:" + "c" * 64,
        "Name": "/" + launch.name,
        "Config": {
            "Image": ch.IMAGE,
            "Labels": {
                "tank.ci-owner": f"{owner.run_id}-{owner.attempt}",
                "tank.ci-repository": owner.repository,
                "tank.ci-run": str(owner.run_id),
                "tank.ci-attempt": str(owner.attempt),
                "tank.ci-shard": str(owner.shard),
                "tank.ci-launch": launch.nonce,
                "tank.ci-service": "clickhouse",
            },
        },
    }


def result(arguments: list[str], output: str = "", code: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(arguments, code, output, "")


def test_exact_context_roundtrip_and_canonical_run_identity(tmp_path: Path) -> None:
    actual = context(tmp_path)
    assert ch.CIContext.from_environment(actual.environment()) == actual
    for key, value in [("GITHUB_RUN_ID", "0123"), ("GITHUB_RUN_ATTEMPT", "0"), ("TANK_CI_SHARD", "3")]:
        with pytest.raises(ValueError):
            ch.CIContext.from_environment(actual.environment() | {key: value})
    with pytest.raises(ValueError):
        ch.CIContext.from_environment(actual.environment() | {"GITHUB_ACTIONS": "false"})


def test_owned_state_refuses_symlink_wrong_mode_and_duplicate_fields(tmp_path: Path) -> None:
    launch = intent(tmp_path)
    path = launch.owner.directory() / ch.INTENT
    ch.write_owned(path, launch)
    assert ch.read_owned(path, ch.Intent) == launch
    path.chmod(0o644)
    with pytest.raises(ValueError, match="owner/mode"):
        ch.read_owned(path, ch.Intent)
    path.chmod(0o600)
    path.write_text('{"nonce":"a","nonce":"b"}')
    with pytest.raises(ValueError):
        ch.read_owned(path, ch.Intent)
    link = path.with_name("link.json")
    link.symlink_to(path)
    with pytest.raises(OSError):
        ch.read_owned(link, ch.Intent)


@pytest.mark.parametrize("change", ["id", "name", "image", "nonce", "attempt"])
def test_foreign_identity_refuses_before_effects(tmp_path: Path, change: str) -> None:
    launch = intent(tmp_path)
    row = ch.OwnedContainer.model_validate(owned(launch))
    if change == "id":
        changed = row.model_copy(update={"identifier": "d" * 64})
    elif change == "name":
        changed = row.model_copy(update={"name": "/another-owned-launch"})
    elif change == "image":
        changed = row.model_copy(update={"config": row.config.model_copy(update={"image": "other"})})
    else:
        labels = row.config.labels.model_copy(update={change: "d" * 32 if change == "nonce" else "3"})
        changed = row.model_copy(update={"config": row.config.model_copy(update={"labels": labels})})
    with pytest.raises(ValueError, match="foreign"):
        changed.require_owner(launch, "b" * 64)


def test_unobserved_create_response_is_not_absence_or_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launch = intent(tmp_path)
    ch.write_owned(launch.owner.directory() / ch.INTENT, launch)
    calls: list[list[str]] = []

    def docker(arguments: list[str], *, timeout: float | None = 30) -> subprocess.CompletedProcess[str]:
        calls.append(arguments)
        assert arguments[:2] == ["container", "ls"]
        return result(arguments)

    monkeypatch.setattr(ch, "docker", docker)
    report = ch.cleanup(launch.owner)
    assert report.kind == "UNOBSERVED_ACCEPTANCE"
    assert report.container_id is None and len(calls) == 1


def test_accepted_cid_is_inspected_even_if_original_name_disappeared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launch = intent(tmp_path)
    directory = launch.owner.directory()
    ch.write_owned(directory / ch.INTENT, launch)
    ch.write_owned(
        directory / ch.RECEIPT,
        ch.Receipt(schema_version="tank-ci-clickhouse-receipt-v1", intent=launch, container_id="b" * 64),
    )
    calls: list[list[str]] = []
    row = owned(launch)
    row["Name"] = "/renamed-container"

    def docker(arguments: list[str], *, timeout: float | None = 30) -> subprocess.CompletedProcess[str]:
        calls.append(arguments)
        if arguments[:2] == ["container", "ls"]:
            assert arguments[-1] == "id=" + "b" * 64
            return result(arguments, "b" * 64 + "\n")
        assert arguments[:1] == ["inspect"]
        return result(arguments, json.dumps(row))

    monkeypatch.setattr(ch, "docker", docker)
    with pytest.raises(ValueError, match="foreign"):
        ch.cleanup(launch.owner)
    assert len(calls) == 2


def test_cleanup_joins_logs_and_exact_id_removal_after_second_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launch = intent(tmp_path)
    ch.write_owned(launch.owner.directory() / ch.INTENT, launch)
    calls: list[list[str]] = []

    def docker(arguments: list[str], *, timeout: float | None = 30) -> subprocess.CompletedProcess[str]:
        calls.append(arguments)
        if arguments[:2] == ["container", "ls"]:
            return result(arguments, "b" * 64 + "\n")
        if arguments[:1] == ["inspect"]:
            return result(arguments, json.dumps(owned(launch)))
        if arguments[:1] == ["logs"]:
            return result(arguments, "owned startup log", 1)
        assert arguments == ["rm", "--force", "b" * 64]
        return result(arguments)

    monkeypatch.setattr(ch, "docker", docker)
    report = ch.cleanup(launch.owner)
    assert report.kind == "REMOVED" and report.logs_exit == 1 and report.removal_exit == 0
    assert [arguments[0] for arguments in calls] == ["container", "inspect", "logs", "inspect", "rm"]
    assert (launch.owner.directory() / "clickhouse.log").read_text() == "owned startup log"


def test_missing_required_labels_never_admit_an_owned_container(tmp_path: Path) -> None:
    row = owned(intent(tmp_path))
    row["Config"] = {"Image": ch.IMAGE, "Labels": {}}
    with pytest.raises(ValidationError):
        ch.OwnedContainer.model_validate(row)


def test_unresolved_launch_cannot_create_a_second_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launch = intent(tmp_path)
    ch.write_owned(launch.owner.directory() / ch.INTENT, launch)

    def refused(arguments: list[str], *, timeout: float | None = 30) -> subprocess.CompletedProcess[str]:
        pytest.fail("an unresolved launch must refuse before any Docker request")

    monkeypatch.setattr(ch, "docker", refused)
    with pytest.raises(FileExistsError):
        ch.start(launch.owner)


def test_identity_change_after_logs_refuses_removal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    launch = intent(tmp_path)
    ch.write_owned(launch.owner.directory() / ch.INTENT, launch)
    calls: list[list[str]] = []
    inspected = 0

    def docker(arguments: list[str], *, timeout: float | None = 30) -> subprocess.CompletedProcess[str]:
        nonlocal inspected
        calls.append(arguments)
        if arguments[:2] == ["container", "ls"]:
            return result(arguments, "b" * 64 + "\n")
        if arguments[:1] == ["inspect"]:
            inspected += 1
            row = owned(launch)
            if inspected == 2:
                row["Name"] = "/foreign-name"
            return result(arguments, json.dumps(row))
        assert arguments == ["logs", "b" * 64]
        return result(arguments, "already admitted owned logs")

    monkeypatch.setattr(ch, "docker", docker)
    with pytest.raises(ValueError, match="foreign"):
        ch.cleanup(launch.owner)
    assert [arguments[0] for arguments in calls] == ["container", "inspect", "logs", "inspect"]


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.timeout(10)
def test_actual_adapter_failed_removal_persists_unconfirmed_phase_and_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launch = intent(tmp_path)
    directory = launch.owner.directory()
    ch.write_owned(directory / ch.INTENT, launch)
    (tmp_path / "inspection.json").write_text(json.dumps(owned(launch)))
    binaries = tmp_path / "bin"
    binaries.mkdir()
    executable = binaries / "docker"
    executable.write_text(
        "#!" + sys.executable + "\n"
        "import os,sys,pathlib\n"
        "root=pathlib.Path(os.environ['CH_REMOVAL_CONTROL'])\n"
        "command=sys.argv[1]\n"
        "with (root/'calls').open('a') as stream: stream.write(command+'\\n')\n"
        "if command=='container': print('b'*64)\n"
        "elif command=='inspect': print((root/'inspection.json').read_text())\n"
        "elif command=='logs': print('actual owned adapter log')\n"
        "elif command=='rm': raise SystemExit(7)\n"
        "else: raise SystemExit(9)\n"
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", str(binaries) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("CH_REMOVAL_CONTROL", str(tmp_path))
    for key, value in launch.owner.environment().items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", ["managed_clickhouse", "cleanup"])
    with pytest.raises(SystemExit) as failed:
        ch.main()
    assert failed.value.code == 1
    report = ch.read_owned(directory / "clickhouse-cleanup.json", ch.CleanupReport)
    assert report.kind == "UNCONFIRMED_REMOVAL"
    assert report.container_id == "b" * 64 and report.logs_exit == 0 and report.removal_exit == 7
    assert (tmp_path / "calls").read_text().splitlines() == ["container", "inspect", "logs", "inspect", "rm"]
    with pytest.raises(ValidationError, match="removal phase"):
        ch.CleanupReport.model_validate(report.model_dump() | {"kind": "REMOVED"})
