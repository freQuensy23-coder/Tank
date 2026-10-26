"""Partition complete units into two measured whole-file lanes without dropping cases."""

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Annotated, Literal

import pytest
from pydantic import Field

from scripts.ci.collect_units import case_from_item, source_head
from scripts.ci.unit_contracts import (
    UnitCollection,
    UnitDeclaration,
    UnitPlan,
    UnitRecord,
    measured_assignments,
    parse_shard,
    read_model,
    shard_for,
    weight_coverage,
)

PLAN = Path(__file__).with_name("unit_shards.json")


class UnitFailure(UnitRecord):
    nodeid: str = Field(min_length=1)
    phase: Literal["setup", "call", "teardown"]
    duration_seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)]


class UnitWorkerCrash(UnitRecord):
    nodeid: str = Field(min_length=1)
    phase: Literal["???"]


def failure_from_report(report: pytest.TestReport) -> UnitFailure | UnitWorkerCrash:
    phase: object = report.when
    if phase == "???":
        return UnitWorkerCrash(nodeid=report.nodeid, phase="???")
    return UnitFailure.model_validate(
        {"nodeid": report.nodeid, "phase": phase, "duration_seconds": report.duration}
    )


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--ci-unit-shard", required=True)
    parser.addoption("--unit-plan", type=Path, default=PLAN)
    parser.addoption("--unit-collection", type=Path, default=Path(".test-results/unit-collection.json"))
    parser.addoption("--unit-declaration-output", type=Path, default=Path(".test-results/unit-run.json"))


def option_path(config: pytest.Config, name: str) -> Path:
    value = config.getoption(name)
    if not isinstance(value, Path):
        raise pytest.UsageError(f"{name} requires a path")
    return value


def declaration(config: pytest.Config) -> tuple[UnitDeclaration, UnitCollection, UnitPlan]:
    value = config.getoption("--ci-unit-shard")
    if not isinstance(value, str):
        raise pytest.UsageError("Unit shard requires text")
    shard = parse_shard(value)
    path = option_path(config, "--unit-plan")
    plan = read_model(path, UnitPlan)
    collection = read_model(option_path(config, "--unit-collection"), UnitCollection)
    actual_head = source_head(config.rootpath)
    if collection.source_head != actual_head:
        raise ValueError("Unit collection belongs to a different source commit")
    record = UnitDeclaration(
        schema_version="tank-unit-declaration-v1",
        source_head=actual_head,
        shard=shard,
        collection_sha256=collection.signature,
        plan_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        coverage=weight_coverage(collection, plan),
    )
    return record, collection, plan


def pytest_sessionstart(session: pytest.Session) -> None:
    if "PYTEST_XDIST_WORKER" in os.environ:
        return
    record, collection, plan = declaration(session.config)
    measured = measured_assignments(plan.files)
    selected = sum(shard_for(case.file, measured) == record.shard.index for case in collection.cases)
    print(
        f"UNIT_SELECTION shard={record.shard.index}/2 selected={selected} full={len(collection.cases)} calibration={plan.source_head} unmeasured={len(record.coverage.unmeasured_files)}",
        flush=True,
    )
    target = option_path(session.config, "--unit-declaration-output")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(record.model_dump_json(indent=2) + "\n")


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    record, collection, plan = declaration(config)
    actual = UnitCollection(
        schema_version="tank-unit-collection-v1",
        source_head=record.source_head,
        cases=[case_from_item(item) for item in items],
    )
    if actual.signature != collection.signature:
        raise pytest.UsageError("Unit run selection differs from the complete recorded collection")
    measured = measured_assignments(plan.files)
    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        target = (
            selected
            if shard_for(item.nodeid.split("::", 1)[0], measured) == record.shard.index
            else deselected
        )
        target.append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if not report.failed or "PYTEST_XDIST_WORKER" in os.environ:
        return
    failure = failure_from_report(report)
    target = Path(".test-results/unit-failures.jsonl")
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a") as stream:
        stream.write(failure.model_dump_json() + "\n")
    print(
        f"\nUNIT_FAILURE nodeid={json.dumps(failure.nodeid)} phase={failure.phase}",
        file=sys.stderr,
        flush=True,
    )
