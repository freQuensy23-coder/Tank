"""Partition integration files deterministically without dropping any test cases."""

import hashlib
import os
from collections import defaultdict
from pathlib import Path

import pytest

from scripts.ci.collect_units import case_from_item, source_head
from scripts.ci.integration_order import (
    COLLECTION,
    OUTPUT,
    PLAN,
    IntegrationCollection,
    MeasuredOrderReport,
    NativeOrderReport,
    load_plan,
    measured_file_order,
    native_file_order,
    read_collection,
)


def parse_shard(value: str) -> tuple[int, int]:
    try:
        index, count = (int(part) for part in value.split("/"))
    except ValueError as error:
        raise ValueError("shard must be INDEX/COUNT") from error
    if not 0 <= index < count:
        raise ValueError("shard must satisfy 0 <= INDEX < COUNT")
    return index, count


def shard_for(path: str, count: int) -> int:
    if count == 3:
        match path:
            case (
                "tests/test_fixture_reference.py"
                | "tests/test_replay.py"
                | "tests/test_milestone_checks.py"
                | "tests/test_storage.py"
            ):
                return 1
            case "tests/test_agents_workflow.py" | "tests/test_apify_deploy.py":
                return 2
    return int.from_bytes(hashlib.sha256(path.encode()).digest()[:8]) % count


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--ci-shard", required=True, help="Zero-based integration shard INDEX/COUNT")
    parser.addoption("--integration-order-plan", type=Path, default=PLAN)
    parser.addoption("--integration-collection", type=Path, default=COLLECTION)
    parser.addoption("--integration-order-output", type=Path, default=OUTPUT)


def order_option_path(config: pytest.Config, name: str) -> Path:
    value = config.getoption(name)
    if not isinstance(value, Path):
        raise pytest.UsageError(f"{name} requires a path")
    return value


def full_collection(config: pytest.Config) -> tuple[IntegrationCollection, str]:
    path = order_option_path(config, "--integration-collection")
    return read_collection(path)


def declared_shard(config: pytest.Config) -> tuple[int, int]:
    value = config.getoption("--ci-shard")
    if not isinstance(value, str):
        raise pytest.UsageError("shard must be INDEX/COUNT text")
    try:
        return parse_shard(value)
    except ValueError as error:
        raise pytest.UsageError(str(error)) from error


def require_native_file_scheduler(config: pytest.Config) -> None:
    if config.getoption("dist") != "loadfile" or config.getoption("loadscopereorder") is not False:
        raise pytest.UsageError("Integration file ordering requires --dist loadfile --no-loadscope-reorder")


def planned_order(config: pytest.Config, files: list[str], index: int) -> tuple[str, ...]:
    if index != 2:
        return native_file_order(files)
    plan = load_plan(order_option_path(config, "--integration-order-plan"))
    if any(shard_for(item.file, 3) != 2 for item in plan.files):
        raise pytest.UsageError("Lane-two integration timing contains a foreign file")
    return measured_file_order(files, plan).files


def pytest_sessionstart(session: pytest.Session) -> None:
    if "PYTEST_XDIST_WORKER" in os.environ:
        return
    config = session.config
    index, count = declared_shard(config)
    if count != 3:
        return
    require_native_file_scheduler(config)
    collection, digest = full_collection(config)
    files = [case.file for case in collection.root if shard_for(case.file, count) == index]
    if not files:
        raise pytest.UsageError("Integration ordering requires a nonempty selected lane")
    checkout = source_head(config.rootpath)
    report: NativeOrderReport | MeasuredOrderReport
    if index == 2:
        plan = load_plan(order_option_path(config, "--integration-order-plan"))
        if any(shard_for(item.file, 3) != 2 for item in plan.files):
            raise pytest.UsageError("Lane-two integration timing contains a foreign file")
        report = MeasuredOrderReport(
            kind="MEASURED_FILE_SLOTS",
            source_head=checkout,
            collection_sha256=digest,
            calibration_source_head=plan.source_head,
            calibration_checkout=plan.checkout_sha,
            calibration_source_tree=plan.source_tree,
            calibration_report_sha256=plan.report_sha256,
            calibration_collection_sha256=plan.collection_sha256,
            calibration_is_historical=checkout != plan.checkout_sha,
            shard_index=index,
            shard_count=count,
            cases=len(files),
            order=measured_file_order(files, plan),
        )
    else:
        report = NativeOrderReport(
            kind="NATIVE_CASE_COUNT",
            source_head=checkout,
            collection_sha256=digest,
            shard_index=index,
            shard_count=count,
            cases=len(files),
            files=native_file_order(files),
        )
    target = order_option_path(config, "--integration-order-output")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(report.model_dump_json(indent=2) + "\n")
    print(f"INTEGRATION_ORDER shard={index}/{count} cases={len(files)} policy={report.kind}", flush=True)


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    index, count = declared_shard(config)
    if count == 3:
        collection, _ = full_collection(config)
        if [case_from_item(item) for item in items] != collection.root:
            raise pytest.UsageError("Integration run differs from its complete recorded collection/order")
    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        target = selected if shard_for(item.nodeid.split("::", 1)[0], count) == index else deselected
        target.append(item)
    if count == 3:
        order = planned_order(config, [item.nodeid.split("::", 1)[0] for item in selected], index)
        grouped: dict[str, list[pytest.Item]] = defaultdict(list)
        for item in selected:
            grouped[item.nodeid.split("::", 1)[0]].append(item)
        items[:] = [item for file in order for item in grouped[file]]
    else:
        items[:] = selected
    config.hook.pytest_deselected(items=deselected)
