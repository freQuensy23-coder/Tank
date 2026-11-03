"""Partition integration files deterministically without dropping any test cases."""

import hashlib
import json

import pytest


def parse_shard(value: str) -> tuple[int, int]:
    try:
        index, count = (int(part) for part in value.split("/"))
    except ValueError as error:
        raise ValueError("shard must be INDEX/COUNT") from error
    if not 0 <= index < count:
        raise ValueError("shard must satisfy 0 <= INDEX < COUNT")
    return index, count


def shard_for(path: str, count: int) -> int:
    return int.from_bytes(hashlib.sha256(path.encode()).digest()[:8]) % count


def pytest_addoption(parser):
    parser.addoption("--ci-shard", required=True, help="Zero-based integration shard INDEX/COUNT")


def pytest_configure(config):
    if not hasattr(config, "workerinput"):
        config.pluginmanager.register(FailureReceipts(config.rootpath / ".test-results"))


class FailureReceipts:
    def __init__(self, directory):
        self.directory = directory

    def pytest_runtest_logreport(self, report):
        if not report.failed:
            return
        record = {
            "nodeid": report.nodeid,
            "when": report.when,
            "outcome": report.outcome,
            "duration": report.duration,
            "longrepr": report.longreprtext[:32768],
        }
        self.directory.mkdir(parents=True, exist_ok=True)
        with (self.directory / "integration-failures.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        print("\n" + report.nodeid + "\n" + record["longrepr"], flush=True)


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    try:
        index, count = parse_shard(config.getoption("--ci-shard"))
    except ValueError as error:
        raise pytest.UsageError(str(error)) from error
    selected, deselected = [], []
    for item in items:
        target = selected if shard_for(item.nodeid.split("::", 1)[0], count) == index else deselected
        target.append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)
