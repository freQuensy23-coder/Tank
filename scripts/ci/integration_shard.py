"""Partition integration files deterministically without dropping any test cases."""

import hashlib

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


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    try:
        selected_shard = config.getoption("--ci-shard")
        if not isinstance(selected_shard, str):
            raise ValueError("shard must be INDEX/COUNT text")
        index, count = parse_shard(selected_shard)
    except ValueError as error:
        raise pytest.UsageError(str(error)) from error
    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        target = selected if shard_for(item.nodeid.split("::", 1)[0], count) == index else deselected
        target.append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)
