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
    return int.from_bytes(hashlib.sha256(path.encode()).digest()[:8]) % count


def selected_for(path: str, selection: str) -> bool:
    request = path == "tests/test_market_truth_request.py"
    if selection == "market-truth":
        return request
    index, count = parse_shard(selection)
    return not request and shard_for(path, count) == index


def pytest_addoption(parser):
    parser.addoption("--ci-shard", required=True, help="Integration shard INDEX/COUNT or market-truth")


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    selection = config.getoption("--ci-shard")
    try:
        if selection != "market-truth":
            parse_shard(selection)
    except ValueError as error:
        raise pytest.UsageError(str(error)) from error
    selected, deselected = [], []
    for item in items:
        target = selected if selected_for(item.nodeid.split("::", 1)[0], selection) else deselected
        target.append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)
