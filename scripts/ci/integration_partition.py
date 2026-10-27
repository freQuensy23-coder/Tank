"""Pure deterministic integration file partition rules."""

import hashlib


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
