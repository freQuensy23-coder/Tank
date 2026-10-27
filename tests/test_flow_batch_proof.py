import json
from collections.abc import Awaitable, Callable
from datetime import timedelta
from pathlib import Path

import pytest
from foundation_helpers import present, text_field
from pydantic import JsonValue
from test_clickhouse_market import Both
from test_clickhouse_market import both as both
from test_storage import repo as repo
from test_storage_native_flow import native_bar, storage_wire

from tank.contracts.json import JsonDocument
from tank.market.trade_contracts import FlowBar
from tank.market_storage import WRITE_CHUNK
from tank.models import Record
from tank.replay import replay_bundle
from tank.settings import Settings
from tank.storage import MemoryRepository, StorageBackend
from tank.util import iso, parse_time


def flow_revisions(reason: str | None) -> tuple[Record, Record]:
    proven = native_bar()
    incoming: FlowBar = type(proven).model_validate(
        proven.model_copy(update={"complete": False, "gap_reason": reason})
    )
    return storage_wire(proven), storage_wire(incoming)


async def assert_batch_retains_proof(repository: StorageBackend, reason: str | None, split: bool) -> None:
    proven, unproven = flow_revisions(reason)
    rows = [proven]
    keys = ["proven-point"]
    if split:
        other = storage_wire(native_bar())
        rows.extend([other] * (WRITE_CHUNK - 1))
        keys.extend(["other-point"] * (WRITE_CHUNK - 1))
    rows.append(unproven)
    keys.append("proven-point")
    await repository.put_market_batch("mkt.flow_bars_1m", rows, keys=keys)
    stored = present(await repository.get("mkt.flow_bars_1m", "proven-point"))
    assert stored["complete"] is True
    assert stored["gap_reason"] is None
    assert stored["buy_qty"] == proven["buy_qty"] and stored["sell_qty"] == proven["sell_qty"]


@pytest.mark.parametrize("reason", [None, "STREAM_COVERAGE_UNPROVEN"])
@pytest.mark.parametrize("split", [False, True])
async def test_batch_complete_proof_survives_unproven_revision(
    repo: StorageBackend, reason: str | None, split: bool
) -> None:
    await assert_batch_retains_proof(repo, reason, split)


async def test_sequential_complete_proof_and_actual_gap_keep_existing_policy(repo: StorageBackend) -> None:
    proven, unproven = flow_revisions("STREAM_COVERAGE_UNPROVEN")
    await repo.put_market_batch("mkt.flow_bars_1m", [proven], keys=["point"])
    assert await repo.put_market_batch("mkt.flow_bars_1m", [unproven], keys=["point"]) == []
    assert present(await repo.get("mkt.flow_bars_1m", "point"))["complete"] is True
    lost = {**unproven, "gap_reason": "STREAM_DISCONNECTED: synthetic fixture"}
    assert len(await repo.put_market_batch("mkt.flow_bars_1m", [lost], keys=["point"])) == 1
    stored = present(await repo.get("mkt.flow_bars_1m", "point"))
    assert stored["complete"] is False
    assert text_field(stored, "gap_reason") == "STREAM_DISCONNECTED: synthetic fixture"


@pytest.mark.integration
@pytest.mark.parametrize("split", [False, True])
async def test_routed_clickhouse_batch_preserves_same_complete_proof(both: Both, split: bool) -> None:
    _, routed, _ = both
    await assert_batch_retains_proof(routed, "STREAM_COVERAGE_UNPROVEN", split)


@pytest.mark.parametrize("actual_gap", [False, True])
async def test_batch_proof_order_and_reported_disconnect(repo: StorageBackend, actual_gap: bool) -> None:
    proven, unproven = flow_revisions("STREAM_COVERAGE_UNPROVEN")
    rows = [unproven, proven]
    if actual_gap:
        rows = [proven, {**unproven, "gap_reason": "STREAM_DISCONNECTED: synthetic fixture"}]
    await repo.put_market_batch("mkt.flow_bars_1m", rows, keys=["point", "point"])
    stored = present(await repo.get("mkt.flow_bars_1m", "point"))
    assert stored["complete"] is (not actual_gap)
    assert stored["gap_reason"] == ("STREAM_DISCONNECTED: synthetic fixture" if actual_gap else None)


async def test_actual_captured_tool_import_preserves_complete_flow_revision(tmp_path: Path) -> None:
    proven, unproven = flow_revisions("STREAM_COVERAGE_UNPROVEN")
    cutoff = iso(parse_time(text_field(proven, "minute_start")) + timedelta(minutes=1))
    settings = Settings(config_version="test", code_version="test", replay=True, _env_file=None)
    packet = tmp_path / "input.json"
    packet.write_text(
        json.dumps(
            {
                "cutoff": cutoff,
                "config_version": "test",
                "code_version": "test",
                "registry": {},
                "config": {
                    "thresholds": settings.load_config("thresholds"),
                    "replay_tool_calls": [
                        {
                            "call_id": "read-item",
                            "tool": "get_item",
                            "arguments": {"item_id": "item"},
                            "available_at": cutoff,
                        }
                    ],
                },
                "records": [
                    {
                        "collection": "ingest.source_items",
                        "key": "item",
                        "available_at": cutoff,
                        "record": {"item_id": "item", "text": "Synthetic flow replay control"},
                    },
                    *[
                        {
                            "collection": "mkt.flow_bars_1m",
                            "key": "flow-point",
                            "available_at": cutoff,
                            "record": row,
                        }
                        for row in (proven, unproven)
                    ],
                ],
                "tool_results": [],
            }
        )
    )
    repository = MemoryRepository()
    run_replay: Callable[[Path, MemoryRepository, Settings], Awaitable[JsonValue]] = replay_bundle
    result = JsonDocument.validate_value(await run_replay(packet, repository, settings))
    assert isinstance(result, dict)
    assert result["mode"] == "CAPTURED_TOOL_REEXECUTION"
    stored = present(await repository.get("mkt.flow_bars_1m", "flow-point"))
    assert stored["complete"] is True
    assert stored["gap_reason"] is None
    assert stored["buy_qty"] == proven["buy_qty"] and stored["sell_qty"] == proven["sell_qty"]
