"""Immutable JSON identity distinguishes booleans, structure and observed values."""

import pytest
from pydantic import JsonValue
from test_storage import repo as repo

from tank.models import Record
from tank.storage import ImmutableRecordError, StorageBackend
from tank.time_precision import collection_clock
from tank.util import canonical_json


@pytest.mark.parametrize(
    ("original", "changed"),
    [
        (True, 1),
        (False, 0),
        (1, True),
        (0, False),
        ([True], [1]),
        ({"flag": True}, {"flag": 1}),
        ({"flag": False}, {"flag": 0.0}),
        (1, "1"),
        (None, "null"),
        ([], {}),
        ([1, 2], [2, 1]),
        ({"present": None}, {}),
        (2**80 + 1, 2**80),
    ],
)
async def test_immutable_json_change_refuses_before_preserving_original(
    repo: StorageBackend, original: JsonValue, changed: JsonValue
) -> None:
    record: Record = {
        "checkpoint_id": "identity",
        "state": {"value": original},
        **collection_clock().fields("at"),
    }
    stored = await repo.put("ops.scheduler_checkpoints", "identity", record, immutable=True)
    before = canonical_json(stored)
    assert await repo.put("ops.scheduler_checkpoints", "identity", record, immutable=True) == stored
    with pytest.raises(ImmutableRecordError):
        await repo.put(
            "ops.scheduler_checkpoints",
            "identity",
            {**record, "state": {"value": changed}},
            immutable=True,
        )
    after = await repo.get("ops.scheduler_checkpoints", "identity")
    assert after is not None and canonical_json(after) == before


@pytest.mark.parametrize("value", [None, True, False, 0, 1, 1.0, -0.0, 1e20, 1e23, "1", [], {}])
async def test_immutable_exact_input_duplicate_survives_json_storage_normalization(
    repo: StorageBackend, value: JsonValue
) -> None:
    record: Record = {
        "checkpoint_id": "duplicate",
        "state": {"value": value},
        **collection_clock().fields("at"),
    }
    await repo.put("ops.scheduler_checkpoints", "duplicate", record, immutable=True)
    stored = await repo.get("ops.scheduler_checkpoints", "duplicate")
    assert stored is not None
    repeated = await repo.put("ops.scheduler_checkpoints", "duplicate", record, immutable=True)
    assert canonical_json(repeated) == canonical_json(stored)
    unchanged = await repo.get("ops.scheduler_checkpoints", "duplicate")
    assert unchanged is not None and canonical_json(unchanged) == canonical_json(stored)


@pytest.mark.parametrize(
    ("original", "equivalent"),
    [(1, 1.0), (-0.0, 0), (1e23, 100000000000000000000000), ({"a": [1.0, -0.0]}, {"a": [1, 0]})],
)
async def test_immutable_json_numeric_value_equivalence_preserves_the_stored_document(
    repo: StorageBackend, original: JsonValue, equivalent: JsonValue
) -> None:
    record: Record = {
        "checkpoint_id": "numeric",
        "state": {"value": original},
        **collection_clock().fields("at"),
    }
    await repo.put("ops.scheduler_checkpoints", "numeric", record, immutable=True)
    stored = await repo.get("ops.scheduler_checkpoints", "numeric")
    assert stored is not None
    repeated = await repo.put(
        "ops.scheduler_checkpoints", "numeric", {**record, "state": {"value": equivalent}}, immutable=True
    )
    assert canonical_json(repeated) == canonical_json(stored)
    after = await repo.get("ops.scheduler_checkpoints", "numeric")
    assert after is not None and canonical_json(after) == canonical_json(stored)


async def test_rejected_immutable_promotion_leaves_the_original_record_mutable(repo: StorageBackend) -> None:
    record: Record = {
        "checkpoint_id": "promotion",
        "state": {"flag": True},
        **collection_clock().fields("at"),
    }
    await repo.put("ops.scheduler_checkpoints", "promotion", record)
    with pytest.raises(ImmutableRecordError):
        await repo.put(
            "ops.scheduler_checkpoints", "promotion", {**record, "state": {"flag": 1}}, immutable=True
        )
    after = await repo.get("ops.scheduler_checkpoints", "promotion")
    assert after is not None and canonical_json(after) == canonical_json(record)
    changed: Record = {**record, "state": {"flag": False}}
    await repo.put("ops.scheduler_checkpoints", "promotion", changed)
    after = await repo.get("ops.scheduler_checkpoints", "promotion")
    assert after is not None and canonical_json(after) == canonical_json(changed)
