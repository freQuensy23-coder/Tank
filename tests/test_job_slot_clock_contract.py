"""Only actual admission wait-clock facets are declared and validated."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.jobs import JobEnvelope
from tank.contracts.json import decode_json
from tank.job_repository import JobRepository
from tank.storage import MemoryRepository
from tank.util import canonical_json


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "facets",
    [
        {},
        {
            "slot_waiting_since_prec": None,
            "slot_waiting_since_prec_reason": None,
            "slot_waiting_since_bucket_anchor": None,
        },
    ],
)
async def test_scalar_or_null_wait_metadata_roundtrips_without_invented_precision(
    facets: dict[str, JsonValue],
) -> None:
    job = await JobRepository(MemoryRepository()).enqueue("TELEGRAM_ANSWER", {"question_id": "q"}, "legacy")
    wire = decode_json(job.model_dump_json(exclude_unset=True))
    assert isinstance(wire, dict)
    wire.update(slot_waiting_since="2026-10-26T04:00:00+04:00", **facets)
    admitted = JobEnvelope.model_validate(wire)
    assert admitted.slot_waiting_since_prec is None and admitted.slot_waiting_since_prec_reason is None
    assert canonical_json(decode_json(admitted.model_dump_json(exclude_unset=True))) == canonical_json(wire)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields",
    [
        {"slot_waiting_since": "2026-10-26T00:00:00"},
        {"slot_waiting_since": None, "slot_waiting_since_prec": "EXACT"},
        {"slot_waiting_since": "2026-10-26T00:00:00Z", "slot_waiting_since_prec": "NOT_A_PRECISION"},
    ],
)
async def test_incoherent_wait_clock_rejects_before_any_queue_write(fields: dict[str, JsonValue]) -> None:
    repo = MemoryRepository()
    jobs = JobRepository(repo)
    job = await jobs.enqueue("TELEGRAM_ANSWER", {"question_id": "q"}, "invalid")
    before = await repo.get("ops.jobs", job.job_id)
    with pytest.raises(ValidationError):
        await jobs.put(job.model_copy(update=fields))
    assert await repo.get("ops.jobs", job.job_id) == before
