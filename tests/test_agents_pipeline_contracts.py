"""Pipeline adapter refusals preserve strict capture and relevance contracts."""

import httpx
import pytest
from agent_source_fixtures import pipeline_settings_fixture, source_item_fixture
from pydantic import ValidationError

from tank.agents.pipeline import Pipeline
from tank.ops_progress import DisabledSourceProgress
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["missing_publisher", "invented_observation_precision"])
async def test_pipeline_refuses_invalid_persisted_capture_before_provider_or_enqueue(defect: str) -> None:
    repo = MemoryRepository()
    record = source_item_fixture("invalid", source_id="S30", text="Ethereum announcement").storage_record()
    if defect == "missing_publisher":
        del record["source"]
    else:
        record["observed_at_prec"] = "BUCKET(1d)"
    await repo.put("ingest.source_items", "invalid", record)

    def provider(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Invalid capture must not reach the classifier")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        pipeline = Pipeline(
            repo,
            pipeline_settings_fixture({"registry": {}, "thresholds": {}}),
            progress=DisabledSourceProgress(),
            http=http,
        )
        with pytest.raises(ValidationError):
            await pipeline.filter_item_wire({"item_id": "invalid"})
    assert await repo.list("ops.jobs") == []
    assert await repo.list("ops.llm_usage") == []


@pytest.mark.asyncio
async def test_pipeline_requires_completed_relevance_and_owned_job_fields() -> None:
    repo = MemoryRepository()
    record = source_item_fixture("pending", source_id="S30", text="Ethereum announcement").storage_record()
    await repo.put("ingest.source_items", "pending", record)

    def provider(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Pending relevance must not reach routing classification")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        pipeline = Pipeline(
            repo,
            pipeline_settings_fixture({"registry": {}, "thresholds": {}}),
            progress=DisabledSourceProgress(),
            http=http,
        )
        with pytest.raises(ValueError, match="relevance"):
            await pipeline.route_item_wire({"item_id": "pending"})
        with pytest.raises(ValidationError):
            await pipeline.filter_item_wire({"item_id": "pending", "source": "forged publisher"})
    assert await repo.list("ops.jobs") == []
    assert await repo.get("ingest.source_items", "pending") == record
