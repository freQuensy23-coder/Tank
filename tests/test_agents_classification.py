"""Typed immutable classification ledger exercised through actual provider and storage adapters."""

from types import SimpleNamespace

import httpx
import pytest
from agent_source_fixtures import source_item_fixture
from pydantic import ValidationError

from tank.agents.classification import JevClassifier
from tank.agents.jev import JevItem, JevState
from tank.agents.pipeline_contracts import JevScoreRecord
from tank.contracts.repository import Collection, Repository
from tank.storage import MemoryRepository


@pytest.mark.asyncio
async def test_classifier_validates_durable_scores_and_reuses_exact_request() -> None:
    store = MemoryRepository()
    scores = Repository(store, Collection("ops.llm_usage", JevScoreRecord))
    calls: list[httpx.Request] = []

    def provider(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13-free",
                "answers": {"relevant": {"type": "noul", "noul": 0.97}},
                "usage": {"input_tokens": 100, "output_tokens": 0},
            },
        )

    item = source_item_fixture("typed-classification", source_id="S30", text="ETH announcement")
    state = JevState(item=JevItem(source=item.source, text=item.text))
    settings = SimpleNamespace(
        opencode_api_key=None, replay=False, config_version="fixture", code_version="fixture"
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        classifier = JevClassifier(scores, settings, http)
        record = await classifier._score_record(item, "relevant", state)
        assert record.score == 0.97
        assert await classifier._score_record(item, "relevant", state) == record
        assert len(calls) == 1
        row = await store.get("ops.llm_usage", record.usage_id)
        assert row is not None
        damaged = MemoryRepository()
        await damaged.put("ops.llm_usage", record.usage_id, {**row, "item_id": "different"})
        classifier = JevClassifier(
            Repository(damaged, Collection("ops.llm_usage", JevScoreRecord)), settings, http
        )
        with pytest.raises(ValidationError):
            await classifier._score_record(item, "relevant", state)
        assert len(calls) == 1
