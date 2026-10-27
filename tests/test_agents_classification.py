"""Typed immutable classification ledger exercised through actual provider and storage adapters."""

from types import SimpleNamespace

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture, source_item_fixture
from pydantic import ValidationError

from tank.agents.classification import DecisionClassifier
from tank.agents.decision import DecisionItem, DecisionState
from tank.agents.pipeline_contracts import DecisionScoreRecord
from tank.contracts.repository import Collection
from tank.repository import Repository
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("contradictory_subject", [True, False])
async def test_classifier_validates_durable_scores_and_reuses_exact_request(
    contradictory_subject: bool,
) -> None:
    store = MemoryRepository()
    scores = Repository(store, Collection("ops.llm_usage", DecisionScoreRecord))
    calls: list[httpx.Request] = []

    def provider(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            200,
            json={
                "model": "microsoft-decision-1",
                "answers": {"relevant": {"type": "noul", "noul": 0.97}},
                "usage": {"input_tokens": 100, "output_tokens": 0},
            },
        )

    item = source_item_fixture("typed-classification", source_id="S30", text="ETH announcement")
    state = DecisionState(item=DecisionItem(source=item.source, text=item.text))
    settings = SimpleNamespace(
        foundry_api_key="FAKE_FOUNDRY_KEY",
        foundry_endpoint="https://synthetic.services.ai.azure.com",
        foundry_deployment="decision-1",
        replay=False,
        config_version="fixture",
        code_version="fixture",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        classifier = DecisionClassifier(scores, settings, http, versions=declared_versions_fixture())
        record = await classifier._score_record(item, "relevant", state)
        assert record.score == 0.97
        assert await classifier._score_record(item, "relevant", state) == record
        assert len(calls) == 1
        row = await store.get("ops.llm_usage", record.usage_id)
        assert row is not None
        damaged = MemoryRepository()
        changes = (
            {"item_id": "different"}
            if contradictory_subject
            else {"item_id": "different", "subject_id": "different"}
        )
        await damaged.put("ops.llm_usage", record.usage_id, {**row, **changes})
        classifier = DecisionClassifier(
            Repository(damaged, Collection("ops.llm_usage", DecisionScoreRecord)),
            settings,
            http,
            versions=declared_versions_fixture(),
        )
        with pytest.raises(ValidationError if contradictory_subject else ValueError):
            await classifier._score_record(item, "relevant", state)
        assert len(calls) == 1
