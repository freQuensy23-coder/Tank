"""Offline Jev provider integration (OpenCode System One); no environment credentials or live requests."""

import json

import httpx
import pytest
from agent_source_fixtures import (
    PipelineFixtureSettings,
    declared_versions_fixture,
    pipeline_settings_fixture,
    source_item_fixture,
)
from pydantic import JsonValue

from tank.agents import Pipeline
from tank.agents.jev import JevItem, JevQuestion, JevState, classify
from tank.agents.pipeline import FILTER, FILTER_CRITERIA
from tank.ops_progress import DisabledSourceProgress
from tank.storage import MemoryRepository

URL = "https://opencode.ai/inference/systemone/v1/systemone"


def settings(*, opencode_api_key: str | None = None) -> PipelineFixtureSettings:
    return pipeline_settings_fixture(
        {"registry": {}, "thresholds": {"filter_cutoff": 0.7}}, opencode_api_key=opencode_api_key
    )


def response(model: str = "jev-1.13-free", **usage: JsonValue) -> dict[str, JsonValue]:
    return {
        "model": model,
        "answers": {"relevant": {"type": "noul", "noul": 0.97}},
        "usage": {"input_tokens": 100, "output_tokens": 22, **usage},
    }


ITEM = source_item_fixture("i", source_id="S30", source="Issuer", author="Issuer", text="ETH crypto asset")


@pytest.mark.asyncio
async def test_free_tier_without_key_sends_no_authorization_and_caches() -> None:
    repo = MemoryRepository()
    calls: list[httpx.Request] = []

    def fake(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert str(request.url) == URL and "authorization" not in request.headers
        assert json.loads(request.content) == {
            "model": "jev-1.13-free",
            "state": Pipeline.state(ITEM).to_wire(),
            "questions": {"relevant": {"type": "noul", "instructions": FILTER, "criteria": FILTER_CRITERIA}},
        }
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(
            repo,
            settings(),
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
    assert len(calls) == 1
    row = (await repo.list("ops.llm_usage"))[0]
    assert row["at"] == row["created_at"] and row["at_prec"] == "COLLECTION_ONLY"
    assert row["jev_provider"] == "opencode" and row["endpoint"] == URL
    assert row["requested_model_id"] == row["resolved_model_id"] == "jev-1.13-free"
    assert row["semantic_model_id"] == "jev-1.13.0" and row["billing_basis"] == "OPENCODE_REPORTED"
    assert row["cost_usd"] is None and row["cost_reason"] == "OpenCode does not report usage.cost"
    missing_fields = row["missing_fields"]
    assert isinstance(missing_fields, dict)
    assert missing_fields["cost_usd"] == row["cost_reason"]


@pytest.mark.asyncio
async def test_opencode_key_selects_paid_model_with_bearer_header() -> None:
    seen: list[httpx.Request] = []

    def fake(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert request.headers["Authorization"] == "Bearer FAKE_OPENCODE_KEY"
        assert json.loads(request.content)["model"] == "jev-1.13"
        return httpx.Response(200, json=response(model="jev-1.13", cost=0.0000042))

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(
            repo,
            settings(opencode_api_key="FAKE_OPENCODE_KEY"),
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
    row = (await repo.list("ops.llm_usage"))[0]
    assert len(seen) == 1 and row["requested_model_id"] == "jev-1.13" and row["cost_usd"] == "0.0000042"


@pytest.mark.asyncio
@pytest.mark.parametrize("resolved", ["jev-1.14", "jev-latest", "jev-1.13", "typesafe/jev-1.13-20260917"])
async def test_resolved_model_cannot_silently_change(resolved: str) -> None:
    repo = MemoryRepository()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response(model=resolved)))
    ) as http:
        with pytest.raises(ValueError, match="unexpected resolved model"):
            await Pipeline(
                repo,
                settings(),
                progress=DisabledSourceProgress(),
                http=http,
                versions=declared_versions_fixture(),
            ).score(ITEM, "relevant", JevState(item=JevItem(source="Fixture", text="data")))
    assert await repo.list("ops.llm_usage") == []


@pytest.mark.asyncio
async def test_filter_item_records_actual_model_provenance_on_first_and_cached_calls() -> None:
    repo = MemoryRepository()
    calls: list[httpx.Request] = []
    await repo.put(
        "ingest.source_items",
        "i",
        source_item_fixture("i", source_id="S30", text="ETH is a crypto asset").storage_record(),
    )

    def fake(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(
            repo,
            settings(),
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        first = await pipeline.filter_item_wire({"item_id": "i"})
        second = await pipeline.filter_item_wire({"item_id": "i"})
    assert len(calls) == 1 and first["relevance"] == second["relevance"]
    relevance = first["relevance"]
    assert isinstance(relevance, dict)
    assert relevance["model"] == relevance["requested_model"] == "jev-1.13-free"
    assert relevance["provider"] == "opencode" and relevance["semantic_model_id"] == "jev-1.13.0"
    usage_id = relevance["usage_id"]
    assert isinstance(usage_id, str)
    stored = await repo.get("ops.llm_usage", usage_id)
    assert stored is not None and stored["model_id"] == relevance["model"]


@pytest.mark.parametrize(
    "status,body", [(429, {"error": {"message": "rate limited"}}), (400, {"error": {"message": "invalid"}})]
)
async def test_provider_errors_are_not_retried_or_chat_wrapped(
    status: int, body: dict[str, JsonValue]
) -> None:
    calls: list[httpx.Request] = []

    def transport(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/inference/systemone/v1/systemone"
        assert "messages" not in json.loads(request.content)
        return httpx.Response(status, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await classify(
                http,
                state=JevState(item=JevItem(source="Fixture", text="data")),
                questions={"relevant": JevQuestion(instructions=FILTER)},
            )
    assert len(calls) == 1


@pytest.mark.parametrize(
    "answer",
    [{"type": "choice", "noul": 0.9}, {"type": "noul", "noul": True}, {"type": "noul", "noul": "0.9"}],
)
async def test_malformed_typed_answer_is_rejected(answer: dict[str, JsonValue]) -> None:
    data = response()
    answers = data["answers"]
    assert isinstance(answers, dict)
    answers["relevant"] = answer
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=data))
    ) as http:
        with pytest.raises(ValueError, match="invalid noul"):
            await classify(
                http,
                state=JevState(item=JevItem(source="Fixture", text="data")),
                questions={"relevant": JevQuestion(instructions=FILTER)},
            )
