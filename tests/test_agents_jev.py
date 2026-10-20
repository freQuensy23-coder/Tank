"""Offline Jev provider integration (OpenCode System One); no environment credentials or live requests."""

import json
from types import SimpleNamespace

import httpx
import pytest

from tank.agents import Pipeline
from tank.agents.jev import classify
from tank.agents.pipeline import FILTER, FILTER_CRITERIA
from tank.storage import MemoryRepository

URL = "https://opencode.ai/inference/systemone/v1/systemone"


def settings(**values):
    return SimpleNamespace(
        replay=False,
        config_version="test",
        code_version="test",
        load_config=lambda name: {"filter_cutoff": 0.7},
        **values,
    )


def response(model="jev-1.13-free", **usage):
    return {
        "model": model,
        "answers": {"relevant": {"type": "noul", "noul": 0.97}},
        "usage": {"input_tokens": 100, "output_tokens": 22, **usage},
    }


ITEM = {"item_id": "i", "source": "Issuer", "author": "Issuer", "text": "ETH crypto asset", "quoted": None}


@pytest.mark.asyncio
async def test_free_tier_without_key_sends_no_authorization_and_caches(tmp_path):
    repo = MemoryRepository()
    calls = []

    def fake(request):
        calls.append(request)
        assert str(request.url) == URL and "authorization" not in request.headers
        assert json.loads(request.content) == {
            "model": "jev-1.13-free",
            "state": Pipeline.state(ITEM),
            "questions": {"relevant": {"type": "noul", "instructions": FILTER, "criteria": FILTER_CRITERIA}},
        }
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(repo, settings(), http=http)
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
    assert len(calls) == 1
    row = (await repo.list("ops.llm_usage"))[0]
    assert row["at"] == row["created_at"] and row["at_prec"] == "COLLECTION_ONLY"
    assert row["jev_provider"] == "opencode" and row["endpoint"] == URL
    assert row["requested_model_id"] == row["resolved_model_id"] == "jev-1.13-free"
    assert row["semantic_model_id"] == "jev-1.13.0" and row["billing_basis"] == "OPENCODE_REPORTED"
    assert row["cost_usd"] is None and row["cost_reason"] == "OpenCode does not report usage.cost"
    assert row["missing_fields"]["cost_usd"] == row["cost_reason"]


@pytest.mark.asyncio
async def test_opencode_key_selects_paid_model_with_bearer_header():
    seen = []

    def fake(request):
        seen.append(request)
        assert request.headers["Authorization"] == "Bearer FAKE_OPENCODE_KEY"
        assert json.loads(request.content)["model"] == "jev-1.13"
        return httpx.Response(200, json=response(model="jev-1.13", cost=0.0000042))

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(repo, settings(opencode_api_key="FAKE_OPENCODE_KEY"), http=http)
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
    row = (await repo.list("ops.llm_usage"))[0]
    assert len(seen) == 1 and row["requested_model_id"] == "jev-1.13" and row["cost_usd"] == "0.0000042"


@pytest.mark.asyncio
@pytest.mark.parametrize("resolved", ["jev-1.14", "jev-latest", "jev-1.13", "typesafe/jev-1.13-20260917"])
async def test_resolved_model_cannot_silently_change(resolved):
    repo = MemoryRepository()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response(model=resolved)))
    ) as http:
        with pytest.raises(ValueError, match="unexpected resolved model"):
            await Pipeline(repo, settings(), http=http).score({"item_id": "i"}, "relevant", {"text": "data"})
    assert await repo.list("ops.llm_usage") == []


@pytest.mark.asyncio
async def test_filter_item_records_actual_model_provenance_on_first_and_cached_calls():
    repo = MemoryRepository()
    calls = []
    await repo.put(
        "ingest.source_items", "i", {"item_id": "i", "source_id": "S30", "text": "ETH is a crypto asset"}
    )

    def fake(request):
        calls.append(request)
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(repo, settings(), http=http)
        first = await pipeline.filter_item({"item_id": "i"})
        second = await pipeline.filter_item({"item_id": "i"})
    assert len(calls) == 1 and first["relevance"] == second["relevance"]
    relevance = first["relevance"]
    assert relevance["model"] == relevance["requested_model"] == "jev-1.13-free"
    assert relevance["provider"] == "opencode" and relevance["semantic_model_id"] == "jev-1.13.0"
    assert (await repo.get("ops.llm_usage", relevance["usage_id"]))["model_id"] == relevance["model"]


@pytest.mark.parametrize(
    "status,body", [(429, {"error": {"message": "rate limited"}}), (400, {"error": {"message": "invalid"}})]
)
async def test_provider_errors_are_not_retried_or_chat_wrapped(status, body):
    calls = []

    def transport(request):
        calls.append(request)
        assert request.url.path == "/inference/systemone/v1/systemone"
        assert "messages" not in json.loads(request.content)
        return httpx.Response(status, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await classify(
                http, state={"text": "data"}, questions={"relevant": {"type": "noul", "instructions": FILTER}}
            )
    assert len(calls) == 1


@pytest.mark.parametrize(
    "answer",
    [{"type": "choice", "noul": 0.9}, {"type": "noul", "noul": True}, {"type": "noul", "noul": "0.9"}],
)
async def test_malformed_typed_answer_is_rejected(answer):
    data = response()
    data["answers"]["relevant"] = answer
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=data))
    ) as http:
        with pytest.raises(ValueError, match="invalid noul"):
            await classify(
                http, state={"text": "data"}, questions={"relevant": {"type": "noul", "instructions": FILTER}}
            )
