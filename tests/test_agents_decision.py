"""Offline Decision-1 provider integration (Azure Foundry); no environment credentials or live requests."""

import json
from types import SimpleNamespace

import httpx
import pytest

from tank.agents import Pipeline
from tank.agents.decision import classify
from tank.agents.pipeline import FILTER, FILTER_CRITERIA
from tank.storage import MemoryRepository

URL = "https://alexeym-4644-resource.services.ai.azure.com/providers/microsoft/v1/systemone"


def settings(**values):
    return SimpleNamespace(
        foundry_api_key=values.pop("foundry_api_key", "FAKE_FOUNDRY_KEY"),
        replay=False,
        config_version="test",
        code_version="test",
        load_config=lambda name: {"filter_cutoff": 0.7},
        **values,
    )


def response(model="microsoft-decision-1", **usage):
    return {
        "model": model,
        "answers": {"relevant": {"type": "noul", "noul": 0.97}},
        "usage": {"input_tokens": 100, "output_tokens": 22, **usage},
    }


ITEM = {"item_id": "i", "source": "Issuer", "author": "Issuer", "text": "ETH crypto asset", "quoted": None}


@pytest.mark.asyncio
async def test_api_key_deployment_and_exact_wire_request_are_cached(tmp_path):
    repo = MemoryRepository()
    calls = []

    def fake(request):
        calls.append(request)
        assert str(request.url) == URL and "authorization" not in request.headers
        assert request.headers["api-key"] == "FAKE_FOUNDRY_KEY"
        assert json.loads(request.content) == {
            "model": "decision-1",
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
    assert row["classifier_provider"] == "azure-foundry" and row["endpoint"] == URL
    assert row["requested_model_id"] == "decision-1" and row["resolved_model_id"] == "microsoft-decision-1"
    assert (
        row["semantic_model_id"] == "microsoft-decision-1:1"
        and row["billing_basis"] == "AZURE_FOUNDRY_REPORTED"
    )
    assert row["cost_usd"] is None and row["cost_reason"] == "Azure Foundry does not report usage.cost"
    assert row["missing_fields"]["cost_usd"] == row["cost_reason"]


@pytest.mark.asyncio
async def test_reported_cost_is_preserved_without_model_fallback():
    seen = []

    def fake(request):
        seen.append(request)
        assert request.headers["api-key"] == "FAKE_FOUNDRY_KEY"
        assert json.loads(request.content)["model"] == "decision-1"
        return httpx.Response(200, json=response(model="microsoft-decision-1", cost=0.0000042))

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(repo, settings(foundry_api_key="FAKE_FOUNDRY_KEY"), http=http)
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
    row = (await repo.list("ops.llm_usage"))[0]
    assert len(seen) == 1 and row["requested_model_id"] == "decision-1" and row["cost_usd"] == "0.0000042"


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
    assert relevance["model"] == "microsoft-decision-1" and relevance["requested_model"] == "decision-1"
    assert (
        relevance["provider"] == "azure-foundry"
        and relevance["semantic_model_id"] == "microsoft-decision-1:1"
    )
    assert (await repo.get("ops.llm_usage", relevance["usage_id"]))["model_id"] == relevance["model"]


@pytest.mark.parametrize(
    "status,body", [(429, {"error": {"message": "rate limited"}}), (400, {"error": {"message": "invalid"}})]
)
async def test_provider_errors_are_not_retried_or_chat_wrapped(status, body):
    calls = []

    def transport(request):
        calls.append(request)
        assert request.url.path == "/providers/microsoft/v1/systemone"
        assert "messages" not in json.loads(request.content)
        return httpx.Response(status, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await classify(
                http,
                api_key="FAKE_FOUNDRY_KEY",
                state={"text": "data"},
                questions={"relevant": {"type": "noul", "instructions": FILTER}},
            )
    assert len(calls) == 1


@pytest.mark.parametrize(
    "answer",
    [
        {"type": "choice", "noul": 0.9},
        *(
            {"type": "noul", "noul": value}
            for value in [True, "0.9", -0.1, 1.1, None, float("nan"), float("inf"), -float("inf")]
        ),
    ],
)
async def test_malformed_typed_answer_is_rejected(answer):
    data = response()
    data["answers"]["relevant"] = answer
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=json.dumps(data)))
    ) as http:
        with pytest.raises(ValueError, match="invalid noul"):
            await classify(
                http,
                api_key="FAKE_FOUNDRY_KEY",
                state={"text": "data"},
                questions={"relevant": {"type": "noul", "instructions": FILTER}},
            )


@pytest.mark.parametrize("key", [None, "", " "])
async def test_missing_key_fails_before_network(key):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: pytest.fail("network call"))
    ) as http:
        with pytest.raises(ValueError, match="TANK_FOUNDRY_API_KEY"):
            await classify(http, api_key=key, state={}, questions={})


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://example.com",
        "https://example.com",
        "https://secret@a.services.ai.azure.com",
        "https://a.services.ai.azure.com/path",
        "https://a.services.ai.azure.com?api-key=secret",
    ],
)
def test_endpoint_cannot_send_key_to_another_service(endpoint):
    from tank.agents.decision import binding

    with pytest.raises(ValueError, match="resource origin"):
        binding(SimpleNamespace(foundry_endpoint=endpoint))


def test_cache_identity_includes_resource_deployment_and_new_model():
    from tank.util import sha256

    opts = settings()
    pipeline = Pipeline(MemoryRepository(), opts, http=object())
    state = Pipeline.state(ITEM)
    identity = pipeline.score_identity(ITEM, "relevant", state)
    key, questions = identity[:2]
    old = sha256(
        {
            "item_id": ITEM["item_id"],
            "model": "jev-1.13-free",
            "provider": "opencode",
            "base_url": "https://opencode.ai/inference/systemone",
            "prompt_version": "jev-v2",
            "questions": questions,
            "state_fingerprint": sha256(state),
        }
    )
    assert key != old
    opts.foundry_deployment = "another-decision-deployment"
    assert pipeline.score_identity(ITEM, "relevant", state)[0] != key
    opts.foundry_deployment = "decision-1"
    opts.foundry_endpoint = "https://other.services.ai.azure.com"
    assert pipeline.score_identity(ITEM, "relevant", state)[0] != key


async def test_replay_never_calls_foundry_without_exact_saved_score():
    repo = MemoryRepository()
    opts = settings()
    opts.replay = True
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: pytest.fail("live replay call"))
    ) as http:
        with pytest.raises(ValueError, match="Replay requires stored Decision-1 score"):
            await Pipeline(repo, opts, http=http).score(ITEM, "relevant", Pipeline.state(ITEM))


def test_compose_wires_foundry_and_preserves_old_image_rollback_key():
    from pathlib import Path

    import yaml

    config = yaml.safe_load(Path("compose.yaml").read_text())
    environment = config["services"]["tank"]["environment"]
    assert environment["TANK_FOUNDRY_API_KEY"] == "${TANK_FOUNDRY_API_KEY:?}"
    assert environment["OPENCODE_API_KEY"] == "${OPENCODE_API_KEY:-}"
    assert "TANK_FOUNDRY_API_KEY" not in config["services"]["openma"]["environment"]


async def test_redirect_cannot_forward_the_key_to_another_origin():
    calls = []

    def redirect(request):
        calls.append(request)
        return httpx.Response(307, headers={"Location": "https://other.example/systemone"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(redirect), follow_redirects=True) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await classify(http, api_key="FAKE_FOUNDRY_KEY", state={}, questions={})
    assert len(calls) == 1 and str(calls[0].url) == URL
