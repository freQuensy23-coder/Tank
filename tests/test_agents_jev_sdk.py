"""Official SDK -> injected fake HTTP, with no credentials or paid requests."""

import gzip
import json
from importlib.metadata import version
from types import SimpleNamespace

import httpx
import pytest
from typesafe_sdk import TypeSafeAPIError, TypeSafeAPIResponseValidationError, TypeSafeAPITimeoutError

from tank.agents import Pipeline
from tank.agents.jev import classify
from tank.agents.pipeline import FILTER, ROUTE
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("question", ["relevant", "same_event"])
async def test_actual_pinned_sdk_preserves_exact_pipeline_contract(question, monkeypatch):
    monkeypatch.setenv("TYPESAFE_BASE_URL", "https://wrong.invalid")
    monkeypatch.setenv("TYPESAFE_DEFAULT_MODEL", "unapproved-model")
    repo = MemoryRepository()
    settings = SimpleNamespace(
        typesafe_api_key="unit-only-key", config_version="config", code_version="code", replay=False
    )
    state = {"source": "Official issuer", "author": "@issuer", "text": "Aave announcement", "quoted": None}
    if question == "same_event":
        state["event"] = {
            "statement": "Previously recorded event",
            "entities": ["aave"],
            "tokens": ["aave:AAVE"],
            "stage": "ANNOUNCED",
            "known_to_us_at": "2026-10-17T00:00:00Z",
        }
    expected = {
        "model": "jev-1.13.0",
        "state": state,
        "questions": {
            question: {
                "type": "noul",
                "instructions": FILTER if question == "relevant" else ROUTE,
                "criteria": {
                    "true": "About crypto" if question == "relevant" else "Same event or a development of it",
                    "false": "Not about crypto" if question == "relevant" else "A different event",
                },
            }
        },
    }
    requests = []

    def fake(request):
        requests.append(request)
        assert request.method == "POST" and str(request.url) == "https://api.typesafe.ai/v1/systemone"
        assert request.headers["Authorization"] == "Bearer unit-only-key"
        assert "0.7.5" in request.headers["X-TypeSafe-SDK"]
        assert "content-encoding" not in request.headers
        assert request.extensions["timeout"] == {"connect": 2.5, "read": 2.5, "write": 2.5, "pool": 2.5}
        assert json.loads(request.content) == expected
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "answers": {question: {"type": "noul", "noul": 0.92}},
                "usage": {"input_tokens": 321, "output_tokens": 0},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake), timeout=2.5) as http:
        pipeline = Pipeline(repo, settings, http=http)
        assert await pipeline.score({"item_id": "item"}, question, state) == 0.92
        assert await pipeline.score({"item_id": "item"}, question, state) == 0.92
        assert not http.is_closed
    assert version("typesafe-sdk") == "0.7.5" and len(requests) == 1
    usage = (await repo.list("ops.llm_usage"))[0]
    assert usage["input_tokens"] == 321 and usage["cost_usd"] == "0.000013482"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 500])
async def test_actual_sdk_has_no_additional_retries_or_sleep(status):
    calls = []

    def fake(request):
        calls.append(request)
        return httpx.Response(
            status, headers={"retry-after": "999"}, json={"error": {"message": "Fake transient"}}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        with pytest.raises(TypeSafeAPIError) as error:
            await classify(
                http,
                api_key="unit-only-key",
                state={"text": "data"},
                questions={"q": {"type": "noul", "instructions": "Question?"}},
            )
        assert error.value.status == status
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_sdk_timeout_preserves_single_attempt_and_does_not_expose_key(caplog):
    calls = []

    def fake(request):
        calls.append(request)
        raise httpx.ReadTimeout("Mock timeout", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake), timeout=1.25) as http:
        with pytest.raises(TypeSafeAPITimeoutError):
            await classify(
                http,
                api_key="unit-only-key",
                state={"text": "data"},
                questions={"q": {"type": "noul", "instructions": "Question?"}},
            )
    assert len(calls) == 1 and "unit-only-key" not in caplog.text


@pytest.mark.asyncio
async def test_bridge_does_not_double_decode_compressed_response():
    response = {
        "model": "jev-1.13.0",
        "answers": {"q": {"type": "noul", "noul": 0.8}},
        "usage": {"input_tokens": 10, "output_tokens": 0},
    }

    def fake(request):
        return httpx.Response(
            200,
            headers={"content-type": "application/json", "content-encoding": "gzip"},
            content=gzip.compress(json.dumps(response).encode()),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        result = await classify(
            http,
            api_key="unit-only-key",
            state={"text": "data"},
            questions={"q": {"type": "noul", "instructions": "Question?"}},
        )
        assert result == response


@pytest.mark.asyncio
async def test_actual_sdk_rejects_invalid_response_without_normalizing_it():
    def fake(request):
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "answers": {"q": {"noul": 0.8}},
                "usage": {"input_tokens": 10, "output_tokens": 0},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        with pytest.raises(TypeSafeAPIResponseValidationError):
            await classify(
                http,
                api_key="unit-only-key",
                state={"text": "data"},
                questions={"q": {"type": "noul", "instructions": "Question?"}},
            )


@pytest.mark.asyncio
async def test_previous_prompt_cache_never_serves_new_wire_question(monkeypatch):
    import tank.agents.pipeline as pipeline_module
    from tank.util import sha256

    repo = MemoryRepository()
    settings = SimpleNamespace(
        typesafe_api_key="unit-only-key", config_version="config", code_version="code", replay=False
    )
    state = {"source": "Outlet", "author": "Outlet", "text": "One covered project", "quoted": None}
    old_key = sha256({"item_id": "item", "question": "relevant", "state": state, "model": "jev-1.13.0"})
    old_record = {
        "usage_id": old_key,
        "score": 0.01,
        "model_id": "jev-1.13.0",
        "prompt_version": "jev-v1",
        "questions": {"relevant": {"type": "noul", "instructions": "Is this about crypto?"}},
        "input_tokens": 123,
        "cost_usd": ".000005166",
    }
    await repo.put("ops.llm_usage", old_key, old_record, immutable=True)
    calls = []

    def fake(request):
        body = json.loads(request.content)
        calls.append(body)
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "answers": {"relevant": {"type": "noul", "noul": 0.92}},
                "usage": {"input_tokens": 321, "output_tokens": 0},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(repo, settings, http=http)
        assert await pipeline.score({"item_id": "item"}, "relevant", state) == 0.92
        assert await pipeline.score({"item_id": "item"}, "relevant", dict(state)) == 0.92
        assert len(calls) == 1 and calls[0]["questions"]["relevant"]["instructions"] == FILTER
        # A literal prompt change must create a distinct request, even if its
        # prompt label, item, question name, model and state remain unchanged.
        monkeypatch.setattr(pipeline_module, "FILTER", FILTER + " Additional literal instruction.")
        assert await pipeline.score({"item_id": "item"}, "relevant", state) == 0.92
        assert len(calls) == 2
        assert await pipeline.score({"item_id": "item"}, "relevant", state) == 0.92
        assert len(calls) == 2
    assert await repo.get("ops.llm_usage", old_key) == old_record
    rows = await repo.list("ops.llm_usage")
    assert len(rows) == 3
    current = [row for row in rows if row.get("request_fingerprint")]
    assert {row["state_fingerprint"] for row in current} == {sha256(state)}
    assert len({row["question_fingerprint"] for row in current}) == 2
    assert {row["request_fingerprint"] for row in current} == {sha256(call) for call in calls}
