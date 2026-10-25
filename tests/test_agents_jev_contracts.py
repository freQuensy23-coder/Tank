"""Decision protocol contracts reject malformed decisions before they reach persistence."""

import httpx
import pytest
from agent_source_fixtures import pipeline_settings_fixture
from pydantic import JsonValue, ValidationError

from tank.agents.decision import (
    DecisionItem,
    DecisionQuestion,
    DecisionResponse,
    DecisionState,
    binding,
    classify,
)


@pytest.mark.parametrize("names", [[], ["other"], ["relevant", "other"]])
async def test_response_answers_match_exact_requested_names(names: list[str]) -> None:
    payload = {
        "model": "microsoft-decision-1",
        "answers": {name: {"type": "noul", "noul": 0.9} for name in names},
        "usage": {},
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as client:
        with pytest.raises(ValueError, match="answer names"):
            await classify(
                client,
                api_key="FAKE_FOUNDRY_KEY",
                pinned=binding(pipeline_settings_fixture({})),
                state=DecisionState(item=DecisionItem(source="Fixture")),
                questions={"relevant": DecisionQuestion(instructions="Classify")},
            )


@pytest.mark.parametrize(
    "usage",
    [{"input_tokens": True}, {"input_tokens": -1}, {"output_tokens": "3"}, {"cost": -0.1}],
)
def test_usage_refuses_invalid_numeric_fields(usage: dict[str, JsonValue]) -> None:
    with pytest.raises(ValidationError):
        DecisionResponse.model_validate(
            {
                "model": "microsoft-decision-1",
                "answers": {"relevant": {"type": "noul", "noul": 0.9}},
                "usage": usage,
            }
        )


def test_omitted_provider_accounting_remains_explicitly_unknown() -> None:
    response = DecisionResponse.model_validate(
        {
            "model": "microsoft-decision-1",
            "answers": {"relevant": {"type": "noul", "noul": 0.9}},
            "usage": {},
        }
    )
    assert response.usage.input_tokens is None
    assert response.usage.output_tokens is None
    assert response.usage.cost is None


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -0.1, 1.1, True, "0.9"])
def test_probability_contract_refuses_non_probability_values(value: JsonValue) -> None:
    with pytest.raises(ValidationError):
        DecisionResponse.model_validate(
            {
                "model": "microsoft-decision-1",
                "answers": {"relevant": {"type": "noul", "noul": value}},
                "usage": {},
            }
        )
