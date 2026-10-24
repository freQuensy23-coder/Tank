"""Jev protocol contracts reject malformed decisions before they reach persistence."""

import httpx
import pytest
from pydantic import ValidationError

from tank.agents.jev import JevItem, JevQuestion, JevResponse, JevState, classify


@pytest.mark.parametrize("names", [[], ["other"], ["relevant", "other"]])
async def test_response_answers_match_exact_requested_names(names):
    payload = {
        "model": "jev-1.13-free",
        "answers": {name: {"type": "noul", "noul": 0.9} for name in names},
        "usage": {},
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as client:
        with pytest.raises(ValueError, match="answer names"):
            await classify(
                client,
                state=JevState(item=JevItem(source="Fixture")),
                questions={"relevant": JevQuestion(instructions="Classify")},
            )


@pytest.mark.parametrize(
    "usage",
    [{"input_tokens": True}, {"input_tokens": -1}, {"output_tokens": "3"}, {"cost": -0.1}],
)
def test_usage_refuses_invalid_numeric_fields(usage):
    with pytest.raises(ValidationError):
        JevResponse.model_validate(
            {
                "model": "jev-1.13-free",
                "answers": {"relevant": {"type": "noul", "noul": 0.9}},
                "usage": usage,
            }
        )


def test_omitted_provider_accounting_remains_explicitly_unknown():
    response = JevResponse.model_validate(
        {
            "model": "jev-1.13-free",
            "answers": {"relevant": {"type": "noul", "noul": 0.9}},
            "usage": {},
        }
    )
    assert response.usage.input_tokens is None
    assert response.usage.output_tokens is None
    assert response.usage.cost is None


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -0.1, 1.1, True, "0.9"])
def test_probability_contract_refuses_non_probability_values(value):
    with pytest.raises(ValidationError):
        JevResponse.model_validate(
            {
                "model": "jev-1.13-free",
                "answers": {"relevant": {"type": "noul", "noul": value}},
                "usage": {},
            }
        )
