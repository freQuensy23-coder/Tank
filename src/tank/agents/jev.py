"""Pinned, typed Jev classification through OpenRouter's System One endpoint (03 §3.1)."""

import math
from dataclasses import dataclass

MODEL = "jev-1.13.0"


@dataclass(frozen=True)
class JevBinding:
    provider: str = "openrouter"
    base_url: str = "https://openrouter.ai/api"
    request_model: str = "typesafe/jev-1.13"
    resolved_models: tuple[str, ...] = ("typesafe/jev-1.13-20260917",)
    semantic_model: str = MODEL


BINDING = JevBinding()


async def classify(http, *, api_key, state, questions):
    response = await http.post(
        BINDING.base_url + "/v1/systemone",
        headers={"Authorization": "Bearer " + api_key},
        json={"model": BINDING.request_model, "state": state, "questions": questions},
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or data.get("model") not in BINDING.resolved_models:
        raise ValueError("Jev returned an unexpected resolved model for the pinned provider binding")
    if not isinstance(data.get("answers"), dict) or not isinstance(data.get("usage"), dict):
        raise TypeError("Jev returned an invalid decision response")
    for name in questions:
        answer = data["answers"].get(name)
        if (
            not isinstance(answer, dict)
            or answer.get("type") != "noul"
            or type(answer.get("noul")) not in (int, float)
            or not math.isfinite(answer["noul"])
            or not 0 <= answer["noul"] <= 1
        ):
            raise ValueError("Jev returned an invalid noul answer")
    return data
