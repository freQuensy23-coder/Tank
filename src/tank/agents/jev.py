"""Pinned, typed Jev classification through OpenCode's System One endpoint (03 §3.1)."""

import math
from dataclasses import dataclass

MODEL = "jev-1.13.0"


@dataclass(frozen=True)
class JevBinding:
    provider: str = "opencode"
    base_url: str = "https://opencode.ai/inference/systemone"
    request_model: str = "jev-1.13-free"
    resolved_models: tuple[str, ...] = ("jev-1.13-free",)
    semantic_model: str = MODEL


# Without a key OpenCode serves the free tier; with an OpenCode key, the paid model.
FREE = JevBinding()
KEYED = JevBinding(request_model="jev-1.13", resolved_models=("jev-1.13",))


def binding(api_key=None):
    return KEYED if api_key else FREE


async def classify(http, *, api_key=None, state, questions):
    pinned = binding(api_key)
    response = await http.post(
        pinned.base_url + "/v1/systemone",
        headers={"Authorization": "Bearer " + api_key} if api_key else {},
        json={"model": pinned.request_model, "state": state, "questions": questions},
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or data.get("model") not in pinned.resolved_models:
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
