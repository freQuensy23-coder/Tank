"""Typed Microsoft-Decision-1 classification through Azure Foundry."""

import math
from dataclasses import dataclass
from urllib.parse import urlsplit

MODEL = "microsoft-decision-1:1"
DEFAULT_ENDPOINT = "https://alexeym-4644-resource.services.ai.azure.com"
DEFAULT_DEPLOYMENT = "decision-1"


@dataclass(frozen=True)
class DecisionBinding:
    provider: str = "azure-foundry"
    base_url: str = DEFAULT_ENDPOINT + "/providers/microsoft"
    request_model: str = DEFAULT_DEPLOYMENT
    resolved_models: tuple[str, ...] = ("microsoft-decision-1",)
    semantic_model: str = MODEL

    @property
    def endpoint(self) -> str:
        return self.base_url + "/v1/systemone"


def binding(settings=None) -> DecisionBinding:
    endpoint = getattr(settings, "foundry_endpoint", DEFAULT_ENDPOINT).rstrip("/")
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or not parsed.hostname.endswith(".services.ai.azure.com")
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Foundry endpoint must be an HTTPS Azure resource origin")
    deployment = getattr(settings, "foundry_deployment", DEFAULT_DEPLOYMENT)
    if not isinstance(deployment, str) or not deployment.strip():
        raise ValueError("Foundry requires a nonempty Decision-1 deployment name")
    return DecisionBinding(base_url=endpoint + "/providers/microsoft", request_model=deployment)


async def classify(http, *, api_key=None, state, questions, pinned=None):
    if not isinstance(api_key, str) or not api_key.strip():
        raise ValueError("Foundry Decision-1 requires TANK_FOUNDRY_API_KEY")
    pinned = pinned or binding()
    response = await http.post(
        pinned.endpoint,
        headers={"api-key": api_key},
        follow_redirects=False,
        json={"model": pinned.request_model, "state": state, "questions": questions},
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or data.get("model") not in pinned.resolved_models:
        raise ValueError("Decision-1 returned an unexpected resolved model for the configured deployment")
    if not isinstance(data.get("answers"), dict) or not isinstance(data.get("usage"), dict):
        raise TypeError("Decision-1 returned an invalid decision response")
    for name in questions:
        answer = data["answers"].get(name)
        if (
            not isinstance(answer, dict)
            or answer.get("type") != "noul"
            or type(answer.get("noul")) not in (int, float)
            or not math.isfinite(answer["noul"])
            or not 0 <= answer["noul"] <= 1
        ):
            raise ValueError("Decision-1 returned an invalid noul answer")
    return data
