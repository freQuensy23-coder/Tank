"""Pinned Microsoft Decision-1 classification through Azure Foundry."""

from decimal import Decimal
from typing import Literal, Protocol, Self
from urllib.parse import urlsplit

import httpx
from pydantic import Field, JsonValue, ValidationError, model_validator

from tank.contracts import DomainModel
from tank.contracts.json import decode_unique_json

MODEL: Literal["microsoft-decision-1:1"] = "microsoft-decision-1:1"


class DecisionItem(DomainModel):
    source: str
    author: str | None = None
    title: str | None = None
    text: str | None = None
    quoted: str | None = None

    def to_wire(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", exclude_none=True)


class DecisionEvent(DomainModel):
    statement: str
    entities: list[str] = Field(default_factory=list)
    tokens: list[str] = Field(default_factory=list)
    stage: str | None = None

    def to_wire(self) -> dict[str, JsonValue]:
        result = self.model_dump(mode="json", exclude_none=True)
        if not self.entities:
            del result["entities"]
        if not self.tokens:
            del result["tokens"]
        return result


class DecisionState(DomainModel):
    item: DecisionItem
    event: DecisionEvent | None = None

    def to_wire(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {"item": self.item.to_wire()}
        if self.event is not None:
            result["event"] = self.event.to_wire()
        return result


class DecisionQuestion(DomainModel):
    type: Literal["noul"] = "noul"
    instructions: str
    criteria: dict[str, str] | None = None

    def to_wire(self) -> dict[str, JsonValue]:
        fields: dict[str, JsonValue] = {"type": self.type, "instructions": self.instructions}
        if self.criteria is not None:
            criteria: dict[str, JsonValue] = {
                name: description for name, description in self.criteria.items()
            }
            fields["criteria"] = criteria
        return fields


class DecisionAnswer(DomainModel):
    type: Literal["noul"]
    noul: float = Field(ge=0, le=1, allow_inf_nan=False)


class DecisionUsage(DomainModel):
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    cost: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)


class DecisionResponse(DomainModel):
    model: str
    answers: dict[str, DecisionAnswer]
    usage: DecisionUsage
    id: str | None = None
    provider: str | None = None


class DecisionSettings(Protocol):
    @property
    def foundry_endpoint(self) -> str: ...
    @property
    def foundry_deployment(self) -> str: ...


class DecisionBinding(DomainModel):
    provider: Literal["azure-foundry"]
    base_url: str
    request_model: str = Field(min_length=1, pattern=r"\S")
    resolved_models: tuple[Literal["microsoft-decision-1"], ...] = Field(min_length=1)
    semantic_model: Literal["microsoft-decision-1:1"]

    @model_validator(mode="after")
    def configured_origin(self) -> Self:
        suffix = "/providers/microsoft"
        if not self.base_url.endswith(suffix):
            raise ValueError("Foundry binding requires its Microsoft provider path")
        _resource_origin(self.base_url[: -len(suffix)])
        if not self.request_model.strip():
            raise ValueError("Foundry requires a nonempty Decision-1 deployment name")
        return self

    @property
    def endpoint(self) -> str:
        return self.base_url + "/v1/systemone"


def _resource_origin(endpoint: str) -> None:
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


def binding(settings: DecisionSettings) -> DecisionBinding:
    endpoint = settings.foundry_endpoint.rstrip("/")
    _resource_origin(endpoint)
    return DecisionBinding(
        provider="azure-foundry",
        base_url=endpoint + "/providers/microsoft",
        request_model=settings.foundry_deployment,
        resolved_models=("microsoft-decision-1",),
        semantic_model=MODEL,
    )


async def classify(
    http: httpx.AsyncClient,
    *,
    api_key: str | None,
    state: DecisionState,
    questions: dict[str, DecisionQuestion],
    pinned: DecisionBinding,
) -> DecisionResponse:
    if api_key is None or not api_key.strip():
        raise ValueError("Foundry Decision-1 requires TANK_FOUNDRY_API_KEY")
    state = DecisionState.model_validate(state)
    pinned = DecisionBinding.model_validate(pinned)
    if not questions or any(not name.strip() for name in questions):
        raise ValueError("Decision-1 requires a question")
    admitted = {name: DecisionQuestion.model_validate(question) for name, question in questions.items()}
    response = await http.post(
        pinned.endpoint,
        headers={"api-key": api_key},
        follow_redirects=False,
        json={
            "model": pinned.request_model,
            "state": state.to_wire(),
            "questions": {name: question.to_wire() for name, question in admitted.items()},
        },
    )
    response.raise_for_status()
    decode_unique_json(response.content)
    try:
        data = DecisionResponse.model_validate_json(response.content)
    except ValidationError:
        raise ValueError("Decision-1 returned an invalid noul decision response") from None
    if data.model not in pinned.resolved_models:
        raise ValueError("Decision-1 returned an unexpected resolved model for the configured deployment")
    if set(data.answers) != set(admitted):
        raise ValueError("Decision-1 returned invalid noul answer names")
    return data
