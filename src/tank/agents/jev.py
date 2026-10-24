"""Pinned, validated Jev classification through OpenCode System One."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

import httpx
from pydantic import Field, JsonValue, ValidationError

from tank.contracts import DomainModel

MODEL = "jev-1.13.0"


class JevItem(DomainModel):
    source: str
    author: str | None = None
    title: str | None = None
    text: str | None = None
    quoted: str | None = None

    def to_wire(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", exclude_none=True)


class JevEvent(DomainModel):
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


class JevState(DomainModel):
    item: JevItem
    event: JevEvent | None = None

    def to_wire(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {"item": self.item.to_wire()}
        if self.event is not None:
            result["event"] = self.event.to_wire()
        return result


class JevQuestion(DomainModel):
    type: Literal["noul"] = "noul"
    instructions: str
    criteria: dict[str, str] | None = None


class JevAnswer(DomainModel):
    type: Literal["noul"]
    noul: float = Field(ge=0, le=1, allow_inf_nan=False)


class JevUsage(DomainModel):
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    cost: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)


class JevResponse(DomainModel):
    model: str
    answers: dict[str, JevAnswer]
    usage: JevUsage
    id: str | None = None
    provider: str | None = None


@dataclass(frozen=True)
class JevBinding:
    provider: str = "opencode"
    base_url: str = "https://opencode.ai/inference/systemone"
    request_model: str = "jev-1.13-free"
    resolved_models: tuple[str, ...] = ("jev-1.13-free",)
    semantic_model: str = MODEL


FREE = JevBinding()
KEYED = JevBinding(request_model="jev-1.13", resolved_models=("jev-1.13",))


def binding(api_key: str | None = None) -> JevBinding:
    return KEYED if api_key else FREE


async def classify(
    http: httpx.AsyncClient,
    *,
    api_key: str | None = None,
    state: JevState,
    questions: dict[str, JevQuestion],
) -> JevResponse:
    pinned = binding(api_key)
    response = await http.post(
        pinned.base_url + "/v1/systemone",
        headers={"Authorization": "Bearer " + api_key} if api_key else {},
        json={
            "model": pinned.request_model,
            "state": state.to_wire(),
            "questions": {
                name: question.model_dump(exclude_none=True) for name, question in questions.items()
            },
        },
    )
    response.raise_for_status()
    try:
        data = JevResponse.model_validate_json(response.content)
    except ValidationError:
        raise ValueError("Jev returned an invalid noul decision response") from None
    if data.model not in pinned.resolved_models:
        raise ValueError("Jev returned an unexpected resolved model for the pinned provider binding")
    if set(data.answers) != set(questions):
        raise ValueError("Jev returned invalid noul answer names")
    return data
