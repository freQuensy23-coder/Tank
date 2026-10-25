"""Exact Tank executable-input requests and native acknowledgment observations."""

from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.agents.tool_contracts import NativeText, NativeToolResult
from tank.contracts import DomainModel


class NativeUserMessage(DomainModel):
    type: Literal["user.message"]
    content: list[NativeText]


type NativeInput = Annotated[NativeUserMessage | NativeToolResult, Field(discriminator="type")]


class NativeInputRequest(DomainModel):
    events: list[NativeInput] = Field(min_length=1, max_length=1)

    @model_validator(mode="after")
    def real_tool_identity(self) -> Self:
        event = self.events[0]
        if isinstance(event, NativeToolResult) and not event.custom_tool_use_id:
            raise ValueError("Native tool results require their actual tool call identity")
        return self


class NativeSentUserMessage(NativeUserMessage):
    id: str = Field(min_length=1)
    processed_at: str | None = None


class NativeSentToolResult(DomainModel):
    id: str = Field(min_length=1)
    type: Literal["user.custom_tool_result"]
    custom_tool_use_id: str = Field(min_length=1)
    content: list[NativeText] | None = None
    is_error: bool | None = None
    session_thread_id: str | None = None
    processed_at: str | None = None

    @model_validator(mode="after")
    def absent_content_is_not_null(self) -> Self:
        if "content" in self.model_fields_set and self.content is None:
            raise ValueError("Native tool-result content can be omitted but cannot be null")
        return self


type NativeSentInput = Annotated[NativeSentUserMessage | NativeSentToolResult, Field(discriminator="type")]


class NativeInputResponse(DomainModel):
    data: list[NativeSentInput] | None = None

    @classmethod
    def from_wire(cls, payload: dict[str, JsonValue]) -> Self:
        if "data" in payload and payload["data"] is None:
            raise ValueError("Native input acknowledgment data cannot be null")
        return cls.model_validate(payload)


class AcceptedNativeInput(DomainModel):
    status: Literal["ACCEPTED"]
    event_id: str = Field(min_length=1)


class UnreportedNativeInput(DomainModel):
    status: Literal["UNREPORTED", "EMPTY"]


type NativeInputAcknowledgment = AcceptedNativeInput | UnreportedNativeInput


def parse_native_input(payload: dict[str, JsonValue]) -> NativeInput:
    return NativeInputRequest.model_validate({"events": [payload]}).events[0]


def observe_native_input(payload: dict[str, JsonValue], expected: NativeInput) -> NativeInputAcknowledgment:
    response = NativeInputResponse.from_wire(payload)
    if response.data is None:
        return UnreportedNativeInput(status="UNREPORTED")
    if not response.data:
        return UnreportedNativeInput(status="EMPTY")
    if len(response.data) != 1:
        raise ValueError("One executable input requires one reported native event")
    observed = response.data[0]
    if observed.type != expected.type:
        raise ValueError("Native acknowledgment does not match the posted input type")
    if isinstance(expected, NativeToolResult):
        if not isinstance(observed, NativeSentToolResult):
            raise ValueError("Native tool-result acknowledgment has the wrong event type")
        if observed.custom_tool_use_id != expected.custom_tool_use_id:
            raise ValueError("Native acknowledgment does not match the posted tool call")
    return AcceptedNativeInput(status="ACCEPTED", event_id=observed.id)
