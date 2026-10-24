"""Tool completion, refusal and native response contracts."""

from typing import Literal, Self

from pydantic import JsonValue

from tank.contracts import DomainModel
from tank.util import canonical_json

type CompletedToolStatus = Literal["DONE", "ERROR", "REFUSED"]
type FailureStatus = Literal["ERROR", "REFUSED"]
type FailureCategory = Literal[
    "PROVENANCE", "VALIDATION", "SOURCE_HTTP", "SOURCE_TRANSPORT", "TOOL_EXECUTION"
]


class CapturedToolResult(DomainModel):
    result: JsonValue
    status: CompletedToolStatus

    @property
    def is_error(self) -> bool:
        return self.status != "DONE"

    @classmethod
    def from_record(cls, record: dict[str, JsonValue]) -> Self:
        captured = cls.model_validate({"result": record["result"], "status": record["status"]})
        if "is_error" in record:
            is_error = record["is_error"]
            if not isinstance(is_error, bool) or is_error != captured.is_error:
                raise ValueError("External replay capture has inconsistent outcome metadata")
        return captured

    def signature(self) -> str:
        return canonical_json(self)


class ToolFailure(DomainModel):
    type: str
    reason: str
    category: FailureCategory


class FailedToolResponse(DomainModel):
    ok: Literal[False]
    error: ToolFailure


class NativeText(DomainModel):
    type: Literal["text"]
    text: str


class NativeToolResult(DomainModel):
    type: Literal["user.custom_tool_result"]
    custom_tool_use_id: str
    content: list[NativeText]
    is_error: Literal[True] | None = None
