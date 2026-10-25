"""Required reaction command port and verified diagnostic assessment capability."""

from typing import Annotated, Literal, Never, Protocol, Self, runtime_checkable

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.util import sha256

type EvidenceDigest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


@runtime_checkable
class ReactionScheduling(Protocol):
    async def ensure_event_reactions(self, event_id: str) -> None: ...


class DiagnosticCall(DomainModel):
    call_id: str = Field(min_length=1)
    tool: str = Field(min_length=1)


class DiagnosticCallCatalog(DomainModel):
    input_sha256: EvidenceDigest
    catalog_sha256: EvidenceDigest
    calls: tuple[DiagnosticCall, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_calls(self) -> Self:
        if len({call.call_id for call in self.calls}) != len(self.calls):
            raise ValueError("Diagnostic catalog repeats a call identity")
        return self

    def declares_assessment(self) -> bool:
        return any(call.tool == "set_assessment" for call in self.calls)


def diagnostic_call_catalog(input_sha256: str, raw: JsonValue) -> DiagnosticCallCatalog:
    if not isinstance(raw, list) or not raw:
        raise ValueError("Diagnostic mode requires its complete captured call catalog")
    calls: list[DiagnosticCall] = []
    for row in raw:
        if not isinstance(row, dict):
            raise ValueError("Diagnostic call catalog entries must be objects")
        calls.append(DiagnosticCall.model_validate({name: row[name] for name in ("call_id", "tool")}))
    return DiagnosticCallCatalog(input_sha256=input_sha256, catalog_sha256=sha256(raw), calls=tuple(calls))


class AssessmentFreeDiagnosticScheduling(DomainModel):
    mode: Literal["ASSESSMENT_FREE_DIAGNOSTIC"]
    catalog: DiagnosticCallCatalog

    @model_validator(mode="after")
    def no_assessment_commands(self) -> Self:
        if self.catalog.declares_assessment():
            raise ValueError("Assessment-free mode cannot contain an assessment command")
        return self

    def refuse_assessment(self) -> Never:
        raise PermissionError("The verified diagnostic catalog does not authorize assessment writes")

    async def ensure_event_reactions(self, event_id: str) -> Never:
        self.refuse_assessment()
