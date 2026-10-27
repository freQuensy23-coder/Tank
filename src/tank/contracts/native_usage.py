"""Consumed native span measurements without inferred totals or billing authority."""

from collections.abc import Mapping
from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp, propagate_timestamp


class NativeServerToolUsage(DomainModel):
    web_search_requests: int | None = Field(default=None, ge=0)
    web_fetch_requests: int | None = Field(default=None, ge=0)


class NativeUsageCounters(DomainModel):
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    cache_read_input_tokens: int | None = Field(default=None, ge=0)
    cache_creation_input_tokens: int | None = Field(default=None, ge=0)
    server_tool_use: NativeServerToolUsage | None = None

    @classmethod
    def from_wire(cls, wire: Mapping[str, JsonValue]) -> Self:
        values = {name: wire[name] for name in cls.model_fields if name in wire}
        if any(value is None for value in values.values()):
            raise ValueError("Reported native usage measurement cannot be null")
        server = values.get("server_tool_use")
        if isinstance(server, dict) and any(value is None for value in server.values()):
            raise ValueError("Reported native tool measurement cannot be null")
        return cls.model_validate(values)


class NativeUsageSpan(DomainModel):
    native_event_id: str = Field(min_length=1, pattern=r"\S")
    phase: Literal["UNREPORTED", "EMPTY", "REPORTED"]
    counters: NativeUsageCounters | None
    raw_usage: dict[str, JsonValue] | None
    reported_model: str | None = Field(min_length=1, pattern=r"\S")
    finish_reason: str | None
    reported_web_searches: int | None = Field(ge=0)
    processed_at: Timestamp

    @model_validator(mode="after")
    def measurement_phase(self) -> Self:
        if self.phase == "REPORTED":
            if self.counters is None or not self.raw_usage:
                raise ValueError("Reported native usage requires its actual measurements")
            if NativeUsageCounters.from_wire(self.raw_usage) != self.counters:
                raise ValueError("Native usage measurements differ from retained evidence")
        elif self.counters is not None:
            raise ValueError("Unreported native usage cannot invent measurements")
        if self.phase == "EMPTY" and self.raw_usage != {}:
            raise ValueError("Empty native usage requires an observed empty object")
        if self.phase == "UNREPORTED" and self.raw_usage is not None:
            raise ValueError("Unreported native usage cannot carry an observed object")
        return self

    @classmethod
    def from_wire(cls, wire: Mapping[str, JsonValue]) -> Self:
        raw = wire.get("model_usage")
        if "model_usage" in wire and raw is None:
            raise ValueError("Reported native usage object cannot be null")
        if raw is not None and not isinstance(raw, dict):
            raise ValueError("Native usage requires a measurement object")
        phase = "UNREPORTED" if raw is None else "REPORTED" if raw else "EMPTY"
        return cls.model_validate(
            {
                "native_event_id": wire["id"],
                "phase": phase,
                "counters": NativeUsageCounters.from_wire(raw) if raw else None,
                "raw_usage": raw,
                "reported_model": wire.get("model"),
                "finish_reason": wire.get("finish_reason"),
                "reported_web_searches": wire.get("web_searches"),
                "processed_at": propagate_timestamp(wire, "processed_at"),
            }
        )

    def web_searches(self) -> int | None:
        if self.counters is not None and self.counters.server_tool_use is not None:
            reported = self.counters.server_tool_use.web_search_requests
            if reported is not None:
                return reported
        return self.reported_web_searches


class NativeUsageSession(DomainModel):
    session_id: str = Field(min_length=1, pattern=r"\S")
    model_id: str | None = Field(min_length=1, pattern=r"\S")
    model_id_basis: str | None
    model_id_reason: str | None
    prompt_version: str | None
    event_id: str | None = Field(min_length=1, pattern=r"\S")
    prompt_version_basis: str | None
    prompt_version_reason: str | None
    prompt_label: str | None
    agent_version: str | None
    effort: str | None
    config_version: str | None
    code_version: str | None

    @classmethod
    def from_record(cls, record: Mapping[str, JsonValue]) -> Self:
        required = {"session_id": record["session_id"]}
        optional = {name: record.get(name) for name in cls.model_fields if name not in required}
        return cls.model_validate({**required, **optional})


class SelectedNativeUsageModel(DomainModel):
    model_id: str = Field(min_length=1, pattern=r"\S")
    basis: Literal["NATIVE_SPAN_MODEL", "RECORDED_SESSION_MODEL"]
    reported_model_id: str | None
    session_model_id: str | None

    @model_validator(mode="after")
    def actual_selected_model_source(self) -> Self:
        if self.basis == "NATIVE_SPAN_MODEL":
            if self.reported_model_id is None or self.model_id != self.reported_model_id:
                raise ValueError("Selected model differs from its actual span report")
        elif (
            self.reported_model_id is not None
            or self.session_model_id is None
            or self.model_id != self.session_model_id
        ):
            raise ValueError("Selected model differs from its actual recorded session model")
        return self


def select_native_usage_model(session: NativeUsageSession, span: NativeUsageSpan) -> SelectedNativeUsageModel:
    session = NativeUsageSession.model_validate(session)
    span = NativeUsageSpan.model_validate(span)
    model = span.reported_model if span.reported_model is not None else session.model_id
    if model is None:
        raise ValueError("Native usage requires an actual reported or recorded session model")
    return SelectedNativeUsageModel(
        model_id=model,
        basis="NATIVE_SPAN_MODEL" if span.reported_model is not None else "RECORDED_SESSION_MODEL",
        reported_model_id=span.reported_model,
        session_model_id=session.model_id,
    )
