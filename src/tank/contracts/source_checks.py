"""Current source checks distinguish saved evidence from observed HTTP probes."""

from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.source_errors import SourceEntryError
from tank.contracts.source_items import SourceContextId
from tank.contracts.sources import AccountProbeFailure, AccountProbeResponse, FeedKind
from tank.time_precision import Timestamp


class SourceCheckpointAddress(DomainModel):
    source_id: SourceContextId
    feed_index: int = Field(ge=0)

    @property
    def key(self) -> str:
        return f"{self.source_id}:{self.feed_index}"


class SavedSourceCheckpoint(DomainModel):
    """A read at an explicit address; the document is original administrative wire evidence."""

    address: SourceCheckpointAddress
    document: dict[str, JsonValue] | None

    @property
    def health_status(self) -> Literal["HEALTHY", "DEGRADED"] | None:
        if self.document is None or self.document.get("source_id") != self.address.source_id:
            return None
        match self.document.get("status"):
            case "HEALTHY":
                return "HEALTHY"
            case "DEGRADED":
                return "DEGRADED"
            case _:
                return None


class SavedSourceHealth(DomainModel):
    """Aggregate saved owned verdicts without claiming a current endpoint observation."""

    source_id: SourceContextId
    configured: bool
    checkpoints: list[SavedSourceCheckpoint]

    @model_validator(mode="after")
    def actual_requested_addresses(self) -> Self:
        if self.configured != bool(self.checkpoints):
            raise ValueError("Saved health configuration conflicts with its requested feed addresses")
        for index, checkpoint in enumerate(self.checkpoints):
            if checkpoint.address.source_id != self.source_id or checkpoint.address.feed_index != index:
                raise ValueError("Saved health contains another requested source/feed address")
        return self

    @property
    def status(self) -> Literal["DEGRADED", "HEALTHY", "UNVERIFIED"]:
        statuses = [checkpoint.health_status for checkpoint in self.checkpoints]
        if "DEGRADED" in statuses:
            return "DEGRADED"
        if statuses and all(status == "HEALTHY" for status in statuses):
            return "HEALTHY"
        return "UNVERIFIED"


class SourceProbeContext(DomainModel):
    source_id: SourceContextId
    feed_index: int = Field(ge=0)
    url: str = Field(min_length=1)
    kind: FeedKind
    started_at: Timestamp
    versions: ResolvedVersionIdentity

    @model_validator(mode="after")
    def actual_start_clock(self) -> Self:
        if self.started_at.value is None or self.started_at.precision != "COLLECTION_ONLY":
            raise ValueError("Source probe requires its actual collection start clock")
        return self


class SourceHttpProbeContext(SourceProbeContext):
    method: Literal["GET", "POST"]


class SourceProbeResponse(DomainModel):
    http_status: int = Field(ge=100, le=599)
    response_content_type: str | None
    payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    response_bytes: int = Field(ge=0)


class SourceProbeObservation(DomainModel):
    context: SourceProbeContext
    finished_at: Timestamp

    @model_validator(mode="after")
    def actual_finish_clock(self) -> Self:
        if self.finished_at.value is None or self.finished_at.precision != "COLLECTION_ONLY":
            raise ValueError("Source probe requires its actual collection finish clock")
        return self


class SourceProbeUnavailable(SourceProbeObservation):
    status: Literal["UNAVAILABLE"]
    item_count: None
    reason: str = Field(min_length=1)


class SourceProbeSuccess(SourceProbeObservation):
    context: SourceHttpProbeContext
    response: SourceProbeResponse
    status: Literal["PASS", "PASS_EMPTY"]
    item_count: int = Field(ge=0)
    validation: str = Field(min_length=1)

    @model_validator(mode="after")
    def actual_item_count(self) -> Self:
        if not 200 <= self.response.http_status < 300:
            raise ValueError("Successful source probe requires its successful HTTP response")
        if (self.status == "PASS_EMPTY") != (self.item_count == 0):
            raise ValueError("Probe success status conflicts with its actual parsed count")
        return self


class SourceProbeFailure(SourceProbeObservation):
    context: SourceHttpProbeContext
    response: SourceProbeResponse | None
    status: Literal["HTTP_ERROR", "NETWORK_ERROR", "PARSE_ERROR"]
    item_count: int | None = Field(ge=0)
    parser_entry_errors: list[SourceEntryError] | None = Field(min_length=1)
    reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def actual_failure_evidence(self) -> Self:
        if self.status == "HTTP_ERROR" and (self.response is None or 200 <= self.response.http_status < 300):
            raise ValueError("HTTP probe failure requires its actual unsuccessful response")
        if self.parser_entry_errors is not None and (
            self.status != "PARSE_ERROR" or self.item_count is None or self.response is None
        ):
            raise ValueError("Parser entry errors require the actual parsed-response observation")
        return self


type SourceProbeResult = SourceProbeUnavailable | SourceProbeSuccess | SourceProbeFailure


def probe_record(result: SourceProbeResult) -> dict[str, JsonValue]:
    """Serialize only actually observed fields in the existing administrative wire shape."""
    context = result.context
    wire: dict[str, JsonValue] = {
        "source_id": context.source_id,
        "feed_index": context.feed_index,
        "url": context.url,
        "kind": context.kind,
        "config_version": context.versions.config_version,
        "code_version": context.versions.code_version,
        **context.started_at.fields("started_at"),
        **result.finished_at.fields("finished_at"),
        "status": result.status,
        "item_count": result.item_count,
    }
    if isinstance(result, SourceProbeUnavailable):
        wire["reason"] = result.reason
        return wire
    wire["method"] = result.context.method
    response = result.response
    if response is not None:
        wire.update(response.model_dump(mode="json"))
    if isinstance(result, SourceProbeSuccess):
        wire["validation"] = result.validation
    else:
        wire["reason"] = result.reason
        if result.parser_entry_errors is not None:
            wire["parser_entry_errors"] = [
                error.model_dump(mode="json", exclude_unset=True) for error in result.parser_entry_errors
            ]
    return wire


type SourceCheckResult = SavedSourceHealth | SourceProbeResult | AccountProbeResponse | AccountProbeFailure


def source_check_record(result: SourceCheckResult) -> dict[str, JsonValue]:
    """Project the closed check result at its administrative output boundary."""
    if isinstance(result, (SourceProbeUnavailable, SourceProbeSuccess, SourceProbeFailure)):
        return probe_record(result)
    if isinstance(result, SavedSourceHealth):
        return {
            "source_id": result.source_id,
            "configured": result.configured,
            "states": [checkpoint.document for checkpoint in result.checkpoints],
            "status": result.status,
        }
    return result.model_dump(mode="json")
