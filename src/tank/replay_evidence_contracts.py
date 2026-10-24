"""Immutable typed replay print evidence and explicit manifest serialization."""

from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel


class EvidenceSource(DomainModel):
    source_ref: str
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    instrument_id: str
    origin_kind: Literal["PACKET_PRINTS"]
    available_at: str | None = None

    def to_wire(self) -> dict[str, JsonValue]:
        row: dict[str, JsonValue] = {
            "source_ref": self.source_ref,
            "source_sha256": self.source_sha256,
            "instrument_id": self.instrument_id,
            "origin_kind": self.origin_kind,
        }
        if self.available_at is not None:
            row["available_at"] = self.available_at
        return row


class EvidenceFile(DomainModel):
    relative_path: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    byte_length: int = Field(ge=0)
    instrument_id: str
    row_count: int = Field(ge=0)
    source_ref: str
    source_sha256: str
    source_refs: list[EvidenceSource] = Field(min_length=1)
    guest_path: str
    projection_cutoff: str
    projection_cutoff_prec: str | None
    projection_cutoff_prec_reason: str | None
    projection_cutoff_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @model_validator(mode="after")
    def content_addressed_identity(self) -> Self:
        if self.relative_path != self.sha256 + ".jsonl":
            raise ValueError("Evidence filename must match its content digest")
        if self.guest_path != "/replay/evidence/" + self.relative_path:
            raise ValueError("Evidence guest path must use its sealed relative path")
        if (
            self.source_ref != self.source_refs[0].source_ref
            or self.source_sha256 != self.source_refs[0].source_sha256
        ):
            raise ValueError("Primary evidence provenance must match the first retained source")
        if any(source.instrument_id != self.instrument_id for source in self.source_refs):
            raise ValueError("Evidence source instrument does not match its print file")
        return self

    def to_wire(self) -> dict[str, JsonValue]:
        row: dict[str, JsonValue] = {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "byte_length": self.byte_length,
            "kind": "NORMALIZED_TRADES",
            "instrument_id": self.instrument_id,
            "row_count": self.row_count,
            "source_ref": self.source_ref,
            "source_sha256": self.source_sha256,
            "source_refs": [source.to_wire() for source in self.source_refs],
            "guest_path": self.guest_path,
            "projection_cutoff": self.projection_cutoff,
            "projection_cutoff_prec": self.projection_cutoff_prec,
            "projection_cutoff_prec_reason": self.projection_cutoff_prec_reason,
            "coverage": [],
        }
        if self.projection_cutoff_bucket_anchor is not None:
            row["projection_cutoff_bucket_anchor"] = self.projection_cutoff_bucket_anchor
        return row


class EvidenceManifest(DomainModel):
    run_id: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    input_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cutoff: str
    cutoff_prec: str | None
    cutoff_prec_reason: str | None
    cutoff_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    files: list[EvidenceFile]
    sealed_at: str
    sealed_at_prec: Literal["COLLECTION_ONLY"]
    sealed_at_prec_reason: str | None

    def to_wire(self) -> dict[str, JsonValue]:
        row: dict[str, JsonValue] = {
            "schema": "tank.replay-evidence.v1",
            "run_id": self.run_id,
            "input_sha256": self.input_sha256,
            "cutoff": self.cutoff,
            "cutoff_prec": self.cutoff_prec,
            "cutoff_prec_reason": self.cutoff_prec_reason,
            "guest_root": "/replay/evidence",
            "files": [entry.to_wire() for entry in self.files],
            "identity_key": ["instrument_id", "trade_id"],
            "format": "jsonl",
            "sealed_at": self.sealed_at,
            "sealed_at_prec": self.sealed_at_prec,
            "sealed_at_prec_reason": self.sealed_at_prec_reason,
        }
        if self.cutoff_bucket_anchor is not None:
            row["cutoff_bucket_anchor"] = self.cutoff_bucket_anchor
        return row


class SealedReplayEvidence(DomainModel):
    manifest: EvidenceManifest
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    sealed_root: str

    def to_wire(self) -> dict[str, JsonValue]:
        return {
            "manifest": self.manifest.to_wire(),
            "manifest_sha256": self.manifest_sha256,
            "sealed_root": self.sealed_root,
        }


class PrefixReceipt(DomainModel):
    manifest_sha256: str
    cutoff: str
    cutoff_prec: str | None
    cutoff_prec_reason: str | None
    cutoff_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    def to_wire(self) -> dict[str, JsonValue]:
        row: dict[str, JsonValue] = {
            "manifest_sha256": self.manifest_sha256,
            "cutoff": self.cutoff,
            "cutoff_prec": self.cutoff_prec,
            "cutoff_prec_reason": self.cutoff_prec_reason,
        }
        if self.cutoff_bucket_anchor is not None:
            row["cutoff_bucket_anchor"] = self.cutoff_bucket_anchor
        return row
