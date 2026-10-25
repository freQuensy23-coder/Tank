"""Exact retained transport decoding and verified historical source locations."""

import gzip
import io
from pathlib import Path
from typing import Annotated, Literal, Protocol, Self

import zstandard
from pydantic import Field, model_validator

from tank.contracts import DomainModel
from tank.util import sha256

type SourceDigest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class RetainedSourceReader(Protocol):
    def read_payload(self, original_path: str) -> bytes: ...


def decode_retained_transport(data: bytes) -> bytes:
    if data.startswith(b"\x28\xb5\x2f\xfd"):
        with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as stream:
            return stream.read()
    if data.startswith(b"\x1f\x8b"):
        return gzip.decompress(data)
    return data


class FilesystemRetainedSources:
    __slots__ = ()

    def read_payload(self, original_path: str) -> bytes:
        return decode_retained_transport(Path(original_path).read_bytes())


FILESYSTEM_RETAINED_SOURCES = FilesystemRetainedSources()


class RetainedInputOrigin(DomainModel):
    fixture_id: str = Field(pattern=r"^F-\d{2}$")
    packet_path: str = Field(min_length=1)
    compressed_sha256: SourceDigest
    field_path: str = Field(min_length=1)


class RequiredRetainedRaw(DomainModel):
    original_path: str = Field(min_length=1)
    expected_payload_sha256: SourceDigest
    hash_basis: Literal["DECOMPRESSED_RETAINED_SOURCE_BYTES"]
    origins: list[RetainedInputOrigin] = Field(min_length=1)


class RequiredRetainedInventory(DomainModel):
    verified_input_packets: int = Field(gt=0)
    excluded_fixture: Literal["F-02_MANIFEST_MISMATCH"]
    raw_inputs: list[RequiredRetainedRaw] = Field(min_length=1)


class StagedRetainedRaw(DomainModel):
    original_path: str = Field(min_length=1)
    expected_payload_sha256: SourceDigest
    source_candidate: str = Field(min_length=1)
    actual_payload_sha256: SourceDigest
    transport_sha256: SourceDigest
    transport_bytes: int = Field(ge=0)
    payload_bytes: int = Field(ge=0)
    staged_path: Path
    status: Literal["verified_staged"]

    @model_validator(mode="after")
    def captured_hash(self) -> Self:
        if self.actual_payload_sha256 != self.expected_payload_sha256:
            raise ValueError("Staged source payload differs from its captured digest")
        return self


class StagedRetainedInventory(DomainModel):
    scope: str = Field(min_length=1)
    input_manifest: str = Field(min_length=1)
    owner_manifest_sha256: SourceDigest
    hash_basis: Literal["DECOMPRESSED_RETAINED_SOURCE_BYTES"]
    entries: list[StagedRetainedRaw] = Field(min_length=1)
    counts: dict[Literal["verified_staged"], int]

    @model_validator(mode="after")
    def counted_entries(self) -> Self:
        if self.counts != {"verified_staged": len(self.entries)}:
            raise ValueError("Staged source manifest count differs from its entries")
        return self


class VerifiedRetainedSources:
    def __init__(self, required_manifest: Path, staged_manifest: Path, *, staged_root: Path) -> None:
        owner_bytes = required_manifest.read_bytes()
        required = RequiredRetainedInventory.model_validate_json(owner_bytes)
        staged = StagedRetainedInventory.model_validate_json(staged_manifest.read_bytes())
        if staged.owner_manifest_sha256 != sha256(owner_bytes):
            raise ValueError("Staged source manifest is not bound to the required input inventory")
        expected = {(row.original_path, row.expected_payload_sha256) for row in required.raw_inputs}
        actual = {(row.original_path, row.expected_payload_sha256) for row in staged.entries}
        if expected != actual or len(staged.entries) != len(actual):
            raise ValueError("Staged sources differ from the exact required reference set")
        if len({path for path, _ in expected}) != len(expected):
            raise ValueError("Historical source path has conflicting captured payloads")
        self._root = staged_root.resolve(strict=True)
        self._entries = {row.original_path: row for row in staged.entries}
        for path in self._entries:
            self.read_payload(path)

    def read_payload(self, original_path: str) -> bytes:
        try:
            entry = self._entries[original_path]
        except KeyError:
            raise ValueError("Historical source is not listed in the verified input inventory") from None
        path = entry.staged_path.resolve(strict=True)
        if not path.is_relative_to(self._root):
            raise ValueError("Historical staged source escapes its declared root")
        data = path.read_bytes()
        if len(data) != entry.transport_bytes or sha256(data) != entry.transport_sha256:
            raise ValueError("Historical staged transport bytes differ from their verified artifact")
        payload = decode_retained_transport(data)
        if len(payload) != entry.payload_bytes or sha256(payload) != entry.expected_payload_sha256:
            raise ValueError("Historical source payload differs from its captured digest")
        return payload
