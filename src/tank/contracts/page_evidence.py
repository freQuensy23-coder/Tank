"""Page bytes and truthful live or verified recorded-read provenance."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp
from tank.util import sha256

type PageDigest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class LivePageProvenance(DomainModel):
    mode: Literal["LIVE_RESPONSE"]
    received_at: Timestamp


class RecordedBodyProvenance(DomainModel):
    mode: Literal["VERIFIED_RECORDED_BODY"]
    input_sha256: PageDigest
    record_id: str = Field(min_length=1)
    captured_body_sha256: PageDigest


type PageReadProvenance = LivePageProvenance | RecordedBodyProvenance


class PageRead(DomainModel):
    body: bytes = Field(max_length=2 * 1024 * 1024)
    body_sha256: PageDigest
    media_type: str | None
    provenance: PageReadProvenance

    @model_validator(mode="after")
    def actual_bytes(self) -> Self:
        if self.body_sha256 != sha256(self.body):
            raise ValueError("Page digest differs from its actual read bytes")
        if (
            isinstance(self.provenance, RecordedBodyProvenance)
            and self.provenance.captured_body_sha256 != self.body_sha256
        ):
            raise ValueError("Recorded body differs from its captured digest")
        return self
