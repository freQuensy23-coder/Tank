"""Controller file preparation phases, distinct from observed native file identities."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.time_precision import Timestamp

type FileIdentity = Annotated[str, Field(min_length=1, pattern=r"\S")]
type CachedDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class NativeFileSourceScope(DomainModel):
    file_key: FileIdentity
    question_id: FileIdentity
    session_id: FileIdentity
    cached_sha256: CachedDigest
    filename: FileIdentity
    mime: FileIdentity
    size: Annotated[int, Field(ge=0)]


class PreparationClock(DomainModel):
    recorded_at: Timestamp

    @model_validator(mode="after")
    def actual_controller_occurrence(self) -> Self:
        if self.recorded_at.value is None:
            raise ValueError("File preparation requires a recorded controller occurrence")
        return self


class NativeFileNotRequested(PreparationClock):
    phase: Literal["NOT_REQUESTED"]
    scope: NativeFileSourceScope


class NativeUploadIntent(PreparationClock):
    scope: NativeFileSourceScope
    versions: ResolvedVersionIdentity


class NativeUploadUnconfirmed(DomainModel):
    phase: Literal["UPLOAD_UNCONFIRMED"]
    intent: NativeUploadIntent


class NativeFileIdObserved(PreparationClock):
    phase: Literal["FILE_ID_OBSERVED"]
    intent: NativeUploadIntent
    file_id: FileIdentity


class NativeMountIntent(PreparationClock):
    upload: NativeFileIdObserved
    mount_path: FileIdentity

    @model_validator(mode="after")
    def workspace_mount(self) -> Self:
        if not self.mount_path.startswith("/workspace/uploads/") or any(
            part in {".", "..", ""} for part in self.mount_path.split("/")[1:]
        ):
            raise ValueError("File preparation requires a canonical workspace upload path")
        return self


class NativeMountUnconfirmed(DomainModel):
    phase: Literal["MOUNT_UNCONFIRMED"]
    intent: NativeMountIntent


class NativeMountConfirmed(PreparationClock):
    phase: Literal["MOUNT_CONFIRMED"]
    intent: NativeMountIntent
    resource_id: FileIdentity


type NativeFilePreparation = Annotated[
    NativeFileNotRequested
    | NativeUploadUnconfirmed
    | NativeFileIdObserved
    | NativeMountUnconfirmed
    | NativeMountConfirmed,
    Field(discriminator="phase"),
]
