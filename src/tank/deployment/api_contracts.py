"""Consumed Apify HTTP projections and exact source-file upload records."""

import base64
from pathlib import PurePosixPath
from typing import Annotated, Literal, Self

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, JsonValue, RootModel, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import FalseFlag, TrueFlag
from tank.deployment.contracts import ActorID, BuildID


def source_path(value: str) -> str:
    path = PurePosixPath(value)
    if not value or "\x00" in value or path.is_absolute() or ".." in path.parts:
        raise ValueError("Invalid build source path")
    return value


type SourcePath = Annotated[str, AfterValidator(source_path)]


class ApifyProjection(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="ignore",
        frozen=True,
        allow_inf_nan=False,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class SourceFolder(ApifyProjection):
    name: SourcePath
    folder: TrueFlag


class FileSourceProjection(ApifyProjection):
    @model_validator(mode="before")
    @classmethod
    def validate_file_marker(cls, value: JsonValue) -> JsonValue:
        if isinstance(value, dict) and "folder" in value and value["folder"] is not False:
            raise ValueError("A source file cannot carry a non-false folder marker")
        return value


class TextSourceFile(FileSourceProjection):
    name: SourcePath
    format: Literal["TEXT"]
    content: str

    def body(self) -> bytes:
        return self.content.encode("utf-8")


class Base64SourceFile(FileSourceProjection):
    name: SourcePath
    format: Literal["BASE64"]
    content: str

    @model_validator(mode="after")
    def validate_content(self) -> Self:
        base64.b64decode(self.content, validate=True)
        return self

    def body(self) -> bytes:
        return base64.b64decode(self.content, validate=True)


type SourceEntry = SourceFolder | TextSourceFile | Base64SourceFile


class SourceEntries(RootModel[list[SourceEntry]]):
    model_config = ConfigDict(strict=True, hide_input_in_errors=True)

    def decoded(self) -> dict[str, bytes]:
        files: dict[str, bytes] = {}
        for entry in self.root:
            if isinstance(entry, SourceFolder):
                continue
            if entry.name in files:
                raise ValueError("Duplicate build source path")
            files[entry.name] = entry.body()
        return files


class SourceSnapshot(ApifyProjection):
    sourceType: Literal["SOURCE_FILES"]
    sourceFiles: list[SourceEntry]

    def decoded(self) -> dict[str, bytes]:
        return SourceEntries(self.sourceFiles).decoded()


class HistoricalSourceSnapshot(SourceSnapshot):
    versionNumber: Literal["0.2"]
    buildTag: Literal["direct"]


class DirectTag(ApifyProjection):
    buildId: BuildID


class ActorTags(ApifyProjection):
    direct: DirectTag


class VersionSummary(ApifyProjection):
    versionNumber: str = Field(min_length=1)


class ActorSnapshot(ApifyProjection):
    id: ActorID
    taggedBuilds: ActorTags
    versions: list[VersionSummary]


class BuildObservation(ApifyProjection):
    id: BuildID
    status: str = Field(min_length=1)


class CompletedBuildSnapshot(ApifyProjection):
    id: BuildID
    actId: ActorID
    status: Literal["SUCCEEDED"]
    actVersion: SourceSnapshot


class HistoricalBuildSnapshot(ApifyProjection):
    id: Literal["c99YRMlKXube5V0gV"]
    actId: ActorID
    status: Literal["SUCCEEDED"]
    buildNumber: Literal["0.2.2"]
    actVersion: HistoricalSourceSnapshot


class SourceUpload(DomainModel):
    name: SourcePath
    format: Literal["BASE64"]
    content: str

    @model_validator(mode="after")
    def validate_content(self) -> Self:
        base64.b64decode(self.content, validate=True)
        return self


class VersionUpload(DomainModel):
    versionNumber: Literal["0.3"]
    sourceType: Literal["SOURCE_FILES"]
    buildTag: None
    envVars: Annotated[list[str], Field(max_length=0)]
    applyEnvVarsToBuild: FalseFlag
    sourceFiles: list[SourceUpload]


class UploadedVersion(SourceSnapshot):
    buildTag: None
    envVars: Annotated[list[str], Field(max_length=0)]
    applyEnvVarsToBuild: FalseFlag


class DirectTagUpdate(DomainModel):
    buildId: BuildID


class DirectTagsUpdate(DomainModel):
    direct: DirectTagUpdate


class ActorTagUpdate(DomainModel):
    taggedBuilds: DirectTagsUpdate


class ProviderErrorDetail(ApifyProjection):
    type: str | None = None
    message: str | None = None


class ProviderErrorEnvelope(ApifyProjection):
    error: ProviderErrorDetail | None = None


class ReportedAPIReason(DomainModel):
    state: Literal["REPORTED"]
    text: Annotated[str, Field(min_length=1, max_length=300)]


class UnreportedAPIReason(DomainModel):
    state: Literal["UNREPORTED"]
    reason: Literal["EMPTY_OR_MISSING", "INVALID_JSON_OR_FIELDS", "READ_FAILED"]


type APIFailureReason = ReportedAPIReason | UnreportedAPIReason
