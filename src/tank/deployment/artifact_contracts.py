"""Pure selected-source and publisher-runtime artifact contracts."""

import hashlib
import json
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import OneInt
from tank.deployment.api_contracts import ApifyProjection, SourcePath
from tank.deployment.contracts import HistoricalRelease, SourceRelease
from tank.deployment.runtime_contracts import RuntimeEvidence

type ImageRepository = Annotated[
    str,
    Field(
        pattern=r"^ghcr\.io/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*/tank$"
    ),
]


class ActorDefinition(ApifyProjection):
    actorSpecification: OneInt
    name: str = Field(min_length=1)

    @model_validator(mode="before")
    @classmethod
    def refuse_build_tag(cls, value: JsonValue) -> JsonValue:
        if isinstance(value, dict) and "buildTag" in value:
            raise ValueError("Worker definition must not activate a build tag")
        return value


class SourceBundle(DomainModel):
    files: dict[SourcePath, bytes]

    def digest(self) -> str:
        hashes = {name: hashlib.sha256(body).hexdigest() for name, body in self.files.items()}
        document = json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(document).hexdigest()


class PublisherArtifactRuntime(DomainModel):
    contract: Literal["tank-publisher-runtime-v1"]
    image_repository: ImageRepository
    runtime: RuntimeEvidence

    @property
    def image_reference(self) -> str:
        return self.image_repository + ":" + self.runtime.orchestrator_sha


class PackageRequest(DomainModel):
    repository: Path
    destination: Path
    publisher: PublisherArtifactRuntime


class SourceArtifact(DomainModel):
    release: SourceRelease
    bundle: SourceBundle

    @model_validator(mode="after")
    def bind_source(self) -> Self:
        if self.bundle.digest() != self.release.source_digest:
            raise ValueError("Selected source bytes do not match release provenance")
        return self


class HistoricalArtifact(DomainModel):
    release: HistoricalRelease


type SelectedArtifact = SourceArtifact | HistoricalArtifact
