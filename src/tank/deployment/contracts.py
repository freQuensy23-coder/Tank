"""Closed publisher release, provenance and durable transaction contracts."""

from typing import Annotated, Literal, Self

from pydantic import ConfigDict, Field, RootModel, model_validator

from tank.contracts import DomainModel
from tank.contracts.scalars import TrueFlag
from tank.deployment.runtime import SHA256, CommitSHA

type ActorID = Literal["rbf5CQKQgKNVl4SfO"]
type BuildID = Annotated[str, Field(pattern=r"^[A-Za-z0-9]{17}$")]
type LegacyMode = Literal["legacy", "unused"]
type CompatibilityDigest = Literal[
    "1dcfa5489d2f3622ddcad2d8d45a5515d9734407c4d5a3533e15e3fce6b3ba39",
    "1be5c4cadef74c3bf1dd77c10fde968d068ef1baad6bbb3969836ccd74a54800",
    "431febb05a9303605ba57962214df687363f223b33a0aaaca85e15e9033c0061",
    "0da234b567166f735ae2c1c33e6de74890a0681088d06b640b1edf470d84a36e",
    "67141bdf567e9c73678efb290e3cae04bae0f11bbd5ad2087db8520bd035035a",
]
type BaselineDigest = Literal["a9a53d45f1cdc30634da29a71ae9d6a0a4a5b57e1f60eaf8a443c7cb7568e310"]


class SourceRelease(DomainModel):
    release_sha: CommitSHA
    source_digest: SHA256
    contract: Literal["tank-x-reader-v1"]
    actor_id: ActorID


class HistoricalRelease(DomainModel):
    release_sha: CommitSHA
    source_digest: BaselineDigest
    contract: Literal["tank-x-reader-legacy-0.2.2"]
    actor_id: ActorID
    worker_mode: LegacyMode
    compatibility_digest: CompatibilityDigest
    build_id: Literal["c99YRMlKXube5V0gV"]

    @model_validator(mode="after")
    def bind_compatibility(self) -> Self:
        unused = (
            self.compatibility_digest == "67141bdf567e9c73678efb290e3cae04bae0f11bbd5ad2087db8520bd035035a"
        )
        if unused != (self.worker_mode == "unused"):
            raise ValueError("Historical worker mode does not match its verified compatibility digest")
        return self


type Release = Annotated[SourceRelease | HistoricalRelease, Field(discriminator="contract")]


class ReleaseDocument(RootModel[Release]):
    model_config = ConfigDict(strict=True)


class SourceBuild(SourceRelease):
    build_id: BuildID


class HistoricalBuild(DomainModel):
    actor_id: ActorID
    contract: Literal["tank-x-reader-legacy-0.2.2"]
    source_digest: BaselineDigest
    build_id: Literal["c99YRMlKXube5V0gV"]
    build_number: Literal["0.2.2"]


type BuildProvenance = Annotated[SourceBuild | HistoricalBuild, Field(discriminator="contract")]


class BuildDocument(RootModel[BuildProvenance]):
    model_config = ConfigDict(strict=True)


class ActiveRelease(DomainModel):
    release_sha: CommitSHA
    build_id: BuildID
    build: BuildProvenance

    @model_validator(mode="after")
    def bind_build(self) -> Self:
        if self.build_id != self.build.build_id:
            raise ValueError("Active release build identity differs from its provenance")
        return self


class DeploymentTransaction(DomainModel):
    release_sha: CommitSHA
    previous: BuildID
    previous_active: ActiveRelease | None
    previous_release_sha: CommitSHA | Literal[""]
    candidate: BuildProvenance
    activation_attempted: bool

    @model_validator(mode="after")
    def bind_previous(self) -> Self:
        if self.previous_active is None:
            if self.previous != "c99YRMlKXube5V0gV":
                raise ValueError("An untracked previous build is not the verified baseline")
        elif (
            self.previous_active.build_id != self.previous
            or self.previous_active.release_sha != self.previous_release_sha
        ):
            raise ValueError("Previous worker and Tank release identities disagree")
        return self


class RolledBackTransaction(DeploymentTransaction):
    rolled_back: TrueFlag


type Transaction = DeploymentTransaction | RolledBackTransaction


class TransactionDocument(RootModel[Transaction]):
    model_config = ConfigDict(strict=True)
