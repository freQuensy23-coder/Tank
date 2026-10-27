"""Named immutable records for exact public-package dependency contexts."""

import base64
import binascii
import re
from typing import Annotated, Literal

from pydantic import AfterValidator, Field

from tank.contracts import DomainModel


def package_name(value: str) -> str:
    if not re.fullmatch(r"(?:@[a-z0-9._-]+/)?[a-z0-9._-]+", value) or value in {".", ".."}:
        raise ValueError("Invalid frozen package name")
    return value


def integrity_value(value: str) -> str:
    if not value.startswith("sha512-"):
        raise ValueError("Frozen archive requires SHA512 integrity")
    try:
        digest = base64.b64decode(value.removeprefix("sha512-"), validate=True)
    except (ValueError, binascii.Error):
        raise ValueError("Invalid frozen archive integrity") from None
    if len(digest) != 64:
        raise ValueError("Invalid frozen archive integrity length")
    return value


type PackageName = Annotated[str, AfterValidator(package_name)]
type Integrity = Annotated[str, AfterValidator(integrity_value)]
type Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
type NonBlank = Annotated[str, Field(min_length=1)]
type Platform = Literal["linux-arm64-gnu", "linux-x64-gnu"]


class RootBinding(DomainModel):
    name: PackageName
    importer: NonBlank
    section: Literal["dependencies", "devDependencies"]
    specifier: NonBlank
    version: NonBlank
    snapshot: NonBlank
    optional_policy: Literal["native-only", "supported"]


class Archive(DomainModel):
    key: NonBlank
    name: PackageName
    version: Annotated[str, Field(pattern=r"^[0-9A-Za-z][0-9A-Za-z.+_-]*$")]
    integrity: Integrity

    @property
    def url(self) -> str:
        return f"https://registry.npmjs.org/{self.name}/-/{self.name.rsplit('/', 1)[-1]}-{self.version}.tgz"

    @property
    def filename(self) -> str:
        return base64.b64decode(self.integrity.removeprefix("sha512-")).hex() + ".tgz"


class Edge(DomainModel):
    name: PackageName
    target: NonBlank
    kind: Literal["dependencies", "optionalDependencies"]


class OmittedEdge(DomainModel):
    name: PackageName
    target: NonBlank
    reason: Literal["UNREQUESTED_OPTIONAL_TEST_FEATURE", "UNSUPPORTED_PLATFORM"]


class Snapshot(DomainModel):
    key: NonBlank
    archive: NonBlank
    context: Digest
    edges: tuple[Edge, ...]
    omitted: tuple[OmittedEdge, ...]


class RootLink(DomainModel):
    name: PackageName
    target: NonBlank
    reason: Literal["DECLARED_IMPORTER", "DECLARED_ROOT_TYPE_SUPPORT"]


class Plan(DomainModel):
    platform: Platform
    snapshots: tuple[Snapshot, ...]
    roots: tuple[RootLink, ...]


class Manifest(DomainModel):
    schema_version: Literal["tank.openma-public-history.dependencies.v1"]
    upstream_lock_sha256: Digest
    bindings: tuple[RootBinding, ...]
    archives: tuple[Archive, ...]
    plans: tuple[Plan, ...]


class Ownership(DomainModel):
    schema_version: Literal["tank.openma-history.owned-setup.v1"]
    manifest_sha256: Digest
    platform: Platform
    uid: Annotated[int, Field(ge=0)]
    purpose: Literal["ARCHIVE_CACHE", "DEPENDENCY_ROOT"]
