"""Actual HTTP, artifact and journal resources for publisher transition controls."""

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from publisher_http_support import PublisherHTTPPeer, publisher_http_peer
from publisher_state_support import Fault, ProviderState, rig
from pydantic import SecretStr

from tank.deployment.apify_api import ApifyAPI
from tank.deployment.artifact_contracts import SourceArtifact
from tank.deployment.artifacts import FilesystemArtifacts
from tank.deployment.journal import PublisherJournal
from tank.deployment.markers import FilesystemTankReleaseState
from tank.deployment.publisher import Publisher


@dataclass(frozen=True)
class HTTPPublisherRig:
    publisher: Publisher
    api: ProviderState
    journal: PublisherJournal
    marker: FilesystemTankReleaseState
    artifact: Path
    selected: SourceArtifact
    previous: SourceArtifact
    peer: PublisherHTTPPeer


def write_artifact(path: Path, artifact: SourceArtifact) -> None:
    path.mkdir()
    for name, body in artifact.bundle.files.items():
        target = path / "worker" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    (path / "manifest.json").write_text(artifact.release.model_dump_json())


@contextmanager
def http_rig(root: Path, *, fault: Fault) -> Iterator[HTTPPublisherRig]:
    fixture = rig(root, fault=fault)
    artifact = root / "release"
    write_artifact(artifact, fixture.selected)
    with publisher_http_peer(fixture.api, fixture.journal, fixture.selected.release.release_sha) as peer:
        publisher = Publisher(
            ApifyAPI(SecretStr("synthetic-publisher-image-token"), peer.endpoint),
            FilesystemArtifacts(artifact),
            fixture.journal,
            fixture.marker,
            fixture.selected.release.release_sha,
            clock=time.monotonic,
            policy=fixture.publisher.policy,
        )
        yield HTTPPublisherRig(
            publisher,
            fixture.api,
            fixture.journal,
            fixture.marker,
            artifact,
            fixture.selected,
            fixture.previous,
            peer,
        )
