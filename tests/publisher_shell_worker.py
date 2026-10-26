"""Typed local shell-ordering stand; container supervision is qualified separately."""

import os
import re
import sys
import time
from pathlib import Path
from typing import Literal, Self
from unittest.mock import patch
from urllib.parse import urlsplit

from pydantic import SecretStr, ValidationError, model_validator

from tank.contracts import DomainModel
from tank.deployment.apify_api import ApifyAPI, ApifyAPIError, ApifyEndpoint
from tank.deployment.artifacts import ArtifactError, FilesystemArtifacts, load_release
from tank.deployment.commands import PublisherAction, PublisherCommand, execute_command
from tank.deployment.contracts import PublisherPolicy, SourceRelease
from tank.deployment.identities import CommitSHA
from tank.deployment.journal import JournalError, PublisherJournal
from tank.deployment.markers import FilesystemTankReleaseState, ReleaseMarkerError
from tank.deployment.publisher import Publisher, PublisherStateError
from tank.time_precision import collection_clock


class StandInvocation(DomainModel):
    image: str
    orchestrator: CommitSHA
    selected: CommitSHA
    transaction: CommitSHA
    action: PublisherAction
    root: Path

    @model_validator(mode="after")
    def exact_image(self) -> Self:
        if self.image != "ghcr.io/frequensy23-coder/tank/tank:" + self.orchestrator:
            raise ValueError("Stand image identity mismatch")
        if self.action in {"prepare", "activate", "commit", "finish"} and self.transaction != self.selected:
            raise ValueError("Stand forward release mismatch")
        return self


class StandConfiguration(DomainModel):
    endpoint: ApifyEndpoint
    interrupt: Literal["cache", "pending", "active", "receipt"] | None

    @model_validator(mode="after")
    def loopback_only(self) -> Self:
        if urlsplit(self.endpoint.base_url).hostname != "127.0.0.1":
            raise ValueError("The shell stand can access only its owned loopback peer")
        return self


def run(invocation: StandInvocation, configuration: StandConfiguration) -> None:
    root = invocation.root
    artifact = root / "releases" / invocation.selected
    target: str | None = None
    match configuration.interrupt:
        case "cache" | "receipt":
            release = load_release(artifact, selected_sha=invocation.selected)
            if not isinstance(release, SourceRelease):
                raise ValueError("A cache interruption requires a source release")
            target = (
                ("submission-" if configuration.interrupt == "receipt" else "")
                + release.source_digest
                + ".json"
            )
        case "pending" | "active":
            target = configuration.interrupt + ".json"
    original_replace = os.replace
    matching_writes = 0

    def replace(source: str, destination: str, *, src_dir_fd: int, dst_dir_fd: int) -> None:
        nonlocal matching_writes
        if destination == target:
            matching_writes += 1
            if configuration.interrupt == "receipt" and matching_writes == 1:
                original_replace(source, destination, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)
                return
            assert re.fullmatch(r"\.[A-Za-z0-9_.]+\.tmp", source)
            os._exit(73)
        original_replace(source, destination, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    publisher = Publisher(
        ApifyAPI(SecretStr("synthetic-publisher-image-token"), configuration.endpoint),
        FilesystemArtifacts(artifact),
        PublisherJournal(root / "apify-state"),
        FilesystemTankReleaseState(root / "deployed"),
        invocation.transaction,
        clock=time.monotonic,
        collection_clock=collection_clock,
        policy=PublisherPolicy(build_timeout_s=600.0),
    )
    with (root / "effects.log").open("a") as stream:
        stream.write(f"PUBLISHER_START {invocation.action} {invocation.transaction}\n")
    with patch("tank.deployment.journal.os.replace", replace):
        result = execute_command(
            PublisherCommand(action=invocation.action, release_sha=invocation.transaction), publisher
        )
    with (root / "effects.log").open("a") as stream:
        stream.write(f"PUBLISHER_DONE {invocation.action} {invocation.transaction}\n")
    for line in result.lines:
        print(line)


def main() -> None:
    try:
        if len(sys.argv) != 7:
            raise ValueError("The stand requires the exact worker protocol")
        invocation = StandInvocation.model_validate(
            dict(
                zip(
                    ("image", "orchestrator", "selected", "transaction", "action", "root"),
                    (*sys.argv[1:6], Path(sys.argv[6])),
                    strict=True,
                )
            )
        )
        configuration = StandConfiguration.model_validate(
            {
                "endpoint": ApifyEndpoint(base_url=os.environ["TANK_PUBLISHER_STAND_ENDPOINT"]),
                "interrupt": os.environ.get("TANK_PUBLISHER_STAND_INTERRUPT"),
            }
        )
        run(invocation, configuration)
    except (
        ApifyAPIError,
        ArtifactError,
        JournalError,
        PublisherStateError,
        ReleaseMarkerError,
        ValidationError,
        OSError,
        ValueError,
    ):
        raise SystemExit("Synthetic publisher shell stand refused") from None


if __name__ == "__main__":
    main()
