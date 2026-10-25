"""Compose the declared publisher runtime without selecting ambient host state."""

import argparse
import os
import time
from pathlib import Path

from pydantic import ValidationError

from tank.deployment.apify_api import ApifyAPI, ApifyAPIError, ApifyEndpoint
from tank.deployment.artifact_contracts import PackageRequest, PublisherArtifactRuntime
from tank.deployment.artifacts import ArtifactError, FilesystemArtifacts, package_release
from tank.deployment.commands import PUBLISHER_ACTIONS, PublisherCommand, execute_command
from tank.deployment.contracts import PublisherPolicy
from tank.deployment.credentials import PublisherCredentialError, host_token
from tank.deployment.journal import JournalError, PublisherJournal
from tank.deployment.markers import FilesystemTankReleaseState, ReleaseMarkerError
from tank.deployment.package_runtime import verify_package_runtime
from tank.deployment.publisher import Publisher, PublisherStateError
from tank.deployment.runtime import PublisherRuntimeError, verify_runtime
from tank.deployment.runtime_contracts import RuntimeInvocation
from tank.deployment.source_runtime import verify_source_archive


class Arguments(argparse.Namespace):
    action: str
    orchestrator_sha: str
    selected_release_sha: str
    transaction_release_sha: str
    runtime_root: Path
    package: Path
    state: Path
    credential_file: Path
    tank_marker: Path
    build_timeout_s: float
    repository: Path
    destination: Path
    image_repository: str


def parse_arguments() -> Arguments:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--orchestrator-sha", required=True)
    common.add_argument("--selected-release-sha", required=True)
    common.add_argument("--runtime-root", required=True, type=Path)
    worker = argparse.ArgumentParser(add_help=False, parents=[common])
    worker.add_argument("--transaction-release-sha", required=True)
    worker.add_argument("--package", required=True, type=Path)
    worker.add_argument("--state", required=True, type=Path)
    worker.add_argument("--credential-file", required=True, type=Path)
    worker.add_argument("--tank-marker", required=True, type=Path)
    worker.add_argument("--build-timeout-s", required=True, type=float)
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("runtime-info", parents=[common])
    package = actions.add_parser("package", parents=[common])
    package.add_argument("--repository", required=True, type=Path)
    package.add_argument("--destination", required=True, type=Path)
    package.add_argument("--image-repository", required=True)
    for action in PUBLISHER_ACTIONS:
        actions.add_parser(action, parents=[worker])
    return parser.parse_args(namespace=Arguments())


def run(arguments: Arguments) -> None:
    invocation = RuntimeInvocation(
        orchestrator_sha=arguments.orchestrator_sha,
        selected_release_sha=arguments.selected_release_sha,
    )
    if arguments.action == "runtime-info":
        print(verify_runtime(invocation, arguments.runtime_root).model_dump_json())
        return
    if arguments.action == "package":
        expected_program = arguments.runtime_root / "src/tank/deployment/cli.py"
        if Path(__file__).resolve() != expected_program.resolve():
            raise PublisherRuntimeError("Publisher packaging must execute the declared source archive")
        evidence = verify_source_archive(arguments.repository, arguments.runtime_root, invocation)
        release = package_release(
            PackageRequest(
                repository=arguments.repository,
                destination=arguments.destination,
                publisher=PublisherArtifactRuntime(
                    contract="tank-publisher-runtime-v1",
                    image_repository=arguments.image_repository,
                    runtime=evidence.runtime,
                ),
            )
        )
        print(release.model_dump_json())
        return
    command = PublisherCommand.model_validate(
        {"action": arguments.action, "release_sha": arguments.transaction_release_sha}
    )
    if (
        command.action in {"prepare", "activate", "commit", "finish"}
        and command.release_sha != invocation.selected_release_sha
    ):
        raise PublisherStateError("Forward publication requires the selected release identity")
    policy = PublisherPolicy(build_timeout_s=arguments.build_timeout_s)
    if os.geteuid() != 0:
        raise PublisherStateError("Production publisher must run as root")
    verify_package_runtime(arguments.package, invocation, arguments.runtime_root)
    token = host_token(arguments.credential_file, owner_uid=0)
    publisher = Publisher(
        ApifyAPI(token, ApifyEndpoint(base_url="https://api.apify.com/v2")),
        FilesystemArtifacts(arguments.package),
        PublisherJournal(arguments.state),
        FilesystemTankReleaseState(arguments.tank_marker),
        command.release_sha,
        clock=time.monotonic,
        policy=policy,
    )
    for line in execute_command(command, publisher).lines:
        print(line)


def main() -> None:
    arguments = parse_arguments()
    try:
        run(arguments)
    except ApifyAPIError as error:
        raise SystemExit(str(error)) from None
    except (
        ArtifactError,
        JournalError,
        PublisherCredentialError,
        PublisherRuntimeError,
        PublisherStateError,
        ReleaseMarkerError,
        OSError,
        UnicodeError,
        ValidationError,
        ValueError,
    ):
        raise SystemExit("Apify deployment refused; no unverified worker release accepted") from None


if __name__ == "__main__":
    main()
