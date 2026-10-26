"""Exercise the unchanged CLI against a loopback TLS provider with networking disabled."""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Literal

from publisher_http_support import publisher_https_peer
from publisher_state_support import ProviderState, source_artifact

from tank.contracts import DomainModel
from tank.contracts.scalars import OneInt, TrueFlag
from tank.deployment.contracts import (
    ActiveRelease,
    BuildCacheKey,
    BuildID,
    RolledBackTransaction,
    SourceBuild,
)
from tank.deployment.journal import PublisherJournal
from tank.deployment.package_runtime import verify_package_runtime
from tank.deployment.runtime_contracts import RuntimeDescriptor, RuntimeInvocation
from tank.deployment.submissions import ReceivedBuildSubmission, UnknownBuildSubmission


class CLIOutcome(DomainModel):
    case: Literal[
        "forward",
        "lost-ack",
        "terminated-after-acceptance",
        "terminated-after-build-acceptance",
        "terminated-before-build-cache",
    ]
    physical_tls_requests: int
    builds: OneInt


class CLIProof(DomainModel):
    actual_cli: TrueFlag
    api_host: Literal["loopback-only-api.apify.com"]
    cases: tuple[CLIOutcome, ...]
    observed_runtime: RuntimeDescriptor
    identity_mode: Literal["equal", "distinct"]


def run_probe(
    root: Path, package: Path, orchestrator_sha: str, sha: str, *, identity_mode: Literal["equal", "distinct"]
) -> None:
    invocation = RuntimeInvocation(orchestrator_sha=orchestrator_sha, selected_release_sha=sha)
    declared = verify_package_runtime(package, invocation, Path("/app"))
    if sys.executable != "/opt/tank/venv/bin/python":
        raise ValueError("The CLI probe requires the actual application image interpreter")
    if (identity_mode == "equal") != (
        declared.runtime.orchestrator_sha == declared.runtime.selected_release_sha
    ):
        raise ValueError("Actual package identities do not match the requested proof mode")
    results: list[CLIOutcome] = []
    for name in (
        "forward",
        "lost-ack",
        "terminated-after-acceptance",
        "terminated-after-build-acceptance",
        "terminated-before-build-cache",
    ):
        uncertain = name == "lost-ack"
        interrupted = name == "terminated-after-acceptance"
        host = root / name
        host.mkdir(mode=0o700)
        state = host / "apify-state"
        journal = PublisherJournal(state)
        previous = source_artifact("a" * 40, b"previous-real-source-shape")
        initial = SourceBuild(
            release_sha=previous.release.release_sha,
            source_digest=previous.release.source_digest,
            contract=previous.release.contract,
            actor_id=previous.release.actor_id,
            build_id="A" * 17,
        )
        journal.write_active(
            ActiveRelease(release_sha=initial.release_sha, build_id=initial.build_id, build=initial)
        )
        marker = host / "deployed"
        marker.write_text(initial.release_sha + "\n")
        credentials = host / ".env"
        credentials.write_text("APIFY_API_TOKEN=synthetic-publisher-image-token\n")
        credentials.chmod(0o600)
        provider = ProviderState(previous, fault="TAG_UNCERTAIN" if uncertain else "NONE")
        environment = dict(os.environ)
        environment["SSL_CERT_FILE"] = "/tls/certificate.pem"
        environment["NO_PROXY"] = "api.apify.com"
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            environment.pop(key, None)

        active_process: subprocess.Popen[bytes] | None = None
        active_action: str | None = None
        build_interruption_fired = False

        def after_tag_update() -> bool:
            if interrupted and active_action == "activate":
                assert active_process is not None and active_process.poll() is None
                active_process.terminate()
                return True
            return False

        def interrupt_build() -> bool:
            nonlocal build_interruption_fired
            if build_interruption_fired or active_action != "prepare":
                return False
            assert active_process is not None and active_process.poll() is None
            active_process.terminate()
            build_interruption_fired = True
            return True

        def after_build_start() -> bool:
            return name == "terminated-after-build-acceptance" and interrupt_build()

        def after_build_snapshot(build_id: BuildID) -> bool:
            return name == "terminated-before-build-cache" and build_id == "B" * 17 and interrupt_build()

        def command(action: str, *, succeeds: bool, expected_signal: bool) -> None:
            nonlocal active_process, active_action
            active_action = action
            active_process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "tank.deployment.cli",
                    action,
                    "--orchestrator-sha",
                    orchestrator_sha,
                    "--selected-release-sha",
                    sha,
                    "--transaction-release-sha",
                    sha,
                    "--runtime-root",
                    "/app",
                    "--package",
                    str(package),
                    "--state",
                    str(state),
                    "--credential-file",
                    str(credentials),
                    "--tank-marker",
                    str(marker),
                    "--build-timeout-s",
                    "600",
                ],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = active_process.communicate(timeout=30)
            code = active_process.returncode
            if expected_signal:
                assert code == -15
            assert (code == 0) is succeeds, (action, code, stderr)
            assert b"synthetic-publisher-image-token" not in stdout + stderr

        with publisher_https_peer(
            provider,
            journal,
            sha,
            certificate=Path("/tls/certificate.pem"),
            private_key=Path("/tls/private-key.pem"),
            after_tag_update=after_tag_update,
            after_build_start=after_build_start,
            after_build_snapshot=after_build_snapshot,
        ) as peer:
            build_interrupted = name in {"terminated-after-build-acceptance", "terminated-before-build-cache"}
            command("prepare", succeeds=not build_interrupted, expected_signal=build_interrupted)
            if build_interrupted:
                assert build_interruption_fired
                submissions = list(state.glob("submission-*.json"))
                assert len(submissions) == 1
                if name == "terminated-after-build-acceptance":
                    unknown = UnknownBuildSubmission.model_validate_json(submissions[0].read_bytes())
                    before = len(peer.requests)
                    command("prepare", succeeds=False, expected_signal=False)
                    assert len(provider.started) == 1
                    assert UnknownBuildSubmission.model_validate_json(submissions[0].read_bytes()) == unknown
                    assert len(peer.requests) > before
                    assert provider.direct == "A" * 17 and provider.tag_updates == []
                    assert journal.read_transaction() is None
                    results.append(CLIOutcome(case=name, physical_tls_requests=len(peer.requests), builds=1))
                    continue
                receipt = ReceivedBuildSubmission.model_validate_json(submissions[0].read_bytes())
                assert receipt.observation.id == "B" * 17
                assert receipt.observation.status == "SUCCEEDED"
                command("prepare", succeeds=True, expected_signal=False)
                assert journal.read_submission(BuildCacheKey(source_digest=receipt.source_digest)) is None
            assert provider.direct == "A" * 17
            command("activate", succeeds=not (uncertain or interrupted), expected_signal=interrupted)
            assert provider.direct == "B" * 17
            if uncertain or interrupted:
                pending = journal.read_transaction()
                assert pending is not None and pending.activation_attempted
                provider.fault = "NONE"
                command("rollback", succeeds=True, expected_signal=False)
                assert provider.direct == "A" * 17
                assert isinstance(journal.read_transaction(), RolledBackTransaction)
                command("rollback_finish", succeeds=True, expected_signal=False)
            else:
                command("commit", succeeds=True, expected_signal=False)
                marker.write_text(sha + "\n")
                command("finish", succeeds=True, expected_signal=False)
                active = journal.read_active()
                assert active is not None and active.release_sha == sha
            assert journal.read_transaction() is None
            assert len(provider.started) == 1
            results.append(
                CLIOutcome.model_validate(
                    {
                        "case": name,
                        "physical_tls_requests": len(peer.requests),
                        "builds": len(provider.started),
                    }
                )
            )
    print(
        CLIProof(
            actual_cli=True,
            api_host="loopback-only-api.apify.com",
            cases=tuple(results),
            observed_runtime=declared.runtime,
            identity_mode=identity_mode,
        ).model_dump_json()
    )


class ProbeArguments(argparse.Namespace):
    root: Path
    package: Path
    orchestrator_sha: str
    selected_sha: str
    identity_mode: Literal["equal", "distinct"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("package", type=Path)
    parser.add_argument("orchestrator_sha")
    parser.add_argument("selected_sha")
    parser.add_argument("--identity-mode", required=True, choices=["equal", "distinct"])
    arguments = parser.parse_args(namespace=ProbeArguments())
    run_probe(
        arguments.root,
        arguments.package,
        arguments.orchestrator_sha,
        arguments.selected_sha,
        identity_mode=arguments.identity_mode,
    )


if __name__ == "__main__":
    main()
