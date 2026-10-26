"""Exercise the unchanged CLI against a loopback TLS provider with networking disabled."""

import json
import os
import subprocess
import sys
from pathlib import Path

from publisher_http_support import publisher_https_peer
from publisher_state_support import ProviderState, source_artifact

from tank.deployment.contracts import ActiveRelease, RolledBackTransaction, SourceBuild
from tank.deployment.journal import PublisherJournal


def run_probe(root: Path, package: Path, sha: str) -> None:
    results: list[dict[str, str | int]] = []
    for name, uncertain in (("forward", False), ("lost-ack", True)):
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

        def command(action: str, *, succeeds: bool) -> None:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tank.deployment.cli",
                    action,
                    "--orchestrator-sha",
                    sha,
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
                capture_output=True,
                check=False,
            )
            assert (result.returncode == 0) is succeeds, (action, result.returncode, result.stderr)
            assert b"synthetic-publisher-image-token" not in result.stdout + result.stderr

        with publisher_https_peer(
            provider,
            journal,
            sha,
            certificate=Path("/tls/certificate.pem"),
            private_key=Path("/tls/private-key.pem"),
        ) as peer:
            command("prepare", succeeds=True)
            assert provider.direct == "A" * 17
            command("activate", succeeds=not uncertain)
            assert provider.direct == "B" * 17
            if uncertain:
                pending = journal.read_transaction()
                assert pending is not None and pending.activation_attempted
                provider.fault = "NONE"
                command("rollback", succeeds=True)
                assert provider.direct == "A" * 17
                assert isinstance(journal.read_transaction(), RolledBackTransaction)
                command("rollback_finish", succeeds=True)
            else:
                command("commit", succeeds=True)
                marker.write_text(sha + "\n")
                command("finish", succeeds=True)
                active = journal.read_active()
                assert active is not None and active.release_sha == sha
            assert journal.read_transaction() is None
            assert len(provider.started) == 1
            results.append(
                {"case": name, "physical_tls_requests": len(peer.requests), "builds": len(provider.started)}
            )
    print(json.dumps({"actual_cli": True, "api_host": "loopback-only-api.apify.com", "cases": results}))


if __name__ == "__main__":
    run_probe(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3])
