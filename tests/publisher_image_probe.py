"""Run real publisher HTTP/journal transactions inside a network-disabled application image."""

import json
import os
import sys
import time
from pathlib import Path

from publisher_http_support import publisher_http_peer
from publisher_state_support import FixedArtifacts, rig
from pydantic import SecretStr

from tank.deployment.apify_api import ApifyAPI, ApifyAPIError
from tank.deployment.contracts import RolledBackTransaction
from tank.deployment.publisher import Publisher
from tank.deployment.runtime import verify_runtime
from tank.deployment.runtime_contracts import RuntimeInvocation


def probe(root: Path, sha: str) -> None:
    runtime = verify_runtime(RuntimeInvocation(orchestrator_sha=sha, selected_release_sha=sha), Path("/app"))
    forward = root / "forward"
    forward.mkdir()
    fixture = rig(forward, fault="NONE")
    with publisher_http_peer(fixture.api, fixture.journal, fixture.selected.release.release_sha) as peer:
        publisher = Publisher(
            ApifyAPI(SecretStr("synthetic-publisher-image-token"), peer.endpoint),
            FixedArtifacts(fixture.selected),
            fixture.journal,
            fixture.marker,
            fixture.selected.release.release_sha,
            clock=time.monotonic,
            policy=fixture.publisher.policy,
        )
        prepared = publisher.prepare()
        assert fixture.api.direct == "A" * 17
        publisher.activate()
        committed = publisher.commit()
        fixture.marker.path.write_text(committed.release_sha + "\n")
        publisher.finish()
        assert fixture.api.direct == prepared.candidate.build_id == "B" * 17
        assert fixture.journal.read_active() == committed
        assert fixture.journal.read_transaction() is None
        assert len(fixture.api.started) == 1
        forward_requests = len(peer.requests)
    recovery = root / "recovery"
    recovery.mkdir()
    fixture = rig(recovery, fault="TAG_UNCERTAIN")
    previous = fixture.journal.read_active()
    with publisher_http_peer(fixture.api, fixture.journal, fixture.selected.release.release_sha) as peer:
        publisher = Publisher(
            ApifyAPI(SecretStr("synthetic-publisher-image-token"), peer.endpoint),
            FixedArtifacts(fixture.selected),
            fixture.journal,
            fixture.marker,
            fixture.selected.release.release_sha,
            clock=time.monotonic,
            policy=fixture.publisher.policy,
        )
        publisher.prepare()
        try:
            publisher.activate()
        except ApifyAPIError:
            pass
        else:
            raise AssertionError("Lost HTTP acknowledgement must not be reported as activation success")
        assert fixture.api.direct == "B" * 17
        pending = fixture.journal.read_transaction()
        assert pending is not None and pending.activation_attempted
        assert fixture.journal.read_active() == previous
        fixture.api.fault = "NONE"
        restarted = Publisher(
            ApifyAPI(SecretStr("synthetic-publisher-image-token"), peer.endpoint),
            FixedArtifacts(fixture.selected),
            fixture.journal,
            fixture.marker,
            fixture.selected.release.release_sha,
            clock=time.monotonic,
            policy=fixture.publisher.policy,
        )
        restarted.rollback()
        assert fixture.api.direct == "A" * 17
        assert fixture.journal.read_active() == previous
        assert isinstance(fixture.journal.read_transaction(), RolledBackTransaction)
        restarted.rollback_finish()
        assert fixture.journal.read_transaction() is None
        assert fixture.api.tag_updates == ["B" * 17, "A" * 17]
        assert len(fixture.api.started) == 1
        recovery_requests = len(peer.requests)
    print(
        json.dumps(
            {
                "runtime": runtime.model_dump(mode="json"),
                "forward_http_requests": forward_requests,
                "recovery_http_requests": recovery_requests,
                "forward": "COMMITTED",
                "lost_ack": "RESTORED_AFTER_RESTART",
            }
        )
    )


if __name__ == "__main__":
    if len(sys.argv) != 2 or os.environ.get("TANK_BUILD_SHA") is None:
        raise SystemExit("Explicit scratch path and actual image identity required")
    probe(Path(sys.argv[1]), os.environ["TANK_BUILD_SHA"])
