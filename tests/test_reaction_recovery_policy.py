"""Recovery eligibility executes independently of physical/runtime dependencies."""

import subprocess
import sys
from pathlib import Path
from typing import Literal

import pytest

from tank.contracts.json import decode_json, encode_json
from tank.contracts.scheduler_commands import RevisedReactionCommand
from tank.models import Record
from tank.util import sha256


@pytest.mark.parametrize(
    ("marker", "reason"), [("ABSENT", "PEER_ABSENT"), ("UNRESOLVED", "MARKER_UNRESOLVED")]
)
def test_eligibility_refuses_missing_evidence_without_storage_or_runtime(
    marker: Literal["ABSENT", "UNRESOLVED"], reason: str
) -> None:
    command = RevisedReactionCommand(
        event_id="event",
        token_id="token",
        checkpoint="POST_24H",
        automatic_revision=True,
        change_id="source-change:POST_24H",
        reason="Declared retained automatic revision",
    )
    payload = command.storage_fields()
    evidence: Record = {
        "original": {
            "job_id": "original",
            "status": "DONE",
            "command": payload,
            "payload_revision": sha256(payload),
            "result": {"coalesced_into": "peer"},
        },
        "peer": None,
        "source": None,
        "checkpoint": None,
        "marker": {"kind": marker},
    }
    script = """
import sys

blocked = {"tank.storage", "tank.runtime_reaction", "tank.delivery", "tank.scheduler"}

class RefuseInfrastructure:
    def find_spec(self, fullname, path=None, target=None):
        if fullname in blocked:
            raise RuntimeError("Recovery policy imported infrastructure: " + fullname)

sys.meta_path.insert(0, RefuseInfrastructure())
from tank.contracts.reaction_recovery import ReactionRecoveryEvidence
from tank.reaction_recovery_policy import recovery_eligibility
result = recovery_eligibility(ReactionRecoveryEvidence.model_validate_json(sys.stdin.read()))
assert not blocked.intersection(sys.modules)
print(result.model_dump_json())
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        input=encode_json(evidence).decode(),
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert decode_json(result.stdout) == {"kind": "UNRESOLVED", "reason": reason}
