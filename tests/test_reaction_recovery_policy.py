"""Recovery eligibility imports no physical or runtime execution dependencies."""

import subprocess
import sys
from pathlib import Path


def test_eligibility_policy_import_is_independent_of_storage_and_runtime() -> None:
    script = """
import sys

blocked = {"tank.storage", "tank.runtime_reaction", "tank.delivery", "tank.scheduler"}

class RefuseInfrastructure:
    def find_spec(self, fullname, path=None, target=None):
        if fullname in blocked:
            raise RuntimeError("Recovery policy imported infrastructure: " + fullname)

sys.meta_path.insert(0, RefuseInfrastructure())
from tank.reaction_recovery_policy import recovery_eligibility
assert callable(recovery_eligibility)
assert not blocked.intersection(sys.modules)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert result.returncode == 0, result.stderr
