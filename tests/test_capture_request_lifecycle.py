"""Actual PostgreSQL regression for Go's sparse capture request lifecycle."""

import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from postgres_helpers import PostgresTemplate
from test_capture_parity import go_environment

ROOT = Path(__file__).parents[1]


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.process
async def test_go_sparse_request_lifecycle_preserves_invalid_evidence(
    postgres_template: PostgresTemplate, tmp_path: Path
) -> None:
    output = ROOT / ".test-results"
    output.mkdir(exist_ok=True)
    evidence = output / f"capture-lifecycle-peer-{uuid4().hex}.json"
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        environment = go_environment(tmp_path)
        environment["TANK_TEST_DATABASE_URL"] = repo.database_url
        environment["TANK_CAPTURE_LIFECYCLE_EVIDENCE"] = str(evidence)
        result = subprocess.run(
            [
                "go",
                "test",
                "-json",
                "-count=1",
                "-timeout=90s",
                "-run",
                "^TestSparseRequestLifecyclePreservesInvalidNumericEvidence$",
                "./internal/store",
            ],
            cwd=ROOT / "capture",
            env=environment,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    report = tmp_path / "go-results.jsonl"
    report.write_text(result.stdout)
    assert result.returncode == 0, result.stdout + result.stderr
    checked = subprocess.run(
        [sys.executable, str(ROOT / "scripts/ci/check_go_results.py"), str(report)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert evidence.is_file()
    print(f"Actual Go lifecycle peer snapshots: {evidence}")
