"""Reject incomplete verification using actual retained parity evidence."""

import gzip
import subprocess
import sys
from pathlib import Path
from typing import Literal

import pytest
from pydantic import JsonValue, ValidationError

from scripts.capture import parity
from tank.contracts.json import decode_json

type ParitySection = Literal["trades", "rows", "bars", "errors", "fingerprints", "watermarks"]


def retained_document() -> dict[str, JsonValue]:
    path = parity.GOLDENS / "rest-binance-spot.json.gz"
    value = decode_json(gzip.decompress(path.read_bytes()).decode())
    if not isinstance(value, dict):
        raise ValueError("Retained output must be a JSON object")
    return value


@pytest.mark.parametrize("section", ["trades", "rows", "bars", "errors", "fingerprints", "watermarks"])
def test_missing_output_section_cannot_become_empty_success(section: ParitySection) -> None:
    wire = retained_document()
    del wire[section]
    with pytest.raises(ValidationError, match=section):
        parity.ParityOutput.model_validate(wire)


def test_new_output_section_cannot_silently_escape_full_comparison() -> None:
    wire = retained_document()
    wire["unverified_output"] = []
    with pytest.raises(ValidationError, match="unverified_output"):
        parity.ParityOutput.model_validate(wire)


def test_missing_retained_inputs_cannot_reduce_the_collected_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(parity, "FRAMES", tmp_path)
    with pytest.raises(ValueError, match="inventory differs"):
        parity.fixtures()


def test_no_verification_action_cannot_report_success() -> None:
    result = subprocess.run(
        [sys.executable, str(parity.ROOT / "scripts" / "capture" / "parity.py")],
        cwd=parity.ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "select actual verification" in result.stderr
    assert not result.stdout


def test_unmatched_selector_cannot_report_zero_case_success() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(parity.ROOT / "scripts" / "capture" / "parity.py"),
            "--check",
            "--only",
            "NONEXISTENT.synthetic.fixture",
        ],
        cwd=parity.ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "did not match" in result.stderr
    assert not result.stdout
