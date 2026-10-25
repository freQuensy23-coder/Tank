"""Behavioral masking and leak-admission checks over serialized acceptance packets."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import JsonValue

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def acceptance_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.syspath_prepend(str(ROOT))


def test_mask_transform_obeys_word_boundaries_and_case_declarations() -> None:
    from scripts.acceptance.masking_contracts import transform_pair

    (forward, _), _ = transform_pair({"Issuer One": "Name01", "ticker": "TOKEN"}, {"Issuer One"})
    assert forward("ISSUER ONE, Issuer One, TICKER, tickerish") == "ISSUER ONE, Name01, TOKEN, tickerish"


def test_masked_json_keys_and_values_round_trip_without_aliasing() -> None:
    from scripts.acceptance.masking_contracts import transform_pair

    original: JsonValue = {"Issuer One": ["Issuer One announced", None, True, 7]}
    (_, forward), (_, inverse) = transform_pair({"Issuer One": "Name01"}, {"Issuer One"})
    masked = forward(original)
    assert masked == {"Name01": ["Name01 announced", None, True, 7]}
    assert inverse(masked) == original
    assert isinstance(masked, dict)
    masked["Name01"] = "changed"
    assert original == {"Issuer One": ["Issuer One announced", None, True, 7]}


def test_mask_refuses_json_key_collision() -> None:
    from scripts.acceptance.masking_contracts import AssemblyError, transform_pair

    (_, forward), _ = transform_pair({"Issuer One": "Name01"}, {"Issuer One"})
    with pytest.raises(AssemblyError, match="collides JSON keys"):
        forward({"Issuer One": "first", "Name01": "second"})


@pytest.mark.parametrize("second", ["Name01", "NAME01"])
def test_many_to_one_mask_cannot_guess_an_inverse_source(second: str) -> None:
    from scripts.acceptance.masking_contracts import MaskInverseAmbiguity, transform_pair

    (forward, _), (inverse, _) = transform_pair({"Issuer One": "Name01", "Issuer Two": second}, ())
    masked = forward("Issuer One and Issuer Two")
    assert masked == "Name01 and " + second
    with pytest.raises(MaskInverseAmbiguity, match="more than one declared source"):
        inverse(masked)


def test_empty_mask_preserves_copy_isolation() -> None:
    from scripts.acceptance.masking_contracts import transform_pair

    original: JsonValue = {"nested": ["unchanged"]}
    (_, forward), _ = transform_pair({}, ())
    copied = forward(original)
    assert isinstance(copied, dict)
    nested = copied["nested"]
    assert isinstance(nested, list)
    nested.append("mutation")
    assert original == {"nested": ["unchanged"]}


def test_leak_scanner_covers_keys_and_values_beyond_declared_mask_metadata() -> None:
    from scripts.acceptance.masking_contracts import leak_scan

    packet: dict[str, JsonValue] = {
        "records": [{"Issuer One": "Issuer One emerged"}],
        "masking": {"replacements": {"Issuer One": "Name01"}},
    }
    result = leak_scan(packet, {"Issuer One": "Name01"}, (), None)
    assert len(result.hits) == 2
    assert all("masking" not in path for path in result.hits)
    assert any(path.startswith("records/Issuer One:") for path in result.hits)


def test_declared_technical_exception_is_limited_to_its_actual_field() -> None:
    from scripts.acceptance.masking_contracts import decode_exceptions, leak_scan

    exception = decode_exceptions(
        {
            "tokens": ["issuer-one"],
            "fields": ["url"],
            "basis": "Exact retained technical-host scope",
        }
    )
    result = leak_scan(
        {"url": "https://issuer-one.test", "text": "issuer-one announced"},
        {"issuer-one": "name-one"},
        (),
        exception,
    )
    assert any(path.startswith("text:") for path in result.hits)
    assert all(not path.startswith("url:") for path in result.hits)
    assert any("declared technical-context exception" in warning for warning in result.warnings)


def test_lowercase_venue_exception_does_not_hide_uppercase_identity() -> None:
    from scripts.acceptance.masking_contracts import leak_scan

    result = leak_scan({"text": "binance BINANCE binanceish"}, {"binance": "Name01"}, {"binance"}, None)
    assert any(path.endswith(":BINANCE") for path in result.hits)
    assert any("technical venue id" in warning for warning in result.warnings)
    assert any(warning.endswith(":binance") for warning in result.warnings)


@pytest.mark.parametrize(
    "document",
    [
        {"tokens": ["issuer"], "fields": ["url"]},
        {"tokens": ["issuer"], "fields": ["url"], "basis": " "},
        {"tokens": [], "fields": ["url"], "basis": "ruling"},
        {"tokens": ["issuer"], "fields": "url", "basis": "ruling"},
        {"tokens": ["issuer"], "fields": ["url"], "basis": "ruling", "unknown": True},
    ],
)
def test_undeclared_or_malformed_exception_cannot_relax_scan(document: JsonValue) -> None:
    from scripts.acceptance.masking_contracts import AssemblyError, decode_exceptions

    with pytest.raises(AssemblyError, match="Invalid declared"):
        decode_exceptions(document)


def test_assembler_declared_launch_imports_its_own_checkout(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/acceptance/assemble_packets.py"), "--help"],
        cwd=tmp_path,
        env={"PYTHONPATH": str(ROOT / "src"), "PATH": os.defpath, "HOME": str(tmp_path)},
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert "--fixtures" in result.stdout
