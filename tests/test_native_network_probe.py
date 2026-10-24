"""Constrained worker contracts use actual local effects and synthetic DNS wire bytes."""

import importlib.util
import json
import struct
import sys
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

if TYPE_CHECKING:
    from deploy.openma_lifecycle import native_network_probe as probe
else:
    source = Path(__file__).parents[1] / "deploy/openma_lifecycle/native_network_probe.py"
    spec = importlib.util.spec_from_file_location("native_network_probe", source)
    assert spec is not None
    assert spec.loader is not None
    probe = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = probe
    spec.loader.exec_module(probe)


def test_attempt_records_real_effect_before_explicit_failure(tmp_path: Path) -> None:
    target = tmp_path / "effect"

    def operation() -> str:
        target.write_text("observed")
        raise OSError("synthetic network unavailable")

    result = probe.attempt(operation)
    assert target.read_text() == "observed"
    assert isinstance(result, probe.ProbeFailure)
    payload = json.loads(json.dumps(result.json(), allow_nan=False))
    assert payload["ok"] is False
    assert payload["error"] == "OSError: synthetic network unavailable"
    assert type(payload["ms"]) is int
    assert payload["ms"] >= 0


def test_attempt_serializes_actual_observed_value(tmp_path: Path) -> None:
    target = tmp_path / "observation"
    target.write_text("measured")

    def operation() -> str:
        return target.read_text()

    result = probe.attempt(operation)
    payload = json.loads(json.dumps(result.json(), allow_nan=False))
    assert payload["ok"] is True
    assert payload["value"] == target.read_text()
    assert "error" not in payload


@pytest.mark.parametrize("rcode,answers", [(3, 1), (0, 0)])
def test_dns_wire_parser_rejects_unsuccessful_response(rcode: int, answers: int) -> None:
    response = struct.pack(">HHHHHH", 0x5441, 0x8100 | rcode, 1, answers, 0, 0)
    with pytest.raises(RuntimeError, match=f"rcode={rcode} answers={answers}"):
        probe.dns_reply(response)


def test_dns_wire_parser_requires_header_and_positive_answer() -> None:
    with pytest.raises(ValueError, match="truncated"):
        probe.dns_reply(b"\x00")
    response = struct.pack(">HHHHHH", 0x5441, 0x8100, 1, 1, 0, 0)
    result = probe.dns_reply(response)
    assert probe.value_json(result) == {"rcode": 0, "answers": 1}


def test_nameserver_file_parser_rejects_missing_address(tmp_path: Path) -> None:
    path = tmp_path / "resolv.conf"
    path.write_text("nameserver 10.0.0.53\nsearch example.test\n")
    assert probe.nameservers(path) == ["10.0.0.53"]
    path.write_text("nameserver\n")
    with pytest.raises(ValueError, match="no address"):
        probe.nameservers(path)


def test_constrained_serialization_rejects_invalid_runtime_values() -> None:
    """Deliberate malformed boundary values test runtime validation beyond annotations."""
    with pytest.raises(TypeError, match="Unsupported probe value"):
        probe.value_json(cast(probe.ProbeValue, float("nan")))
    with pytest.raises(ValueError, match="nonnegative integer"):
        probe.ProbeSuccess("observed", True)
    with pytest.raises(TypeError, match="success or failure records"):
        probe.attempt_json(cast(probe.ProbeAttempt, {"ok": True}))
