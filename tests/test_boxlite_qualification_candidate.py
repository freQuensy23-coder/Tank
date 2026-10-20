import hashlib
import importlib.util
import json
import struct
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "candidate_runtime", ROOT / "deploy/prepare_boxlite_runtime.py"
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    base = tmp_path / "recipe"
    registry = base / "boxlite/v0.10.6"
    (registry / "replay-dns").mkdir(parents=True)
    bundle = tmp_path / "candidate"
    bundle.mkdir()
    current = json.loads((ROOT / "deploy/boxlite/v0.10.6/qualified-artifact.json").read_text())
    interpreter = b"/lib/ld-linux-aarch64.so.1\0"
    elf = bytearray(120 + len(interpreter))
    elf[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<H", elf, 18, 183)
    struct.pack_into("<Q", elf, 32, 64)
    struct.pack_into("<HH", elf, 54, 56, 1)
    struct.pack_into("<I", elf, 64, 3)
    struct.pack_into("<Q", elf, 72, 120)
    struct.pack_into("<Q", elf, 96, len(interpreter))
    elf[120:] = interpreter
    for name in current["files"]:
        (bundle / name).write_bytes(bytes(elf) if name == "boxlite-shim" else name.encode())
    files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in bundle.iterdir()}
    current["files"] = {**files, "boxlite-shim": "0" * 64}
    provenance = {
        "status": "UNQUALIFIED_REPLAY_DNS_CANDIDATE",
        "source": current["source"],
        "official_runtime": current["official_runtime"],
        "files": files,
        "shim_sha256": files["boxlite-shim"],
        "qualification": {"native": "NOT_RUN", "production_adoption": False},
    }
    raw = (json.dumps(provenance) + "\n").encode()
    (bundle / "provenance.json").write_bytes(raw)
    (bundle / "SHA256SUMS").write_text(
        "".join(f"{digest}  {name}\n" for name, digest in sorted(files.items()))
    )
    manifest = {
        "status": provenance["status"],
        "source": provenance["source"],
        "files": files,
        "provenance_sha256": hashlib.sha256(raw).hexdigest(),
    }
    (registry / "qualified-artifact.json").write_text(json.dumps(current))
    (registry / "replay-dns-qualified-artifact.json").write_text(
        json.dumps(
            {
                "status": "QUALIFIED_NATIVE_REPLAY_NETWORK",
                "files": current["files"],
                "shim_sha256": "0" * 64,
                "qualification": {"status": "PASS"},
            }
        )
    )
    (registry / "replay-dns/candidate-artifact.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(helper, "BASE", base)
    return helper, bundle, files


def test_candidate_is_explicitly_unqualified_and_default_rejects(candidate):
    helper, bundle, files = candidate
    result = helper.verify_qualification_candidate(bundle)
    assert result["qualification_only"] is True
    assert result["qualification"] == {"native": "NOT_RUN", "production_adoption": False}
    assert result["artifact_sha256"] == files["boxlite-shim"]
    with pytest.raises(ValueError):
        helper.verify_runtime(bundle)


@pytest.mark.parametrize("name", ["boxlite-shim", "libkrunfw.so.5", "provenance.json", "SHA256SUMS"])
def test_candidate_rejects_modified_registered_bytes(candidate, name):
    helper, bundle, _ = candidate
    (bundle / name).write_bytes((bundle / name).read_bytes() + b"changed")
    with pytest.raises(ValueError):
        helper.verify_qualification_candidate(bundle)


def test_candidate_refuses_matching_symlink_and_unlisted_files(candidate, tmp_path):
    helper, bundle, _ = candidate
    original = bundle / "boxlite-shim"
    outside = tmp_path / "outside-shim"
    outside.write_bytes(original.read_bytes())
    original.unlink()
    original.symlink_to(outside)
    with pytest.raises(ValueError, match="plain files"):
        helper.verify_qualification_candidate(bundle)
    original.unlink()
    original.write_bytes(outside.read_bytes())
    (bundle / "unapproved").write_text("unregistered content")
    with pytest.raises(ValueError, match="file set"):
        helper.verify_qualification_candidate(bundle)


def test_candidate_has_no_operator_selected_manifest_path():
    spec = importlib.util.spec_from_file_location(
        "runtime_signature", ROOT / "deploy/prepare_boxlite_runtime.py"
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    with pytest.raises(TypeError):
        helper.verify_qualification_candidate("unused", manifest="operator-controlled.json")


def test_external_qualification_keeps_build_provenance_immutable(candidate):
    helper, bundle, files = candidate
    raw = (bundle / "provenance.json").read_bytes()
    provenance = json.loads(raw)
    qualification = {
        "status": "PASS",
        "scope": "native factory replay network and read-only database component",
        "managed_session_or_full_E2E_acceptance": False,
        "raw_candidate_provenance_relabelled": False,
    }
    registered = {
        "status": "QUALIFIED_NATIVE_REPLAY_NETWORK",
        "source": provenance["source"],
        "files": files,
        "shim_sha256": files["boxlite-shim"],
        "provenance_sha256": hashlib.sha256(raw).hexdigest(),
        "qualification": qualification,
    }
    (helper.BASE / "boxlite/v0.10.6/replay-dns-qualified-artifact.json").write_text(json.dumps(registered))
    result = helper.verify_runtime(bundle)
    assert result["external_qualification"] == qualification
    assert result["status"] == "UNQUALIFIED_REPLAY_DNS_CANDIDATE"
    assert result["qualification"]["native"] == "NOT_RUN"
    assert (bundle / "provenance.json").read_bytes() == raw
    # A candidate whose status was relabelled after the fact no longer matches
    # the exact externally qualified build provenance.
    provenance["qualification"]["native"] = "PASS"
    (bundle / "provenance.json").write_text(json.dumps(provenance))
    with pytest.raises(ValueError, match="provenance hash mismatch"):
        helper.verify_runtime(bundle)


def test_registered_native_artifact_keeps_immutable_historical_build_anchor():
    base = ROOT / "deploy/boxlite/v0.10.6"
    registration = json.loads((base / "replay-dns-qualified-artifact.json").read_text())
    historical = base / registration["historical_baseline"]["path"]
    assert (
        hashlib.sha256(historical.read_bytes()).hexdigest() == registration["historical_baseline"]["sha256"]
    )
    original = json.loads(historical.read_text())
    assert original["shim_sha256"] == "d44e3d54bf227b288bd059ece8c751339c6db0fb62a3d7c2077cd93241421255"
    assert original["shim_sha256"] != registration["shim_sha256"]
    raw_candidate = base / "replay-dns/candidate-artifact.json"
    assert (
        hashlib.sha256(raw_candidate.read_bytes()).hexdigest()
        == registration["candidate_registration_sha256"]
    )
    assert json.loads(raw_candidate.read_text())["native_qualification"] == "NOT_RUN"
    assert registration["qualification"]["managed_session_or_full_E2E_acceptance"] is False


def test_candidate_packager_refuses_patched_runtime_as_historical_baseline(candidate, tmp_path):
    _, patched_runtime, _ = candidate
    path = ROOT / "deploy/boxlite/v0.10.6/replay-dns/package_candidate.py"
    spec = importlib.util.spec_from_file_location("candidate_packager", path)
    packager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(packager)
    destination = tmp_path / "must-stay-absent"
    with pytest.raises(ValueError, match="Unexpected previous runtime"):
        packager.package(tmp_path / "unused-build", patched_runtime, destination)
    assert not destination.exists()
