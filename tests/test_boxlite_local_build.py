"""Explicitly synthetic evidence tests local-build admission; no KVM claim."""

import copy
import hashlib
import json
import shutil

import pytest
from test_boxlite_platforms import RECIPE, ROOT, helper, synthetic_shim


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(params=["amd64", "arm64"])
def local_build(tmp_path, monkeypatch, request):
    architecture = request.param
    module = helper(ROOT / "deploy/prepare_boxlite_runtime.py")
    base = tmp_path / "deploy"
    recipe = base / "boxlite/v0.10.6"
    shutil.copytree(RECIPE, recipe)
    monkeypatch.setattr(module, "BASE", base)
    runtime = tmp_path / "runtime"
    evidence = runtime / "build-evidence"
    evidence.mkdir(parents=True)
    platforms = json.loads((recipe / "platforms.json").read_text())
    for name in platforms["platforms"][architecture]["original_files"]:
        (runtime / name).write_bytes(
            synthetic_shim(architecture) if name == "boxlite-shim" else (architecture + name).encode()
        )
    files = {p.name: sha(p) for p in runtime.iterdir() if p.is_file()}
    platforms["platforms"][architecture]["original_files"] = copy.deepcopy(files)
    (recipe / "platforms.json").write_text(json.dumps(platforms))
    contract = json.loads((recipe / "replay-dns/source-contract.json").read_text())
    (evidence / "source.patch").write_bytes(b"explicit synthetic patch body for verifier boundary tests")
    contract["tracked_source_diff_sha256"] = sha(evidence / "source.patch")
    (recipe / "replay-dns/source-contract.json").write_text(json.dumps(contract))
    (evidence / "patch-receipt.json").write_text(json.dumps(contract["native_dns_patch"]))
    (evidence / "submodules.txt").write_bytes((recipe / "submodules.txt").read_bytes())
    (evidence / "go-version.txt").write_text("go version go1.25.14 linux/" + architecture + "\n")
    (evidence / "rustc-version.txt").write_text("rustc 1.90.0 synthetic-test\n")
    (evidence / "glibc-version.txt").write_text("glibc 2.36\n")
    recipes = module.load_recipe("replay-dns/monitored_build.py").recipe_hashes(recipe / "replay-dns")
    for phase in ("build", "test"):
        receipt = {
            "phase": phase,
            "status": "PASS",
            "exit_code": 0,
            "architecture": architecture,
            "image": "sha256:" + "1" * 64,
            "network": "none" if phase == "test" else "default",
            "recipe_sha256": recipes,
        }
        if phase == "build":
            receipt["artifact_sha256"] = files["boxlite-shim"]
        (evidence / (phase + "-receipt.json")).write_text(json.dumps(receipt))
    source = json.loads((recipe / "source-pin.json").read_text())
    source["source_tracked_diff"] = (
        "clean pinned baseline; exact reviewed replay DNS overlay retained unchanged through compilation"
    )
    meta = {
        "status": "UNQUALIFIED_REPLAY_DNS_CANDIDATE",
        "architecture": architecture,
        "source": source,
        "official_runtime": platforms["platforms"][architecture]["official_runtime"],
        "original_files": platforms["platforms"][architecture]["original_files"],
        "native_dns_patch": contract["native_dns_patch"],
        "qualification": {
            "native": "NOT_RUN",
            "production_adoption": False,
            "inherited_hardware_results": False,
        },
        "files": files,
        "shim_sha256": files["boxlite-shim"],
        "build": {"image_sha256": "1" * 64, "sdk_abi_changed": False},
        "elf": module.load_recipe("finalize-runtime.py").elf_contract(
            (runtime / "boxlite-shim").read_bytes(), architecture
        ),
        "build_contract": {
            "schema": "tank.boxlite-local-build.v1",
            "evidence_sha256": {p.name: sha(p) for p in evidence.iterdir()},
        },
    }

    def save():
        (runtime / "provenance.json").write_text(json.dumps(meta))
        (runtime / "SHA256SUMS").write_text(
            "".join(f"{digest}  {name}\n" for name, digest in sorted(meta["files"].items()))
        )

    save()
    return module, runtime, meta, save


def test_both_platforms_admit_pinned_build_without_private_sha_registration(local_build):
    module, runtime, _, _ = local_build
    # Deliberately remove even the historical ARM registration. Local source
    # builds are checked by their public build/source contract instead.
    (module.BASE / "boxlite/v0.10.6/replay-dns-qualified-artifact.json").unlink()
    result = module.verify_runtime(runtime)
    assert result["source_build_verification"] == "PINNED_LOCAL_BUILD_VERIFIED_NATIVE_NOT_RUN"
    assert result["qualification"]["native"] == "NOT_RUN"
    assert result["qualification"]["production_adoption"] is False
    assert "external_qualification" not in result


@pytest.mark.parametrize(
    "name",
    [
        "boxlite-shim",
        "libkrunfw.so.5",
        "build-evidence/source.patch",
        "build-evidence/build-receipt.json",
        "build-evidence/submodules.txt",
    ],
)
def test_local_build_rejects_changed_output_or_evidence(local_build, name):
    module, runtime, _, _ = local_build
    (runtime / name).write_bytes((runtime / name).read_bytes() + b"changed")
    with pytest.raises(ValueError):
        module.verify_runtime(runtime)


@pytest.mark.parametrize("field", ["source", "native_dns_patch", "elf", "qualification"])
def test_local_build_rejects_unpinned_source_unsafe_dns_or_fabricated_hardware(local_build, field):
    module, runtime, meta, save = local_build
    meta[field] = {}
    save()
    with pytest.raises(ValueError):
        module.verify_runtime(runtime)


def test_local_build_rejects_receipt_rebound_to_different_binary(local_build):
    module, runtime, meta, save = local_build
    path = runtime / "build-evidence/build-receipt.json"
    receipt = json.loads(path.read_text())
    receipt["artifact_sha256"] = "0" * 64
    path.write_text(json.dumps(receipt))
    meta["build_contract"]["evidence_sha256"][path.name] = sha(path)
    save()
    with pytest.raises(ValueError, match="does not match"):
        module.verify_runtime(runtime)


def test_local_build_rejects_arbitrary_non_elf_even_with_rebound_checksums(local_build):
    module, runtime, meta, save = local_build
    (runtime / "boxlite-shim").write_bytes(b"arbitrary binary is not a pinned Linux shim")
    meta["files"]["boxlite-shim"] = meta["shim_sha256"] = sha(runtime / "boxlite-shim")
    path = runtime / "build-evidence/build-receipt.json"
    receipt = json.loads(path.read_text())
    receipt["artifact_sha256"] = meta["shim_sha256"]
    path.write_text(json.dumps(receipt))
    meta["build_contract"]["evidence_sha256"][path.name] = sha(path)
    save()
    with pytest.raises(ValueError, match="ELF"):
        module.verify_runtime(runtime)
