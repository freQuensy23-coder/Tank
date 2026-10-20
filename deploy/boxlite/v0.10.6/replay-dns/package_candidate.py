"""Package a DNS-patched candidate, without inheriting d44's qualification."""

import argparse
import hashlib
import importlib.util
import json
import shutil
import struct
import subprocess
from pathlib import Path

BASE = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tracked_source_diff(source):
    return subprocess.check_output(["git", "-C", str(source), "diff", "--binary", "--abbrev=8"])


def package(
    build, previous, destination, test_receipt="test-receipt.json", build_receipt="build-receipt.json"
):
    for name in [test_receipt, build_receipt]:
        if Path(name).name != name:
            raise ValueError("Receipt must be an exact build-root filename")
    if destination.exists() or destination.is_symlink():
        raise ValueError("Candidate destination must be new")
    expected = json.loads((BASE.parent / "qualified-artifact.json").read_text())
    platforms = json.loads((BASE.parent / "platforms.json").read_text())["platforms"]
    official = not (previous / "provenance.json").exists()
    if (previous / "provenance.json").is_symlink():
        raise ValueError("Previous runtime provenance must be a plain file")
    if not official:
        previous_meta = json.loads((previous / "provenance.json").read_text())
    else:
        # A clean host only needs the official eight-file archive as input.
        # Do not compile an unused unpatched shim or require a private baseline.
        original = (previous / "boxlite-shim").read_bytes()
        architecture = (
            {183: "arm64", 62: "amd64"}.get(struct.unpack_from("<H", original, 18)[0])
            if len(original) >= 64
            else None
        )
        if architecture not in platforms or {p.name for p in previous.iterdir()} != set(
            platforms[architecture]["original_files"]
        ):
            raise ValueError("Expected exactly the pinned official runtime input")
        previous_meta = {
            "architecture": architecture,
            "status": "PINNED_OFFICIAL_RUNTIME_INPUT",
            "files": platforms[architecture]["original_files"],
            "shim_sha256": platforms[architecture]["original_files"]["boxlite-shim"],
        }
    architecture = previous_meta.get("architecture", "arm64")
    if architecture not in platforms:
        raise ValueError("Unsupported previous runtime architecture")
    platform = platforms[architecture]
    historical_arm = (
        architecture == "arm64"
        and previous_meta["shim_sha256"] == "d44e3d54bf227b288bd059ece8c751339c6db0fb62a3d7c2077cd93241421255"
    )
    if (
        not historical_arm
        and not official
        and (
            previous_meta.get("status") != "UNQUALIFIED_REBUILD"
            or previous_meta.get("source") != expected["source"]
            or previous_meta.get("official_runtime") != platform["official_runtime"]
            or previous_meta.get("original_files") != platform["original_files"]
        )
    ):
        raise ValueError("Unexpected previous runtime; expected a pinned unqualified base export")
    names = set(platform["original_files"])
    for name in names:
        path = previous / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("Previous runtime must contain plain files")
        digest = sha(path)
        wanted = previous_meta["files"][name] if name == "boxlite-shim" else platform["original_files"][name]
        if digest != wanted:
            raise ValueError("Previous runtime hash mismatch: " + name)
    actual_diff = tracked_source_diff(build / "source")
    if actual_diff != (build / "output/source.patch").read_bytes():
        raise ValueError("Reviewed tracked source overlay changed during build")
    output = build / "output"
    shim = output / "boxlite-shim"
    if shim.is_symlink() or not shim.is_file():
        raise ValueError("Expected plain newly built shim")
    digest = sha(shim)
    if digest == previous_meta["shim_sha256"]:
        raise ValueError("DNS candidate must not reuse the unpatched shim")
    if (output / "SHA256SUMS").read_text().split() != [digest, "boxlite-shim"]:
        raise ValueError("Build checksum mismatch")
    receipt = json.loads((build / "source/tank-replay-dns-patch.json").read_text())
    for name, wanted in receipt["patched_sha256"].items():
        if sha(build / "source" / name) != wanted:
            raise ValueError("Patched source changed: " + name)
    if (output / "submodules.txt").read_bytes() != (BASE.parent / "submodules.txt").read_bytes():
        raise ValueError("Submodule pins changed")
    if (output / "go-version.txt").read_text().strip() != "go version go1.25.14 linux/" + architecture:
        raise ValueError("Go toolchain changed")
    if not (output / "rustc-version.txt").read_text().startswith("rustc 1.90.0 "):
        raise ValueError("Rust toolchain changed")
    if (output / "glibc-version.txt").read_text().strip() != "glibc 2.36":
        raise ValueError("Build glibc changed")
    test_result = json.loads((build / test_receipt).read_text())
    if (
        test_result.get("status") != "PASS"
        or test_result.get("network") != "none"
        or test_result.get("phase") != "test"
    ):
        raise ValueError("Offline patch controls did not pass")
    build_result = json.loads((build / build_receipt).read_text())
    if build_result.get("status") != "PASS" or build_result.get("phase") != "build":
        raise ValueError("Native build did not pass")
    if (
        build_result.get("architecture") != architecture
        or test_result.get("architecture") != architecture
        or build_result.get("image") != test_result.get("image")
    ):
        raise ValueError("Build and offline controls must use the same native builder")
    monitor_spec = importlib.util.spec_from_file_location("tank_runtime_monitor", BASE / "monitored_build.py")
    monitor = importlib.util.module_from_spec(monitor_spec)
    monitor_spec.loader.exec_module(monitor)
    recipes = monitor.recipe_hashes(BASE)
    if (
        build_result.get("recipe_sha256") != recipes
        or test_result.get("recipe_sha256") != recipes
        or build_result.get("artifact_sha256") != digest
    ):
        raise ValueError("Native output and phase receipts must match the current pinned recipe")
    spec = importlib.util.spec_from_file_location("tank_runtime_elf", BASE.parent / "finalize-runtime.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    elf = module.elf_contract(shim.read_bytes(), architecture)
    hashes = {name: digest if name == "boxlite-shim" else sha(previous / name) for name in sorted(names)}
    provenance = {
        "status": "UNQUALIFIED_REPLAY_DNS_CANDIDATE",
        "architecture": architecture,
        "source": {
            **expected["source"],
            "source_tracked_diff": "clean pinned baseline; exact reviewed replay DNS overlay retained unchanged through compilation",
        },
        "official_runtime": platform["official_runtime"],
        "original_files": platform["original_files"],
        "files": hashes,
        "shim_sha256": digest,
        "elf": elf,
        "previous_runtime": {
            "shim_sha256": previous_meta["shim_sha256"],
            "provenance_sha256": sha(previous / "provenance.json") if not official else None,
            **(
                {"official_archive_sha256": platform["official_runtime"]["archive_sha256"]}
                if official
                else {}
            ),
        },
        "native_dns_patch": receipt,
        "build": {
            "image_sha256": build_result["image"].removeprefix("sha256:"),
            "source_overlay": "deploy/boxlite/v0.10.6/replay-dns",
            "link_flags": "same upstream static relocation flags; -crt-static",
            "sdk_abi_changed": False,
        },
        "build_evidence_sha256": {p.name: sha(p) for p in sorted(output.iterdir()) if p.is_file()},
        "build_phase_receipts_sha256": {p.name: sha(p) for p in sorted(build.glob("*-receipt.json"))},
        "qualification": {
            "required_for_this_artifact": True,
            "native": "NOT_RUN",
            "production_adoption": False,
            "inherited_hardware_results": False,
        },
    }
    evidence_files = {
        "build-receipt.json": build / build_receipt,
        "test-receipt.json": build / test_receipt,
        **{
            name: output / name
            for name in (
                "source.patch",
                "patch-receipt.json",
                "go-version.txt",
                "rustc-version.txt",
                "glibc-version.txt",
                "submodules.txt",
            )
        },
    }
    for path in evidence_files.values():
        if path.is_symlink() or not path.is_file():
            raise ValueError("Local native build evidence requires plain files")
    provenance["build_contract"] = {
        "schema": "tank.boxlite-local-build.v1",
        "evidence_sha256": {name: sha(path) for name, path in evidence_files.items()},
        "trust": "Locally built operator artifact; evidence hashes are not third-party signed attestation",
    }
    destination.mkdir()
    for name in sorted(names):
        shutil.copy2(shim if name == "boxlite-shim" else previous / name, destination / name)
    (destination / "build-evidence").mkdir()
    for name, path in evidence_files.items():
        shutil.copy2(path, destination / "build-evidence" / name)
    (destination / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    (destination / "SHA256SUMS").write_text("".join(f"{value}  {name}\n" for name, value in hashes.items()))
    return provenance


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", required=True, type=Path)
    parser.add_argument("--previous-runtime", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--test-receipt", default="test-receipt.json")
    parser.add_argument("--build-receipt", default="build-receipt.json")
    args = parser.parse_args()
    print(
        json.dumps(
            package(
                args.build, args.previous_runtime, args.destination, args.test_receipt, args.build_receipt
            ),
            indent=2,
        )
    )
