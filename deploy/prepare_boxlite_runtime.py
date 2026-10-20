#!/usr/bin/env python3
"""Verify pinned Linux BoxLite bundles against architecture-specific qualification."""

import argparse
import hashlib
import importlib.util
import json
import struct
from pathlib import Path

BASE = Path(__file__).resolve().parent


def load_recipe(relative):
    path = BASE / "boxlite/v0.10.6" / relative
    spec = importlib.util.spec_from_file_location("tank_runtime_contract_" + path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def verify_local_build(directory, actual):
    """Verify an operator's pinned local build; do not invent native acceptance."""
    recipe = BASE / "boxlite/v0.10.6"
    architecture = actual.get("architecture")
    platforms = json.loads((recipe / "platforms.json").read_text())["platforms"]
    if architecture not in platforms:
        raise ValueError("Local build requires Linux amd64 or arm64")
    platform = platforms[architecture]
    source = json.loads((recipe / "source-pin.json").read_text())
    source["source_tracked_diff"] = (
        "clean pinned baseline; exact reviewed replay DNS overlay retained unchanged through compilation"
    )
    contract = json.loads((recipe / "replay-dns/source-contract.json").read_text())
    if (
        actual.get("status") != "UNQUALIFIED_REPLAY_DNS_CANDIDATE"
        or actual.get("source") != source
        or actual.get("official_runtime") != platform["official_runtime"]
        or actual.get("original_files") != platform["original_files"]
        or actual.get("native_dns_patch") != contract["native_dns_patch"]
        or actual.get("qualification", {}).get("native") != "NOT_RUN"
        or actual.get("qualification", {}).get("inherited_hardware_results") is not False
        or actual.get("qualification", {}).get("production_adoption") is not False
    ):
        raise ValueError("Pinned local build/source/DNS contract mismatch")
    names = set(platform["original_files"])
    if set(actual.get("files", {})) != names or {p.name for p in directory.iterdir()} != names | {
        "provenance.json",
        "SHA256SUMS",
        "build-evidence",
    }:
        raise ValueError("Unexpected local runtime file inventory")
    for name in names | {"provenance.json", "SHA256SUMS"}:
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("Local runtime requires plain files")
        if name in names:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != actual["files"][name] or (
                name != "boxlite-shim" and digest != platform["original_files"][name]
            ):
                raise ValueError("Local runtime file hash mismatch: " + name)
    files = actual["files"]
    if actual.get("shim_sha256") != files["boxlite-shim"] or (
        directory / "SHA256SUMS"
    ).read_text() != "".join(f"{digest}  {name}\n" for name, digest in sorted(files.items())):
        raise ValueError("Local runtime checksum inventory mismatch")
    evidence = directory / "build-evidence"
    expected_names = {
        "build-receipt.json",
        "test-receipt.json",
        "source.patch",
        "patch-receipt.json",
        "go-version.txt",
        "rustc-version.txt",
        "glibc-version.txt",
        "submodules.txt",
    }
    if (
        evidence.is_symlink()
        or not evidence.is_dir()
        or {p.name for p in evidence.iterdir()} != expected_names
    ):
        raise ValueError("Local build requires complete scoped build evidence")
    if set(actual["build_contract"].get("evidence_sha256", {})) != expected_names:
        raise ValueError("Local build evidence inventory mismatch")
    for path in evidence.iterdir():
        if (
            path.is_symlink()
            or not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest()
            != actual["build_contract"]["evidence_sha256"][path.name]
        ):
            raise ValueError("Local build evidence hash mismatch: " + path.name)
    if (
        hashlib.sha256((evidence / "source.patch").read_bytes()).hexdigest()
        != contract["tracked_source_diff_sha256"]
        or json.loads((evidence / "patch-receipt.json").read_text()) != contract["native_dns_patch"]
    ):
        raise ValueError("Local native DNS patch evidence mismatch")
    if (
        (evidence / "submodules.txt").read_bytes() != (recipe / "submodules.txt").read_bytes()
        or (evidence / "go-version.txt").read_text().strip() != "go version go1.25.14 linux/" + architecture
        or not (evidence / "rustc-version.txt").read_text().startswith("rustc 1.90.0 ")
        or (evidence / "glibc-version.txt").read_text().strip() != "glibc 2.36"
    ):
        raise ValueError("Local build toolchain/submodule contract mismatch")
    recipes = load_recipe("replay-dns/monitored_build.py").recipe_hashes(recipe / "replay-dns")
    receipts = [json.loads((evidence / (phase + "-receipt.json")).read_text()) for phase in ("build", "test")]
    for phase, receipt in zip(("build", "test"), receipts, strict=True):
        if (
            receipt.get("phase") != phase
            or receipt.get("status") != "PASS"
            or receipt.get("exit_code") != 0
            or receipt.get("architecture") != architecture
            or receipt.get("recipe_sha256") != recipes
        ):
            raise ValueError("Local build/offline controls did not pass the pinned recipe")
    if (
        receipts[1].get("network") != "none"
        or receipts[0].get("image") != receipts[1].get("image")
        or receipts[0].get("artifact_sha256") != files["boxlite-shim"]
    ):
        raise ValueError("Local native artifact does not match its build/offline-test receipts")
    image = receipts[0].get("image", "")
    if (
        len(image) != 71
        or not image.startswith("sha256:")
        or any(c not in "0123456789abcdef" for c in image[7:])
        or actual.get("build", {}).get("image_sha256") != image[7:]
        or actual.get("build", {}).get("sdk_abi_changed") is not False
    ):
        raise ValueError(
            "Local native build requires the exact immutable builder image and unchanged SDK ABI"
        )
    elf = load_recipe("finalize-runtime.py").elf_contract(
        (directory / "boxlite-shim").read_bytes(), architecture
    )
    if actual.get("elf") != elf:
        raise ValueError("Local runtime ELF/provenance contract mismatch")
    return {
        **actual,
        "boxlite_version": "0.10.6",
        "artifact_sha256": files["boxlite-shim"],
        "source_build_verification": "PINNED_LOCAL_BUILD_VERIFIED_NATIVE_NOT_RUN",
    }


def verify_qualification_candidate(directory):
    """Verify only the fixed reviewed candidate; grants no production acceptance."""
    directory = Path(directory).resolve()
    candidate = json.loads((BASE / "boxlite/v0.10.6/replay-dns/candidate-artifact.json").read_text())
    registered = json.loads((BASE / "boxlite/v0.10.6/qualified-artifact.json").read_text())
    if candidate.get("status") != "UNQUALIFIED_REPLAY_DNS_CANDIDATE":
        raise ValueError("Expected an explicit unqualified candidate registration")
    allowed = set(registered["files"]) | {"provenance.json", "SHA256SUMS"}
    if {p.name for p in directory.iterdir()} != allowed:
        raise ValueError("Qualification candidate file set mismatch")
    for name in allowed:
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("Qualification candidate must contain scoped plain files")
    if (
        hashlib.sha256((directory / "provenance.json").read_bytes()).hexdigest()
        != candidate["provenance_sha256"]
    ):
        raise ValueError("Qualification candidate provenance hash mismatch")
    actual = json.loads((directory / "provenance.json").read_text())
    if (
        actual.get("status") != candidate["status"]
        or actual.get("source") != candidate.get("source")
        or {key: value for key, value in actual.get("source", {}).items() if key != "source_tracked_diff"}
        != {key: value for key, value in registered["source"].items() if key != "source_tracked_diff"}
        or actual.get("official_runtime") != registered["official_runtime"]
        or actual.get("files") != candidate["files"]
        or actual.get("qualification", {}).get("native") != "NOT_RUN"
        or actual.get("qualification", {}).get("production_adoption") is not False
    ):
        raise ValueError("Qualification candidate source/status mismatch")
    if set(candidate["files"]) != set(registered["files"]):
        raise ValueError("Qualification candidate runtime inventory mismatch")
    for name, digest in candidate["files"].items():
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest:
            raise ValueError("Qualification candidate runtime hash mismatch: " + name)
        if name != "boxlite-shim" and digest != registered["files"][name]:
            raise ValueError("Qualification candidate changed official runtime: " + name)
    if actual.get("shim_sha256") != candidate["files"]["boxlite-shim"]:
        raise ValueError("Qualification candidate shim/provenance mismatch")
    expected_sums = "".join(f"{digest}  {name}\n" for name, digest in sorted(candidate["files"].items()))
    if (directory / "SHA256SUMS").read_text() != expected_sums:
        raise ValueError("Qualification candidate checksum inventory mismatch")
    return {**actual, "artifact_sha256": actual["shim_sha256"], "qualification_only": True}


def verify_runtime(directory):
    directory = Path(directory).resolve()
    actual = json.loads((directory / "provenance.json").read_text())
    if actual.get("build_contract", {}).get("schema") == "tank.boxlite-local-build.v1":
        return verify_local_build(directory, actual)
    version = actual.get("boxlite_version") or actual.get("source", {}).get("version")
    elf_architecture = actual.get("elf", {}).get("architecture", "aarch64")
    architecture = actual.get("architecture") or {"aarch64": "arm64", "x86_64": "amd64"}.get(elf_architecture)
    if architecture not in {"arm64", "amd64"}:
        raise ValueError("Supported Linux architectures are amd64 and arm64")
    if version == "0.10.6":
        if architecture != "arm64":
            raise ValueError("A new architecture runtime requires a verified pinned local build contract")
        registration = BASE / "boxlite/v0.10.6/replay-dns-qualified-artifact.json"
        expected = json.loads(registration.read_text())
        if (
            expected.get("status") != "QUALIFIED_NATIVE_REPLAY_NETWORK"
            or expected.get("qualification", {}).get("status") != "PASS"
            or actual.get("files") != expected["files"]
            or actual.get("shim_sha256") != expected["shim_sha256"]
        ):
            raise ValueError(
                "Strict replay DNS-qualified BoxLite artifact required; historical d44 fallback is not accepted"
            )
        provenance = directory / "provenance.json"
        if provenance.is_symlink() or not provenance.is_file():
            raise ValueError("BoxLite qualification requires plain provenance")
        if hashlib.sha256(provenance.read_bytes()).hexdigest() != expected["provenance_sha256"]:
            raise ValueError("BoxLite DNS qualification provenance hash mismatch")
        if actual.get("source") != expected["source"]:
            raise ValueError("BoxLite DNS qualification source mismatch")
        allowed = set(expected["files"]) | {"provenance.json", "SHA256SUMS"}
        if {p.name for p in directory.iterdir()} != allowed:
            raise ValueError("Unexpected file set in DNS-qualified runtime bundle")
        sums = directory / "SHA256SUMS"
        expected_sums = "".join(f"{digest}  {name}\n" for name, digest in sorted(expected["files"].items()))
        if sums.is_symlink() or not sums.is_file() or sums.read_text() != expected_sums:
            raise ValueError("BoxLite DNS qualification checksum inventory mismatch")
        actual = {
            **actual,
            "boxlite_version": version,
            "artifact_sha256": actual["shim_sha256"],
            # This later receipt is external: the raw build provenance remains
            # unchanged, including its truthful pre-qualification NOT_RUN state.
            "external_qualification": expected["qualification"],
        }
    elif version == "0.9.0":
        if architecture != "arm64":
            raise ValueError("Legacy runtime qualification covers arm64 only")
        expected = json.loads((BASE / "boxlite/provenance-template.json").read_text())
        for key in (
            "source_commit",
            "libkrun_commit",
            "boxlite_version",
            "target",
            "upstream_runtime_archive_sha256",
            "lockfile_sha256",
        ):
            if actual.get(key) != expected[key]:
                raise ValueError("BoxLite provenance mismatch: " + key)
        if actual.get("source_changes") is not False or actual.get("isolation_changes") is not False:
            raise ValueError("BoxLite source/isolation changes are not permitted")
    else:
        raise ValueError("Unsupported BoxLite runtime version")
    files = actual.get("files", {})
    allowed = set(expected["files"]) | {"provenance.json", "SHA256SUMS"}
    if {p.name for p in directory.iterdir()} - allowed:
        raise ValueError("Unexpected file in runtime bundle; never copy credentials into build context")
    if set(files) != set(expected["files"]):
        raise ValueError("Unexpected BoxLite runtime file set")
    for name, digest in files.items():
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("Missing/unsafe runtime file: " + name)
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError("BoxLite runtime hash mismatch: " + name)
        if name != "boxlite-shim" and digest != expected["files"][name]:
            raise ValueError("Original pinned runtime modified: " + name)
    data = (directory / "boxlite-shim").read_bytes()
    machine, interpreter = {
        "arm64": (183, "/lib/ld-linux-aarch64.so.1"),
        "amd64": (62, "/lib64/ld-linux-x86-64.so.2"),
    }[architecture]
    if len(data) < 64 or data[:6] != b"\x7fELF\x02\x01" or struct.unpack_from("<H", data, 18)[0] != machine:
        raise ValueError("Expected little-endian ELF64 " + architecture + " shim")
    phoff = struct.unpack_from("<Q", data, 32)[0]
    size, count = struct.unpack_from("<HH", data, 54)
    interpreters = []
    for i in range(count):
        p = phoff + i * size
        if struct.unpack_from("<I", data, p)[0] == 3:
            start = struct.unpack_from("<Q", data, p + 8)[0]
            length = struct.unpack_from("<Q", data, p + 32)[0]
            interpreters.append(data[start : start + length].rstrip(b"\0").decode())
    if interpreters != [interpreter]:
        raise ValueError("Shim must use the Bookworm system dynamic loader")
    if actual.get("artifact_sha256") != files["boxlite-shim"]:
        raise ValueError("Shim artifact/provenance hash mismatch")
    return {**actual, "architecture": architecture}


def prepare(directory):
    directory = Path(directory)
    provenance = directory / "provenance.json"
    if (
        provenance.is_file()
        and json.loads(provenance.read_text()).get("source", {}).get("version") == "0.10.6"
    ):
        # The versioned builder supplies its complete source/toolchain evidence.
        # Never relabel a new bundle with the legacy 0.9 export template.
        return verify_runtime(directory)
    if (directory / "guest-resize2fs").exists():
        raise ValueError(
            "A BoxLite 0.10.6 export requires its versioned provenance and qualification manifest"
        )
    meta = json.loads((BASE / "boxlite/provenance-template.json").read_text())
    meta["files"] = {n: hashlib.sha256((directory / n).read_bytes()).hexdigest() for n in meta["files"]}
    meta["artifact_sha256"] = meta["files"]["boxlite-shim"]
    meta["verification"] = {"real_kvm_guest_exec": "REQUIRED_FOR_THIS_ARTIFACT"}
    (directory / "provenance.json").write_text(json.dumps(meta, indent=2) + "\n")
    verify_runtime(directory)
    (directory / "SHA256SUMS").write_text("".join(f"{h}  {n}\n" for n, h in meta["files"].items()))
    return meta


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--verify", action="store_true")
    mode.add_argument("--verify-qualification-candidate", action="store_true")
    args = parser.parse_args()
    if args.verify_qualification_candidate:
        result = verify_qualification_candidate(args.directory)
    else:
        result = verify_runtime(args.directory) if args.verify else prepare(args.directory)
    print(
        json.dumps(
            {
                "artifact_sha256": result["artifact_sha256"],
                "verified_files": len(result["files"]),
                **(
                    {
                        "source_build_verification": result["source_build_verification"],
                        "native_qualification": "NOT_RUN",
                    }
                    if "source_build_verification" in result
                    else {}
                ),
                **(
                    {"qualification_only": True, "status": result["status"]}
                    if args.verify_qualification_candidate
                    else {}
                ),
            }
        )
    )
