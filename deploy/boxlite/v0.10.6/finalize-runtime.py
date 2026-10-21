#!/usr/bin/env python3
"""Package a pinned builder export; deliberately grants no hardware qualification."""

import argparse
import copy
import hashlib
import json
import re
import shutil
import struct
from pathlib import Path

BASE = Path(__file__).resolve().parent


def platform_contract(architecture):
    platforms = json.loads((BASE / "platforms.json").read_text())["platforms"]
    try:
        return platforms[architecture]
    except KeyError:
        raise ValueError("Supported Linux architectures are amd64 and arm64") from None


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def elf_contract(data, architecture=None):
    if len(data) < 64 or data[:6] != b"\x7fELF\x02\x01":
        raise ValueError("Expected little-endian Linux ELF64")
    machine = struct.unpack_from("<H", data, 18)[0]
    detected = {183: "arm64", 62: "amd64"}.get(machine)
    if detected is None or (architecture is not None and architecture != detected):
        raise ValueError("ELF machine does not match supported/requested architecture")
    architecture = detected
    platform = platform_contract(architecture)
    offset = struct.unpack_from("<Q", data, 32)[0]
    size, count = struct.unpack_from("<HH", data, 54)
    if size != 56 or not count or offset < 64 or offset + size * count > len(data):
        raise ValueError("Invalid ELF program headers")
    headers = [struct.unpack_from("<IIQQQQQQ", data, offset + i * size) for i in range(count)]
    if any(p[2] + p[5] > len(data) for p in headers):
        raise ValueError("ELF segment outside file")
    interpreters = [data[p[2] : p[2] + p[5]].rstrip(b"\0").decode() for p in headers if p[0] == 3]
    if interpreters != [platform["interpreter"]]:
        raise ValueError("Expected system Bookworm dynamic loader")
    dynamic = [p for p in headers if p[0] == 2]
    if len(dynamic) != 1:
        raise ValueError("Missing unique dynamic section")
    p = dynamic[0]
    tags = list(struct.iter_unpack("<qQ", data[p[2] : p[2] + p[5]]))
    strtab = [value for tag, value in tags if tag == 5]
    if len(strtab) != 1:
        raise ValueError("Missing dynamic string table")
    segments = [p for p in headers if p[0] == 1 and p[3] <= strtab[0] < p[3] + p[5]]
    if len(segments) != 1:
        raise ValueError("Invalid string table mapping")
    p = segments[0]
    start = p[2] + strtab[0] - p[3]

    def string(n):
        return data[start + n : data.index(b"\0", start + n)].decode()

    needed = [string(value) for tag, value in tags if tag == 1]
    runpath = [string(value) for tag, value in tags if tag == 29]
    if sorted(needed) != sorted(platform["needed_libraries"]) or runpath != ["$ORIGIN"]:
        raise ValueError("Unexpected shim dependencies or RUNPATH")
    versions = [tuple(map(int, v.split(b"."))) for v in re.findall(rb"GLIBC_(\d+\.\d+(?:\.\d+)?)\0", data)]
    if not versions or max(versions) > (2, 36):
        raise ValueError("Shim requires a newer glibc than Bookworm")
    return {
        "architecture": platform["elf_architecture"],
        "interpreter": interpreters[0],
        "needed": needed,
        "RUNPATH": runpath[0],
        "highest_required_glibc": ".".join(map(str, max(versions))),
        "build_system_glibc": "2.36",
        "shipped_libc_or_loader": False,
    }


def finalize(export, destination, architecture=None):
    expected = json.loads((BASE / "qualified-artifact.json").read_text())
    runtime, evidence = export / "runtime", export / "build-evidence"
    if (runtime / "boxlite-shim").is_symlink() or not (runtime / "boxlite-shim").is_file():
        raise ValueError("Expected a plain shim export")
    elf = elf_contract((runtime / "boxlite-shim").read_bytes(), architecture)
    architecture = {"aarch64": "arm64", "x86_64": "amd64"}[elf["architecture"]]
    platform = platform_contract(architecture)
    names = set(platform["original_files"])
    if {p.name for p in runtime.iterdir()} != names:
        raise ValueError("Runtime must contain exactly the eight official files")
    evidence_names = {
        "SHA256SUMS",
        "boxlite-shim",
        "build-packages.txt",
        "dynamic.txt",
        "file.txt",
        "glibc-version.txt",
        "go-version.txt",
        "ldd.txt",
        "program-headers.txt",
        "rustc-version.txt",
        "submodules.txt",
        "versions.txt",
    }
    if {p.name for p in evidence.iterdir()} != evidence_names:
        raise ValueError("Unexpected build evidence file set")
    for path in [*runtime.iterdir(), *evidence.iterdir()]:
        if path.is_symlink() or not path.is_file():
            raise ValueError("Only regular, non-symlink files are allowed: " + str(path))
    hashes = {name: sha(runtime / name) for name in sorted(names)}
    for name in names - {"boxlite-shim"}:
        if hashes[name] != platform["original_files"][name]:
            raise ValueError("Official runtime file changed: " + name)
    digest = hashes["boxlite-shim"]
    if sha(evidence / "boxlite-shim") != digest or (evidence / "SHA256SUMS").read_text().split() != [
        digest,
        "boxlite-shim",
    ]:
        raise ValueError("Build output/shim checksum mismatch")
    if (evidence / "submodules.txt").read_bytes() != (BASE / "submodules.txt").read_bytes():
        raise ValueError("Submodule pins differ")
    if not (evidence / "rustc-version.txt").read_text().startswith("rustc 1.90.0 "):
        raise ValueError("Unexpected Rust toolchain")
    if (evidence / "go-version.txt").read_text().strip() != "go version go1.25.14 linux/" + architecture:
        raise ValueError("Unexpected Go toolchain")
    if (evidence / "glibc-version.txt").read_text().strip() != "glibc 2.36":
        raise ValueError("Unexpected build glibc")
    meta = {
        key: copy.deepcopy(expected[key])
        for key in (
            "source",
            "official_runtime",
            "original_files",
            "build",
            "changed_files",
            "reproducibility",
        )
    }
    meta["build"].pop("elapsed_compile_seconds", None)
    meta["official_runtime"] = copy.deepcopy(platform["official_runtime"])
    meta["original_files"] = copy.deepcopy(platform["original_files"])
    meta["build"]["go_archive_sha256"] = platform["go_archive_sha256"]
    meta["build"]["command"] = (
        "cargo build --locked --release -p boxlite-shim --target " + platform["rust_target"]
    )
    meta.update(
        status="UNQUALIFIED_REBUILD",
        architecture=architecture,
        files=hashes,
        shim_sha256=digest,
        elf=elf,
        build_evidence_sha256={p.name: sha(p) for p in sorted(evidence.iterdir())},
        qualification={
            "required_for_this_artifact": True,
            "production_adoption": False,
            "inherited_hardware_results": False,
        },
    )
    destination.mkdir(parents=False, exist_ok=False)
    for name in sorted(names):
        shutil.copy2(runtime / name, destination / name)
    (destination / "provenance.json").write_text(json.dumps(meta, indent=2) + "\n")
    (destination / "SHA256SUMS").write_text("".join(f"{digest}  {name}\n" for name, digest in hashes.items()))
    return meta


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--architecture", choices=["amd64", "arm64"])
    args = parser.parse_args()
    print(json.dumps(finalize(args.export, args.destination, args.architecture), indent=2))
