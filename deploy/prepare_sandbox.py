#!/usr/bin/env python3
"""Verify and unpack a Docker buildx OCI export for BoxLite rootfsPath."""

import argparse
import hashlib
import json
import shutil
import tarfile
from pathlib import Path, PurePosixPath


def prepare(archive: Path, destination: Path, *, boxlite_version="0.9.0", architecture=None):
    if boxlite_version not in {"0.9.0", "0.10.6"}:
        raise ValueError("Unsupported qualified BoxLite loader version")
    if architecture is not None and architecture not in {"amd64", "arm64"}:
        raise ValueError("Supported Linux architectures are amd64 and arm64")
    destination = Path(destination)
    if destination.exists():
        raise ValueError("OCI destination must be new")
    destination.mkdir(parents=True)
    try:
        with tarfile.open(archive, "r:*") as bundle:
            for member in bundle:
                relative = PurePosixPath(member.name)
                if (
                    relative.is_absolute()
                    or ".." in relative.parts
                    or not (member.isfile() or member.isdir())
                ):
                    raise ValueError("Unsafe OCI archive member")
                target = destination.joinpath(*relative.parts)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with bundle.extractfile(member) as source, target.open("wb") as output:
                        shutil.copyfileobj(source, output)
        layout = json.loads((destination / "oci-layout").read_text())
        if layout != {"imageLayoutVersion": "1.0.0"}:
            raise ValueError("Unsupported OCI layout")
        index = json.loads((destination / "index.json").read_text())
        if index.get("schemaVersion") != 2 or not index.get("manifests"):
            raise ValueError("Invalid OCI index")
        verified = 0
        platforms = set()

        def verify(descriptor):
            nonlocal verified
            descriptor_platforms = set()
            algorithm, digest = descriptor["digest"].split(":", 1)
            if algorithm != "sha256" or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("Unsupported OCI digest")
            path = destination / "blobs" / algorithm / digest
            with path.open("rb") as blob:
                actual_digest = hashlib.file_digest(blob, "sha256").hexdigest()
            if path.stat().st_size != descriptor["size"] or actual_digest != digest:
                raise ValueError("OCI digest/size mismatch")
            verified += 1
            if descriptor.get("mediaType") in {
                "application/vnd.oci.image.config.v1+json",
                "application/vnd.docker.container.image.v1+json",
            }:
                config = json.loads(path.read_text())
                platform = (config.get("os"), config.get("architecture"))
                if platform not in {("linux", "amd64"), ("linux", "arm64")}:
                    raise ValueError("OCI guest must be Linux amd64 or arm64")
                platforms.add(platform)
                descriptor_platforms.add(platform)
            if descriptor.get("mediaType", "").endswith(("manifest.v1+json", "index.v1+json")):
                document = json.loads(path.read_text())
                for child in (
                    document.get("manifests", [])
                    + document.get("layers", [])
                    + ([document["config"]] if "config" in document else [])
                ):
                    descriptor_platforms.update(verify(child))
            declared = descriptor.get("platform")
            if declared is not None and descriptor_platforms != {
                (declared.get("os"), declared.get("architecture"))
            }:
                raise ValueError("OCI descriptor platform does not match its image config")
            return descriptor_platforms

        for descriptor in index["manifests"]:
            verify(descriptor)
        if len(platforms) != 1:
            raise ValueError("Export exactly one guest architecture for the native BoxLite runtime")
        _, actual_architecture = platforms.pop()
        if architecture is not None and actual_architecture != architecture:
            raise ValueError("OCI guest architecture does not match requested native runtime")
        proof = {
            "status": "VERIFIED_OCI_LAYOUT",
            "verified_descriptors": verified,
            "index_sha256": hashlib.sha256((destination / "index.json").read_bytes()).hexdigest(),
            "boxlite_version": boxlite_version,
            "architecture": actual_architecture,
            "loader_option": "rootfsPath",
            "vm_execution": "UNVERIFIED_KVM_REQUIRED",
        }
        (destination / "tank-oci-verification.json").write_text(json.dumps(proof, indent=2) + "\n")
        return proof
    except Exception:
        shutil.rmtree(destination)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--boxlite-version", choices=["0.9.0", "0.10.6"], default="0.9.0")
    parser.add_argument("--architecture", choices=["amd64", "arm64"])
    args = parser.parse_args()
    print(
        json.dumps(
            prepare(
                args.archive,
                args.destination,
                boxlite_version=args.boxlite_version,
                architecture=args.architecture,
            )
        )
    )
