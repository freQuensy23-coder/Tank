#!/usr/bin/env python3
"""Verify and unpack a Docker buildx OCI export for BoxLite rootfsPath."""

import argparse
import hashlib
import json
import shutil
import tarfile
from pathlib import Path, PurePosixPath


def prepare(archive: Path, destination: Path):
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

        def verify(descriptor):
            nonlocal verified
            algorithm, digest = descriptor["digest"].split(":", 1)
            if algorithm != "sha256" or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("Unsupported OCI digest")
            path = destination / "blobs" / algorithm / digest
            with path.open("rb") as blob:
                actual_digest = hashlib.file_digest(blob, "sha256").hexdigest()
            if path.stat().st_size != descriptor["size"] or actual_digest != digest:
                raise ValueError("OCI digest/size mismatch")
            verified += 1
            if descriptor.get("mediaType", "").endswith(("manifest.v1+json", "index.v1+json")):
                document = json.loads(path.read_text())
                for child in (
                    document.get("manifests", [])
                    + document.get("layers", [])
                    + ([document["config"]] if "config" in document else [])
                ):
                    verify(child)

        for descriptor in index["manifests"]:
            verify(descriptor)
        proof = {
            "status": "VERIFIED_OCI_LAYOUT",
            "verified_descriptors": verified,
            "index_sha256": hashlib.sha256((destination / "index.json").read_bytes()).hexdigest(),
            "boxlite_version": "0.9.0",
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
    args = parser.parse_args()
    print(json.dumps(prepare(args.archive, args.destination)))
