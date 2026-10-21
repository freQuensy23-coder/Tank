"""Verify and stage exact public supplementary captures for historical source tests."""

from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
from pathlib import Path, PurePosixPath

BUNDLE = Path(__file__).resolve().parent
ROOT = BUNDLE.parents[2]
DEFAULT_DESTINATION = ROOT / "data/calibration/supplementary-sweep-20261019-run1"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_name(name: str) -> bool:
    path = PurePosixPath(name)
    return (
        not path.is_absolute()
        and ".." not in path.parts
        and str(path) == name
        and (
            name == "raw-items.new.jsonl"
            or any(name.startswith(f"runtime-data/raw/items/{source}/") for source in ("S08", "S15", "S28"))
        )
    )


def stage(destination: Path) -> dict:
    manifest = json.loads((BUNDLE / "manifest.json").read_text())
    archive = BUNDLE / "captures.tar.gz"
    if archive.stat().st_size != manifest["archive_bytes"] or digest(archive.read_bytes()) != manifest["archive_sha256"]:
        raise ValueError("supplementary archive checksum mismatch")
    entries = {entry["path"]: entry for entry in manifest["files"]}
    if len(entries) != len(manifest["files"]) or len(entries) != manifest["total_files"]:
        raise ValueError("duplicate or incomplete supplementary manifest")
    if not all(safe_name(name) for name in entries):
        raise ValueError("unsafe supplementary manifest path")
    if entries["raw-items.new.jsonl"]["sha256"] != manifest["raw_index_sha256"]:
        raise ValueError("supplementary raw-index checksum mismatch")
    staged = {}
    with tarfile.open(archive, "r:gz") as source:
        for member in source:
            name = member.name
            if (
                not member.isfile()
                or member.mode & 0o111
                or not safe_name(name)
                or name not in entries
                or name in staged
                or member.size != entries[name]["bytes"]
            ):
                raise ValueError("unsafe or unexpected supplementary archive member")
            stream = source.extractfile(member)
            if stream is None:
                raise ValueError("supplementary archive member has no bytes")
            data = stream.read(member.size + 1)
            if len(data) != member.size or digest(data) != entries[name]["sha256"]:
                raise ValueError("supplementary member checksum mismatch")
            staged[name] = data
    if staged.keys() != entries.keys():
        raise ValueError("supplementary archive is incomplete")
    destination = destination.absolute()
    if any(parent.is_symlink() for parent in (destination, *destination.parents)):
        raise ValueError("supplementary destination must not traverse a symlink")
    destination.mkdir(parents=True, exist_ok=True)
    for name, data in staged.items():
        target = destination / name
        if any(parent.is_symlink() for parent in (target, *target.parents)):
            raise ValueError("supplementary target must not traverse a symlink")
        if target.exists():
            if not target.is_file() or target.read_bytes() != data:
                raise ValueError("refusing to overwrite different supplementary input")
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
    return {"files": len(staged), "archive_sha256": manifest["archive_sha256"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, default=DEFAULT_DESTINATION)
    args = parser.parse_args()
    print(json.dumps(stage(args.destination)))


if __name__ == "__main__":
    main()
