"""Verify and stage the frozen capture bytes; never regenerate expected results."""

import argparse
import hashlib
import json
import tarfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "tests/fixtures/retained-market"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stage(destination: Path) -> dict:
    manifest = json.loads((BUNDLE / "manifest.json").read_text())
    archive = BUNDLE / "captures.tar.gz"
    if digest(archive.read_bytes()) != manifest["archive_sha256"]:
        raise ValueError("retained archive checksum mismatch")
    entries = {entry["path"]: entry for entry in manifest["files"]}
    if len(entries) != len(manifest["files"]):
        raise ValueError("duplicate manifest paths")
    staged = {}
    with tarfile.open(archive, "r:gz") as source:
        for member in source:
            name = member.name
            path = PurePosixPath(name)
            if (
                not member.isfile()
                or path.is_absolute()
                or ".." in path.parts
                or str(path) != name
                or not (name == "results.json" or name.startswith("data/"))
                or name not in entries
                or name in staged
                or member.size != entries[name]["bytes"]
            ):
                raise ValueError("unsafe or unexpected retained archive member")
            stream = source.extractfile(member)
            if stream is None:
                raise ValueError("retained archive member has no bytes")
            data = stream.read(member.size + 1)
            if len(data) != member.size or digest(data) != entries[name]["sha256"]:
                raise ValueError("retained member checksum mismatch")
            staged[name] = data
    if staged.keys() != entries.keys():
        raise ValueError("retained archive is incomplete")
    if destination.is_symlink():
        raise ValueError("fixture destination must not be a symlink")
    destination.mkdir(parents=True, exist_ok=True)
    for name, data in staged.items():
        target = destination / name
        for ancestor in target.relative_to(destination).parents:
            parent = destination / ancestor
            if parent.is_symlink():
                raise ValueError("fixture parent must not be a symlink")
        if target.is_symlink():
            raise ValueError("fixture target must not be a symlink")
        if target.exists():
            if not target.is_file() or target.read_bytes() != data:
                raise ValueError("refusing to overwrite different retained input")
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
    return {"files": len(staged), "archive_sha256": manifest["archive_sha256"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, default=ROOT / "fixture_check")
    args = parser.parse_args()
    result = stage(args.destination)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
